package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultArtifactListLimit = 20

var errArtifactListScopeNotFound = errors.New(
	"project or repository is not available to this caller",
)

type artifactListAuthorizer interface {
	AuthorizeListArtifacts(
		ctx context.Context,
		principal operationHTTPPrincipal,
		projectID string,
		repositoryID string,
	) error
}

type postgresArtifactListAuthorizer struct {
	pool *pgxpool.Pool
}

var _ artifactListAuthorizer = (*postgresArtifactListAuthorizer)(nil)

func newPostgresArtifactListAuthorizer(
	pool *pgxpool.Pool,
) *postgresArtifactListAuthorizer {
	if pool == nil {
		return nil
	}

	return &postgresArtifactListAuthorizer{
		pool: pool,
	}
}

func (a *postgresArtifactListAuthorizer) AuthorizeListArtifacts(
	ctx context.Context,
	principal operationHTTPPrincipal,
	projectID string,
	repositoryID string,
) error {
	if a == nil || a.pool == nil {
		return errors.New(
			"artifact list authorization is not configured",
		)
	}

	if !validOperationSubject(principal.Subject) {
		return errArtifactListScopeNotFound
	}

	normalizedProjectID, err := normalizeOperationUUID(projectID)
	if err != nil {
		return errArtifactListScopeNotFound
	}

	normalizedRepositoryID, err := normalizeOperationUUID(repositoryID)
	if err != nil {
		return errArtifactListScopeNotFound
	}

	projectUUID, err := operationUUIDValue(normalizedProjectID)
	if err != nil {
		return errArtifactListScopeNotFound
	}

	repositoryUUID, err := operationUUIDValue(normalizedRepositoryID)
	if err != nil {
		return errArtifactListScopeNotFound
	}

	var authorized int

	err = a.pool.QueryRow(
		ctx,
		`
			SELECT 1
			FROM projects AS p
			JOIN repositories AS r
			  ON r.project_id = p.id
			WHERE p.id = $1::uuid
			  AND r.id = $2::uuid
			  AND p.owner_subject = $3
		`,
		projectUUID,
		repositoryUUID,
		principal.Subject,
	).Scan(&authorized)

	if errors.Is(err, pgx.ErrNoRows) {
		return errArtifactListScopeNotFound
	}

	if err != nil {
		return fmt.Errorf(
			"verify artifact list project ownership and repository scope: %w",
			err,
		)
	}

	return nil
}

func listRepositoryArtifactsHandler(
	pool *pgxpool.Pool,
	authenticator operationHTTPAuthenticator,
	authorizer artifactListAuthorizer,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if pool == nil || authenticator == nil || authorizer == nil {
			writeOperationProblem(
				w,
				http.StatusServiceUnavailable,
				"Artifact listing is unavailable",
				"artifact_list_not_configured",
				"",
			)
			return
		}

		token, ok := bearerTokenFromRequest(r)
		if !ok {
			w.Header().Set(
				"WWW-Authenticate",
				`Bearer realm="nexvarin"`,
			)
			writeOperationProblem(
				w,
				http.StatusUnauthorized,
				"Authentication required",
				"unauthorized",
				"",
			)
			return
		}

		principal, err := authenticator.Authenticate(
			r.Context(),
			token,
		)
		if err != nil {
			if errors.Is(err, errOperationUnauthenticated) {
				w.Header().Set(
					"WWW-Authenticate",
					`Bearer realm="nexvarin"`,
				)
				writeOperationProblem(
					w,
					http.StatusUnauthorized,
					"Authentication required",
					"unauthorized",
					"",
				)
				return
			}

			writeOperationProblem(
				w,
				http.StatusInternalServerError,
				"Authentication is temporarily unavailable",
				"authentication_unavailable",
				"",
			)
			return
		}

		if !validOperationSubject(principal.Subject) {
			writeOperationProblem(
				w,
				http.StatusInternalServerError,
				"Authentication is not configured correctly",
				"authentication_unavailable",
				"",
			)
			return
		}

		projectID, err := normalizeOperationUUID(
			r.PathValue("projectId"),
		)
		if err != nil {
			writeOperationProblem(
				w,
				http.StatusUnprocessableEntity,
				"Invalid project identifier",
				"invalid_project_id",
				"",
			)
			return
		}

		repositoryID, err := normalizeOperationUUID(
			r.PathValue("repositoryId"),
		)
		if err != nil {
			writeOperationProblem(
				w,
				http.StatusUnprocessableEntity,
				"Invalid repository identifier",
				"invalid_repository_id",
				"",
			)
			return
		}

		limit, cursor, err := parseArtifactListQuery(r)
		if err != nil {
			switch {
			case errors.Is(err, errArtifactListInvalidLimit):
				writeOperationProblem(
					w,
					http.StatusUnprocessableEntity,
					"Invalid artifact list limit",
					"invalid_limit",
					"",
				)
			case errors.Is(err, errArtifactListInvalidCursor):
				writeOperationProblem(
					w,
					http.StatusUnprocessableEntity,
					"Invalid artifact list cursor",
					"invalid_cursor",
					"",
				)
			default:
				writeOperationProblem(
					w,
					http.StatusUnprocessableEntity,
					"Invalid artifact list query",
					"invalid_query",
					"",
				)
			}
			return
		}

		err = authorizer.AuthorizeListArtifacts(
			r.Context(),
			principal,
			projectID,
			repositoryID,
		)
		if err != nil {
			if errors.Is(err, errArtifactListScopeNotFound) {
				writeOperationProblem(
					w,
					http.StatusNotFound,
					"Project or repository not found",
					"not_found",
					"",
				)
				return
			}

			writeOperationProblem(
				w,
				http.StatusInternalServerError,
				"Could not authorize artifact listing",
				"internal_error",
				"",
			)
			return
		}

		result, err := listRepositoryArtifacts(
			r.Context(),
			pool,
			projectID,
			repositoryID,
			limit,
			cursor,
		)
		if err != nil {
			switch {
			case errors.Is(err, errArtifactListInvalidLimit):
				writeOperationProblem(
					w,
					http.StatusUnprocessableEntity,
					"Invalid artifact list limit",
					"invalid_limit",
					"",
				)
			case errors.Is(err, errArtifactListInvalidCursor):
				writeOperationProblem(
					w,
					http.StatusUnprocessableEntity,
					"Invalid artifact list cursor",
					"invalid_cursor",
					"",
				)
			default:
				writeOperationProblem(
					w,
					http.StatusInternalServerError,
					"Could not list repository artifacts",
					"internal_error",
					"",
				)
			}
			return
		}

		responseBody, err := json.Marshal(result)
		if err != nil {
			writeOperationProblem(
				w,
				http.StatusInternalServerError,
				"Could not encode artifact list",
				"internal_error",
				"",
			)
			return
		}

		w.Header().Set(
			"Content-Type",
			"application/json; charset=utf-8",
		)
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)

		_, _ = w.Write(append(responseBody, '\n'))
	})
}

func parseArtifactListQuery(
	r *http.Request,
) (int, string, error) {
	query := r.URL.Query()

	limit := defaultArtifactListLimit

	if values, exists := query["limit"]; exists {
		if len(values) != 1 {
			return 0, "", errArtifactListInvalidLimit
		}

		parsedLimit, err := strconv.Atoi(values[0])
		if err != nil || parsedLimit < 1 || parsedLimit > 100 {
			return 0, "", errArtifactListInvalidLimit
		}

		limit = parsedLimit
	}

	cursor := ""

	if values, exists := query["cursor"]; exists {
		if len(values) != 1 ||
			values[0] == "" ||
			len(values[0]) > 1024 {
			return 0, "", errArtifactListInvalidCursor
		}

		cursor = values[0]
	}

	return limit, cursor, nil
}
