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

const defaultOperationListLimit = 20

var errOperationListScopeNotFound = errors.New(
	"project or repository is not available to this caller",
)

type operationListAuthorizer interface {
	AuthorizeListOperations(
		ctx context.Context,
		principal operationHTTPPrincipal,
		projectID string,
		repositoryID string,
	) error
}

type postgresOperationListAuthorizer struct {
	pool *pgxpool.Pool
}

var _ operationListAuthorizer = (*postgresOperationListAuthorizer)(nil)

func newPostgresOperationListAuthorizer(
	pool *pgxpool.Pool,
) *postgresOperationListAuthorizer {
	if pool == nil {
		return nil
	}

	return &postgresOperationListAuthorizer{
		pool: pool,
	}
}

func (a *postgresOperationListAuthorizer) AuthorizeListOperations(
	ctx context.Context,
	principal operationHTTPPrincipal,
	projectID string,
	repositoryID string,
) error {
	if a == nil || a.pool == nil {
		return errors.New(
			"operation list authorization is not configured",
		)
	}

	if !validOperationSubject(principal.Subject) {
		return errOperationListScopeNotFound
	}

	normalizedProjectID, err := normalizeOperationUUID(projectID)
	if err != nil {
		return errOperationListScopeNotFound
	}

	normalizedRepositoryID, err := normalizeOperationUUID(repositoryID)
	if err != nil {
		return errOperationListScopeNotFound
	}

	projectUUID, err := operationUUIDValue(normalizedProjectID)
	if err != nil {
		return errOperationListScopeNotFound
	}

	repositoryUUID, err := operationUUIDValue(normalizedRepositoryID)
	if err != nil {
		return errOperationListScopeNotFound
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
		return errOperationListScopeNotFound
	}

	if err != nil {
		return fmt.Errorf(
			"verify operation list project ownership and repository scope: %w",
			err,
		)
	}

	return nil
}

func listRepositoryOperationsHandler(
	pool *pgxpool.Pool,
	authenticator operationHTTPAuthenticator,
	authorizer operationListAuthorizer,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if pool == nil || authenticator == nil || authorizer == nil {
			writeOperationProblem(
				w,
				http.StatusServiceUnavailable,
				"Operation listing is unavailable",
				"operation_list_not_configured",
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

		limit, cursor, err := parseOperationListQuery(r)
		if err != nil {
			switch {
			case errors.Is(err, errOperationListInvalidLimit):
				writeOperationProblem(
					w,
					http.StatusUnprocessableEntity,
					"Invalid operation list limit",
					"invalid_limit",
					"",
				)
			case errors.Is(err, errOperationListInvalidCursor):
				writeOperationProblem(
					w,
					http.StatusUnprocessableEntity,
					"Invalid operation list cursor",
					"invalid_cursor",
					"",
				)
			default:
				writeOperationProblem(
					w,
					http.StatusUnprocessableEntity,
					"Invalid operation list query",
					"invalid_query",
					"",
				)
			}
			return
		}

		err = authorizer.AuthorizeListOperations(
			r.Context(),
			principal,
			projectID,
			repositoryID,
		)
		if err != nil {
			if errors.Is(err, errOperationListScopeNotFound) {
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
				"Could not authorize operation listing",
				"internal_error",
				"",
			)
			return
		}

		result, err := listRepositoryOperations(
			r.Context(),
			pool,
			projectID,
			repositoryID,
			limit,
			cursor,
		)
		if err != nil {
			switch {
			case errors.Is(err, errOperationListInvalidLimit):
				writeOperationProblem(
					w,
					http.StatusUnprocessableEntity,
					"Invalid operation list limit",
					"invalid_limit",
					"",
				)
			case errors.Is(err, errOperationListInvalidCursor):
				writeOperationProblem(
					w,
					http.StatusUnprocessableEntity,
					"Invalid operation list cursor",
					"invalid_cursor",
					"",
				)
			default:
				writeOperationProblem(
					w,
					http.StatusInternalServerError,
					"Could not list repository operations",
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
				"Could not encode operation list",
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

func parseOperationListQuery(
	r *http.Request,
) (int, string, error) {
	query := r.URL.Query()

	limit := defaultOperationListLimit

	if values, exists := query["limit"]; exists {
		if len(values) != 1 {
			return 0, "", errOperationListInvalidLimit
		}

		parsedLimit, err := strconv.Atoi(values[0])
		if err != nil || parsedLimit < 1 || parsedLimit > 100 {
			return 0, "", errOperationListInvalidLimit
		}

		limit = parsedLimit
	}

	cursor := ""

	if values, exists := query["cursor"]; exists {
		if len(values) != 1 ||
			values[0] == "" ||
			len(values[0]) > 1024 {
			return 0, "", errOperationListInvalidCursor
		}

		cursor = values[0]
	}

	return limit, cursor, nil
}
