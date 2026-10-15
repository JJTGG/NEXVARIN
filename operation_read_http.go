package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var errOperationReadForbidden = errors.New(
	"request is not authorized to inspect this operation",
)

type operationReadAuthorizer interface {
	AuthorizeReadOperation(
		ctx context.Context,
		principal operationHTTPPrincipal,
		projectID string,
	) error
}

type postgresOperationReadAuthorizer struct {
	pool *pgxpool.Pool
}

var _ operationReadAuthorizer = (*postgresOperationReadAuthorizer)(nil)

func newPostgresOperationReadAuthorizer(
	pool *pgxpool.Pool,
) *postgresOperationReadAuthorizer {
	if pool == nil {
		return nil
	}

	return &postgresOperationReadAuthorizer{
		pool: pool,
	}
}

func (a *postgresOperationReadAuthorizer) AuthorizeReadOperation(
	ctx context.Context,
	principal operationHTTPPrincipal,
	projectID string,
) error {
	if a == nil || a.pool == nil {
		return errors.New(
			"operation read authorization is not configured",
		)
	}

	if !validOperationSubject(principal.Subject) {
		return errOperationReadForbidden
	}

	normalizedProjectID, err := normalizeOperationUUID(projectID)
	if err != nil {
		return fmt.Errorf("projectId: %w", err)
	}

	projectUUID, err := operationUUIDValue(normalizedProjectID)
	if err != nil {
		return fmt.Errorf("projectId: %w", err)
	}

	var authorized int

	err = a.pool.QueryRow(
		ctx,
		`
			SELECT 1
			FROM projects
			WHERE id = $1::uuid
			  AND owner_subject = $2
		`,
		projectUUID,
		principal.Subject,
	).Scan(&authorized)

	if errors.Is(err, pgx.ErrNoRows) {
		return errOperationReadForbidden
	}

	if err != nil {
		return fmt.Errorf(
			"verify operation project ownership: %w",
			err,
		)
	}

	return nil
}

func getOperationHandler(
	pool *pgxpool.Pool,
	authenticator operationHTTPAuthenticator,
	authorizer operationReadAuthorizer,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if pool == nil || authenticator == nil || authorizer == nil {
			writeOperationProblem(
				w,
				http.StatusServiceUnavailable,
				"Operation inspection is unavailable",
				"operation_read_not_configured",
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
				http.StatusServiceUnavailable,
				"Authentication is temporarily unavailable",
				"authentication_unavailable",
				"",
			)
			return
		}

		if !validOperationSubject(principal.Subject) {
			writeOperationProblem(
				w,
				http.StatusServiceUnavailable,
				"Authentication is not configured correctly",
				"authentication_unavailable",
				"",
			)
			return
		}

		operationID, err := normalizeOperationUUID(
			r.PathValue("operationId"),
		)
		if err != nil {
			writeOperationProblem(
				w,
				http.StatusUnprocessableEntity,
				"Invalid operation identifier",
				"invalid_operation_id",
				"",
			)
			return
		}

		record, err := loadOperationRecord(
			r.Context(),
			pool,
			operationID,
		)
		if errors.Is(err, errOperationRecordNotFound) {
			writeOperationProblem(
				w,
				http.StatusNotFound,
				"Operation not found",
				"not_found",
				"",
			)
			return
		}
		if err != nil {
			writeOperationProblem(
				w,
				http.StatusInternalServerError,
				"Could not load operation",
				"internal_error",
				"",
			)
			return
		}

		err = authorizer.AuthorizeReadOperation(
			r.Context(),
			principal,
			record.ProjectID,
		)
		if err != nil {
			if errors.Is(err, errOperationReadForbidden) {
				writeOperationProblem(
					w,
					http.StatusForbidden,
					"Operation inspection is forbidden",
					"forbidden",
					"",
				)
				return
			}

			writeOperationProblem(
				w,
				http.StatusInternalServerError,
				"Could not authorize operation inspection",
				"internal_error",
				"",
			)
			return
		}

		responseBody, err := json.Marshal(record)
		if err != nil {
			writeOperationProblem(
				w,
				http.StatusInternalServerError,
				"Could not encode operation response",
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
