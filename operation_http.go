package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxOperationRequestBodyBytes = 64 << 10

var (
	errOperationUnauthenticated = errors.New(
		"request authentication failed",
	)
	errOperationCreationForbidden = errors.New(
		"request is not authorized to create operations",
	)
)

// operationHTTPPrincipal represents a subject established by a trusted
// authenticator. Never populate it directly from client-controlled headers.
type operationHTTPPrincipal struct {
	Subject string
}

// operationHTTPAuthenticator must verify a bearer token and return its
// trusted subject. Invalid or missing credentials must return
// errOperationUnauthenticated. Infrastructure failures must return another
// error so the handler can fail closed without misreporting a 401.
type operationHTTPAuthenticator interface {
	Authenticate(
		ctx context.Context,
		bearerToken string,
	) (operationHTTPPrincipal, error)
}

// operationCreationAuthorizer must verify that the principal may create
// operations for the requested project and repository.
type operationCreationAuthorizer interface {
	AuthorizeCreateOperation(
		ctx context.Context,
		principal operationHTTPPrincipal,
		projectID string,
		repositoryID string,
	) error
}

type operationProblemResponse struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Detail    string `json:"detail,omitempty"`
	Code      string `json:"code,omitempty"`
	RequestID string `json:"requestId,omitempty"`
}

// createRepositoryOperationHandler returns the HTTP boundary for creating
// an operation. It is deliberately not registered by main.go until real
// authentication and authorization implementations are provided.
//
// Expected route:
//
//	POST /v1/projects/{projectId}/repositories/{repositoryId}/operations
func createRepositoryOperationHandler(
	pool *pgxpool.Pool,
	authenticator operationHTTPAuthenticator,
	authorizer operationCreationAuthorizer,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeOperationProblem(
				w,
				http.StatusMethodNotAllowed,
				"Method not allowed",
				"method_not_allowed",
				"",
			)
			return
		}

		// This boundary stays closed unless all required dependencies exist.
		if pool == nil || authenticator == nil || authorizer == nil {
			writeOperationProblem(
				w,
				http.StatusServiceUnavailable,
				"Operation creation is unavailable",
				"operation_creation_not_configured",
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

		principal, err := authenticator.Authenticate(r.Context(), token)
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

		projectID, err := normalizeOperationUUID(r.PathValue("projectId"))
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

		repositoryID, err := normalizeOperationUUID(r.PathValue("repositoryId"))
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

		// Authorization is performed before request data is sent to the store.
		err = authorizer.AuthorizeCreateOperation(
			r.Context(),
			principal,
			projectID,
			repositoryID,
		)
		if err != nil {
			switch {
			case errors.Is(err, errOperationCreationForbidden):
				writeOperationProblem(
					w,
					http.StatusForbidden,
					"Operation creation is forbidden",
					"forbidden",
					"",
				)

			case errors.Is(err, errOperationScopeNotFound):
				writeOperationProblem(
					w,
					http.StatusNotFound,
					"Project or repository not found",
					"not_found",
					"",
				)

			default:
				writeOperationProblem(
					w,
					http.StatusServiceUnavailable,
					"Authorization is temporarily unavailable",
					"authorization_unavailable",
					"",
				)
			}
			return
		}

		idempotencyKey, err := operationIdempotencyKey(r)
		if err != nil {
			writeOperationProblem(
				w,
				http.StatusBadRequest,
				"Invalid Idempotency-Key header",
				"invalid_idempotency_key",
				"",
			)
			return
		}

		requestBody, err := readOperationRequestBody(w, r)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeOperationProblem(
					w,
					http.StatusRequestEntityTooLarge,
					"Request body is too large",
					"request_body_too_large",
					"",
				)
				return
			}

			writeOperationProblem(
				w,
				http.StatusBadRequest,
				"Invalid request body",
				"invalid_request_body",
				"",
			)
			return
		}

		// Invalid JSON syntax is a 400. Valid JSON that violates the
		// operation request contract is a 422.
		if !json.Valid(requestBody) {
			writeOperationProblem(
				w,
				http.StatusBadRequest,
				"Malformed JSON",
				"malformed_json",
				"",
			)
			return
		}

		if _, _, _, err := normalizeOperationRequest(requestBody); err != nil {
			writeOperationProblem(
				w,
				http.StatusUnprocessableEntity,
				"Operation request is invalid",
				"invalid_operation_request",
				"",
			)
			return
		}

		// Scope is server-derived so clients cannot choose another
		// project's or repository's idempotency namespace.
		result, err := createOperation(
			r.Context(),
			pool,
			createOperationInput{
				ProjectID:        projectID,
				RepositoryID:     repositoryID,
				RequestedBy:      principal.Subject,
				IdempotencyScope: "repository:" + repositoryID,
				IdempotencyKey:   idempotencyKey,
				RequestBody:      requestBody,
			},
		)
		if err != nil {
			switch {
			case errors.Is(err, errIdempotencyConflict):
				writeOperationProblem(
					w,
					http.StatusConflict,
					"Idempotency key conflict",
					"idempotency_conflict",
					"",
				)

			case errors.Is(err, errOperationScopeNotFound),
				errors.Is(err, errOperationResourceUnavailable):
				writeOperationProblem(
					w,
					http.StatusNotFound,
					"Operation resource not found",
					"not_found",
					"",
				)

			case errors.Is(err, errOperationScopeInactive):
				writeOperationProblem(
					w,
					http.StatusConflict,
					"Project or repository is not active",
					"operation_scope_inactive",
					"",
				)

			default:
				var postgresError *pgconn.PgError
				if errors.As(err, &postgresError) {
					// Do not disclose SQL errors, constraint names, or
					// internal database diagnostics to the client.
					writeOperationProblem(
						w,
						http.StatusInternalServerError,
						"Could not create operation",
						"internal_error",
						"",
					)
					return
				}

				writeOperationProblem(
					w,
					http.StatusServiceUnavailable,
					"Operation storage is temporarily unavailable",
					"operation_store_unavailable",
					"",
				)
			}
			return
		}

		statusCode := http.StatusAccepted
		replayed := "false"
		if result.Replayed {
			statusCode = http.StatusOK
			replayed = "true"
		}

		responseBody, err := json.Marshal(result.Operation)
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

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Idempotency-Replayed", replayed)
		w.WriteHeader(statusCode)

		_, _ = w.Write(append(responseBody, '\n'))
	})
}

func bearerTokenFromRequest(r *http.Request) (string, bool) {
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		return "", false
	}

	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", false
	}

	return parts[1], true
}

func validOperationSubject(subject string) bool {
	return strings.TrimSpace(subject) != "" &&
		utf8.ValidString(subject) &&
		strings.IndexByte(subject, 0) < 0
}

func operationIdempotencyKey(r *http.Request) (string, error) {
	values := r.Header.Values("Idempotency-Key")
	if len(values) != 1 {
		return "", errors.New("exactly one Idempotency-Key header is required")
	}

	key := values[0]
	if strings.TrimSpace(key) == "" {
		return "", errors.New("Idempotency-Key cannot be empty")
	}
	if !utf8.ValidString(key) || strings.IndexByte(key, 0) >= 0 {
		return "", errors.New("Idempotency-Key is invalid")
	}
	if utf8.RuneCountInString(key) > 255 {
		return "", errors.New("Idempotency-Key exceeds 255 characters")
	}

	return key, nil
}

func readOperationRequestBody(
	w http.ResponseWriter,
	r *http.Request,
) ([]byte, error) {
	contentType := r.Header.Get("Content-Type")
	mediaType, parameters, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		return nil, fmt.Errorf("Content-Type must be application/json")
	}

	if charset, ok := parameters["charset"]; ok &&
		!strings.EqualFold(charset, "utf-8") {
		return nil, errors.New("JSON request charset must be UTF-8")
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxOperationRequestBodyBytes)
	defer r.Body.Close()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func writeOperationProblem(
	w http.ResponseWriter,
	statusCode int,
	title string,
	code string,
	detail string,
) {
	body, err := json.Marshal(operationProblemResponse{
		Type:   "about:blank",
		Title:  title,
		Status: statusCode,
		Code:   code,
		Detail: detail,
	})
	if err != nil {
		// The fields above are fixed strings, so encoding should not fail.
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/problem+json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(statusCode)
	_, _ = w.Write(append(body, '\n'))
}
