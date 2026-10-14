package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	operationHTTPTestProjectID = "123e4567-e89b-12d3-a456-426614174000"
	operationHTTPTestRepoID    = "123e4567-e89b-12d3-a456-426614174001"
	operationHTTPTestToken     = "test-bearer-token"
)

type operationHTTPTestAuthenticator struct {
	principal     operationHTTPPrincipal
	err           error
	calls         int
	receivedToken string
}

func (a *operationHTTPTestAuthenticator) Authenticate(
	_ context.Context,
	token string,
) (operationHTTPPrincipal, error) {
	a.calls++
	a.receivedToken = token

	if a.err != nil {
		return operationHTTPPrincipal{}, a.err
	}

	return a.principal, nil
}

type operationHTTPTestAuthorizer struct {
	err               error
	calls             int
	receivedPrincipal operationHTTPPrincipal
	projectID         string
	repositoryID      string
}

func (a *operationHTTPTestAuthorizer) AuthorizeCreateOperation(
	_ context.Context,
	principal operationHTTPPrincipal,
	projectID string,
	repositoryID string,
) error {
	a.calls++
	a.receivedPrincipal = principal
	a.projectID = projectID
	a.repositoryID = repositoryID

	return a.err
}

func operationHTTPTestBuildRequest(revision string) []byte {
	return []byte(
		`{"operationType":"build","sourceRevision":"` +
			revision + `"}`,
	)
}

func serveOperationHTTPTestRequest(
	t *testing.T,
	handler http.Handler,
	method string,
	projectID string,
	repositoryID string,
	body []byte,
	configure func(*http.Request),
) *httptest.ResponseRecorder {
	t.Helper()

	path := "/v1/projects/" + projectID +
		"/repositories/" + repositoryID + "/operations"

	request := httptest.NewRequest(
		method,
		path,
		bytes.NewReader(body),
	)
	request.SetPathValue("projectId", projectID)
	request.SetPathValue("repositoryId", repositoryID)

	request.Header.Set("Authorization", "Bearer "+operationHTTPTestToken)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "http-test-idempotency-key")

	if configure != nil {
		configure(request)
	}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	return response
}

func assertOperationHTTPProblem(
	t *testing.T,
	response *httptest.ResponseRecorder,
	wantStatus int,
	wantCode string,
) operationProblemResponse {
	t.Helper()

	if response.Code != wantStatus {
		t.Fatalf(
			"HTTP status = %d, want %d; body: %s",
			response.Code,
			wantStatus,
			response.Body.String(),
		)
	}

	contentType := response.Header().Get("Content-Type")
	if !strings.HasPrefix(contentType, "application/problem+json") {
		t.Errorf(
			"Content-Type = %q, want application/problem+json",
			contentType,
		)
	}

	var problem operationProblemResponse
	if err := json.Unmarshal(response.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode problem response: %v", err)
	}

	if problem.Status != wantStatus {
		t.Errorf(
			"problem status = %d, want %d",
			problem.Status,
			wantStatus,
		)
	}

	if problem.Code != wantCode {
		t.Errorf(
			"problem code = %q, want %q",
			problem.Code,
			wantCode,
		)
	}

	if problem.Title == "" {
		t.Error("problem title is empty")
	}

	if response.Header().Get("Cache-Control") != "no-store" {
		t.Errorf(
			"Cache-Control = %q, want no-store",
			response.Header().Get("Cache-Control"),
		)
	}

	return problem
}

func newOperationHTTPTestDependencies(
	subject string,
) (*operationHTTPTestAuthenticator, *operationHTTPTestAuthorizer) {
	return &operationHTTPTestAuthenticator{
		principal: operationHTTPPrincipal{Subject: subject},
	}, &operationHTTPTestAuthorizer{}
}

func TestCreateRepositoryOperationHandlerFailsClosedWithoutDependencies(
	t *testing.T,
) {
	authenticator, authorizer := newOperationHTTPTestDependencies(
		"test-subject",
	)

	tests := []struct {
		name          string
		pool          *pgxpool.Pool
		authenticator operationHTTPAuthenticator
		authorizer    operationCreationAuthorizer
	}{
		{
			name:          "missing database pool",
			pool:          nil,
			authenticator: authenticator,
			authorizer:    authorizer,
		},
		{
			name:          "missing authenticator",
			pool:          &pgxpool.Pool{},
			authenticator: nil,
			authorizer:    authorizer,
		},
		{
			name:          "missing authorizer",
			pool:          &pgxpool.Pool{},
			authenticator: authenticator,
			authorizer:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := createRepositoryOperationHandler(
				test.pool,
				test.authenticator,
				test.authorizer,
			)

			response := serveOperationHTTPTestRequest(
				t,
				handler,
				http.MethodPost,
				operationHTTPTestProjectID,
				operationHTTPTestRepoID,
				operationHTTPTestBuildRequest(strings.Repeat("a", 40)),
				nil,
			)

			assertOperationHTTPProblem(
				t,
				response,
				http.StatusServiceUnavailable,
				"operation_creation_not_configured",
			)
		})
	}
}

func TestCreateRepositoryOperationHandlerRejectsWrongMethod(
	t *testing.T,
) {
	handler := createRepositoryOperationHandler(nil, nil, nil)

	response := serveOperationHTTPTestRequest(
		t,
		handler,
		http.MethodGet,
		operationHTTPTestProjectID,
		operationHTTPTestRepoID,
		nil,
		nil,
	)

	assertOperationHTTPProblem(
		t,
		response,
		http.StatusMethodNotAllowed,
		"method_not_allowed",
	)

	if got := response.Header().Get("Allow"); got != http.MethodPost {
		t.Errorf("Allow header = %q, want POST", got)
	}
}

func TestCreateRepositoryOperationHandlerRejectsInvalidBearerHeaders(
	t *testing.T,
) {
	authenticator, authorizer := newOperationHTTPTestDependencies(
		"test-subject",
	)
	handler := createRepositoryOperationHandler(
		&pgxpool.Pool{},
		authenticator,
		authorizer,
	)

	tests := []struct {
		name      string
		configure func(*http.Request)
	}{
		{
			name: "missing authorization",
			configure: func(request *http.Request) {
				request.Header.Del("Authorization")
			},
		},
		{
			name: "non bearer scheme",
			configure: func(request *http.Request) {
				request.Header.Set("Authorization", "Basic credentials")
			},
		},
		{
			name: "duplicate authorization headers",
			configure: func(request *http.Request) {
				request.Header.Add(
					"Authorization",
					"Bearer second-token",
				)
			},
		},
		{
			name: "empty bearer token",
			configure: func(request *http.Request) {
				request.Header.Set("Authorization", "Bearer")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authenticator.calls = 0
			authorizer.calls = 0

			response := serveOperationHTTPTestRequest(
				t,
				handler,
				http.MethodPost,
				operationHTTPTestProjectID,
				operationHTTPTestRepoID,
				operationHTTPTestBuildRequest(strings.Repeat("a", 40)),
				test.configure,
			)

			assertOperationHTTPProblem(
				t,
				response,
				http.StatusUnauthorized,
				"unauthorized",
			)

			if got := response.Header().Get("WWW-Authenticate"); got == "" {
				t.Error("WWW-Authenticate header is missing")
			}

			if authenticator.calls != 0 {
				t.Errorf(
					"authenticator calls = %d, want 0",
					authenticator.calls,
				)
			}

			if authorizer.calls != 0 {
				t.Errorf(
					"authorizer calls = %d, want 0",
					authorizer.calls,
				)
			}
		})
	}
}

func TestCreateRepositoryOperationHandlerHandlesAuthenticationErrors(
	t *testing.T,
) {
	tests := []struct {
		name          string
		principal     operationHTTPPrincipal
		authErr       error
		wantStatus    int
		wantCode      string
		wantChallenge bool
	}{
		{
			name:          "invalid credentials",
			authErr:       errOperationUnauthenticated,
			wantStatus:    http.StatusUnauthorized,
			wantCode:      "unauthorized",
			wantChallenge: true,
		},
		{
			name:       "authentication infrastructure failure",
			authErr:    errors.New("identity provider unavailable"),
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "authentication_unavailable",
		},
		{
			name:       "empty trusted subject",
			principal:  operationHTTPPrincipal{},
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "authentication_unavailable",
		},
		{
			name:       "whitespace-only trusted subject",
			principal:  operationHTTPPrincipal{Subject: "   "},
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "authentication_unavailable",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authenticator := &operationHTTPTestAuthenticator{
				principal: test.principal,
				err:       test.authErr,
			}
			authorizer := &operationHTTPTestAuthorizer{}

			if test.authErr == nil &&
				test.principal.Subject == "" &&
				test.name == "empty trusted subject" {
				authenticator.principal = operationHTTPPrincipal{}
			}

			handler := createRepositoryOperationHandler(
				&pgxpool.Pool{},
				authenticator,
				authorizer,
			)

			response := serveOperationHTTPTestRequest(
				t,
				handler,
				http.MethodPost,
				operationHTTPTestProjectID,
				operationHTTPTestRepoID,
				operationHTTPTestBuildRequest(strings.Repeat("a", 40)),
				nil,
			)

			assertOperationHTTPProblem(
				t,
				response,
				test.wantStatus,
				test.wantCode,
			)

			if test.wantChallenge &&
				response.Header().Get("WWW-Authenticate") == "" {
				t.Error("WWW-Authenticate header is missing")
			}

			if authorizer.calls != 0 {
				t.Errorf(
					"authorizer calls = %d, want 0",
					authorizer.calls,
				)
			}
		})
	}
}

func TestCreateRepositoryOperationHandlerValidatesPathIdentifiers(
	t *testing.T,
) {
	tests := []struct {
		name         string
		projectID    string
		repositoryID string
		wantCode     string
	}{
		{
			name:         "invalid project ID",
			projectID:    "not-a-uuid",
			repositoryID: operationHTTPTestRepoID,
			wantCode:     "invalid_project_id",
		},
		{
			name:         "invalid repository ID",
			projectID:    operationHTTPTestProjectID,
			repositoryID: "not-a-uuid",
			wantCode:     "invalid_repository_id",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authenticator, authorizer := newOperationHTTPTestDependencies(
				"test-subject",
			)
			handler := createRepositoryOperationHandler(
				&pgxpool.Pool{},
				authenticator,
				authorizer,
			)

			response := serveOperationHTTPTestRequest(
				t,
				handler,
				http.MethodPost,
				test.projectID,
				test.repositoryID,
				operationHTTPTestBuildRequest(strings.Repeat("a", 40)),
				nil,
			)

			assertOperationHTTPProblem(
				t,
				response,
				http.StatusUnprocessableEntity,
				test.wantCode,
			)

			if authorizer.calls != 0 {
				t.Errorf(
					"authorizer calls = %d, want 0",
					authorizer.calls,
				)
			}
		})
	}
}

func TestCreateRepositoryOperationHandlerHandlesAuthorizationErrors(
	t *testing.T,
) {
	tests := []struct {
		name       string
		authzErr   error
		wantStatus int
		wantCode   string
	}{
		{
			name:       "forbidden",
			authzErr:   errOperationCreationForbidden,
			wantStatus: http.StatusForbidden,
			wantCode:   "forbidden",
		},
		{
			name:       "scope not found",
			authzErr:   errOperationScopeNotFound,
			wantStatus: http.StatusNotFound,
			wantCode:   "not_found",
		},
		{
			name:       "authorization infrastructure failure",
			authzErr:   errors.New("authorization service unavailable"),
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "authorization_unavailable",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authenticator, authorizer := newOperationHTTPTestDependencies(
				"test-subject",
			)
			authorizer.err = test.authzErr

			handler := createRepositoryOperationHandler(
				&pgxpool.Pool{},
				authenticator,
				authorizer,
			)

			response := serveOperationHTTPTestRequest(
				t,
				handler,
				http.MethodPost,
				operationHTTPTestProjectID,
				operationHTTPTestRepoID,
				operationHTTPTestBuildRequest(strings.Repeat("a", 40)),
				nil,
			)

			assertOperationHTTPProblem(
				t,
				response,
				test.wantStatus,
				test.wantCode,
			)

			if authorizer.calls != 1 {
				t.Errorf(
					"authorizer calls = %d, want 1",
					authorizer.calls,
				)
			}
		})
	}
}

func TestCreateRepositoryOperationHandlerValidatesRequest(
	t *testing.T,
) {
	validBody := operationHTTPTestBuildRequest(strings.Repeat("a", 40))

	tests := []struct {
		name       string
		body       []byte
		configure  func(*http.Request)
		wantStatus int
		wantCode   string
	}{
		{
			name: "missing idempotency key",
			body: validBody,
			configure: func(request *http.Request) {
				request.Header.Del("Idempotency-Key")
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_idempotency_key",
		},
		{
			name: "duplicate idempotency keys",
			body: validBody,
			configure: func(request *http.Request) {
				request.Header.Add("Idempotency-Key", "another-key")
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_idempotency_key",
		},
		{
			name: "blank idempotency key",
			body: validBody,
			configure: func(request *http.Request) {
				request.Header.Set("Idempotency-Key", "   ")
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_idempotency_key",
		},
		{
			name: "oversized idempotency key",
			body: validBody,
			configure: func(request *http.Request) {
				request.Header.Set(
					"Idempotency-Key",
					strings.Repeat("k", 256),
				)
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_idempotency_key",
		},
		{
			name: "unsupported media type",
			body: validBody,
			configure: func(request *http.Request) {
				request.Header.Set("Content-Type", "text/plain")
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request_body",
		},
		{
			name: "unsupported charset",
			body: validBody,
			configure: func(request *http.Request) {
				request.Header.Set(
					"Content-Type",
					"application/json; charset=iso-8859-1",
				)
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request_body",
		},
		{
			name:       "malformed JSON",
			body:       []byte(`{"operationType":`),
			wantStatus: http.StatusBadRequest,
			wantCode:   "malformed_json",
		},
		{
			name: "valid JSON with unknown field",
			body: []byte(
				`{"operationType":"build","sourceRevision":"` +
					strings.Repeat("a", 40) +
					`","unknown":true}`,
			),
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "invalid_operation_request",
		},
		{
			name:       "invalid git revision",
			body:       operationHTTPTestBuildRequest("not-a-revision"),
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "invalid_operation_request",
		},
		{
			name:       "unsupported operation type",
			body:       []byte(`{"operationType":"unknown"}`),
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "invalid_operation_request",
		},
		{
			name: "body exceeds limit",
			body: []byte(strings.Repeat(
				"x",
				maxOperationRequestBodyBytes+1,
			)),
			wantStatus: http.StatusRequestEntityTooLarge,
			wantCode:   "request_body_too_large",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authenticator, authorizer := newOperationHTTPTestDependencies(
				"test-subject",
			)
			handler := createRepositoryOperationHandler(
				&pgxpool.Pool{},
				authenticator,
				authorizer,
			)

			response := serveOperationHTTPTestRequest(
				t,
				handler,
				http.MethodPost,
				operationHTTPTestProjectID,
				operationHTTPTestRepoID,
				test.body,
				test.configure,
			)

			assertOperationHTTPProblem(
				t,
				response,
				test.wantStatus,
				test.wantCode,
			)

			if authorizer.calls != 1 {
				t.Errorf(
					"authorizer calls = %d, want 1",
					authorizer.calls,
				)
			}
		})
	}
}

func TestCreateRepositoryOperationHandlerCreatesAndReplays(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	authenticator, authorizer := newOperationHTTPTestDependencies(
		fixture.requestedBy,
	)
	handler := createRepositoryOperationHandler(
		fixture.pool,
		authenticator,
		authorizer,
	)

	revision := strings.Repeat("b", 40)
	body := operationHTTPTestBuildRequest(revision)
	const idempotencyKey = "http-create-replay"

	configureKey := func(request *http.Request) {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}

	firstResponse := serveOperationHTTPTestRequest(
		t,
		handler,
		http.MethodPost,
		fixture.projectID,
		fixture.repositoryID,
		body,
		configureKey,
	)

	if firstResponse.Code != http.StatusAccepted {
		t.Fatalf(
			"first HTTP status = %d, want %d; body: %s",
			firstResponse.Code,
			http.StatusAccepted,
			firstResponse.Body.String(),
		)
	}

	if got := firstResponse.Header().Get("Idempotency-Replayed"); got != "false" {
		t.Errorf("first replay header = %q, want false", got)
	}

	if got := firstResponse.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", got)
	}

	var firstOperation operationRecord
	if err := json.Unmarshal(firstResponse.Body.Bytes(), &firstOperation); err != nil {
		t.Fatalf("decode created operation: %v", err)
	}

	if firstOperation.ID == "" {
		t.Fatal("created operation ID is empty")
	}
	if firstOperation.ProjectID != fixture.projectID {
		t.Errorf(
			"project ID = %q, want %q",
			firstOperation.ProjectID,
			fixture.projectID,
		)
	}
	if firstOperation.RepositoryID != fixture.repositoryID {
		t.Errorf(
			"repository ID = %q, want %q",
			firstOperation.RepositoryID,
			fixture.repositoryID,
		)
	}
	if firstOperation.OperationType != operationTypeBuild {
		t.Errorf(
			"operation type = %q, want %q",
			firstOperation.OperationType,
			operationTypeBuild,
		)
	}

	equivalentBody := []byte(
		"{\n  \"sourceRevision\": \"" + revision +
			"\",\n  \"operationType\": \"build\"\n}",
	)

	secondResponse := serveOperationHTTPTestRequest(
		t,
		handler,
		http.MethodPost,
		fixture.projectID,
		fixture.repositoryID,
		equivalentBody,
		configureKey,
	)

	if secondResponse.Code != http.StatusOK {
		t.Fatalf(
			"replay HTTP status = %d, want %d; body: %s",
			secondResponse.Code,
			http.StatusOK,
			secondResponse.Body.String(),
		)
	}

	if got := secondResponse.Header().Get("Idempotency-Replayed"); got != "true" {
		t.Errorf("replay header = %q, want true", got)
	}

	var secondOperation operationRecord
	if err := json.Unmarshal(secondResponse.Body.Bytes(), &secondOperation); err != nil {
		t.Fatalf("decode replayed operation: %v", err)
	}

	if secondOperation.ID != firstOperation.ID {
		t.Errorf(
			"replayed operation ID = %q, want original ID %q",
			secondOperation.ID,
			firstOperation.ID,
		)
	}

	if authenticator.receivedToken != operationHTTPTestToken {
		t.Errorf(
			"authenticator received token %q, want test token",
			authenticator.receivedToken,
		)
	}

	if authorizer.receivedPrincipal.Subject != fixture.requestedBy {
		t.Errorf(
			"authorized subject = %q, want %q",
			authorizer.receivedPrincipal.Subject,
			fixture.requestedBy,
		)
	}

	if authorizer.projectID != fixture.projectID {
		t.Errorf(
			"authorized project ID = %q, want %q",
			authorizer.projectID,
			fixture.projectID,
		)
	}

	if authorizer.repositoryID != fixture.repositoryID {
		t.Errorf(
			"authorized repository ID = %q, want %q",
			authorizer.repositoryID,
			fixture.repositoryID,
		)
	}
}

func TestCreateRepositoryOperationHandlerRejectsConflictingKeyReuse(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	authenticator, authorizer := newOperationHTTPTestDependencies(
		fixture.requestedBy,
	)
	handler := createRepositoryOperationHandler(
		fixture.pool,
		authenticator,
		authorizer,
	)

	const idempotencyKey = "http-conflicting-reuse"

	configureKey := func(request *http.Request) {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}

	firstResponse := serveOperationHTTPTestRequest(
		t,
		handler,
		http.MethodPost,
		fixture.projectID,
		fixture.repositoryID,
		operationHTTPTestBuildRequest(strings.Repeat("c", 40)),
		configureKey,
	)

	if firstResponse.Code != http.StatusAccepted {
		t.Fatalf(
			"first HTTP status = %d, want %d; body: %s",
			firstResponse.Code,
			http.StatusAccepted,
			firstResponse.Body.String(),
		)
	}

	secondResponse := serveOperationHTTPTestRequest(
		t,
		handler,
		http.MethodPost,
		fixture.projectID,
		fixture.repositoryID,
		operationHTTPTestBuildRequest(strings.Repeat("d", 40)),
		configureKey,
	)

	assertOperationHTTPProblem(
		t,
		secondResponse,
		http.StatusConflict,
		"idempotency_conflict",
	)
}

func TestCreateRepositoryOperationHandlerMapsMissingStoreResources(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	authenticator, authorizer := newOperationHTTPTestDependencies(
		fixture.requestedBy,
	)
	handler := createRepositoryOperationHandler(
		fixture.pool,
		authenticator,
		authorizer,
	)

	t.Run("project and repository scope not found", func(t *testing.T) {
		response := serveOperationHTTPTestRequest(
			t,
			handler,
			http.MethodPost,
			operationHTTPTestProjectID,
			operationHTTPTestRepoID,
			operationHTTPTestBuildRequest(strings.Repeat("e", 40)),
			nil,
		)

		assertOperationHTTPProblem(
			t,
			response,
			http.StatusNotFound,
			"not_found",
		)
	})

	t.Run("cleanup deployment not found", func(t *testing.T) {
		body := []byte(
			`{"operationType":"cleanup",` +
				`"deploymentId":"123e4567-e89b-12d3-a456-426614174099"}`,
		)

		response := serveOperationHTTPTestRequest(
			t,
			handler,
			http.MethodPost,
			fixture.projectID,
			fixture.repositoryID,
			body,
			func(request *http.Request) {
				request.Header.Set(
					"Idempotency-Key",
					"missing-cleanup-deployment",
				)
			},
		)

		assertOperationHTTPProblem(
			t,
			response,
			http.StatusNotFound,
			"not_found",
		)
	})
}
