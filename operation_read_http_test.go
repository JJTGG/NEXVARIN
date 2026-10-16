package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func serveOperationReadHTTPTestRequest(
	t *testing.T,
	handler http.Handler,
	operationID string,
	token string,
) *httptest.ResponseRecorder {
	t.Helper()

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/operations/"+operationID,
		nil,
	)
	request.SetPathValue("operationId", operationID)

	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	return response
}

func newOperationReadHTTPTestHandler(
	fixture *operationStoreTestFixture,
	subject string,
) http.Handler {
	authenticator := &operationHTTPTestAuthenticator{
		principal: operationHTTPPrincipal{
			Subject: subject,
		},
	}

	return getOperationHandler(
		fixture.pool,
		authenticator,
		newPostgresOperationReadAuthorizer(fixture.pool),
	)
}

func TestGetOperationHandlerReturnsOperationToOwner(t *testing.T) {
	fixture := newOperationStoreTestFixture(t)

	revision := strings.Repeat("a", 40)
	created, err := fixture.create(
		"read-operation-owner",
		buildOperationRequestForTest(revision),
	)
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}

	handler := newOperationReadHTTPTestHandler(
		fixture,
		fixture.requestedBy,
	)

	response := serveOperationReadHTTPTestRequest(
		t,
		handler,
		created.Operation.ID,
		operationHTTPTestToken,
	)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"HTTP status = %d, want %d; body: %s",
			response.Code,
			http.StatusOK,
			response.Body.String(),
		)
	}

	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", got)
	}

	var record operationRecord
	if err := json.Unmarshal(response.Body.Bytes(), &record); err != nil {
		t.Fatalf("decode operation response: %v", err)
	}

	if record.ID != created.Operation.ID {
		t.Errorf(
			"operation ID = %q, want %q",
			record.ID,
			created.Operation.ID,
		)
	}

	if record.ProjectID != fixture.projectID {
		t.Errorf(
			"project ID = %q, want %q",
			record.ProjectID,
			fixture.projectID,
		)
	}

	if record.RepositoryID != fixture.repositoryID {
		t.Errorf(
			"repository ID = %q, want %q",
			record.RepositoryID,
			fixture.repositoryID,
		)
	}

	if record.Status != operationStatusQueued {
		t.Errorf(
			"operation status = %q, want %q",
			record.Status,
			operationStatusQueued,
		)
	}

	if record.SourceRevision == nil || *record.SourceRevision != revision {
		t.Errorf(
			"source revision = %v, want %q",
			record.SourceRevision,
			revision,
		)
	}
}

func TestGetOperationHandlerHidesOperationFromOtherOwner(t *testing.T) {
	fixture := newOperationStoreTestFixture(t)

	created, err := fixture.create(
		"read-operation-other-owner",
		buildOperationRequestForTest(strings.Repeat("b", 40)),
	)
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}

	handler := newOperationReadHTTPTestHandler(
		fixture,
		"another-project-owner",
	)

	response := serveOperationReadHTTPTestRequest(
		t,
		handler,
		created.Operation.ID,
		operationHTTPTestToken,
	)

	assertOperationHTTPProblem(
		t,
		response,
		http.StatusNotFound,
		"not_found",
	)
}

func TestGetOperationHandlerReturnsNotFoundForMissingOperation(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	const missingOperationID = "123e4567-e89b-12d3-a456-426614174099"

	handler := newOperationReadHTTPTestHandler(
		fixture,
		fixture.requestedBy,
	)

	response := serveOperationReadHTTPTestRequest(
		t,
		handler,
		missingOperationID,
		operationHTTPTestToken,
	)

	assertOperationHTTPProblem(
		t,
		response,
		http.StatusNotFound,
		"not_found",
	)
}
