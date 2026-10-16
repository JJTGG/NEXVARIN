package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func serveOperationListHTTPTestRequest(
	t *testing.T,
	handler http.Handler,
	projectID string,
	repositoryID string,
	query string,
	token string,
) *httptest.ResponseRecorder {
	t.Helper()

	path := "/v1/projects/" + projectID +
		"/repositories/" + repositoryID +
		"/operations" + query

	request := httptest.NewRequest(http.MethodGet, path, nil)
	request.SetPathValue("projectId", projectID)
	request.SetPathValue("repositoryId", repositoryID)

	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	return response
}

func newOperationListHTTPTestHandler(
	fixture *operationStoreTestFixture,
	subject string,
) http.Handler {
	authenticator := &operationHTTPTestAuthenticator{
		principal: operationHTTPPrincipal{
			Subject: subject,
		},
	}

	return listRepositoryOperationsHandler(
		fixture.pool,
		authenticator,
		newPostgresOperationListAuthorizer(fixture.pool),
	)
}

func TestListRepositoryOperationsHandlerPaginatesForOwner(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	expectedIDs := make(map[string]bool)

	for index, revision := range []string{"a", "b", "c"} {
		created, err := fixture.create(
			"list-http-"+revision,
			buildOperationRequestForTest(strings.Repeat(revision, 40)),
		)
		if err != nil {
			t.Fatalf("create operation %d: %v", index, err)
		}

		expectedIDs[created.Operation.ID] = true
	}

	handler := newOperationListHTTPTestHandler(
		fixture,
		fixture.requestedBy,
	)

	firstResponse := serveOperationListHTTPTestRequest(
		t,
		handler,
		fixture.projectID,
		fixture.repositoryID,
		"?limit=2",
		operationHTTPTestToken,
	)

	if firstResponse.Code != http.StatusOK {
		t.Fatalf(
			"first page HTTP status = %d, want %d; body: %s",
			firstResponse.Code,
			http.StatusOK,
			firstResponse.Body.String(),
		)
	}

	if got := firstResponse.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", got)
	}

	var firstPage operationListResult
	if err := json.Unmarshal(firstResponse.Body.Bytes(), &firstPage); err != nil {
		t.Fatalf("decode first page: %v", err)
	}

	if len(firstPage.Items) != 2 {
		t.Fatalf("first page items = %d, want 2", len(firstPage.Items))
	}

	if firstPage.NextCursor == nil || *firstPage.NextCursor == "" {
		t.Fatal("first page should contain a next cursor")
	}

	secondResponse := serveOperationListHTTPTestRequest(
		t,
		handler,
		fixture.projectID,
		fixture.repositoryID,
		"?limit=2&cursor="+url.QueryEscape(*firstPage.NextCursor),
		operationHTTPTestToken,
	)

	if secondResponse.Code != http.StatusOK {
		t.Fatalf(
			"second page HTTP status = %d, want %d; body: %s",
			secondResponse.Code,
			http.StatusOK,
			secondResponse.Body.String(),
		)
	}

	var secondPage operationListResult
	if err := json.Unmarshal(secondResponse.Body.Bytes(), &secondPage); err != nil {
		t.Fatalf("decode second page: %v", err)
	}

	if len(secondPage.Items) != 1 {
		t.Fatalf("second page items = %d, want 1", len(secondPage.Items))
	}

	if secondPage.NextCursor != nil {
		t.Error("last page should not contain a next cursor")
	}

	seen := make(map[string]bool)

	for _, item := range append(firstPage.Items, secondPage.Items...) {
		if !expectedIDs[item.ID] {
			t.Errorf("unexpected operation ID %q", item.ID)
		}

		if seen[item.ID] {
			t.Errorf("operation %q appeared on multiple pages", item.ID)
		}

		seen[item.ID] = true
	}

	if len(seen) != len(expectedIDs) {
		t.Errorf(
			"unique operation count = %d, want %d",
			len(seen),
			len(expectedIDs),
		)
	}
}

func TestListRepositoryOperationsHandlerHidesOtherOwnersRepository(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	_, err := fixture.create(
		"list-http-private",
		buildOperationRequestForTest(strings.Repeat("d", 40)),
	)
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}

	handler := newOperationListHTTPTestHandler(
		fixture,
		"another-project-owner",
	)

	response := serveOperationListHTTPTestRequest(
		t,
		handler,
		fixture.projectID,
		fixture.repositoryID,
		"",
		operationHTTPTestToken,
	)

	assertOperationHTTPProblem(
		t,
		response,
		http.StatusNotFound,
		"not_found",
	)
}

func TestListRepositoryOperationsHandlerRequiresAuthentication(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	handler := newOperationListHTTPTestHandler(
		fixture,
		fixture.requestedBy,
	)

	response := serveOperationListHTTPTestRequest(
		t,
		handler,
		fixture.projectID,
		fixture.repositoryID,
		"",
		"",
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
}

func TestListRepositoryOperationsHandlerValidatesQueryParameters(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	handler := newOperationListHTTPTestHandler(
		fixture,
		fixture.requestedBy,
	)

	tests := []struct {
		name      string
		query     string
		wantCode  string
	}{
		{
			name:     "zero limit",
			query:    "?limit=0",
			wantCode: "invalid_limit",
		},
		{
			name:     "limit above maximum",
			query:    "?limit=101",
			wantCode: "invalid_limit",
		},
		{
			name:     "duplicate limits",
			query:    "?limit=1&limit=2",
			wantCode: "invalid_limit",
		},
		{
			name:     "invalid cursor",
			query:    "?cursor=not-a-cursor",
			wantCode: "invalid_cursor",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := serveOperationListHTTPTestRequest(
				t,
				handler,
				fixture.projectID,
				fixture.repositoryID,
				test.query,
				operationHTTPTestToken,
			)

			assertOperationHTTPProblem(
				t,
				response,
				http.StatusUnprocessableEntity,
				test.wantCode,
			)
		})
	}
}

func TestListRepositoryOperationsHandlerFailsClosedWhenUnconfigured(
	t *testing.T,
) {
	handler := listRepositoryOperationsHandler(nil, nil, nil)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/projects/123e4567-e89b-12d3-a456-426614174000/repositories/123e4567-e89b-12d3-a456-426614174001/operations",
		nil,
	)

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	assertOperationHTTPProblem(
		t,
		response,
		http.StatusServiceUnavailable,
		"operation_list_not_configured",
	)
}
