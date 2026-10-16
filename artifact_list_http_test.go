package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"
)

func serveArtifactListHTTPTestRequest(
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
		"/artifacts" + query

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

func newArtifactListHTTPTestHandler(
	fixture *operationStoreTestFixture,
	subject string,
) http.Handler {
	authenticator := &operationHTTPTestAuthenticator{
		principal: operationHTTPPrincipal{
			Subject: subject,
		},
	}

	return listRepositoryArtifactsHandler(
		fixture.pool,
		authenticator,
		newPostgresArtifactListAuthorizer(fixture.pool),
	)
}

func TestListRepositoryArtifactsHandlerPaginatesForOwner(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)
	buildOperationID := createArtifactListTestBuild(t, fixture)

	fixedTimestamp := time.Date(
		2026, time.October, 16,
		0, 0, 0, 0,
		time.UTC,
	)

	var expectedIDs []string

	for _, digestCharacter := range []string{"a", "b", "c"} {
		imageReference := "localhost/nexvarin/build-" +
			strings.Repeat(digestCharacter, 8) + ":latest"

		artifactID := insertArtifactListTestArtifact(
			t,
			fixture,
			buildOperationID,
			digestCharacter,
			"verified",
			imageReference,
			fixedTimestamp,
		)

		expectedIDs = append(expectedIDs, artifactID)
	}

	// Pending artifacts must not appear in the HTTP response.
	insertArtifactListTestArtifact(
		t,
		fixture,
		buildOperationID,
		"d",
		"pending",
		"",
		fixedTimestamp,
	)

	sort.Slice(expectedIDs, func(i, j int) bool {
		return expectedIDs[i] > expectedIDs[j]
	})

	handler := newArtifactListHTTPTestHandler(
		fixture,
		fixture.requestedBy,
	)

	firstResponse := serveArtifactListHTTPTestRequest(
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

	var firstPage artifactListResult
	if err := json.Unmarshal(
		firstResponse.Body.Bytes(),
		&firstPage,
	); err != nil {
		t.Fatalf("decode first artifact page: %v", err)
	}

	if len(firstPage.Items) != 2 {
		t.Fatalf(
			"first page items = %d, want 2",
			len(firstPage.Items),
		)
	}

	if firstPage.NextCursor == nil || *firstPage.NextCursor == "" {
		t.Fatal("first page should contain a next cursor")
	}

	for i, item := range firstPage.Items {
		if item.ID != expectedIDs[i] {
			t.Errorf(
				"first page item %d ID = %q, want %q",
				i,
				item.ID,
				expectedIDs[i],
			)
		}

		if item.VerificationStatus != "verified" {
			t.Errorf(
				"artifact %q status = %q, want verified",
				item.ID,
				item.VerificationStatus,
			)
		}

		if item.ImageReference == "" {
			t.Errorf(
				"artifact %q has no image reference",
				item.ID,
			)
		}
	}

	secondResponse := serveArtifactListHTTPTestRequest(
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

	var secondPage artifactListResult
	if err := json.Unmarshal(
		secondResponse.Body.Bytes(),
		&secondPage,
	); err != nil {
		t.Fatalf("decode second artifact page: %v", err)
	}

	if len(secondPage.Items) != 1 {
		t.Fatalf(
			"second page items = %d, want 1",
			len(secondPage.Items),
		)
	}

	if secondPage.Items[0].ID != expectedIDs[2] {
		t.Errorf(
			"second page artifact ID = %q, want %q",
			secondPage.Items[0].ID,
			expectedIDs[2],
		)
	}

	if secondPage.NextCursor != nil {
		t.Error("last page should not contain a next cursor")
	}

	seen := make(map[string]bool)

	for _, item := range append(
		firstPage.Items,
		secondPage.Items...,
	) {
		if seen[item.ID] {
			t.Errorf(
				"artifact %q appeared on multiple pages",
				item.ID,
			)
		}

		seen[item.ID] = true
	}

	if len(seen) != len(expectedIDs) {
		t.Errorf(
			"unique artifact count = %d, want %d",
			len(seen),
			len(expectedIDs),
		)
	}
}

func TestListRepositoryArtifactsHandlerHidesOtherOwnersRepository(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	buildOperationID := createArtifactListTestBuild(t, fixture)

	insertArtifactListTestArtifact(
		t,
		fixture,
		buildOperationID,
		"a",
		"verified",
		"localhost/nexvarin/test:latest",
		time.Now().UTC(),
	)

	handler := newArtifactListHTTPTestHandler(
		fixture,
		"another-project-owner",
	)

	response := serveArtifactListHTTPTestRequest(
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

func TestListRepositoryArtifactsHandlerRequiresAuthentication(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	handler := newArtifactListHTTPTestHandler(
		fixture,
		fixture.requestedBy,
	)

	response := serveArtifactListHTTPTestRequest(
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

func TestListRepositoryArtifactsHandlerValidatesQueryParameters(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	handler := newArtifactListHTTPTestHandler(
		fixture,
		fixture.requestedBy,
	)

	tests := []struct {
		name     string
		query    string
		wantCode string
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
			response := serveArtifactListHTTPTestRequest(
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

func TestListRepositoryArtifactsHandlerFailsClosedWhenUnconfigured(
	t *testing.T,
) {
	handler := listRepositoryArtifactsHandler(nil, nil, nil)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/projects/123e4567-e89b-12d3-a456-426614174000/repositories/123e4567-e89b-12d3-a456-426614174001/artifacts",
		nil,
	)

	request.SetPathValue(
		"projectId",
		"123e4567-e89b-12d3-a456-426614174000",
	)
	request.SetPathValue(
		"repositoryId",
		"123e4567-e89b-12d3-a456-426614174001",
	)

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	assertOperationHTTPProblem(
		t,
		response,
		http.StatusServiceUnavailable,
		"artifact_list_not_configured",
	)
}

func TestListRepositoryArtifactsHandlerMapsAuthenticationFailure(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	authenticator := &operationHTTPTestAuthenticator{
		err: errors.New("authentication backend unavailable"),
	}

	handler := listRepositoryArtifactsHandler(
		fixture.pool,
		authenticator,
		newPostgresArtifactListAuthorizer(fixture.pool),
	)

	response := serveArtifactListHTTPTestRequest(
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
		http.StatusInternalServerError,
		"authentication_unavailable",
	)
}
