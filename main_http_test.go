package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const mainHTTPTestOperationPath = "/v1/projects/" +
	"123e4567-e89b-12d3-a456-426614174000/repositories/" +
	"123e4567-e89b-12d3-a456-426614174001/operations"

type mainHTTPTestAuthenticator struct{}

func (mainHTTPTestAuthenticator) Authenticate(
	_ context.Context,
	_ string,
) (operationHTTPPrincipal, error) {
	return operationHTTPPrincipal{Subject: "main-http-test"}, nil
}

type mainHTTPTestAuthorizer struct{}

func (mainHTTPTestAuthorizer) AuthorizeCreateOperation(
	_ context.Context,
	_ operationHTTPPrincipal,
	_ string,
	_ string,
) error {
	return nil
}

func serveMainHTTPTestRequest(
	handler http.Handler,
	method string,
	path string,
	configure func(*http.Request),
) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, nil)

	if configure != nil {
		configure(request)
	}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	return response
}

func TestNewHTTPHandlerHealthEndpoints(t *testing.T) {
	handler := newHTTPHandler(nil, nil, nil)

	tests := []struct {
		name       string
		path       string
		wantStatus int
		wantHealth string
	}{
		{
			name:       "liveness reports healthy",
			path:       "/health/live",
			wantStatus: http.StatusOK,
			wantHealth: "ok",
		},
		{
			name:       "readiness fails without database pool",
			path:       "/health/ready",
			wantStatus: http.StatusServiceUnavailable,
			wantHealth: "unavailable",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := serveMainHTTPTestRequest(
				handler,
				http.MethodGet,
				test.path,
				nil,
			)

			if response.Code != test.wantStatus {
				t.Fatalf(
					"HTTP status = %d, want %d; body: %s",
					response.Code,
					test.wantStatus,
					response.Body.String(),
				)
			}

			if got := response.Header().Get("Cache-Control"); got != "no-store" {
				t.Errorf("Cache-Control = %q, want no-store", got)
			}

			var body healthResponse
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode health response: %v", err)
			}

			if body.Status != test.wantHealth {
				t.Errorf(
					"health status = %q, want %q",
					body.Status,
					test.wantHealth,
				)
			}
		})
	}
}

func TestNewHTTPHandlerRegistersOperationRoute(t *testing.T) {
	handler := newHTTPHandler(
		&pgxpool.Pool{},
		mainHTTPTestAuthenticator{},
		mainHTTPTestAuthorizer{},
	)

	response := serveMainHTTPTestRequest(
		handler,
		http.MethodPost,
		mainHTTPTestOperationPath,
		nil,
	)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf(
			"HTTP status = %d, want %d; body: %s",
			response.Code,
			http.StatusUnauthorized,
			response.Body.String(),
		)
	}

	if got := response.Header().Get("WWW-Authenticate"); got == "" {
		t.Error("WWW-Authenticate header is missing")
	}

	if !strings.HasPrefix(
		response.Header().Get("Content-Type"),
		"application/problem+json",
	) {
		t.Errorf(
			"Content-Type = %q, want application/problem+json",
			response.Header().Get("Content-Type"),
		)
	}

	var problem operationProblemResponse
	if err := json.Unmarshal(response.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode operation problem response: %v", err)
	}

	if problem.Code != "unauthorized" {
		t.Errorf("problem code = %q, want unauthorized", problem.Code)
	}
}

func TestNewHTTPHandlerOperationRouteFailsClosedWhenUnconfigured(
	t *testing.T,
) {
	handler := newHTTPHandler(nil, nil, nil)

	response := serveMainHTTPTestRequest(
		handler,
		http.MethodPost,
		mainHTTPTestOperationPath,
		nil,
	)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf(
			"HTTP status = %d, want %d; body: %s",
			response.Code,
			http.StatusServiceUnavailable,
			response.Body.String(),
		)
	}

	var problem operationProblemResponse
	if err := json.Unmarshal(response.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode operation problem response: %v", err)
	}

	if problem.Code != "operation_creation_not_configured" {
		t.Errorf(
			"problem code = %q, want operation_creation_not_configured",
			problem.Code,
		)
	}
}

func TestNewHTTPHandlerRejectsUnsupportedOperationMethod(t *testing.T) {
	handler := newHTTPHandler(nil, nil, nil)

	response := serveMainHTTPTestRequest(
		handler,
		http.MethodGet,
		mainHTTPTestOperationPath,
		nil,
	)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf(
			"HTTP status = %d, want %d; body: %s",
			response.Code,
			http.StatusMethodNotAllowed,
			response.Body.String(),
		)
	}

	if got := response.Header().Get("Allow"); got != http.MethodPost {
		t.Errorf("Allow header = %q, want POST", got)
	}
}

func TestNewHTTPHandlerReturnsNotFoundForUnknownRoute(t *testing.T) {
	handler := newHTTPHandler(nil, nil, nil)

	response := serveMainHTTPTestRequest(
		handler,
		http.MethodGet,
		"/this-route-does-not-exist",
		nil,
	)

	if response.Code != http.StatusNotFound {
		t.Errorf(
			"HTTP status = %d, want %d",
			response.Code,
			http.StatusNotFound,
		)
	}
}
