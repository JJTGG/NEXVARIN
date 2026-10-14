package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWriteHealth(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		status     string
	}{
		{
			name:       "healthy response",
			statusCode: http.StatusOK,
			status:     "ok",
		},
		{
			name:       "unavailable response",
			statusCode: http.StatusServiceUnavailable,
			status:     "unavailable",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()

			writeHealth(
				recorder,
				test.statusCode,
				test.status,
			)

			if recorder.Code != test.statusCode {
				t.Errorf(
					"status code = %d, want %d",
					recorder.Code,
					test.statusCode,
				)
			}

			contentType := recorder.Header().Get("Content-Type")
			if contentType != "application/json; charset=utf-8" {
				t.Errorf(
					"Content-Type = %q, want JSON",
					contentType,
				)
			}

			if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
				t.Errorf(
					"Cache-Control = %q, want %q",
					got,
					"no-store",
				)
			}

			var response healthResponse
			if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
				t.Fatalf("decode health response: %v", err)
			}

			if response.Status != test.status {
				t.Errorf(
					"response status = %q, want %q",
					response.Status,
					test.status,
				)
			}
		})
	}
}

func TestReadinessUnavailableWithoutDatabase(t *testing.T) {
	handler := readinessHandler(nil)
	request := httptest.NewRequest(
		http.MethodGet,
		"/health/ready",
		nil,
	)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf(
			"status code = %d, want %d",
			recorder.Code,
			http.StatusServiceUnavailable,
		)
	}

	var response healthResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode readiness response: %v", err)
	}

	if response.Status != "unavailable" {
		t.Errorf(
			"response status = %q, want %q",
			response.Status,
			"unavailable",
		)
	}
}
