package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"
)

func assertCaddyRouteVerificationReadOnly(
	t *testing.T,
	recorder *caddyRouteMutationTestRecorder,
) {
	t.Helper()

	requests := recorder.snapshot()
	if len(requests) != 1 {
		t.Fatalf(
			"request count = %d, want exactly one read-back request",
			len(requests),
		)
	}

	request := requests[0]
	if request.Method != http.MethodGet {
		t.Errorf(
			"request method = %q, want GET",
			request.Method,
		)
	}

	if request.Path != "/config/apps/http/servers/nexvarin/routes" {
		t.Errorf(
			"request path = %q, want the configured server routes path",
			request.Path,
		)
	}

	if request.ETag != "" {
		t.Errorf(
			"read-back If-Match = %q, want no mutation precondition",
			request.ETag,
		)
	}

	if len(request.Body) != 0 {
		t.Errorf(
			"read-back request body = %q, want empty",
			request.Body,
		)
	}
}

func newCaddyRouteVerificationTestMutator(
	client *caddyAdminClient,
) caddyRouteMutator {
	return caddyRouteMutator{
		Client:        client,
		ServerName:    "nexvarin",
		PreviewSuffix: caddyRouteConfigTestPreviewSuffix,
	}
}

func TestCaddyRouteVerifierAcceptsExactActiveRoute(
	t *testing.T,
) {
	expected := caddyRouteMutationTestExpected()

	managed := caddyRouteConfigTestManagedRoute(
		t,
		expected.ID,
		expected.Hostname,
		expected.UpstreamAddress,
	)

	snapshot := caddyRouteMutationTestSnapshot(
		t,
		[]json.RawMessage{managed},
	)

	client, recorder := newCaddyRouteMutationTestFixture(
		t,
		snapshot,
		http.StatusOK,
	)

	mutator := newCaddyRouteVerificationTestMutator(client)

	if err := mutator.VerifyRouteActive(
		context.Background(),
		expected,
	); err != nil {
		t.Fatalf("verify exact active route: %v", err)
	}

	assertCaddyRouteVerificationReadOnly(t, recorder)
}

func TestCaddyRouteVerifierRejectsActiveRouteMismatch(
	t *testing.T,
) {
	expected := caddyRouteMutationTestExpected()

	tests := []struct {
		name       string
		children   []json.RawMessage
		wantError  error
	}{
		{
			name:      "route absent",
			children:  []json.RawMessage{},
			wantError: errCaddyRouteVerificationMismatch,
		},
		{
			name: "upstream drift",
			children: []json.RawMessage{
				caddyRouteConfigTestManagedRoute(
					t,
					expected.ID,
					expected.Hostname,
					"127.0.0.1:31009",
				),
			},
			wantError: errCaddyRouteVerificationMismatch,
		},
		{
			name: "stable ID belongs to another hostname",
			children: []json.RawMessage{
				caddyRouteConfigTestManagedRoute(
					t,
					expected.ID,
					"other.preview.example.test",
					expected.UpstreamAddress,
				),
			},
			wantError: errCaddyRouteIdentityConflict,
		},
		{
			name: "foreign route conflicts with expected hostname",
			children: []json.RawMessage{
				caddyRouteConfigTestManagedRoute(
					t,
					expected.ID,
					expected.Hostname,
					expected.UpstreamAddress,
				),
				caddyRouteConfigTestOperatorRoute(
					t,
					"operator-conflict",
					expected.Hostname,
				),
			},
			wantError: errCaddyRouteHostnameConflict,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := caddyRouteMutationTestSnapshot(
				t,
				test.children,
			)

			client, recorder := newCaddyRouteMutationTestFixture(
				t,
				snapshot,
				http.StatusOK,
			)

			mutator := newCaddyRouteVerificationTestMutator(client)

			err := mutator.VerifyRouteActive(
				context.Background(),
				expected,
			)
			if !errors.Is(err, test.wantError) {
				t.Fatalf(
					"verification error = %v, want %v",
					err,
					test.wantError,
				)
			}

			assertCaddyRouteVerificationReadOnly(t, recorder)
		})
	}
}

func TestCaddyRouteVerifierAcceptsConfirmedRemoval(
	t *testing.T,
) {
	expected := caddyRouteMutationTestExpected()

	snapshot := caddyRouteMutationTestSnapshot(
		t,
		[]json.RawMessage{},
	)

	client, recorder := newCaddyRouteMutationTestFixture(
		t,
		snapshot,
		http.StatusOK,
	)

	mutator := newCaddyRouteVerificationTestMutator(client)

	if err := mutator.VerifyRouteRemoved(
		context.Background(),
		expected.ID,
		expected.Hostname,
	); err != nil {
		t.Fatalf("verify removed route: %v", err)
	}

	assertCaddyRouteVerificationReadOnly(t, recorder)
}

func TestCaddyRouteVerifierRejectsUnconfirmedRemoval(
	t *testing.T,
) {
	expected := caddyRouteMutationTestExpected()

	tests := []struct {
		name      string
		children  []json.RawMessage
		wantError error
	}{
		{
			name: "managed route ID still present",
			children: []json.RawMessage{
				caddyRouteConfigTestManagedRoute(
					t,
					expected.ID,
					expected.Hostname,
					expected.UpstreamAddress,
				),
			},
			wantError: errCaddyRouteVerificationMismatch,
		},
		{
			name: "foreign route still claims hostname",
			children: []json.RawMessage{
				caddyRouteConfigTestOperatorRoute(
					t,
					"operator-conflict",
					expected.Hostname,
				),
			},
			wantError: errCaddyRouteHostnameConflict,
		},
		{
			name: "another managed ID claims hostname",
			children: []json.RawMessage{
				caddyRouteConfigTestManagedRoute(
					t,
					caddyRouteIDPrefix+"00000000-0000-4000-8000-000000000002",
					expected.Hostname,
					expected.UpstreamAddress,
				),
			},
			wantError: errCaddyRouteHostnameConflict,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := caddyRouteMutationTestSnapshot(
				t,
				test.children,
			)

			client, recorder := newCaddyRouteMutationTestFixture(
				t,
				snapshot,
				http.StatusOK,
			)

			mutator := newCaddyRouteVerificationTestMutator(client)

			err := mutator.VerifyRouteRemoved(
				context.Background(),
				expected.ID,
				expected.Hostname,
			)
			if !errors.Is(err, test.wantError) {
				t.Fatalf(
					"verification error = %v, want %v",
					err,
					test.wantError,
				)
			}

			assertCaddyRouteVerificationReadOnly(t, recorder)
		})
	}
}

func TestCaddyRouteVerifierRejectsInvalidExpectedRouteBeforeNetwork(
	t *testing.T,
) {
	expected := caddyRouteMutationTestExpected()

	snapshot := caddyRouteMutationTestSnapshot(
		t,
		[]json.RawMessage{},
	)

	client, recorder := newCaddyRouteMutationTestFixture(
		t,
		snapshot,
		http.StatusOK,
	)

	mutator := newCaddyRouteVerificationTestMutator(client)

	invalid := expected
	invalid.UpstreamAddress = "0.0.0.0:31001"

	err := mutator.VerifyRouteActive(
		context.Background(),
		invalid,
	)
	if !errors.Is(err, errCaddyRouteConfigInvalid) {
		t.Fatalf(
			"active verification error = %v, want invalid route configuration",
			err,
		)
	}

	err = mutator.VerifyRouteRemoved(
		context.Background(),
		"operator-route",
		expected.Hostname,
	)
	if !errors.Is(err, errCaddyRouteConfigInvalid) {
		t.Fatalf(
			"removal verification error = %v, want invalid route configuration",
			err,
		)
	}

	if requests := recorder.snapshot(); len(requests) != 0 {
		t.Fatalf(
			"requests = %#v, want no network requests for invalid inputs",
			requests,
		)
	}
}

func TestCaddyRouteVerifierClassifiesUnavailableReadBackAsUnknown(
	t *testing.T,
) {
	tests := []struct {
		name string
		run  func(caddyRouteMutator, caddyRouteConfig) error
	}{
		{
			name: "active verification",
			run: func(
				mutator caddyRouteMutator,
				expected caddyRouteConfig,
			) error {
				return mutator.VerifyRouteActive(
					context.Background(),
					expected,
				)
			},
		},
		{
			name: "removal verification",
			run: func(
				mutator caddyRouteMutator,
				expected caddyRouteConfig,
			) error {
				return mutator.VerifyRouteRemoved(
					context.Background(),
					expected.ID,
					expected.Hostname,
				)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := &caddyRouteMutationTestRecorder{}

			client := newCaddyAdminTestClient(
				t,
				http.HandlerFunc(func(
					writer http.ResponseWriter,
					request *http.Request,
				) {
					recorder.record(caddyRouteMutationTestRequest{
						Method: request.Method,
						Path:   request.URL.Path,
						ETag:   request.Header.Get("If-Match"),
					})

					writer.WriteHeader(http.StatusServiceUnavailable)
				}),
				caddyAdminClientConfig{},
			)

			mutator := newCaddyRouteVerificationTestMutator(client)

			err := test.run(
				mutator,
				caddyRouteMutationTestExpected(),
			)
			if !errors.Is(err, errCaddyAdminOutcomeUnknown) {
				t.Fatalf(
					"verification error = %v, want unknown outcome",
					err,
				)
			}

			assertCaddyRouteVerificationReadOnly(t, recorder)
		})
	}
}

func TestCaddyRouteVerifierAgainstPinnedCaddyLifecycle(
	t *testing.T,
) {
	integration := startCaddyAdminIntegration(t)

	mutator := caddyRouteMutator{
		Client:        integration.client,
		ServerName:    "nexvarin",
		PreviewSuffix: caddyRouteConfigTestPreviewSuffix,
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	expected := caddyRouteConfig{
		ID:              caddyAdminIntegrationRouteID,
		Hostname:        caddyAdminIntegrationHostname,
		UpstreamAddress: "127.0.0.1:31001",
	}

	if err := mutator.VerifyRouteRemoved(
		ctx,
		expected.ID,
		expected.Hostname,
	); err != nil {
		t.Fatalf(
			"verify initially absent route: %v",
			err,
		)
	}

	if err := mutator.VerifyRouteActive(
		ctx,
		expected,
	); !errors.Is(err, errCaddyRouteVerificationMismatch) {
		t.Fatalf(
			"verify initially absent active route error = %v, want verification mismatch",
			err,
		)
	}

	added, err := mutator.EnsureRoute(ctx, expected)
	if err != nil {
		t.Fatalf("install route through mutator: %v", err)
	}
	if added.Kind != caddyRouteMutationAdded || !added.Changed {
		t.Fatalf("route installation result = %#v", added)
	}

	if err := mutator.VerifyRouteActive(
		ctx,
		expected,
	); err != nil {
		t.Fatalf(
			"verify active route after installation: %v",
			err,
		)
	}

	drifted := expected
	drifted.UpstreamAddress = "127.0.0.1:31002"

	if err := mutator.VerifyRouteActive(
		ctx,
		drifted,
	); !errors.Is(err, errCaddyRouteVerificationMismatch) {
		t.Fatalf(
			"verify route with drifted upstream error = %v, want verification mismatch",
			err,
		)
	}

	if err := mutator.VerifyRouteRemoved(
		ctx,
		expected.ID,
		expected.Hostname,
	); !errors.Is(err, errCaddyRouteVerificationMismatch) {
		t.Fatalf(
			"verify removal before deletion error = %v, want verification mismatch",
			err,
		)
	}

	removed, err := mutator.RemoveRoute(
		ctx,
		expected.ID,
		expected.Hostname,
	)
	if err != nil {
		t.Fatalf("remove route through mutator: %v", err)
	}
	if removed.Kind != caddyRouteMutationRemoved || !removed.Changed {
		t.Fatalf("route removal result = %#v", removed)
	}

	if err := mutator.VerifyRouteRemoved(
		ctx,
		expected.ID,
		expected.Hostname,
	); err != nil {
		t.Fatalf(
			"verify route removal after deletion: %v",
			err,
		)
	}

	finalRoutes, _ := integration.readServerRoutes(t)
	if len(finalRoutes) != 2 {
		t.Fatalf(
			"final server route count = %d, want 2",
			len(finalRoutes),
		)
	}

	if finalRoutes[1]["@id"] != caddyAdminIntegrationOperatorRouteID {
		t.Errorf(
			"operator route after verification lifecycle = %v, want %q",
			finalRoutes[1]["@id"],
			caddyAdminIntegrationOperatorRouteID,
		)
	}

	finalNamespace := caddyAdminIntegrationNamespaceRoutes(
		t,
		finalRoutes,
	)
	if len(finalNamespace) != 0 {
		t.Errorf(
			"final NEXVARIN route count = %d, want 0",
			len(finalNamespace),
		)
	}
}
