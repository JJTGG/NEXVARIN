package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const caddyRouteMutationTestETag = `"mutation-snapshot-v1"`

type caddyRouteMutationTestRequest struct {
	Method string
	Path   string
	ETag   string
	Body   []byte
}

type caddyRouteMutationTestRecorder struct {
	mu       sync.Mutex
	requests []caddyRouteMutationTestRequest
}

func (recorder *caddyRouteMutationTestRecorder) record(
	request caddyRouteMutationTestRequest,
) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()

	request.Body = append([]byte(nil), request.Body...)
	recorder.requests = append(recorder.requests, request)
}

func (recorder *caddyRouteMutationTestRecorder) snapshot() []caddyRouteMutationTestRequest {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()

	requests := make([]caddyRouteMutationTestRequest, len(recorder.requests))
	copy(requests, recorder.requests)

	for index := range requests {
		requests[index].Body = append(
			[]byte(nil),
			requests[index].Body...,
		)
	}

	return requests
}

func caddyRouteMutationTestExpected() caddyRouteConfig {
	return caddyRouteConfig{
		ID:              caddyRouteConfigTestRouteID,
		Hostname:        caddyRouteConfigTestHostname,
		UpstreamAddress: caddyRouteConfigTestUpstream,
	}
}

func caddyRouteMutationTestSnapshot(
	t *testing.T,
	children []json.RawMessage,
) []byte {
	t.Helper()

	operator := caddyRouteConfigTestOperatorRoute(
		t,
		"operator-before",
		"operator.example.test",
	)
	namespace := caddyRouteConfigTestNamespaceRoute(
		t,
		caddyRouteConfigTestPreviewSuffix,
		children,
	)

	body, err := json.Marshal([]json.RawMessage{
		operator,
		namespace,
	})
	if err != nil {
		t.Fatalf("marshal Caddy mutation snapshot: %v", err)
	}

	return body
}

func caddyRouteMutationTestNestedSnapshot(
	t *testing.T,
	children []json.RawMessage,
) []byte {
	t.Helper()

	operatorBefore := caddyRouteConfigTestOperatorRoute(
		t,
		"operator-before",
		"operator-before.example.test",
	)
	operatorMiddle := caddyRouteConfigTestOperatorRoute(
		t,
		"operator-middle",
		"operator-middle.example.test",
	)

	parent := caddyRouteConfigTestJSON(t, map[string]any{
		"match": []any{
			map[string]any{
				"host": []string{
					"*." + caddyRouteConfigTestPreviewSuffix,
				},
			},
		},
		"handle": []any{
			map[string]any{
				"handler": "static_response",
				"body":    "operator handler",
			},
			map[string]any{
				"handler": "subroute",
				"@id":     caddyRouteNamespaceID,
				"routes":  children,
			},
		},
	})

	body, err := json.Marshal([]json.RawMessage{
		operatorBefore,
		operatorMiddle,
		parent,
	})
	if err != nil {
		t.Fatalf("marshal nested Caddy mutation snapshot: %v", err)
	}

	return body
}

func newCaddyRouteMutationTestFixture(
	t *testing.T,
	snapshot []byte,
	mutationStatus int,
) (*caddyAdminClient, *caddyRouteMutationTestRecorder) {
	t.Helper()

	recorder := &caddyRouteMutationTestRecorder{}

	client := newCaddyAdminTestClient(
		t,
		http.HandlerFunc(func(
			writer http.ResponseWriter,
			request *http.Request,
		) {
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Errorf("read Caddy mutation request body: %v", err)
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}

			recorder.record(caddyRouteMutationTestRequest{
				Method: request.Method,
				Path:   request.URL.Path,
				ETag:   request.Header.Get("If-Match"),
				Body:   body,
			})

			if request.Method == http.MethodGet {
				writer.Header().Set("ETag", caddyRouteMutationTestETag)
				writer.Header().Set(
					"Content-Type",
					"application/json",
				)
				writer.WriteHeader(http.StatusOK)
				_, _ = writer.Write(snapshot)
				return
			}

			writer.WriteHeader(mutationStatus)
		}),
		caddyAdminClientConfig{},
	)

	return client, recorder
}

func newCaddyRouteMutationTestMutator(
	client *caddyAdminClient,
) caddyRouteMutator {
	return caddyRouteMutator{
		Client:        client,
		ServerName:    "nexvarin",
		PreviewSuffix: caddyRouteConfigTestPreviewSuffix,
	}
}

func TestCaddyRouteMutatorEnsureRouteAppendsWithSnapshotETag(
	t *testing.T,
) {
	snapshot := caddyRouteMutationTestSnapshot(
		t,
		[]json.RawMessage{},
	)
	client, recorder := newCaddyRouteMutationTestFixture(
		t,
		snapshot,
		http.StatusOK,
	)

	mutator := newCaddyRouteMutationTestMutator(client)
	expected := caddyRouteMutationTestExpected()

	result, err := mutator.EnsureRoute(
		context.Background(),
		expected,
	)
	if err != nil {
		t.Fatalf("ensure route: %v", err)
	}

	if result.Kind != caddyRouteMutationAdded ||
		!result.Changed ||
		result.StatusCode != http.StatusOK {
		t.Fatalf("mutation result = %#v, want an added route", result)
	}

	wantPath := "/config/apps/http/servers/nexvarin/routes/1/handle/0/routes"
	if result.Path != wantPath {
		t.Fatalf("mutation path = %q, want %q", result.Path, wantPath)
	}

	requests := recorder.snapshot()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want GET and POST", len(requests))
	}

	if requests[0].Method != http.MethodGet ||
		requests[0].Path != "/config/apps/http/servers/nexvarin/routes" ||
		requests[0].ETag != "" {
		t.Fatalf("snapshot request = %#v", requests[0])
	}

	mutation := requests[1]
	if mutation.Method != http.MethodPost ||
		mutation.Path != wantPath ||
		mutation.ETag != caddyRouteMutationTestETag {
		t.Fatalf("append request = %#v", mutation)
	}

	got, err := decodeCaddyManagedRoute(mutation.Body)
	if err != nil {
		t.Fatalf("decode appended route: %v", err)
	}

	if got != expected {
		t.Fatalf("appended route = %#v, want %#v", got, expected)
	}
}

func TestCaddyRouteMutatorEnsureRouteReplacesAtSnapshotIndex(
	t *testing.T,
) {
	expected := caddyRouteMutationTestExpected()
	oldRoute := caddyRouteConfigTestManagedRoute(
		t,
		expected.ID,
		expected.Hostname,
		"127.0.0.1:31009",
	)
	foreignSibling := caddyRouteConfigTestOperatorRoute(
		t,
		"operator-sibling",
		"sibling.example.test",
	)

	snapshot := caddyRouteMutationTestNestedSnapshot(
		t,
		[]json.RawMessage{
			oldRoute,
			foreignSibling,
		},
	)
	client, recorder := newCaddyRouteMutationTestFixture(
		t,
		snapshot,
		http.StatusOK,
	)

	mutator := newCaddyRouteMutationTestMutator(client)

	result, err := mutator.EnsureRoute(
		context.Background(),
		expected,
	)
	if err != nil {
		t.Fatalf("ensure replacement route: %v", err)
	}

	wantPath := "/config/apps/http/servers/nexvarin/routes/2/handle/1/routes/0"
	if result.Kind != caddyRouteMutationReplaced ||
		!result.Changed ||
		result.Path != wantPath ||
		result.StatusCode != http.StatusOK {
		t.Fatalf("replacement result = %#v", result)
	}

	requests := recorder.snapshot()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want GET and PATCH", len(requests))
	}

	mutation := requests[1]
	if mutation.Method != http.MethodPatch ||
		mutation.Path != wantPath ||
		mutation.ETag != caddyRouteMutationTestETag {
		t.Fatalf("replacement request = %#v", mutation)
	}

	got, err := decodeCaddyManagedRoute(mutation.Body)
	if err != nil {
		t.Fatalf("decode replacement route: %v", err)
	}
	if got != expected {
		t.Fatalf("replacement body = %#v, want %#v", got, expected)
	}
}

func TestCaddyRouteMutatorRemoveRouteUsesSnapshotIndex(
	t *testing.T,
) {
	expected := caddyRouteMutationTestExpected()
	foreignSibling := caddyRouteConfigTestOperatorRoute(
		t,
		"operator-sibling",
		"sibling.example.test",
	)
	managed := caddyRouteConfigTestManagedRoute(
		t,
		expected.ID,
		expected.Hostname,
		expected.UpstreamAddress,
	)

	snapshot := caddyRouteMutationTestNestedSnapshot(
		t,
		[]json.RawMessage{
			foreignSibling,
			managed,
		},
	)
	client, recorder := newCaddyRouteMutationTestFixture(
		t,
		snapshot,
		http.StatusOK,
	)

	mutator := newCaddyRouteMutationTestMutator(client)

	result, err := mutator.RemoveRoute(
		context.Background(),
		expected.ID,
		expected.Hostname,
	)
	if err != nil {
		t.Fatalf("remove route: %v", err)
	}

	wantPath := "/config/apps/http/servers/nexvarin/routes/2/handle/1/routes/1"
	if result.Kind != caddyRouteMutationRemoved ||
		!result.Changed ||
		result.Path != wantPath ||
		result.StatusCode != http.StatusOK {
		t.Fatalf("removal result = %#v", result)
	}

	requests := recorder.snapshot()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want GET and DELETE", len(requests))
	}

	mutation := requests[1]
	if mutation.Method != http.MethodDelete ||
		mutation.Path != wantPath ||
		mutation.ETag != caddyRouteMutationTestETag ||
		len(mutation.Body) != 0 {
		t.Fatalf("removal request = %#v", mutation)
	}
}

func TestCaddyRouteMutatorDoesNotMutateOnHostnameConflict(
	t *testing.T,
) {
	expected := caddyRouteMutationTestExpected()
	foreignConflict := caddyRouteConfigTestOperatorRoute(
		t,
		"operator-conflict",
		expected.Hostname,
	)

	snapshot := caddyRouteMutationTestSnapshot(
		t,
		[]json.RawMessage{foreignConflict},
	)
	client, recorder := newCaddyRouteMutationTestFixture(
		t,
		snapshot,
		http.StatusOK,
	)

	mutator := newCaddyRouteMutationTestMutator(client)

	_, err := mutator.EnsureRoute(
		context.Background(),
		expected,
	)
	if !errors.Is(err, errCaddyRouteHostnameConflict) {
		t.Fatalf(
			"ensure error = %v, want hostname conflict",
			err,
		)
	}

	requests := recorder.snapshot()
	if len(requests) != 1 ||
		requests[0].Method != http.MethodGet {
		t.Fatalf(
			"requests = %#v, want only the configuration read",
			requests,
		)
	}
}

func TestCaddyRouteMutatorReturnsPreconditionFailureWithoutRetry(
	t *testing.T,
) {
	snapshot := caddyRouteMutationTestSnapshot(
		t,
		[]json.RawMessage{},
	)
	client, recorder := newCaddyRouteMutationTestFixture(
		t,
		snapshot,
		http.StatusPreconditionFailed,
	)

	mutator := newCaddyRouteMutationTestMutator(client)

	_, err := mutator.EnsureRoute(
		context.Background(),
		caddyRouteMutationTestExpected(),
	)
	if !errors.Is(err, errCaddyRouteMutationPreconditionFailed) {
		t.Fatalf(
			"mutation error = %v, want precondition failure",
			err,
		)
	}

	requests := recorder.snapshot()
	if len(requests) != 2 {
		t.Fatalf(
			"request count = %d, want exactly one GET and one POST",
			len(requests),
		)
	}

	if requests[1].Method != http.MethodPost ||
		requests[1].ETag != caddyRouteMutationTestETag {
		t.Fatalf("conditional mutation request = %#v", requests[1])
	}
}

func TestCaddyRouteMutatorPreservesUnknownMutationOutcome(
	t *testing.T,
) {
	snapshot := caddyRouteMutationTestSnapshot(
		t,
		[]json.RawMessage{},
	)

	var requestCount atomic.Int32

	client := newCaddyAdminTestClient(
		t,
		http.HandlerFunc(func(
			writer http.ResponseWriter,
			request *http.Request,
		) {
			requestCount.Add(1)

			if request.Method == http.MethodGet {
				writer.Header().Set("ETag", caddyRouteMutationTestETag)
				writer.Header().Set(
					"Content-Type",
					"application/json",
				)
				writer.WriteHeader(http.StatusOK)
				_, _ = writer.Write(snapshot)
				return
			}

			hijacker, ok := writer.(http.Hijacker)
			if !ok {
				t.Error("test server does not support connection hijacking")
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}

			connection, _, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("hijack mutation connection: %v", err)
				return
			}
			_ = connection.Close()
		}),
		caddyAdminClientConfig{},
	)

	mutator := newCaddyRouteMutationTestMutator(client)

	_, err := mutator.EnsureRoute(
		context.Background(),
		caddyRouteMutationTestExpected(),
	)
	if !errors.Is(err, errCaddyAdminOutcomeUnknown) {
		t.Fatalf(
			"mutation error = %v, want unknown Caddy outcome",
			err,
		)
	}

	if got := requestCount.Load(); got != 2 {
		t.Fatalf(
			"request count = %d, want one GET and one mutation",
			got,
		)
	}
}

func TestCaddyRouteMutatorRejectsInvalidInputsBeforeNetwork(
	t *testing.T,
) {
	mutator := caddyRouteMutator{
		ServerName:    "nexvarin",
		PreviewSuffix: caddyRouteConfigTestPreviewSuffix,
	}

	expected := caddyRouteMutationTestExpected()

	invalidRoutes := []struct {
		name   string
		config caddyRouteConfig
	}{
		{
			name: "non-NEXVARIN route ID",
			config: caddyRouteConfig{
				ID:              "operator-route",
				Hostname:        expected.Hostname,
				UpstreamAddress: expected.UpstreamAddress,
			},
		},
		{
			name: "non-canonical route UUID",
			config: caddyRouteConfig{
				ID:              caddyRouteIDPrefix + "NOT-A-UUID",
				Hostname:        expected.Hostname,
				UpstreamAddress: expected.UpstreamAddress,
			},
		},
		{
			name: "hostname outside preview suffix",
			config: caddyRouteConfig{
				ID:              expected.ID,
				Hostname:        "app.other.example.test",
				UpstreamAddress: expected.UpstreamAddress,
			},
		},
		{
			name: "non-loopback upstream",
			config: caddyRouteConfig{
				ID:              expected.ID,
				Hostname:        expected.Hostname,
				UpstreamAddress: "0.0.0.0:31001",
			},
		},
	}

	for _, test := range invalidRoutes {
		t.Run("ensure/"+test.name, func(t *testing.T) {
			_, err := mutator.EnsureRoute(
				context.Background(),
				test.config,
			)
			if !errors.Is(err, errCaddyRouteConfigInvalid) {
				t.Fatalf(
					"ensure error = %v, want invalid route configuration",
					err,
				)
			}
		})
	}

	_, err := mutator.RemoveRoute(
		context.Background(),
		"operator-route",
		expected.Hostname,
	)
	if !errors.Is(err, errCaddyRouteConfigInvalid) {
		t.Fatalf(
			"remove error = %v, want invalid route configuration",
			err,
		)
	}
}

func TestCaddyRouteMutatorAgainstPinnedCaddyLifecycle(
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
		15*time.Second,
	)
	defer cancel()

	expected := caddyRouteConfig{
		ID:              caddyAdminIntegrationRouteID,
		Hostname:        caddyAdminIntegrationHostname,
		UpstreamAddress: "127.0.0.1:31001",
	}

	added, err := mutator.EnsureRoute(ctx, expected)
	if err != nil {
		t.Fatalf("add route through mutator: %v", err)
	}
	if added.Kind != caddyRouteMutationAdded || !added.Changed {
		t.Fatalf("add result = %#v", added)
	}

	unchanged, err := mutator.EnsureRoute(ctx, expected)
	if err != nil {
		t.Fatalf("ensure unchanged route: %v", err)
	}
	if unchanged.Kind != caddyRouteMutationUnchanged || unchanged.Changed {
		t.Fatalf("unchanged result = %#v", unchanged)
	}

	expected.UpstreamAddress = "127.0.0.1:31002"

	replaced, err := mutator.EnsureRoute(ctx, expected)
	if err != nil {
		t.Fatalf("replace route through mutator: %v", err)
	}
	if replaced.Kind != caddyRouteMutationReplaced || !replaced.Changed {
		t.Fatalf("replace result = %#v", replaced)
	}

	snapshot, err := mutator.readSnapshot(
		ctx,
		caddyRouteConfigTestPreviewSuffix,
	)
	if err != nil {
		t.Fatalf("read route after replacement: %v", err)
	}

	entry, exists := snapshot.Inventory.ByID[expected.ID]
	if !exists ||
		entry.Hostname != expected.Hostname ||
		entry.UpstreamAddress != expected.UpstreamAddress {
		t.Fatalf(
			"active route after replacement = %#v, found=%t",
			entry,
			exists,
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
		t.Fatalf("remove result = %#v", removed)
	}

	removedAgain, err := mutator.RemoveRoute(
		ctx,
		expected.ID,
		expected.Hostname,
	)
	if err != nil {
		t.Fatalf("repeat route removal: %v", err)
	}
	if removedAgain.Kind != caddyRouteMutationUnchanged ||
		removedAgain.Changed {
		t.Fatalf("repeat removal result = %#v", removedAgain)
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
			"operator route after NEXVARIN lifecycle = %v, want %q",
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

func TestCaddyRouteMutationTestRecorderReturnsIndependentSnapshot(
	t *testing.T,
) {
	recorder := &caddyRouteMutationTestRecorder{}

	recorder.record(caddyRouteMutationTestRequest{
		Method: http.MethodPost,
		Path:   "/routes",
		ETag:   caddyRouteMutationTestETag,
		Body:   []byte(`{"@id":"route"}`),
	})

	first := recorder.snapshot()
	if len(first) != 1 {
		t.Fatalf("first snapshot length = %d, want 1", len(first))
	}

	first[0].Body[0] = 'x'

	second := recorder.snapshot()
	if len(second) != 1 {
		t.Fatalf("second snapshot length = %d, want 1", len(second))
	}

	if !reflect.DeepEqual(
		second[0].Body,
		[]byte(`{"@id":"route"}`),
	) {
		t.Fatalf("recorder body was modified through snapshot: %q", second[0].Body)
	}
}
