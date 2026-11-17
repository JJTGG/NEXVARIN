package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"
)

const (
	caddyRouteConfigTestPreviewSuffix = "preview.example.test"
	caddyRouteConfigTestRouteUUID     = "00000000-0000-4000-8000-000000000001"
	caddyRouteConfigTestRouteID       = caddyRouteIDPrefix + caddyRouteConfigTestRouteUUID
	caddyRouteConfigTestHostname      = "app.preview.example.test"
	caddyRouteConfigTestUpstream      = "127.0.0.1:31001"
)

func caddyRouteConfigTestJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal Caddy route test fixture: %v", err)
	}
	return encoded
}

func caddyRouteConfigTestManagedRoute(
	t *testing.T,
	id, hostname, upstream string,
) json.RawMessage {
	t.Helper()
	return caddyRouteConfigTestJSON(t, map[string]any{
		"@id": id,
		"match": []any{
			map[string]any{"host": []string{hostname}},
		},
		"handle": []any{
			map[string]any{
				"handler": "reverse_proxy",
				"upstreams": []any{
					map[string]any{"dial": upstream},
				},
			},
		},
	})
}

func caddyRouteConfigTestNamespaceRoute(
	t *testing.T,
	suffix string,
	children []json.RawMessage,
) json.RawMessage {
	t.Helper()
	if children == nil {
		children = []json.RawMessage{}
	}
	return caddyRouteConfigTestJSON(t, map[string]any{
		"match": []any{
			map[string]any{"host": []string{"*." + suffix}},
		},
		"handle": []any{
			map[string]any{
				"handler": "subroute",
				"@id":     caddyRouteNamespaceID,
				"routes":  children,
			},
		},
	})
}

func caddyRouteConfigTestOperatorRoute(
	t *testing.T,
	id, hostname string,
) json.RawMessage {
	t.Helper()
	return caddyRouteConfigTestJSON(t, map[string]any{
		"@id": id,
		"match": []any{
			map[string]any{"host": []string{hostname}},
		},
		"handle": []any{
			map[string]any{
				"handler": "static_response",
				"body":    "operator-owned",
			},
		},
	})
}

func caddyRouteConfigTestResponse(
	t *testing.T,
	status int,
	etag string,
	routes []json.RawMessage,
) caddyAdminResponse {
	t.Helper()
	body, err := json.Marshal(routes)
	if err != nil {
		t.Fatalf("marshal server route fixture: %v", err)
	}
	return caddyAdminResponse{StatusCode: status, ETag: etag, Body: body}
}

func TestCaddyAdminServerRoutesPath(t *testing.T) {
	tests := []struct {
		name, server, want string
		invalid            bool
	}{
		{name: "ordinary", server: "nexvarin", want: "/config/apps/http/servers/nexvarin/routes"},
		{name: "separators", server: "preview-edge_1", want: "/config/apps/http/servers/preview-edge_1/routes"},
		{name: "empty", server: "", invalid: true},
		{name: "leading whitespace", server: " nexvarin", invalid: true},
		{name: "trailing whitespace", server: "nexvarin ", invalid: true},
		{name: "dot segment", server: ".", invalid: true},
		{name: "dot dot segment", server: "..", invalid: true},
		{name: "slash", server: "edge/routes", invalid: true},
		{name: "backslash", server: `edge\routes`, invalid: true},
		{name: "query", server: "edge?admin", invalid: true},
		{name: "fragment", server: "edge#admin", invalid: true},
		{name: "unicode", server: "préview", invalid: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := caddyAdminServerRoutesPath(test.server)
			if test.invalid {
				if !errors.Is(err, errCaddyRouteConfigInvalid) {
					t.Fatalf("error = %v, want invalid configuration", err)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("path = %q, error = %v; want %q", got, err, test.want)
			}
		})
	}
}

func TestParseCaddyAdminServerRoutesResponse(t *testing.T) {
	managed := caddyRouteConfigTestManagedRoute(
		t,
		caddyRouteConfigTestRouteID,
		caddyRouteConfigTestHostname,
		caddyRouteConfigTestUpstream,
	)
	operator := caddyRouteConfigTestOperatorRoute(t, "operator-route", "operator.example.test")
	anchor := caddyRouteConfigTestNamespaceRoute(t, caddyRouteConfigTestPreviewSuffix, []json.RawMessage{managed})

	response := caddyRouteConfigTestResponse(
		t,
		http.StatusOK,
		`"caddy-snapshot-opaque-1"`,
		[]json.RawMessage{operator, anchor},
	)
	snapshot, err := parseCaddyAdminServerRoutesResponse(
		response,
		"nexvarin",
		caddyRouteConfigTestPreviewSuffix,
	)
	if err != nil {
		t.Fatalf("parse server routes: %v", err)
	}

	if snapshot.ServerRoutesPath != "/config/apps/http/servers/nexvarin/routes" {
		t.Errorf("server routes path = %q", snapshot.ServerRoutesPath)
	}
	if snapshot.ETag != response.ETag {
		t.Errorf("ETag = %q, want exact %q", snapshot.ETag, response.ETag)
	}
	if len(snapshot.ServerRoutes) != 2 || snapshot.Namespace.ParentRouteIndex != 1 {
		t.Fatalf("snapshot routes=%d namespace parent index=%d", len(snapshot.ServerRoutes), snapshot.Namespace.ParentRouteIndex)
	}
	if snapshot.Namespace.HandlerIndex != 0 || snapshot.Namespace.RoutesPath != "/config/apps/http/servers/nexvarin/routes/1/handle/0/routes" {
		t.Errorf("namespace handler/index path = %d %q", snapshot.Namespace.HandlerIndex, snapshot.Namespace.RoutesPath)
	}
	entry, ok := snapshot.Inventory.ByID[caddyRouteConfigTestRouteID]
	if !ok || !entry.Managed || entry.Hostname != caddyRouteConfigTestHostname || entry.UpstreamAddress != caddyRouteConfigTestUpstream {
		t.Fatalf("managed route inventory entry = %#v, found=%t", entry, ok)
	}
}

func TestParseCaddyAdminServerRoutesResponseRejectsInvalidEnvelope(t *testing.T) {
	tests := []struct {
		name      string
		response  caddyAdminResponse
		server    string
		wantError error
	}{
		{
			name:     "non-success status",
			response: caddyAdminResponse{StatusCode: http.StatusServiceUnavailable, ETag: `"v1"`, Body: []byte(`[]`)},
			server:   "nexvarin", wantError: errCaddyRouteConfigInvalid,
		},
		{
			name:     "missing ETag",
			response: caddyAdminResponse{StatusCode: http.StatusOK, Body: []byte(`[]`)},
			server:   "nexvarin", wantError: errCaddyRouteConfigInvalid,
		},
		{
			name:     "whitespace ETag",
			response: caddyAdminResponse{StatusCode: http.StatusOK, ETag: `"v1" `, Body: []byte(`[]`)},
			server:   "nexvarin", wantError: errCaddyRouteConfigInvalid,
		},
		{
			name:     "malformed JSON",
			response: caddyAdminResponse{StatusCode: http.StatusOK, ETag: `"v1"`, Body: []byte(`[{`)},
			server:   "nexvarin", wantError: errCaddyRouteConfigInvalid,
		},
		{
			name:     "null instead of array",
			response: caddyAdminResponse{StatusCode: http.StatusOK, ETag: `"v1"`, Body: []byte(`null`)},
			server:   "nexvarin", wantError: errCaddyRouteConfigInvalid,
		},
		{
			name:     "missing namespace",
			response: caddyAdminResponse{StatusCode: http.StatusOK, ETag: `"v1"`, Body: []byte(`[]`)},
			server:   "nexvarin", wantError: errCaddyRouteAnchorMissing,
		},
		{
			name:     "unsafe server name",
			response: caddyAdminResponse{StatusCode: http.StatusOK, ETag: `"v1"`, Body: []byte(`[]`)},
			server:   "../nexvarin", wantError: errCaddyRouteConfigInvalid,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseCaddyAdminServerRoutesResponse(
				test.response,
				test.server,
				caddyRouteConfigTestPreviewSuffix,
			)
			if !errors.Is(err, test.wantError) {
				t.Fatalf("error = %v, want errors.Is(_, %v)", err, test.wantError)
			}
		})
	}
}

func TestDiscoverCaddyRouteNamespaceFailsClosed(t *testing.T) {
	valid := caddyRouteConfigTestNamespaceRoute(t, caddyRouteConfigTestPreviewSuffix, nil)
	invalidHandler := caddyRouteConfigTestJSON(t, map[string]any{
		"match":  []any{map[string]any{"host": []string{"*." + caddyRouteConfigTestPreviewSuffix}}},
		"handle": []any{map[string]any{"handler": "static_response", "@id": caddyRouteNamespaceID}},
	})
	missingRoutes := caddyRouteConfigTestJSON(t, map[string]any{
		"match":  []any{map[string]any{"host": []string{"*." + caddyRouteConfigTestPreviewSuffix}}},
		"handle": []any{map[string]any{"handler": "subroute", "@id": caddyRouteNamespaceID}},
	})
	wrongParentHost := caddyRouteConfigTestJSON(t, map[string]any{
		"match":  []any{map[string]any{"host": []string{"*.other.example.test"}}},
		"handle": []any{map[string]any{"handler": "subroute", "@id": caddyRouteNamespaceID, "routes": []any{}}},
	})
	nestedAnchor := caddyRouteConfigTestJSON(t, map[string]any{
		"handle": []any{map[string]any{
			"handler": "subroute",
			"routes": []any{map[string]any{
				"handle": []any{map[string]any{"handler": "subroute", "@id": caddyRouteNamespaceID, "routes": []any{}}},
			}},
		}},
	})

	tests := []struct {
		name      string
		routes    []json.RawMessage
		wantError error
	}{
		{name: "missing", routes: []json.RawMessage{}, wantError: errCaddyRouteAnchorMissing},
		{name: "wrong handler", routes: []json.RawMessage{invalidHandler}, wantError: errCaddyRouteAnchorInvalid},
		{name: "missing routes", routes: []json.RawMessage{missingRoutes}, wantError: errCaddyRouteAnchorInvalid},
		{name: "wrong parent host", routes: []json.RawMessage{wrongParentHost}, wantError: errCaddyRouteAnchorInvalid},
		{name: "duplicate anchors", routes: []json.RawMessage{valid, valid}, wantError: errCaddyRouteAnchorInvalid},
		{name: "nested anchor", routes: []json.RawMessage{nestedAnchor}, wantError: errCaddyRouteAnchorInvalid},
		{
			name: "null child routes",
			routes: []json.RawMessage{caddyRouteConfigTestJSON(t, map[string]any{
				"match":  []any{map[string]any{"host": []string{"*." + caddyRouteConfigTestPreviewSuffix}}},
				"handle": []any{map[string]any{"handler": "subroute", "@id": caddyRouteNamespaceID, "routes": nil}},
			})},
			wantError: errCaddyRouteAnchorInvalid,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := discoverCaddyRouteNamespace(
				test.routes,
				"/config/apps/http/servers/nexvarin/routes",
				caddyRouteConfigTestPreviewSuffix,
			)
			if !errors.Is(err, test.wantError) {
				t.Fatalf("error = %v, want errors.Is(_, %v)", err, test.wantError)
			}
		})
	}
}

func TestDiscoverCaddyRouteNamespaceUsesCurrentIndices(t *testing.T) {
	operator := caddyRouteConfigTestOperatorRoute(t, "operator-before", "operator.example.test")
	parent := caddyRouteConfigTestJSON(t, map[string]any{
		"match": []any{map[string]any{"host": []string{"*." + caddyRouteConfigTestPreviewSuffix}}},
		"handle": []any{
			map[string]any{"handler": "static_response", "body": "operator handler"},
			map[string]any{"handler": "subroute", "@id": caddyRouteNamespaceID, "routes": []any{}},
		},
	})

	namespace, err := discoverCaddyRouteNamespace(
		[]json.RawMessage{operator, parent},
		"/config/apps/http/servers/nexvarin/routes",
		caddyRouteConfigTestPreviewSuffix,
	)
	if err != nil {
		t.Fatalf("discover namespace: %v", err)
	}
	if namespace.ParentRouteIndex != 1 || namespace.HandlerIndex != 1 {
		t.Fatalf("indices = (%d, %d), want (1, 1)", namespace.ParentRouteIndex, namespace.HandlerIndex)
	}
	if namespace.RoutesPath != "/config/apps/http/servers/nexvarin/routes/1/handle/1/routes" {
		t.Fatalf("routes path = %q", namespace.RoutesPath)
	}
	if len(namespace.Routes) != 0 {
		t.Fatalf("routes = %v, want zero child routes", namespace.Routes)
	}
}

func TestInspectCaddyRouteInventory(t *testing.T) {
	managed := caddyRouteConfigTestManagedRoute(
		t, caddyRouteConfigTestRouteID, caddyRouteConfigTestHostname, caddyRouteConfigTestUpstream,
	)
	foreign := caddyRouteConfigTestOperatorRoute(t, "operator-route", "operator.example.test")
	catchAll := caddyRouteConfigTestJSON(t, map[string]any{
		"handle": []any{map[string]any{"handler": "static_response"}},
	})

	inventory, err := inspectCaddyRouteInventory([]json.RawMessage{managed, foreign, catchAll})
	if err != nil {
		t.Fatalf("inspect mixed inventory: %v", err)
	}
	if len(inventory.Entries) != 3 || len(inventory.ByID) != 1 || len(inventory.ByHostname) != 1 {
		t.Fatalf("inventory sizes: entries=%d IDs=%d hostnames=%d", len(inventory.Entries), len(inventory.ByID), len(inventory.ByHostname))
	}
	if !inventory.ByID[caddyRouteConfigTestRouteID].Managed {
		t.Fatal("NEXVARIN route was not marked managed")
	}
	if inventory.Entries[1].Managed {
		t.Fatal("operator-owned route was incorrectly marked managed")
	}
	if !inventory.Entries[2].MatchesAllHosts {
		t.Fatal("route without a host matcher was not classified as catch-all")
	}

	tests := []struct {
		name      string
		routes    []json.RawMessage
		wantError error
	}{
		{
			name:      "duplicate ID",
			routes:    []json.RawMessage{managed, managed},
			wantError: errCaddyRouteIdentityConflict,
		},
		{
			name: "duplicate hostname",
			routes: []json.RawMessage{
				managed,
				caddyRouteConfigTestManagedRoute(t, caddyRouteIDPrefix+"00000000-0000-4000-8000-000000000002", caddyRouteConfigTestHostname, "127.0.0.1:31002"),
			},
			wantError: errCaddyRouteHostnameConflict,
		},
		{
			name: "malformed managed route",
			routes: []json.RawMessage{caddyRouteConfigTestJSON(t, map[string]any{
				"@id": caddyRouteConfigTestRouteID, "match": []any{}, "handle": []any{}, "terminal": true,
			})},
			wantError: errCaddyRouteConfigInvalid,
		},
		{name: "non-object route", routes: []json.RawMessage{json.RawMessage(`null`)}, wantError: errCaddyRouteConfigInvalid},
		{
			name:      "malformed matcher",
			routes:    []json.RawMessage{caddyRouteConfigTestJSON(t, map[string]any{"match": []any{"not-an-object"}})},
			wantError: errCaddyRouteConfigInvalid,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := inspectCaddyRouteInventory(test.routes)
			if !errors.Is(err, test.wantError) {
				t.Fatalf("error = %v, want errors.Is(_, %v)", err, test.wantError)
			}
		})
	}
}

func TestCaddyRouteInventoryValidateExpectedRoute(t *testing.T) {
	expected := caddyRouteConfig{
		ID: caddyRouteConfigTestRouteID, Hostname: caddyRouteConfigTestHostname, UpstreamAddress: caddyRouteConfigTestUpstream,
	}
	tests := []struct {
		name      string
		routes    []json.RawMessage
		wantError error
	}{
		{name: "empty", routes: []json.RawMessage{}},
		{
			name:   "same ID and hostname with old upstream is replaceable",
			routes: []json.RawMessage{caddyRouteConfigTestManagedRoute(t, expected.ID, expected.Hostname, "127.0.0.1:31009")},
		},
		{
			name:      "same ID different hostname",
			routes:    []json.RawMessage{caddyRouteConfigTestManagedRoute(t, expected.ID, "other.preview.example.test", expected.UpstreamAddress)},
			wantError: errCaddyRouteIdentityConflict,
		},
		{
			name:      "different ID same hostname",
			routes:    []json.RawMessage{caddyRouteConfigTestManagedRoute(t, caddyRouteIDPrefix+"00000000-0000-4000-8000-000000000002", expected.Hostname, expected.UpstreamAddress)},
			wantError: errCaddyRouteHostnameConflict,
		},
		{
			name:      "foreign exact hostname",
			routes:    []json.RawMessage{caddyRouteConfigTestOperatorRoute(t, "operator-exact", expected.Hostname)},
			wantError: errCaddyRouteHostnameConflict,
		},
		{
			name: "foreign wildcard hostname",
			routes: []json.RawMessage{caddyRouteConfigTestJSON(t, map[string]any{
				"match":  []any{map[string]any{"host": []string{"*.preview.example.test"}}},
				"handle": []any{map[string]any{"handler": "static_response"}},
			})},
			wantError: errCaddyRouteHostnameConflict,
		},
		{
			name: "foreign catch-all",
			routes: []json.RawMessage{caddyRouteConfigTestJSON(t, map[string]any{
				"handle": []any{map[string]any{"handler": "static_response"}},
			})},
			wantError: errCaddyRouteHostnameConflict,
		},
		{
			name:   "foreign non-overlapping host",
			routes: []json.RawMessage{caddyRouteConfigTestOperatorRoute(t, "operator-other", "operator.example.test")},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inventory, err := inspectCaddyRouteInventory(test.routes)
			if err != nil {
				t.Fatalf("inspect inventory: %v", err)
			}
			err = inventory.validateExpectedRoute(expected)
			if test.wantError != nil {
				if !errors.Is(err, test.wantError) {
					t.Fatalf("validation error = %v, want errors.Is(_, %v)", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}

func TestNewCaddyRouteConfigAndUpstreamValidation(t *testing.T) {
	hostname, err := deploymentRouteHostname(caddyRouteConfigTestRouteUUID, caddyRouteConfigTestPreviewSuffix)
	if err != nil {
		t.Fatalf("derive deployment hostname: %v", err)
	}
	route := deploymentRouteRecord{ID: caddyRouteConfigTestRouteUUID, Hostname: hostname}
	config, err := newCaddyRouteConfig(route, caddyRouteConfigTestUpstream, caddyRouteConfigTestPreviewSuffix)
	if err != nil {
		t.Fatalf("create Caddy route config: %v", err)
	}
	if config.ID != caddyRouteConfigTestRouteID || config.Hostname != hostname || config.UpstreamAddress != caddyRouteConfigTestUpstream {
		t.Fatalf("unexpected route config: %#v", config)
	}

	invalid := []struct {
		name     string
		route    deploymentRouteRecord
		upstream string
		suffix   string
	}{
		{name: "invalid UUID", route: deploymentRouteRecord{ID: "not-a-uuid", Hostname: hostname}, upstream: caddyRouteConfigTestUpstream, suffix: caddyRouteConfigTestPreviewSuffix},
		{name: "uppercase UUID", route: deploymentRouteRecord{ID: "00000000-0000-4000-8000-00000000000A", Hostname: hostname}, upstream: caddyRouteConfigTestUpstream, suffix: caddyRouteConfigTestPreviewSuffix},
		{name: "wrong suffix", route: deploymentRouteRecord{ID: route.ID, Hostname: "app.other.example.test"}, upstream: caddyRouteConfigTestUpstream, suffix: caddyRouteConfigTestPreviewSuffix},
		{name: "uppercase hostname", route: deploymentRouteRecord{ID: route.ID, Hostname: "APP." + caddyRouteConfigTestPreviewSuffix}, upstream: caddyRouteConfigTestUpstream, suffix: caddyRouteConfigTestPreviewSuffix},
		{name: "non-loopback upstream", route: route, upstream: "0.0.0.0:31001", suffix: caddyRouteConfigTestPreviewSuffix},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			_, err := newCaddyRouteConfig(test.route, test.upstream, test.suffix)
			if !errors.Is(err, errCaddyRouteConfigInvalid) {
				t.Fatalf("error = %v, want invalid route configuration", err)
			}
		})
	}

	upstreams := []struct {
		value string
		valid bool
	}{
		{value: "127.0.0.1:1", valid: true},
		{value: "127.0.0.1:31001", valid: true},
		{value: "127.0.0.1:65535", valid: true},
		{value: "", valid: false},
		{value: " 127.0.0.1:31001", valid: false},
		{value: "127.0.0.1:31001 ", valid: false},
		{value: "localhost:31001", valid: false},
		{value: "0.0.0.0:31001", valid: false},
		{value: "[::1]:31001", valid: false},
		{value: "127.0.0.1:0", valid: false},
		{value: "127.0.0.1:65536", valid: false},
		{value: "127.0.0.1:031001", valid: false},
		{value: "127.0.0.1", valid: false},
	}
	for _, test := range upstreams {
		t.Run("upstream/"+test.value, func(t *testing.T) {
			if got := validCaddyRouteUpstreamAddress(test.value); got != test.valid {
				t.Fatalf("validCaddyRouteUpstreamAddress(%q) = %t, want %t", test.value, got, test.valid)
			}
		})
	}
}

func TestMarshalAndDecodeCaddyRouteConfig(t *testing.T) {
	want := caddyRouteConfig{
		ID: caddyRouteConfigTestRouteID, Hostname: caddyRouteConfigTestHostname, UpstreamAddress: caddyRouteConfigTestUpstream,
	}
	body, err := marshalCaddyRouteConfig(want)
	if err != nil {
		t.Fatalf("marshal expected route: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatalf("decode route body: %v", err)
	}
	if !caddyRouteJSONObjectHasExactKeys(fields, "@id", "match", "handle") {
		t.Fatalf("unexpected route keys: %v", fields)
	}
	got, err := decodeCaddyManagedRoute(body)
	if err != nil {
		t.Fatalf("decode managed route: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded route = %#v, want %#v", got, want)
	}

	mutations := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "extra top-level field", mutate: func(route map[string]any) { route["terminal"] = true }},
		{name: "multiple matchers", mutate: func(route map[string]any) {
			route["match"] = []any{map[string]any{"host": []string{want.Hostname}}, map[string]any{"path": []string{"/*"}}}
		}},
		{name: "multiple hosts", mutate: func(route map[string]any) {
			route["match"] = []any{map[string]any{"host": []string{want.Hostname, "other.preview.example.test"}}}
		}},
		{name: "extra matcher field", mutate: func(route map[string]any) { route["match"].([]any)[0].(map[string]any)["path"] = []string{"/*"} }},
		{name: "multiple handlers", mutate: func(route map[string]any) {
			route["handle"] = append(route["handle"].([]any), map[string]any{"handler": "static_response"})
		}},
		{name: "extra handler field", mutate: func(route map[string]any) {
			route["handle"].([]any)[0].(map[string]any)["trusted_proxies"] = []string{"private_ranges"}
		}},
		{name: "multiple upstreams", mutate: func(route map[string]any) {
			route["handle"].([]any)[0].(map[string]any)["upstreams"] = append(route["handle"].([]any)[0].(map[string]any)["upstreams"].([]any), map[string]any{"dial": "127.0.0.1:31002"})
		}},
		{name: "extra upstream field", mutate: func(route map[string]any) {
			route["handle"].([]any)[0].(map[string]any)["upstreams"].([]any)[0].(map[string]any)["max_conns"] = 1
		}},
		{name: "wrong handler", mutate: func(route map[string]any) { route["handle"].([]any)[0].(map[string]any)["handler"] = "static_response" }},
		{name: "invalid route UUID", mutate: func(route map[string]any) { route["@id"] = caddyRouteIDPrefix + "NOT-A-UUID" }},
		{name: "uppercase hostname", mutate: func(route map[string]any) {
			route["match"].([]any)[0].(map[string]any)["host"] = []string{"APP.PREVIEW.EXAMPLE.TEST"}
		}},
		{name: "non-loopback upstream", mutate: func(route map[string]any) {
			route["handle"].([]any)[0].(map[string]any)["upstreams"].([]any)[0].(map[string]any)["dial"] = "0.0.0.0:31001"
		}},
	}

	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			var route map[string]any
			if err := json.Unmarshal(body, &route); err != nil {
				t.Fatalf("decode route fixture: %v", err)
			}
			test.mutate(route)
			if _, err := decodeCaddyManagedRoute(caddyRouteConfigTestJSON(t, route)); !errors.Is(err, errCaddyRouteConfigInvalid) {
				t.Fatalf("decode error = %v, want invalid route configuration", err)
			}
		})
	}
}

func TestCaddyRouteHostPatternOverlaps(t *testing.T) {
	tests := []struct {
		pattern, hostname string
		want              bool
	}{
		{pattern: "app.preview.example.test", hostname: "app.preview.example.test", want: true},
		{pattern: "APP.PREVIEW.EXAMPLE.TEST", hostname: "app.preview.example.test", want: true},
		{pattern: "*.preview.example.test", hostname: "app.preview.example.test", want: true},
		{pattern: "*.example.test", hostname: "app.preview.example.test", want: true},
		{pattern: "*.preview.example.test", hostname: "preview.example.test", want: false},
		{pattern: "operator.example.test", hostname: "app.preview.example.test", want: false},
		{pattern: "*", hostname: "app.preview.example.test", want: true},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%s/%s", test.pattern, test.hostname), func(t *testing.T) {
			if got := caddyRouteHostPatternOverlaps(test.pattern, test.hostname); got != test.want {
				t.Fatalf("overlap = %t, want %t", got, test.want)
			}
		})
	}
}

func TestCaddyRouteConfigParsesPinnedCaddySnapshot(t *testing.T) {
	integration := startCaddyAdminIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	response, err := integration.client.do(ctx, http.MethodGet, caddyAdminIntegrationServerRoutesPath, nil, "")
	if err != nil {
		t.Fatalf("read pinned Caddy routes: %v", err)
	}
	snapshot, err := parseCaddyAdminServerRoutesResponse(response, "nexvarin", caddyRouteConfigTestPreviewSuffix)
	if err != nil {
		t.Fatalf("parse pinned Caddy snapshot: %v", err)
	}
	if snapshot.ETag == "" || snapshot.Namespace.RoutesPath != caddyAdminIntegrationNamespacePath {
		t.Fatalf("unexpected pinned Caddy snapshot: ETag=%q path=%q", snapshot.ETag, snapshot.Namespace.RoutesPath)
	}
	if snapshot.Namespace.ParentRouteIndex != 0 || snapshot.Namespace.HandlerIndex != 0 || len(snapshot.Inventory.Entries) != 0 {
		t.Fatalf("unexpected pinned namespace: route=%d handler=%d entries=%d", snapshot.Namespace.ParentRouteIndex, snapshot.Namespace.HandlerIndex, len(snapshot.Inventory.Entries))
	}

	expected := caddyRouteConfig{
		ID:              caddyRouteConfigTestRouteID,
		Hostname:        caddyRouteConfigTestHostname,
		UpstreamAddress: caddyRouteConfigTestUpstream,
	}
	if err := snapshot.Inventory.validateExpectedRoute(expected); err != nil {
		t.Fatalf("validate route against empty pinned namespace: %v", err)
	}

	body, err := marshalCaddyRouteConfig(expected)
	if err != nil {
		t.Fatalf("marshal route for pinned Caddy: %v", err)
	}

	mutation, err := integration.client.do(
		ctx,
		http.MethodPost,
		snapshot.Namespace.RoutesPath,
		body,
		snapshot.ETag,
	)
	if err != nil {
		t.Fatalf("append route to pinned Caddy: %v", err)
	}
	assertCaddyAdminIntegrationMutationSuccess(t, mutation)

	updatedResponse, err := integration.client.do(
		ctx,
		http.MethodGet,
		caddyAdminIntegrationServerRoutesPath,
		nil,
		"",
	)
	if err != nil {
		t.Fatalf("read updated pinned Caddy routes: %v", err)
	}

	updatedSnapshot, err := parseCaddyAdminServerRoutesResponse(
		updatedResponse,
		"nexvarin",
		caddyRouteConfigTestPreviewSuffix,
	)
	if err != nil {
		t.Fatalf("parse updated pinned Caddy snapshot: %v", err)
	}

	entry, exists := updatedSnapshot.Inventory.ByID[expected.ID]
	if !exists || entry.Hostname != expected.Hostname || entry.UpstreamAddress != expected.UpstreamAddress {
		t.Fatalf("pinned Caddy managed route = %#v, found=%t", entry, exists)
	}
}
