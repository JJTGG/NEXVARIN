package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	caddyAdminIntegrationVersion = "v2.11.9"

	caddyAdminIntegrationServerRoutesPath = "/config/apps/http/servers/nexvarin/routes"
	caddyAdminIntegrationNamespaceID      = "nexvarin-preview-route-namespace"
	caddyAdminIntegrationNamespacePath    = caddyAdminIntegrationServerRoutesPath +
		"/0/handle/0/routes"

	caddyAdminIntegrationOperatorRouteID = "operator-owned-route"

	caddyAdminIntegrationRouteID  = "nexvarin-route-00000000-0000-4000-8000-000000000001"
	caddyAdminIntegrationHostname = "app.preview.example.test"
)

type caddyAdminIntegration struct {
	process *caddyAdminIntegrationProcess
	client  *caddyAdminClient
}

type caddyAdminIntegrationProcess struct {
	command *exec.Cmd
	done    chan struct{}
	waitErr error
	output  bytes.Buffer
}

func startCaddyAdminIntegration(
	t *testing.T,
) *caddyAdminIntegration {
	t.Helper()

	binary := strings.TrimSpace(
		os.Getenv("CADDY_TEST_BINARY"),
	)
	if binary == "" {
		t.Skip("CADDY_TEST_BINARY is not set; pinned Caddy integration tests are unavailable")
	}

	if !filepath.IsAbs(binary) {
		t.Fatalf(
			"CADDY_TEST_BINARY must be an absolute path, got %q",
			binary,
		)
	}

	versionContext, versionCancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer versionCancel()

	versionCommand := exec.CommandContext(
		versionContext,
		binary,
		"version",
	)
	versionOutput, err := versionCommand.CombinedOutput()
	if err != nil {
		t.Fatalf(
			"execute pinned Caddy binary: %v; output: %s",
			err,
			strings.TrimSpace(string(versionOutput)),
		)
	}

	versionFields := strings.Fields(string(versionOutput))
	if len(versionFields) == 0 ||
		versionFields[0] != caddyAdminIntegrationVersion {
		t.Fatalf(
			"Caddy version = %q, want %q",
			strings.TrimSpace(string(versionOutput)),
			caddyAdminIntegrationVersion,
		)
	}

	tempDirectory := t.TempDir()
	socketPath := filepath.Join(
		tempDirectory,
		"admin.sock",
	)
	configPath := filepath.Join(
		tempDirectory,
		"caddy.json",
	)

	config := caddyAdminIntegrationConfig(socketPath)

	configBytes, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		t.Fatalf("marshal Caddy integration configuration: %v", err)
	}

	if err := os.WriteFile(configPath, configBytes, 0600); err != nil {
		t.Fatalf("write Caddy integration configuration: %v", err)
	}

	command := exec.Command(
		binary,
		"run",
		"--config",
		configPath,
	)
	command.Env = caddyAdminIntegrationEnvironment(tempDirectory)

	process := &caddyAdminIntegrationProcess{
		command: command,
		done:    make(chan struct{}),
	}

	command.Stdout = &process.output
	command.Stderr = &process.output

	if err := command.Start(); err != nil {
		t.Fatalf("start pinned Caddy process: %v", err)
	}

	go func() {
		process.waitErr = command.Wait()
		close(process.done)
	}()

	t.Cleanup(func() {
		process.stop(t)
	})

	client, err := newCaddyAdminClient(caddyAdminClientConfig{
		SocketPath:     socketPath,
		RequestTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("create Caddy Admin integration client: %v", err)
	}

	t.Cleanup(client.Close)

	integration := &caddyAdminIntegration{
		process: process,
		client:  client,
	}

	if err := integration.waitForAdminAPI(); err != nil {
		process.stop(t)

		t.Fatalf(
			"wait for pinned Caddy Admin API: %v; process output:\n%s",
			err,
			process.output.String(),
		)
	}

	return integration
}

func caddyAdminIntegrationConfig(
	socketPath string,
) map[string]any {
	return map[string]any{
		"admin": map[string]any{
			"listen": "unix/" + socketPath,
			"config": map[string]any{
				"persist": false,
			},
		},
		"apps": map[string]any{
			"http": map[string]any{
				"servers": map[string]any{
					"nexvarin": map[string]any{
						"automatic_https": map[string]any{
							"disable": true,
						},
						"listen": []string{
							"127.0.0.1:0",
						},
						"routes": []any{
							map[string]any{
								"match": []any{
									map[string]any{
										"host": []string{
											"*.preview.example.test",
										},
									},
								},
								"handle": []any{
									map[string]any{
										"handler": "subroute",
										"@id":     caddyAdminIntegrationNamespaceID,
										"routes":  []any{},
									},
								},
							},
							map[string]any{
								"@id": caddyAdminIntegrationOperatorRouteID,
								"match": []any{
									map[string]any{
										"host": []string{
											"operator.preview.example.test",
										},
									},
								},
								"handle": []any{
									map[string]any{
										"handler": "static_response",
										"body":    "operator-owned",
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func caddyAdminIntegrationEnvironment(
	tempDirectory string,
) []string {
	overrides := []struct {
		name  string
		value string
	}{
		{
			name:  "XDG_CONFIG_HOME",
			value: filepath.Join(tempDirectory, "config"),
		},
		{
			name:  "XDG_DATA_HOME",
			value: filepath.Join(tempDirectory, "data"),
		},
		{
			name:  "XDG_CACHE_HOME",
			value: filepath.Join(tempDirectory, "cache"),
		},
		{
			name:  "CADDY_ADMIN",
			value: "",
		},
	}

	replaced := make(map[string]struct{}, len(overrides))
	for _, override := range overrides {
		replaced[override.name] = struct{}{}
	}

	environment := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if _, ok := replaced[name]; ok {
			continue
		}

		environment = append(environment, entry)
	}

	for _, override := range overrides {
		environment = append(
			environment,
			override.name+"="+override.value,
		)
	}

	return environment
}

func (process *caddyAdminIntegrationProcess) stop(
	t *testing.T,
) {
	t.Helper()

	if process == nil ||
		process.command == nil ||
		process.command.Process == nil {
		return
	}

	select {
	case <-process.done:
		return
	default:
	}

	if err := process.command.Process.Signal(os.Interrupt); err != nil &&
		!errors.Is(err, os.ErrProcessDone) {
		t.Errorf("send Caddy shutdown signal: %v", err)
	}

	select {
	case <-process.done:
		return

	case <-time.After(5 * time.Second):
		_ = process.command.Process.Kill()
		<-process.done

		t.Errorf(
			"Caddy did not stop after the shutdown signal; process output:\n%s",
			process.output.String(),
		)
	}
}

func (integration *caddyAdminIntegration) waitForAdminAPI() error {
	deadline := time.Now().Add(10 * time.Second)

	for {
		select {
		case <-integration.process.done:
			return fmt.Errorf(
				"Caddy exited before its Admin API became ready: %w",
				integration.process.waitErr,
			)
		default:
		}

		ctx, cancel := context.WithTimeout(
			context.Background(),
			500*time.Millisecond,
		)

		response, err := integration.client.do(
			ctx,
			http.MethodGet,
			caddyAdminIntegrationServerRoutesPath,
			nil,
			"",
		)
		cancel()

		if err == nil &&
			response.StatusCode == http.StatusOK &&
			response.ETag != "" {
			return nil
		}

		if time.Now().After(deadline) {
			return errors.New(
				"Caddy Admin API did not return a configuration snapshot with an ETag within 10 seconds",
			)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (integration *caddyAdminIntegration) readServerRoutes(
	t *testing.T,
) ([]map[string]any, string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	response, err := integration.client.do(
		ctx,
		http.MethodGet,
		caddyAdminIntegrationServerRoutesPath,
		nil,
		"",
	)
	if err != nil {
		t.Fatalf("read Caddy server routes: %v", err)
	}

	if response.StatusCode != http.StatusOK {
		t.Fatalf(
			"Caddy server-routes status = %d, want %d; body: %s",
			response.StatusCode,
			http.StatusOK,
			response.Body,
		)
	}

	if response.ETag == "" {
		t.Fatal("Caddy server-routes response did not contain an ETag")
	}

	var routes []map[string]any
	if err := json.Unmarshal(response.Body, &routes); err != nil {
		t.Fatalf("decode Caddy server routes: %v", err)
	}

	return routes, response.ETag
}

func caddyAdminIntegrationNamespaceRoutes(
	t *testing.T,
	serverRoutes []map[string]any,
) []any {
	t.Helper()

	var (
		namespaceCount  int
		namespaceRoutes []any
	)

	for _, route := range serverRoutes {
		handlers, ok := route["handle"].([]any)
		if !ok {
			continue
		}

		for _, handlerValue := range handlers {
			handler, ok := handlerValue.(map[string]any)
			if !ok ||
				handler["@id"] != caddyAdminIntegrationNamespaceID {
				continue
			}

			namespaceCount++

			if handler["handler"] != "subroute" {
				t.Fatalf(
					"NEXVARIN namespace handler = %v, want subroute",
					handler["handler"],
				)
			}

			children, ok := handler["routes"].([]any)
			if !ok {
				t.Fatal("NEXVARIN namespace routes value is not an array")
			}

			namespaceRoutes = children
		}
	}

	if namespaceCount != 1 {
		t.Fatalf(
			"NEXVARIN namespace anchor count = %d, want exactly 1",
			namespaceCount,
		)
	}

	return namespaceRoutes
}

func caddyAdminIntegrationRoute(
	upstream string,
) map[string]any {
	return map[string]any{
		"@id": caddyAdminIntegrationRouteID,
		"match": []any{
			map[string]any{
				"host": []string{
					caddyAdminIntegrationHostname,
				},
			},
		},
		"handle": []any{
			map[string]any{
				"handler": "reverse_proxy",
				"upstreams": []any{
					map[string]any{
						"dial": upstream,
					},
				},
			},
		},
	}
}

func marshalCaddyAdminIntegrationRoute(
	t *testing.T,
	route map[string]any,
) []byte {
	t.Helper()

	body, err := json.Marshal(route)
	if err != nil {
		t.Fatalf("marshal Caddy integration route: %v", err)
	}

	return body
}

func assertCaddyAdminIntegrationMutationSuccess(
	t *testing.T,
	response caddyAdminResponse,
) {
	t.Helper()

	if response.StatusCode < http.StatusOK ||
		response.StatusCode >= http.StatusMultipleChoices {
		t.Fatalf(
			"Caddy mutation status = %d, want a 2xx response; body: %s",
			response.StatusCode,
			response.Body,
		)
	}
}

func assertCaddyAdminIntegrationRoute(
	t *testing.T,
	route map[string]any,
	wantUpstream string,
) {
	t.Helper()

	if route["@id"] != caddyAdminIntegrationRouteID {
		t.Errorf(
			"route ID = %v, want %q",
			route["@id"],
			caddyAdminIntegrationRouteID,
		)
	}

	matchers, ok := route["match"].([]any)
	if !ok || len(matchers) != 1 {
		t.Fatalf("route matchers = %v, want exactly one matcher", route["match"])
	}

	matcher, ok := matchers[0].(map[string]any)
	if !ok {
		t.Fatalf("route matcher has unexpected type: %T", matchers[0])
	}

	hosts, ok := matcher["host"].([]any)
	if !ok || len(hosts) != 1 || hosts[0] != caddyAdminIntegrationHostname {
		t.Errorf(
			"route hosts = %v, want exactly %q",
			matcher["host"],
			caddyAdminIntegrationHostname,
		)
	}

	handlers, ok := route["handle"].([]any)
	if !ok || len(handlers) != 1 {
		t.Fatalf("route handlers = %v, want exactly one handler", route["handle"])
	}

	handler, ok := handlers[0].(map[string]any)
	if !ok {
		t.Fatalf("route handler has unexpected type: %T", handlers[0])
	}

	if handler["handler"] != "reverse_proxy" {
		t.Errorf(
			"route handler = %v, want reverse_proxy",
			handler["handler"],
		)
	}

	upstreams, ok := handler["upstreams"].([]any)
	if !ok || len(upstreams) != 1 {
		t.Fatalf(
			"reverse proxy upstreams = %v, want exactly one upstream",
			handler["upstreams"],
		)
	}

	upstream, ok := upstreams[0].(map[string]any)
	if !ok {
		t.Fatalf(
			"reverse proxy upstream has unexpected type: %T",
			upstreams[0],
		)
	}

	if upstream["dial"] != wantUpstream {
		t.Errorf(
			"upstream dial = %v, want %q",
			upstream["dial"],
			wantUpstream,
		)
	}
}

func TestCaddyAdminIntegrationConditionalRouteMutations(
	t *testing.T,
) {
	integration := startCaddyAdminIntegration(t)

	initialRoutes, initialETag := integration.readServerRoutes(t)

	if len(initialRoutes) != 2 {
		t.Fatalf(
			"initial server route count = %d, want 2",
			len(initialRoutes),
		)
	}

	initialNamespaceRoutes := caddyAdminIntegrationNamespaceRoutes(
		t,
		initialRoutes,
	)
	if len(initialNamespaceRoutes) != 0 {
		t.Fatalf(
			"initial NEXVARIN route count = %d, want 0",
			len(initialNamespaceRoutes),
		)
	}

	route := caddyAdminIntegrationRoute("127.0.0.1:31001")
	routeBody := marshalCaddyAdminIntegrationRoute(t, route)

	appendResponse, err := integration.client.do(
		context.Background(),
		http.MethodPost,
		caddyAdminIntegrationNamespacePath,
		routeBody,
		initialETag,
	)
	if err != nil {
		t.Fatalf("append NEXVARIN route using Caddy Admin API: %v", err)
	}
	assertCaddyAdminIntegrationMutationSuccess(t, appendResponse)

	// Reusing the original ETag must fail after the first mutation.
	// This proves Caddy protects the parent routes scope, not merely
	// the exact nested array endpoint.
	staleRoute := caddyAdminIntegrationRoute("127.0.0.1:31003")
	staleRoute["@id"] = "nexvarin-route-stale"
	staleBody := marshalCaddyAdminIntegrationRoute(t, staleRoute)

	staleResponse, err := integration.client.do(
		context.Background(),
		http.MethodPost,
		caddyAdminIntegrationNamespacePath,
		staleBody,
		initialETag,
	)
	if err != nil {
		t.Fatalf("submit stale conditional mutation: %v", err)
	}

	if staleResponse.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf(
			"stale mutation status = %d, want HTTP 412; body: %s",
			staleResponse.StatusCode,
			staleResponse.Body,
		)
	}

	routesAfterAppend, appendETag := integration.readServerRoutes(t)
	if appendETag == initialETag {
		t.Fatal("Caddy ETag did not change after route append")
	}

	childrenAfterAppend := caddyAdminIntegrationNamespaceRoutes(
		t,
		routesAfterAppend,
	)
	if len(childrenAfterAppend) != 1 {
		t.Fatalf(
			"NEXVARIN route count after append = %d, want 1",
			len(childrenAfterAppend),
		)
	}

	installedRoute, ok := childrenAfterAppend[0].(map[string]any)
	if !ok {
		t.Fatalf(
			"installed route has unexpected type: %T",
			childrenAfterAppend[0],
		)
	}
	assertCaddyAdminIntegrationRoute(
		t,
		installedRoute,
		"127.0.0.1:31001",
	)

	// Replace the route at its current index using the ETag obtained
	// from the latest complete server-routes snapshot.
	replacement := caddyAdminIntegrationRoute("127.0.0.1:31002")
	replacementBody := marshalCaddyAdminIntegrationRoute(t, replacement)

	replaceResponse, err := integration.client.do(
		context.Background(),
		http.MethodPatch,
		caddyAdminIntegrationNamespacePath+"/0",
		replacementBody,
		appendETag,
	)
	if err != nil {
		t.Fatalf("replace NEXVARIN route using Caddy Admin API: %v", err)
	}
	assertCaddyAdminIntegrationMutationSuccess(t, replaceResponse)

	routesAfterReplace, replaceETag := integration.readServerRoutes(t)
	if replaceETag == appendETag {
		t.Fatal("Caddy ETag did not change after route replacement")
	}

	childrenAfterReplace := caddyAdminIntegrationNamespaceRoutes(
		t,
		routesAfterReplace,
	)
	if len(childrenAfterReplace) != 1 {
		t.Fatalf(
			"NEXVARIN route count after replacement = %d, want 1",
			len(childrenAfterReplace),
		)
	}

	replacedRoute, ok := childrenAfterReplace[0].(map[string]any)
	if !ok {
		t.Fatalf(
			"replaced route has unexpected type: %T",
			childrenAfterReplace[0],
		)
	}
	assertCaddyAdminIntegrationRoute(
		t,
		replacedRoute,
		"127.0.0.1:31002",
	)

	// Remove only the owned route. The operator-owned sibling must remain.
	deleteResponse, err := integration.client.do(
		context.Background(),
		http.MethodDelete,
		caddyAdminIntegrationNamespacePath+"/0",
		nil,
		replaceETag,
	)
	if err != nil {
		t.Fatalf("delete NEXVARIN route using Caddy Admin API: %v", err)
	}
	assertCaddyAdminIntegrationMutationSuccess(t, deleteResponse)

	finalRoutes, finalETag := integration.readServerRoutes(t)
	if finalETag == replaceETag {
		t.Fatal("Caddy ETag did not change after route deletion")
	}

	finalNamespaceRoutes := caddyAdminIntegrationNamespaceRoutes(
		t,
		finalRoutes,
	)
	if len(finalNamespaceRoutes) != 0 {
		t.Fatalf(
			"NEXVARIN route count after deletion = %d, want 0",
			len(finalNamespaceRoutes),
		)
	}

	if len(finalRoutes) != 2 {
		t.Fatalf(
			"final server route count = %d, want 2; operator route may have been modified",
			len(finalRoutes),
		)
	}

	operatorRoute := finalRoutes[1]
	if operatorRoute["@id"] != caddyAdminIntegrationOperatorRouteID {
		t.Errorf(
			"operator route ID after NEXVARIN mutations = %v, want %q",
			operatorRoute["@id"],
			caddyAdminIntegrationOperatorRouteID,
		)
	}
}
