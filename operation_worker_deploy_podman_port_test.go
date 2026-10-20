package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOperationWorkerDeployPodmanRuntimePublishesApplicationPortOnLoopback(
	t *testing.T,
) {
	request := newOperationWorkerDeployPodmanTestRequest(t)
	request.ApplicationPort = 8080

	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("nexvarin-ready\n"))
		},
	))
	defer server.Close()

	portOutput := strings.TrimPrefix(server.URL, "http://")

	runtime, logPath := newOperationWorkerDeployPodmanPortTestRuntime(
		t,
		request,
		portOutput,
	)

	first, err := runtime.Deploy(context.Background(), request)
	if err != nil {
		t.Fatalf("deploy with loopback port publishing: %v", err)
	}

	if first.RuntimeInstanceReference != operationWorkerDeployPodmanTestContainerID {
		t.Fatalf(
			"runtime reference = %q, want %q",
			first.RuntimeInstanceReference,
			operationWorkerDeployPodmanTestContainerID,
		)
	}

	log := readOperationWorkerDeployPodmanTestLog(t, logPath)

	for _, expected := range []string{
		"--network=slirp4netns",
		"--publish 127.0.0.1::8080/tcp",
		operationWorkerDeployPodmanApplicationPortLabel + "=8080",
		"--cap-drop=ALL",
		"--security-opt=no-new-privileges",
		"--pids-limit=256",
		"--memory=512m",
		"--cpus=1",
	} {
		if !strings.Contains(log, expected) {
			t.Errorf("Podman command log does not contain %q", expected)
		}
	}

	if strings.Contains(log, "--publish 0.0.0.0:") ||
		strings.Contains(log, "--publish :::") {
		t.Fatal("application port must not be published on a non-loopback host address")
	}

	if strings.Contains(log, "--network=none") {
		t.Fatal("a published application port must not use network=none")
	}

	second, err := runtime.Deploy(context.Background(), request)
	if err != nil {
		t.Fatalf("recover loopback deployment on retry: %v", err)
	}

	if second.RuntimeInstanceReference != first.RuntimeInstanceReference {
		t.Fatalf(
			"retry reference = %q, first reference = %q",
			second.RuntimeInstanceReference,
			first.RuntimeInstanceReference,
		)
	}

	log = readOperationWorkerDeployPodmanTestLog(t, logPath)
	if countOperationWorkerDeployPodmanTestCommands(log, "run ") != 1 {
		t.Fatal("retry must recover the existing container instead of creating another")
	}

	changedPortRequest := request
	changedPortRequest.ApplicationPort = 8081

	_, err = runtime.Deploy(context.Background(), changedPortRequest)
	if !errors.Is(err, errOperationWorkerDeployRuntimeOutcomeUnknown) {
		t.Fatalf(
			"deployment with a different port error = %v, want unknown runtime outcome",
			err,
		)
	}

	log = readOperationWorkerDeployPodmanTestLog(t, logPath)
	if countOperationWorkerDeployPodmanTestCommands(log, "run ") != 1 {
		t.Fatal("a conflicting application port must not create another container")
	}
}

func TestOperationWorkerDeployPodmanRuntimeRejectsInvalidApplicationPort(
	t *testing.T,
) {
	for _, port := range []int{-1, 65536} {
		t.Run(strconv.Itoa(port), func(t *testing.T) {
			request := newOperationWorkerDeployPodmanTestRequest(t)
			request.ApplicationPort = port

			runtime, _, _ := newOperationWorkerDeployPodmanTestRuntime(
				t,
				request,
				false,
			)

			_, err := runtime.Deploy(context.Background(), request)
			if err == nil {
				t.Fatalf("Deploy() with port %d succeeded, want error", port)
			}
		})
	}
}

func TestOperationWorkerDeployPodmanRuntimeRejectsUnsafePortBindings(
	t *testing.T,
) {
	tests := []struct {
		name       string
		portOutput string
	}{
		{
			name:       "non-loopback address",
			portOutput: "8080/tcp -> 0.0.0.0:12345",
		},
		{
			name: "ambiguous mappings",
			portOutput: "8080/tcp -> 127.0.0.1:12345\n" +
				"8080/tcp -> 127.0.0.1:12346",
		},
		{
			name:       "malformed mapping",
			portOutput: "not-a-valid-binding",
		},
		{
			name:       "missing mapping",
			portOutput: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := newOperationWorkerDeployPodmanTestRequest(t)
			request.ApplicationPort = 8080

			runtime, logPath := newOperationWorkerDeployPodmanPortTestRuntime(
				t,
				request,
				test.portOutput,
			)

			_, err := runtime.Deploy(context.Background(), request)
			if !errors.Is(err, errOperationWorkerDeployRuntimeOutcomeUnknown) {
				t.Fatalf(
					"Deploy() error = %v, want unknown runtime outcome",
					err,
				)
			}

			log := readOperationWorkerDeployPodmanTestLog(t, logPath)
			if countOperationWorkerDeployPodmanTestCommands(log, "run ") != 1 {
				t.Fatal("failed port verification must not create a duplicate container")
			}
		})
	}
}

func TestOperationWorkerDeployPodmanRuntimeRejectsUnhealthyHTTP(
	t *testing.T,
) {
	var requestCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			requestCount.Add(1)
			http.Error(w, "application is not ready", http.StatusServiceUnavailable)
		},
	))
	defer server.Close()

	_, hostPort, err := net.SplitHostPort(
		strings.TrimPrefix(server.URL, "http://"),
	)
	if err != nil {
		t.Fatalf("parse test server address: %v", err)
	}

	runtime := operationWorkerDeployPodmanRuntime{}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		1500*time.Millisecond,
	)
	defer cancel()

	err = runtime.waitForApplicationHTTP(ctx, hostPort)
	if err == nil {
		t.Fatal("HTTP readiness succeeded for an unhealthy application")
	}

	if requestCount.Load() == 0 {
		t.Fatal("readiness test did not issue an HTTP request")
	}
}

func newOperationWorkerDeployPodmanPortTestRuntime(
	t *testing.T,
	request operationWorkerDeployRuntimeRequest,
	portOutput string,
) (
	operationWorkerDeployPodmanRuntime,
	string,
) {
	t.Helper()

	root := t.TempDir()
	podmanPath := filepath.Join(root, "fake-podman")
	statePath := filepath.Join(root, "container-state.json")
	logPath := filepath.Join(root, "commands.log")

	containerJSON := newOperationWorkerDeployPodmanTestContainer(
		request,
		true,
	)

	var containers []operationWorkerDeployPodmanContainer
	if err := json.Unmarshal(containerJSON, &containers); err != nil {
		t.Fatalf("decode fake container state: %v", err)
	}

	if len(containers) != 1 {
		t.Fatalf("fake container count = %d, want 1", len(containers))
	}

	containers[0].Config.Labels[operationWorkerDeployPodmanApplicationPortLabel] =
		strconv.Itoa(request.ApplicationPort)

	containerJSON, err := json.Marshal(containers)
	if err != nil {
		t.Fatalf("encode fake container state: %v", err)
	}

	script := `#!/bin/sh
set -eu

printf '%s\n' "$*" >> "$FAKE_PODMAN_LOG"

case "${1-}" in
  info)
    printf '%s\n' "true"
    exit 0
    ;;
  container)
    case "${2-}" in
      exists)
        if [ -f "$FAKE_PODMAN_STATE" ]; then
          exit 0
        fi
        exit 1
        ;;
      inspect)
        if [ -f "$FAKE_PODMAN_STATE" ]; then
          cat "$FAKE_PODMAN_STATE"
          exit 0
        fi
        exit 1
        ;;
    esac
    ;;
  load)
    exit 0
    ;;
  run|start)
    printf '%s' "$FAKE_PODMAN_STATE_JSON" > "$FAKE_PODMAN_STATE"
    exit 0
    ;;
  port)
    if [ "${3-}" != "$FAKE_PODMAN_APPLICATION_PORT_SPEC" ]; then
      exit 125
    fi
    printf '%s\n' "$FAKE_PODMAN_PORT_OUTPUT"
    exit 0
    ;;
esac

exit 125
`

	if err := os.WriteFile(
		podmanPath,
		[]byte(script),
		0o700,
	); err != nil {
		t.Fatalf("create fake Podman executable: %v", err)
	}

	t.Setenv("FAKE_PODMAN_STATE", statePath)
	t.Setenv("FAKE_PODMAN_LOG", logPath)
	t.Setenv("FAKE_PODMAN_STATE_JSON", string(containerJSON))
	t.Setenv("FAKE_PODMAN_PORT_OUTPUT", portOutput)
	t.Setenv(
		"FAKE_PODMAN_APPLICATION_PORT_SPEC",
		strconv.Itoa(request.ApplicationPort)+"/tcp",
	)

	runtime := operationWorkerDeployPodmanRuntime{
		PodmanPath:     podmanPath,
		CommandTimeout: 5 * time.Second,
	}

	return runtime, logPath
}
