package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const operationWorkerDeployPodmanNetworkIntegrationBuildID = "123e4567-e89b-12d3-a456-426614174025"

func TestOperationWorkerPodmanLoopbackHTTPReadinessIntegration(
	t *testing.T,
) {
	if os.Getenv("NEXVARIN_PODMAN_INTEGRATION") != "1" {
		t.Skip("set NEXVARIN_PODMAN_INTEGRATION=1 to run live Podman integration")
	}

	podmanPath := strings.TrimSpace(os.Getenv("NEXVARIN_PODMAN_PATH"))
	if podmanPath == "" {
		podmanPath = "podman"
	}

	resolvedPath, err := exec.LookPath(podmanPath)
	if err != nil {
		t.Fatalf("locate Podman executable: %v", err)
	}

	podmanPath, err = filepath.Abs(resolvedPath)
	if err != nil {
		t.Fatalf("resolve Podman executable path: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		6*time.Minute,
	)
	defer cancel()

	rootlessOutput, err := runOperationWorkerDeployPodmanIntegrationCommand(
		ctx,
		podmanPath,
		"info",
		"--format",
		"{{.Host.Security.Rootless}}",
	)
	if err != nil {
		t.Fatalf(
			"query actual Podman rootless mode: %v; output: %s",
			err,
			strings.TrimSpace(string(rootlessOutput)),
		)
	}

	if strings.TrimSpace(string(rootlessOutput)) != "true" {
		t.Fatalf(
			"Podman is not running rootlessly: %q",
			strings.TrimSpace(string(rootlessOutput)),
		)
	}

	root := t.TempDir()
	dockerfilePath := filepath.Join(root, "Dockerfile")

	dockerfile := []byte(
		"FROM " + operationWorkerDeployPodmanIntegrationBaseImage + "\n" +
			"RUN apk add --no-cache busybox-extras && mkdir -p /srv && printf 'nexvarin-ready\\n' > /srv/index.html\n" +
			"EXPOSE 8080/tcp\n" +
			"CMD [\"/bin/busybox-extras\", \"httpd\", \"-f\", \"-p\", \"8080\", \"-h\", \"/srv\"]\n",
	)

	if err := os.WriteFile(
		dockerfilePath,
		dockerfile,
		0o600,
	); err != nil {
		t.Fatalf("write readiness integration Dockerfile: %v", err)
	}

	imageReference, err := operationWorkerBuildImageReference(
		operationWorkerDeployPodmanNetworkIntegrationBuildID,
	)
	if err != nil {
		t.Fatalf("create readiness integration image reference: %v", err)
	}

	containerName := "nexvarin-loopback-probe-" +
		strconv.Itoa(os.Getpid())

	defer func() {
		cleanupContext, cancelCleanup := context.WithTimeout(
			context.Background(),
			30*time.Second,
		)
		defer cancelCleanup()

		_, _ = runOperationWorkerDeployPodmanIntegrationCommand(
			cleanupContext,
			podmanPath,
			"container",
			"rm",
			"--force",
			containerName,
		)

		_, _ = runOperationWorkerDeployPodmanIntegrationCommand(
			cleanupContext,
			podmanPath,
			"image",
			"rm",
			"--force",
			imageReference,
		)
	}()

	if output, err := runOperationWorkerDeployPodmanIntegrationCommand(
		ctx,
		podmanPath,
		"pull",
		operationWorkerDeployPodmanIntegrationBaseImage,
	); err != nil {
		t.Fatalf(
			"pull readiness integration base image: %v; output: %s",
			err,
			strings.TrimSpace(string(output)),
		)
	}

	if output, err := runOperationWorkerDeployPodmanIntegrationCommand(
		ctx,
		podmanPath,
		"build",
		"--pull=never",
		"--file",
		dockerfilePath,
		"--tag",
		imageReference,
		root,
	); err != nil {
		t.Fatalf(
			"build readiness integration image: %v; output: %s",
			err,
			strings.TrimSpace(string(output)),
		)
	}

	if output, err := runOperationWorkerDeployPodmanIntegrationCommand(
		ctx,
		podmanPath,
		"run",
		"--detach",
		"--name",
		containerName,
		"--network",
		"slirp4netns",
		"--publish",
		"127.0.0.1::8080/tcp",
		"--pull=never",
		"--cap-drop=ALL",
		"--security-opt=no-new-privileges",
		"--pids-limit=256",
		"--memory=512m",
		"--cpus=1",
		imageReference,
	); err != nil {
		t.Fatalf(
			"start HTTP probe container with rootless networking: %v; output: %s",
			err,
			strings.TrimSpace(string(output)),
		)
	}

	portOutput, err := runOperationWorkerDeployPodmanIntegrationCommand(
		ctx,
		podmanPath,
		"port",
		containerName,
		"8080/tcp",
	)
	if err != nil {
		t.Fatalf(
			"inspect published container port: %v; output: %s",
			err,
			strings.TrimSpace(string(portOutput)),
		)
	}

	binding := ""
	for _, line := range strings.Split(string(portOutput), "\n") {
		index := strings.LastIndex(line, "127.0.0.1:")
		if index >= 0 {
			binding = strings.TrimSpace(line[index:])
			break
		}
	}

	if binding == "" {
		stateOutput, stateErr := runOperationWorkerDeployPodmanIntegrationCommand(
			ctx,
			podmanPath,
			"container",
			"inspect",
			"--format",
			"{{.State.Status}}",
			containerName,
		)

		mappingsOutput, mappingsErr := runOperationWorkerDeployPodmanIntegrationCommand(
			ctx,
			podmanPath,
			"container",
			"inspect",
			"--format",
			"{{json .NetworkSettings.Ports}}",
			containerName,
		)

		logsOutput, logsErr := runOperationWorkerDeployPodmanIntegrationCommand(
			ctx,
			podmanPath,
			"logs",
			containerName,
		)

		t.Fatalf(
			"expected a published 127.0.0.1 binding; port_output=%q container_state=%q state_error=%v port_mappings=%q mappings_error=%v container_logs=%q logs_error=%v",
			strings.TrimSpace(string(portOutput)),
			strings.TrimSpace(string(stateOutput)),
			stateErr,
			strings.TrimSpace(string(mappingsOutput)),
			mappingsErr,
			strings.TrimSpace(string(logsOutput)),
			logsErr,
		)
	}

	host, hostPort, err := net.SplitHostPort(binding)
	if err != nil {
		t.Fatalf("parse Podman port binding %q: %v", binding, err)
	}

	if host != "127.0.0.1" {
		t.Fatalf("published port host = %q, want 127.0.0.1", host)
	}

	portNumber, err := strconv.Atoi(hostPort)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		t.Fatalf("Podman returned an invalid host port: %q", hostPort)
	}

	probeURL := "http://" + net.JoinHostPort(host, hostPort) + "/"
	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	probeDeadline := time.Now().Add(20 * time.Second)
	lastResult := "the HTTP server did not become ready"

	for time.Now().Before(probeDeadline) {
		request, err := http.NewRequestWithContext(
			ctx,
			http.MethodGet,
			probeURL,
			nil,
		)
		if err != nil {
			t.Fatalf("create HTTP readiness request: %v", err)
		}

		response, requestErr := client.Do(request)
		if requestErr == nil {
			body, readErr := io.ReadAll(
				io.LimitReader(response.Body, 1024),
			)
			closeErr := response.Body.Close()

			if readErr == nil &&
				closeErr == nil &&
				response.StatusCode == http.StatusOK &&
				strings.TrimSpace(string(body)) == "nexvarin-ready" {
				t.Logf(
					"rootless Podman loopback HTTP readiness succeeded at %s",
					probeURL,
				)
				return
			}

			lastResult = fmt.Sprintf(
				"status=%d body=%q read_error=%v close_error=%v",
				response.StatusCode,
				strings.TrimSpace(string(body)),
				readErr,
				closeErr,
			)
		} else {
			lastResult = requestErr.Error()
		}

		select {
		case <-ctx.Done():
			t.Fatalf(
				"HTTP readiness probe context ended: %v; last result: %s",
				ctx.Err(),
				lastResult,
			)
		case <-time.After(250 * time.Millisecond):
		}
	}

	t.Fatalf(
		"HTTP readiness probe failed at %s: %s",
		probeURL,
		lastResult,
	)
}
