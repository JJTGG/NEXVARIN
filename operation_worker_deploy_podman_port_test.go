package main

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
)

func TestOperationWorkerDeployPodmanRuntimePublishesApplicationPortOnLoopback(
	t *testing.T,
) {
	request := newOperationWorkerDeployPodmanTestRequest(t)
	request.ApplicationPort = 8080

	runtime, _, logPath := newOperationWorkerDeployPodmanTestRuntime(
		t,
		request,
		false,
	)

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
	t.Setenv("FAKE_PODMAN_STATE_JSON", string(containerJSON))

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
