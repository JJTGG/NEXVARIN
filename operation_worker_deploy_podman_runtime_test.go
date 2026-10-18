package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	operationWorkerDeployPodmanTestOperationID =
		"123e4567-e89b-12d3-a456-426614174010"

	operationWorkerDeployPodmanTestDeploymentID =
		"123e4567-e89b-12d3-a456-426614174011"

	operationWorkerDeployPodmanTestBuildOperationID =
		"123e4567-e89b-12d3-a456-426614174012"

	operationWorkerDeployPodmanTestContainerID =
		"test-container-instance-001"
)

func newOperationWorkerDeployPodmanTestRequest(
	t *testing.T,
) operationWorkerDeployRuntimeRequest {
	t.Helper()

	root := t.TempDir()
	artifactPath := filepath.Join(root, "artifact.oci.tar")

	if err := os.WriteFile(
		artifactPath,
		[]byte("test artifact placeholder"),
		0o600,
	); err != nil {
		t.Fatalf("create test artifact: %v", err)
	}

	imageReference, err := operationWorkerBuildImageReference(
		operationWorkerDeployPodmanTestBuildOperationID,
	)
	if err != nil {
		t.Fatalf("generate test image reference: %v", err)
	}

	return operationWorkerDeployRuntimeRequest{
		OperationID:    operationWorkerDeployPodmanTestOperationID,
		DeploymentID:   operationWorkerDeployPodmanTestDeploymentID,
		ArtifactPath:   artifactPath,
		ArtifactDigest: "sha256:" + strings.Repeat("a", 64),
		ImageReference: imageReference,
	}
}

func newOperationWorkerDeployPodmanTestContainer(
	request operationWorkerDeployRuntimeRequest,
	running bool,
) []byte {
	container := operationWorkerDeployPodmanContainer{
		ID: operationWorkerDeployPodmanTestContainerID,
	}

	container.Config.Labels = map[string]string{
		operationWorkerDeployPodmanOperationLabel:   request.OperationID,
		operationWorkerDeployPodmanDeploymentLabel: request.DeploymentID,
		operationWorkerDeployPodmanDigestLabel:     request.ArtifactDigest,
		operationWorkerDeployPodmanImageLabel:      request.ImageReference,
	}

	container.State.Running = running

	result, err := json.Marshal(
		[]operationWorkerDeployPodmanContainer{container},
	)
	if err != nil {
		panic("cannot encode static Podman test container: " + err.Error())
	}

	return result
}

func newOperationWorkerDeployPodmanTestRuntime(
	t *testing.T,
	request operationWorkerDeployRuntimeRequest,
	existingContainer bool,
) (
	operationWorkerDeployPodmanRuntime,
	string,
	string,
) {
	t.Helper()

	root := t.TempDir()
	podmanPath := filepath.Join(root, "fake-podman")
	statePath := filepath.Join(root, "container-state.json")
	logPath := filepath.Join(root, "commands.log")

	stateJSON := newOperationWorkerDeployPodmanTestContainer(
		request,
		true,
	)

	if existingContainer {
		if err := os.WriteFile(
			statePath,
			stateJSON,
			0o600,
		); err != nil {
			t.Fatalf("create existing fake container state: %v", err)
		}
	}

	script := `#!/bin/sh
set -eu

printf '%s\n' "$*" >> "$FAKE_PODMAN_LOG"

case "${1-}" in
  container)
    case "${2-}" in
      exists)
        if [ -f "$FAKE_PODMAN_STATE" ]; then
          exit 0
        fi
        exit 1
        ;;
      inspect)
        if [ "${FAKE_PODMAN_FAIL_INSPECT:-0}" = "1" ]; then
          exit 125
        fi
        if [ -f "$FAKE_PODMAN_STATE" ]; then
          cat "$FAKE_PODMAN_STATE"
          exit 0
        fi
        exit 1
        ;;
    esac
    ;;
  load)
    if [ "${FAKE_PODMAN_FAIL_LOAD:-0}" = "1" ]; then
      exit 125
    fi
    exit 0
    ;;
  run)
    if [ "${FAKE_PODMAN_CREATE_STATE:-1}" = "1" ]; then
      printf '%s' "$FAKE_PODMAN_STATE_JSON" > "$FAKE_PODMAN_STATE"
    fi
    exit "${FAKE_PODMAN_RUN_EXIT:-0}"
    ;;
  start)
    printf '%s' "$FAKE_PODMAN_STATE_JSON" > "$FAKE_PODMAN_STATE"
    exit "${FAKE_PODMAN_START_EXIT:-0}"
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
	t.Setenv("FAKE_PODMAN_STATE_JSON", string(stateJSON))
	t.Setenv("FAKE_PODMAN_FAIL_INSPECT", "0")
	t.Setenv("FAKE_PODMAN_FAIL_LOAD", "0")
	t.Setenv("FAKE_PODMAN_CREATE_STATE", "1")
	t.Setenv("FAKE_PODMAN_RUN_EXIT", "0")
	t.Setenv("FAKE_PODMAN_START_EXIT", "0")

	runtime := operationWorkerDeployPodmanRuntime{
		PodmanPath:     podmanPath,
		CommandTimeout: 5 * time.Second,
	}

	return runtime, statePath, logPath
}

func readOperationWorkerDeployPodmanTestLog(
	t *testing.T,
	logPath string,
) string {
	t.Helper()

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read fake Podman command log: %v", err)
	}

	return string(data)
}

func countOperationWorkerDeployPodmanTestCommands(
	log string,
	prefix string,
) int {
	count := 0

	for _, line := range strings.Split(
		strings.TrimSpace(log),
		"\n",
	) {
		if strings.HasPrefix(line, prefix) {
			count++
		}
	}

	return count
}

func TestOperationWorkerDeployPodmanRuntimeDeployCreatesIsolatedContainer(
	t *testing.T,
) {
	request := newOperationWorkerDeployPodmanTestRequest(t)

	runtime, _, logPath :=
		newOperationWorkerDeployPodmanTestRuntime(
			t,
			request,
			false,
		)

	result, err := runtime.Deploy(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatalf("deploy using fake Podman: %v", err)
	}

	if result.RuntimeInstanceReference !=
		operationWorkerDeployPodmanTestContainerID {
		t.Fatalf(
			"runtime reference = %q, want %q",
			result.RuntimeInstanceReference,
			operationWorkerDeployPodmanTestContainerID,
		)
	}

	log := readOperationWorkerDeployPodmanTestLog(t, logPath)

	if countOperationWorkerDeployPodmanTestCommands(
		log,
		"load ",
	) != 1 {
		t.Error("expected exactly one image-load command")
	}

	if countOperationWorkerDeployPodmanTestCommands(
		log,
		"run ",
	) != 1 {
		t.Error("expected exactly one container creation command")
	}

	requiredArguments := []string{
		"--detach",
		"--name " + operationWorkerDeployPodmanContainerName(
			request.DeploymentID,
		),
		"--network=none",
		"--cap-drop=ALL",
		"--security-opt=no-new-privileges",
		"--pids-limit=256",
		"--memory=512m",
		"--cpus=1",
		"--pull=never",
		operationWorkerDeployPodmanOperationLabel + "=" + request.OperationID,
		operationWorkerDeployPodmanDeploymentLabel + "=" + request.DeploymentID,
		operationWorkerDeployPodmanDigestLabel + "=" + request.ArtifactDigest,
		operationWorkerDeployPodmanImageLabel + "=" + request.ImageReference,
	}

	for _, argument := range requiredArguments {
		if !strings.Contains(log, argument) {
			t.Errorf(
				"Podman command log does not contain %q",
				argument,
			)
		}
	}

	if strings.Contains(log, "--publish ") ||
		strings.Contains(log, " -p ") {
		t.Error("initial deployment must not publish container ports")
	}
}

func TestOperationWorkerDeployPodmanRuntimeRecoversExistingContainer(
	t *testing.T,
) {
	request := newOperationWorkerDeployPodmanTestRequest(t)

	runtime, _, logPath :=
		newOperationWorkerDeployPodmanTestRuntime(
			t,
			request,
			true,
		)

	result, err := runtime.Deploy(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatalf("recover existing container: %v", err)
	}

	if result.RuntimeInstanceReference !=
		operationWorkerDeployPodmanTestContainerID {
		t.Errorf(
			"runtime reference = %q, want %q",
			result.RuntimeInstanceReference,
			operationWorkerDeployPodmanTestContainerID,
		)
	}

	log := readOperationWorkerDeployPodmanTestLog(t, logPath)

	if countOperationWorkerDeployPodmanTestCommands(
		log,
		"load ",
	) != 0 {
		t.Error("recovery must not reload an already running container's image")
	}

	if countOperationWorkerDeployPodmanTestCommands(
		log,
		"run ",
	) != 0 {
		t.Error("recovery must not create a duplicate container")
	}
}

func TestOperationWorkerDeployPodmanRuntimeRejectsContainerIdentityMismatch(
	t *testing.T,
) {
	request := newOperationWorkerDeployPodmanTestRequest(t)

	runtime, _, logPath :=
		newOperationWorkerDeployPodmanTestRuntime(
			t,
			request,
			true,
		)

	// The name exists, but the persisted labels identify a different
	// operation. The runtime must not reuse or start that container.
	foreignRequest := request
	foreignRequest.OperationID = "123e4567-e89b-12d3-a456-426614174099"

	if err := os.WriteFile(
		os.Getenv("FAKE_PODMAN_STATE"),
		newOperationWorkerDeployPodmanTestContainer(
			foreignRequest,
			false,
		),
		0o600,
	); err != nil {
		t.Fatalf("write mismatched container state: %v", err)
	}

	_, err := runtime.Deploy(
		context.Background(),
		request,
	)
	if !errors.Is(
		err,
		errOperationWorkerDeployRuntimeOutcomeUnknown,
	) {
		t.Fatalf(
			"error = %v, want uncertain deployment outcome",
			err,
		)
	}

	log := readOperationWorkerDeployPodmanTestLog(t, logPath)

	if countOperationWorkerDeployPodmanTestCommands(
		log,
		"start ",
	) != 0 {
		t.Error("runtime must not start a container with mismatched identity")
	}

	if countOperationWorkerDeployPodmanTestCommands(
		log,
		"run ",
	) != 0 {
		t.Error("runtime must not create a replacement after an identity conflict")
	}
}

func TestOperationWorkerDeployPodmanRuntimeRetriesKnownCreationFailure(
	t *testing.T,
) {
	request := newOperationWorkerDeployPodmanTestRequest(t)

	runtime, statePath, _ :=
		newOperationWorkerDeployPodmanTestRuntime(
			t,
			request,
			false,
		)

	t.Setenv("FAKE_PODMAN_CREATE_STATE", "0")
	t.Setenv("FAKE_PODMAN_RUN_EXIT", "125")

	_, err := runtime.Deploy(
		context.Background(),
		request,
	)
	if err == nil {
		t.Fatal("expected container creation failure")
	}

	if errors.Is(
		err,
		errOperationWorkerDeployRuntimeOutcomeUnknown,
	) {
		t.Fatalf(
			"known failure without a created container was classified as uncertain: %v",
			err,
		)
	}

	if _, err := os.Stat(statePath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf(
			"container state exists after known failure; stat error = %v",
			err,
		)
	}
}

func TestOperationWorkerDeployPodmanRuntimeReconcilesUnknownCreationOutcome(
	t *testing.T,
) {
	request := newOperationWorkerDeployPodmanTestRequest(t)

	runtime, statePath, _ :=
		newOperationWorkerDeployPodmanTestRuntime(
			t,
			request,
			false,
		)

	// The create command reports failure after writing a possible
	// container state, and inspection cannot establish what happened.
	t.Setenv("FAKE_PODMAN_RUN_EXIT", "125")
	t.Setenv("FAKE_PODMAN_FAIL_INSPECT", "1")

	_, err := runtime.Deploy(
		context.Background(),
		request,
	)
	if !errors.Is(
		err,
		errOperationWorkerDeployRuntimeOutcomeUnknown,
	) {
		t.Fatalf(
			"error = %v, want uncertain deployment outcome",
			err,
		)
	}

	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf(
			"fake runtime did not simulate a possible side effect: %v",
			err,
		)
	}
}

func TestOperationWorkerDeployPodmanRuntimeRejectsInvalidRequestBeforeCommands(
	t *testing.T,
) {
	tests := []struct {
		name   string
		mutate func(*operationWorkerDeployRuntimeRequest)
	}{
		{
			name: "invalid artifact digest",
			mutate: func(request *operationWorkerDeployRuntimeRequest) {
				request.ArtifactDigest = "sha256:invalid"
			},
		},
		{
			name: "invalid image reference",
			mutate: func(request *operationWorkerDeployRuntimeRequest) {
				request.ImageReference = "not-an-image"
			},
		},
		{
			name: "relative artifact path",
			mutate: func(request *operationWorkerDeployRuntimeRequest) {
				request.ArtifactPath = "artifact.oci.tar"
			},
		},
		{
			name: "invalid deployment ID",
			mutate: func(request *operationWorkerDeployRuntimeRequest) {
				request.DeploymentID = "not-a-uuid"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := newOperationWorkerDeployPodmanTestRequest(t)

			runtime, _, logPath :=
				newOperationWorkerDeployPodmanTestRuntime(
					t,
					request,
					false,
				)

			test.mutate(&request)

			_, err := runtime.Deploy(
				context.Background(),
				request,
			)
			if err == nil {
				t.Fatal("expected invalid deployment request to be rejected")
			}

			if _, err := os.Stat(logPath); !errors.Is(err, os.ErrNotExist) {
				t.Errorf(
					"Podman was invoked before request validation; stat error = %v",
					err,
				)
			}
		})
	}
}

func TestValidOperationWorkerDeployPodmanImageReference(
	t *testing.T,
) {
	imageReference, err := operationWorkerBuildImageReference(
		operationWorkerDeployPodmanTestBuildOperationID,
	)
	if err != nil {
		t.Fatalf("generate test image reference: %v", err)
	}

	if !validOperationWorkerDeployPodmanImageReference(
		imageReference,
	) {
		t.Fatalf(
			"expected generated image reference %q to be valid",
			imageReference,
		)
	}

	invalidReferences := []string{
		"",
		"not-an-image",
		"localhost/nexvarin/build-not-a-uuid:latest",
		"localhost/other/build-" +
			operationWorkerDeployPodmanTestBuildOperationID +
			":latest",
	}

	for _, value := range invalidReferences {
		if validOperationWorkerDeployPodmanImageReference(value) {
			t.Errorf(
				"invalid image reference %q was accepted",
				value,
			)
		}
	}
}

func TestValidOperationWorkerDeployPodmanDigest(
	t *testing.T,
) {
	valid := "sha256:" + strings.Repeat("a", 64)

	if !validOperationWorkerDeployPodmanDigest(valid) {
		t.Fatal("expected valid SHA-256 digest to be accepted")
	}

	invalidDigests := []string{
		"",
		"sha256:invalid",
		"sha256:" + strings.Repeat("A", 64),
		"md5:" + strings.Repeat("a", 64),
	}

	for _, value := range invalidDigests {
		if validOperationWorkerDeployPodmanDigest(value) {
			t.Errorf("invalid digest %q was accepted", value)
		}
	}
}
