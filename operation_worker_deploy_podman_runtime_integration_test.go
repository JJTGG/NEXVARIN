package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	operationWorkerDeployPodmanIntegrationOperationID = "123e4567-e89b-12d3-a456-426614174020"

	operationWorkerDeployPodmanIntegrationDeploymentID = "123e4567-e89b-12d3-a456-426614174021"

	operationWorkerDeployPodmanIntegrationBuildID = "123e4567-e89b-12d3-a456-426614174022"

	operationWorkerDeployPodmanIntegrationBaseImage = "docker.io/library/alpine:3.22"
)

func TestOperationWorkerDeployPodmanRuntimeLiveRootlessIntegration(
	t *testing.T,
) {
	if os.Getenv("NEXVARIN_PODMAN_INTEGRATION") != "1" {
		t.Skip("set NEXVARIN_PODMAN_INTEGRATION=1 to run live Podman integration")
	}

	podmanPath := strings.TrimSpace(
		os.Getenv("NEXVARIN_PODMAN_PATH"),
	)
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
		12*time.Minute,
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
	artifactPath := filepath.Join(root, "artifact.oci.tar")

	dockerfile := []byte(
		"FROM " + operationWorkerDeployPodmanIntegrationBaseImage + "\n" +
			"CMD [\"sleep\", \"300\"]\n",
	)

	if err := os.WriteFile(
		dockerfilePath,
		dockerfile,
		0o600,
	); err != nil {
		t.Fatalf("write integration Dockerfile: %v", err)
	}

	imageReference, err := operationWorkerBuildImageReference(
		operationWorkerDeployPodmanIntegrationBuildID,
	)
	if err != nil {
		t.Fatalf("create integration image reference: %v", err)
	}

	containerName := operationWorkerDeployPodmanContainerName(
		operationWorkerDeployPodmanIntegrationDeploymentID,
	)

	// Always attempt cleanup, including after an uncertain runtime result.
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
			"pull integration base image: %v; output: %s",
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
			"build integration image: %v; output: %s",
			err,
			strings.TrimSpace(string(output)),
		)
	}

	if output, err := runOperationWorkerDeployPodmanIntegrationCommand(
		ctx,
		podmanPath,
		"save",
		"--format",
		"oci-archive",
		"--output",
		artifactPath,
		imageReference,
	); err != nil {
		t.Fatalf(
			"export integration OCI archive: %v; output: %s",
			err,
			strings.TrimSpace(string(output)),
		)
	}

	if err := os.Chmod(artifactPath, 0o600); err != nil {
		t.Fatalf("secure integration artifact file: %v", err)
	}

	artifactDigest, err :=
		digestOperationWorkerDeployPodmanIntegrationArtifact(
			artifactPath,
		)
	if err != nil {
		t.Fatalf("calculate OCI archive digest: %v", err)
	}

	// Remove the tag so Deploy must really import the OCI archive.
	if output, err := runOperationWorkerDeployPodmanIntegrationCommand(
		ctx,
		podmanPath,
		"image",
		"rm",
		"--force",
		imageReference,
	); err != nil {
		t.Fatalf(
			"remove locally tagged integration image before import: %v; output: %s",
			err,
			strings.TrimSpace(string(output)),
		)
	}

	runtime := operationWorkerDeployPodmanRuntime{
		PodmanPath:     podmanPath,
		CommandTimeout: 2 * time.Minute,
	}

	request := operationWorkerDeployRuntimeRequest{
		OperationID:    operationWorkerDeployPodmanIntegrationOperationID,
		DeploymentID:   operationWorkerDeployPodmanIntegrationDeploymentID,
		ArtifactPath:   artifactPath,
		ArtifactDigest: artifactDigest,
		ImageReference: imageReference,
	}

	firstResult, err := runtime.Deploy(ctx, request)
	if err != nil {
		t.Fatalf("deploy with actual rootless Podman: %v", err)
	}

	if !validDeployRuntimeInstanceReference(
		firstResult.RuntimeInstanceReference,
	) {
		t.Fatalf(
			"invalid runtime instance reference: %q",
			firstResult.RuntimeInstanceReference,
		)
	}

	secondResult, err := runtime.Deploy(ctx, request)
	if err != nil {
		t.Fatalf("recover deployment with actual rootless Podman: %v", err)
	}

	if secondResult.RuntimeInstanceReference !=
		firstResult.RuntimeInstanceReference {
		t.Fatalf(
			"retry returned container %q; first deployment returned %q",
			secondResult.RuntimeInstanceReference,
			firstResult.RuntimeInstanceReference,
		)
	}

	runningOutput, err :=
		runOperationWorkerDeployPodmanIntegrationCommand(
			ctx,
			podmanPath,
			"container",
			"inspect",
			"--format",
			"{{.State.Running}}",
			containerName,
		)
	if err != nil {
		t.Fatalf(
			"inspect integration container: %v; output: %s",
			err,
			strings.TrimSpace(string(runningOutput)),
		)
	}

	if strings.TrimSpace(string(runningOutput)) != "true" {
		t.Fatalf(
			"integration container is not running: %q",
			strings.TrimSpace(string(runningOutput)),
		)
	}

	t.Logf(
		"live rootless Podman deployment and retry recovery succeeded for container %s",
		firstResult.RuntimeInstanceReference,
	)
}

func runOperationWorkerDeployPodmanIntegrationCommand(
	ctx context.Context,
	podmanPath string,
	arguments ...string,
) ([]byte, error) {
	command := exec.CommandContext(
		ctx,
		podmanPath,
		arguments...,
	)

	return command.CombinedOutput()
}

func digestOperationWorkerDeployPodmanIntegrationArtifact(
	path string,
) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open artifact: %w", err)
	}
	defer file.Close()

	hash := sha256.New()

	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("hash artifact: %w", err)
	}

	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}
