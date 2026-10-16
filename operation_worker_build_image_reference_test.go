package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOperationWorkerBuildImageReference(t *testing.T) {
	const operationID = "123e4567-e89b-12d3-a456-426614174002"
	const want = "localhost/nexvarin/build-123e4567-e89b-12d3-a456-426614174002:latest"

	got, err := operationWorkerBuildImageReference(operationID)
	if err != nil {
		t.Fatalf("generate image reference: %v", err)
	}

	if got != want {
		t.Errorf("image reference = %q, want %q", got, want)
	}
}

func TestOperationWorkerBuildImageReferenceRejectsInvalidID(
	t *testing.T,
) {
	tests := []struct {
		name string
		id   string
	}{
		{
			name: "empty ID",
			id:   "",
		},
		{
			name: "malformed ID",
			id:   "not-a-uuid",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := operationWorkerBuildImageReference(test.id)
			if err == nil {
				t.Fatal("expected invalid operation ID to be rejected")
			}
		})
	}
}

func TestRunOperationWorkerBuildKitNamedPassesImageReference(
	t *testing.T,
) {
	root := t.TempDir()
	sourceArchive := filepath.Join(root, "source.oci.tar")
	outputArchive := filepath.Join(root, "output.oci.tar")

	writeTestOCIArchive(
		t,
		sourceArchive,
		validTestOCIArchiveEntries(),
	)

	const operationID = "123e4567-e89b-12d3-a456-426614174002"

	imageReference, err := operationWorkerBuildImageReference(operationID)
	if err != nil {
		t.Fatalf("generate image reference: %v", err)
	}

	expectedOutput := "type=oci,dest=" + outputArchive +
		",name=" + imageReference

	buildctl := filepath.Join(root, "fake-buildctl")

	script := "#!/bin/sh\nset -eu\noutput=''\n"
	script += "while [ \"$#\" -gt 0 ]; do\n"
	script += "  if [ \"$1\" = \"--output\" ]; then\n"
	script += "    shift\n"
	script += "    output=${1-}\n"
	script += "  fi\n"
	script += "  shift\n"
	script += "done\n"
	script += "test \"$output\" = " +
		shellQuoteBuildExecutorTest(expectedOutput) + "\n"
	script += "cp " +
		shellQuoteBuildExecutorTest(sourceArchive) + " " +
		shellQuoteBuildExecutorTest(outputArchive) + "\n"

	if err := os.WriteFile(buildctl, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake BuildKit client: %v", err)
	}

	sourceDirectory := filepath.Join(root, "source")
	if err := os.Mkdir(sourceDirectory, 0o700); err != nil {
		t.Fatalf("create source directory: %v", err)
	}

	err = runOperationWorkerBuildKitNamed(
		context.Background(),
		buildctl,
		"unix:///run/buildkit/buildkitd.sock",
		sourceDirectory,
		outputArchive,
		5*time.Second,
		imageReference,
	)
	if err != nil {
		t.Fatalf("run fake BuildKit client: %v", err)
	}

	if _, err := inspectOperationWorkerOCIArchive(outputArchive); err != nil {
		t.Fatalf("inspect fake BuildKit output: %v", err)
	}
}

func TestRunOperationWorkerBuildKitNamedRejectsInvalidImageReference(
	t *testing.T,
) {
	err := runOperationWorkerBuildKitNamed(
		context.Background(),
		"/buildctl-must-not-be-invoked",
		"unix:///run/buildkit/buildkitd.sock",
		t.TempDir(),
		filepath.Join(t.TempDir(), "output.oci.tar"),
		time.Second,
		"localhost/nexvarin/build-not-a-uuid:latest",
	)

	if err == nil {
		t.Fatal("expected invalid image reference to be rejected")
	}
}
