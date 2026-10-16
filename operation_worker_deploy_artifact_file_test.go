
package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newDeployArtifactFileFixture(
	t *testing.T,
) (string, operationWorkerDeployArtifact, string) {
	t.Helper()

	root := t.TempDir()

	// Ensure the test directory has private permissions.
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatalf("make artifact root directory private: %v", err)
	}

	digestDirectory := filepath.Join(root, "sha256")
	if err := os.Mkdir(digestDirectory, 0o700); err != nil {
		t.Fatalf("create artifact digest directory: %v", err)
	}

	stagingPath := filepath.Join(root, "staging.oci.tar")
	writeTestOCIArchive(
		t,
		stagingPath,
		validTestOCIArchiveEntries(),
	)

	archiveMetadata, err := inspectOperationWorkerOCIArchive(
		stagingPath,
	)
	if err != nil {
		t.Fatalf("inspect test OCI archive: %v", err)
	}

	digestHex := strings.TrimPrefix(
		archiveMetadata.ArchiveDigest,
		"sha256:",
	)
	artifactPath := filepath.Join(
		digestDirectory,
		digestHex+".oci.tar",
	)

	if err := os.Rename(stagingPath, artifactPath); err != nil {
		t.Fatalf("publish test OCI archive: %v", err)
	}

	artifact := operationWorkerDeployArtifact{
		ArtifactReference: artifactPath,
		ContentDigest:     archiveMetadata.ArchiveDigest,
		Metadata: operationWorkerBuildArtifactMetadata{
			Format:             "oci-archive-tar",
			ContentDigestScope: "complete-archive-file",
			ArchiveDigest:      archiveMetadata.ArchiveDigest,
			OCIIndexDigest:     archiveMetadata.IndexDigest,
		},
	}

	return root, artifact, artifactPath
}

func TestVerifyOperationWorkerDeployArtifactFileAcceptsValidArchive(
	t *testing.T,
) {
	root, artifact, wantPath := newDeployArtifactFileFixture(t)

	gotPath, err := verifyOperationWorkerDeployArtifactFile(
		root,
		artifact,
	)
	if err != nil {
		t.Fatalf("verify valid artifact: %v", err)
	}

	if gotPath != wantPath {
		t.Errorf("verified path = %q, want %q", gotPath, wantPath)
	}
}

func TestVerifyOperationWorkerDeployArtifactFileRejectsInvalidMetadata(
	t *testing.T,
) {
	tests := []struct {
		name   string
		mutate func(
			root string,
			artifact *operationWorkerDeployArtifact,
		)
	}{
		{
			name: "invalid content digest",
			mutate: func(
				_ string,
				artifact *operationWorkerDeployArtifact,
			) {
				artifact.ContentDigest = "sha256:invalid"
			},
		},
		{
			name: "archive digest mismatch",
			mutate: func(
				_ string,
				artifact *operationWorkerDeployArtifact,
			) {
				artifact.Metadata.ArchiveDigest =
					"sha256:" + strings.Repeat("f", 64)
			},
		},
		{
			name: "unexpected artifact path",
			mutate: func(
				root string,
				artifact *operationWorkerDeployArtifact,
			) {
				artifact.ArtifactReference = filepath.Join(
					root,
					"other.oci.tar",
				)
			},
		},
		{
			name: "unsupported artifact format",
			mutate: func(
				_ string,
				artifact *operationWorkerDeployArtifact,
			) {
				artifact.Metadata.Format = "unknown"
			},
		},
		{
			name: "incorrect digest scope",
			mutate: func(
				_ string,
				artifact *operationWorkerDeployArtifact,
			) {
				artifact.Metadata.ContentDigestScope =
					"oci-index-only"
			},
		},
		{
			name: "OCI index digest mismatch",
			mutate: func(
				_ string,
				artifact *operationWorkerDeployArtifact,
			) {
				artifact.Metadata.OCIIndexDigest =
					"sha256:" + strings.Repeat("f", 64)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, artifact, _ := newDeployArtifactFileFixture(t)
			test.mutate(root, &artifact)

			_, err := verifyOperationWorkerDeployArtifactFile(
				root,
				artifact,
			)
			if !errors.Is(
				err,
				errOperationWorkerDeployArtifactFileIntegrity,
			) {
				t.Fatalf(
					"verification error = %v, want integrity error",
					err,
				)
			}
		})
	}
}

func TestVerifyOperationWorkerDeployArtifactFileRejectsMissingFile(
	t *testing.T,
) {
	root, artifact, artifactPath := newDeployArtifactFileFixture(t)

	if err := os.Remove(artifactPath); err != nil {
		t.Fatalf("remove test artifact: %v", err)
	}

	_, err := verifyOperationWorkerDeployArtifactFile(root, artifact)
	if !errors.Is(
		err,
		errOperationWorkerDeployArtifactFileUnavailable,
	) {
		t.Fatalf(
			"verification error = %v, want artifact-unavailable",
			err,
		)
	}
}

func TestVerifyOperationWorkerDeployArtifactFileRejectsMissingDirectory(
	t *testing.T,
) {
	root, artifact, _ := newDeployArtifactFileFixture(t)

	_, err := verifyOperationWorkerDeployArtifactFile(
		filepath.Join(root, "missing"),
		artifact,
	)
	if !errors.Is(
		err,
		errOperationWorkerDeployArtifactFileUnavailable,
	) {
		t.Fatalf(
			"verification error = %v, want artifact-unavailable",
			err,
		)
	}
}

func TestVerifyOperationWorkerDeployArtifactFileRejectsCorruptArchive(
	t *testing.T,
) {
	root, artifact, artifactPath := newDeployArtifactFileFixture(t)

	if err := os.WriteFile(
		artifactPath,
		[]byte("this is not a valid OCI archive"),
		0o600,
	); err != nil {
		t.Fatalf("corrupt test artifact: %v", err)
	}

	_, err := verifyOperationWorkerDeployArtifactFile(root, artifact)
	if !errors.Is(
		err,
		errOperationWorkerDeployArtifactFileIntegrity,
	) {
		t.Fatalf(
			"verification error = %v, want integrity error",
			err,
		)
	}
}

func TestVerifyOperationWorkerDeployArtifactFileRejectsFileSymlink(
	t *testing.T,
) {
	root, artifact, artifactPath := newDeployArtifactFileFixture(t)
	targetPath := filepath.Join(root, "other.oci.tar")

	writeTestOCIArchive(
		t,
		targetPath,
		validTestOCIArchiveEntries(),
	)

	if err := os.Remove(artifactPath); err != nil {
		t.Fatalf("remove original artifact: %v", err)
	}

	if err := os.Symlink(targetPath, artifactPath); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	_, err := verifyOperationWorkerDeployArtifactFile(root, artifact)
	if !errors.Is(
		err,
		errOperationWorkerDeployArtifactFileIntegrity,
	) {
		t.Fatalf(
			"verification error = %v, want integrity error",
			err,
		)
	}
}

func TestVerifyOperationWorkerDeployArtifactFileRejectsDirectorySymlink(
	t *testing.T,
) {
	root, artifact, _ := newDeployArtifactFileFixture(t)

	digestDirectory := filepath.Join(root, "sha256")
	realDirectory := filepath.Join(root, "private-digests")

	if err := os.Rename(digestDirectory, realDirectory); err != nil {
		t.Fatalf("rename digest directory: %v", err)
	}

	if err := os.Symlink(realDirectory, digestDirectory); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	_, err := verifyOperationWorkerDeployArtifactFile(root, artifact)
	if !errors.Is(
		err,
		errOperationWorkerDeployArtifactFileIntegrity,
	) {
		t.Fatalf(
			"verification error = %v, want integrity error",
			err,
		)
	}
}

func TestVerifyOperationWorkerDeployArtifactFileRejectsPermissiveFile(
	t *testing.T,
) {
	root, artifact, artifactPath := newDeployArtifactFileFixture(t)

	if err := os.Chmod(artifactPath, 0o644); err != nil {
		t.Fatalf("change artifact permissions: %v", err)
	}

	_, err := verifyOperationWorkerDeployArtifactFile(root, artifact)
	if !errors.Is(
		err,
		errOperationWorkerDeployArtifactFileIntegrity,
	) {
		t.Fatalf(
			"verification error = %v, want integrity error",
			err,
		)
	}
}

func TestVerifyOperationWorkerDeployArtifactFileRejectsPermissiveDirectory(
	t *testing.T,
) {
	root, artifact, _ := newDeployArtifactFileFixture(t)

	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatalf("change artifact directory permissions: %v", err)
	}

	_, err := verifyOperationWorkerDeployArtifactFile(root, artifact)
	if !errors.Is(
		err,
		errOperationWorkerDeployArtifactFileIntegrity,
	) {
		t.Fatalf(
			"verification error = %v, want integrity error",
			err,
		)
	}
}

func TestVerifyOperationWorkerDeployArtifactFileRejectsPermissiveDigestDirectory(
	t *testing.T,
) {
	root, artifact, _ := newDeployArtifactFileFixture(t)
	digestDirectory := filepath.Join(root, "sha256")

	if err := os.Chmod(digestDirectory, 0o755); err != nil {
		t.Fatalf("change digest directory permissions: %v", err)
	}

	_, err := verifyOperationWorkerDeployArtifactFile(root, artifact)
	if !errors.Is(
		err,
		errOperationWorkerDeployArtifactFileIntegrity,
	) {
		t.Fatalf(
			"verification error = %v, want integrity error",
			err,
		)
	}
}

func TestVerifyOperationWorkerDeployArtifactFileRejectsEmptyDirectory(
	t *testing.T,
) {
	_, artifact, _ := newDeployArtifactFileFixture(t)

	_, err := verifyOperationWorkerDeployArtifactFile(" ", artifact)
	if err == nil {
		t.Fatal("expected empty artifact directory to be rejected")
	}
}
