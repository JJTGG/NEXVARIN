package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var (
	errOperationWorkerDeployArtifactFileUnavailable = errors.New(
		"deployment artifact file is unavailable",
	)

	errOperationWorkerDeployArtifactFileIntegrity = errors.New(
		"deployment artifact file failed integrity validation",
	)
)

// verifyOperationWorkerDeployArtifactFile confirms that the artifact
// stored on disk matches its verified database record.
//
// It restricts access to the expected content-addressed storage path,
// rejects symlinks and non-private files, and revalidates the OCI archive.
//
// This function does not start a container.
func verifyOperationWorkerDeployArtifactFile(
	artifactDirectory string,
	artifact operationWorkerDeployArtifact,
) (string, error) {
	if strings.TrimSpace(artifactDirectory) == "" {
		return "", errors.New(
			"deployment artifact directory is required",
		)
	}

	const digestPrefix = "sha256:"

	if !strings.HasPrefix(
		artifact.ContentDigest,
		digestPrefix,
	) {
		return "", errOperationWorkerDeployArtifactFileIntegrity
	}

	digestHex := strings.TrimPrefix(
		artifact.ContentDigest,
		digestPrefix,
	)

	if len(digestHex) != 64 {
		return "", errOperationWorkerDeployArtifactFileIntegrity
	}

	decodedDigest, err := hex.DecodeString(digestHex)
	if err != nil ||
		len(decodedDigest) != 32 ||
		hex.EncodeToString(decodedDigest) != digestHex {
		return "", errOperationWorkerDeployArtifactFileIntegrity
	}

	if artifact.Metadata.Format != "oci-archive-tar" ||
		artifact.Metadata.ContentDigestScope != "complete-archive-file" ||
		artifact.Metadata.ArchiveDigest != artifact.ContentDigest {
		return "", errOperationWorkerDeployArtifactFileIntegrity
	}

	absoluteDirectory, err := filepath.Abs(
		strings.TrimSpace(artifactDirectory),
	)
	if err != nil {
		return "", fmt.Errorf(
			"resolve deployment artifact directory: %w",
			err,
		)
	}

	directoryInfo, err := os.Lstat(absoluteDirectory)
	if errors.Is(err, os.ErrNotExist) {
		return "", errOperationWorkerDeployArtifactFileUnavailable
	}
	if err != nil {
		return "", fmt.Errorf(
			"inspect deployment artifact directory: %w",
			err,
		)
	}

	if directoryInfo.Mode()&os.ModeSymlink != 0 ||
		!directoryInfo.IsDir() ||
		directoryInfo.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf(
			"%w: artifact directory is not private",
			errOperationWorkerDeployArtifactFileIntegrity,
		)
	}

	digestDirectory := filepath.Join(
		absoluteDirectory,
		"sha256",
	)

	digestDirectoryInfo, err := os.Lstat(digestDirectory)
	if errors.Is(err, os.ErrNotExist) {
		return "", errOperationWorkerDeployArtifactFileUnavailable
	}
	if err != nil {
		return "", fmt.Errorf(
			"inspect artifact digest directory: %w",
			err,
		)
	}

	if digestDirectoryInfo.Mode()&os.ModeSymlink != 0 ||
		!digestDirectoryInfo.IsDir() ||
		digestDirectoryInfo.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf(
			"%w: digest directory is not private",
			errOperationWorkerDeployArtifactFileIntegrity,
		)
	}

	expectedPath := filepath.Join(
		digestDirectory,
		digestHex+".oci.tar",
	)

	if !filepath.IsAbs(artifact.ArtifactReference) ||
		filepath.Clean(artifact.ArtifactReference) != expectedPath {
		return "", fmt.Errorf(
			"%w: artifact reference does not match its content digest",
			errOperationWorkerDeployArtifactFileIntegrity,
		)
	}

	fileInfo, err := os.Lstat(expectedPath)
	if errors.Is(err, os.ErrNotExist) {
		return "", errOperationWorkerDeployArtifactFileUnavailable
	}
	if err != nil {
		return "", fmt.Errorf(
			"inspect deployment artifact file: %w",
			err,
		)
	}

	if fileInfo.Mode()&os.ModeSymlink != 0 ||
		!fileInfo.Mode().IsRegular() ||
		fileInfo.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf(
			"%w: artifact must be a private regular file",
			errOperationWorkerDeployArtifactFileIntegrity,
		)
	}

	actual, err := inspectOperationWorkerOCIArchive(expectedPath)
	if err != nil {
		return "", fmt.Errorf(
			"%w: OCI archive validation failed",
			errOperationWorkerDeployArtifactFileIntegrity,
		)
	}

	if actual.ArchiveDigest != artifact.ContentDigest ||
		actual.IndexDigest != artifact.Metadata.OCIIndexDigest {
		return "", errOperationWorkerDeployArtifactFileIntegrity
	}

	return expectedPath, nil
}
