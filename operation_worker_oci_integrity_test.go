package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestInspectOperationWorkerOCIArchiveRejectsCorruptManifestBlob(
	t *testing.T,
) {
	entries := validTestOCIArchiveEntries()

	// Keep the original digest-based filename, but corrupt the manifest.
	manifestPath := entries[2].name
	entries = replaceTestOCIArchiveEntry(
		entries,
		manifestPath,
		[]byte(`{"schemaVersion":2,"config":{},"layers":[]}`),
	)

	archivePath := filepath.Join(t.TempDir(), "corrupt-manifest.oci.tar")
	writeTestOCIArchive(t, archivePath, entries)

	_, err := inspectOperationWorkerOCIArchive(archivePath)
	if err == nil {
		t.Fatal("expected corrupted manifest blob to be rejected")
	}
}

func TestInspectOperationWorkerOCIArchiveRejectsMissingConfigBlob(
	t *testing.T,
) {
	entries := validTestOCIArchiveEntries()

	// Remove the configuration blob referenced by the image manifest.
	configPath := entries[3].name
	entries = removeTestOCIArchiveEntry(entries, configPath)

	archivePath := filepath.Join(t.TempDir(), "missing-config.oci.tar")
	writeTestOCIArchive(t, archivePath, entries)

	_, err := inspectOperationWorkerOCIArchive(archivePath)
	if err == nil {
		t.Fatal("expected missing configuration blob to be rejected")
	}
}

func TestInspectOperationWorkerOCIArchiveRejectsMismatchedConfigSize(
	t *testing.T,
) {
	entries := cloneTestOCIArchiveEntries(
		validTestOCIArchiveEntries(),
	)

	// Falsify the config size in the manifest.
	var manifest operationWorkerOCIManifestDocument
	if err := json.Unmarshal(entries[2].content, &manifest); err != nil {
		t.Fatalf("decode fixture manifest: %v", err)
	}

	manifest.Config.Size++

	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("encode modified manifest: %v", err)
	}

	manifestHash := sha256.Sum256(manifestBytes)
	manifestHexDigest := hex.EncodeToString(manifestHash[:])

	entries[2] = testOCIArchiveEntry{
		name:    "blobs/sha256/" + manifestHexDigest,
		content: manifestBytes,
	}

	// Keep the index consistent with the modified manifest. This ensures
	// the test reaches the incorrect config-size check.
	var index operationWorkerOCIIndexDocument
	if err := json.Unmarshal(entries[1].content, &index); err != nil {
		t.Fatalf("decode fixture index: %v", err)
	}

	if len(index.Manifests) != 1 {
		t.Fatalf(
			"fixture manifest count = %d, want 1",
			len(index.Manifests),
		)
	}

	index.Manifests[0].Digest = "sha256:" + manifestHexDigest
	index.Manifests[0].Size = int64(len(manifestBytes))

	indexBytes, err := json.Marshal(index)
	if err != nil {
		t.Fatalf("encode modified index: %v", err)
	}
	entries[1].content = indexBytes

	archivePath := filepath.Join(t.TempDir(), "wrong-config-size.oci.tar")
	writeTestOCIArchive(t, archivePath, entries)

	_, err = inspectOperationWorkerOCIArchive(archivePath)
	if err == nil {
		t.Fatal("expected mismatched configuration size to be rejected")
	}
}

func TestInspectOperationWorkerOCIArchiveAcceptsValidFixture(
	t *testing.T,
) {
	archivePath := filepath.Join(t.TempDir(), "valid.oci.tar")
	writeTestOCIArchive(t, archivePath, validTestOCIArchiveEntries())

	metadata, err := inspectOperationWorkerOCIArchive(archivePath)
	if err != nil {
		t.Fatalf("inspect valid fixture: %v", err)
	}

	if metadata.ArchiveDigest == "" {
		t.Error("archive digest is empty")
	}

	if metadata.IndexDigest == "" {
		t.Error("index digest is empty")
	}

	if metadata.Size <= 0 {
		t.Errorf("archive size = %d, want positive size", metadata.Size)
	}
}
