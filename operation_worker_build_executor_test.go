package main

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type testOCIArchiveEntry struct {
	name    string
	content []byte
}

func validTestOCIArchiveEntries() []testOCIArchiveEntry {
	config := []byte(
		`{"architecture":"amd64","os":"linux","rootfs":{"type":"layers","diff_ids":[]},"config":{}}`,
	)
	configHash := sha256.Sum256(config)
	configDigest := hex.EncodeToString(configHash[:])

	manifest := []byte(fmt.Sprintf(
		`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:%s","size":%d},"layers":[]}`,
		configDigest,
		len(config),
	))
	manifestHash := sha256.Sum256(manifest)
	manifestDigest := hex.EncodeToString(manifestHash[:])

	index := []byte(fmt.Sprintf(
		`{"schemaVersion":2,"manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:%s","size":%d}]}`,
		manifestDigest,
		len(manifest),
	))

	return []testOCIArchiveEntry{
		{
			name:    "oci-layout",
			content: []byte(`{"imageLayoutVersion":"1.0.0"}`),
		},
		{
			name:    "index.json",
			content: index,
		},
		{
			name:    "blobs/sha256/" + manifestDigest,
			content: manifest,
		},
		{
			name:    "blobs/sha256/" + configDigest,
			content: config,
		},
	}
}

func cloneTestOCIArchiveEntries(
	entries []testOCIArchiveEntry,
) []testOCIArchiveEntry {
	cloned := make([]testOCIArchiveEntry, len(entries))

	for i, entry := range entries {
		cloned[i] = testOCIArchiveEntry{
			name:    entry.name,
			content: append([]byte(nil), entry.content...),
		}
	}

	return cloned
}

func removeTestOCIArchiveEntry(
	entries []testOCIArchiveEntry,
	name string,
) []testOCIArchiveEntry {
	filtered := make([]testOCIArchiveEntry, 0, len(entries))

	for _, entry := range entries {
		if entry.name != name {
			filtered = append(filtered, entry)
		}
	}

	return filtered
}

func replaceTestOCIArchiveEntry(
	entries []testOCIArchiveEntry,
	name string,
	content []byte,
) []testOCIArchiveEntry {
	replaced := cloneTestOCIArchiveEntries(entries)

	for i := range replaced {
		if replaced[i].name == name {
			replaced[i].content = append([]byte(nil), content...)
		}
	}

	return replaced
}

func writeTestOCIArchive(
	t *testing.T,
	archivePath string,
	entries []testOCIArchiveEntry,
) {
	t.Helper()

	file, err := os.OpenFile(
		archivePath,
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		0o600,
	)
	if err != nil {
		t.Fatalf("create test OCI archive: %v", err)
	}

	writer := tar.NewWriter(file)

	for _, entry := range entries {
		header := &tar.Header{
			Name:     entry.name,
			Mode:     0o600,
			Size:     int64(len(entry.content)),
			ModTime:  time.Unix(0, 0).UTC(),
			Typeflag: tar.TypeReg,
			Format:   tar.FormatUSTAR,
		}

		if err := writer.WriteHeader(header); err != nil {
			_ = writer.Close()
			_ = file.Close()
			t.Fatalf("write test OCI header %q: %v", entry.name, err)
		}

		if _, err := writer.Write(entry.content); err != nil {
			_ = writer.Close()
			_ = file.Close()
			t.Fatalf("write test OCI entry %q: %v", entry.name, err)
		}
	}

	if err := writer.Close(); err != nil {
		_ = file.Close()
		t.Fatalf("finish test OCI archive: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("close test OCI archive: %v", err)
	}
}

func TestValidateOperationWorkerBuildkitAddress(t *testing.T) {
	tests := []struct {
		name    string
		address string
		wantErr bool
	}{
		{
			name:    "absolute local Unix socket",
			address: "unix:///run/buildkit/buildkitd.sock",
		},
		{
			name:    "empty address",
			address: "",
			wantErr: true,
		},
		{
			name:    "TCP is not enabled",
			address: "tcp://127.0.0.1:1234",
			wantErr: true,
		},
		{
			name:    "HTTPS is not enabled",
			address: "https://builder.example.com",
			wantErr: true,
		},
		{
			name:    "Unix socket with a remote host",
			address: "unix://builder.example.com/run/buildkit.sock",
			wantErr: true,
		},
		{
			name:    "relative Unix path",
			address: "unix:relative.sock",
			wantErr: true,
		},
		{
			name:    "query string",
			address: "unix:///run/buildkit.sock?token=secret",
			wantErr: true,
		},
		{
			name:    "fragment",
			address: "unix:///run/buildkit.sock#builder",
			wantErr: true,
		},
		{
			name:    "malformed URL",
			address: "unix://[",
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := validateOperationWorkerBuildkitAddress(
				test.address,
			)

			if test.wantErr {
				if err == nil {
					t.Fatal("expected address to be rejected")
				}
				return
			}

			if err != nil {
				t.Fatalf("validate BuildKit address: %v", err)
			}

			if got != strings.TrimSpace(test.address) {
				t.Errorf(
					"address = %q, want %q",
					got,
					strings.TrimSpace(test.address),
				)
			}
		})
	}
}

func TestPrepareOperationWorkerArtifactDirectory(t *testing.T) {
	t.Run("creates private directory", func(t *testing.T) {
		requestedPath := filepath.Join(t.TempDir(), "artifacts")

		got, err := prepareOperationWorkerArtifactDirectory(
			requestedPath,
		)
		if err != nil {
			t.Fatalf("prepare artifact directory: %v", err)
		}

		if got != requestedPath {
			t.Errorf("path = %q, want %q", got, requestedPath)
		}

		info, err := os.Stat(got)
		if err != nil {
			t.Fatalf("stat artifact directory: %v", err)
		}

		if !info.IsDir() {
			t.Fatal("artifact path is not a directory")
		}

		if info.Mode().Perm()&0o077 != 0 {
			t.Errorf(
				"artifact directory permissions = %04o, want no group/world access",
				info.Mode().Perm(),
			)
		}
	})

	t.Run("rejects empty path", func(t *testing.T) {
		if _, err := prepareOperationWorkerArtifactDirectory(" "); err == nil {
			t.Fatal("expected empty path to be rejected")
		}
	})

	t.Run("rejects an existing non-private directory", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "public-artifacts")
		if err := os.Mkdir(directory, 0o755); err != nil {
			t.Fatalf("create non-private directory: %v", err)
		}

		if _, err := prepareOperationWorkerArtifactDirectory(directory); err == nil {
			t.Fatal("expected non-private directory to be rejected")
		}
	})

	t.Run("rejects directory symlink", func(t *testing.T) {
		root := t.TempDir()
		target := filepath.Join(root, "target")
		link := filepath.Join(root, "link")

		if err := os.Mkdir(target, 0o700); err != nil {
			t.Fatalf("create target: %v", err)
		}

		if err := os.Symlink(target, link); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}

		if _, err := prepareOperationWorkerArtifactDirectory(link); err == nil {
			t.Fatal("expected symlinked artifact directory to be rejected")
		}
	})
}

func TestInspectOperationWorkerOCIArchive(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "valid.oci.tar")
	entries := validTestOCIArchiveEntries()

	writeTestOCIArchive(t, archivePath, entries)

	got, err := inspectOperationWorkerOCIArchive(archivePath)
	if err != nil {
		t.Fatalf("inspect valid OCI archive: %v", err)
	}

	content, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}

	archiveHash := sha256.Sum256(content)
	wantArchiveDigest := "sha256:" + hex.EncodeToString(archiveHash[:])

	if got.ArchiveDigest != wantArchiveDigest {
		t.Errorf(
			"archive digest = %q, want %q",
			got.ArchiveDigest,
			wantArchiveDigest,
		)
	}

	indexHash := sha256.Sum256(entries[1].content)
	wantIndexDigest := "sha256:" + hex.EncodeToString(indexHash[:])

	if got.IndexDigest != wantIndexDigest {
		t.Errorf(
			"index digest = %q, want %q",
			got.IndexDigest,
			wantIndexDigest,
		)
	}

	if got.Size != int64(len(content)) {
		t.Errorf("archive size = %d, want %d", got.Size, len(content))
	}
}

func TestInspectOperationWorkerOCIArchiveRejectsInvalidArchives(
	t *testing.T,
) {
	base := validTestOCIArchiveEntries()

	tests := []struct {
		name    string
		entries []testOCIArchiveEntry
	}{
		{
			name: "missing layout",
			entries: removeTestOCIArchiveEntry(
				base,
				"oci-layout",
			),
		},
		{
			name: "missing index",
			entries: removeTestOCIArchiveEntry(
				base,
				"index.json",
			),
		},
		{
			name: "missing manifest blob",
			entries: removeTestOCIArchiveEntry(
				base,
				base[2].name,
			),
		},
		{
			name: "unsupported layout version",
			entries: replaceTestOCIArchiveEntry(
				base,
				"oci-layout",
				[]byte(`{"imageLayoutVersion":"9.9.9"}`),
			),
		},
		{
			name: "malformed index",
			entries: replaceTestOCIArchiveEntry(
				base,
				"index.json",
				[]byte(`not valid JSON`),
			),
		},
		{
			name: "empty manifest list",
			entries: replaceTestOCIArchiveEntry(
				base,
				"index.json",
				[]byte(`{"schemaVersion":2,"manifests":[]}`),
			),
		},
		{
			name: "duplicate entry",
			entries: append(
				cloneTestOCIArchiveEntries(base),
				base[1],
			),
		},
		{
			name: "path traversal",
			entries: append(
				cloneTestOCIArchiveEntries(base),
				testOCIArchiveEntry{
					name:    "../outside",
					content: []byte("invalid"),
				},
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			archivePath := filepath.Join(
				t.TempDir(),
				"invalid.oci.tar",
			)
			writeTestOCIArchive(t, archivePath, test.entries)

			_, err := inspectOperationWorkerOCIArchive(archivePath)
			if err == nil {
				t.Fatal("expected invalid OCI archive to be rejected")
			}
		})
	}
}

func TestInspectOperationWorkerOCIArchiveRejectsMissingFile(
	t *testing.T,
) {
	_, err := inspectOperationWorkerOCIArchive(
		filepath.Join(t.TempDir(), "missing.oci.tar"),
	)
	if err == nil {
		t.Fatal("expected missing archive to be rejected")
	}
}

func TestPublishOperationWorkerArtifact(t *testing.T) {
	root := t.TempDir()
	stagingPath := filepath.Join(root, "stage.oci.tar")

	writeTestOCIArchive(
		t,
		stagingPath,
		validTestOCIArchiveEntries(),
	)

	metadata, err := inspectOperationWorkerOCIArchive(stagingPath)
	if err != nil {
		t.Fatalf("inspect staged archive: %v", err)
	}

	publishedPath, err := publishOperationWorkerArtifact(
		root,
		stagingPath,
		metadata.ArchiveDigest,
	)
	if err != nil {
		t.Fatalf("publish artifact: %v", err)
	}

	expectedPath := filepath.Join(
		root,
		"sha256",
		strings.TrimPrefix(metadata.ArchiveDigest, "sha256:")+".oci.tar",
	)
	if publishedPath != expectedPath {
		t.Errorf("published path = %q, want %q", publishedPath, expectedPath)
	}

	publishedMetadata, err := inspectOperationWorkerOCIArchive(publishedPath)
	if err != nil {
		t.Fatalf("inspect published archive: %v", err)
	}

	if publishedMetadata.ArchiveDigest != metadata.ArchiveDigest {
		t.Errorf(
			"published digest = %q, want %q",
			publishedMetadata.ArchiveDigest,
			metadata.ArchiveDigest,
		)
	}
}

func TestPublishOperationWorkerArtifactDeduplicatesExistingContent(
	t *testing.T,
) {
	root := t.TempDir()
	stagingPath := filepath.Join(root, "first-stage.oci.tar")

	writeTestOCIArchive(
		t,
		stagingPath,
		validTestOCIArchiveEntries(),
	)

	metadata, err := inspectOperationWorkerOCIArchive(stagingPath)
	if err != nil {
		t.Fatalf("inspect first stage: %v", err)
	}

	publishedPath, err := publishOperationWorkerArtifact(
		root,
		stagingPath,
		metadata.ArchiveDigest,
	)
	if err != nil {
		t.Fatalf("publish first archive: %v", err)
	}

	secondStage := filepath.Join(root, "second-stage.oci.tar")
	content, err := os.ReadFile(publishedPath)
	if err != nil {
		t.Fatalf("read published archive: %v", err)
	}

	if err := os.WriteFile(secondStage, content, 0o600); err != nil {
		t.Fatalf("write duplicate archive: %v", err)
	}

	secondPath, err := publishOperationWorkerArtifact(
		root,
		secondStage,
		metadata.ArchiveDigest,
	)
	if err != nil {
		t.Fatalf("publish duplicate archive: %v", err)
	}

	if secondPath != publishedPath {
		t.Errorf("duplicate path = %q, want %q", secondPath, publishedPath)
	}

	if _, err := os.Stat(secondStage); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("duplicate staging file remains or stat failed: %v", err)
	}
}

func TestPublishOperationWorkerArtifactRejectsInvalidDigest(
	t *testing.T,
) {
	_, err := publishOperationWorkerArtifact(
		t.TempDir(),
		"unused.oci.tar",
		"sha256:not-a-valid-digest",
	)
	if err == nil {
		t.Fatal("expected invalid digest to be rejected")
	}
}

func TestPublishOperationWorkerArtifactRejectsCorruptExistingArtifact(
	t *testing.T,
) {
	root := t.TempDir()
	stagingPath := filepath.Join(root, "stage.oci.tar")

	writeTestOCIArchive(
		t,
		stagingPath,
		validTestOCIArchiveEntries(),
	)

	metadata, err := inspectOperationWorkerOCIArchive(stagingPath)
	if err != nil {
		t.Fatalf("inspect staged archive: %v", err)
	}

	digestDirectory := filepath.Join(root, "sha256")
	if err := os.Mkdir(digestDirectory, 0o700); err != nil {
		t.Fatalf("create digest directory: %v", err)
	}

	corruptPath := filepath.Join(
		digestDirectory,
		strings.TrimPrefix(metadata.ArchiveDigest, "sha256:")+".oci.tar",
	)
	if err := os.WriteFile(corruptPath, []byte("corrupt"), 0o600); err != nil {
		t.Fatalf("write corrupt artifact: %v", err)
	}

	_, err = publishOperationWorkerArtifact(
		root,
		stagingPath,
		metadata.ArchiveDigest,
	)
	if err == nil {
		t.Fatal("expected corrupt existing artifact to be rejected")
	}
}

func TestRunOperationWorkerBuildKitWritesOCIOutput(t *testing.T) {
	root := t.TempDir()
	sourceArchive := filepath.Join(root, "source.oci.tar")
	outputArchive := filepath.Join(root, "output.oci.tar")

	writeTestOCIArchive(
		t,
		sourceArchive,
		validTestOCIArchiveEntries(),
	)

	buildctl := filepath.Join(root, "fake-buildctl")
	script := "#!/bin/sh\nset -eu\noutput=''\n"
	script += "for arg in \"$@\"; do\n"
	script += "  case \"$arg\" in\n"
	script += "    type=oci,dest=*) output=${arg#type=oci,dest=} ;;\n"
	script += "  esac\n"
	script += "done\n"
	script += "test -n \"$output\"\n"
	script += "cp " + shellQuoteBuildExecutorTest(sourceArchive) + " \"$output\"\n"

	if err := os.WriteFile(buildctl, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake BuildKit client: %v", err)
	}

	sourceDirectory := filepath.Join(root, "source")
	if err := os.Mkdir(sourceDirectory, 0o700); err != nil {
		t.Fatalf("create source directory: %v", err)
	}

	err := runOperationWorkerBuildKit(
		context.Background(),
		buildctl,
		"unix:///run/buildkit/buildkitd.sock",
		sourceDirectory,
		outputArchive,
		5*time.Second,
	)
	if err != nil {
		t.Fatalf("run fake BuildKit client: %v", err)
	}

	if _, err := inspectOperationWorkerOCIArchive(outputArchive); err != nil {
		t.Fatalf("inspect fake BuildKit output: %v", err)
	}
}

func TestRunOperationWorkerBuildKitReportsCommandFailure(t *testing.T) {
	buildctl := filepath.Join(t.TempDir(), "fake-buildctl")
	if err := os.WriteFile(
		buildctl,
		[]byte("#!/bin/sh\nexit 19\n"),
		0o700,
	); err != nil {
		t.Fatalf("write failing BuildKit client: %v", err)
	}

	err := runOperationWorkerBuildKit(
		context.Background(),
		buildctl,
		"unix:///run/buildkit/buildkitd.sock",
		t.TempDir(),
		filepath.Join(t.TempDir(), "output.oci.tar"),
		5*time.Second,
	)
	if err == nil {
		t.Fatal("expected BuildKit command failure")
	}

	if errors.Is(err, errOperationWorkerBuildOutcomeUncertain) {
		t.Errorf("ordinary command failure incorrectly marked uncertain: %v", err)
	}
}

func TestRunOperationWorkerBuildKitReportsTimeout(t *testing.T) {
	buildctl := filepath.Join(t.TempDir(), "fake-buildctl")
	if err := os.WriteFile(
		buildctl,
		[]byte("#!/bin/sh\nexec /bin/sleep 2\n"),
		0o700,
	); err != nil {
		t.Fatalf("write slow BuildKit client: %v", err)
	}

	err := runOperationWorkerBuildKit(
		context.Background(),
		buildctl,
		"unix:///run/buildkit/buildkitd.sock",
		t.TempDir(),
		filepath.Join(t.TempDir(), "output.oci.tar"),
		25*time.Millisecond,
	)
	if !errors.Is(err, errOperationWorkerBuildOutcomeUncertain) {
		t.Fatalf(
			"timeout error = %v, want uncertain build outcome",
			err,
		)
	}
}

func shellQuoteBuildExecutorTest(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
