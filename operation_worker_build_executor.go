package main

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	operationWorkerBuildTimeout       = 10 * time.Minute
	operationWorkerBuildRetryDelay    = 15 * time.Second
	operationWorkerBuildMetadataLimit = 1 << 20
)

var errOperationWorkerBuildOutcomeUncertain = errors.New(
	"BuildKit execution outcome is uncertain",
)

type operationWorkerBuildExecutor struct {
	Pool              *pgxpool.Pool
	BuildctlPath      string
	BuildkitAddress   string
	ArtifactDirectory string
	BuildTimeout      time.Duration
}

type operationWorkerOCIArchiveMetadata struct {
	ArchiveDigest string
	IndexDigest   string
	Size          int64
}

type operationWorkerOCILayoutDocument struct {
	ImageLayoutVersion string `json:"imageLayoutVersion"`
}

type operationWorkerOCIIndexDocument struct {
	SchemaVersion int                            `json:"schemaVersion"`
	Manifests     []operationWorkerOCIDescriptor `json:"manifests"`
}

type operationWorkerOCIManifestDocument struct {
	SchemaVersion int                            `json:"schemaVersion"`
	Config        operationWorkerOCIDescriptor   `json:"config"`
	Layers        []operationWorkerOCIDescriptor `json:"layers"`
}

type operationWorkerOCIDescriptor struct {
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}

type operationWorkerBuildArtifactMetadata struct {
	Format             string `json:"format"`
	ImageReference     string `json:"imageReference"`
	ContentDigestScope string `json:"contentDigestScope"`
	ArchiveDigest      string `json:"archiveDigest"`
	OCIIndexDigest     string `json:"ociIndexDigest"`
	SourceProvider     string `json:"sourceProvider"`
	SourceRevision     string `json:"sourceRevision"`
	BuildOperationID   string `json:"buildOperationId"`
}

// Execute builds the source revision associated with a claimed build task.
//
// This initial implementation targets a single controlled Linux host with
// a local BuildKit Unix socket and a private, persistent artifact directory.
// It is not a public multi-tenant build service.
func (executor operationWorkerBuildExecutor) Execute(
	ctx context.Context,
	task operationWorkerTask,
) operationExecutionResult {
	if ctx == nil {
		return failedBuildResult(
			"invalid_build_context",
			"A valid build execution context is required.",
		)
	}

	if executor.Pool == nil {
		return failedBuildResult(
			"build_executor_not_configured",
			"The build executor database is not configured.",
		)
	}

	if task.Request.OperationType != operationTypeBuild ||
		task.Claimed.Operation.OperationType != operationTypeBuild {
		return failedBuildResult(
			"invalid_build_task",
			"The build executor received a non-build operation.",
		)
	}

	revision := task.Request.SourceRevision
	if !isValidGitRevision(revision) ||
		task.Claimed.Operation.SourceRevision == nil ||
		*task.Claimed.Operation.SourceRevision != revision {
		return failedBuildResult(
			"invalid_build_revision",
			"The requested source revision failed integrity validation.",
		)
	}

	buildctlPath := strings.TrimSpace(executor.BuildctlPath)
	if buildctlPath == "" {
		buildctlPath = "buildctl"
	}

	resolvedBuildctl, err := exec.LookPath(buildctlPath)
	if err != nil {
		return failedBuildResult(
			"buildkit_unavailable",
			"The configured BuildKit client executable is unavailable.",
		)
	}

	buildkitAddress, err := validateOperationWorkerBuildkitAddress(
		executor.BuildkitAddress,
	)
	if err != nil {
		return failedBuildResult(
			"invalid_buildkit_configuration",
			"The configured BuildKit address is invalid.",
		)
	}

	artifactDirectory, err := prepareOperationWorkerArtifactDirectory(
		executor.ArtifactDirectory,
	)
	if err != nil {
		return failedBuildResult(
			"invalid_artifact_storage",
			"The configured artifact directory is unavailable or insufficiently protected.",
		)
	}

	timeout := executor.BuildTimeout
	if timeout <= 0 {
		timeout = operationWorkerBuildTimeout
	}

	imageReference, err := operationWorkerBuildImageReference(
		task.Claimed.Operation.ID,
	)
	if err != nil {
		return failedBuildResult(
			"invalid_build_operation_id",
			"The build operation identifier is invalid.",
		)
	}

	source, err := loadOperationWorkerSource(
		ctx,
		executor.Pool,
		task,
	)
	if err != nil {
		switch {
		case errors.Is(err, errOperationWorkerSourceUnavailable):
			return failedBuildResult(
				"source_unavailable",
				"The registered project or repository is not active in the requested scope.",
			)
		case errors.Is(err, errOperationWorkerSourceURLInvalid):
			return failedBuildResult(
				"invalid_source_url",
				"The registered repository URL failed validation.",
			)
		default:
			return retryBuildResult(
				"source_lookup_failed",
				"The worker could not load the registered repository.",
			)
		}
	}

	checkout, err := checkoutOperationWorkerSource(
		ctx,
		source,
		revision,
	)
	if err != nil {
		if ctx.Err() != nil {
			return reconciliationBuildResult(
				"source_checkout_interrupted",
				"Source checkout was interrupted; the worker must verify the operation before continuing.",
			)
		}

		// Source retrieval is read-only. A later attempt may safely fetch
		// the same immutable revision again.
		return retryBuildResult(
			"source_checkout_failed",
			"The requested source revision could not be retrieved.",
		)
	}

	checkoutCleaned := false
	defer func() {
		if !checkoutCleaned {
			_ = checkout.Cleanup()
		}
	}()

	dockerfilePath := filepath.Join(checkout.Directory(), "Dockerfile")
	dockerfileInfo, err := os.Lstat(dockerfilePath)
	if err != nil {
		return failedBuildResult(
			"dockerfile_not_found",
			"The registered source revision does not contain a readable Dockerfile at its root.",
		)
	}
	if !dockerfileInfo.Mode().IsRegular() {
		return failedBuildResult(
			"invalid_dockerfile",
			"The root Dockerfile must be a regular file rather than a symlink or special file.",
		)
	}

	stagingFile, err := os.CreateTemp(
		artifactDirectory,
		".nexvarin-build-*.oci.tar",
	)
	if err != nil {
		return retryBuildResult(
			"artifact_staging_failed",
			"The worker could not allocate temporary artifact storage.",
		)
	}

	stagingPath := stagingFile.Name()
	if err := stagingFile.Close(); err != nil {
		_ = os.Remove(stagingPath)
		return retryBuildResult(
			"artifact_staging_failed",
			"The worker could not prepare temporary artifact storage.",
		)
	}

	// BuildKit's OCI exporter creates the output file. Remove the empty
	// placeholder before invoking it.
	if err := os.Remove(stagingPath); err != nil {
		return retryBuildResult(
			"artifact_staging_failed",
			"The worker could not prepare the artifact output path.",
		)
	}

	defer func() {
		if stagingPath != "" {
			_ = os.Remove(stagingPath)
		}
	}()

	if err := runOperationWorkerBuildKitNamed(
		ctx,
		resolvedBuildctl,
		buildkitAddress,
		checkout.Directory(),
		stagingPath,
		timeout,
		imageReference,
	); err != nil {
		if errors.Is(err, errOperationWorkerBuildOutcomeUncertain) {
			return reconciliationBuildResult(
				"build_outcome_unknown",
				"BuildKit execution was interrupted before its outcome could be established.",
			)
		}

		return failedBuildResult(
			"build_failed",
			"BuildKit could not complete the requested source build.",
		)
	}

	archiveMetadata, err := inspectOperationWorkerOCIArchive(stagingPath)
	if err != nil {
		return failedBuildResult(
			"invalid_build_artifact",
			"BuildKit output failed OCI archive structural and content-integrity validation.",
		)
	}

	if err := os.Chmod(stagingPath, 0o600); err != nil {
		return retryBuildResult(
			"artifact_storage_failed",
			"The worker could not secure the built artifact.",
		)
	}

	artifactReference, err := publishOperationWorkerArtifact(
		artifactDirectory,
		stagingPath,
		archiveMetadata.ArchiveDigest,
	)
	if err != nil {
		return reconciliationBuildResult(
			"artifact_publish_uncertain",
			"The worker could not establish the final state of the content-addressed artifact.",
		)
	}

	// Publication moved or deduplicated the staging file.
	stagingPath = ""

	metadata, err := json.Marshal(operationWorkerBuildArtifactMetadata{
		Format:             "oci-archive-tar",
		ImageReference:     imageReference,
		ContentDigestScope: "complete-archive-file",
		ArchiveDigest:      archiveMetadata.ArchiveDigest,
		OCIIndexDigest:     archiveMetadata.IndexDigest,
		SourceProvider:     source.Provider,
		SourceRevision:     revision,
		BuildOperationID:   task.Claimed.Operation.ID,
	})
	if err != nil {
		return reconciliationBuildResult(
			"artifact_metadata_failed",
			"The artifact was published but its metadata could not be prepared.",
		)
	}

	var artifactID string

	err = executor.Pool.QueryRow(ctx, `
		INSERT INTO artifacts (
			project_id,
			repository_id,
			build_operation_id,
			source_revision,
			artifact_kind,
			artifact_reference,
			content_digest,
			verification_status,
			verified_at,
			metadata
		)
		VALUES (
			$1::uuid,
			$2::uuid,
			$3::uuid,
			$4,
			'oci_image',
			$5,
			$6,
			'verified',
			now(),
			$7::jsonb
		)
		ON CONFLICT (build_operation_id, content_digest)
		DO UPDATE SET
			artifact_reference = EXCLUDED.artifact_reference,
			verification_status = 'verified',
			verified_at = now(),
			metadata = EXCLUDED.metadata
		RETURNING id::text
	`,
		task.Claimed.Operation.ProjectID,
		task.Claimed.Operation.RepositoryID,
		task.Claimed.Operation.ID,
		revision,
		artifactReference,
		archiveMetadata.ArchiveDigest,
		string(metadata),
	).Scan(&artifactID)

	if err != nil || artifactID == "" {
		return reconciliationBuildResult(
			"artifact_registration_uncertain",
			"The artifact was published but database registration could not be confirmed.",
		)
	}

	if err := checkout.Cleanup(); err != nil {
		return reconciliationBuildResult(
			"source_cleanup_uncertain",
			"The artifact was registered, but cleanup of the temporary source checkout could not be confirmed.",
		)
	}
	checkoutCleaned = true

	return operationExecutionResult{
		Disposition: operationExecutionSucceeded,
	}
}

func validateOperationWorkerBuildkitAddress(
	rawAddress string,
) (string, error) {
	rawAddress = strings.TrimSpace(rawAddress)
	if rawAddress == "" {
		return "", errors.New("BuildKit address is required")
	}

	parsed, err := url.Parse(rawAddress)
	if err != nil {
		return "", errors.New("invalid BuildKit address")
	}

	if parsed.Scheme != "unix" ||
		parsed.Host != "" ||
		parsed.User != nil ||
		parsed.Opaque != "" ||
		parsed.RawQuery != "" ||
		parsed.Fragment != "" ||
		!filepath.IsAbs(parsed.Path) {
		return "", errors.New(
			"BuildKit must use an absolute local Unix socket address",
		)
	}

	return rawAddress, nil
}

func prepareOperationWorkerArtifactDirectory(
	rawDirectory string,
) (string, error) {
	rawDirectory = strings.TrimSpace(rawDirectory)
	if rawDirectory == "" {
		return "", errors.New("artifact directory is required")
	}

	absolute, err := filepath.Abs(rawDirectory)
	if err != nil {
		return "", fmt.Errorf("resolve artifact directory: %w", err)
	}

	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return "", fmt.Errorf("create artifact directory: %w", err)
	}

	info, err := os.Lstat(absolute)
	if err != nil {
		return "", fmt.Errorf("inspect artifact directory: %w", err)
	}

	if info.Mode()&os.ModeSymlink != 0 ||
		!info.IsDir() ||
		info.Mode().Perm()&0o077 != 0 {
		return "", errors.New(
			"artifact directory must be a private, non-symlink directory",
		)
	}

	return absolute, nil
}

func runOperationWorkerBuildKit(
	ctx context.Context,
	buildctlPath string,
	address string,
	sourceDirectory string,
	outputPath string,
	timeout time.Duration,
) error {
	return runOperationWorkerBuildKitNamed(
		ctx,
		buildctlPath,
		address,
		sourceDirectory,
		outputPath,
		timeout,
		"",
	)
}

func runOperationWorkerBuildKitNamed(
	ctx context.Context,
	buildctlPath string,
	address string,
	sourceDirectory string,
	outputPath string,
	timeout time.Duration,
	imageReference string,
) error {
	if ctx == nil {
		return errors.New("BuildKit context is required")
	}

	if strings.Contains(outputPath, ",") {
		return errors.New("artifact output path cannot contain commas")
	}

	if imageReference != "" {
		prefix := "localhost/nexvarin/build-"
		suffix := ":latest"
		operationID := strings.TrimPrefix(imageReference, prefix)
		operationID = strings.TrimSuffix(operationID, suffix)

		expectedReference, err := operationWorkerBuildImageReference(
			operationID,
		)
		if err != nil || expectedReference != imageReference {
			return errors.New("BuildKit image reference is invalid")
		}
	}

	outputSpec := "type=oci,dest=" + outputPath
	if imageReference != "" {
		outputSpec += ",name=" + imageReference
	}

	homeDirectory, err := os.MkdirTemp("", "nexvarin-build-home-")
	if err != nil {
		return fmt.Errorf("create isolated BuildKit client home: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(homeDirectory)
	}()

	buildContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	command := exec.CommandContext(
		buildContext,
		buildctlPath,
		"--addr",
		address,
		"build",
		"--progress",
		"plain",
		"--frontend",
		"dockerfile.v0",
		"--local",
		"context="+sourceDirectory,
		"--local",
		"dockerfile="+sourceDirectory,
		"--opt",
		"filename=Dockerfile",
		"--output",
		outputSpec,
	)

	command.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + homeDirectory,
		"DOCKER_CONFIG=" + filepath.Join(homeDirectory, ".docker"),
		"LC_ALL=C",
	}

	command.Stdout = io.Discard
	command.Stderr = io.Discard

	if err := command.Run(); err != nil {
		if buildContext.Err() != nil {
			return fmt.Errorf(
				"%w: %v",
				errOperationWorkerBuildOutcomeUncertain,
				buildContext.Err(),
			)
		}

		return errors.New("BuildKit command failed")
	}

	return nil
}

func operationWorkerBuildImageReference(
	operationID string,
) (string, error) {
	normalizedID, err := normalizeOperationUUID(operationID)
	if err != nil {
		return "", fmt.Errorf("operation ID: %w", err)
	}

	return "localhost/nexvarin/build-" + normalizedID + ":latest", nil
}

func inspectOperationWorkerOCIArchive(
	archivePath string,
) (operationWorkerOCIArchiveMetadata, error) {
	var empty operationWorkerOCIArchiveMetadata

	file, err := os.Open(archivePath)
	if err != nil {
		return empty, fmt.Errorf("open OCI archive: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return empty, fmt.Errorf("stat OCI archive: %w", err)
	}

	if !info.Mode().IsRegular() || info.Size() <= 0 {
		return empty, errors.New("OCI archive must be a non-empty regular file")
	}

	archiveHash := sha256.New()
	if _, err := io.Copy(archiveHash, file); err != nil {
		return empty, fmt.Errorf("hash OCI archive: %w", err)
	}

	archiveDigest := "sha256:" + hex.EncodeToString(archiveHash.Sum(nil))

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return empty, fmt.Errorf("rewind OCI archive: %w", err)
	}

	reader := tar.NewReader(file)
	entries := make(map[string]bool)
	blobSizes := make(map[string]int64)
	blobContents := make(map[string][]byte)

	var layoutBytes []byte
	var indexBytes []byte

	const blobPrefix = "blobs/sha256/"

	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return empty, fmt.Errorf("read OCI archive header: %w", err)
		}

		name := strings.TrimPrefix(header.Name, "./")

		if header.Typeflag == tar.TypeDir {
			name = strings.TrimSuffix(name, "/")

			// An explicit archive-root directory is harmless.
			if name == "" || name == "." {
				continue
			}
		}

		cleanName := path.Clean(name)

		if name == "" ||
			path.IsAbs(name) ||
			cleanName != name ||
			cleanName == ".." ||
			strings.HasPrefix(cleanName, "../") {
			return empty, errors.New("OCI archive contains an invalid path")
		}

		isRegular := header.Typeflag == tar.TypeReg ||
			header.Typeflag == tar.TypeRegA

		if !isRegular && header.Typeflag != tar.TypeDir {
			return empty, errors.New(
				"OCI archive contains a non-regular, non-directory entry",
			)
		}

		if _, duplicate := entries[name]; duplicate {
			return empty, errors.New("OCI archive contains duplicate paths")
		}
		entries[name] = isRegular

		switch name {
		case "oci-layout", "index.json":
			if !isRegular ||
				header.Size <= 0 ||
				header.Size > operationWorkerBuildMetadataLimit {
				return empty, errors.New(
					"OCI layout metadata is not a bounded regular file",
				)
			}

			content, err := io.ReadAll(reader)
			if err != nil {
				return empty, fmt.Errorf(
					"read OCI layout metadata: %w",
					err,
				)
			}

			if int64(len(content)) != header.Size {
				return empty, errors.New(
					"OCI layout metadata size does not match its tar header",
				)
			}

			if name == "oci-layout" {
				layoutBytes = content
			} else {
				indexBytes = content
			}

		default:
			if !strings.HasPrefix(name, blobPrefix) {
				continue
			}

			if !isRegular || strings.Count(name, "/") != 2 {
				return empty, errors.New(
					"OCI content-addressed blob path is invalid",
				)
			}

			digestHex := strings.TrimPrefix(name, blobPrefix)
			if len(digestHex) != sha256.Size*2 {
				return empty, errors.New(
					"OCI content-addressed blob digest is invalid",
				)
			}

			decodedDigest, err := hex.DecodeString(digestHex)
			if err != nil ||
				len(decodedDigest) != sha256.Size ||
				hex.EncodeToString(decodedDigest) != digestHex {
				return empty, errors.New(
					"OCI content-addressed blob digest is invalid",
				)
			}

			blobHash := sha256.New()
			var blobSize int64

			if header.Size <= operationWorkerBuildMetadataLimit {
				content, err := io.ReadAll(reader)
				if err != nil {
					return empty, fmt.Errorf(
						"read OCI content-addressed blob: %w",
						err,
					)
				}

				blobSize = int64(len(content))
				_, _ = blobHash.Write(content)
				blobContents[name] = content
			} else {
				blobSize, err = io.Copy(blobHash, reader)
				if err != nil {
					return empty, fmt.Errorf(
						"hash OCI content-addressed blob: %w",
						err,
					)
				}
			}

			if blobSize != header.Size {
				return empty, errors.New(
					"OCI content-addressed blob size does not match its tar header",
				)
			}

			actualDigest := hex.EncodeToString(blobHash.Sum(nil))
			if actualDigest != digestHex {
				return empty, errors.New(
					"OCI content-addressed blob contents do not match their digest",
				)
			}

			blobSizes[name] = blobSize
		}
	}

	if len(layoutBytes) == 0 || len(indexBytes) == 0 {
		return empty, errors.New(
			"OCI archive is missing oci-layout or index.json",
		)
	}

	var layoutDocument operationWorkerOCILayoutDocument
	if err := json.Unmarshal(layoutBytes, &layoutDocument); err != nil {
		return empty, errors.New("OCI layout metadata is invalid")
	}

	if layoutDocument.ImageLayoutVersion != "1.0.0" {
		return empty, errors.New("unsupported OCI image-layout version")
	}

	var indexDocument operationWorkerOCIIndexDocument
	if err := json.Unmarshal(indexBytes, &indexDocument); err != nil {
		return empty, errors.New("OCI index is invalid")
	}

	if indexDocument.SchemaVersion != 2 ||
		len(indexDocument.Manifests) == 0 {
		return empty, errors.New(
			"OCI index must have schema version 2 and at least one manifest",
		)
	}

	for _, descriptor := range indexDocument.Manifests {
		manifestPath, err := validateOperationWorkerOCIArchiveDescriptor(
			descriptor,
			entries,
			blobSizes,
		)
		if err != nil {
			return empty, fmt.Errorf(
				"validate OCI index manifest: %w",
				err,
			)
		}

		manifestBytes, ok := blobContents[manifestPath]
		if !ok {
			return empty, errors.New(
				"OCI manifest exceeds the validation metadata limit",
			)
		}

		var manifestDocument operationWorkerOCIManifestDocument
		if err := json.Unmarshal(manifestBytes, &manifestDocument); err != nil {
			return empty, errors.New("OCI image manifest is invalid")
		}

		if manifestDocument.SchemaVersion != 2 ||
			manifestDocument.Layers == nil {
			return empty, errors.New(
				"OCI image manifest has an unsupported schema or missing layers",
			)
		}

		referencedBlobs := make(
			[]operationWorkerOCIDescriptor,
			0,
			1+len(manifestDocument.Layers),
		)
		referencedBlobs = append(
			referencedBlobs,
			manifestDocument.Config,
		)
		referencedBlobs = append(
			referencedBlobs,
			manifestDocument.Layers...,
		)

		for _, blobDescriptor := range referencedBlobs {
			if _, err := validateOperationWorkerOCIArchiveDescriptor(
				blobDescriptor,
				entries,
				blobSizes,
			); err != nil {
				return empty, fmt.Errorf(
					"validate OCI manifest referenced blob: %w",
					err,
				)
			}
		}
	}

	indexHash := sha256.Sum256(indexBytes)

	return operationWorkerOCIArchiveMetadata{
		ArchiveDigest: archiveDigest,
		IndexDigest:   "sha256:" + hex.EncodeToString(indexHash[:]),
		Size:          info.Size(),
	}, nil
}

func validateOperationWorkerOCIArchiveDescriptor(
	descriptor operationWorkerOCIDescriptor,
	entries map[string]bool,
	blobSizes map[string]int64,
) (string, error) {
	const digestPrefix = "sha256:"

	if !strings.HasPrefix(descriptor.Digest, digestPrefix) ||
		descriptor.Size < 0 {
		return "", errors.New("OCI descriptor digest or size is invalid")
	}

	hexDigest := strings.TrimPrefix(descriptor.Digest, digestPrefix)
	if len(hexDigest) != sha256.Size*2 {
		return "", errors.New("OCI descriptor digest is invalid")
	}

	decoded, err := hex.DecodeString(hexDigest)
	if err != nil ||
		len(decoded) != sha256.Size ||
		hex.EncodeToString(decoded) != hexDigest {
		return "", errors.New("OCI descriptor digest is invalid")
	}

	blobPath := "blobs/sha256/" + hexDigest

	isRegular, exists := entries[blobPath]
	if !exists || !isRegular {
		return "", errors.New(
			"OCI descriptor references a missing or non-regular blob",
		)
	}

	actualSize, exists := blobSizes[blobPath]
	if !exists || actualSize != descriptor.Size {
		return "", errors.New(
			"OCI descriptor size does not match its referenced blob",
		)
	}

	return blobPath, nil
}

func publishOperationWorkerArtifact(
	artifactDirectory string,
	stagingPath string,
	archiveDigest string,
) (string, error) {
	const digestPrefix = "sha256:"

	if !strings.HasPrefix(archiveDigest, digestPrefix) ||
		len(archiveDigest) != len(digestPrefix)+sha256.Size*2 {
		return "", errors.New("invalid artifact archive digest")
	}

	hexDigest := strings.TrimPrefix(archiveDigest, digestPrefix)
	if _, err := hex.DecodeString(hexDigest); err != nil {
		return "", errors.New("invalid artifact archive digest")
	}

	digestDirectory := filepath.Join(artifactDirectory, "sha256")
	if err := os.MkdirAll(digestDirectory, 0o700); err != nil {
		return "", fmt.Errorf("create digest directory: %w", err)
	}

	info, err := os.Lstat(digestDirectory)
	if err != nil {
		return "", fmt.Errorf("inspect digest directory: %w", err)
	}

	if info.Mode()&os.ModeSymlink != 0 ||
		!info.IsDir() ||
		info.Mode().Perm()&0o077 != 0 {
		return "", errors.New("artifact digest directory is not private")
	}

	finalPath := filepath.Join(
		digestDirectory,
		hexDigest+".oci.tar",
	)

	existingMetadata, err := inspectOperationWorkerOCIArchive(finalPath)
	if err == nil {
		if existingMetadata.ArchiveDigest != archiveDigest {
			return "", errors.New("existing artifact failed digest comparison")
		}

		if err := os.Remove(stagingPath); err != nil {
			return "", fmt.Errorf("remove duplicate staging archive: %w", err)
		}

		return finalPath, nil
	}

	if _, statErr := os.Lstat(finalPath); statErr == nil {
		return "", errors.New(
			"existing content-addressed artifact failed validation",
		)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return "", fmt.Errorf("inspect destination artifact: %w", statErr)
	}

	if err := os.Rename(stagingPath, finalPath); err != nil {
		return "", fmt.Errorf("publish content-addressed artifact: %w", err)
	}

	return finalPath, nil
}

func failedBuildResult(
	code string,
	summary string,
) operationExecutionResult {
	return operationExecutionResult{
		Disposition:  operationExecutionFailed,
		ErrorCode:    code,
		ErrorSummary: summary,
	}
}

func retryBuildResult(
	code string,
	summary string,
) operationExecutionResult {
	return operationExecutionResult{
		Disposition:  operationExecutionRetry,
		ErrorCode:    code,
		ErrorSummary: summary,
		RetryDelay:   operationWorkerBuildRetryDelay,
	}
}

func reconciliationBuildResult(
	code string,
	summary string,
) operationExecutionResult {
	return operationExecutionResult{
		Disposition:  operationExecutionNeedsReconciliation,
		ErrorCode:    code,
		ErrorSummary: summary,
	}
}
