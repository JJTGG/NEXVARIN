
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	errOperationWorkerDeployArtifactUnavailable = errors.New(
		"verified artifact is unavailable in the requested repository",
	)

	errOperationWorkerDeployArtifactIntegrity = errors.New(
		"verified artifact metadata failed integrity validation",
	)
)

// operationWorkerDeployArtifact contains the verified artifact
// information required by the deployment worker.
//
// The artifact reference is an internal storage path, not a public URL.
type operationWorkerDeployArtifact struct {
	ID                string
	ProjectID         string
	RepositoryID      string
	BuildOperationID  string
	SourceRevision    string
	ArtifactReference string
	ContentDigest     string
	Metadata          operationWorkerBuildArtifactMetadata
}

// loadOperationWorkerDeployArtifact retrieves an artifact only when
// it belongs to the deploying operation's project and repository
// and has already been marked as verified.
//
// This function does not start a container. Runtime execution will
// be implemented separately.
func loadOperationWorkerDeployArtifact(
	ctx context.Context,
	pool *pgxpool.Pool,
	task operationWorkerTask,
) (operationWorkerDeployArtifact, error) {
	var empty operationWorkerDeployArtifact

	if ctx == nil {
		return empty, errors.New(
			"deployment artifact context is required",
		)
	}

	if pool == nil {
		return empty, errors.New(
			"deployment artifact database pool is required",
		)
	}

	if task.Request.OperationType != operationTypeDeploy ||
		task.Claimed.Operation.OperationType != operationTypeDeploy {
		return empty, errors.New(
			"deployment artifact loader received a non-deployment task",
		)
	}

	artifactID, err := normalizeOperationUUID(
		task.Request.ArtifactID,
	)
	if err != nil {
		return empty, fmt.Errorf(
			"validate requested artifact ID: %w",
			err,
		)
	}

	projectID, err := normalizeOperationUUID(
		task.Claimed.Operation.ProjectID,
	)
	if err != nil {
		return empty, fmt.Errorf(
			"validate deployment project ID: %w",
			err,
		)
	}

	repositoryID, err := normalizeOperationUUID(
		task.Claimed.Operation.RepositoryID,
	)
	if err != nil {
		return empty, fmt.Errorf(
			"validate deployment repository ID: %w",
			err,
		)
	}

	var artifact operationWorkerDeployArtifact
	var metadataJSON string

	err = pool.QueryRow(ctx, `
		SELECT
			id::text,
			project_id::text,
			repository_id::text,
			build_operation_id::text,
			source_revision,
			artifact_reference,
			content_digest,
			metadata::text
		FROM artifacts
		WHERE id = $1::uuid
		  AND project_id = $2::uuid
		  AND repository_id = $3::uuid
		  AND artifact_kind = 'oci_image'
		  AND verification_status = 'verified'
	`, artifactID, projectID, repositoryID).Scan(
		&artifact.ID,
		&artifact.ProjectID,
		&artifact.RepositoryID,
		&artifact.BuildOperationID,
		&artifact.SourceRevision,
		&artifact.ArtifactReference,
		&artifact.ContentDigest,
		&metadataJSON,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return empty, errOperationWorkerDeployArtifactUnavailable
	}

	if err != nil {
		return empty, fmt.Errorf(
			"load verified deployment artifact: %w",
			err,
		)
	}

	if artifact.ID != artifactID ||
		artifact.ProjectID != projectID ||
		artifact.RepositoryID != repositoryID {
		return empty, errOperationWorkerDeployArtifactIntegrity
	}

	if !isValidGitRevision(artifact.SourceRevision) {
		return empty, errOperationWorkerDeployArtifactIntegrity
	}

	if strings.TrimSpace(artifact.ArtifactReference) == "" {
		return empty, errOperationWorkerDeployArtifactIntegrity
	}

	buildOperationID, err := normalizeOperationUUID(
		artifact.BuildOperationID,
	)
	if err != nil {
		return empty, fmt.Errorf(
			"%w: invalid build operation ID",
			errOperationWorkerDeployArtifactIntegrity,
		)
	}

	if buildOperationID != artifact.BuildOperationID {
		return empty, errOperationWorkerDeployArtifactIntegrity
	}

	if err := json.Unmarshal(
		[]byte(metadataJSON),
		&artifact.Metadata,
	); err != nil {
		return empty, fmt.Errorf(
			"%w: metadata is not valid JSON",
			errOperationWorkerDeployArtifactIntegrity,
		)
	}

	expectedImageReference, err := operationWorkerBuildImageReference(
		buildOperationID,
	)
	if err != nil {
		return empty, fmt.Errorf(
			"%w: image reference could not be validated",
			errOperationWorkerDeployArtifactIntegrity,
		)
	}

	if artifact.Metadata.Format != "oci-archive-tar" ||
		artifact.Metadata.ContentDigestScope != "complete-archive-file" ||
		artifact.Metadata.ArchiveDigest != artifact.ContentDigest ||
		artifact.Metadata.SourceRevision != artifact.SourceRevision ||
		artifact.Metadata.BuildOperationID != buildOperationID ||
		artifact.Metadata.ImageReference != expectedImageReference {
		return empty, errOperationWorkerDeployArtifactIntegrity
	}

	return artifact, nil
}
