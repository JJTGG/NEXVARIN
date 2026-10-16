package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

const deployArtifactTestRevision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func insertDeployArtifactForTest(
	t *testing.T,
	fixture *operationStoreTestFixture,
	status string,
) operationWorkerDeployArtifact {
	t.Helper()

	build, err := fixture.create(
		"deploy-artifact-build",
		buildOperationRequestForTest(deployArtifactTestRevision),
	)
	if err != nil {
		t.Fatalf("create build operation: %v", err)
	}

	imageReference, err := operationWorkerBuildImageReference(
		build.Operation.ID,
	)
	if err != nil {
		t.Fatalf("create test image reference: %v", err)
	}

	artifact := operationWorkerDeployArtifact{
		ProjectID:         fixture.projectID,
		RepositoryID:      fixture.repositoryID,
		BuildOperationID:  build.Operation.ID,
		SourceRevision:    deployArtifactTestRevision,
		ArtifactReference: "/test-artifacts/" + build.Operation.ID + ".oci.tar",
		ContentDigest:     "sha256:" + strings.Repeat("a", 64),
		Metadata: operationWorkerBuildArtifactMetadata{
			Format:             "oci-archive-tar",
			ImageReference:     imageReference,
			ContentDigestScope: "complete-archive-file",
			ArchiveDigest:      "sha256:" + strings.Repeat("a", 64),
			OCIIndexDigest:     "sha256:" + strings.Repeat("b", 64),
			SourceProvider:     "generic_git",
			SourceRevision:     deployArtifactTestRevision,
			BuildOperationID:   build.Operation.ID,
		},
	}

	metadataJSON, err := json.Marshal(artifact.Metadata)
	if err != nil {
		t.Fatalf("encode test artifact metadata: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	err = fixture.pool.QueryRow(ctx, `
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
			$7,
			CASE WHEN $7 = 'verified' THEN now() ELSE NULL END,
			$8::jsonb
		)
		RETURNING id::text
	`,
		fixture.projectID,
		fixture.repositoryID,
		build.Operation.ID,
		deployArtifactTestRevision,
		artifact.ArtifactReference,
		artifact.ContentDigest,
		status,
		string(metadataJSON),
	).Scan(&artifact.ID)
	if err != nil {
		t.Fatalf("insert test artifact: %v", err)
	}

	return artifact
}

func createClaimedDeployTaskForArtifactTest(
	t *testing.T,
	fixture *operationStoreTestFixture,
	artifactID string,
) operationWorkerTask {
	t.Helper()

	requestBody, err := json.Marshal(struct {
		OperationType operationType `json:"operationType"`
		ArtifactID    string        `json:"artifactId"`
	}{
		OperationType: operationTypeDeploy,
		ArtifactID:    artifactID,
	})
	if err != nil {
		t.Fatalf("encode deployment request: %v", err)
	}

	created, err := fixture.create(
		"deploy-artifact-deploy",
		requestBody,
	)
	if err != nil {
		t.Fatalf("create deployment operation: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	claimed, err := claimOperation(
		ctx,
		fixture.pool,
		created.Operation.ID,
		"worker-deploy-artifact-test",
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim deployment operation: %v", err)
	}

	task, err := loadClaimedOperationTask(
		ctx,
		fixture.pool,
		claimed,
	)
	if err != nil {
		t.Fatalf("load claimed deployment task: %v", err)
	}

	return task
}

func TestLoadOperationWorkerDeployArtifactIntegrationLoadsVerifiedArtifact(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)
	want := insertDeployArtifactForTest(t, fixture, "verified")

	task := createClaimedDeployTaskForArtifactTest(
		t,
		fixture,
		want.ID,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	got, err := loadOperationWorkerDeployArtifact(
		ctx,
		fixture.pool,
		task,
	)
	if err != nil {
		t.Fatalf("load verified artifact: %v", err)
	}

	if got.ID != want.ID {
		t.Errorf("artifact ID = %q, want %q", got.ID, want.ID)
	}

	if got.ProjectID != want.ProjectID {
		t.Errorf("project ID = %q, want %q", got.ProjectID, want.ProjectID)
	}

	if got.RepositoryID != want.RepositoryID {
		t.Errorf(
			"repository ID = %q, want %q",
			got.RepositoryID,
			want.RepositoryID,
		)
	}

	if got.BuildOperationID != want.BuildOperationID {
		t.Errorf(
			"build operation ID = %q, want %q",
			got.BuildOperationID,
			want.BuildOperationID,
		)
	}

	if got.ArtifactReference != want.ArtifactReference {
		t.Errorf(
			"artifact reference = %q, want %q",
			got.ArtifactReference,
			want.ArtifactReference,
		)
	}

	if got.ContentDigest != want.ContentDigest {
		t.Errorf(
			"content digest = %q, want %q",
			got.ContentDigest,
			want.ContentDigest,
		)
	}

	if got.Metadata != want.Metadata {
		t.Errorf(
			"artifact metadata = %+v, want %+v",
			got.Metadata,
			want.Metadata,
		)
	}
}

func TestLoadOperationWorkerDeployArtifactIntegrationRejectsUnverifiedArtifact(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	// Deployment creation requires a verified artifact.
	// Create a valid deployment first, then simulate the artifact
	// becoming unverified before the worker attempts to load it.
	artifact := insertDeployArtifactForTest(t, fixture, "verified")

	task := createClaimedDeployTaskForArtifactTest(
		t,
		fixture,
		artifact.ID,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	result, err := fixture.pool.Exec(ctx, `
		UPDATE artifacts
		SET verification_status = 'pending',
		    verified_at = NULL
		WHERE id = $1::uuid
	`, artifact.ID)
	if err != nil {
		t.Fatalf("change artifact verification state: %v", err)
	}

	if result.RowsAffected() != 1 {
		t.Fatalf(
			"artifact update affected %d rows, want 1",
			result.RowsAffected(),
		)
	}

	_, err = loadOperationWorkerDeployArtifact(
		ctx,
		fixture.pool,
		task,
	)
	if !errors.Is(
		err,
		errOperationWorkerDeployArtifactUnavailable,
	) {
		t.Fatalf(
			"load pending artifact error = %v, want artifact-unavailable",
			err,
		)
	}
}

func TestLoadOperationWorkerDeployArtifactIntegrationRejectsOtherRepository(
	t *testing.T,
) {
	localFixture := newOperationStoreTestFixture(t)
	otherFixture := newOperationStoreTestFixture(t)

	// Both repositories have valid artifacts.
	localArtifact := insertDeployArtifactForTest(
		t,
		localFixture,
		"verified",
	)

	foreignArtifact := insertDeployArtifactForTest(
		t,
		otherFixture,
		"verified",
	)

	// Create a legitimate deployment for the local artifact.
	task := createClaimedDeployTaskForArtifactTest(
		t,
		localFixture,
		localArtifact.ID,
	)

	// Simulate a mismatched artifact ID reaching the loader.
	// The loader must enforce repository scope independently.
	task.Request.ArtifactID = foreignArtifact.ID

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	_, err := loadOperationWorkerDeployArtifact(
		ctx,
		localFixture.pool,
		task,
	)
	if !errors.Is(
		err,
		errOperationWorkerDeployArtifactUnavailable,
	) {
		t.Fatalf(
			"load foreign artifact error = %v, want artifact-unavailable",
			err,
		)
	}
}

func TestLoadOperationWorkerDeployArtifactIntegrationRejectsMetadataMismatch(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)
	artifact := insertDeployArtifactForTest(t, fixture, "verified")

	alteredMetadata := artifact.Metadata
	alteredMetadata.ArchiveDigest = "sha256:" + strings.Repeat("f", 64)

	metadataJSON, err := json.Marshal(alteredMetadata)
	if err != nil {
		t.Fatalf("encode altered artifact metadata: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	result, err := fixture.pool.Exec(ctx, `
		UPDATE artifacts
		SET metadata = $2::jsonb
		WHERE id = $1::uuid
	`, artifact.ID, string(metadataJSON))
	if err != nil {
		t.Fatalf("alter artifact metadata: %v", err)
	}

	if result.RowsAffected() != 1 {
		t.Fatalf(
			"metadata update affected %d rows, want 1",
			result.RowsAffected(),
		)
	}

	task := createClaimedDeployTaskForArtifactTest(
		t,
		fixture,
		artifact.ID,
	)

	_, err = loadOperationWorkerDeployArtifact(
		ctx,
		fixture.pool,
		task,
	)
	if !errors.Is(
		err,
		errOperationWorkerDeployArtifactIntegrity,
	) {
		t.Fatalf(
			"load artifact with mismatched digest error = %v, want integrity error",
			err,
		)
	}
}

func TestLoadOperationWorkerDeployArtifactRejectsNonDeployTask(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	_, err := loadOperationWorkerDeployArtifact(
		context.Background(),
		fixture.pool,
		operationWorkerTask{},
	)
	if err == nil {
		t.Fatal("expected non-deployment task to be rejected")
	}
}

func TestLoadOperationWorkerDeployArtifactRejectsNilPool(
	t *testing.T,
) {
	_, err := loadOperationWorkerDeployArtifact(
		context.Background(),
		nil,
		operationWorkerTask{},
	)
	if err == nil {
		t.Fatal("expected nil database pool to be rejected")
	}
}
