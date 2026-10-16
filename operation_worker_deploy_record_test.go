package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestEnsureOperationWorkerDeployRecordIntegrationCreatesAndRecovers(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)
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

	first, err := ensureOperationWorkerDeployRecord(
		ctx,
		fixture.pool,
		task,
		artifact,
	)
	if err != nil {
		t.Fatalf("create deployment record: %v", err)
	}

	if first.ID == "" {
		t.Fatal("created deployment record has an empty ID")
	}

	if first.ArtifactID != artifact.ID {
		t.Errorf(
			"artifact ID = %q, want %q",
			first.ArtifactID,
			artifact.ID,
		)
	}

	second, err := ensureOperationWorkerDeployRecord(
		ctx,
		fixture.pool,
		task,
		artifact,
	)
	if err != nil {
		t.Fatalf("recover deployment record: %v", err)
	}

	if second.ID != first.ID {
		t.Errorf(
			"recovered deployment ID = %q, want original ID %q",
			second.ID,
			first.ID,
		)
	}

	if second.ArtifactID != first.ArtifactID {
		t.Errorf(
			"recovered artifact ID = %q, want %q",
			second.ArtifactID,
			first.ArtifactID,
		)
	}

	var count int

	err = fixture.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM deployments
		WHERE project_id = $1::uuid
		  AND repository_id = $2::uuid
		  AND last_operation_id = $3::uuid
	`,
		fixture.projectID,
		fixture.repositoryID,
		task.Claimed.Operation.ID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count deployment records: %v", err)
	}

	if count != 1 {
		t.Errorf(
			"deployment record count = %d, want 1",
			count,
		)
	}

	var (
		persistedID   string
		persistedRepo string
		persistedArt  string
		lastOperation string
		desiredState  string
		observedState string
		createdBy     string
	)

	err = fixture.pool.QueryRow(ctx, `
		SELECT
			id::text,
			repository_id::text,
			artifact_id::text,
			last_operation_id::text,
			desired_state,
			observed_state,
			created_by_subject
		FROM deployments
		WHERE project_id = $1::uuid
		  AND repository_id = $2::uuid
		  AND last_operation_id = $3::uuid
	`,
		fixture.projectID,
		fixture.repositoryID,
		task.Claimed.Operation.ID,
	).Scan(
		&persistedID,
		&persistedRepo,
		&persistedArt,
		&lastOperation,
		&desiredState,
		&observedState,
		&createdBy,
	)
	if err != nil {
		t.Fatalf("read persisted deployment record: %v", err)
	}

	if persistedID != first.ID {
		t.Errorf(
			"persisted ID = %q, want %q",
			persistedID,
			first.ID,
		)
	}

	if persistedRepo != fixture.repositoryID {
		t.Errorf(
			"persisted repository = %q, want %q",
			persistedRepo,
			fixture.repositoryID,
		)
	}

	if persistedArt != artifact.ID {
		t.Errorf(
			"persisted artifact = %q, want %q",
			persistedArt,
			artifact.ID,
		)
	}

	if lastOperation != task.Claimed.Operation.ID {
		t.Errorf(
			"last operation = %q, want %q",
			lastOperation,
			task.Claimed.Operation.ID,
		)
	}

	if desiredState != "active" {
		t.Errorf(
			"desired state = %q, want active",
			desiredState,
		)
	}

	if observedState != "pending" {
		t.Errorf(
			"observed state = %q, want pending",
			observedState,
		)
	}

	if createdBy != fixture.requestedBy {
		t.Errorf(
			"created by = %q, want %q",
			createdBy,
			fixture.requestedBy,
		)
	}
}

func TestEnsureOperationWorkerDeployRecordIntegrationRejectsForeignArtifact(
	t *testing.T,
) {
	localFixture := newOperationStoreTestFixture(t)
	foreignFixture := newOperationStoreTestFixture(t)

	localArtifact := insertDeployArtifactForTest(
		t,
		localFixture,
		"verified",
	)

	foreignArtifact := insertDeployArtifactForTest(
		t,
		foreignFixture,
		"verified",
	)

	task := createClaimedDeployTaskForArtifactTest(
		t,
		localFixture,
		localArtifact.ID,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	_, err := ensureOperationWorkerDeployRecord(
		ctx,
		localFixture.pool,
		task,
		foreignArtifact,
	)
	if !errors.Is(err, errOperationWorkerDeployRecordConflict) {
		t.Fatalf(
			"foreign artifact error = %v, want deployment record conflict",
			err,
		)
	}

	var count int

	err = localFixture.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM deployments
		WHERE project_id = $1::uuid
		  AND repository_id = $2::uuid
		  AND last_operation_id = $3::uuid
	`,
		localFixture.projectID,
		localFixture.repositoryID,
		task.Claimed.Operation.ID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count deployment records: %v", err)
	}

	if count != 0 {
		t.Errorf(
			"deployment record count = %d, want 0",
			count,
		)
	}
}

func TestEnsureOperationWorkerDeployRecordIntegrationRejectsExpiredLease(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)
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
		UPDATE operations
		SET lease_expires_at = clock_timestamp() - interval '1 second'
		WHERE id = $1::uuid
		  AND lease_owner = $2
	`,
		task.Claimed.Operation.ID,
		task.Claimed.LeaseOwner,
	)
	if err != nil {
		t.Fatalf("expire operation lease: %v", err)
	}

	if result.RowsAffected() != 1 {
		t.Fatalf(
			"expired operation rows = %d, want 1",
			result.RowsAffected(),
		)
	}

	_, err = ensureOperationWorkerDeployRecord(
		ctx,
		fixture.pool,
		task,
		artifact,
	)
	if !errors.Is(err, errOperationLeaseNotOwned) {
		t.Fatalf(
			"expired lease error = %v, want lease-not-owned",
			err,
		)
	}

	var count int

	err = fixture.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM deployments
		WHERE project_id = $1::uuid
		  AND repository_id = $2::uuid
		  AND last_operation_id = $3::uuid
	`,
		fixture.projectID,
		fixture.repositoryID,
		task.Claimed.Operation.ID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count deployment records: %v", err)
	}

	if count != 0 {
		t.Errorf(
			"deployment record count = %d, want 0",
			count,
		)
	}
}
