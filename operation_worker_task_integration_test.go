package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestLoadClaimedOperationTaskIntegrationLoadsValidTask(
	t *testing.T,
) {
	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 0)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	task, err := loadClaimedOperationTask(
		ctx,
		fixture.pool,
		claimed,
	)
	if err != nil {
		t.Fatalf("load claimed operation task: %v", err)
	}

	if task.Claimed.Operation.ID != claimed.Operation.ID {
		t.Errorf(
			"task operation ID = %q, want %q",
			task.Claimed.Operation.ID,
			claimed.Operation.ID,
		)
	}

	if task.Claimed.LeaseOwner != claimed.LeaseOwner {
		t.Errorf(
			"task lease owner = %q, want original lease owner",
			task.Claimed.LeaseOwner,
		)
	}

	if task.Request.OperationType != operationTypeBuild {
		t.Errorf(
			"request type = %q, want build",
			task.Request.OperationType,
		)
	}

	const expectedRevision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	if task.Request.SourceRevision != expectedRevision {
		t.Errorf(
			"source revision = %q, want %q",
			task.Request.SourceRevision,
			expectedRevision,
		)
	}

	if task.Request.ArtifactID != "" ||
		task.Request.DeploymentID != "" ||
		task.Request.ParentOperationID != "" {
		t.Errorf(
			"unexpected fields in normalized build request: %+v",
			task.Request,
		)
	}
}

func TestLoadClaimedOperationTaskIntegrationRejectsExpiredLease(
	t *testing.T,
) {
	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 0)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	result, err := fixture.pool.Exec(ctx, `
		UPDATE operations
		SET lease_expires_at =
			clock_timestamp() - interval '1 second'
		WHERE id = $1::uuid
		  AND lease_owner = $2
	`, claimed.Operation.ID, claimed.LeaseOwner)
	if err != nil {
		t.Fatalf("expire operation lease: %v", err)
	}

	if result.RowsAffected() != 1 {
		t.Fatalf(
			"expired lease update affected %d rows, want 1",
			result.RowsAffected(),
		)
	}

	_, err = loadClaimedOperationTask(
		ctx,
		fixture.pool,
		claimed,
	)
	if !errors.Is(err, errOperationLeaseNotOwned) {
		t.Fatalf(
			"load with expired lease error = %v, want lease-not-owned",
			err,
		)
	}
}

func TestLoadClaimedOperationTaskIntegrationRejectsWrongOwner(
	t *testing.T,
) {
	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 0)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	wrongOwner := claimed.LeaseOwner
	last := len(wrongOwner) - 1

	if wrongOwner[last] == '0' {
		wrongOwner = wrongOwner[:last] + "1"
	} else {
		wrongOwner = wrongOwner[:last] + "0"
	}

	claimed.LeaseOwner = wrongOwner

	_, err := loadClaimedOperationTask(
		ctx,
		fixture.pool,
		claimed,
	)
	if !errors.Is(err, errOperationLeaseNotOwned) {
		t.Fatalf(
			"load with wrong lease owner error = %v, want lease-not-owned",
			err,
		)
	}
}

func TestLoadClaimedOperationTaskIntegrationRejectsClaimMetadataMismatch(
	t *testing.T,
) {
	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 0)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	wrongRevision := strings.Repeat("b", 40)
	claimed.Operation.SourceRevision = &wrongRevision

	_, err := loadClaimedOperationTask(
		ctx,
		fixture.pool,
		claimed,
	)
	if !errors.Is(err, errOperationTaskIntegrity) {
		t.Fatalf(
			"load with mismatched claim metadata error = %v, want task-integrity error",
			err,
		)
	}

	var status string
	err = fixture.pool.QueryRow(ctx, `
		SELECT status
		FROM operations
		WHERE id = $1::uuid
	`, claimed.Operation.ID).Scan(&status)
	if err != nil {
		t.Fatalf("read operation after metadata rejection: %v", err)
	}

	if status != string(operationStatusRunning) {
		t.Errorf("status = %q, want running", status)
	}
}

func TestLoadClaimedOperationTaskIntegrationRejectsPayloadFingerprintMismatch(
	t *testing.T,
) {
	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 0)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	alteredPayload := `{
		"operationType": "build",
		"sourceRevision": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	}`

	result, err := fixture.pool.Exec(ctx, `
		UPDATE operations
		SET request_payload = $2::jsonb
		WHERE id = $1::uuid
	`, claimed.Operation.ID, alteredPayload)
	if err != nil {
		t.Fatalf("alter persisted request payload: %v", err)
	}

	if result.RowsAffected() != 1 {
		t.Fatalf(
			"payload update affected %d rows, want 1",
			result.RowsAffected(),
		)
	}

	_, err = loadClaimedOperationTask(
		ctx,
		fixture.pool,
		claimed,
	)
	if !errors.Is(err, errOperationTaskIntegrity) {
		t.Fatalf(
			"load with payload fingerprint mismatch error = %v, want task-integrity error",
			err,
		)
	}

	var status string
	var leaseOwner *string

	err = fixture.pool.QueryRow(ctx, `
		SELECT status, lease_owner
		FROM operations
		WHERE id = $1::uuid
	`, claimed.Operation.ID).Scan(&status, &leaseOwner)
	if err != nil {
		t.Fatalf("read operation after fingerprint rejection: %v", err)
	}

	if status != string(operationStatusRunning) {
		t.Errorf("status = %q, want running", status)
	}

	if leaseOwner == nil || *leaseOwner != claimed.LeaseOwner {
		t.Errorf(
			"lease owner = %v, want original lease owner",
			leaseOwner,
		)
	}
}

func TestLoadClaimedOperationTaskIntegrationRejectsCompletedOperation(
	t *testing.T,
) {
	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 0)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if _, err := completeOperation(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		claimed.LeaseOwner,
	); err != nil {
		t.Fatalf("complete operation: %v", err)
	}

	_, err := loadClaimedOperationTask(
		ctx,
		fixture.pool,
		claimed,
	)
	if !errors.Is(err, errOperationOutcomeStateConflict) {
		t.Fatalf(
			"load completed operation error = %v, want state conflict",
			err,
		)
	}
}
