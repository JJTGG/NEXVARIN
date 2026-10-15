package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMarkOperationNeedsReconciliationIntegrationRecordsOutcome(
	t *testing.T,
) {
	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 0)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	const errorCode = "deployment_outcome_unknown"
	const errorSummary = "Deployment result is uncertain"

	record, err := markOperationNeedsReconciliation(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		claimed.LeaseOwner,
		errorCode,
		errorSummary,
	)
	if err != nil {
		t.Fatalf("mark operation for reconciliation: %v", err)
	}

	if record.Status != operationStatusNeedsReconciliation {
		t.Errorf(
			"status = %q, want needs_reconciliation",
			record.Status,
		)
	}
	if record.FinishedAt != nil {
		t.Errorf(
			"finished_at = %v, want nil while outcome is unresolved",
			record.FinishedAt,
		)
	}
	if record.ErrorCode == nil || *record.ErrorCode != errorCode {
		t.Errorf("error code = %v, want %q", record.ErrorCode, errorCode)
	}
	if record.ErrorSummary == nil || *record.ErrorSummary != errorSummary {
		t.Errorf(
			"error summary = %v, want %q",
			record.ErrorSummary,
			errorSummary,
		)
	}

	var status string
	var leaseOwner *string
	var leaseExpiresAt *time.Time
	var finishedAt *time.Time
	var storedErrorCode *string
	var storedErrorSummary *string

	err = fixture.pool.QueryRow(ctx, `
		SELECT
			status,
			lease_owner,
			lease_expires_at,
			finished_at,
			error_code,
			error_summary
		FROM operations
		WHERE id = $1::uuid
	`, claimed.Operation.ID).Scan(
		&status,
		&leaseOwner,
		&leaseExpiresAt,
		&finishedAt,
		&storedErrorCode,
		&storedErrorSummary,
	)
	if err != nil {
		t.Fatalf("read reconciled operation: %v", err)
	}

	if status != string(operationStatusNeedsReconciliation) {
		t.Errorf(
			"persisted status = %q, want needs_reconciliation",
			status,
		)
	}
	if leaseOwner != nil {
		t.Errorf("lease owner = %q, want NULL", *leaseOwner)
	}
	if leaseExpiresAt != nil {
		t.Errorf("lease expiry = %v, want NULL", *leaseExpiresAt)
	}
	if finishedAt != nil {
		t.Errorf("finished_at = %v, want NULL", *finishedAt)
	}
	if storedErrorCode == nil || *storedErrorCode != errorCode {
		t.Errorf("persisted error code = %v, want %q", storedErrorCode, errorCode)
	}
	if storedErrorSummary == nil || *storedErrorSummary != errorSummary {
		t.Errorf(
			"persisted error summary = %v, want %q",
			storedErrorSummary,
			errorSummary,
		)
	}

	var eventCount int
	var actor string
	var fromStatus string
	var toStatus string
	var eventErrorCode string

	err = fixture.pool.QueryRow(ctx, `
		SELECT
			count(*),
			MAX(actor_subject),
			MAX(from_status),
			MAX(to_status),
			MAX(details->>'errorCode')
		FROM operation_events
		WHERE operation_id = $1::uuid
		  AND event_type = 'operation.needs_reconciliation'
	`, claimed.Operation.ID).Scan(
		&eventCount,
		&actor,
		&fromStatus,
		&toStatus,
		&eventErrorCode,
	)
	if err != nil {
		t.Fatalf("read reconciliation history: %v", err)
	}

	if eventCount != 1 {
		t.Errorf("reconciliation event count = %d, want 1", eventCount)
	}
	if actor != "worker-outcome-test" {
		t.Errorf("event actor = %q, want worker-outcome-test", actor)
	}
	if fromStatus != string(operationStatusRunning) {
		t.Errorf("from status = %q, want running", fromStatus)
	}
	if toStatus != string(operationStatusNeedsReconciliation) {
		t.Errorf(
			"to status = %q, want needs_reconciliation",
			toStatus,
		)
	}
	if eventErrorCode != errorCode {
		t.Errorf("event error code = %q, want %q", eventErrorCode, errorCode)
	}
}

func TestMarkOperationNeedsReconciliationIntegrationRejectsExpiredLease(
	t *testing.T,
) {
	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 0)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	_, err := fixture.pool.Exec(ctx, `
		UPDATE operations
		SET lease_expires_at =
			clock_timestamp() - interval '1 second'
		WHERE id = $1::uuid
		  AND lease_owner = $2
	`, claimed.Operation.ID, claimed.LeaseOwner)
	if err != nil {
		t.Fatalf("expire operation lease: %v", err)
	}

	_, err = markOperationNeedsReconciliation(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		claimed.LeaseOwner,
		"deployment_outcome_unknown",
		"Deployment result is uncertain",
	)
	if !errors.Is(err, errOperationLeaseNotOwned) {
		t.Fatalf(
			"reconciliation with expired lease error = %v, want lease-not-owned",
			err,
		)
	}

	var status string
	var leaseOwner *string
	var eventCount int

	err = fixture.pool.QueryRow(ctx, `
		SELECT
			status,
			lease_owner,
			(
				SELECT count(*)
				FROM operation_events
				WHERE operation_id = operations.id
				  AND event_type = 'operation.needs_reconciliation'
			)
		FROM operations
		WHERE id = $1::uuid
	`, claimed.Operation.ID).Scan(
		&status,
		&leaseOwner,
		&eventCount,
	)
	if err != nil {
		t.Fatalf("read operation after rejected reconciliation: %v", err)
	}

	if status != string(operationStatusRunning) {
		t.Errorf("status = %q, want running", status)
	}
	if leaseOwner == nil || *leaseOwner != claimed.LeaseOwner {
		t.Errorf("lease owner = %v, want original lease token", leaseOwner)
	}
	if eventCount != 0 {
		t.Errorf("reconciliation event count = %d, want 0", eventCount)
	}
}

func TestMarkOperationNeedsReconciliationIntegrationRejectsWrongOwner(
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

	_, err := markOperationNeedsReconciliation(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		wrongOwner,
		"deployment_outcome_unknown",
		"Deployment result is uncertain",
	)
	if !errors.Is(err, errOperationLeaseNotOwned) {
		t.Fatalf(
			"reconciliation with wrong owner error = %v, want lease-not-owned",
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
		t.Fatalf("read operation after wrong-owner rejection: %v", err)
	}

	if status != string(operationStatusRunning) {
		t.Errorf("status = %q, want running", status)
	}
	if leaseOwner == nil || *leaseOwner != claimed.LeaseOwner {
		t.Errorf("lease owner = %v, want original lease token", leaseOwner)
	}
}

func TestMarkOperationNeedsReconciliationIntegrationRejectsCompletedOperation(
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

	_, err := markOperationNeedsReconciliation(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		claimed.LeaseOwner,
		"deployment_outcome_unknown",
		"Deployment result is uncertain",
	)
	if !errors.Is(err, errOperationOutcomeStateConflict) {
		t.Fatalf(
			"reconciliation of completed operation error = %v, want state conflict",
			err,
		)
	}

	var status string
	var eventCount int

	err = fixture.pool.QueryRow(ctx, `
		SELECT
			status,
			(
				SELECT count(*)
				FROM operation_events
				WHERE operation_id = operations.id
				  AND event_type = 'operation.needs_reconciliation'
			)
		FROM operations
		WHERE id = $1::uuid
	`, claimed.Operation.ID).Scan(&status, &eventCount)
	if err != nil {
		t.Fatalf("read completed operation: %v", err)
	}

	if status != string(operationStatusSucceeded) {
		t.Errorf("status = %q, want succeeded", status)
	}
	if eventCount != 0 {
		t.Errorf("reconciliation event count = %d, want 0", eventCount)
	}
}

func TestMarkOperationNeedsReconciliationIntegrationRollsBackWhenHistoryFails(
	t *testing.T,
) {
	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 0)
	installReconciliationHistoryFailureTrigger(t, fixture)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	_, err := markOperationNeedsReconciliation(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		claimed.LeaseOwner,
		"deployment_outcome_unknown",
		"Deployment result is uncertain",
	)
	if err == nil {
		t.Fatal("reconciliation error = nil, want simulated history insert failure")
	}

	var status string
	var leaseOwner *string
	var leaseExpiresAt *time.Time
	var finishedAt *time.Time
	var storedErrorCode *string

	err = fixture.pool.QueryRow(ctx, `
		SELECT
			status,
			lease_owner,
			lease_expires_at,
			finished_at,
			error_code
		FROM operations
		WHERE id = $1::uuid
	`, claimed.Operation.ID).Scan(
		&status,
		&leaseOwner,
		&leaseExpiresAt,
		&finishedAt,
		&storedErrorCode,
	)
	if err != nil {
		t.Fatalf("read operation after reconciliation rollback: %v", err)
	}

	if status != string(operationStatusRunning) {
		t.Errorf("status after rollback = %q, want running", status)
	}
	if leaseOwner == nil || *leaseOwner != claimed.LeaseOwner {
		t.Errorf("lease owner after rollback = %v, want original lease token", leaseOwner)
	}
	if leaseExpiresAt == nil {
		t.Error("lease expiry after rollback is NULL")
	}
	if finishedAt != nil {
		t.Errorf("finished_at after rollback = %v, want NULL", finishedAt)
	}
	if storedErrorCode != nil {
		t.Errorf("error code after rollback = %q, want NULL", *storedErrorCode)
	}

	var eventCount int
	err = fixture.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM operation_events
		WHERE operation_id = $1::uuid
		  AND event_type = 'operation.needs_reconciliation'
	`, claimed.Operation.ID).Scan(&eventCount)
	if err != nil {
		t.Fatalf("count reconciliation events after rollback: %v", err)
	}
	if eventCount != 0 {
		t.Errorf("reconciliation events after rollback = %d, want 0", eventCount)
	}
}

func installReconciliationHistoryFailureTrigger(
	t *testing.T,
	fixture *operationStoreTestFixture,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	const triggerName = "nexvarin_test_fail_reconciliation_event_insert"
	const functionName = "nexvarin_test_fail_reconciliation_event_insert"

	dropTrigger := "DROP TRIGGER IF EXISTS " + triggerName +
		" ON public.operation_events"
	dropFunction := "DROP FUNCTION IF EXISTS public." + functionName + "()"

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()

		if _, err := fixture.pool.Exec(cleanupCtx, dropTrigger); err != nil {
			t.Errorf("drop reconciliation failure trigger: %v", err)
		}
		if _, err := fixture.pool.Exec(cleanupCtx, dropFunction); err != nil {
			t.Errorf("drop reconciliation failure function: %v", err)
		}
	})

	if _, err := fixture.pool.Exec(ctx, dropTrigger); err != nil {
		t.Fatalf("remove previous reconciliation failure trigger: %v", err)
	}
	if _, err := fixture.pool.Exec(ctx, dropFunction); err != nil {
		t.Fatalf("remove previous reconciliation failure function: %v", err)
	}

	createFunction := `
		CREATE FUNCTION public.nexvarin_test_fail_reconciliation_event_insert()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			IF NEW.event_type = 'operation.needs_reconciliation' THEN
				RAISE EXCEPTION 'simulated reconciliation history failure';
			END IF;
			RETURN NEW;
		END;
		$$
	`
	if _, err := fixture.pool.Exec(ctx, createFunction); err != nil {
		t.Fatalf("create reconciliation failure function: %v", err)
	}

	createTrigger := `
		CREATE TRIGGER nexvarin_test_fail_reconciliation_event_insert
		BEFORE INSERT ON public.operation_events
		FOR EACH ROW
		EXECUTE FUNCTION public.nexvarin_test_fail_reconciliation_event_insert()
	`
	if _, err := fixture.pool.Exec(ctx, createTrigger); err != nil {
		t.Fatalf("create reconciliation failure trigger: %v", err)
	}
}
