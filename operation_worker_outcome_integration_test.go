package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func createAndClaimOperationForOutcomeTest(
	t *testing.T,
	maxAttempts int,
) (*operationStoreTestFixture, claimedOperation) {
	t.Helper()

	fixture := newOperationStoreTestFixture(t)
	created, err := fixture.create(
		"outcome-"+strings.ToLower(t.Name()),
		buildOperationRequestForTest(strings.Repeat("a", 40)),
	)
	if err != nil {
		t.Fatalf("create operation for outcome test: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if maxAttempts > 0 {
		_, err := fixture.pool.Exec(ctx, `
			UPDATE operations
			SET max_attempts = $2
			WHERE id = $1::uuid
		`, created.Operation.ID, maxAttempts)
		if err != nil {
			t.Fatalf("set operation attempt limit: %v", err)
		}
	}

	claimed, err := claimOperation(
		ctx,
		fixture.pool,
		created.Operation.ID,
		"worker-outcome-test",
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim operation for outcome test: %v", err)
	}

	return fixture, claimed
}

func TestCompleteOperationIntegrationRecordsSuccess(t *testing.T) {
	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	completed, err := completeOperation(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		claimed.LeaseOwner,
	)
	if err != nil {
		t.Fatalf("complete operation: %v", err)
	}

	if completed.Status != operationStatusSucceeded {
		t.Errorf("status = %q, want succeeded", completed.Status)
	}
	if completed.FinishedAt == nil {
		t.Error("finished_at was not set")
	}
	if completed.ErrorCode != nil {
		t.Errorf("error code = %q, want nil", *completed.ErrorCode)
	}
	if completed.ErrorSummary != nil {
		t.Errorf("error summary = %q, want nil", *completed.ErrorSummary)
	}

	var status string
	var attemptCount int
	var leaseOwner *string
	var leaseExpiresAt *time.Time
	var finishedAt *time.Time
	err = fixture.pool.QueryRow(ctx, `
		SELECT
			status,
			attempt_count,
			lease_owner,
			lease_expires_at,
			finished_at
		FROM operations
		WHERE id = $1::uuid
	`, claimed.Operation.ID).Scan(
		&status,
		&attemptCount,
		&leaseOwner,
		&leaseExpiresAt,
		&finishedAt,
	)
	if err != nil {
		t.Fatalf("read completed operation: %v", err)
	}

	if status != string(operationStatusSucceeded) {
		t.Errorf("persisted status = %q, want succeeded", status)
	}
	if attemptCount != 1 {
		t.Errorf("attempt count = %d, want 1", attemptCount)
	}
	if leaseOwner != nil {
		t.Errorf("lease owner = %q, want NULL", *leaseOwner)
	}
	if leaseExpiresAt != nil {
		t.Errorf("lease expiry = %v, want NULL", *leaseExpiresAt)
	}
	if finishedAt == nil {
		t.Error("persisted finished_at is NULL")
	}

	var eventCount int
	var actor string
	var fromStatus string
	var toStatus string
	var attemptDetail string
	err = fixture.pool.QueryRow(ctx, `
		SELECT
			count(*),
			MAX(actor_subject),
			MAX(from_status),
			MAX(to_status),
			MAX(details->>'attemptCount')
		FROM operation_events
		WHERE operation_id = $1::uuid
		  AND event_type = 'operation.succeeded'
	`, claimed.Operation.ID).Scan(
		&eventCount,
		&actor,
		&fromStatus,
		&toStatus,
		&attemptDetail,
	)
	if err != nil {
		t.Fatalf("read success history event: %v", err)
	}

	if eventCount != 1 {
		t.Errorf("success history event count = %d, want 1", eventCount)
	}
	if actor != "worker-outcome-test" {
		t.Errorf("event actor = %q, want worker-outcome-test", actor)
	}
	if fromStatus != string(operationStatusRunning) {
		t.Errorf("from status = %q, want running", fromStatus)
	}
	if toStatus != string(operationStatusSucceeded) {
		t.Errorf("to status = %q, want succeeded", toStatus)
	}
	if attemptDetail != "1" {
		t.Errorf("event attempt count = %q, want 1", attemptDetail)
	}
}

func TestRetryOperationIntegrationSchedulesRetry(t *testing.T) {
	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const retryDelay = 45 * time.Second
	const errorCode = "build_timeout"
	const errorSummary = "Build exceeded its execution deadline"

	retried, err := retryOperation(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		claimed.LeaseOwner,
		errorCode,
		errorSummary,
		retryDelay,
	)
	if err != nil {
		t.Fatalf("schedule operation retry: %v", err)
	}

	if retried.Status != operationStatusRetryWait {
		t.Errorf("status = %q, want retry_wait", retried.Status)
	}
	if retried.FinishedAt != nil {
		t.Errorf("finished_at = %v, want nil for a retry", retried.FinishedAt)
	}
	if retried.ErrorCode == nil || *retried.ErrorCode != errorCode {
		t.Errorf("error code = %v, want %q", retried.ErrorCode, errorCode)
	}
	if retried.ErrorSummary == nil || *retried.ErrorSummary != errorSummary {
		t.Errorf("error summary = %v, want %q", retried.ErrorSummary, errorSummary)
	}

	var status string
	var retryIsInExpectedWindow bool
	var leaseOwner *string
	var leaseExpiresAt *time.Time
	err = fixture.pool.QueryRow(ctx, `
		SELECT
			status,
			next_attempt_at > now() + interval '40 seconds'
				AND next_attempt_at < now() + interval '46 seconds',
			lease_owner,
			lease_expires_at
		FROM operations
		WHERE id = $1::uuid
	`, claimed.Operation.ID).Scan(
		&status,
		&retryIsInExpectedWindow,
		&leaseOwner,
		&leaseExpiresAt,
	)
	if err != nil {
		t.Fatalf("read scheduled retry: %v", err)
	}

	if status != string(operationStatusRetryWait) {
		t.Errorf("persisted status = %q, want retry_wait", status)
	}
	if !retryIsInExpectedWindow {
		t.Error("next_attempt_at is outside the expected retry window")
	}
	if leaseOwner != nil {
		t.Errorf("lease owner = %q, want NULL after scheduling retry", *leaseOwner)
	}
	if leaseExpiresAt != nil {
		t.Errorf("lease expiry = %v, want NULL after scheduling retry", *leaseExpiresAt)
	}

	var eventCount int
	var fromStatus string
	var toStatus string
	var actor string
	err = fixture.pool.QueryRow(ctx, `
		SELECT
			count(*),
			MAX(from_status),
			MAX(to_status),
			MAX(actor_subject)
		FROM operation_events
		WHERE operation_id = $1::uuid
		  AND event_type = 'operation.retry_scheduled'
	`, claimed.Operation.ID).Scan(
		&eventCount,
		&fromStatus,
		&toStatus,
		&actor,
	)
	if err != nil {
		t.Fatalf("read retry history event: %v", err)
	}

	if eventCount != 1 {
		t.Errorf("retry history event count = %d, want 1", eventCount)
	}
	if fromStatus != string(operationStatusRunning) {
		t.Errorf("from status = %q, want running", fromStatus)
	}
	if toStatus != string(operationStatusRetryWait) {
		t.Errorf("to status = %q, want retry_wait", toStatus)
	}
	if actor != "worker-outcome-test" {
		t.Errorf("event actor = %q, want worker-outcome-test", actor)
	}
}

func TestFailOperationIntegrationRecordsTerminalFailure(t *testing.T) {
	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const errorCode = "invalid_build_configuration"
	const errorSummary = "Required build configuration is missing"

	failed, err := failOperation(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		claimed.LeaseOwner,
		errorCode,
		errorSummary,
	)
	if err != nil {
		t.Fatalf("fail operation: %v", err)
	}

	if failed.Status != operationStatusFailed {
		t.Errorf("status = %q, want failed", failed.Status)
	}
	if failed.FinishedAt == nil {
		t.Error("finished_at was not set")
	}
	if failed.ErrorCode == nil || *failed.ErrorCode != errorCode {
		t.Errorf("error code = %v, want %q", failed.ErrorCode, errorCode)
	}
	if failed.ErrorSummary == nil || *failed.ErrorSummary != errorSummary {
		t.Errorf("error summary = %v, want %q", failed.ErrorSummary, errorSummary)
	}

	var status string
	var leaseOwner *string
	var leaseExpiresAt *time.Time
	var finishedAt *time.Time
	err = fixture.pool.QueryRow(ctx, `
		SELECT status, lease_owner, lease_expires_at, finished_at
		FROM operations
		WHERE id = $1::uuid
	`, claimed.Operation.ID).Scan(
		&status,
		&leaseOwner,
		&leaseExpiresAt,
		&finishedAt,
	)
	if err != nil {
		t.Fatalf("read failed operation: %v", err)
	}

	if status != string(operationStatusFailed) {
		t.Errorf("persisted status = %q, want failed", status)
	}
	if leaseOwner != nil {
		t.Errorf("lease owner = %q, want NULL", *leaseOwner)
	}
	if leaseExpiresAt != nil {
		t.Errorf("lease expiry = %v, want NULL", *leaseExpiresAt)
	}
	if finishedAt == nil {
		t.Error("persisted finished_at is NULL")
	}

	var eventCount int
	var actor string
	var fromStatus string
	var toStatus string
	err = fixture.pool.QueryRow(ctx, `
		SELECT
			count(*),
			MAX(actor_subject),
			MAX(from_status),
			MAX(to_status)
		FROM operation_events
		WHERE operation_id = $1::uuid
		  AND event_type = 'operation.failed'
	`, claimed.Operation.ID).Scan(
		&eventCount,
		&actor,
		&fromStatus,
		&toStatus,
	)
	if err != nil {
		t.Fatalf("read failure history event: %v", err)
	}

	if eventCount != 1 {
		t.Errorf("failure history event count = %d, want 1", eventCount)
	}
	if actor != "worker-outcome-test" {
		t.Errorf("event actor = %q, want worker-outcome-test", actor)
	}
	if fromStatus != string(operationStatusRunning) {
		t.Errorf("from status = %q, want running", fromStatus)
	}
	if toStatus != string(operationStatusFailed) {
		t.Errorf("to status = %q, want failed", toStatus)
	}
}

func TestRetryOperationIntegrationFailsWhenAttemptsAreExhausted(t *testing.T) {
	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	failed, err := retryOperation(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		claimed.LeaseOwner,
		"build_failed",
		"Build failed on its final attempt",
		time.Second,
	)
	if err != nil {
		t.Fatalf("record exhausted retry: %v", err)
	}

	if failed.Status != operationStatusFailed {
		t.Errorf("status = %q, want failed", failed.Status)
	}
	if failed.FinishedAt == nil {
		t.Error("finished_at was not set for exhausted operation")
	}

	var eventCount int
	var status string
	var retryExhausted string
	var leaseOwner *string
	err = fixture.pool.QueryRow(ctx, `
		SELECT
			o.status,
			o.lease_owner,
			(
				SELECT count(*)
				FROM operation_events e
				WHERE e.operation_id = o.id
				  AND e.event_type = 'operation.failed'
			),
			(
				SELECT MAX(e.details->>'retryExhausted')
				FROM operation_events e
				WHERE e.operation_id = o.id
				  AND e.event_type = 'operation.failed'
			)
		FROM operations o
		WHERE o.id = $1::uuid
	`, claimed.Operation.ID).Scan(
		&status,
		&leaseOwner,
		&eventCount,
		&retryExhausted,
	)
	if err != nil {
		t.Fatalf("read exhausted operation: %v", err)
	}

	if status != string(operationStatusFailed) {
		t.Errorf("persisted status = %q, want failed", status)
	}
	if leaseOwner != nil {
		t.Errorf("lease owner = %q, want NULL", *leaseOwner)
	}
	if eventCount != 1 {
		t.Errorf("failure event count = %d, want 1", eventCount)
	}
	if retryExhausted != "true" {
		t.Errorf("retryExhausted = %q, want true", retryExhausted)
	}
}

func TestOperationOutcomeIntegrationRejectsExpiredLease(t *testing.T) {
	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := fixture.pool.Exec(ctx, `
		UPDATE operations
		SET lease_expires_at = now() - interval '1 second'
		WHERE id = $1::uuid
		  AND lease_owner = $2
	`, claimed.Operation.ID, claimed.LeaseOwner)
	if err != nil {
		t.Fatalf("expire operation lease: %v", err)
	}
	if result.RowsAffected() != 1 {
		t.Fatalf("expired lease update affected %d rows, want 1", result.RowsAffected())
	}

	if _, err := completeOperation(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		claimed.LeaseOwner,
	); !errors.Is(err, errOperationLeaseNotOwned) {
		t.Fatalf("complete with expired lease error = %v, want lease-not-owned", err)
	}

	var status string
	var attemptCount int
	var leaseOwner *string
	var finishedAt *time.Time
	err = fixture.pool.QueryRow(ctx, `
		SELECT status, attempt_count, lease_owner, finished_at
		FROM operations
		WHERE id = $1::uuid
	`, claimed.Operation.ID).Scan(
		&status,
		&attemptCount,
		&leaseOwner,
		&finishedAt,
	)
	if err != nil {
		t.Fatalf("read operation after rejected stale outcome: %v", err)
	}

	if status != string(operationStatusRunning) {
		t.Errorf("status = %q, want running", status)
	}
	if attemptCount != 1 {
		t.Errorf("attempt count = %d, want 1", attemptCount)
	}
	if leaseOwner == nil || *leaseOwner != claimed.LeaseOwner {
		t.Errorf("lease owner = %v, want original lease token", leaseOwner)
	}
	if finishedAt != nil {
		t.Errorf("finished_at = %v, want nil", finishedAt)
	}
}

func TestOperationOutcomeIntegrationRollsBackWhenHistoryInsertFails(
	t *testing.T,
) {
	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 0)
	installOperationOutcomeHistoryFailureTrigger(t, fixture)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := completeOperation(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		claimed.LeaseOwner,
	); err == nil {
		t.Fatal("complete operation error = nil, want simulated history insert failure")
	}

	var status string
	var leaseOwner *string
	var leaseExpiresAt *time.Time
	var finishedAt *time.Time
	err := fixture.pool.QueryRow(ctx, `
		SELECT status, lease_owner, lease_expires_at, finished_at
		FROM operations
		WHERE id = $1::uuid
	`, claimed.Operation.ID).Scan(
		&status,
		&leaseOwner,
		&leaseExpiresAt,
		&finishedAt,
	)
	if err != nil {
		t.Fatalf("read operation after failed outcome transaction: %v", err)
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

	var successEventCount int
	if err := fixture.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM operation_events
		WHERE operation_id = $1::uuid
		  AND event_type = 'operation.succeeded'
	`, claimed.Operation.ID).Scan(&successEventCount); err != nil {
		t.Fatalf("count success events after rollback: %v", err)
	}
	if successEventCount != 0 {
		t.Errorf("success events after rollback = %d, want 0", successEventCount)
	}
}

func installOperationOutcomeHistoryFailureTrigger(
	t *testing.T,
	fixture *operationStoreTestFixture,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const triggerName = "nexvarin_test_fail_operation_outcome_insert"
	const functionName = "nexvarin_test_fail_operation_outcome_insert"

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
			t.Errorf("drop operation outcome failure trigger: %v", err)
		}
		if _, err := fixture.pool.Exec(cleanupCtx, dropFunction); err != nil {
			t.Errorf("drop operation outcome failure function: %v", err)
		}
	})

	if _, err := fixture.pool.Exec(ctx, dropTrigger); err != nil {
		t.Fatalf("remove previous operation outcome trigger: %v", err)
	}
	if _, err := fixture.pool.Exec(ctx, dropFunction); err != nil {
		t.Fatalf("remove previous operation outcome function: %v", err)
	}

	createFunction := `
		CREATE FUNCTION public.nexvarin_test_fail_operation_outcome_insert()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			IF NEW.event_type = 'operation.succeeded' THEN
				RAISE EXCEPTION 'simulated operation outcome history failure';
			END IF;
			RETURN NEW;
		END;
		$$
	`
	if _, err := fixture.pool.Exec(ctx, createFunction); err != nil {
		t.Fatalf("create operation outcome failure function: %v", err)
	}

	createTrigger := `
		CREATE TRIGGER nexvarin_test_fail_operation_outcome_insert
		BEFORE INSERT ON public.operation_events
		FOR EACH ROW
		EXECUTE FUNCTION public.nexvarin_test_fail_operation_outcome_insert()
	`
	if _, err := fixture.pool.Exec(ctx, createTrigger); err != nil {
		t.Fatalf("create operation outcome failure trigger: %v", err)
	}
}
