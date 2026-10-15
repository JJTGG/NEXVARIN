package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

type operationWorkerRunnerTestExecutor struct {
	result   operationExecutionResult
	calls    int
	lastTask operationWorkerTask
}

func (executor *operationWorkerRunnerTestExecutor) Execute(
	_ context.Context,
	task operationWorkerTask,
) operationExecutionResult {
	executor.calls++
	executor.lastTask = task

	return executor.result
}

func newOperationWorkerRunnerForIntegrationTest(
	poolFixture *operationStoreTestFixture,
	executor operationExecutor,
) operationWorkerRunner {
	return operationWorkerRunner{
		Pool:              poolFixture.pool,
		WorkerID:          "worker-runner-integration",
		Executor:          executor,
		LeaseDuration:     time.Minute,
		HeartbeatInterval: time.Second,
		PollInterval:      10 * time.Millisecond,
	}
}

func TestOperationWorkerRunnerIntegrationExecutesAndCompletesOperation(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	revision := strings.Repeat("a", 40)
	operationID := createQueuedOperationForQueueTest(
		t,
		fixture,
		"runner-success",
		revision,
	)

	executor := &operationWorkerRunnerTestExecutor{
		result: operationExecutionResult{
			Disposition: operationExecutionSucceeded,
		},
	}
	worker := newOperationWorkerRunnerForIntegrationTest(
		fixture,
		executor,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	processed, err := worker.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run worker once: %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}

	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
	if executor.lastTask.Claimed.Operation.ID != operationID {
		t.Errorf(
			"executed operation ID = %q, want %q",
			executor.lastTask.Claimed.Operation.ID,
			operationID,
		)
	}
	if executor.lastTask.Request.OperationType != operationTypeBuild {
		t.Errorf(
			"executed operation type = %q, want build",
			executor.lastTask.Request.OperationType,
		)
	}
	if executor.lastTask.Request.SourceRevision != revision {
		t.Errorf(
			"executed source revision = %q, want %q",
			executor.lastTask.Request.SourceRevision,
			revision,
		)
	}

	var (
		status         string
		leaseOwner     *string
		leaseExpiresAt *time.Time
		finishedAt     *time.Time
		errorCode      *string
		errorSummary   *string
	)

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
	`, operationID).Scan(
		&status,
		&leaseOwner,
		&leaseExpiresAt,
		&finishedAt,
		&errorCode,
		&errorSummary,
	)
	if err != nil {
		t.Fatalf("read completed operation: %v", err)
	}

	if status != string(operationStatusSucceeded) {
		t.Errorf("status = %q, want succeeded", status)
	}
	if leaseOwner != nil {
		t.Errorf("lease owner = %q, want NULL", *leaseOwner)
	}
	if leaseExpiresAt != nil {
		t.Errorf("lease expiry = %v, want NULL", *leaseExpiresAt)
	}
	if finishedAt == nil {
		t.Error("finished_at is NULL")
	}
	if errorCode != nil {
		t.Errorf("error code = %q, want NULL", *errorCode)
	}
	if errorSummary != nil {
		t.Errorf("error summary = %q, want NULL", *errorSummary)
	}

	var successEventCount int
	err = fixture.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM operation_events
		WHERE operation_id = $1::uuid
		  AND event_type = 'operation.succeeded'
	`, operationID).Scan(&successEventCount)
	if err != nil {
		t.Fatalf("count success events: %v", err)
	}
	if successEventCount != 1 {
		t.Errorf(
			"success event count = %d, want 1",
			successEventCount,
		)
	}
}

func TestOperationWorkerRunnerIntegrationSchedulesSafeRetry(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	operationID := createQueuedOperationForQueueTest(
		t,
		fixture,
		"runner-retry",
		strings.Repeat("b", 40),
	)

	const errorCode = "temporary_build_failure"
	const errorSummary = "Builder stopped before producing an artifact"

	executor := &operationWorkerRunnerTestExecutor{
		result: operationExecutionResult{
			Disposition:  operationExecutionRetry,
			ErrorCode:    errorCode,
			ErrorSummary: errorSummary,
			RetryDelay:   30 * time.Second,
		},
	}
	worker := newOperationWorkerRunnerForIntegrationTest(
		fixture,
		executor,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	processed, err := worker.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run worker once: %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}

	var (
		status            string
		nextAttemptIsSoon bool
		leaseOwner        *string
		leaseExpiresAt    *time.Time
		finishedAt        *time.Time
		storedErrorCode   *string
		storedErrorText   *string
	)

	err = fixture.pool.QueryRow(ctx, `
		SELECT
			status,
			next_attempt_at > clock_timestamp()
				AND next_attempt_at < clock_timestamp()
					+ interval '31 seconds',
			lease_owner,
			lease_expires_at,
			finished_at,
			error_code,
			error_summary
		FROM operations
		WHERE id = $1::uuid
	`, operationID).Scan(
		&status,
		&nextAttemptIsSoon,
		&leaseOwner,
		&leaseExpiresAt,
		&finishedAt,
		&storedErrorCode,
		&storedErrorText,
	)
	if err != nil {
		t.Fatalf("read operation after retry: %v", err)
	}

	if status != string(operationStatusRetryWait) {
		t.Errorf("status = %q, want retry_wait", status)
	}
	if !nextAttemptIsSoon {
		t.Error("next_attempt_at is not within the expected retry window")
	}
	if leaseOwner != nil {
		t.Errorf("lease owner = %q, want NULL", *leaseOwner)
	}
	if leaseExpiresAt != nil {
		t.Errorf("lease expiry = %v, want NULL", *leaseExpiresAt)
	}
	if finishedAt != nil {
		t.Errorf("finished_at = %v, want NULL for retry_wait", *finishedAt)
	}
	if storedErrorCode == nil || *storedErrorCode != errorCode {
		t.Errorf("stored error code = %v, want %q", storedErrorCode, errorCode)
	}
	if storedErrorText == nil || *storedErrorText != errorSummary {
		t.Errorf(
			"stored error summary = %v, want %q",
			storedErrorText,
			errorSummary,
		)
	}

	var retryEventCount int
	err = fixture.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM operation_events
		WHERE operation_id = $1::uuid
		  AND event_type = 'operation.retry_scheduled'
	`, operationID).Scan(&retryEventCount)
	if err != nil {
		t.Fatalf("count retry events: %v", err)
	}
	if retryEventCount != 1 {
		t.Errorf("retry event count = %d, want 1", retryEventCount)
	}
}

func TestOperationWorkerRunnerIntegrationDoesNotExecuteEmptyQueue(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	executor := &operationWorkerRunnerTestExecutor{
		result: operationExecutionResult{
			Disposition: operationExecutionSucceeded,
		},
	}
	worker := newOperationWorkerRunnerForIntegrationTest(
		fixture,
		executor,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	processed, err := worker.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run worker with empty queue: %v", err)
	}
	if processed {
		t.Error("processed = true, want false for an empty queue")
	}
	if executor.calls != 0 {
		t.Errorf("executor calls = %d, want 0", executor.calls)
	}
}
