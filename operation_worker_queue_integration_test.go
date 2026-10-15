package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func createQueuedOperationForQueueTest(
	t *testing.T,
	fixture *operationStoreTestFixture,
	key string,
	revision string,
) string {
	t.Helper()

	result, err := fixture.create(
		key,
		buildOperationRequestForTest(revision),
	)
	if err != nil {
		t.Fatalf("create queued operation: %v", err)
	}

	return result.Operation.ID
}

func TestClaimNextOperationIntegrationSelectsEarliestDueOperation(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	firstID := createQueuedOperationForQueueTest(
		t,
		fixture,
		"queue-order-first",
		strings.Repeat("a", 40),
	)
	secondID := createQueuedOperationForQueueTest(
		t,
		fixture,
		"queue-order-second",
		strings.Repeat("b", 40),
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	// The first-created operation is due later. This makes the test
	// verify scheduling priority rather than merely creation order.
	_, err := fixture.pool.Exec(ctx, `
		UPDATE operations
		SET next_attempt_at = clock_timestamp() - interval '5 minutes'
		WHERE id = $1::uuid
	`, firstID)
	if err != nil {
		t.Fatalf("schedule first operation: %v", err)
	}

	_, err = fixture.pool.Exec(ctx, `
		UPDATE operations
		SET next_attempt_at = clock_timestamp() - interval '10 minutes'
		WHERE id = $1::uuid
	`, secondID)
	if err != nil {
		t.Fatalf("schedule second operation: %v", err)
	}

	claimed, err := claimNextOperation(
		ctx,
		fixture.pool,
		"worker-queue-order",
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim next due operation: %v", err)
	}

	if claimed.Operation.ID != secondID {
		t.Fatalf(
			"claimed operation = %q, want earliest-due operation %q",
			claimed.Operation.ID,
			secondID,
		)
	}

	if claimed.Operation.Status != operationStatusRunning {
		t.Errorf(
			"claimed operation status = %q, want running",
			claimed.Operation.Status,
		)
	}

	var firstStatus string
	if err := fixture.pool.QueryRow(ctx, `
		SELECT status
		FROM operations
		WHERE id = $1::uuid
	`, firstID).Scan(&firstStatus); err != nil {
		t.Fatalf("read first operation status: %v", err)
	}

	if firstStatus != string(operationStatusQueued) {
		t.Errorf(
			"later-due operation status = %q, want queued",
			firstStatus,
		)
	}
}

func TestClaimNextOperationIntegrationSkipsFutureRetry(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	operationID := createQueuedOperationForQueueTest(
		t,
		fixture,
		"queue-future-retry",
		strings.Repeat("c", 40),
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	_, err := fixture.pool.Exec(ctx, `
		UPDATE operations
		SET
			status = 'retry_wait',
			next_attempt_at = clock_timestamp() + interval '1 hour'
		WHERE id = $1::uuid
	`, operationID)
	if err != nil {
		t.Fatalf("schedule future retry: %v", err)
	}

	_, err = claimNextOperation(
		ctx,
		fixture.pool,
		"worker-future-retry",
		time.Minute,
	)
	if !errors.Is(err, errNoDueOperationAvailable) {
		t.Fatalf(
			"claim result error = %v, want no-due-operation",
			err,
		)
	}

	var status string
	var attemptCount int
	var leaseOwner *string
	err = fixture.pool.QueryRow(ctx, `
		SELECT status, attempt_count, lease_owner
		FROM operations
		WHERE id = $1::uuid
	`, operationID).Scan(
		&status,
		&attemptCount,
		&leaseOwner,
	)
	if err != nil {
		t.Fatalf("read future retry operation: %v", err)
	}

	if status != string(operationStatusRetryWait) {
		t.Errorf("status = %q, want retry_wait", status)
	}
	if attemptCount != 0 {
		t.Errorf("attempt count = %d, want 0", attemptCount)
	}
	if leaseOwner != nil {
		t.Errorf("lease owner = %q, want NULL", *leaseOwner)
	}
}

func TestClaimNextOperationIntegrationReturnsEmptyQueue(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	_, err := claimNextOperation(
		ctx,
		fixture.pool,
		"worker-empty-queue",
		time.Minute,
	)
	if !errors.Is(err, errNoDueOperationAvailable) {
		t.Fatalf(
			"empty queue error = %v, want no-due-operation",
			err,
		)
	}
}

func TestClaimNextOperationIntegrationAllowsOnlyOneConcurrentWorker(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	operationID := createQueuedOperationForQueueTest(
		t,
		fixture,
		"queue-concurrent-claim",
		strings.Repeat("d", 40),
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	type claimResult struct {
		claimed claimedOperation
		err     error
	}

	start := make(chan struct{})
	results := make(chan claimResult, 2)

	for _, workerID := range []string{
		"worker-queue-race-a",
		"worker-queue-race-b",
	} {
		workerID := workerID

		go func() {
			<-start

			claimed, err := claimNextOperation(
				ctx,
				fixture.pool,
				workerID,
				time.Minute,
			)

			results <- claimResult{
				claimed: claimed,
				err:     err,
			}
		}()
	}

	close(start)

	successfulClaims := 0
	emptyQueueResults := 0

	for range 2 {
		result := <-results

		switch {
		case result.err == nil:
			successfulClaims++

			if result.claimed.Operation.ID != operationID {
				t.Errorf(
					"claimed operation ID = %q, want %q",
					result.claimed.Operation.ID,
					operationID,
				)
			}

			if result.claimed.LeaseOwner == "" {
				t.Error("successful claim returned an empty lease token")
			}

		case errors.Is(result.err, errNoDueOperationAvailable):
			emptyQueueResults++

		default:
			t.Errorf("unexpected queue claim error: %v", result.err)
		}
	}

	if successfulClaims != 1 || emptyQueueResults != 1 {
		t.Fatalf(
			"successful claims = %d, empty queue results = %d; want one each",
			successfulClaims,
			emptyQueueResults,
		)
	}

	var status string
	var attemptCount int
	var claimEventCount int

	err := fixture.pool.QueryRow(ctx, `
		SELECT status, attempt_count
		FROM operations
		WHERE id = $1::uuid
	`, operationID).Scan(&status, &attemptCount)
	if err != nil {
		t.Fatalf("read operation after concurrent queue claims: %v", err)
	}

	if status != string(operationStatusRunning) {
		t.Errorf("persisted status = %q, want running", status)
	}
	if attemptCount != 1 {
		t.Errorf("persisted attempt count = %d, want 1", attemptCount)
	}

	err = fixture.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM operation_events
		WHERE operation_id = $1::uuid
		  AND event_type = 'operation.claimed'
	`, operationID).Scan(&claimEventCount)
	if err != nil {
		t.Fatalf("count operation claim events: %v", err)
	}

	if claimEventCount != 1 {
		t.Errorf("claim event count = %d, want 1", claimEventCount)
	}
}
