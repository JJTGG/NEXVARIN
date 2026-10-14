package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func createOperationForClaimTest(
	t *testing.T,
) (*operationStoreTestFixture, string) {
	t.Helper()

	fixture := newOperationStoreTestFixture(t)
	result, err := fixture.create(
		"claim-"+strings.ToLower(t.Name()),
		buildOperationRequestForTest(strings.Repeat("a", 40)),
	)
	if err != nil {
		t.Fatalf("create operation for claim test: %v", err)
	}

	return fixture, result.Operation.ID
}

func TestClaimOperationIntegrationClaimsQueuedOperationAndRecordsHistory(
	t *testing.T,
) {
	fixture, operationID := createOperationForClaimTest(t)
	const workerID = "worker-claim-test"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	claimed, err := claimOperation(
		ctx,
		fixture.pool,
		operationID,
		workerID,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim queued operation: %v", err)
	}

	if claimed.Operation.ID != operationID {
		t.Errorf("claimed operation ID = %q, want %q", claimed.Operation.ID, operationID)
	}
	if claimed.Operation.Status != operationStatusRunning {
		t.Errorf("status = %q, want running", claimed.Operation.Status)
	}
	if claimed.Operation.AttemptCount != 1 {
		t.Errorf("attempt count = %d, want 1", claimed.Operation.AttemptCount)
	}
	if claimed.Operation.StartedAt == nil {
		t.Error("started_at was not set")
	}
	if !strings.HasPrefix(claimed.LeaseOwner, workerID+":") {
		t.Errorf("lease owner = %q, want %q prefix", claimed.LeaseOwner, workerID+":")
	}
	if !claimed.LeaseExpiresAt.After(time.Now()) {
		t.Error("lease expiration should be in the future")
	}

	var eventType string
	var fromStatus string
	var toStatus string
	var actorSubject string
	var attemptCount string
	err = fixture.pool.QueryRow(ctx, `
		SELECT
			event_type,
			from_status,
			to_status,
			actor_subject,
			details->>'attemptCount'
		FROM operation_events
		WHERE operation_id = $1::uuid
		  AND event_type = 'operation.claimed'
	`, operationID).Scan(
		&eventType,
		&fromStatus,
		&toStatus,
		&actorSubject,
		&attemptCount,
	)
	if err != nil {
		t.Fatalf("read operation claim history event: %v", err)
	}

	if eventType != "operation.claimed" {
		t.Errorf("event type = %q, want operation.claimed", eventType)
	}
	if fromStatus != string(operationStatusQueued) {
		t.Errorf("from status = %q, want queued", fromStatus)
	}
	if toStatus != string(operationStatusRunning) {
		t.Errorf("to status = %q, want running", toStatus)
	}
	if actorSubject != workerID {
		t.Errorf("actor subject = %q, want %q", actorSubject, workerID)
	}
	if attemptCount != "1" {
		t.Errorf("event attempt count = %q, want 1", attemptCount)
	}

	var historyCount int
	if err := fixture.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM operation_events
		WHERE operation_id = $1::uuid
	`, operationID).Scan(&historyCount); err != nil {
		t.Fatalf("count operation history events: %v", err)
	}
	if historyCount != 2 {
		t.Errorf("history event count = %d, want initial queued event plus claim event (2)", historyCount)
	}
}

func TestClaimOperationIntegrationAllowsOnlyOneConcurrentClaim(t *testing.T) {
	fixture, operationID := createOperationForClaimTest(t)

	type claimOutcome struct {
		claimed claimedOperation
		err     error
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	start := make(chan struct{})
	results := make(chan claimOutcome, 2)
	for _, workerID := range []string{"worker-race-a", "worker-race-b"} {
		workerID := workerID
		go func() {
			<-start
			claimed, err := claimOperation(
				ctx,
				fixture.pool,
				operationID,
				workerID,
				time.Minute,
			)
			results <- claimOutcome{claimed: claimed, err: err}
		}()
	}
	close(start)

	claimedCount := 0
	notClaimableCount := 0
	for range 2 {
		outcome := <-results
		switch {
		case outcome.err == nil:
			claimedCount++
			if outcome.claimed.Operation.ID != operationID {
				t.Errorf("claimed operation ID = %q, want %q", outcome.claimed.Operation.ID, operationID)
			}
			if outcome.claimed.LeaseOwner == "" {
				t.Error("successful claim returned an empty lease owner")
			}
		case errors.Is(outcome.err, errOperationNotClaimable):
			notClaimableCount++
		default:
			t.Errorf("concurrent claim returned unexpected error: %v", outcome.err)
		}
	}

	if claimedCount != 1 || notClaimableCount != 1 {
		t.Fatalf(
			"successful claims = %d, not-claimable results = %d; want one of each",
			claimedCount,
			notClaimableCount,
		)
	}

	var status string
	var attemptCount int
	var historyCount int
	if err := fixture.pool.QueryRow(ctx, `
		SELECT status, attempt_count
		FROM operations
		WHERE id = $1::uuid
	`, operationID).Scan(&status, &attemptCount); err != nil {
		t.Fatalf("read operation after concurrent claims: %v", err)
	}
	if status != string(operationStatusRunning) {
		t.Errorf("persisted status = %q, want running", status)
	}
	if attemptCount != 1 {
		t.Errorf("persisted attempt count = %d, want 1", attemptCount)
	}

	if err := fixture.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM operation_events
		WHERE operation_id = $1::uuid
		  AND event_type = 'operation.claimed'
	`, operationID).Scan(&historyCount); err != nil {
		t.Fatalf("count claim history events after race: %v", err)
	}
	if historyCount != 1 {
		t.Errorf("operation.claimed history count = %d, want 1", historyCount)
	}
}

func TestClaimOperationIntegrationRejectsRetryBeforeDueTime(t *testing.T) {
	fixture, operationID := createOperationForClaimTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := fixture.pool.Exec(ctx, `
		UPDATE operations
		SET status = 'retry_wait',
		    next_attempt_at = now() + interval '1 hour'
		WHERE id = $1::uuid
	`, operationID)
	if err != nil {
		t.Fatalf("schedule future retry: %v", err)
	}
	if result.RowsAffected() != 1 {
		t.Fatalf("scheduled retry update affected %d rows, want 1", result.RowsAffected())
	}

	if _, err := claimOperation(
		ctx,
		fixture.pool,
		operationID,
		"worker-early-retry",
		time.Minute,
	); !errors.Is(err, errOperationNotClaimable) {
		t.Fatalf("claim before retry due time error = %v, want operation-not-claimable", err)
	}

	var status string
	var attemptCount int
	var leaseOwner *string
	if err := fixture.pool.QueryRow(ctx, `
		SELECT status, attempt_count, lease_owner
		FROM operations
		WHERE id = $1::uuid
	`, operationID).Scan(&status, &attemptCount, &leaseOwner); err != nil {
		t.Fatalf("read operation after early claim attempt: %v", err)
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

func TestClaimOperationIntegrationRejectsExhaustedAttempts(t *testing.T) {
	fixture, operationID := createOperationForClaimTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := fixture.pool.Exec(ctx, `
		UPDATE operations
		SET attempt_count = max_attempts
		WHERE id = $1::uuid
	`, operationID)
	if err != nil {
		t.Fatalf("exhaust operation attempt budget: %v", err)
	}
	if result.RowsAffected() != 1 {
		t.Fatalf("attempt budget update affected %d rows, want 1", result.RowsAffected())
	}

	if _, err := claimOperation(
		ctx,
		fixture.pool,
		operationID,
		"worker-exhausted",
		time.Minute,
	); !errors.Is(err, errOperationAttemptsExhausted) {
		t.Fatalf("claim exhausted operation error = %v, want attempts-exhausted", err)
	}

	var status string
	var attemptCount int
	var leaseOwner *string
	if err := fixture.pool.QueryRow(ctx, `
		SELECT status, attempt_count, lease_owner
		FROM operations
		WHERE id = $1::uuid
	`, operationID).Scan(&status, &attemptCount, &leaseOwner); err != nil {
		t.Fatalf("read operation after exhausted claim attempt: %v", err)
	}
	if status != string(operationStatusQueued) {
		t.Errorf("status = %q, want queued", status)
	}
	if attemptCount != 3 {
		t.Errorf("attempt count = %d, want default maximum of 3", attemptCount)
	}
	if leaseOwner != nil {
		t.Errorf("lease owner = %q, want NULL", *leaseOwner)
	}
}

func TestClaimOperationIntegrationRollsBackWhenHistoryInsertFails(t *testing.T) {
	fixture, operationID := createOperationForClaimTest(t)
	installOperationClaimHistoryFailureTrigger(t, fixture)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := claimOperation(
		ctx,
		fixture.pool,
		operationID,
		"worker-history-failure",
		time.Minute,
	); err == nil {
		t.Fatal("claim operation error = nil, want simulated history insert failure")
	}

	var status string
	var attemptCount int
	var leaseOwner *string
	var leaseExpiresAt *time.Time
	var startedAt *time.Time
	if err := fixture.pool.QueryRow(ctx, `
		SELECT status, attempt_count, lease_owner, lease_expires_at, started_at
		FROM operations
		WHERE id = $1::uuid
	`, operationID).Scan(
		&status,
		&attemptCount,
		&leaseOwner,
		&leaseExpiresAt,
		&startedAt,
	); err != nil {
		t.Fatalf("read operation after failed claim transaction: %v", err)
	}

	if status != string(operationStatusQueued) {
		t.Errorf("status after rollback = %q, want queued", status)
	}
	if attemptCount != 0 {
		t.Errorf("attempt count after rollback = %d, want 0", attemptCount)
	}
	if leaseOwner != nil {
		t.Errorf("lease owner after rollback = %q, want NULL", *leaseOwner)
	}
	if leaseExpiresAt != nil {
		t.Errorf("lease expiry after rollback = %v, want NULL", *leaseExpiresAt)
	}
	if startedAt != nil {
		t.Errorf("started_at after rollback = %v, want NULL", *startedAt)
	}

	var claimEventCount int
	if err := fixture.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM operation_events
		WHERE operation_id = $1::uuid
		  AND event_type = 'operation.claimed'
	`, operationID).Scan(&claimEventCount); err != nil {
		t.Fatalf("count claim events after rollback: %v", err)
	}
	if claimEventCount != 0 {
		t.Errorf("claim history events after rollback = %d, want 0", claimEventCount)
	}
}

func installOperationClaimHistoryFailureTrigger(
	t *testing.T,
	fixture *operationStoreTestFixture,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const triggerName = "nexvarin_test_fail_operation_claim_insert"
	const functionName = "nexvarin_test_fail_operation_claim_insert"
	dropTrigger := "DROP TRIGGER IF EXISTS " + triggerName + " ON public.operation_events"
	dropFunction := "DROP FUNCTION IF EXISTS public." + functionName + "()"

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()

		if _, err := fixture.pool.Exec(cleanupCtx, dropTrigger); err != nil {
			t.Errorf("drop operation claim failure trigger: %v", err)
		}
		if _, err := fixture.pool.Exec(cleanupCtx, dropFunction); err != nil {
			t.Errorf("drop operation claim failure function: %v", err)
		}
	})

	if _, err := fixture.pool.Exec(ctx, dropTrigger); err != nil {
		t.Fatalf("remove previous operation claim failure trigger: %v", err)
	}
	if _, err := fixture.pool.Exec(ctx, dropFunction); err != nil {
		t.Fatalf("remove previous operation claim failure function: %v", err)
	}

	createFunction := `
		CREATE FUNCTION public.nexvarin_test_fail_operation_claim_insert()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			IF NEW.event_type = 'operation.claimed' THEN
				RAISE EXCEPTION 'simulated operation claim history failure';
			END IF;
			RETURN NEW;
		END;
		$$
	`
	if _, err := fixture.pool.Exec(ctx, createFunction); err != nil {
		t.Fatalf("create operation claim failure function: %v", err)
	}

	createTrigger := `
		CREATE TRIGGER nexvarin_test_fail_operation_claim_insert
		BEFORE INSERT ON public.operation_events
		FOR EACH ROW
		EXECUTE FUNCTION public.nexvarin_test_fail_operation_claim_insert()
	`
	if _, err := fixture.pool.Exec(ctx, createTrigger); err != nil {
		t.Fatalf("create operation claim failure trigger: %v", err)
	}
}
