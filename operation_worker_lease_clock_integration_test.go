package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestOperationOutcomeRejectsLeaseExpiredWhileWaitingForRowLock(
	t *testing.T,
) {
	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 0)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	// Update the lease while holding the operation row lock. The new
	// expiry is committed only when this transaction is released.
	lockTx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin row-lock transaction: %v", err)
	}
	defer func() {
		_ = lockTx.Rollback(context.Background())
	}()

	var leaseExpiresAt time.Time
	err = lockTx.QueryRow(ctx, `
		UPDATE operations
		SET lease_expires_at =
			clock_timestamp() + interval '1 second'
		WHERE id = $1::uuid
		  AND lease_owner = $2
		RETURNING lease_expires_at
	`, claimed.Operation.ID, claimed.LeaseOwner).Scan(
		&leaseExpiresAt,
	)
	if err != nil {
		t.Fatalf("set short lease while holding row lock: %v", err)
	}

	type outcomeResult struct {
		record operationRecord
		err    error
	}

	outcomes := make(chan outcomeResult, 1)

	go func() {
		record, outcomeErr := completeOperation(
			ctx,
			fixture.pool,
			claimed.Operation.ID,
			claimed.LeaseOwner,
		)
		outcomes <- outcomeResult{
			record: record,
			err:    outcomeErr,
		}
	}()

	// Confirm the completion transaction actually reached SELECT FOR UPDATE
	// and is blocked. This ensures its transaction began before the lease
	// expired, which is essential to reproduce the stale-now() edge case.
	lockWaitDeadline := time.Now().Add(3 * time.Second)

	for {
		var waiting bool
		err := fixture.pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE datname = current_database()
				  AND pid <> pg_backend_pid()
				  AND state = 'active'
				  AND wait_event_type = 'Lock'
				  AND query ILIKE '%max_attempts%'
				  AND query ILIKE '%FROM operations%'
				  AND query ILIKE '%FOR UPDATE%'
			)
		`).Scan(&waiting)
		if err != nil {
			t.Fatalf("inspect PostgreSQL lock wait: %v", err)
		}

		if waiting {
			break
		}

		select {
		case result := <-outcomes:
			t.Fatalf(
				"completion returned before the expected lock wait: %v",
				result.err,
			)
		default:
		}

		if time.Now().After(lockWaitDeadline) {
			t.Fatal("completion transaction did not wait on the operation row lock")
		}

		time.Sleep(10 * time.Millisecond)
	}

	// Wait using the database clock rather than relying on the application
	// host's clock. The row remains locked while the lease expires.
	expiryDeadline := time.Now().Add(4 * time.Second)

	for {
		var expired bool
		err := fixture.pool.QueryRow(ctx, `
			SELECT clock_timestamp() >= $1::timestamptz
		`, leaseExpiresAt).Scan(&expired)
		if err != nil {
			t.Fatalf("check lease expiration: %v", err)
		}

		if expired {
			break
		}

		if time.Now().After(expiryDeadline) {
			t.Fatal("database lease did not expire within the expected time")
		}

		time.Sleep(10 * time.Millisecond)
	}

	// Once the lease has expired, allow the waiting transaction to proceed.
	if err := lockTx.Commit(ctx); err != nil {
		t.Fatalf("release operation row lock: %v", err)
	}

	result := <-outcomes
	if !errors.Is(result.err, errOperationLeaseNotOwned) {
		t.Fatalf(
			"completion error = %v, want lease-not-owned",
			result.err,
		)
	}

	var status string
	var attemptCount int
	var leaseOwner *string
	var persistedExpiry time.Time
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
		&persistedExpiry,
		&finishedAt,
	)
	if err != nil {
		t.Fatalf("read operation after rejected completion: %v", err)
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
	if persistedExpiry.After(time.Now()) {
		t.Errorf("lease expiry = %v, want an expired lease", persistedExpiry)
	}
	if finishedAt != nil {
		t.Errorf("finished_at = %v, want NULL", finishedAt)
	}

	var successEvents int
	if err := fixture.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM operation_events
		WHERE operation_id = $1::uuid
		  AND event_type = 'operation.succeeded'
	`, claimed.Operation.ID).Scan(&successEvents); err != nil {
		t.Fatalf("count success history events: %v", err)
	}
	if successEvents != 0 {
		t.Errorf("success events = %d, want 0", successEvents)
	}
}
