package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	errOperationNotFound = errors.New(
		"operation was not found",
	)
	errOperationNotClaimable = errors.New(
		"operation is not due and claimable",
	)
	errOperationAttemptsExhausted = errors.New(
		"operation has exhausted its execution attempts",
	)
)

type claimedOperation struct {
	Operation      operationRecord
	LeaseOwner     string
	LeaseExpiresAt time.Time
}

// claimOperation atomically claims one due operation for execution.
//
// Only queued and retry_wait operations are eligible. Expired running
// operations are deliberately excluded because lease expiry alone does
// not establish whether an external side effect occurred.
//
// A successful claim transitions the operation to running, acquires a
// unique lease token, increments its attempt count, and appends a history
// event in one transaction.
//
// The caller must retain LeaseOwner and use that exact token for subsequent
// lease-protected operations.
func claimOperation(
	ctx context.Context,
	pool *pgxpool.Pool,
	operationID string,
	workerID string,
	leaseDuration time.Duration,
) (claimedOperation, error) {
	var empty claimedOperation

	if pool == nil {
		return empty, errors.New("database pool is required")
	}

	normalizedID, err := normalizeOperationUUID(operationID)
	if err != nil {
		return empty, fmt.Errorf("operation ID: %w", err)
	}

	workerID, err = normalizeOperationWorkerID(workerID)
	if err != nil {
		return empty, err
	}

	if leaseDuration < time.Microsecond {
		return empty, errors.New(
			"lease duration must be at least one microsecond",
		)
	}

	leaseMicroseconds := durationMicroseconds(leaseDuration)

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return empty, fmt.Errorf(
			"begin operation claim transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	// Serialize competing claim attempts for this operation. The database
	// clock decides whether a retry is due.
	var currentStatus string
	var attemptCount int
	var maxAttempts int
	var isDue bool

	err = tx.QueryRow(ctx, `
		SELECT
			status,
			attempt_count,
			max_attempts,
			next_attempt_at <= now()
		FROM operations
		WHERE id = $1::uuid
		FOR UPDATE
	`, normalizedID).Scan(
		&currentStatus,
		&attemptCount,
		&maxAttempts,
		&isDue,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return empty, errOperationNotFound
	}
	if err != nil {
		return empty, fmt.Errorf(
			"load operation for claim: %w",
			err,
		)
	}

	status := operationStatus(currentStatus)

	if !isKnownOperationStatus(status) {
		return empty, fmt.Errorf(
			"operation has unknown persisted status %q",
			currentStatus,
		)
	}

	if !canTransitionOperationStatus(
		status,
		operationStatusRunning,
	) || !isDue {
		return empty, errOperationNotClaimable
	}

	if attemptCount >= maxAttempts {
		return empty, errOperationAttemptsExhausted
	}

	// Recheck eligibility in the update as a defensive database guard.
	// The row lock already serializes claim attempts for this operation.
	record, err := scanOperationRecord(tx.QueryRow(ctx, `
		UPDATE operations
		SET
			status = 'running',
			lease_owner = $2 || ':' || gen_random_uuid()::text,
			lease_expires_at = now()
				+ ($3::double precision * interval '1 microsecond'),
			attempt_count = attempt_count + 1,
			started_at = COALESCE(started_at, now()),
			updated_at = now()
		WHERE id = $1::uuid
		  AND status IN ('queued', 'retry_wait')
		  AND next_attempt_at <= now()
		  AND attempt_count < max_attempts
		RETURNING `+operationRecordColumns,
		normalizedID,
		workerID,
		leaseMicroseconds,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return empty, errOperationNotClaimable
	}
	if err != nil {
		return empty, fmt.Errorf(
			"acquire operation lease: %w",
			err,
		)
	}

	var leaseOwner string
	var leaseExpiresAt time.Time

	if err := tx.QueryRow(ctx, `
		SELECT lease_owner, lease_expires_at
		FROM operations
		WHERE id = $1::uuid
	`, normalizedID).Scan(
		&leaseOwner,
		&leaseExpiresAt,
	); err != nil {
		return empty, fmt.Errorf(
			"read claimed operation lease: %w",
			err,
		)
	}

	details, err := json.Marshal(struct {
		AttemptCount int `json:"attemptCount"`
	}{
		AttemptCount: record.AttemptCount,
	})
	if err != nil {
		return empty, fmt.Errorf(
			"encode operation claim event: %w",
			err,
		)
	}

	// Record the state transition in the same transaction as the claim.
	if _, err := tx.Exec(ctx, `
		INSERT INTO operation_events (
			project_id,
			operation_id,
			event_type,
			from_status,
			to_status,
			actor_subject,
			details
		)
		VALUES (
			$1::uuid,
			$2::uuid,
			'operation.claimed',
			$3,
			'running',
			$4,
			$5::jsonb
		)
	`,
		record.ProjectID,
		record.ID,
		currentStatus,
		workerID,
		string(details),
	); err != nil {
		return empty, fmt.Errorf(
			"record operation claim event: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return empty, fmt.Errorf(
			"commit operation claim transaction: %w",
			err,
		)
	}

	return claimedOperation{
		Operation:      record,
		LeaseOwner:     leaseOwner,
		LeaseExpiresAt: leaseExpiresAt,
	}, nil
}

func normalizeOperationWorkerID(workerID string) (string, error) {
	workerID = strings.TrimSpace(workerID)

	if workerID == "" {
		return "", errors.New("worker ID is required")
	}

	// A separator and generated UUID are appended to each lease token.
	if len(workerID) > 218 ||
		!utf8.ValidString(workerID) ||
		strings.ContainsAny(workerID, "\x00\r\n") {
		return "", errors.New("worker ID is invalid")
	}

	return workerID, nil
}