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

var errOutboxLeaseNotOwned = errors.New(
	"outbox event lease is not owned by this worker",
)

type outboxEventRecord struct {
	ID             string
	ProjectID      string
	OperationID    string
	EventType      string
	DedupeKey      string
	Payload        json.RawMessage
	AttemptCount   int32
	LeaseOwner     string
	LeaseExpiresAt time.Time
}

// claimOutboxEvents atomically leases due outbox events to a worker.
//
// Each claimed event receives a unique lease owner token. Callers must pass
// the returned LeaseOwner to acknowledgeOutboxEvent or
// retryOrDeadLetterOutboxEvent. A worker identifier alone is not sufficient.
//
// Events whose attempt budgets are exhausted are dead-lettered in bounded
// batches. Claiming records dispatch attempts; it does not execute events.
func claimOutboxEvents(
	ctx context.Context,
	pool *pgxpool.Pool,
	workerID string,
	limit int,
	maxAttempts int,
	leaseDuration time.Duration,
) ([]outboxEventRecord, error) {
	if pool == nil {
		return nil, errors.New("database pool is required")
	}

	workerID, err := normalizeOutboxWorkerID(workerID)
	if err != nil {
		return nil, err
	}

	if limit < 1 || limit > 1000 {
		return nil, errors.New("claim limit must be between 1 and 1000")
	}

	if maxAttempts < 1 || maxAttempts > 2147483647 {
		return nil, errors.New(
			"maximum attempts must be between 1 and 2147483647",
		)
	}

	if leaseDuration < time.Microsecond {
		return nil, errors.New(
			"lease duration must be at least one microsecond",
		)
	}

	leaseMicroseconds := durationMicroseconds(leaseDuration)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"begin outbox claim transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	// Bound dead-letter work per claim call. SKIP LOCKED allows concurrent
	// workers to process separate batches without waiting on one another.
	_, err = tx.Exec(ctx, `
		WITH exhausted AS (
			SELECT id
			FROM outbox_events
			WHERE delivered_at IS NULL
			  AND dead_lettered_at IS NULL
			  AND available_at <= now()
			  AND attempt_count >= $1
			  AND (
				  lease_owner IS NULL
				  OR lease_expires_at <= now()
			  )
			ORDER BY available_at, created_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_events AS oe
		SET
			dead_lettered_at = now(),
			lease_owner = NULL,
			lease_expires_at = NULL,
			last_error_code = COALESCE(
				oe.last_error_code,
				'attempts_exhausted'
			)
		FROM exhausted
		WHERE oe.id = exhausted.id
	`, maxAttempts, limit)
	if err != nil {
		return nil, fmt.Errorf(
			"dead-letter exhausted outbox events: %w",
			err,
		)
	}

	rows, err := tx.Query(ctx, `
		WITH candidates AS (
			SELECT id
			FROM outbox_events
			WHERE delivered_at IS NULL
			  AND dead_lettered_at IS NULL
			  AND available_at <= now()
			  AND attempt_count < $1
			  AND (
				  lease_owner IS NULL
				  OR lease_expires_at <= now()
			  )
			ORDER BY available_at, created_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_events AS oe
		SET
			lease_owner = $3 || ':' || gen_random_uuid()::text,
			lease_expires_at = now()
				+ ($4::double precision * interval '1 microsecond'),
			attempt_count = oe.attempt_count + 1
		FROM candidates
		WHERE oe.id = candidates.id
		RETURNING
			oe.id::text,
			oe.project_id::text,
			oe.operation_id::text,
			oe.event_type,
			oe.dedupe_key,
			oe.payload,
			oe.attempt_count,
			oe.lease_owner,
			oe.lease_expires_at
	`,
		maxAttempts,
		limit,
		workerID,
		leaseMicroseconds,
	)
	if err != nil {
		return nil, fmt.Errorf("claim due outbox events: %w", err)
	}
	defer rows.Close()

	events := make([]outboxEventRecord, 0, limit)

	for rows.Next() {
		var event outboxEventRecord
		var payload []byte

		if err := rows.Scan(
			&event.ID,
			&event.ProjectID,
			&event.OperationID,
			&event.EventType,
			&event.DedupeKey,
			&payload,
			&event.AttemptCount,
			&event.LeaseOwner,
			&event.LeaseExpiresAt,
		); err != nil {
			return nil, fmt.Errorf(
				"scan claimed outbox event: %w",
				err,
			)
		}

		event.Payload = append(json.RawMessage(nil), payload...)
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate claimed outbox events: %w",
			err,
		)
	}

	rows.Close()

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf(
			"commit outbox claim transaction: %w",
			err,
		)
	}

	return events, nil
}

// acknowledgeOutboxEvent marks an event as delivered only when the caller
// still owns its unexpired lease.
//
// Pass event.LeaseOwner from claimOutboxEvents, not the worker's stable ID.
func acknowledgeOutboxEvent(
	ctx context.Context,
	pool *pgxpool.Pool,
	eventID string,
	leaseOwner string,
) error {
	if pool == nil {
		return errors.New("database pool is required")
	}

	normalizedID, err := normalizeOperationUUID(eventID)
	if err != nil {
		return fmt.Errorf("event ID: %w", err)
	}

	if err := validateOutboxLeaseOwner(leaseOwner); err != nil {
		return err
	}

	result, err := pool.Exec(ctx, `
		UPDATE outbox_events
		SET
			delivered_at = now(),
			lease_owner = NULL,
			lease_expires_at = NULL,
			last_error_code = NULL
		WHERE id = $1::uuid
		  AND lease_owner = $2
		  AND lease_expires_at > now()
		  AND delivered_at IS NULL
		  AND dead_lettered_at IS NULL
	`, normalizedID, leaseOwner)
	if err != nil {
		return fmt.Errorf("acknowledge outbox event: %w", err)
	}

	if result.RowsAffected() != 1 {
		return errOutboxLeaseNotOwned
	}

	return nil
}

// retryOrDeadLetterOutboxEvent schedules a failed event for delayed retry.
// The event is dead-lettered when its claimed attempt count reaches the
// configured maximum.
//
// Pass event.LeaseOwner from claimOutboxEvents, not the worker's stable ID.
// The returned bool is true when the event was dead-lettered.
func retryOrDeadLetterOutboxEvent(
	ctx context.Context,
	pool *pgxpool.Pool,
	eventID string,
	leaseOwner string,
	maxAttempts int,
	retryDelay time.Duration,
	errorCode string,
) (bool, error) {
	if pool == nil {
		return false, errors.New("database pool is required")
	}

	normalizedID, err := normalizeOperationUUID(eventID)
	if err != nil {
		return false, fmt.Errorf("event ID: %w", err)
	}

	if err := validateOutboxLeaseOwner(leaseOwner); err != nil {
		return false, err
	}

	if maxAttempts < 1 || maxAttempts > 2147483647 {
		return false, errors.New(
			"maximum attempts must be between 1 and 2147483647",
		)
	}

	if retryDelay < 0 {
		return false, errors.New("retry delay cannot be negative")
	}

	if !validOutboxErrorCode(errorCode) {
		return false, errors.New("error code is invalid")
	}

	retryMicroseconds := durationMicroseconds(retryDelay)

	var deadLettered bool

	err = pool.QueryRow(ctx, `
		UPDATE outbox_events
		SET
			available_at = CASE
				WHEN attempt_count >= $3 THEN available_at
				ELSE now()
					+ ($4::double precision * interval '1 microsecond')
			END,
			dead_lettered_at = CASE
				WHEN attempt_count >= $3 THEN now()
				ELSE NULL
			END,
			lease_owner = NULL,
			lease_expires_at = NULL,
			last_error_code = $5
		WHERE id = $1::uuid
		  AND lease_owner = $2
		  AND lease_expires_at > now()
		  AND delivered_at IS NULL
		  AND dead_lettered_at IS NULL
		RETURNING dead_lettered_at IS NOT NULL
	`,
		normalizedID,
		leaseOwner,
		maxAttempts,
		retryMicroseconds,
		errorCode,
	).Scan(&deadLettered)

	if errors.Is(err, pgx.ErrNoRows) {
		return false, errOutboxLeaseNotOwned
	}

	if err != nil {
		return false, fmt.Errorf(
			"schedule outbox event retry: %w",
			err,
		)
	}

	return deadLettered, nil
}

func normalizeOutboxWorkerID(workerID string) (string, error) {
	workerID = strings.TrimSpace(workerID)

	if workerID == "" {
		return "", errors.New("worker ID is required")
	}

	// A UUID and separator are appended to this identifier for every
	// claimed event. Keep the final lease_owner within 255 bytes.
	if len(workerID) > 218 ||
		!utf8.ValidString(workerID) ||
		strings.ContainsAny(workerID, "\x00\r\n") {
		return "", errors.New("worker ID is invalid")
	}

	return workerID, nil
}

func validateOutboxLeaseOwner(leaseOwner string) error {
	if leaseOwner == "" ||
		strings.TrimSpace(leaseOwner) != leaseOwner ||
		len(leaseOwner) > 255 ||
		!utf8.ValidString(leaseOwner) ||
		strings.ContainsAny(leaseOwner, "\x00\r\n") {
		return errors.New("lease owner token is invalid")
	}

	return nil
}

// durationMicroseconds rounds positive durations up to the next microsecond,
// preventing a requested lease or retry delay from being shortened.
func durationMicroseconds(duration time.Duration) int64 {
	microseconds := int64(duration / time.Microsecond)

	if duration%time.Microsecond != 0 {
		microseconds++
	}

	return microseconds
}

func validOutboxErrorCode(code string) bool {
	if len(code) == 0 || len(code) > 128 {
		return false
	}

	for _, character := range code {
		if (character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') ||
			character == '_' ||
			character == '-' ||
			character == '.' {
			continue
		}

		return false
	}

	return true
}