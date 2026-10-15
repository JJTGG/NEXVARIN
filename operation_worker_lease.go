package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// renewOperationLease extends a running operation's lease while the caller
// still owns an unexpired lease.
//
// The row is locked before the database clock is checked. The lease token,
// running status, and expiry are checked again in the update. The extension
// and its transaction commit happen before the new expiry is returned.
//
// Renewals do not append operation history events; doing so for every
// heartbeat would create unnecessary history noise.
func renewOperationLease(
	ctx context.Context,
	pool *pgxpool.Pool,
	operationID string,
	leaseOwner string,
	leaseDuration time.Duration,
) (time.Time, error) {
	var empty time.Time

	if pool == nil {
		return empty, errors.New("database pool is required")
	}

	normalizedID, err := normalizeOperationUUID(operationID)
	if err != nil {
		return empty, fmt.Errorf("operation ID: %w", err)
	}

	if _, err := operationWorkerIDFromLeaseOwner(leaseOwner); err != nil {
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
			"begin operation lease renewal: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	var status string
	var storedLeaseOwner pgtype.Text
	var storedLeaseExpiry pgtype.Timestamptz

	err = tx.QueryRow(ctx, `
		SELECT
			status,
			lease_owner,
			lease_expires_at
		FROM operations
		WHERE id = $1::uuid
		FOR UPDATE
	`, normalizedID).Scan(
		&status,
		&storedLeaseOwner,
		&storedLeaseExpiry,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return empty, errOperationNotFound
	}
	if err != nil {
		return empty, fmt.Errorf(
			"load operation for lease renewal: %w",
			err,
		)
	}

	if status != string(operationStatusRunning) {
		return empty, errOperationOutcomeStateConflict
	}

	if !storedLeaseOwner.Valid ||
		storedLeaseOwner.String != leaseOwner ||
		!storedLeaseExpiry.Valid {
		return empty, errOperationLeaseNotOwned
	}

	// Check expiry after acquiring the row lock. PostgreSQL now() uses the
	// transaction start time and can become stale during a lock wait.
	var leaseIsCurrent bool
	if err := tx.QueryRow(ctx, `
		SELECT lease_expires_at > clock_timestamp()
		FROM operations
		WHERE id = $1::uuid
	`, normalizedID).Scan(&leaseIsCurrent); err != nil {
		return empty, fmt.Errorf(
			"check operation lease before renewal: %w",
			err,
		)
	}

	if !leaseIsCurrent {
		return empty, errOperationLeaseNotOwned
	}

	var renewedExpiry time.Time

	err = tx.QueryRow(ctx, `
		UPDATE operations
		SET
			lease_expires_at = GREATEST(
				lease_expires_at,
				clock_timestamp()
					+ ($3::double precision * interval '1 microsecond')
			),
			updated_at = clock_timestamp()
		WHERE id = $1::uuid
		  AND status = 'running'
		  AND lease_owner = $2
		  AND lease_expires_at > clock_timestamp()
		RETURNING lease_expires_at
	`,
		normalizedID,
		leaseOwner,
		leaseMicroseconds,
	).Scan(&renewedExpiry)

	if errors.Is(err, pgx.ErrNoRows) {
		return empty, errOperationLeaseNotOwned
	}
	if err != nil {
		return empty, fmt.Errorf(
			"extend operation lease: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return empty, fmt.Errorf(
			"commit operation lease renewal: %w",
			err,
		)
	}

	return renewedExpiry, nil
}
