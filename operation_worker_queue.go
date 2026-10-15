package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var errNoDueOperationAvailable = errors.New(
	"no due operation is available",
)

const operationClaimScanBatchSize = 32

// claimNextOperation claims the next due operation from the durable
// operations queue.
//
// The operations table is the source of truth for executable work.
// Outbox events remain independently leased for event delivery.
//
// Candidate selection is ordered by retry time, creation time, and ID.
// Competing workers can select the same candidate, but claimOperation
// serializes the final claim and permits only one worker to succeed.
//
// Candidates that another worker claims, or that become ineligible
// before their claim is acquired, are skipped. The query is repeated
// until an operation is claimed or no due candidate remains.
//
// The caller must retain the returned LeaseOwner and use it for
// subsequent lease-protected operation transitions.
func claimNextOperation(
	ctx context.Context,
	pool *pgxpool.Pool,
	workerID string,
	leaseDuration time.Duration,
) (claimedOperation, error) {
	var empty claimedOperation

	if pool == nil {
		return empty, errors.New("database pool is required")
	}

	workerID, err := normalizeOperationWorkerID(workerID)
	if err != nil {
		return empty, err
	}

	if leaseDuration < time.Microsecond {
		return empty, errors.New(
			"lease duration must be at least one microsecond",
		)
	}

	for {
		operationIDs, err := findDueOperationCandidates(
			ctx,
			pool,
			operationClaimScanBatchSize,
		)
		if err != nil {
			return empty, err
		}

		if len(operationIDs) == 0 {
			return empty, errNoDueOperationAvailable
		}

		for _, operationID := range operationIDs {
			claimed, err := claimOperation(
				ctx,
				pool,
				operationID,
				workerID,
				leaseDuration,
			)
			if err == nil {
				return claimed, nil
			}

			// Candidate selection is intentionally separate from the
			// transactional claim. A competing worker may have claimed
			// the operation after our candidate query returned.
			if errors.Is(err, errOperationNotClaimable) ||
				errors.Is(err, errOperationAttemptsExhausted) ||
				errors.Is(err, errOperationNotFound) {
				continue
			}

			return empty, fmt.Errorf(
				"claim due operation %s: %w",
				operationID,
				err,
			)
		}

		// All candidates changed while we were attempting their claims.
		// Re-query rather than reporting an empty queue prematurely.
	}
}

func findDueOperationCandidates(
	ctx context.Context,
	pool *pgxpool.Pool,
	limit int,
) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT id::text
		FROM operations
		WHERE status IN ('queued', 'retry_wait')
		  AND next_attempt_at <= clock_timestamp()
		  AND attempt_count < max_attempts
		ORDER BY next_attempt_at, created_at, id
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf(
			"find due operation candidates: %w",
			err,
		)
	}

	operationIDs := make([]string, 0, limit)

	for rows.Next() {
		var operationID string
		if err := rows.Scan(&operationID); err != nil {
			rows.Close()
			return nil, fmt.Errorf(
				"scan due operation candidate: %w",
				err,
			)
		}

		operationIDs = append(operationIDs, operationID)
	}

	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf(
			"iterate due operation candidates: %w",
			err,
		)
	}

	rows.Close()

	return operationIDs, nil
}

// Keep this compile-time reference explicit: the queue uses the same
// pgx-backed operation claim contract as single-operation claiming.
var _ = pgx.ErrNoRows
