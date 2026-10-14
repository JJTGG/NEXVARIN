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
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	errOperationLeaseNotOwned = errors.New(
		"operation lease is not owned by this worker",
	)
	errOperationOutcomeStateConflict = errors.New(
		"operation is not in a state that accepts worker outcomes",
	)
)

type operationOutcomeDetails struct {
	AttemptCount     int    `json:"attemptCount"`
	MaxAttempts      int    `json:"maxAttempts"`
	ErrorCode        string `json:"errorCode,omitempty"`
	RetryDelayMicros int64  `json:"retryDelayMicros,omitempty"`
	RetryExhausted   bool   `json:"retryExhausted,omitempty"`
}

// completeOperation atomically marks a running operation as succeeded.
//
// The operation must still have an unexpired lease owned by the caller.
// Its status change, lease release, completion timestamp, and history event
// are committed in one transaction.
func completeOperation(
	ctx context.Context,
	pool *pgxpool.Pool,
	operationID string,
	leaseOwner string,
) (operationRecord, error) {
	return recordOperationOutcome(
		ctx,
		pool,
		operationID,
		leaseOwner,
		operationStatusSucceeded,
		"",
		"",
		0,
	)
}

// failOperation atomically marks a running operation as failed.
//
// Error summaries must be sanitized by the caller. Do not pass credentials,
// access tokens, or other secrets in failure details.
func failOperation(
	ctx context.Context,
	pool *pgxpool.Pool,
	operationID string,
	leaseOwner string,
	errorCode string,
	errorSummary string,
) (operationRecord, error) {
	return recordOperationOutcome(
		ctx,
		pool,
		operationID,
		leaseOwner,
		operationStatusFailed,
		errorCode,
		errorSummary,
		0,
	)
}

// retryOperation schedules another attempt after retryDelay.
//
// If the current attempt has exhausted the operation's configured attempt
// budget, the operation is marked failed instead of being scheduled again.
// The returned record identifies which transition occurred.
func retryOperation(
	ctx context.Context,
	pool *pgxpool.Pool,
	operationID string,
	leaseOwner string,
	errorCode string,
	errorSummary string,
	retryDelay time.Duration,
) (operationRecord, error) {
	if retryDelay < 0 {
		return operationRecord{}, errors.New(
			"operation retry delay cannot be negative",
		)
	}

	return recordOperationOutcome(
		ctx,
		pool,
		operationID,
		leaseOwner,
		operationStatusRetryWait,
		errorCode,
		errorSummary,
		retryDelay,
	)
}

func recordOperationOutcome(
	ctx context.Context,
	pool *pgxpool.Pool,
	operationID string,
	leaseOwner string,
	requestedStatus operationStatus,
	errorCode string,
	errorSummary string,
	retryDelay time.Duration,
) (operationRecord, error) {
	var empty operationRecord

	if pool == nil {
		return empty, errors.New("database pool is required")
	}

	normalizedID, err := normalizeOperationUUID(operationID)
	if err != nil {
		return empty, fmt.Errorf("operation ID: %w", err)
	}

	workerID, err := operationWorkerIDFromLeaseOwner(leaseOwner)
	if err != nil {
		return empty, err
	}

	if requestedStatus != operationStatusSucceeded &&
		requestedStatus != operationStatusFailed &&
		requestedStatus != operationStatusRetryWait {
		return empty, errors.New("operation outcome status is invalid")
	}

	if requestedStatus == operationStatusFailed ||
		requestedStatus == operationStatusRetryWait {
		errorCode, errorSummary, err = normalizeOperationFailure(
			errorCode,
			errorSummary,
		)
		if err != nil {
			return empty, err
		}
	}

	retryMicroseconds := durationMicroseconds(retryDelay)

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return empty, fmt.Errorf(
			"begin operation outcome transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	var currentStatus string
	var currentLeaseOwner pgtype.Text
	var currentLeaseExpiry pgtype.Timestamptz
	var leaseIsCurrent bool
	var attemptCount int
	var maxAttempts int

	err = tx.QueryRow(ctx, `
		SELECT
			status,
			lease_owner,
			lease_expires_at,
			COALESCE(lease_expires_at > now(), FALSE),
			attempt_count,
			max_attempts
		FROM operations
		WHERE id = $1::uuid
		FOR UPDATE
	`, normalizedID).Scan(
		&currentStatus,
		&currentLeaseOwner,
		&currentLeaseExpiry,
		&leaseIsCurrent,
		&attemptCount,
		&maxAttempts,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return empty, errOperationNotFound
	}
	if err != nil {
		return empty, fmt.Errorf(
			"load operation for outcome: %w",
			err,
		)
	}

	if currentStatus != string(operationStatusRunning) {
		return empty, errOperationOutcomeStateConflict
	}

	if !currentLeaseOwner.Valid ||
		currentLeaseOwner.String != leaseOwner ||
		!currentLeaseExpiry.Valid ||
		!leaseIsCurrent {
		return empty, errOperationLeaseNotOwned
	}

	finalStatus := requestedStatus
	retryExhausted := false

	if requestedStatus == operationStatusRetryWait &&
		attemptCount >= maxAttempts {
		finalStatus = operationStatusFailed
		retryExhausted = true
	}

	if !canTransitionOperationStatus(
		operationStatus(currentStatus),
		finalStatus,
	) {
		return empty, errOperationOutcomeStateConflict
	}

	record, err := scanOperationRecord(tx.QueryRow(ctx, `
		UPDATE operations
		SET
			status = $3,
			lease_owner = NULL,
			lease_expires_at = NULL,
			next_attempt_at = CASE
				WHEN $3 = 'retry_wait' THEN now()
					+ ($6::double precision * interval '1 microsecond')
				ELSE next_attempt_at
			END,
			error_code = CASE
				WHEN $3 = 'succeeded' THEN NULL
				ELSE NULLIF($4, '')
			END,
			error_summary = CASE
				WHEN $3 = 'succeeded' THEN NULL
				ELSE NULLIF($5, '')
			END,
			finished_at = CASE
				WHEN $3 IN ('succeeded', 'failed') THEN now()
				ELSE NULL
			END,
			updated_at = now()
		WHERE id = $1::uuid
		  AND status = 'running'
		  AND lease_owner = $2
		  AND lease_expires_at > now()
		RETURNING `+operationRecordColumns,
		normalizedID,
		leaseOwner,
		string(finalStatus),
		errorCode,
		errorSummary,
		retryMicroseconds,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return empty, errOperationLeaseNotOwned
	}
	if err != nil {
		return empty, fmt.Errorf(
			"update operation outcome: %w",
			err,
		)
	}

	eventType := operationOutcomeEventType(finalStatus)

	details, err := json.Marshal(operationOutcomeDetails{
		AttemptCount: attemptCount,
		MaxAttempts:  maxAttempts,
		ErrorCode:    errorCode,
		RetryDelayMicros: retryMicrosecondsForOutcome(
			finalStatus,
			retryMicroseconds,
		),
		RetryExhausted: retryExhausted,
	})
	if err != nil {
		return empty, fmt.Errorf(
			"encode operation outcome history: %w",
			err,
		)
	}

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
			$3,
			'running',
			$4,
			$5,
			$6::jsonb
		)
	`,
		record.ProjectID,
		record.ID,
		eventType,
		string(finalStatus),
		workerID,
		string(details),
	); err != nil {
		return empty, fmt.Errorf(
			"record operation outcome history: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return empty, fmt.Errorf(
			"commit operation outcome transaction: %w",
			err,
		)
	}

	return record, nil
}

func operationOutcomeEventType(status operationStatus) string {
	switch status {
	case operationStatusSucceeded:
		return "operation.succeeded"
	case operationStatusRetryWait:
		return "operation.retry_scheduled"
	case operationStatusFailed:
		return "operation.failed"
	default:
		return "operation.outcome_unknown"
	}
}

func retryMicrosecondsForOutcome(
	status operationStatus,
	retryMicroseconds int64,
) int64 {
	if status != operationStatusRetryWait {
		return 0
	}

	return retryMicroseconds
}

func operationWorkerIDFromLeaseOwner(leaseOwner string) (string, error) {
	if leaseOwner == "" ||
		strings.TrimSpace(leaseOwner) != leaseOwner ||
		len(leaseOwner) > 255 ||
		!utf8.ValidString(leaseOwner) ||
		strings.ContainsAny(leaseOwner, "\x00\r\n") {
		return "", errors.New("operation lease owner token is invalid")
	}

	separator := strings.LastIndex(leaseOwner, ":")
	if separator < 1 || separator == len(leaseOwner)-1 {
		return "", errors.New("operation lease owner token is invalid")
	}

	workerID, err := normalizeOperationWorkerID(
		leaseOwner[:separator],
	)
	if err != nil {
		return "", errors.New("operation lease owner token is invalid")
	}

	tokenID := leaseOwner[separator+1:]
	normalizedTokenID, err := normalizeOperationUUID(tokenID)
	if err != nil || normalizedTokenID != tokenID {
		return "", errors.New("operation lease owner token is invalid")
	}

	return workerID, nil
}

func normalizeOperationFailure(
	errorCode string,
	errorSummary string,
) (string, string, error) {
	errorCode = strings.TrimSpace(errorCode)
	errorSummary = strings.TrimSpace(errorSummary)

	if !validOperationErrorCode(errorCode) {
		return "", "", errors.New("operation error code is invalid")
	}

	if !utf8.ValidString(errorSummary) ||
		len(errorSummary) > 2000 ||
		strings.ContainsRune(errorSummary, '\x00') {
		return "", "", errors.New("operation error summary is invalid")
	}

	return errorCode, errorSummary, nil
}

func validOperationErrorCode(code string) bool {
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
