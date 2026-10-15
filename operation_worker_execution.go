package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type operationExecutionDisposition string

const (
	operationExecutionSucceeded            operationExecutionDisposition = "succeeded"
	operationExecutionRetry                operationExecutionDisposition = "retry"
	operationExecutionFailed               operationExecutionDisposition = "failed"
	operationExecutionNeedsReconciliation operationExecutionDisposition = "needs_reconciliation"
)

// operationExecutionResult is the executor's classification of an attempt.
//
// Retry is permitted only when the executor knows another attempt is safe.
// If an external side effect may have occurred and its outcome is uncertain,
// the executor must return operationExecutionNeedsReconciliation instead.
type operationExecutionResult struct {
	Disposition  operationExecutionDisposition
	ErrorCode    string
	ErrorSummary string
	RetryDelay   time.Duration
}

// operationExecutor executes a validated, claimed task.
//
// Implementations must return a disposition for every attempt. They must
// classify uncertain external effects as requiring reconciliation rather
// than ordinary retry. Error summaries must not contain credentials,
// access tokens, or other secrets.
type operationExecutor interface {
	Execute(
		ctx context.Context,
		task operationWorkerTask,
	) operationExecutionResult
}

// recordOperationExecutionResult translates an executor result into the
// existing lease-protected database outcome transaction.
//
// An invalid or incomplete result is treated as uncertain: the operation
// is moved to needs_reconciliation using a fixed, sanitized reason. This
// avoids silently converting an ambiguous attempt into an automatic retry.
func recordOperationExecutionResult(
	ctx context.Context,
	pool *pgxpool.Pool,
	task operationWorkerTask,
	result operationExecutionResult,
) (operationRecord, error) {
	if pool == nil {
		return operationRecord{}, errors.New("database pool is required")
	}

	switch result.Disposition {
	case operationExecutionSucceeded:
		if result.ErrorCode != "" ||
			result.ErrorSummary != "" ||
			result.RetryDelay != 0 {
			return reconcileInvalidExecutionResult(ctx, pool, task)
		}

		return completeOperation(
			ctx,
			pool,
			task.Claimed.Operation.ID,
			task.Claimed.LeaseOwner,
		)

	case operationExecutionRetry:
		errorCode, errorSummary, valid := validateExecutionFailureResult(
			result.ErrorCode,
			result.ErrorSummary,
		)
		if !valid || result.RetryDelay < 0 {
			return reconcileInvalidExecutionResult(ctx, pool, task)
		}

		return retryOperation(
			ctx,
			pool,
			task.Claimed.Operation.ID,
			task.Claimed.LeaseOwner,
			errorCode,
			errorSummary,
			result.RetryDelay,
		)

	case operationExecutionFailed:
		errorCode, errorSummary, valid := validateExecutionFailureResult(
			result.ErrorCode,
			result.ErrorSummary,
		)
		if !valid || result.RetryDelay != 0 {
			return reconcileInvalidExecutionResult(ctx, pool, task)
		}

		return failOperation(
			ctx,
			pool,
			task.Claimed.Operation.ID,
			task.Claimed.LeaseOwner,
			errorCode,
			errorSummary,
		)

	case operationExecutionNeedsReconciliation:
		errorCode, errorSummary, valid := validateExecutionFailureResult(
			result.ErrorCode,
			result.ErrorSummary,
		)
		if !valid || result.RetryDelay != 0 {
			return reconcileInvalidExecutionResult(ctx, pool, task)
		}

		return markOperationNeedsReconciliation(
			ctx,
			pool,
			task.Claimed.Operation.ID,
			task.Claimed.LeaseOwner,
			errorCode,
			errorSummary,
		)

	default:
		return reconcileInvalidExecutionResult(ctx, pool, task)
	}
}

func validateExecutionFailureResult(
	errorCode string,
	errorSummary string,
) (string, string, bool) {
	errorCode, errorSummary, err := normalizeOperationFailure(
		errorCode,
		errorSummary,
	)
	if err != nil {
		return "", "", false
	}

	if !validOperationErrorCode(errorCode) ||
		strings.TrimSpace(errorSummary) == "" {
		return "", "", false
	}

	return errorCode, errorSummary, true
}

func reconcileInvalidExecutionResult(
	ctx context.Context,
	pool *pgxpool.Pool,
	task operationWorkerTask,
) (operationRecord, error) {
	record, err := markOperationNeedsReconciliation(
		ctx,
		pool,
		task.Claimed.Operation.ID,
		task.Claimed.LeaseOwner,
		"invalid_execution_result",
		"Executor returned an invalid or incomplete result; external effects require reconciliation.",
	)
	if err != nil {
		return operationRecord{}, fmt.Errorf(
			"record invalid execution result for reconciliation: %w",
			err,
		)
	}

	return record, nil
}
