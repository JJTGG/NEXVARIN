package main

import (
	"context"
	"testing"
	"time"
)

func createClaimedTaskForExecutionTest(
	t *testing.T,
) (*operationStoreTestFixture, operationWorkerTask) {
	t.Helper()

	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 0)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	task, err := loadClaimedOperationTask(
		ctx,
		fixture.pool,
		claimed,
	)
	if err != nil {
		t.Fatalf("load claimed task for execution test: %v", err)
	}

	return fixture, task
}

func TestRecordOperationExecutionResultIntegrationMapsDispositions(
	t *testing.T,
) {
	tests := []struct {
		name               string
		result             operationExecutionResult
		wantStatus         operationStatus
		wantEvent          string
		wantErrorCode      string
		wantErrorSummary   string
		wantFinishedAt     bool
		wantRetryScheduled bool
	}{
		{
			name:       "success",
			result:     operationExecutionResult{
				Disposition: operationExecutionSucceeded,
			},
			wantStatus:     operationStatusSucceeded,
			wantEvent:      "operation.succeeded",
			wantFinishedAt: true,
		},
		{
			name: "safe retry",
			result: operationExecutionResult{
				Disposition: operationExecutionRetry,
				ErrorCode:   "temporary_build_failure",
				ErrorSummary: "Builder stopped before producing an artifact",
				RetryDelay:  45 * time.Second,
			},
			wantStatus:         operationStatusRetryWait,
			wantEvent:          "operation.retry_scheduled",
			wantErrorCode:      "temporary_build_failure",
			wantErrorSummary:   "Builder stopped before producing an artifact",
			wantRetryScheduled: true,
		},
		{
			name: "terminal failure",
			result: operationExecutionResult{
				Disposition:  operationExecutionFailed,
				ErrorCode:    "invalid_build_configuration",
				ErrorSummary: "Required build configuration is missing",
			},
			wantStatus:       operationStatusFailed,
			wantEvent:        "operation.failed",
			wantErrorCode:    "invalid_build_configuration",
			wantErrorSummary: "Required build configuration is missing",
			wantFinishedAt:   true,
		},
		{
			name: "uncertain external effect",
			result: operationExecutionResult{
				Disposition:  operationExecutionNeedsReconciliation,
				ErrorCode:    "deployment_outcome_unknown",
				ErrorSummary: "Deployment result could not be established",
			},
			wantStatus:       operationStatusNeedsReconciliation,
			wantEvent:        "operation.needs_reconciliation",
			wantErrorCode:    "deployment_outcome_unknown",
			wantErrorSummary: "Deployment result could not be established",
		},
		{
			name: "invalid retry result",
			result: operationExecutionResult{
				Disposition: operationExecutionRetry,
				ErrorCode:   "",
				ErrorSummary: "",
			},
			wantStatus:       operationStatusNeedsReconciliation,
			wantEvent:        "operation.needs_reconciliation",
			wantErrorCode:    "invalid_execution_result",
			wantErrorSummary: "Executor returned an invalid or incomplete result; external effects require reconciliation.",
		},
		{
			name: "unknown disposition",
			result: operationExecutionResult{
				Disposition: operationExecutionDisposition("unknown"),
			},
			wantStatus:       operationStatusNeedsReconciliation,
			wantEvent:        "operation.needs_reconciliation",
			wantErrorCode:    "invalid_execution_result",
			wantErrorSummary: "Executor returned an invalid or incomplete result; external effects require reconciliation.",
		},
		{
			name: "success with contradictory metadata",
			result: operationExecutionResult{
				Disposition: operationExecutionSucceeded,
				ErrorSummary: "Unexpected error metadata",
			},
			wantStatus:       operationStatusNeedsReconciliation,
			wantEvent:        "operation.needs_reconciliation",
			wantErrorCode:    "invalid_execution_result",
			wantErrorSummary: "Executor returned an invalid or incomplete result; external effects require reconciliation.",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, task := createClaimedTaskForExecutionTest(t)

			ctx, cancel := context.WithTimeout(
				context.Background(),
				10*time.Second,
			)
			defer cancel()

			record, err := recordOperationExecutionResult(
				ctx,
				fixture.pool,
				task,
				test.result,
			)
			if err != nil {
				t.Fatalf("record execution result: %v", err)
			}

			if record.Status != test.wantStatus {
				t.Errorf(
					"returned status = %q, want %q",
					record.Status,
					test.wantStatus,
				)
			}

			if test.wantErrorCode == "" {
				if record.ErrorCode != nil {
					t.Errorf(
						"returned error code = %q, want nil",
						*record.ErrorCode,
					)
				}
			} else if record.ErrorCode == nil ||
				*record.ErrorCode != test.wantErrorCode {
				t.Errorf(
					"returned error code = %v, want %q",
					record.ErrorCode,
					test.wantErrorCode,
				)
			}

			if test.wantErrorSummary == "" {
				if record.ErrorSummary != nil {
					t.Errorf(
						"returned error summary = %q, want nil",
						*record.ErrorSummary,
					)
				}
			} else if record.ErrorSummary == nil ||
				*record.ErrorSummary != test.wantErrorSummary {
				t.Errorf(
					"returned error summary = %v, want %q",
					record.ErrorSummary,
					test.wantErrorSummary,
				)
			}

			if test.wantFinishedAt {
				if record.FinishedAt == nil {
					t.Error("returned finished_at is nil")
				}
			} else if record.FinishedAt != nil {
				t.Errorf(
					"returned finished_at = %v, want nil",
					record.FinishedAt,
				)
			}

			var (
				status          string
				leaseOwner      *string
				leaseExpiresAt  *time.Time
				finishedAt      *time.Time
				errorCode       *string
				errorSummary    *string
				retryIsInWindow bool
			)

			err = fixture.pool.QueryRow(ctx, `
				SELECT
					status,
					lease_owner,
					lease_expires_at,
					finished_at,
					error_code,
					error_summary,
					next_attempt_at > clock_timestamp()
						+ interval '35 seconds'
						AND next_attempt_at < clock_timestamp()
						+ interval '60 seconds'
				FROM operations
				WHERE id = $1::uuid
			`, task.Claimed.Operation.ID).Scan(
				&status,
				&leaseOwner,
				&leaseExpiresAt,
				&finishedAt,
				&errorCode,
				&errorSummary,
				&retryIsInWindow,
			)
			if err != nil {
				t.Fatalf("read persisted outcome: %v", err)
			}

			if status != string(test.wantStatus) {
				t.Errorf(
					"persisted status = %q, want %q",
					status,
					test.wantStatus,
				)
			}

			if leaseOwner != nil {
				t.Errorf(
					"lease owner = %q, want NULL",
					*leaseOwner,
				)
			}
			if leaseExpiresAt != nil {
				t.Errorf(
					"lease expiry = %v, want NULL",
					*leaseExpiresAt,
				)
			}

			if test.wantFinishedAt && finishedAt == nil {
				t.Error("persisted finished_at is NULL")
			}
			if !test.wantFinishedAt && finishedAt != nil {
				t.Errorf(
					"persisted finished_at = %v, want NULL",
					*finishedAt,
				)
			}

			if test.wantErrorCode == "" {
				if errorCode != nil {
					t.Errorf(
						"persisted error code = %q, want NULL",
						*errorCode,
					)
				}
			} else if errorCode == nil ||
				*errorCode != test.wantErrorCode {
				t.Errorf(
					"persisted error code = %v, want %q",
					errorCode,
					test.wantErrorCode,
				)
			}

			if test.wantErrorSummary == "" {
				if errorSummary != nil {
					t.Errorf(
						"persisted error summary = %q, want NULL",
						*errorSummary,
					)
				}
			} else if errorSummary == nil ||
				*errorSummary != test.wantErrorSummary {
				t.Errorf(
					"persisted error summary = %v, want %q",
					errorSummary,
					test.wantErrorSummary,
				)
			}

			if test.wantRetryScheduled && !retryIsInWindow {
				t.Error(
					"next_attempt_at is outside the expected retry window",
				)
			}

			var (
				eventCount     int
				eventType      string
				toStatus       string
				eventErrorCode *string
			)

			err = fixture.pool.QueryRow(ctx, `
				SELECT
					count(*),
					MAX(event_type),
					MAX(to_status),
					MAX(details->>'errorCode')
				FROM operation_events
				WHERE operation_id = $1::uuid
				  AND event_type IN (
					'operation.succeeded',
					'operation.retry_scheduled',
					'operation.failed',
					'operation.needs_reconciliation'
				  )
			`, task.Claimed.Operation.ID).Scan(
				&eventCount,
				&eventType,
				&toStatus,
				&eventErrorCode,
			)
			if err != nil {
				t.Fatalf("read outcome history event: %v", err)
			}

			if eventCount != 1 {
				t.Errorf(
					"outcome event count = %d, want 1",
					eventCount,
				)
			}

			if eventType != test.wantEvent {
				t.Errorf(
					"event type = %q, want %q",
					eventType,
					test.wantEvent,
				)
			}

			if toStatus != string(test.wantStatus) {
				t.Errorf(
					"event destination status = %q, want %q",
					toStatus,
					test.wantStatus,
				)
			}

			if test.wantErrorCode == "" {
				if eventErrorCode != nil {
					t.Errorf(
						"event error code = %q, want NULL",
						*eventErrorCode,
					)
				}
			} else if eventErrorCode == nil ||
				*eventErrorCode != test.wantErrorCode {
				t.Errorf(
					"event error code = %v, want %q",
					eventErrorCode,
					test.wantErrorCode,
				)
			}
		})
	}
}
