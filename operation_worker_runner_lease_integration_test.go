package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type operationWorkerLeaseRenewalTestExecutor struct {
	pool                       *pgxpool.Pool
	observedRenewal            bool
	observedAfterInitialExpiry bool
}

func (executor *operationWorkerLeaseRenewalTestExecutor) Execute(
	ctx context.Context,
	task operationWorkerTask,
) operationExecutionResult {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		var (
			currentExpiry       time.Time
			renewed             bool
			leaseStillValid     bool
			initialLeaseExpired bool
		)

		err := executor.pool.QueryRow(ctx, `
			SELECT
				lease_expires_at,
				lease_expires_at > $2::timestamptz,
				lease_expires_at > clock_timestamp(),
				clock_timestamp() > $2::timestamptz
			FROM operations
			WHERE id = $1::uuid
		`,
			task.Claimed.Operation.ID,
			task.Claimed.LeaseExpiresAt,
		).Scan(
			&currentExpiry,
			&renewed,
			&leaseStillValid,
			&initialLeaseExpired,
		)
		if err != nil {
			return operationExecutionResult{
				Disposition:  operationExecutionFailed,
				ErrorCode:    "lease_observation_failed",
				ErrorSummary: "The test executor could not observe the operation lease.",
			}
		}

		if renewed {
			executor.observedRenewal = true
		}

		if initialLeaseExpired {
			executor.observedAfterInitialExpiry = true

			if !renewed || !leaseStillValid {
				return operationExecutionResult{
					Disposition:  operationExecutionFailed,
					ErrorCode:    "lease_renewal_not_observed",
					ErrorSummary: "The lease was not renewed before its original expiry.",
				}
			}

			return operationExecutionResult{
				Disposition: operationExecutionSucceeded,
			}
		}

		select {
		case <-ctx.Done():
			return operationExecutionResult{
				Disposition:  operationExecutionNeedsReconciliation,
				ErrorCode:    "lease_test_cancelled",
				ErrorSummary: "The lease renewal test was cancelled before completion.",
			}
		case <-ticker.C:
		}
	}
}

func TestOperationWorkerRunnerIntegrationRenewsLeaseDuringExecution(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	operationID := createQueuedOperationForQueueTest(
		t,
		fixture,
		"runner-lease-renewal",
		strings.Repeat("c", 40),
	)

	executor := &operationWorkerLeaseRenewalTestExecutor{
		pool: fixture.pool,
	}

	worker := operationWorkerRunner{
		Pool:              fixture.pool,
		WorkerID:          "worker-runner-lease-renewal",
		Executor:          executor,
		LeaseDuration:     600 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		PollInterval:      10 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		8*time.Second,
	)
	defer cancel()

	processed, err := worker.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run worker once: %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}

	if !executor.observedRenewal {
		t.Error("executor never observed the lease being renewed")
	}
	if !executor.observedAfterInitialExpiry {
		t.Error("executor did not continue past the initial lease expiry")
	}

	var (
		status         string
		leaseOwner     *string
		leaseExpiresAt *time.Time
		finishedAt     *time.Time
	)

	err = fixture.pool.QueryRow(ctx, `
		SELECT
			status,
			lease_owner,
			lease_expires_at,
			finished_at
		FROM operations
		WHERE id = $1::uuid
	`, operationID).Scan(
		&status,
		&leaseOwner,
		&leaseExpiresAt,
		&finishedAt,
	)
	if err != nil {
		t.Fatalf("read operation after long execution: %v", err)
	}

	if status != string(operationStatusSucceeded) {
		t.Errorf("status = %q, want succeeded", status)
	}
	if leaseOwner != nil {
		t.Errorf("lease owner = %q, want NULL", *leaseOwner)
	}
	if leaseExpiresAt != nil {
		t.Errorf("lease expiry = %v, want NULL", *leaseExpiresAt)
	}
	if finishedAt == nil {
		t.Error("finished_at is NULL")
	}
}
