package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	operationWorkerTaskLoadRetryDelay   = 5 * time.Second
	operationWorkerRecoveryWriteTimeout = 2 * time.Second
)

type operationWorkerRunner struct {
	Pool              *pgxpool.Pool
	WorkerID          string
	Executor          operationExecutor
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	PollInterval      time.Duration
}

// Run polls the durable operation queue until the context is cancelled.
// It returns on an operational error so a process supervisor can restart the
// worker instead of hiding a persistent database or lease problem.
func (worker operationWorkerRunner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("worker context is required")
	}

	if err := worker.validate(); err != nil {
		return err
	}

	for {
		processed, err := worker.RunOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return err
		}

		if processed {
			continue
		}

		timer := time.NewTimer(worker.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

// RunOnce claims and processes at most one due operation. The bool is true
// when an operation was claimed, even if processing later returns an error.
func (worker operationWorkerRunner) RunOnce(
	ctx context.Context,
) (bool, error) {
	if ctx == nil {
		return false, errors.New("worker context is required")
	}

	if err := worker.validate(); err != nil {
		return false, err
	}

	claimed, err := claimNextOperation(
		ctx,
		worker.Pool,
		worker.WorkerID,
		worker.LeaseDuration,
	)
	if errors.Is(err, errNoDueOperationAvailable) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim next operation: %w", err)
	}

	if err := worker.executeClaimedOperation(ctx, claimed); err != nil {
		return true, fmt.Errorf(
			"process operation %s: %w",
			claimed.Operation.ID,
			err,
		)
	}

	return true, nil
}

func (worker operationWorkerRunner) validate() error {
	if worker.Pool == nil {
		return errors.New("database pool is required")
	}
	if worker.Executor == nil {
		return errors.New("operation executor is required")
	}
	if _, err := normalizeOperationWorkerID(worker.WorkerID); err != nil {
		return fmt.Errorf("worker ID: %w", err)
	}
	if worker.LeaseDuration < time.Microsecond {
		return errors.New("lease duration must be at least one microsecond")
	}
	if worker.HeartbeatInterval <= 0 ||
		worker.HeartbeatInterval >= worker.LeaseDuration/2 {
		return errors.New(
			"heartbeat interval must be positive and less than half the lease duration",
		)
	}
	if worker.PollInterval <= 0 {
		return errors.New("poll interval must be positive")
	}

	return nil
}

func (worker operationWorkerRunner) executeClaimedOperation(
	ctx context.Context,
	claimed claimedOperation,
) error {
	task, err := loadClaimedOperationTask(ctx, worker.Pool, claimed)
	if err != nil {
		if errors.Is(err, errOperationTaskIntegrity) {
			_, reconcileErr := worker.markNeedsReconciliation(
				claimed,
				"task_integrity_failure",
				"The persisted task failed integrity checks before execution; reconciliation is required.",
			)
			if reconcileErr != nil {
				return errors.Join(
					fmt.Errorf("load claimed task: %w", err),
					fmt.Errorf("record task integrity failure: %w", reconcileErr),
				)
			}

			return nil
		}

		// Execution has not started, so retrying task loading is safe. A
		// lease conflict or database outage can still prevent this write.
		retryContext, cancelRetry := context.WithTimeout(
			context.Background(),
			operationWorkerRecoveryWriteTimeout,
		)
		defer cancelRetry()

		_, retryErr := recordOperationExecutionResult(
			retryContext,
			worker.Pool,
			operationWorkerTask{Claimed: claimed},
			operationExecutionResult{
				Disposition:  operationExecutionRetry,
				ErrorCode:    "task_load_failed",
				ErrorSummary: "The worker could not load the persisted task; execution was not started.",
				RetryDelay:   operationWorkerTaskLoadRetryDelay,
			},
		)
		if retryErr != nil {
			return errors.Join(
				fmt.Errorf("load claimed task: %w", err),
				fmt.Errorf("schedule safe task-load retry: %w", retryErr),
			)
		}

		return nil
	}

	return worker.executeLoadedTask(ctx, task)
}

func (worker operationWorkerRunner) executeLoadedTask(
	ctx context.Context,
	task operationWorkerTask,
) error {
	executionContext, cancelExecution := context.WithCancel(ctx)
	defer cancelExecution()

	heartbeatStop := make(chan struct{})
	heartbeatDone := make(chan struct{})
	heartbeatErrors := make(chan error, 1)

	go worker.renewLeaseUntilStopped(
		ctx,
		task.Claimed,
		cancelExecution,
		heartbeatStop,
		heartbeatErrors,
		heartbeatDone,
	)

	result := worker.Executor.Execute(executionContext, task)
	close(heartbeatStop)
	<-heartbeatDone
	cancelExecution()

	select {
	case heartbeatErr := <-heartbeatErrors:
		_, reconcileErr := worker.markNeedsReconciliation(
			task.Claimed,
			"lease_renewal_failed",
			"Lease renewal failed while work was running; the external outcome requires reconciliation.",
		)
		if reconcileErr != nil {
			return errors.Join(
				fmt.Errorf("renew operation lease: %w", heartbeatErr),
				fmt.Errorf("record uncertain execution outcome: %w", reconcileErr),
			)
		}

		return nil
	default:
	}

	if ctx.Err() != nil {
		_, reconcileErr := worker.markNeedsReconciliation(
			task.Claimed,
			"worker_cancelled",
			"The worker stopped while execution was in progress; the external outcome requires reconciliation.",
		)
		if reconcileErr != nil {
			return errors.Join(ctx.Err(), reconcileErr)
		}

		return ctx.Err()
	}

	if _, err := recordOperationExecutionResult(
		ctx,
		worker.Pool,
		task,
		result,
	); err != nil {
		_, reconcileErr := worker.markNeedsReconciliation(
			task.Claimed,
			"outcome_persist_failed",
			"The worker could not confirm persistence of the execution outcome; reconciliation is required.",
		)
		if reconcileErr != nil {
			return errors.Join(
				fmt.Errorf("record execution outcome: %w", err),
				fmt.Errorf("record uncertain execution outcome: %w", reconcileErr),
			)
		}

		return fmt.Errorf("record execution outcome: %w", err)
	}

	return nil
}

func (worker operationWorkerRunner) renewLeaseUntilStopped(
	ctx context.Context,
	claimed claimedOperation,
	cancelExecution context.CancelFunc,
	stop <-chan struct{},
	errorsFound chan<- error,
	done chan<- struct{},
) {
	defer close(done)

	ticker := time.NewTicker(worker.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			renewContext, cancel := context.WithTimeout(
				ctx,
				worker.HeartbeatInterval,
			)
			_, err := renewOperationLease(
				renewContext,
				worker.Pool,
				claimed.Operation.ID,
				claimed.LeaseOwner,
				worker.LeaseDuration,
			)
			cancel()

			if err == nil {
				continue
			}

			select {
			case <-stop:
				return
			default:
			}
			if ctx.Err() != nil {
				return
			}

			errorsFound <- err
			cancelExecution()
			return
		}
	}
}

func (worker operationWorkerRunner) markNeedsReconciliation(
	claimed claimedOperation,
	errorCode string,
	errorSummary string,
) (operationRecord, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		operationWorkerRecoveryWriteTimeout,
	)
	defer cancel()

	return markOperationNeedsReconciliation(
		ctx,
		worker.Pool,
		claimed.Operation.ID,
		claimed.LeaseOwner,
		errorCode,
		errorSummary,
	)
}
