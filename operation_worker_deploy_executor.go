package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const operationWorkerDeployRetryDelay = 15 * time.Second

type operationWorkerDeployExecutor struct {
	Pool              *pgxpool.Pool
	ArtifactDirectory string
	Runtime           operationWorkerDeployRuntime
	RetryDelay        time.Duration
}

// Execute verifies the deployment inputs, ensures a durable deployment
// record exists, invokes the configured runtime, and persists its result.
//
// This executor is not registered by main.go until a real runtime adapter
// is configured. Tests can inject a fake runtime without starting containers.
func (executor operationWorkerDeployExecutor) Execute(
	ctx context.Context,
	task operationWorkerTask,
) operationExecutionResult {
	if ctx == nil {
		return failedDeployResult(
			"invalid_deploy_context",
			"A valid deployment execution context is required.",
		)
	}

	if executor.Pool == nil {
		return failedDeployResult(
			"deploy_executor_not_configured",
			"The deployment executor database is not configured.",
		)
	}

	if task.Request.OperationType != operationTypeDeploy ||
		task.Claimed.Operation.OperationType != operationTypeDeploy {
		return failedDeployResult(
			"invalid_deploy_task",
			"The deployment executor received a non-deployment operation.",
		)
	}

	if executor.Runtime == nil {
		return failedDeployResult(
			"deployment_runtime_not_configured",
			"No deployment runtime is configured.",
		)
	}

	artifact, err := loadOperationWorkerDeployArtifact(
		ctx,
		executor.Pool,
		task,
	)
	if err != nil {
		if errors.Is(
			err,
			errOperationWorkerDeployArtifactUnavailable,
		) {
			return failedDeployResult(
				"deployment_artifact_unavailable",
				"The requested verified artifact is unavailable in this repository.",
			)
		}

		if errors.Is(
			err,
			errOperationWorkerDeployArtifactIntegrity,
		) {
			return failedDeployResult(
				"deployment_artifact_integrity_failed",
				"The stored artifact metadata failed integrity validation.",
			)
		}

		return retryDeployResult(
			"deployment_artifact_lookup_failed",
			"The worker could not load the deployment artifact.",
			executor.retryDelay(),
		)
	}

	if ctx.Err() != nil {
		return retryDeployResult(
			"deployment_preflight_interrupted",
			"Deployment preflight was interrupted before runtime execution.",
			executor.retryDelay(),
		)
	}

	artifactPath, err := verifyOperationWorkerDeployArtifactFile(
		executor.ArtifactDirectory,
		artifact,
	)
	if err != nil {
		if errors.Is(
			err,
			errOperationWorkerDeployArtifactFileUnavailable,
		) {
			return failedDeployResult(
				"deployment_artifact_file_unavailable",
				"The verified artifact file is missing from local storage.",
			)
		}

		if errors.Is(
			err,
			errOperationWorkerDeployArtifactFileIntegrity,
		) {
			return failedDeployResult(
				"deployment_artifact_file_integrity_failed",
				"The artifact file failed integrity validation.",
			)
		}

		return retryDeployResult(
			"deployment_artifact_file_check_failed",
			"The worker could not complete the artifact file check.",
			executor.retryDelay(),
		)
	}

	if ctx.Err() != nil {
		return retryDeployResult(
			"deployment_preflight_interrupted",
			"Deployment preflight was interrupted before runtime execution.",
			executor.retryDelay(),
		)
	}

	deployment, err := ensureOperationWorkerDeployRecord(
		ctx,
		executor.Pool,
		task,
		artifact,
	)
	if err != nil {
		if errors.Is(
			err,
			errOperationWorkerDeployRecordConflict,
		) {
			return failedDeployResult(
				"deployment_record_conflict",
				"The existing deployment record conflicts with the requested artifact.",
			)
		}

		// Record creation is idempotent. If its transaction outcome is
		// unknown, a later attempt can recover the same record.
		return retryDeployResult(
			"deployment_record_unavailable",
			"The worker could not confirm the deployment record.",
			executor.retryDelay(),
		)
	}

	if ctx.Err() != nil {
		return retryDeployResult(
			"deployment_preflight_interrupted",
			"Deployment preflight was interrupted before runtime execution.",
			executor.retryDelay(),
		)
	}

	runtimeResult, err := executor.Runtime.Deploy(
		ctx,
		operationWorkerDeployRuntimeRequest{
			OperationID:     task.Claimed.Operation.ID,
			DeploymentID:    deployment.ID,
			ArtifactPath:    artifactPath,
			ArtifactDigest:  artifact.ContentDigest,
			ImageReference:  artifact.Metadata.ImageReference,
			ApplicationPort: task.Request.ApplicationPort,
		},
	)
	if err != nil {
		if ctx.Err() != nil ||
			errors.Is(
				err,
				errOperationWorkerDeployRuntimeOutcomeUnknown,
			) {
			return reconciliationDeployResult(
				"deployment_outcome_unknown",
				"The runtime deployment outcome could not be established.",
			)
		}

		// The runtime contract requires ordinary errors to mean that no
		// runtime side effect remains active. Repeating Deploy with the
		// same operation and deployment IDs must be idempotent.
		return retryDeployResult(
			"deployment_runtime_failed",
			"The runtime could not complete the deployment attempt.",
			executor.retryDelay(),
		)
	}

	if !validDeployRuntimeInstanceReference(
		runtimeResult.RuntimeInstanceReference,
	) {
		return reconciliationDeployResult(
			"deployment_runtime_reference_unknown",
			"The runtime returned an invalid instance reference after deployment.",
		)
	}

	if err := recordOperationWorkerDeployRuntimeResult(
		ctx,
		executor.Pool,
		task,
		deployment,
		artifact,
		runtimeResult.RuntimeInstanceReference,
	); err != nil {
		return reconciliationDeployResult(
			"deployment_result_persistence_unknown",
			"The runtime may have started the application, but its result could not be confirmed in storage.",
		)
	}

	return operationExecutionResult{
		Disposition: operationExecutionSucceeded,
	}
}

func (executor operationWorkerDeployExecutor) retryDelay() time.Duration {
	if executor.RetryDelay <= 0 {
		return operationWorkerDeployRetryDelay
	}

	return executor.RetryDelay
}

func validDeployRuntimeInstanceReference(value string) bool {
	return value != "" &&
		strings.TrimSpace(value) == value &&
		len(value) <= 1024 &&
		utf8.ValidString(value) &&
		!strings.ContainsAny(value, "\x00\r\n")
}

func failedDeployResult(
	code string,
	summary string,
) operationExecutionResult {
	return operationExecutionResult{
		Disposition:  operationExecutionFailed,
		ErrorCode:    code,
		ErrorSummary: summary,
	}
}

func retryDeployResult(
	code string,
	summary string,
	delay time.Duration,
) operationExecutionResult {
	return operationExecutionResult{
		Disposition:  operationExecutionRetry,
		ErrorCode:    code,
		ErrorSummary: summary,
		RetryDelay:   delay,
	}
}

func reconciliationDeployResult(
	code string,
	summary string,
) operationExecutionResult {
	return operationExecutionResult{
		Disposition:  operationExecutionNeedsReconciliation,
		ErrorCode:    code,
		ErrorSummary: summary,
	}
}

// recordOperationWorkerDeployRuntimeResult stores the runtime instance
// reference while confirming that this worker still owns the operation
// lease.
//
// The deployment remains in "deploying" state. This function deliberately
// does not claim the application is healthy or ready.
func recordOperationWorkerDeployRuntimeResult(
	ctx context.Context,
	pool *pgxpool.Pool,
	task operationWorkerTask,
	deployment operationWorkerDeployRecord,
	artifact operationWorkerDeployArtifact,
	runtimeInstanceReference string,
) error {
	if ctx == nil {
		return errors.New(
			"deployment result context is required",
		)
	}

	if pool == nil {
		return errors.New(
			"deployment result database pool is required",
		)
	}

	if !validDeployRuntimeInstanceReference(
		runtimeInstanceReference,
	) {
		return errors.New(
			"runtime instance reference is invalid",
		)
	}

	operationID, err := normalizeOperationUUID(
		task.Claimed.Operation.ID,
	)
	if err != nil || operationID != task.Claimed.Operation.ID {
		return errors.New(
			"deployment operation ID is invalid",
		)
	}

	projectID, err := normalizeOperationUUID(
		task.Claimed.Operation.ProjectID,
	)
	if err != nil || projectID != task.Claimed.Operation.ProjectID {
		return errors.New(
			"deployment project ID is invalid",
		)
	}

	repositoryID, err := normalizeOperationUUID(
		task.Claimed.Operation.RepositoryID,
	)
	if err != nil || repositoryID != task.Claimed.Operation.RepositoryID {
		return errors.New(
			"deployment repository ID is invalid",
		)
	}

	deploymentID, err := normalizeOperationUUID(deployment.ID)
	if err != nil || deploymentID != deployment.ID {
		return errors.New(
			"deployment record ID is invalid",
		)
	}

	artifactID, err := normalizeOperationUUID(artifact.ID)
	if err != nil || artifactID != artifact.ID {
		return errors.New(
			"deployment artifact ID is invalid",
		)
	}

	if deployment.ArtifactID != artifactID ||
		task.Request.OperationType != operationTypeDeploy ||
		task.Claimed.Operation.OperationType != operationTypeDeploy {
		return errOperationWorkerDeployRecordConflict
	}

	if _, err := operationWorkerIDFromLeaseOwner(
		task.Claimed.LeaseOwner,
	); err != nil {
		return errOperationLeaseNotOwned
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf(
			"begin runtime result transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	var (
		status     string
		leaseOwner string
	)

	err = tx.QueryRow(ctx, `
		SELECT status, lease_owner
		FROM operations
		WHERE id = $1::uuid
		  AND project_id = $2::uuid
		  AND repository_id = $3::uuid
		  AND operation_type = 'deploy'
		FOR SHARE
	`,
		operationID,
		projectID,
		repositoryID,
	).Scan(&status, &leaseOwner)

	if errors.Is(err, pgx.ErrNoRows) {
		return errOperationLeaseNotOwned
	}

	if err != nil {
		return fmt.Errorf(
			"load deployment operation lease: %w",
			err,
		)
	}

	if status != string(operationStatusRunning) ||
		leaseOwner != task.Claimed.LeaseOwner {
		return errOperationLeaseNotOwned
	}

	var leaseIsCurrent bool

	err = tx.QueryRow(ctx, `
		SELECT lease_expires_at > clock_timestamp()
		FROM operations
		WHERE id = $1::uuid
	`, operationID).Scan(&leaseIsCurrent)
	if err != nil {
		return fmt.Errorf(
			"check deployment operation lease expiry: %w",
			err,
		)
	}

	if !leaseIsCurrent {
		return errOperationLeaseNotOwned
	}

	var updatedDeploymentID string

	err = tx.QueryRow(ctx, `
		UPDATE deployments
		SET
			observed_state = 'deploying',
			runtime_instance_ref = $6,
			updated_at = now()
		WHERE id = $1::uuid
		  AND project_id = $2::uuid
		  AND repository_id = $3::uuid
		  AND artifact_id = $4::uuid
		  AND last_operation_id = $5::uuid
		  AND desired_state = 'active'
		  AND deleted_at IS NULL
		RETURNING id::text
	`,
		deploymentID,
		projectID,
		repositoryID,
		artifactID,
		operationID,
		runtimeInstanceReference,
	).Scan(&updatedDeploymentID)

	if errors.Is(err, pgx.ErrNoRows) {
		return errOperationWorkerDeployRecordConflict
	}

	if err != nil {
		return fmt.Errorf(
			"persist deployment runtime reference: %w",
			err,
		)
	}

	if updatedDeploymentID != deploymentID {
		return errOperationWorkerDeployRecordConflict
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"commit deployment runtime result: %w",
			err,
		)
	}

	return nil
}

var _ operationExecutor = operationWorkerDeployExecutor{}
