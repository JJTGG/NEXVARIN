package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	errOperationWorkerDeployRecordConflict = errors.New(
		"deployment record conflicts with the requested operation",
	)

	errOperationWorkerDeployRecordOutcomeUnknown = errors.New(
		"deployment record transaction outcome is unknown",
	)
)

type operationWorkerDeployRecord struct {
	ID         string
	ArtifactID string
}

// ensureOperationWorkerDeployRecord creates or recovers the deployment
// record associated with a claimed deploy operation.
//
// The operation ID is the idempotency key for this database record.
// The database uniqueness constraint prevents one operation from being
// associated with multiple deployment records.
//
// This function does not start an application or invoke the runtime.
func ensureOperationWorkerDeployRecord(
	ctx context.Context,
	pool *pgxpool.Pool,
	task operationWorkerTask,
	artifact operationWorkerDeployArtifact,
) (operationWorkerDeployRecord, error) {
	var empty operationWorkerDeployRecord

	if ctx == nil {
		return empty, errors.New(
			"deployment record context is required",
		)
	}

	if pool == nil {
		return empty, errors.New(
			"deployment record database pool is required",
		)
	}

	if task.Request.OperationType != operationTypeDeploy ||
		task.Claimed.Operation.OperationType != operationTypeDeploy {
		return empty, errors.New(
			"deployment record helper received a non-deployment task",
		)
	}

	operationID, err := normalizeOperationUUID(
		task.Claimed.Operation.ID,
	)
	if err != nil || operationID != task.Claimed.Operation.ID {
		return empty, errors.New(
			"deployment operation ID is invalid",
		)
	}

	projectID, err := normalizeOperationUUID(
		task.Claimed.Operation.ProjectID,
	)
	if err != nil || projectID != task.Claimed.Operation.ProjectID {
		return empty, errors.New(
			"deployment project ID is invalid",
		)
	}

	repositoryID, err := normalizeOperationUUID(
		task.Claimed.Operation.RepositoryID,
	)
	if err != nil || repositoryID != task.Claimed.Operation.RepositoryID {
		return empty, errors.New(
			"deployment repository ID is invalid",
		)
	}

	requestedArtifactID, err := normalizeOperationUUID(
		task.Request.ArtifactID,
	)
	if err != nil {
		return empty, errors.New(
			"requested deployment artifact ID is invalid",
		)
	}

	artifactID, err := normalizeOperationUUID(artifact.ID)
	if err != nil ||
		artifactID != artifact.ID ||
		artifactID != requestedArtifactID ||
		artifact.ProjectID != projectID ||
		artifact.RepositoryID != repositoryID {
		return empty, errOperationWorkerDeployRecordConflict
	}

	if _, err := operationWorkerIDFromLeaseOwner(
		task.Claimed.LeaseOwner,
	); err != nil {
		return empty, errOperationLeaseNotOwned
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return empty, fmt.Errorf(
			"begin deployment record transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	// Only the current lease owner may create or recover this record.
	// Check the database clock after acquiring the row lock.
	var requestedBy string

	err = tx.QueryRow(ctx, `
		SELECT requested_by
		FROM operations
		WHERE id = $1::uuid
		  AND project_id = $2::uuid
		  AND repository_id = $3::uuid
		  AND operation_type = 'deploy'
		  AND status = 'running'
		  AND lease_owner = $4
		  AND lease_expires_at > clock_timestamp()
		FOR SHARE
	`,
		operationID,
		projectID,
		repositoryID,
		task.Claimed.LeaseOwner,
	).Scan(&requestedBy)

	if errors.Is(err, pgx.ErrNoRows) {
		return empty, errOperationLeaseNotOwned
	}

	if err != nil {
		return empty, fmt.Errorf(
			"validate deployment operation lease: %w",
			err,
		)
	}

	if strings.TrimSpace(requestedBy) == "" {
		return empty, errors.New(
			"deployment operation requester is invalid",
		)
	}

	var deploymentID string

	err = tx.QueryRow(ctx, `
		INSERT INTO deployments (
			project_id,
			repository_id,
			artifact_id,
			last_operation_id,
			created_by_subject
		)
		VALUES (
			$1::uuid,
			$2::uuid,
			$3::uuid,
			$4::uuid,
			$5
		)
		ON CONFLICT (
			project_id,
			repository_id,
			last_operation_id
		)
		WHERE last_operation_id IS NOT NULL
		DO NOTHING
		RETURNING id::text
	`,
		projectID,
		repositoryID,
		artifactID,
		operationID,
		requestedBy,
	).Scan(&deploymentID)

	switch {
	case err == nil:
		// A new record was created in this transaction.

	case errors.Is(err, pgx.ErrNoRows):
		// A previous attempt already created the record.
		// Recover it instead of inserting a duplicate.
		var (
			existingArtifactID string
			desiredState       string
			isDeleted          bool
		)

		err = tx.QueryRow(ctx, `
			SELECT
				id::text,
				COALESCE(artifact_id::text, ''),
				desired_state,
				deleted_at IS NOT NULL
			FROM deployments
			WHERE project_id = $1::uuid
			  AND repository_id = $2::uuid
			  AND last_operation_id = $3::uuid
			FOR UPDATE
		`,
			projectID,
			repositoryID,
			operationID,
		).Scan(
			&deploymentID,
			&existingArtifactID,
			&desiredState,
			&isDeleted,
		)

		if errors.Is(err, pgx.ErrNoRows) {
			return empty, errOperationWorkerDeployRecordConflict
		}

		if err != nil {
			return empty, fmt.Errorf(
				"recover existing deployment record: %w",
				err,
			)
		}

		if existingArtifactID != artifactID ||
			desiredState != "active" ||
			isDeleted {
			return empty, errOperationWorkerDeployRecordConflict
		}

	default:
		return empty, fmt.Errorf(
			"create deployment record: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return empty, fmt.Errorf(
			"%w: %v",
			errOperationWorkerDeployRecordOutcomeUnknown,
			err,
		)
	}

	return operationWorkerDeployRecord{
		ID:         deploymentID,
		ArtifactID: artifactID,
	}, nil
}
