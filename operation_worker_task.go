package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

var errOperationTaskIntegrity = errors.New(
	"claimed operation task failed its integrity check",
)

type operationWorkerTask struct {
	Claimed claimedOperation
	Request normalizedOperationRequest
}

// loadClaimedOperationTask loads and verifies the persisted request for a
// claimed operation. It refuses to return an executable task if the lease
// is stale, the request fingerprint is inconsistent, or the persisted
// operation metadata disagrees with the claim.
//
// The row lock protects the metadata snapshot while it is being checked.
// The caller must still renew the lease during execution and use the same
// lease owner for any subsequent operation outcome.
func loadClaimedOperationTask(
	ctx context.Context,
	pool *pgxpool.Pool,
	claimed claimedOperation,
) (operationWorkerTask, error) {
	var empty operationWorkerTask

	if pool == nil {
		return empty, errors.New("database pool is required")
	}

	operationID, err := normalizeOperationUUID(claimed.Operation.ID)
	if err != nil {
		return empty, fmt.Errorf("operation ID: %w", err)
	}

	if _, err := operationWorkerIDFromLeaseOwner(claimed.LeaseOwner); err != nil {
		return empty, err
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return empty, fmt.Errorf(
			"begin claimed operation task load: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	var (
		projectID              string
		repositoryID            string
		operationTypeValue      string
		status                  string
		leaseOwner              pgtype.Text
		leaseExpiry             pgtype.Timestamptz
		requestPayload          []byte
		requestFingerprint      string
		sourceRevision          pgtype.Text
		parentOperationID       pgtype.Text
	)

	err = tx.QueryRow(ctx, `
		SELECT
			project_id::text,
			repository_id::text,
			operation_type,
			status,
			lease_owner,
			lease_expires_at,
			request_payload::text,
			request_fingerprint,
			source_revision,
			parent_operation_id::text
		FROM operations
		WHERE id = $1::uuid
		FOR SHARE
	`, operationID).Scan(
		&projectID,
		&repositoryID,
		&operationTypeValue,
		&status,
		&leaseOwner,
		&leaseExpiry,
		&requestPayload,
		&requestFingerprint,
		&sourceRevision,
		&parentOperationID,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return empty, errOperationNotFound
	}
	if err != nil {
		return empty, fmt.Errorf(
			"load claimed operation task: %w",
			err,
		)
	}

	if status != string(operationStatusRunning) {
		return empty, errOperationOutcomeStateConflict
	}

	if !leaseOwner.Valid ||
		leaseOwner.String != claimed.LeaseOwner ||
		!leaseExpiry.Valid {
		return empty, errOperationLeaseNotOwned
	}

	// Check the lease after obtaining the row lock. Transaction-start time
	// is not sufficient after a potentially long lock wait.
	var leaseIsCurrent bool

	if err := tx.QueryRow(ctx, `
		SELECT lease_expires_at > clock_timestamp()
		FROM operations
		WHERE id = $1::uuid
	`, operationID).Scan(&leaseIsCurrent); err != nil {
		return empty, fmt.Errorf(
			"check claimed operation lease: %w",
			err,
		)
	}

	if !leaseIsCurrent {
		return empty, errOperationLeaseNotOwned
	}

	// The claim's operation record must match the persisted row.
	if claimed.Operation.ID != operationID ||
		claimed.Operation.ProjectID != projectID ||
		claimed.Operation.RepositoryID != repositoryID ||
		string(claimed.Operation.OperationType) != operationTypeValue ||
		claimed.Operation.RequestFingerprint != requestFingerprint {
		return empty, errOperationTaskIntegrity
	}

	if !operationTaskOptionalTextMatches(
		sourceRevision,
		claimed.Operation.SourceRevision,
	) {
		return empty, errOperationTaskIntegrity
	}

	if !operationTaskOptionalTextMatches(
		parentOperationID,
		claimed.Operation.ParentOperationID,
	) {
		return empty, errOperationTaskIntegrity
	}

	// Re-normalize the persisted JSON and recompute its fingerprint rather
	// than trusting that request_payload still matches the original request.
	request, _, fingerprint, err := normalizeOperationRequest(
		requestPayload,
	)
	if err != nil {
		return empty, fmt.Errorf(
			"%w: persisted request payload is invalid",
			errOperationTaskIntegrity,
		)
	}

	if fingerprint != requestFingerprint {
		return empty, errOperationTaskIntegrity
	}

	if request.OperationType != claimed.Operation.OperationType {
		return empty, errOperationTaskIntegrity
	}

	if sourceRevision.Valid {
		if request.SourceRevision != sourceRevision.String {
			return empty, errOperationTaskIntegrity
		}
	} else if request.SourceRevision != "" {
		return empty, errOperationTaskIntegrity
	}

	if parentOperationID.Valid {
		if request.ParentOperationID != parentOperationID.String {
			return empty, errOperationTaskIntegrity
		}
	} else if request.ParentOperationID != "" {
		return empty, errOperationTaskIntegrity
	}

	if err := tx.Commit(ctx); err != nil {
		return empty, fmt.Errorf(
			"commit claimed operation task load: %w",
			err,
		)
	}

	return operationWorkerTask{
		Claimed: claimed,
		Request: request,
	}, nil
}

func operationTaskOptionalTextMatches(
	stored pgtype.Text,
	expected *string,
) bool {
	if expected == nil {
		return !stored.Valid
	}

	return stored.Valid && stored.String == *expected
}
