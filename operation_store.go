package main

import (
	"context"
	"encoding/hex"
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
	errIdempotencyConflict = errors.New(
		"idempotency key was already used with a different request",
	)
	errOperationScopeNotFound = errors.New(
		"project or repository was not found in the requested scope",
	)
	errOperationScopeInactive = errors.New(
		"project or repository is not active for this operation",
	)
	errOperationResourceUnavailable = errors.New(
		"referenced resource is not available in the requested project and repository",
	)
)

// createOperationInput contains the server-verified scope and the raw
// request body. Callers must authenticate and authorize the requester
// before invoking createOperation.
type createOperationInput struct {
	ProjectID        string
	RepositoryID     string
	RequestedBy      string
	IdempotencyScope string
	IdempotencyKey   string
	RequestBody      []byte
}

// createOperationResult identifies whether the returned operation was
// newly created or replayed from a previous request with the same key
// and normalized request fingerprint.
type createOperationResult struct {
	Operation operationRecord
	Replayed  bool
}

// operationRecord is the persisted operation representation used by
// the control plane. RequestFingerprint is internal and is not an API
// response field.
type operationRecord struct {
	ID                 string          `json:"id"`
	ProjectID          string          `json:"projectId"`
	RepositoryID       string          `json:"repositoryId"`
	ParentOperationID  *string         `json:"parentOperationId,omitempty"`
	OperationType      operationType   `json:"operationType"`
	Status             operationStatus `json:"status"`
	RequestFingerprint string          `json:"-"`
	SourceRevision     *string         `json:"sourceRevision,omitempty"`
	AttemptCount       int             `json:"attemptCount"`
	MaxAttempts        int             `json:"maxAttempts"`
	ErrorCode          *string         `json:"errorCode,omitempty"`
	ErrorSummary       *string         `json:"errorSummary,omitempty"`
	StartedAt          *time.Time      `json:"startedAt,omitempty"`
	FinishedAt         *time.Time      `json:"finishedAt,omitempty"`
	CreatedAt          time.Time       `json:"createdAt"`
	UpdatedAt          time.Time       `json:"updatedAt"`
}

const operationRecordColumns = `
	id::text,
	project_id::text,
	repository_id::text,
	parent_operation_id::text,
	operation_type,
	status,
	request_fingerprint,
	source_revision,
	attempt_count,
	max_attempts,
	error_code,
	error_summary,
	started_at,
	finished_at,
	created_at,
	updated_at
`

// createOperation atomically inserts an operation, its initial history
// event, and its initial outbox event. A repeated idempotency key returns
// the original operation only when its normalized request fingerprint
// matches exactly.
//
// Authentication and authorization are deliberately outside this store
// and must be enforced by the API layer before this function is called.
func createOperation(
	ctx context.Context,
	pool *pgxpool.Pool,
	input createOperationInput,
) (createOperationResult, error) {
	var emptyResult createOperationResult

	if pool == nil {
		return emptyResult, errors.New("database pool is required")
	}

	projectID, err := normalizeOperationUUID(input.ProjectID)
	if err != nil {
		return emptyResult, fmt.Errorf("projectId: %w", err)
	}

	repositoryID, err := normalizeOperationUUID(input.RepositoryID)
	if err != nil {
		return emptyResult, fmt.Errorf("repositoryId: %w", err)
	}

	projectUUID, err := operationUUIDValue(projectID)
	if err != nil {
		return emptyResult, fmt.Errorf("projectId: %w", err)
	}

	repositoryUUID, err := operationUUIDValue(repositoryID)
	if err != nil {
		return emptyResult, fmt.Errorf("repositoryId: %w", err)
	}

	if strings.TrimSpace(input.RequestedBy) == "" ||
		!utf8.ValidString(input.RequestedBy) ||
		strings.IndexByte(input.RequestedBy, 0) >= 0 {
		return emptyResult, errors.New(
			"requestedBy must be a non-empty UTF-8 subject",
		)
	}

	if strings.TrimSpace(input.IdempotencyScope) == "" ||
		!utf8.ValidString(input.IdempotencyScope) ||
		strings.IndexByte(input.IdempotencyScope, 0) >= 0 ||
		utf8.RuneCountInString(input.IdempotencyScope) > 255 {
		return emptyResult, errors.New(
			"idempotencyScope must contain between 1 and 255 characters",
		)
	}

	if strings.TrimSpace(input.IdempotencyKey) == "" ||
		!utf8.ValidString(input.IdempotencyKey) ||
		strings.IndexByte(input.IdempotencyKey, 0) >= 0 ||
		utf8.RuneCountInString(input.IdempotencyKey) > 255 {
		return emptyResult, errors.New(
			"idempotencyKey must contain between 1 and 255 characters",
		)
	}

	normalized, canonicalJSON, fingerprint, err :=
		normalizeOperationRequest(input.RequestBody)
	if err != nil {
		return emptyResult, fmt.Errorf(
			"validate operation request: %w",
			err,
		)
	}

	var parentOperationID any
	var parentOperationUUID pgtype.UUID

	if normalized.ParentOperationID != "" {
		parentOperationUUID, err =
			operationUUIDValue(normalized.ParentOperationID)
		if err != nil {
			return emptyResult, fmt.Errorf(
				"parentOperationId: %w",
				err,
			)
		}
		parentOperationID = parentOperationUUID
	}

	var sourceRevision any
	if normalized.SourceRevision != "" {
		sourceRevision = normalized.SourceRevision
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return emptyResult, fmt.Errorf(
			"begin operation transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	// Resolve the requested scope before attempting the idempotent insert.
	// Existence is required for both new requests and replays; active-state
	// checks are only applied when this transaction creates new work.
	var projectStatus string
	var repositoryActive bool

	err = tx.QueryRow(ctx, `
		SELECT p.status, r.is_active
		FROM projects p
		JOIN repositories r ON r.project_id = p.id
		WHERE p.id = $1::uuid
		  AND r.id = $2::uuid
		FOR SHARE OF p, r
	`, projectUUID, repositoryUUID).Scan(
		&projectStatus,
		&repositoryActive,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return emptyResult, errOperationScopeNotFound
	}
	if err != nil {
		return emptyResult, fmt.Errorf(
			"validate project and repository scope: %w",
			err,
		)
	}

	// Resolve an existing key before validating request-specific resources.
	// Reusing a key for a different normalized request is a conflict even
	// if that new request points to a missing artifact or parent operation.
	replay, found, err := lookupIdempotentOperation(
		ctx,
		tx,
		projectUUID,
		repositoryID,
		input.RequestedBy,
		input.IdempotencyScope,
		input.IdempotencyKey,
		fingerprint,
	)
	if err != nil {
		return emptyResult, err
	}

	if found {
		if err := tx.Commit(ctx); err != nil {
			return emptyResult, fmt.Errorf(
				"commit idempotent replay transaction: %w",
				err,
			)
		}
		return replay, nil
	}

	// A reconcile operation must reference an existing operation in the
	// same project and repository. The composite foreign key enforces the
	// same invariant at the database boundary as well.
	if normalized.OperationType == operationTypeReconcile {
		var parentID string

		err := tx.QueryRow(ctx, `
			SELECT id::text
			FROM operations
			WHERE project_id = $1::uuid
			  AND repository_id = $2::uuid
			  AND id = $3::uuid
			FOR SHARE
		`, projectUUID, repositoryUUID, parentOperationUUID).Scan(
			&parentID,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return emptyResult, errOperationResourceUnavailable
		}
		if err != nil {
			return emptyResult, fmt.Errorf(
				"check reconciliation parent: %w",
				err,
			)
		}
	}

	insertQuery := `
		INSERT INTO operations (
			project_id,
			repository_id,
			parent_operation_id,
			operation_type,
			status,
			requested_by,
			idempotency_scope,
			idempotency_key,
			request_fingerprint,
			request_payload,
			source_revision
		)
		VALUES (
			$1::uuid,
			$2::uuid,
			$3::uuid,
			$4,
			'queued',
			$5,
			$6,
			$7,
			$8,
			$9::jsonb,
			$10
		)
		ON CONFLICT (
			project_id,
			requested_by,
			idempotency_scope,
			idempotency_key
		) DO NOTHING
		RETURNING ` + operationRecordColumns

	record, err := scanOperationRecord(tx.QueryRow(
		ctx,
		insertQuery,
		projectUUID,
		repositoryUUID,
		parentOperationID,
		string(normalized.OperationType),
		input.RequestedBy,
		input.IdempotencyScope,
		input.IdempotencyKey,
		fingerprint,
		string(canonicalJSON),
		sourceRevision,
	))

	if errors.Is(err, pgx.ErrNoRows) {
		// Another transaction may have created this key after our initial
		// lookup. Re-read it and apply the same replay/conflict rules.
		replay, found, lookupErr := lookupIdempotentOperation(
			ctx,
			tx,
			projectUUID,
			repositoryID,
			input.RequestedBy,
			input.IdempotencyScope,
			input.IdempotencyKey,
			fingerprint,
		)
		if lookupErr != nil {
			return emptyResult, lookupErr
		}
		if !found {
			return emptyResult, errors.New(
				"idempotency conflict occurred but the existing operation could not be found",
			)
		}

		if err := tx.Commit(ctx); err != nil {
			return emptyResult, fmt.Errorf(
				"commit concurrent idempotent replay transaction: %w",
				err,
			)
		}
		return replay, nil
	}

	if err != nil {
		return emptyResult, fmt.Errorf("insert operation: %w", err)
	}

	if (normalized.OperationType == operationTypeBuild ||
		normalized.OperationType == operationTypeDeploy) &&
		(projectStatus != "active" || !repositoryActive) {
		return emptyResult, errOperationScopeInactive
	}

	if err := validateNewOperationResource(
		ctx,
		tx,
		projectUUID,
		repositoryUUID,
		normalized,
	); err != nil {
		return emptyResult, err
	}

	recordUUID, err := operationUUIDValue(record.ID)
	if err != nil {
		return emptyResult, fmt.Errorf(
			"parse operation ID returned by PostgreSQL: %w",
			err,
		)
	}

	// Keep the queued history record in the same transaction as the
	// operation itself.
	if _, err := tx.Exec(ctx, `
		INSERT INTO operation_events (
			project_id,
			operation_id,
			event_type,
			from_status,
			to_status,
			actor_subject
		)
		VALUES (
			$1::uuid,
			$2::uuid,
			'operation.queued',
			NULL,
			'queued',
			$3
		)
	`, projectUUID, recordUUID, input.RequestedBy); err != nil {
		return emptyResult, fmt.Errorf(
			"insert initial operation event: %w",
			err,
		)
	}

	outboxPayload, err := json.Marshal(struct {
		OperationID   string        `json:"operationId"`
		OperationType operationType `json:"operationType"`
	}{
		OperationID:   record.ID,
		OperationType: normalized.OperationType,
	})
	if err != nil {
		return emptyResult, fmt.Errorf(
			"encode initial outbox payload: %w",
			err,
		)
	}

	// The worker can discover the work through the durable outbox only
	// after this transaction commits.
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (
			project_id,
			operation_id,
			event_type,
			dedupe_key,
			payload
		)
		VALUES (
			$1::uuid,
			$2::uuid,
			'operation.queued',
			$3,
			$4::jsonb
		)
	`,
		projectUUID,
		recordUUID,
		"operation:"+record.ID+":queued",
		string(outboxPayload),
	); err != nil {
		return emptyResult, fmt.Errorf(
			"insert initial outbox event: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return emptyResult, fmt.Errorf(
			"commit operation transaction: %w",
			err,
		)
	}

	return createOperationResult{
		Operation: record,
		Replayed:  false,
	}, nil
}

func operationUUIDValue(value string) (pgtype.UUID, error) {
	normalized, err := normalizeOperationUUID(value)
	if err != nil {
		return pgtype.UUID{}, err
	}

	decoded, err := hex.DecodeString(
		strings.ReplaceAll(normalized, "-", ""),
	)
	if err != nil {
		return pgtype.UUID{}, errors.New(
			"UUID contains invalid hexadecimal characters",
		)
	}

	if len(decoded) != 16 {
		return pgtype.UUID{}, errors.New(
			"UUID must contain exactly 16 bytes",
		)
	}

	var valueUUID pgtype.UUID
	copy(valueUUID.Bytes[:], decoded)
	valueUUID.Valid = true

	return valueUUID, nil
}

func lookupIdempotentOperation(
	ctx context.Context,
	tx pgx.Tx,
	projectID pgtype.UUID,
	repositoryID string,
	requestedBy string,
	scope string,
	key string,
	fingerprint string,
) (createOperationResult, bool, error) {
	existing, err := scanOperationRecord(tx.QueryRow(ctx, `
		SELECT `+operationRecordColumns+`
		FROM operations
		WHERE project_id = $1::uuid
		  AND requested_by = $2
		  AND idempotency_scope = $3
		  AND idempotency_key = $4
	`, projectID, requestedBy, scope, key))

	if errors.Is(err, pgx.ErrNoRows) {
		return createOperationResult{}, false, nil
	}
	if err != nil {
		return createOperationResult{}, false, fmt.Errorf(
			"load existing idempotent operation: %w",
			err,
		)
	}

	if existing.RepositoryID != repositoryID ||
		existing.RequestFingerprint != fingerprint {
		return createOperationResult{}, false, errIdempotencyConflict
	}

	return createOperationResult{
		Operation: existing,
		Replayed:  true,
	}, true, nil
}

func scanOperationRecord(row pgx.Row) (operationRecord, error) {
	var record operationRecord
	var operationTypeValue string
	var statusValue string
	var parentOperationID pgtype.Text
	var sourceRevision pgtype.Text
	var errorCode pgtype.Text
	var errorSummary pgtype.Text
	var startedAt pgtype.Timestamptz
	var finishedAt pgtype.Timestamptz

	if err := row.Scan(
		&record.ID,
		&record.ProjectID,
		&record.RepositoryID,
		&parentOperationID,
		&operationTypeValue,
		&statusValue,
		&record.RequestFingerprint,
		&sourceRevision,
		&record.AttemptCount,
		&record.MaxAttempts,
		&errorCode,
		&errorSummary,
		&startedAt,
		&finishedAt,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return operationRecord{}, err
	}

	record.OperationType = operationType(operationTypeValue)
	record.Status = operationStatus(statusValue)
	record.ParentOperationID = textPointer(parentOperationID)
	record.SourceRevision = textPointer(sourceRevision)
	record.ErrorCode = textPointer(errorCode)
	record.ErrorSummary = textPointer(errorSummary)
	record.StartedAt = timestampPointer(startedAt)
	record.FinishedAt = timestampPointer(finishedAt)

	return record, nil
}

func textPointer(value pgtype.Text) *string {
	if !value.Valid {
		return nil
	}

	result := value.String
	return &result
}

func timestampPointer(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}

	result := value.Time
	return &result
}

func validateNewOperationResource(
	ctx context.Context,
	tx pgx.Tx,
	projectID pgtype.UUID,
	repositoryID pgtype.UUID,
	request normalizedOperationRequest,
) error {
	switch request.OperationType {
	case operationTypeDeploy:
		artifactUUID, err := operationUUIDValue(request.ArtifactID)
		if err != nil {
			return fmt.Errorf("artifactId: %w", err)
		}

		var foundArtifact string
		err = tx.QueryRow(ctx, `
			SELECT id::text
			FROM artifacts
			WHERE id = $1::uuid
			  AND project_id = $2::uuid
			  AND repository_id = $3::uuid
			  AND verification_status = 'verified'
			FOR SHARE
		`, artifactUUID, projectID, repositoryID).Scan(
			&foundArtifact,
		)

		if errors.Is(err, pgx.ErrNoRows) {
			return errOperationResourceUnavailable
		}
		if err != nil {
			return fmt.Errorf("validate deployment artifact: %w", err)
		}

	case operationTypeCleanup:
		deploymentUUID, err := operationUUIDValue(request.DeploymentID)
		if err != nil {
			return fmt.Errorf("deploymentId: %w", err)
		}

		var foundDeployment string
		err = tx.QueryRow(ctx, `
			SELECT id::text
			FROM deployments
			WHERE id = $1::uuid
			  AND project_id = $2::uuid
			  AND repository_id = $3::uuid
			FOR SHARE
		`, deploymentUUID, projectID, repositoryID).Scan(
			&foundDeployment,
		)

		if errors.Is(err, pgx.ErrNoRows) {
			return errOperationResourceUnavailable
		}
		if err != nil {
			return fmt.Errorf("validate cleanup deployment: %w", err)
		}
	}

	return nil
}