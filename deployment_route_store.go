
package main

import (
	"context"
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
	errDeploymentRouteNotFound = errors.New(
		"deployment route was not found in the requested scope",
	)
	errDeploymentRouteDeploymentUnavailable = errors.New(
		"deployment is inactive or deleted and cannot receive a new route",
	)
	errDeploymentRouteOutcomeUnknown = errors.New(
		"deployment route creation transaction outcome is unknown",
	)
)

type deploymentRouteRecord struct {
	ID                    string     `json:"id"`
	ProjectID             string     `json:"projectId"`
	RepositoryID          string     `json:"repositoryId"`
	DeploymentID          string     `json:"deploymentId"`
	Hostname              string     `json:"hostname"`
	DesiredState          string     `json:"desiredState"`
	ObservedState         string     `json:"observedState"`
	DesiredGeneration     int64      `json:"desiredGeneration"`
	ObservedGeneration    *int64     `json:"observedGeneration,omitempty"`
	ReconcileAttemptCount int32      `json:"reconcileAttemptCount"`
	NextReconcileAt       time.Time  `json:"nextReconcileAt"`
	LeaseOwner            *string    `json:"leaseOwner,omitempty"`
	LeaseExpiresAt        *time.Time `json:"leaseExpiresAt,omitempty"`
	LastErrorCode         *string    `json:"lastErrorCode,omitempty"`
	LastReconciledAt      *time.Time `json:"lastReconciledAt,omitempty"`
	CreatedAt             time.Time  `json:"createdAt"`
	UpdatedAt             time.Time  `json:"updatedAt"`
}

type createDeploymentRouteInput struct {
	ProjectID     string
	RepositoryID  string
	DeploymentID  string
	PreviewSuffix string
}

type createDeploymentRouteResult struct {
	Route    deploymentRouteRecord
	Replayed bool
}

const deploymentRouteRecordColumns = `
	id::text,
	project_id::text,
	repository_id::text,
	deployment_id::text,
	hostname,
	desired_state,
	observed_state,
	desired_generation,
	observed_generation,
	reconcile_attempt_count,
	next_reconcile_at,
	lease_owner,
	lease_expires_at,
	last_error_code,
	last_reconciled_at,
	created_at,
	updated_at
`

// createDeploymentRoute creates the durable route resource for a deployment.
//
// Hostnames are derived from the deployment UUID and an operator-controlled
// preview suffix. Callers must not pass arbitrary client-supplied hostnames.
//
// The route begins in pending observed state. This function does not inspect
// Caddy, activate an ingress route, or modify deployments.preview_address.
//
// A route can be created while its deployment is still starting. The ingress
// controller must independently verify deployment eligibility before routing.
func createDeploymentRoute(
	ctx context.Context,
	pool *pgxpool.Pool,
	input createDeploymentRouteInput,
) (createDeploymentRouteResult, error) {
	var empty createDeploymentRouteResult

	if ctx == nil {
		return empty, errors.New(
			"deployment route context is required",
		)
	}

	if pool == nil {
		return empty, errors.New(
			"deployment route database pool is required",
		)
	}

	projectID, err := normalizeOperationUUID(input.ProjectID)
	if err != nil {
		return empty, fmt.Errorf("projectId: %w", err)
	}

	repositoryID, err := normalizeOperationUUID(input.RepositoryID)
	if err != nil {
		return empty, fmt.Errorf("repositoryId: %w", err)
	}

	deploymentID, err := normalizeOperationUUID(input.DeploymentID)
	if err != nil {
		return empty, fmt.Errorf("deploymentId: %w", err)
	}

	projectUUID, err := operationUUIDValue(projectID)
	if err != nil {
		return empty, fmt.Errorf("projectId: %w", err)
	}

	repositoryUUID, err := operationUUIDValue(repositoryID)
	if err != nil {
		return empty, fmt.Errorf("repositoryId: %w", err)
	}

	deploymentUUID, err := operationUUIDValue(deploymentID)
	if err != nil {
		return empty, fmt.Errorf("deploymentId: %w", err)
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return empty, fmt.Errorf(
			"begin deployment route transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	// A route is immutable once created. Check for an existing route first
	// so retries continue to return its original hostname, even if the
	// operator changes the configured preview suffix later.
	existing, err := scanDeploymentRouteRecord(tx.QueryRow(
		ctx,
		`
			SELECT `+deploymentRouteRecordColumns+`
			FROM deployment_routes
			WHERE project_id = $1::uuid
			  AND repository_id = $2::uuid
			  AND deployment_id = $3::uuid
		`,
		projectUUID,
		repositoryUUID,
		deploymentUUID,
	))

	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return empty, fmt.Errorf(
				"commit existing deployment route lookup: %w",
				err,
			)
		}

		return createDeploymentRouteResult{
			Route:    existing,
			Replayed: true,
		}, nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return empty, fmt.Errorf(
			"check existing deployment route: %w",
			err,
		)
	}

	hostname, err := deploymentRouteHostname(
		deploymentID,
		input.PreviewSuffix,
	)
	if err != nil {
		return empty, fmt.Errorf(
			"allocate deployment route hostname: %w",
			err,
		)
	}

	// Keep deployment eligibility stable until the route record commits.
	// The AFTER UPDATE trigger will schedule reconciliation if deployment
	// eligibility changes after this transaction releases its lock.
	var (
		desiredState string
		isDeleted    bool
	)

	err = tx.QueryRow(ctx, `
		SELECT
			desired_state,
			deleted_at IS NOT NULL
		FROM deployments
		WHERE project_id = $1::uuid
		  AND repository_id = $2::uuid
		  AND id = $3::uuid
		FOR SHARE
	`,
		projectUUID,
		repositoryUUID,
		deploymentUUID,
	).Scan(
		&desiredState,
		&isDeleted,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return empty, errDeploymentRouteNotFound
	}

	if err != nil {
		return empty, fmt.Errorf(
			"validate deployment route ownership: %w",
			err,
		)
	}

	if desiredState != "active" || isDeleted {
		return empty, errDeploymentRouteDeploymentUnavailable
	}

	record, err := scanDeploymentRouteRecord(tx.QueryRow(
		ctx,
		`
			INSERT INTO deployment_routes (
				project_id,
				repository_id,
				deployment_id,
				hostname
			)
			VALUES (
				$1::uuid,
				$2::uuid,
				$3::uuid,
				$4
			)
			ON CONFLICT (deployment_id) DO NOTHING
			RETURNING `+deploymentRouteRecordColumns,
		projectUUID,
		repositoryUUID,
		deploymentUUID,
		hostname,
	))

	replayed := false

	if errors.Is(err, pgx.ErrNoRows) {
		// A concurrent request may have created this deployment's route
		// after the initial lookup. Recover the existing immutable record.
		record, err = scanDeploymentRouteRecord(tx.QueryRow(
			ctx,
			`
				SELECT `+deploymentRouteRecordColumns+`
				FROM deployment_routes
				WHERE project_id = $1::uuid
				  AND repository_id = $2::uuid
				  AND deployment_id = $3::uuid
			`,
			projectUUID,
			repositoryUUID,
			deploymentUUID,
		))

		if errors.Is(err, pgx.ErrNoRows) {
			return empty, errors.New(
				"deployment route creation conflicted but the existing route could not be recovered",
			)
		}

		if err == nil {
			replayed = true
		}
	}

	if err != nil {
		return empty, fmt.Errorf(
			"create deployment route: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return empty, fmt.Errorf(
			"%w: %v",
			errDeploymentRouteOutcomeUnknown,
			err,
		)
	}

	return createDeploymentRouteResult{
		Route:    record,
		Replayed: replayed,
	}, nil
}

// loadDeploymentRoute retrieves a route using its complete ownership scope.
// Authorization must be performed by the caller before returning it to a client.
func loadDeploymentRoute(
	ctx context.Context,
	pool *pgxpool.Pool,
	projectID string,
	repositoryID string,
	deploymentID string,
) (deploymentRouteRecord, error) {
	if ctx == nil {
		return deploymentRouteRecord{}, errors.New(
			"deployment route context is required",
		)
	}

	if pool == nil {
		return deploymentRouteRecord{}, errors.New(
			"deployment route database pool is required",
		)
	}

	projectID, err := normalizeOperationUUID(projectID)
	if err != nil {
		return deploymentRouteRecord{}, fmt.Errorf(
			"projectId: %w",
			err,
		)
	}

	repositoryID, err = normalizeOperationUUID(repositoryID)
	if err != nil {
		return deploymentRouteRecord{}, fmt.Errorf(
			"repositoryId: %w",
			err,
		)
	}

	deploymentID, err = normalizeOperationUUID(deploymentID)
	if err != nil {
		return deploymentRouteRecord{}, fmt.Errorf(
			"deploymentId: %w",
			err,
		)
	}

	projectUUID, err := operationUUIDValue(projectID)
	if err != nil {
		return deploymentRouteRecord{}, fmt.Errorf(
			"projectId: %w",
			err,
		)
	}

	repositoryUUID, err := operationUUIDValue(repositoryID)
	if err != nil {
		return deploymentRouteRecord{}, fmt.Errorf(
			"repositoryId: %w",
			err,
		)
	}

	deploymentUUID, err := operationUUIDValue(deploymentID)
	if err != nil {
		return deploymentRouteRecord{}, fmt.Errorf(
			"deploymentId: %w",
			err,
		)
	}

	record, err := scanDeploymentRouteRecord(pool.QueryRow(
		ctx,
		`
			SELECT `+deploymentRouteRecordColumns+`
			FROM deployment_routes
			WHERE project_id = $1::uuid
			  AND repository_id = $2::uuid
			  AND deployment_id = $3::uuid
		`,
		projectUUID,
		repositoryUUID,
		deploymentUUID,
	))

	if errors.Is(err, pgx.ErrNoRows) {
		return deploymentRouteRecord{}, errDeploymentRouteNotFound
	}

	if err != nil {
		return deploymentRouteRecord{}, fmt.Errorf(
			"load deployment route: %w",
			err,
		)
	}

	return record, nil
}

func scanDeploymentRouteRecord(
	row pgx.Row,
) (deploymentRouteRecord, error) {
	var (
		record             deploymentRouteRecord
		observedGeneration pgtype.Int8
		leaseOwner         pgtype.Text
		leaseExpiresAt     pgtype.Timestamptz
		lastErrorCode      pgtype.Text
		lastReconciledAt   pgtype.Timestamptz
	)

	err := row.Scan(
		&record.ID,
		&record.ProjectID,
		&record.RepositoryID,
		&record.DeploymentID,
		&record.Hostname,
		&record.DesiredState,
		&record.ObservedState,
		&record.DesiredGeneration,
		&observedGeneration,
		&record.ReconcileAttemptCount,
		&record.NextReconcileAt,
		&leaseOwner,
		&leaseExpiresAt,
		&lastErrorCode,
		&lastReconciledAt,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err != nil {
		return deploymentRouteRecord{}, err
	}

	if observedGeneration.Valid {
		value := observedGeneration.Int64
		record.ObservedGeneration = &value
	}

	if leaseOwner.Valid {
		value := leaseOwner.String
		record.LeaseOwner = &value
	}

	if leaseExpiresAt.Valid {
		value := leaseExpiresAt.Time
		record.LeaseExpiresAt = &value
	}

	if lastErrorCode.Valid {
		value := lastErrorCode.String
		record.LastErrorCode = &value
	}

	if lastReconciledAt.Valid {
		value := lastReconciledAt.Time
		record.LastReconciledAt = &value
	}

	return record, nil
}

// deploymentRouteHostname allocates a stable hostname beneath an
// operator-controlled DNS suffix. The complete deployment UUID is used
// to avoid short-prefix collisions between deployments.
func deploymentRouteHostname(
	deploymentID string,
	previewSuffix string,
) (string, error) {
	normalizedID, err := normalizeOperationUUID(deploymentID)
	if err != nil {
		return "", errors.New(
			"deployment ID is invalid",
		)
	}

	suffix, err := normalizeDeploymentRoutePreviewSuffix(
		previewSuffix,
	)
	if err != nil {
		return "", err
	}

	deploymentToken := strings.ReplaceAll(
		normalizedID,
		"-",
		"",
	)

	hostname := "d-" + deploymentToken + "." + suffix

	if !validDeploymentRouteDNSName(hostname) {
		return "", errors.New(
			"allocated deployment hostname is not a valid DNS name",
		)
	}

	return hostname, nil
}

func normalizeDeploymentRoutePreviewSuffix(
	value string,
) (string, error) {
	if value == "" ||
		strings.TrimSpace(value) != value ||
		!utf8.ValidString(value) ||
		strings.IndexByte(value, 0) >= 0 {
		return "", errors.New(
			"preview suffix must be a non-empty DNS name without surrounding whitespace",
		)
	}

	normalized := strings.ToLower(value)

	// The generated deployment label is 34 bytes, followed by a dot.
	// Keep the complete hostname within the DNS 253-byte limit.
	if len(normalized) > 218 ||
		!validDeploymentRouteDNSName(normalized) {
		return "", errors.New(
			"preview suffix must be a valid ASCII DNS name that fits the hostname length limit",
		)
	}

	return normalized, nil
}

func validDeploymentRouteDNSName(value string) bool {
	if value == "" || len(value) > 253 {
		return false
	}

	labels := strings.Split(value, ".")

	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 {
			return false
		}

		if !isDeploymentRouteDNSAlphanumeric(label[0]) ||
			!isDeploymentRouteDNSAlphanumeric(label[len(label)-1]) {
			return false
		}

		for i := 0; i < len(label); i++ {
			character := label[i]

			if !isDeploymentRouteDNSAlphanumeric(character) &&
				character != '-' {
				return false
			}
		}
	}

	return true
}

func isDeploymentRouteDNSAlphanumeric(value byte) bool {
	return (value >= 'a' && value <= 'z') ||
		(value >= '0' && value <= '9')
}
