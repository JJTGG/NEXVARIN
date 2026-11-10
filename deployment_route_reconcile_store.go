
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	errDeploymentRouteLeaseNotOwned = errors.New(
		"deployment route lease is not owned by this controller",
	)
	errDeploymentRouteReconciliationOutcomeUnknown = errors.New(
		"deployment route reconciliation transaction outcome is unknown",
	)
)

type deploymentRouteDeploymentSnapshot struct {
	DesiredState    string
	ObservedState   string
	DeletedAt       *time.Time
	UpstreamAddress *string
}

type claimedDeploymentRoute struct {
	Route          deploymentRouteRecord
	LeaseOwner     string
	LeaseExpiresAt time.Time
	Deployment     deploymentRouteDeploymentSnapshot
}

type deploymentRouteLeaseState struct {
	DesiredGeneration  int64
	ObservedGeneration pgtype.Int8
}

const deploymentRouteClaimColumns = `
	route.id::text,
	route.project_id::text,
	route.repository_id::text,
	route.deployment_id::text,
	route.hostname,
	route.desired_state,
	route.observed_state,
	route.desired_generation,
	route.observed_generation,
	route.reconcile_attempt_count,
	route.next_reconcile_at,
	route.lease_owner,
	route.lease_expires_at,
	route.last_error_code,
	route.last_reconciled_at,
	route.created_at,
	route.updated_at,
	deployment.desired_state,
	deployment.observed_state,
	deployment.deleted_at,
	deployment.upstream_address
`

// claimDeploymentRoutes atomically leases due route records to a controller.
//
// PostgreSQL row locking allows multiple controllers to claim separate routes
// without claiming the same row in the same lease window. Each claim receives
// a unique token; callers must retain that exact token and the claimed desired
// generation for subsequent lease-protected operations.
//
// The deployment fields are a point-in-time snapshot for reconciliation
// planning. They are not authorization to install a route: the controller
// must re-check current deployment eligibility before changing Caddy.
func claimDeploymentRoutes(
	ctx context.Context,
	pool *pgxpool.Pool,
	workerID string,
	limit int,
	leaseDuration time.Duration,
) ([]claimedDeploymentRoute, error) {
	if ctx == nil {
		return nil, errors.New("deployment route claim context is required")
	}
	if pool == nil {
		return nil, errors.New("database pool is required")
	}

	workerID, err := normalizeOperationWorkerID(workerID)
	if err != nil {
		return nil, fmt.Errorf("worker ID: %w", err)
	}
	if limit < 1 || limit > 1000 {
		return nil, errors.New("deployment route claim limit must be between 1 and 1000")
	}
	if leaseDuration < time.Microsecond {
		return nil, errors.New("deployment route lease must be at least one microsecond")
	}

	leaseMicroseconds := durationMicroseconds(leaseDuration)

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin deployment route claim: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	rows, err := tx.Query(ctx, `
		WITH candidates AS (
			SELECT route.id
			FROM deployment_routes AS route
			WHERE route.next_reconcile_at <= clock_timestamp()
			  AND (
				  route.observed_generation
				      IS DISTINCT FROM route.desired_generation
				  OR route.observed_state IN (
					  'pending',
					  'removing',
					  'needs_reconciliation'
				  )
			  )
			  AND (
				  route.lease_owner IS NULL
				  OR route.lease_expires_at <= clock_timestamp()
			  )
			ORDER BY
				route.next_reconcile_at,
				route.created_at,
				route.id
			LIMIT $1
			FOR UPDATE OF route SKIP LOCKED
		)
		UPDATE deployment_routes AS route
		SET
			lease_owner = $2 || ':' || gen_random_uuid()::text,
			lease_expires_at = clock_timestamp()
				+ ($3::double precision * interval '1 microsecond'),
			reconcile_attempt_count =
				route.reconcile_attempt_count + 1
		FROM candidates, deployments AS deployment
		WHERE route.id = candidates.id
		  AND deployment.id = route.deployment_id
		  AND deployment.project_id = route.project_id
		  AND deployment.repository_id = route.repository_id
		RETURNING `+deploymentRouteClaimColumns,
		limit,
		workerID,
		leaseMicroseconds,
	)
	if err != nil {
		return nil, fmt.Errorf("claim due deployment routes: %w", err)
	}

	claims := make([]claimedDeploymentRoute, 0, limit)
	for rows.Next() {
		claim, err := scanClaimedDeploymentRoute(rows)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan claimed deployment route: %w", err)
		}
		claims = append(claims, claim)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate claimed deployment routes: %w", err)
	}
	rows.Close()

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf(
			"%w: %v",
			errDeploymentRouteReconciliationOutcomeUnknown,
			err,
		)
	}

	return claims, nil
}

// renewDeploymentRouteLease extends an unexpired lease owned by the supplied
// controller token. The route row is locked before checking expiry so a lock
// wait cannot make an earlier expiry check stale.
func renewDeploymentRouteLease(
	ctx context.Context,
	pool *pgxpool.Pool,
	routeID string,
	leaseOwner string,
	leaseDuration time.Duration,
) (time.Time, error) {
	var empty time.Time

	if ctx == nil {
		return empty, errors.New("deployment route lease context is required")
	}
	if pool == nil {
		return empty, errors.New("database pool is required")
	}

	routeUUID, err := normalizeDeploymentRouteLeaseInput(routeID, leaseOwner)
	if err != nil {
		return empty, err
	}
	if leaseDuration < time.Microsecond {
		return empty, errors.New("deployment route lease must be at least one microsecond")
	}

	leaseMicroseconds := durationMicroseconds(leaseDuration)

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return empty, fmt.Errorf("begin deployment route lease renewal: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	if _, err := lockDeploymentRouteLease(ctx, tx, routeUUID, leaseOwner); err != nil {
		return empty, err
	}

	var renewedExpiry time.Time
	err = tx.QueryRow(ctx, `
		UPDATE deployment_routes
		SET lease_expires_at = GREATEST(
			lease_expires_at,
			clock_timestamp()
				+ ($3::double precision * interval '1 microsecond')
		)
		WHERE id = $1::uuid
		  AND lease_owner = $2
		  AND lease_expires_at > clock_timestamp()
		RETURNING lease_expires_at
	`, routeUUID, leaseOwner, leaseMicroseconds).Scan(&renewedExpiry)
	if errors.Is(err, pgx.ErrNoRows) {
		return empty, errDeploymentRouteLeaseNotOwned
	}
	if err != nil {
		return empty, fmt.Errorf("renew deployment route lease: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return empty, fmt.Errorf(
			"%w: %v",
			errDeploymentRouteReconciliationOutcomeUnknown,
			err,
		)
	}

	return renewedExpiry, nil
}

// recordDeploymentRouteReconciliationSuccess records a confirmed external
// observation for the generation captured by the original claim. If the
// desired generation advanced while reconciliation was running, this records
// the older observation without marking the newer generation as reconciled.
func recordDeploymentRouteReconciliationSuccess(
	ctx context.Context,
	pool *pgxpool.Pool,
	claim claimedDeploymentRoute,
	observedState string,
) (deploymentRouteRecord, error) {
	if ctx == nil {
		return deploymentRouteRecord{}, errors.New(
			"deployment route reconciliation context is required",
		)
	}
	if pool == nil {
		return deploymentRouteRecord{}, errors.New("database pool is required")
	}
	if observedState != "active" && observedState != "removed" {
		return deploymentRouteRecord{}, errors.New(
			"successful deployment route observation must be active or removed",
		)
	}

	routeUUID, err := normalizeDeploymentRouteLeaseInput(
		claim.Route.ID,
		claim.LeaseOwner,
	)
	if err != nil {
		return deploymentRouteRecord{}, err
	}
	claimedGeneration := claim.Route.DesiredGeneration
	if claimedGeneration < 1 {
		return deploymentRouteRecord{}, errors.New(
			"claimed deployment route generation must be positive",
		)
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return deploymentRouteRecord{}, fmt.Errorf(
			"begin deployment route reconciliation success: %w",
			err,
		)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	leaseState, err := lockDeploymentRouteLease(
		ctx,
		tx,
		routeUUID,
		claim.LeaseOwner,
	)
	if err != nil {
		return deploymentRouteRecord{}, err
	}
	if claimedGeneration > leaseState.DesiredGeneration {
		return deploymentRouteRecord{}, errors.New(
			"claimed generation exceeds the current desired generation",
		)
	}
	if leaseState.ObservedGeneration.Valid &&
		claimedGeneration < leaseState.ObservedGeneration.Int64 {
		return deploymentRouteRecord{}, errors.New(
			"claimed generation is older than the current observed generation",
		)
	}

	record, err := scanDeploymentRouteRecord(tx.QueryRow(ctx, `
		UPDATE deployment_routes
		SET
			observed_state = $3,
			observed_generation = $4,
			reconcile_attempt_count = 0,
			lease_owner = NULL,
			lease_expires_at = NULL,
			last_error_code = NULL,
			last_reconciled_at = clock_timestamp()
		WHERE id = $1::uuid
		  AND lease_owner = $2
		  AND lease_expires_at > clock_timestamp()
		RETURNING `+deploymentRouteRecordColumns,
		routeUUID,
		claim.LeaseOwner,
		observedState,
		claimedGeneration,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return deploymentRouteRecord{}, errDeploymentRouteLeaseNotOwned
	}
	if err != nil {
		return deploymentRouteRecord{}, fmt.Errorf(
			"record deployment route observation: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return deploymentRouteRecord{}, fmt.Errorf(
			"%w: %v",
			errDeploymentRouteReconciliationOutcomeUnknown,
			err,
		)
	}

	return record, nil
}

// retryDeploymentRouteReconciliation records a failed attempt, releases the
// lease, and schedules another attempt. A newer desired generation bypasses
// the old attempt's backoff because it represents newer work.
func retryDeploymentRouteReconciliation(
	ctx context.Context,
	pool *pgxpool.Pool,
	claim claimedDeploymentRoute,
	retryDelay time.Duration,
	errorCode string,
) (time.Time, error) {
	var empty time.Time

	if ctx == nil {
		return empty, errors.New(
			"deployment route reconciliation context is required",
		)
	}
	if pool == nil {
		return empty, errors.New("database pool is required")
	}
	if retryDelay < 0 {
		return empty, errors.New("deployment route retry delay cannot be negative")
	}
	if !validOperationErrorCode(errorCode) {
		return empty, errors.New("deployment route error code is invalid")
	}

	routeUUID, err := normalizeDeploymentRouteLeaseInput(
		claim.Route.ID,
		claim.LeaseOwner,
	)
	if err != nil {
		return empty, err
	}
	claimedGeneration := claim.Route.DesiredGeneration
	if claimedGeneration < 1 {
		return empty, errors.New(
			"claimed deployment route generation must be positive",
		)
	}

	retryMicroseconds := durationMicroseconds(retryDelay)

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return empty, fmt.Errorf(
			"begin deployment route retry scheduling: %w",
			err,
		)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	leaseState, err := lockDeploymentRouteLease(
		ctx,
		tx,
		routeUUID,
		claim.LeaseOwner,
	)
	if err != nil {
		return empty, err
	}
	if claimedGeneration > leaseState.DesiredGeneration {
		return empty, errors.New(
			"claimed generation exceeds the current desired generation",
		)
	}

	var nextReconcileAt time.Time
	err = tx.QueryRow(ctx, `
		UPDATE deployment_routes
		SET
			observed_state = 'needs_reconciliation',
			next_reconcile_at = clock_timestamp()
				+ (
					CASE
						WHEN desired_generation = $3
						THEN $4::double precision
						ELSE 0::double precision
					END
					* interval '1 microsecond'
				),
			lease_owner = NULL,
			lease_expires_at = NULL,
			last_error_code = $5
		WHERE id = $1::uuid
		  AND lease_owner = $2
		  AND lease_expires_at > clock_timestamp()
		RETURNING next_reconcile_at
	`,
		routeUUID,
		claim.LeaseOwner,
		claimedGeneration,
		retryMicroseconds,
		errorCode,
	).Scan(&nextReconcileAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return empty, errDeploymentRouteLeaseNotOwned
	}
	if err != nil {
		return empty, fmt.Errorf(
			"schedule deployment route retry: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return empty, fmt.Errorf(
			"%w: %v",
			errDeploymentRouteReconciliationOutcomeUnknown,
			err,
		)
	}

	return nextReconcileAt, nil
}

func lockDeploymentRouteLease(
	ctx context.Context,
	tx pgx.Tx,
	routeUUID pgtype.UUID,
	leaseOwner string,
) (deploymentRouteLeaseState, error) {
	var (
		state             deploymentRouteLeaseState
		storedLeaseOwner  pgtype.Text
		storedLeaseExpiry pgtype.Timestamptz
	)

	err := tx.QueryRow(ctx, `
		SELECT
			desired_generation,
			observed_generation,
			lease_owner,
			lease_expires_at
		FROM deployment_routes
		WHERE id = $1::uuid
		FOR UPDATE
	`, routeUUID).Scan(
		&state.DesiredGeneration,
		&state.ObservedGeneration,
		&storedLeaseOwner,
		&storedLeaseExpiry,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return deploymentRouteLeaseState{}, errDeploymentRouteNotFound
	}
	if err != nil {
		return deploymentRouteLeaseState{}, fmt.Errorf(
			"lock deployment route lease: %w",
			err,
		)
	}
	if !storedLeaseOwner.Valid ||
		storedLeaseOwner.String != leaseOwner ||
		!storedLeaseExpiry.Valid {
		return deploymentRouteLeaseState{}, errDeploymentRouteLeaseNotOwned
	}

	// Check expiry only after acquiring the row lock. now() is fixed at the
	// transaction start and could be stale after waiting for the lock.
	var leaseIsCurrent bool
	if err := tx.QueryRow(ctx, `
		SELECT lease_expires_at > clock_timestamp()
		FROM deployment_routes
		WHERE id = $1::uuid
	`, routeUUID).Scan(&leaseIsCurrent); err != nil {
		return deploymentRouteLeaseState{}, fmt.Errorf(
			"check deployment route lease expiry: %w",
			err,
		)
	}
	if !leaseIsCurrent {
		return deploymentRouteLeaseState{}, errDeploymentRouteLeaseNotOwned
	}

	return state, nil
}

func scanClaimedDeploymentRoute(
	row pgx.Row,
) (claimedDeploymentRoute, error) {
	var (
		claim              claimedDeploymentRoute
		observedGeneration pgtype.Int8
		leaseOwner         pgtype.Text
		leaseExpiresAt     pgtype.Timestamptz
		lastErrorCode      pgtype.Text
		lastReconciledAt   pgtype.Timestamptz
		deletedAt          pgtype.Timestamptz
		upstreamAddress    pgtype.Text
	)

	err := row.Scan(
		&claim.Route.ID,
		&claim.Route.ProjectID,
		&claim.Route.RepositoryID,
		&claim.Route.DeploymentID,
		&claim.Route.Hostname,
		&claim.Route.DesiredState,
		&claim.Route.ObservedState,
		&claim.Route.DesiredGeneration,
		&observedGeneration,
		&claim.Route.ReconcileAttemptCount,
		&claim.Route.NextReconcileAt,
		&leaseOwner,
		&leaseExpiresAt,
		&lastErrorCode,
		&lastReconciledAt,
		&claim.Route.CreatedAt,
		&claim.Route.UpdatedAt,
		&claim.Deployment.DesiredState,
		&claim.Deployment.ObservedState,
		&deletedAt,
		&upstreamAddress,
	)
	if err != nil {
		return claimedDeploymentRoute{}, err
	}
	if !leaseOwner.Valid || !leaseExpiresAt.Valid {
		return claimedDeploymentRoute{}, errors.New(
			"claimed deployment route has no lease token or expiry",
		)
	}

	claim.LeaseOwner = leaseOwner.String
	claim.LeaseExpiresAt = leaseExpiresAt.Time
	claim.Route.LeaseOwner = &claim.LeaseOwner
	claim.Route.LeaseExpiresAt = &claim.LeaseExpiresAt

	if observedGeneration.Valid {
		value := observedGeneration.Int64
		claim.Route.ObservedGeneration = &value
	}
	claim.Route.LastErrorCode = textPointer(lastErrorCode)
	claim.Route.LastReconciledAt = timestampPointer(lastReconciledAt)
	claim.Deployment.DeletedAt = timestampPointer(deletedAt)
	claim.Deployment.UpstreamAddress = textPointer(upstreamAddress)

	return claim, nil
}

func normalizeDeploymentRouteLeaseInput(
	routeID string,
	leaseOwner string,
) (pgtype.UUID, error) {
	normalizedID, err := normalizeOperationUUID(routeID)
	if err != nil {
		return pgtype.UUID{}, fmt.Errorf("deployment route ID: %w", err)
	}
	if _, err := operationWorkerIDFromLeaseOwner(leaseOwner); err != nil {
		return pgtype.UUID{}, errors.New(
			"deployment route lease owner token is invalid",
		)
	}

	routeUUID, err := operationUUIDValue(normalizedID)
	if err != nil {
		return pgtype.UUID{}, fmt.Errorf("deployment route ID: %w", err)
	}

	return routeUUID, nil
}
