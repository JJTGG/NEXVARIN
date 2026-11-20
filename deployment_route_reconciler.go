package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	deploymentRouteReconcileMaxMutationAttempts = 3
	deploymentRouteReconcileLeaseDuration       = 2 * time.Minute
	deploymentRouteReconcileRetryDelay          = 5 * time.Second
	deploymentRouteReconcileRecoveryTimeout    = 2 * time.Second
)

type deploymentRouteReconcileAdapter interface {
	EnsureRoute(
		ctx context.Context,
		expected caddyRouteConfig,
	) (caddyRouteMutationResult, error)

	RemoveRoute(
		ctx context.Context,
		routeID string,
		hostname string,
	) (caddyRouteMutationResult, error)

	VerifyRouteActive(
		ctx context.Context,
		expected caddyRouteConfig,
	) error

	VerifyRouteRemoved(
		ctx context.Context,
		routeID string,
		hostname string,
	) error
}

type deploymentRouteReconciler struct {
	Pool          *pgxpool.Pool
	Adapter       deploymentRouteReconcileAdapter
	PreviewSuffix string
}

// reconcileClaim processes one previously claimed deployment route.
//
// Every mutation attempt starts with a fresh database preflight. PostgreSQL
// locks are released before lease renewal and all Caddy Admin API calls.
// A route observation is persisted only after active-configuration read-back
// confirms the exact desired state.
//
// ETag conflicts and uncertain mutation outcomes are resolved through
// read-back and, when necessary, a fresh preflight. No reconciliation pass
// makes more than three mutation attempts.
func (reconciler deploymentRouteReconciler) reconcileClaim(
	ctx context.Context,
	claim claimedDeploymentRoute,
) error {
	if ctx == nil {
		return errors.New("deployment route reconciler context is required")
	}
	if reconciler.Pool == nil {
		return errors.New("deployment route reconciler database pool is required")
	}
	if reconciler.Adapter == nil {
		return errors.New("deployment route reconciliation adapter is required")
	}

	for attempt := 1; attempt <= deploymentRouteReconcileMaxMutationAttempts; attempt++ {
		plan, err := loadDeploymentRouteReconcilePlan(
			ctx,
			reconciler.Pool,
			claim,
		)
		if errors.Is(err, errDeploymentRouteLeaseNotOwned) {
			// Another controller owns the route now, or this lease expired.
			// The old claim must not perform external work.
			return nil
		}
		if errors.Is(err, errDeploymentRouteClaimStale) {
			return reconciler.scheduleRetry(
				ctx,
				claim,
				0,
				"stale_generation",
			)
		}
		if err != nil {
			return fmt.Errorf(
				"revalidate deployment route claim: %w",
				err,
			)
		}

		expected, observedState, err := deploymentRouteExpectedState(
			plan,
			reconciler.PreviewSuffix,
		)
		if err != nil {
			return reconciler.scheduleRetry(
				ctx,
				claim,
				deploymentRouteReconcileRetryDelay,
				deploymentRouteCaddyErrorCode(err),
			)
		}

		// Renew after the database preflight and before any Caddy request.
		// The renewal transaction commits before external communication.
		_, err = renewDeploymentRouteLease(
			ctx,
			reconciler.Pool,
			plan.Route.ID,
			claim.LeaseOwner,
			deploymentRouteReconcileLeaseDuration,
		)
		if errors.Is(err, errDeploymentRouteLeaseNotOwned) {
			return nil
		}
		if err != nil {
			return fmt.Errorf(
				"renew deployment route lease before Caddy operation: %w",
				err,
			)
		}

		var mutationErr error

		switch plan.Action {
		case deploymentRouteReconcileEnsure:
			_, mutationErr = reconciler.Adapter.EnsureRoute(
				ctx,
				expected,
			)

		case deploymentRouteReconcileRemove:
			_, mutationErr = reconciler.Adapter.RemoveRoute(
				ctx,
				expected.ID,
				expected.Hostname,
			)

		default:
			return reconciler.scheduleRetry(
				ctx,
				claim,
				deploymentRouteReconcileRetryDelay,
				"caddy_route_invalid",
			)
		}

		// A transport failure or server failure during a mutation may mean
		// Caddy applied the change even though the client did not receive a
		// definitive response. A 412 means the snapshot was stale. Both
		// cases require read-back before another mutation is considered.
		if mutationErr != nil &&
			!errors.Is(mutationErr, errCaddyAdminOutcomeUnknown) &&
			!errors.Is(mutationErr, errCaddyRouteMutationPreconditionFailed) {
			return reconciler.scheduleRetry(
				ctx,
				claim,
				deploymentRouteReconcileRetryDelay,
				deploymentRouteCaddyErrorCode(mutationErr),
			)
		}

		var verificationErr error

		switch observedState {
		case "active":
			verificationErr = reconciler.Adapter.VerifyRouteActive(
				ctx,
				expected,
			)

		case "removed":
			verificationErr = reconciler.Adapter.VerifyRouteRemoved(
				ctx,
				expected.ID,
				expected.Hostname,
			)

		default:
			return reconciler.scheduleRetry(
				ctx,
				claim,
				deploymentRouteReconcileRetryDelay,
				"caddy_route_invalid",
			)
		}

		if verificationErr == nil {
			_, err := recordDeploymentRouteReconciliationSuccess(
				ctx,
				reconciler.Pool,
				claim,
				observedState,
			)
			if errors.Is(err, errDeploymentRouteLeaseNotOwned) {
				// The external state was verified, but this claim can no
				// longer publish the observation. A current owner will
				// re-read and reconcile the route.
				return nil
			}
			if err != nil {
				retryErr := reconciler.scheduleRetry(
					ctx,
					claim,
					deploymentRouteReconcileRetryDelay,
					"outcome_persist_failed",
				)
				if retryErr != nil {
					return errors.Join(
						fmt.Errorf(
							"record verified deployment route observation: %w",
							err,
						),
						retryErr,
					)
				}

				return fmt.Errorf(
					"record verified deployment route observation: %w",
					err,
				)
			}

			return nil
		}

		if errors.Is(
			verificationErr,
			errCaddyRouteVerificationMismatch,
		) && attempt < deploymentRouteReconcileMaxMutationAttempts {
			// The read succeeded and established that the desired state is
			// not present. The next pass will revalidate PostgreSQL and
			// obtain a new Caddy snapshot before recomputing the mutation.
			continue
		}

		errorCode := deploymentRouteCaddyErrorCode(verificationErr)
		if errors.Is(
			verificationErr,
			errCaddyRouteVerificationMismatch,
		) && errors.Is(
			mutationErr,
			errCaddyRouteMutationPreconditionFailed,
		) {
			errorCode = "caddy_config_conflict"
		}

		return reconciler.scheduleRetry(
			ctx,
			claim,
			deploymentRouteReconcileRetryDelay,
			errorCode,
		)
	}

	return reconciler.scheduleRetry(
		ctx,
		claim,
		deploymentRouteReconcileRetryDelay,
		"caddy_route_invalid",
	)
}

func deploymentRouteExpectedState(
	plan deploymentRouteReconcilePlan,
	previewSuffix string,
) (caddyRouteConfig, string, error) {
	switch plan.Action {
	case deploymentRouteReconcileEnsure:
		expected, err := newCaddyRouteConfig(
			plan.Route,
			plan.UpstreamAddress,
			previewSuffix,
		)
		if err != nil {
			return caddyRouteConfig{}, "", err
		}

		return expected, "active", nil

	case deploymentRouteReconcileRemove:
		routeUUID, err := normalizeOperationUUID(plan.Route.ID)
		if err != nil || routeUUID != plan.Route.ID {
			return caddyRouteConfig{}, "", fmt.Errorf(
				"%w: deployment route UUID is invalid or noncanonical",
				errCaddyRouteConfigInvalid,
			)
		}

		if plan.Route.Hostname == "" {
			return caddyRouteConfig{}, "", fmt.Errorf(
				"%w: deployment route hostname is required",
				errCaddyRouteConfigInvalid,
			)
		}

		return caddyRouteConfig{
			ID:       caddyRouteIDPrefix + routeUUID,
			Hostname: plan.Route.Hostname,
		}, "removed", nil

	default:
		return caddyRouteConfig{}, "", fmt.Errorf(
			"%w: unsupported deployment route reconciliation action %q",
			errCaddyRouteConfigInvalid,
			plan.Action,
		)
	}
}

func deploymentRouteCaddyErrorCode(err error) string {
	switch {
	case errors.Is(err, errCaddyRouteMutationPreconditionFailed):
		return "caddy_config_conflict"

	case errors.Is(err, errCaddyAdminOutcomeUnknown):
		return "caddy_outcome_unknown"

	case errors.Is(err, errCaddyAdminUnavailable):
		return "caddy_unavailable"

	case errors.Is(err, errCaddyRouteAnchorMissing):
		return "caddy_config_anchor_missing"

	case errors.Is(err, errCaddyRouteAnchorInvalid):
		return "caddy_config_anchor_invalid"

	case errors.Is(err, errCaddyRouteIdentityConflict):
		return "caddy_route_identity_conflict"

	case errors.Is(err, errCaddyRouteHostnameConflict):
		return "caddy_route_hostname_conflict"

	default:
		return "caddy_route_invalid"
	}
}

// scheduleRetry releases the current lease and schedules another durable
// reconciliation attempt. If the processing context has been cancelled,
// use a bounded recovery context so shutdown does not prevent lease release.
func (reconciler deploymentRouteReconciler) scheduleRetry(
	ctx context.Context,
	claim claimedDeploymentRoute,
	delay time.Duration,
	errorCode string,
) error {
	baseContext := ctx
	if ctx.Err() != nil {
		baseContext = context.Background()
	}

	recoveryContext, cancel := context.WithTimeout(
		baseContext,
		deploymentRouteReconcileRecoveryTimeout,
	)
	defer cancel()

	_, err := retryDeploymentRouteReconciliation(
		recoveryContext,
		reconciler.Pool,
		claim,
		delay,
		errorCode,
	)
	if errors.Is(err, errDeploymentRouteLeaseNotOwned) {
		return nil
	}
	if err != nil {
		return fmt.Errorf(
			"schedule deployment route reconciliation retry: %w",
			err,
		)
	}

	return nil
}
