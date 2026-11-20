package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

var errDeploymentRouteClaimStale = errors.New(
	"deployment route reconciliation claim is stale",
)

type deploymentRouteReconcileAction string

const (
	deploymentRouteReconcileEnsure deploymentRouteReconcileAction = "ensure"
	deploymentRouteReconcileRemove deploymentRouteReconcileAction = "remove"
)

type deploymentRouteReconcilePlan struct {
	Route           deploymentRouteRecord
	Action          deploymentRouteReconcileAction
	UpstreamAddress string
}

// loadDeploymentRouteReconcilePlan revalidates a claimed route against
// current database state before the controller performs any external Caddy
// operation.
//
// The deployment row is locked before the route row, matching the lock
// order used by recordDeploymentRouteReconciliationSuccess and deployment
// update triggers.
//
// All database locks are released before this function returns. Callers
// must never hold these locks across Caddy Admin API requests.
//
// An active route is eligible for installation only when:
//   - the route still has the claimed desired generation;
//   - the route still has the claimed desired state;
//   - the deployment is active, ready, and not deleted;
//   - the current deployment upstream is valid for both the runtime and
//     the Caddy ingress adapter;
//   - the deployment snapshot captured by the claim still matches.
//
// Ineligible deployments and inactive routes produce a removal plan.
func loadDeploymentRouteReconcilePlan(
	ctx context.Context,
	pool *pgxpool.Pool,
	claim claimedDeploymentRoute,
) (deploymentRouteReconcilePlan, error) {
	var empty deploymentRouteReconcilePlan

	if ctx == nil {
		return empty, errors.New(
			"deployment route reconciliation context is required",
		)
	}

	if pool == nil {
		return empty, errors.New(
			"deployment route reconciliation database pool is required",
		)
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

	tx, err := pool.Begin(ctx)
	if err != nil {
		return empty, fmt.Errorf(
			"begin deployment route reconciliation preflight: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	// Preserve the established lock order:
	// deployments first, deployment_routes second.
	deploymentState, err := lockDeploymentRouteDeployment(
		ctx,
		tx,
		claim,
	)
	if err != nil {
		return empty, fmt.Errorf(
			"load current deployment eligibility: %w",
			err,
		)
	}

	leaseState, err := lockDeploymentRouteLease(
		ctx,
		tx,
		routeUUID,
		claim.LeaseOwner,
	)
	if err != nil {
		return empty, fmt.Errorf(
			"revalidate deployment route lease: %w",
			err,
		)
	}

	if leaseState.ProjectID != claim.Route.ProjectID ||
		leaseState.RepositoryID != claim.Route.RepositoryID ||
		leaseState.DeploymentID != claim.Route.DeploymentID ||
		leaseState.Hostname != claim.Route.Hostname {
		return empty, errors.New(
			"claimed deployment route identity does not match the stored route",
		)
	}

	if claimedGeneration > leaseState.DesiredGeneration {
		return empty, errors.New(
			"claimed deployment route generation exceeds the current generation",
		)
	}

	if claimedGeneration < leaseState.DesiredGeneration {
		return empty, fmt.Errorf(
			"%w: claimed generation %d, current generation %d",
			errDeploymentRouteClaimStale,
			claimedGeneration,
			leaseState.DesiredGeneration,
		)
	}

	if claim.Route.DesiredState != leaseState.DesiredState {
		return empty, fmt.Errorf(
			"%w: claimed desired state does not match the current route state",
			errDeploymentRouteClaimStale,
		)
	}

	if leaseState.DesiredState != "active" &&
		leaseState.DesiredState != "inactive" {
		return empty, fmt.Errorf(
			"deployment route has an unsupported desired state %q",
			leaseState.DesiredState,
		)
	}

	deploymentEligible :=
		deploymentState.DesiredState == "active" &&
			deploymentState.ObservedState == "ready" &&
			!deploymentState.DeletedAt.Valid &&
			deploymentState.UpstreamAddress.Valid &&
			validOperationWorkerDeployUpstreamAddress(
				deploymentState.UpstreamAddress.String,
			) &&
			validCaddyRouteUpstreamAddress(
				deploymentState.UpstreamAddress.String,
			)

	plan := deploymentRouteReconcilePlan{
		Route: claim.Route,
	}

	plan.Route.DesiredState = leaseState.DesiredState
	plan.Route.DesiredGeneration = leaseState.DesiredGeneration

	if leaseState.DesiredState == "active" && deploymentEligible {
		// The success recorder compares the original claim snapshot with
		// the current deployment. Reject a changed snapshot before making
		// an external mutation that could not subsequently be recorded as
		// convergence for this claim.
		if claim.Deployment.DesiredState != deploymentState.DesiredState ||
			claim.Deployment.ObservedState != deploymentState.ObservedState ||
			claim.Deployment.DeletedAt != nil ||
			claim.Deployment.UpstreamAddress == nil ||
			*claim.Deployment.UpstreamAddress != deploymentState.UpstreamAddress.String {
			return empty, fmt.Errorf(
				"%w: deployment eligibility or upstream changed after the route was claimed",
				errDeploymentRouteClaimStale,
			)
		}

		plan.Action = deploymentRouteReconcileEnsure
		plan.UpstreamAddress = deploymentState.UpstreamAddress.String
	} else {
		plan.Action = deploymentRouteReconcileRemove
	}

	// This transaction performs no writes. Committing here releases both
	// locks before the caller begins external reconciliation.
	if err := tx.Commit(ctx); err != nil {
		return empty, fmt.Errorf(
			"commit deployment route reconciliation preflight: %w",
			err,
		)
	}

	return plan, nil
}
