package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLoadDeploymentRouteReconcilePlanIntegrationEnsuresEligibleDeployment(
	t *testing.T,
) {
	fixture := newDeploymentRouteReconcileIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := fixture.prepareDeploymentReady(ctx, fixture.deploymentID); err != nil {
		t.Fatalf("prepare deployment for route activation: %v", err)
	}

	if _, err := fixture.createRoute(ctx, "preview.example.test"); err != nil {
		t.Fatalf("create deployment route: %v", err)
	}

	claims, err := claimDeploymentRoutes(
		ctx,
		fixture.pool,
		"route-controller-a",
		1,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim deployment route: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("claimed route count = %d, want 1", len(claims))
	}

	claim := claims[0]

	plan, err := loadDeploymentRouteReconcilePlan(
		ctx,
		fixture.pool,
		claim,
	)
	if err != nil {
		t.Fatalf("load deployment route reconciliation plan: %v", err)
	}

	if plan.Action != deploymentRouteReconcileEnsure {
		t.Errorf(
			"reconciliation action = %q, want %q",
			plan.Action,
			deploymentRouteReconcileEnsure,
		)
	}
	if plan.UpstreamAddress != "127.0.0.1:31001" {
		t.Errorf(
			"plan upstream = %q, want %q",
			plan.UpstreamAddress,
			"127.0.0.1:31001",
		)
	}
	if plan.Route.ID != claim.Route.ID {
		t.Errorf(
			"plan route ID = %q, want %q",
			plan.Route.ID,
			claim.Route.ID,
		)
	}
	if plan.Route.DesiredState != "active" {
		t.Errorf(
			"plan desired state = %q, want active",
			plan.Route.DesiredState,
		)
	}
	if plan.Route.DesiredGeneration != claim.Route.DesiredGeneration {
		t.Errorf(
			"plan generation = %d, want %d",
			plan.Route.DesiredGeneration,
			claim.Route.DesiredGeneration,
		)
	}

	storedRoute, err := loadDeploymentRoute(
		ctx,
		fixture.pool,
		fixture.projectID,
		fixture.repositoryID,
		fixture.deploymentID,
	)
	if err != nil {
		t.Fatalf("load route after preflight: %v", err)
	}
	if storedRoute.LeaseOwner == nil ||
		*storedRoute.LeaseOwner != claim.LeaseOwner {
		t.Error("preflight unexpectedly changed or released the route lease")
	}

	// The preflight must return only after releasing both database row
	// locks. A new transaction must be able to lock both rows immediately.
	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock-release verification transaction: %v", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	var lockedDeploymentID string
	if err := tx.QueryRow(ctx, `
		SELECT id::text
		FROM deployments
		WHERE id = $1::uuid
		FOR UPDATE NOWAIT
	`, fixture.deploymentID).Scan(&lockedDeploymentID); err != nil {
		t.Fatalf("lock deployment after preflight returned: %v", err)
	}

	var lockedRouteID string
	if err := tx.QueryRow(ctx, `
		SELECT id::text
		FROM deployment_routes
		WHERE id = $1::uuid
		FOR UPDATE NOWAIT
	`, claim.Route.ID).Scan(&lockedRouteID); err != nil {
		t.Fatalf("lock route after preflight returned: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit lock-release verification transaction: %v", err)
	}
}

func TestLoadDeploymentRouteReconcilePlanIntegrationRemovesIneligibleDeployment(
	t *testing.T,
) {
	fixture := newDeploymentRouteReconcileIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if _, err := fixture.createRoute(ctx, "preview.example.test"); err != nil {
		t.Fatalf("create deployment route: %v", err)
	}

	claims, err := claimDeploymentRoutes(
		ctx,
		fixture.pool,
		"route-controller-a",
		1,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim deployment route: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("claimed route count = %d, want 1", len(claims))
	}

	plan, err := loadDeploymentRouteReconcilePlan(
		ctx,
		fixture.pool,
		claims[0],
	)
	if err != nil {
		t.Fatalf("load deployment route reconciliation plan: %v", err)
	}

	if plan.Action != deploymentRouteReconcileRemove {
		t.Errorf(
			"reconciliation action = %q, want %q",
			plan.Action,
			deploymentRouteReconcileRemove,
		)
	}
	if plan.UpstreamAddress != "" {
		t.Errorf(
			"removal plan upstream = %q, want empty",
			plan.UpstreamAddress,
		)
	}
}

func TestLoadDeploymentRouteReconcilePlanIntegrationRemovesInactiveRoute(
	t *testing.T,
) {
	fixture := newDeploymentRouteReconcileIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := fixture.prepareDeploymentReady(ctx, fixture.deploymentID); err != nil {
		t.Fatalf("prepare deployment: %v", err)
	}

	created, err := fixture.createRoute(ctx, "preview.example.test")
	if err != nil {
		t.Fatalf("create deployment route: %v", err)
	}

	if _, err := fixture.pool.Exec(ctx, `
		UPDATE deployment_routes
		SET desired_state = 'inactive'
		WHERE id = $1::uuid
	`, created.Route.ID); err != nil {
		t.Fatalf("mark deployment route inactive: %v", err)
	}

	claims, err := claimDeploymentRoutes(
		ctx,
		fixture.pool,
		"route-controller-a",
		1,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim inactive deployment route: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("claimed route count = %d, want 1", len(claims))
	}

	plan, err := loadDeploymentRouteReconcilePlan(
		ctx,
		fixture.pool,
		claims[0],
	)
	if err != nil {
		t.Fatalf("load inactive route reconciliation plan: %v", err)
	}

	if plan.Action != deploymentRouteReconcileRemove {
		t.Errorf(
			"reconciliation action = %q, want %q",
			plan.Action,
			deploymentRouteReconcileRemove,
		)
	}
	if plan.Route.DesiredState != "inactive" {
		t.Errorf(
			"plan desired state = %q, want inactive",
			plan.Route.DesiredState,
		)
	}
}

func TestLoadDeploymentRouteReconcilePlanIntegrationRejectsStaleGeneration(
	t *testing.T,
) {
	fixture := newDeploymentRouteReconcileIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if _, err := fixture.createRoute(ctx, "preview.example.test"); err != nil {
		t.Fatalf("create deployment route: %v", err)
	}

	claims, err := claimDeploymentRoutes(
		ctx,
		fixture.pool,
		"route-controller-a",
		1,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim deployment route: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("claimed route count = %d, want 1", len(claims))
	}

	if _, err := fixture.pool.Exec(ctx, `
		UPDATE deployments
		SET desired_state = 'inactive'
		WHERE id = $1::uuid
	`, fixture.deploymentID); err != nil {
		t.Fatalf("change deployment desired state: %v", err)
	}

	_, err = loadDeploymentRouteReconcilePlan(
		ctx,
		fixture.pool,
		claims[0],
	)
	if !errors.Is(err, errDeploymentRouteClaimStale) {
		t.Fatalf(
			"preflight error = %v, want stale deployment route claim",
			err,
		)
	}
}

func TestLoadDeploymentRouteReconcilePlanIntegrationRejectsChangedDeploymentSnapshot(
	t *testing.T,
) {
	fixture := newDeploymentRouteReconcileIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := fixture.prepareDeploymentReady(ctx, fixture.deploymentID); err != nil {
		t.Fatalf("prepare deployment: %v", err)
	}

	if _, err := fixture.createRoute(ctx, "preview.example.test"); err != nil {
		t.Fatalf("create deployment route: %v", err)
	}

	claims, err := claimDeploymentRoutes(
		ctx,
		fixture.pool,
		"route-controller-a",
		1,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim deployment route: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("claimed route count = %d, want 1", len(claims))
	}

	claim := claims[0]
	changedUpstream := "127.0.0.1:31002"
	claim.Deployment.UpstreamAddress = &changedUpstream

	_, err = loadDeploymentRouteReconcilePlan(
		ctx,
		fixture.pool,
		claim,
	)
	if !errors.Is(err, errDeploymentRouteClaimStale) {
		t.Fatalf(
			"preflight error = %v, want stale deployment snapshot",
			err,
		)
	}
}

func TestLoadDeploymentRouteReconcilePlanIntegrationRejectsInvalidLeaseAndIdentity(
	t *testing.T,
) {
	fixture := newDeploymentRouteReconcileIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	created, err := fixture.createRoute(ctx, "preview.example.test")
	if err != nil {
		t.Fatalf("create deployment route: %v", err)
	}

	claims, err := claimDeploymentRoutes(
		ctx,
		fixture.pool,
		"route-controller-a",
		1,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim deployment route: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("claimed route count = %d, want 1", len(claims))
	}

	claim := claims[0]

	wrongIdentityClaim := claim
	wrongIdentityClaim.Route.Hostname = "other.preview.example.test"

	if _, err := loadDeploymentRouteReconcilePlan(
		ctx,
		fixture.pool,
		wrongIdentityClaim,
	); err == nil {
		t.Fatal("preflight accepted a claim with a mismatched route hostname")
	}

	wrongOwnerClaim := claim
	wrongOwnerClaim.LeaseOwner =
		"route-controller-b:00000000-0000-4000-8000-000000000000"

	_, err = loadDeploymentRouteReconcilePlan(
		ctx,
		fixture.pool,
		wrongOwnerClaim,
	)
	if !errors.Is(err, errDeploymentRouteLeaseNotOwned) {
		t.Fatalf(
			"preflight error with wrong lease owner = %v, want lease-not-owned",
			err,
		)
	}

	if _, err := fixture.pool.Exec(ctx, `
		UPDATE deployment_routes
		SET lease_expires_at = clock_timestamp() - interval '1 second'
		WHERE id = $1::uuid
	`, created.Route.ID); err != nil {
		t.Fatalf("expire deployment route lease: %v", err)
	}

	_, err = loadDeploymentRouteReconcilePlan(
		ctx,
		fixture.pool,
		claim,
	)
	if !errors.Is(err, errDeploymentRouteLeaseNotOwned) {
		t.Fatalf(
			"preflight error with expired lease = %v, want lease-not-owned",
			err,
		)
	}
}
