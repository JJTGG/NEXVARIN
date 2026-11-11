package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type deploymentRouteReconcileIntegrationFixture struct {
	*deploymentRouteStoreIntegrationFixture
}

func newDeploymentRouteReconcileIntegrationFixture(
	t *testing.T,
) *deploymentRouteReconcileIntegrationFixture {
	t.Helper()

	return &deploymentRouteReconcileIntegrationFixture{
		deploymentRouteStoreIntegrationFixture: newDeploymentRouteStoreIntegrationFixture(t),
	}
}

func (f *deploymentRouteReconcileIntegrationFixture) createRouteForDeployment(
	ctx context.Context,
	deploymentID string,
) (createDeploymentRouteResult, error) {
	return createDeploymentRoute(
		ctx,
		f.pool,
		createDeploymentRouteInput{
			ProjectID:     f.projectID,
			RepositoryID:  f.repositoryID,
			DeploymentID:  deploymentID,
			PreviewSuffix: "preview.example.test",
		},
	)
}

func (f *deploymentRouteReconcileIntegrationFixture) createAdditionalDeployment(
	ctx context.Context,
) (string, error) {
	var deploymentID string
	err := f.pool.QueryRow(ctx, `
		INSERT INTO deployments (
			project_id,
			repository_id,
			created_by_subject
		)
		VALUES (
			$1::uuid,
			$2::uuid,
			$3
		)
		RETURNING id::text
	`, f.projectID, f.repositoryID, f.requestedBy).Scan(&deploymentID)
	if err != nil {
		return "", fmt.Errorf("create additional deployment: %w", err)
	}

	return deploymentID, nil
}

func TestClaimDeploymentRoutesIntegrationClaimsDueRouteAndExcludesActiveLease(
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
		10,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim due deployment route: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("claimed route count = %d, want 1", len(claims))
	}

	claim := claims[0]
	if claim.Route.DeploymentID != fixture.deploymentID {
		t.Errorf(
			"claimed deployment ID = %q, want %q",
			claim.Route.DeploymentID,
			fixture.deploymentID,
		)
	}
	if claim.Route.ObservedState != "pending" {
		t.Errorf("claimed route observed state = %q, want pending", claim.Route.ObservedState)
	}
	if claim.Route.DesiredGeneration != 1 {
		t.Errorf("claimed desired generation = %d, want 1", claim.Route.DesiredGeneration)
	}
	if claim.Route.ReconcileAttemptCount != 1 {
		t.Errorf("claim attempt count = %d, want 1", claim.Route.ReconcileAttemptCount)
	}
	if claim.LeaseOwner == "" ||
		claim.Route.LeaseOwner == nil ||
		*claim.Route.LeaseOwner != claim.LeaseOwner {
		t.Error("claim did not return a consistent lease owner token")
	}
	if !claim.LeaseExpiresAt.After(time.Now()) {
		t.Errorf("lease expiry %v is not in the future", claim.LeaseExpiresAt)
	}
	if claim.Deployment.DeletedAt != nil {
		t.Errorf("new deployment snapshot deleted_at = %v, want nil", claim.Deployment.DeletedAt)
	}
	if claim.Deployment.DesiredState != "active" {
		t.Errorf("deployment desired state = %q, want active", claim.Deployment.DesiredState)
	}
	if claim.Deployment.ObservedState != "pending" {
		t.Errorf("deployment observed state = %q, want pending", claim.Deployment.ObservedState)
	}
	if claim.Deployment.UpstreamAddress != nil {
		t.Errorf("deployment upstream address = %v, want nil", claim.Deployment.UpstreamAddress)
	}

	workerID, err := operationWorkerIDFromLeaseOwner(claim.LeaseOwner)
	if err != nil {
		t.Fatalf("validate returned lease token: %v", err)
	}
	if workerID != "route-controller-a" {
		t.Errorf("lease worker ID = %q, want route-controller-a", workerID)
	}

	secondClaims, err := claimDeploymentRoutes(
		ctx,
		fixture.pool,
		"route-controller-b",
		10,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim while route lease is active: %v", err)
	}
	if len(secondClaims) != 0 {
		t.Errorf("second controller claimed %d routes while the only route was leased", len(secondClaims))
	}
}

func TestClaimDeploymentRoutesIntegrationConcurrentControllersClaimDistinctRoutes(
	t *testing.T,
) {
	fixture := newDeploymentRouteReconcileIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	additionalDeploymentID, err := fixture.createAdditionalDeployment(ctx)
	if err != nil {
		t.Fatalf("create second deployment: %v", err)
	}

	if _, err := fixture.createRouteForDeployment(ctx, fixture.deploymentID); err != nil {
		t.Fatalf("create first route: %v", err)
	}
	if _, err := fixture.createRouteForDeployment(ctx, additionalDeploymentID); err != nil {
		t.Fatalf("create second route: %v", err)
	}

	type outcome struct {
		claims []claimedDeploymentRoute
		err    error
	}

	start := make(chan struct{})
	results := make(chan outcome, 2)
	for _, workerID := range []string{"route-controller-a", "route-controller-b"} {
		go func(workerID string) {
			<-start
			claims, err := claimDeploymentRoutes(
				ctx,
				fixture.pool,
				workerID,
				1,
				time.Minute,
			)
			results <- outcome{claims: claims, err: err}
		}(workerID)
	}
	close(start)

	claimedDeploymentIDs := make(map[string]struct{}, 2)
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent route claim: %v", result.err)
		}
		if len(result.claims) != 1 {
			t.Fatalf("concurrent controller claimed %d routes, want 1", len(result.claims))
		}
		deploymentID := result.claims[0].Route.DeploymentID
		if _, exists := claimedDeploymentIDs[deploymentID]; exists {
			t.Errorf("deployment %s was claimed by more than one controller", deploymentID)
		}
		claimedDeploymentIDs[deploymentID] = struct{}{}
	}

	if len(claimedDeploymentIDs) != 2 {
		t.Errorf("distinct deployment claims = %d, want 2", len(claimedDeploymentIDs))
	}
}

func TestRenewDeploymentRouteLeaseIntegrationExtendsOwnedLease(
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
		10*time.Second,
	)
	if err != nil {
		t.Fatalf("claim deployment route: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("claimed route count = %d, want 1", len(claims))
	}

	oldExpiry := claims[0].LeaseExpiresAt
	newExpiry, err := renewDeploymentRouteLease(
		ctx,
		fixture.pool,
		claims[0].Route.ID,
		claims[0].LeaseOwner,
		30*time.Second,
	)
	if err != nil {
		t.Fatalf("renew owned deployment route lease: %v", err)
	}
	if !newExpiry.After(oldExpiry) {
		t.Errorf("renewed expiry %v does not extend original expiry %v", newExpiry, oldExpiry)
	}

	storedRoute, err := loadDeploymentRoute(
		ctx,
		fixture.pool,
		fixture.projectID,
		fixture.repositoryID,
		fixture.deploymentID,
	)
	if err != nil {
		t.Fatalf("load renewed deployment route: %v", err)
	}
	if storedRoute.LeaseExpiresAt == nil || !storedRoute.LeaseExpiresAt.Equal(newExpiry) {
		t.Errorf("stored lease expiry = %v, want %v", storedRoute.LeaseExpiresAt, newExpiry)
	}
}

func TestDeploymentRouteLeaseIntegrationRejectsWrongAndExpiredOwners(
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
		20*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("claim deployment route: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("claimed route count = %d, want 1", len(claims))
	}
	originalClaim := claims[0]

	wrongOwner := "other-controller:00000000-0000-4000-8000-000000000000"
	_, err = renewDeploymentRouteLease(
		ctx,
		fixture.pool,
		originalClaim.Route.ID,
		wrongOwner,
		time.Minute,
	)
	if !errors.Is(err, errDeploymentRouteLeaseNotOwned) {
		t.Fatalf("renew with wrong owner error = %v, want lease-not-owned", err)
	}

	time.Sleep(50 * time.Millisecond)

	reclaimed, err := claimDeploymentRoutes(
		ctx,
		fixture.pool,
		"route-controller-b",
		1,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("reclaim expired deployment route: %v", err)
	}
	if len(reclaimed) != 1 {
		t.Fatalf("reclaimed route count = %d, want 1", len(reclaimed))
	}
	if reclaimed[0].LeaseOwner == originalClaim.LeaseOwner {
		t.Fatal("reclaimed route retained the expired lease token")
	}
	if reclaimed[0].Route.ReconcileAttemptCount != 2 {
		t.Errorf("reclaimed attempt count = %d, want 2", reclaimed[0].Route.ReconcileAttemptCount)
	}

	_, err = renewDeploymentRouteLease(
		ctx,
		fixture.pool,
		originalClaim.Route.ID,
		originalClaim.LeaseOwner,
		time.Minute,
	)
	if !errors.Is(err, errDeploymentRouteLeaseNotOwned) {
		t.Errorf("renew with expired owner error = %v, want lease-not-owned", err)
	}

	_, err = recordDeploymentRouteReconciliationSuccess(
		ctx,
		fixture.pool,
		originalClaim,
		"active",
	)
	if !errors.Is(err, errDeploymentRouteLeaseNotOwned) {
		t.Errorf("record success with expired owner error = %v, want lease-not-owned", err)
	}
}

func TestRecordDeploymentRouteReconciliationSuccessIntegrationCompletesGeneration(
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

	record, err := recordDeploymentRouteReconciliationSuccess(
		ctx,
		fixture.pool,
		claims[0],
		"active",
	)
	if err != nil {
		t.Fatalf("record successful route reconciliation: %v", err)
	}
	if record.ObservedState != "active" {
		t.Errorf("observed state = %q, want active", record.ObservedState)
	}
	if record.ObservedGeneration == nil ||
		*record.ObservedGeneration != claims[0].Route.DesiredGeneration {
		t.Errorf("observed generation = %v, want %d", record.ObservedGeneration, claims[0].Route.DesiredGeneration)
	}
	if record.LeaseOwner != nil || record.LeaseExpiresAt != nil {
		t.Error("successful reconciliation did not release its lease")
	}
	if record.ReconcileAttemptCount != 0 {
		t.Errorf("successful reconciliation attempt count = %d, want 0", record.ReconcileAttemptCount)
	}
	if record.LastErrorCode != nil {
		t.Errorf("successful reconciliation retained error code %q", *record.LastErrorCode)
	}
	if record.LastReconciledAt == nil {
		t.Error("successful reconciliation did not record last_reconciled_at")
	}

	remaining, err := claimDeploymentRoutes(
		ctx,
		fixture.pool,
		"route-controller-b",
		10,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("check completed route is no longer due: %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("completed route was claimed again %d times", len(remaining))
	}
}

func TestRecordDeploymentRouteReconciliationSuccessIntegrationPreservesStaleGeneration(
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

	record, err := recordDeploymentRouteReconciliationSuccess(
		ctx,
		fixture.pool,
		claims[0],
		"removed",
	)
	if err != nil {
		t.Fatalf("record stale-generation route observation: %v", err)
	}
	if record.DesiredGeneration != claims[0].Route.DesiredGeneration+1 {
		t.Errorf("desired generation = %d, want %d", record.DesiredGeneration, claims[0].Route.DesiredGeneration+1)
	}
	if record.ObservedGeneration == nil ||
		*record.ObservedGeneration != claims[0].Route.DesiredGeneration {
		t.Errorf("observed generation = %v, want stale generation %d", record.ObservedGeneration, claims[0].Route.DesiredGeneration)
	}
	if record.ObservedState != "removed" {
		t.Errorf("observed state = %q, want removed", record.ObservedState)
	}

	nextClaims, err := claimDeploymentRoutes(
		ctx,
		fixture.pool,
		"route-controller-b",
		1,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim newer desired generation: %v", err)
	}
	if len(nextClaims) != 1 {
		t.Fatalf("newer generation claim count = %d, want 1", len(nextClaims))
	}
	if nextClaims[0].Route.DesiredGeneration != record.DesiredGeneration {
		t.Errorf("next claim generation = %d, want %d", nextClaims[0].Route.DesiredGeneration, record.DesiredGeneration)
	}
	if nextClaims[0].Deployment.DesiredState != "inactive" {
		t.Errorf("next claim deployment desired state = %q, want inactive", nextClaims[0].Deployment.DesiredState)
	}
}

func TestRetryDeploymentRouteReconciliationIntegrationSchedulesBackoff(
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

	const retryDelay = 2 * time.Minute
	nextAttempt, err := retryDeploymentRouteReconciliation(
		ctx,
		fixture.pool,
		claims[0],
		retryDelay,
		"caddy_unavailable",
	)
	if err != nil {
		t.Fatalf("schedule deployment route retry: %v", err)
	}
	if time.Until(nextAttempt) < retryDelay-time.Second {
		t.Errorf("retry scheduled too early: next attempt %v, delay %v", nextAttempt, retryDelay)
	}

	record, err := loadDeploymentRoute(
		ctx,
		fixture.pool,
		fixture.projectID,
		fixture.repositoryID,
		fixture.deploymentID,
	)
	if err != nil {
		t.Fatalf("load route after retry scheduling: %v", err)
	}
	if record.ObservedState != "needs_reconciliation" {
		t.Errorf("observed state = %q, want needs_reconciliation", record.ObservedState)
	}
	if record.LastErrorCode == nil || *record.LastErrorCode != "caddy_unavailable" {
		t.Errorf("last error code = %v, want caddy_unavailable", record.LastErrorCode)
	}
	if record.LeaseOwner != nil || record.LeaseExpiresAt != nil {
		t.Error("retry scheduling did not release the lease")
	}

	dueClaims, err := claimDeploymentRoutes(
		ctx,
		fixture.pool,
		"route-controller-b",
		1,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("check route remains in backoff: %v", err)
	}
	if len(dueClaims) != 0 {
		t.Error("route was claimed before its retry time")
	}
}

func TestRetryDeploymentRouteReconciliationIntegrationBypassesBackoffForNewGeneration(
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
		t.Fatalf("advance deployment route generation: %v", err)
	}

	nextAttempt, err := retryDeploymentRouteReconciliation(
		ctx,
		fixture.pool,
		claims[0],
		time.Hour,
		"stale_reconciliation",
	)
	if err != nil {
		t.Fatalf("schedule retry for stale generation: %v", err)
	}
	if time.Until(nextAttempt) > 5*time.Second {
		t.Errorf("new generation was delayed by retry backoff until %v", nextAttempt)
	}

	nextClaims, err := claimDeploymentRoutes(
		ctx,
		fixture.pool,
		"route-controller-b",
		1,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim newer generation after retry: %v", err)
	}
	if len(nextClaims) != 1 {
		t.Fatalf("new generation claim count = %d, want 1", len(nextClaims))
	}
	if nextClaims[0].Route.DesiredGeneration != claims[0].Route.DesiredGeneration+1 {
		t.Errorf("reclaimed generation = %d, want %d", nextClaims[0].Route.DesiredGeneration, claims[0].Route.DesiredGeneration+1)
	}
}
