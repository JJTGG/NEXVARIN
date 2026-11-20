
package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type deploymentRouteReconcileRemoveCall struct {
	RouteID  string
	Hostname string
}

type deploymentRouteReconcileScriptedAdapter struct {
	ensureRouteFn func(
		context.Context,
		caddyRouteConfig,
	) (caddyRouteMutationResult, error)

	removeRouteFn func(
		context.Context,
		string,
		string,
	) (caddyRouteMutationResult, error)

	verifyRouteActiveFn func(
		context.Context,
		caddyRouteConfig,
	) error

	verifyRouteRemovedFn func(
		context.Context,
		string,
		string,
	) error

	ensureRouteCalls   []caddyRouteConfig
	removeRouteCalls   []deploymentRouteReconcileRemoveCall
	verifyActiveCalls  []caddyRouteConfig
	verifyRemovedCalls []deploymentRouteReconcileRemoveCall
}

func (adapter *deploymentRouteReconcileScriptedAdapter) EnsureRoute(
	ctx context.Context,
	expected caddyRouteConfig,
) (caddyRouteMutationResult, error) {
	adapter.ensureRouteCalls = append(
		adapter.ensureRouteCalls,
		expected,
	)

	if adapter.ensureRouteFn != nil {
		return adapter.ensureRouteFn(ctx, expected)
	}

	return caddyRouteMutationResult{
		Kind:    caddyRouteMutationAdded,
		Changed: true,
	}, nil
}

func (adapter *deploymentRouteReconcileScriptedAdapter) RemoveRoute(
	ctx context.Context,
	routeID string,
	hostname string,
) (caddyRouteMutationResult, error) {
	adapter.removeRouteCalls = append(
		adapter.removeRouteCalls,
		deploymentRouteReconcileRemoveCall{
			RouteID:  routeID,
			Hostname: hostname,
		},
	)

	if adapter.removeRouteFn != nil {
		return adapter.removeRouteFn(ctx, routeID, hostname)
	}

	return caddyRouteMutationResult{
		Kind:    caddyRouteMutationRemoved,
		Changed: true,
	}, nil
}

func (adapter *deploymentRouteReconcileScriptedAdapter) VerifyRouteActive(
	ctx context.Context,
	expected caddyRouteConfig,
) error {
	adapter.verifyActiveCalls = append(
		adapter.verifyActiveCalls,
		expected,
	)

	if adapter.verifyRouteActiveFn != nil {
		return adapter.verifyRouteActiveFn(ctx, expected)
	}

	return nil
}

func (adapter *deploymentRouteReconcileScriptedAdapter) VerifyRouteRemoved(
	ctx context.Context,
	routeID string,
	hostname string,
) error {
	adapter.verifyRemovedCalls = append(
		adapter.verifyRemovedCalls,
		deploymentRouteReconcileRemoveCall{
			RouteID:  routeID,
			Hostname: hostname,
		},
	)

	if adapter.verifyRouteRemovedFn != nil {
		return adapter.verifyRouteRemovedFn(
			ctx,
			routeID,
			hostname,
		)
	}

	return nil
}

func createClaimedReadyDeploymentRoute(
	t *testing.T,
	ctx context.Context,
	fixture *deploymentRouteReconcileIntegrationFixture,
) claimedDeploymentRoute {
	t.Helper()

	if err := fixture.prepareDeploymentReady(
		ctx,
		fixture.deploymentID,
	); err != nil {
		t.Fatalf("prepare deployment as ready: %v", err)
	}

	if _, err := fixture.createRoute(
		ctx,
		"preview.example.test",
	); err != nil {
		t.Fatalf("create deployment route: %v", err)
	}

	return claimSingleDeploymentRoute(t, ctx, fixture)
}

func createClaimedIneligibleDeploymentRoute(
	t *testing.T,
	ctx context.Context,
	fixture *deploymentRouteReconcileIntegrationFixture,
) claimedDeploymentRoute {
	t.Helper()

	if _, err := fixture.createRoute(
		ctx,
		"preview.example.test",
	); err != nil {
		t.Fatalf("create deployment route: %v", err)
	}

	return claimSingleDeploymentRoute(t, ctx, fixture)
}

func claimSingleDeploymentRoute(
	t *testing.T,
	ctx context.Context,
	fixture *deploymentRouteReconcileIntegrationFixture,
) claimedDeploymentRoute {
	t.Helper()

	claims, err := claimDeploymentRoutes(
		ctx,
		fixture.pool,
		"route-controller-reconciler",
		1,
		30*time.Second,
	)
	if err != nil {
		t.Fatalf("claim deployment route: %v", err)
	}

	if len(claims) != 1 {
		t.Fatalf(
			"claimed route count = %d, want 1",
			len(claims),
		)
	}

	return claims[0]
}

func newDeploymentRouteReconcilerForTest(
	fixture *deploymentRouteReconcileIntegrationFixture,
	adapter deploymentRouteReconcileAdapter,
) deploymentRouteReconciler {
	return deploymentRouteReconciler{
		Pool:          fixture.pool,
		Adapter:       adapter,
		PreviewSuffix: "preview.example.test",
	}
}

func assertDeploymentRouteReconcileObservation(
	t *testing.T,
	ctx context.Context,
	fixture *deploymentRouteReconcileIntegrationFixture,
	wantObservedState string,
	wantPreviewAddress string,
	wantObservedGeneration bool,
) deploymentRouteRecord {
	t.Helper()

	record, err := loadDeploymentRoute(
		ctx,
		fixture.pool,
		fixture.projectID,
		fixture.repositoryID,
		fixture.deploymentID,
	)
	if err != nil {
		t.Fatalf("load reconciled deployment route: %v", err)
	}

	if record.ObservedState != wantObservedState {
		t.Errorf(
			"observed state = %q, want %q",
			record.ObservedState,
			wantObservedState,
		)
	}

	if wantObservedGeneration {
		if record.ObservedGeneration == nil {
			t.Error("observed generation is nil")
		} else if *record.ObservedGeneration != record.DesiredGeneration {
			t.Errorf(
				"observed generation = %d, want desired generation %d",
				*record.ObservedGeneration,
				record.DesiredGeneration,
			)
		}
	} else if record.ObservedGeneration != nil {
		t.Errorf(
			"observed generation = %d, want nil",
			*record.ObservedGeneration,
		)
	}

	if record.LeaseOwner != nil || record.LeaseExpiresAt != nil {
		t.Error("reconciliation left a lease attached to the route")
	}

	previewAddress, err := fixture.loadPreviewAddress(
		ctx,
		fixture.deploymentID,
	)
	if err != nil {
		t.Fatalf("load deployment preview address: %v", err)
	}

	if previewAddress != wantPreviewAddress {
		t.Errorf(
			"preview address = %q, want %q",
			previewAddress,
			wantPreviewAddress,
		)
	}

	return record
}

func TestDeploymentRouteReconcilerIntegrationEnsuresAndPersistsVerifiedRoute(
	t *testing.T,
) {
	fixture := newDeploymentRouteReconcileIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	claim := createClaimedReadyDeploymentRoute(
		t,
		ctx,
		fixture,
	)

	var leaseRenewedBeforeMutation bool

	adapter := &deploymentRouteReconcileScriptedAdapter{}
	adapter.ensureRouteFn = func(
		ctx context.Context,
		expected caddyRouteConfig,
	) (caddyRouteMutationResult, error) {
		err := fixture.pool.QueryRow(
			ctx,
			`
				SELECT lease_expires_at >
					clock_timestamp() + interval '90 seconds'
				FROM deployment_routes
				WHERE id = $1::uuid
			`,
			claim.Route.ID,
		).Scan(&leaseRenewedBeforeMutation)
		if err != nil {
			return caddyRouteMutationResult{}, fmt.Errorf(
				"inspect lease before adapter mutation: %w",
				err,
			)
		}

		return caddyRouteMutationResult{
			Kind:    caddyRouteMutationAdded,
			Changed: true,
		}, nil
	}

	reconciler := newDeploymentRouteReconcilerForTest(
		fixture,
		adapter,
	)

	if err := reconciler.reconcileClaim(ctx, claim); err != nil {
		t.Fatalf("reconcile deployment route: %v", err)
	}

	if len(adapter.ensureRouteCalls) != 1 {
		t.Fatalf(
			"EnsureRoute calls = %d, want 1",
			len(adapter.ensureRouteCalls),
		)
	}

	expected := adapter.ensureRouteCalls[0]

	if expected.ID != caddyRouteIDPrefix+claim.Route.ID {
		t.Errorf(
			"expected route ID = %q, want %q",
			expected.ID,
			caddyRouteIDPrefix+claim.Route.ID,
		)
	}

	if expected.Hostname != claim.Route.Hostname {
		t.Errorf(
			"expected hostname = %q, want %q",
			expected.Hostname,
			claim.Route.Hostname,
		)
	}

	if expected.UpstreamAddress != "127.0.0.1:31001" {
		t.Errorf(
			"expected upstream = %q, want %q",
			expected.UpstreamAddress,
			"127.0.0.1:31001",
		)
	}

	if !leaseRenewedBeforeMutation {
		t.Error("route lease was not extended before the external mutation")
	}

	if len(adapter.verifyActiveCalls) != 1 {
		t.Fatalf(
			"VerifyRouteActive calls = %d, want 1",
			len(adapter.verifyActiveCalls),
		)
	}

	if adapter.verifyActiveCalls[0] != expected {
		t.Error("verified route does not match the mutation's expected route")
	}

	if len(adapter.removeRouteCalls) != 0 {
		t.Errorf(
			"RemoveRoute calls = %d, want 0",
			len(adapter.removeRouteCalls),
		)
	}

	record := assertDeploymentRouteReconcileObservation(
		t,
		ctx,
		fixture,
		"active",
		claim.Route.Hostname,
		true,
	)

	if record.LastErrorCode != nil {
		t.Errorf(
			"last error code = %q, want nil",
			*record.LastErrorCode,
		)
	}

	if record.ReconcileAttemptCount != 0 {
		t.Errorf(
			"reconcile attempt count = %d, want 0",
			record.ReconcileAttemptCount,
		)
	}
}

func TestDeploymentRouteReconcilerIntegrationRemovesIneligibleDeploymentRoute(
	t *testing.T,
) {
	fixture := newDeploymentRouteReconcileIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	claim := createClaimedIneligibleDeploymentRoute(
		t,
		ctx,
		fixture,
	)

	adapter := &deploymentRouteReconcileScriptedAdapter{}

	reconciler := newDeploymentRouteReconcilerForTest(
		fixture,
		adapter,
	)

	if err := reconciler.reconcileClaim(ctx, claim); err != nil {
		t.Fatalf("reconcile ineligible deployment route: %v", err)
	}

	if len(adapter.ensureRouteCalls) != 0 {
		t.Errorf(
			"EnsureRoute calls = %d, want 0",
			len(adapter.ensureRouteCalls),
		)
	}

	if len(adapter.removeRouteCalls) != 1 {
		t.Fatalf(
			"RemoveRoute calls = %d, want 1",
			len(adapter.removeRouteCalls),
		)
	}

	removed := adapter.removeRouteCalls[0]

	if removed.RouteID != caddyRouteIDPrefix+claim.Route.ID {
		t.Errorf(
			"removed route ID = %q, want %q",
			removed.RouteID,
			caddyRouteIDPrefix+claim.Route.ID,
		)
	}

	if removed.Hostname != claim.Route.Hostname {
		t.Errorf(
			"removed hostname = %q, want %q",
			removed.Hostname,
			claim.Route.Hostname,
		)
	}

	if len(adapter.verifyRemovedCalls) != 1 {
		t.Fatalf(
			"VerifyRouteRemoved calls = %d, want 1",
			len(adapter.verifyRemovedCalls),
		)
	}

	record := assertDeploymentRouteReconcileObservation(
		t,
		ctx,
		fixture,
		"removed",
		"",
		true,
	)

	if record.LastErrorCode != nil {
		t.Errorf(
			"last error code = %q, want nil",
			*record.LastErrorCode,
		)
	}
}

func TestDeploymentRouteReconcilerIntegrationResolvesUnknownMutationOutcomeByReadBack(
	t *testing.T,
) {
	fixture := newDeploymentRouteReconcileIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	claim := createClaimedReadyDeploymentRoute(
		t,
		ctx,
		fixture,
	)

	adapter := &deploymentRouteReconcileScriptedAdapter{
		ensureRouteFn: func(
			context.Context,
			caddyRouteConfig,
		) (caddyRouteMutationResult, error) {
			return caddyRouteMutationResult{},
				errCaddyAdminOutcomeUnknown
		},
	}

	reconciler := newDeploymentRouteReconcilerForTest(
		fixture,
		adapter,
	)

	if err := reconciler.reconcileClaim(ctx, claim); err != nil {
		t.Fatalf(
			"reconcile route after unknown mutation outcome: %v",
			err,
		)
	}

	if len(adapter.ensureRouteCalls) != 1 {
		t.Errorf(
			"EnsureRoute calls = %d, want 1",
			len(adapter.ensureRouteCalls),
		)
	}

	if len(adapter.verifyActiveCalls) != 1 {
		t.Errorf(
			"VerifyRouteActive calls = %d, want 1",
			len(adapter.verifyActiveCalls),
		)
	}

	assertDeploymentRouteReconcileObservation(
		t,
		ctx,
		fixture,
		"active",
		claim.Route.Hostname,
		true,
	)
}

func TestDeploymentRouteReconcilerIntegrationRetriesPreconditionConflictAfterReadBack(
	t *testing.T,
) {
	fixture := newDeploymentRouteReconcileIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	claim := createClaimedReadyDeploymentRoute(
		t,
		ctx,
		fixture,
	)

	adapter := &deploymentRouteReconcileScriptedAdapter{}

	adapter.ensureRouteFn = func(
		context.Context,
		caddyRouteConfig,
	) (caddyRouteMutationResult, error) {
		if len(adapter.ensureRouteCalls) == 1 {
			return caddyRouteMutationResult{},
				errCaddyRouteMutationPreconditionFailed
		}

		return caddyRouteMutationResult{
			Kind:    caddyRouteMutationAdded,
			Changed: true,
		}, nil
	}

	adapter.verifyRouteActiveFn = func(
		context.Context,
		caddyRouteConfig,
	) error {
		if len(adapter.verifyActiveCalls) == 1 {
			return fmt.Errorf(
				"%w: route absent after first conditional mutation",
				errCaddyRouteVerificationMismatch,
			)
		}

		return nil
	}

	reconciler := newDeploymentRouteReconcilerForTest(
		fixture,
		adapter,
	)

	if err := reconciler.reconcileClaim(ctx, claim); err != nil {
		t.Fatalf(
			"reconcile after Caddy precondition conflict: %v",
			err,
		)
	}

	if len(adapter.ensureRouteCalls) != 2 {
		t.Errorf(
			"EnsureRoute calls = %d, want 2",
			len(adapter.ensureRouteCalls),
		)
	}

	if len(adapter.verifyActiveCalls) != 2 {
		t.Errorf(
			"VerifyRouteActive calls = %d, want 2",
			len(adapter.verifyActiveCalls),
		)
	}

	assertDeploymentRouteReconcileObservation(
		t,
		ctx,
		fixture,
		"active",
		claim.Route.Hostname,
		true,
	)
}

func TestDeploymentRouteReconcilerIntegrationRevalidatesChangedUpstreamBeforeRetry(
	t *testing.T,
) {
	fixture := newDeploymentRouteReconcileIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	claim := createClaimedReadyDeploymentRoute(
		t,
		ctx,
		fixture,
	)

	adapter := &deploymentRouteReconcileScriptedAdapter{}

	adapter.ensureRouteFn = func(
		context.Context,
		caddyRouteConfig,
	) (caddyRouteMutationResult, error) {
		return caddyRouteMutationResult{},
			errCaddyRouteMutationPreconditionFailed
	}

	adapter.verifyRouteActiveFn = func(
		ctx context.Context,
		expected caddyRouteConfig,
	) error {
		_, err := fixture.pool.Exec(
			ctx,
			`
				UPDATE deployments
				SET upstream_address = $2
				WHERE id = $1::uuid
			`,
			fixture.deploymentID,
			"127.0.0.1:31002",
		)
		if err != nil {
			return fmt.Errorf(
				"change deployment upstream during verification: %w",
				err,
			)
		}

		return fmt.Errorf(
			"%w: route absent after conditional mutation",
			errCaddyRouteVerificationMismatch,
		)
	}

	reconciler := newDeploymentRouteReconcilerForTest(
		fixture,
		adapter,
	)

	if err := reconciler.reconcileClaim(ctx, claim); err != nil {
		t.Fatalf(
			"reconcile after upstream changed: %v",
			err,
		)
	}

	if len(adapter.ensureRouteCalls) != 1 {
		t.Errorf(
			"EnsureRoute calls = %d, want 1; stale upstream must not be retried",
			len(adapter.ensureRouteCalls),
		)
	}

	if len(adapter.verifyActiveCalls) != 1 {
		t.Errorf(
			"VerifyRouteActive calls = %d, want 1",
			len(adapter.verifyActiveCalls),
		)
	}

	record := assertDeploymentRouteReconcileObservation(
		t,
		ctx,
		fixture,
		"needs_reconciliation",
		"",
		false,
	)

	if record.LastErrorCode == nil ||
		*record.LastErrorCode != "stale_generation" {
		t.Errorf(
			"last error code = %v, want stale_generation",
			record.LastErrorCode,
		)
	}
}

func TestDeploymentRouteReconcilerIntegrationSchedulesRetryAfterPersistentVerificationMismatch(
	t *testing.T,
) {
	fixture := newDeploymentRouteReconcileIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	claim := createClaimedReadyDeploymentRoute(
		t,
		ctx,
		fixture,
	)

	adapter := &deploymentRouteReconcileScriptedAdapter{
		verifyRouteActiveFn: func(
			context.Context,
			caddyRouteConfig,
		) error {
			return fmt.Errorf(
				"%w: expected route is absent",
				errCaddyRouteVerificationMismatch,
			)
		},
	}

	reconciler := newDeploymentRouteReconcilerForTest(
		fixture,
		adapter,
	)

	if err := reconciler.reconcileClaim(ctx, claim); err != nil {
		t.Fatalf(
			"reconcile after persistent verification mismatch: %v",
			err,
		)
	}

	if len(adapter.ensureRouteCalls) != deploymentRouteReconcileMaxMutationAttempts {
		t.Errorf(
			"EnsureRoute calls = %d, want %d",
			len(adapter.ensureRouteCalls),
			deploymentRouteReconcileMaxMutationAttempts,
		)
	}

	if len(adapter.verifyActiveCalls) != deploymentRouteReconcileMaxMutationAttempts {
		t.Errorf(
			"VerifyRouteActive calls = %d, want %d",
			len(adapter.verifyActiveCalls),
			deploymentRouteReconcileMaxMutationAttempts,
		)
	}

	record := assertDeploymentRouteReconcileObservation(
		t,
		ctx,
		fixture,
		"needs_reconciliation",
		"",
		false,
	)

	if record.LastErrorCode == nil ||
		*record.LastErrorCode != "caddy_route_verification_mismatch" {
		t.Errorf(
			"last error code = %v, want caddy_route_verification_mismatch",
			record.LastErrorCode,
		)
	}

	var retryScheduled bool
	if err := fixture.pool.QueryRow(
		ctx,
		`
			SELECT
				next_reconcile_at >
					clock_timestamp() + interval '4 seconds'
				AND next_reconcile_at <
					clock_timestamp() + interval '10 seconds'
			FROM deployment_routes
			WHERE id = $1::uuid
		`,
		claim.Route.ID,
	).Scan(&retryScheduled); err != nil {
		t.Fatalf("inspect scheduled retry time: %v", err)
	}

	if !retryScheduled {
		t.Error("persistent verification mismatch did not schedule the expected retry delay")
	}
}

func TestDeploymentRouteReconcilerIntegrationDoesNotMutateAfterLeaseLostBeforePreflight(
	t *testing.T,
) {
	fixture := newDeploymentRouteReconcileIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	claim := createClaimedReadyDeploymentRoute(
		t,
		ctx,
		fixture,
	)

	const replacementOwner =
		"other-controller:00000000-0000-4000-8000-000000000000"

	_, err := fixture.pool.Exec(
		ctx,
		`
			UPDATE deployment_routes
			SET
				lease_owner = $2,
				lease_expires_at = clock_timestamp() + interval '2 minutes'
			WHERE id = $1::uuid
		`,
		claim.Route.ID,
		replacementOwner,
	)
	if err != nil {
		t.Fatalf("replace route lease owner: %v", err)
	}

	adapter := &deploymentRouteReconcileScriptedAdapter{}

	reconciler := newDeploymentRouteReconcilerForTest(
		fixture,
		adapter,
	)

	if err := reconciler.reconcileClaim(ctx, claim); err != nil {
		t.Fatalf("reconcile claim after lease loss: %v", err)
	}

	if len(adapter.ensureRouteCalls) != 0 ||
		len(adapter.removeRouteCalls) != 0 ||
		len(adapter.verifyActiveCalls) != 0 ||
		len(adapter.verifyRemovedCalls) != 0 {
		t.Error("reconciler performed external work after losing lease ownership")
	}

	record, err := loadDeploymentRoute(
		ctx,
		fixture.pool,
		fixture.projectID,
		fixture.repositoryID,
		fixture.deploymentID,
	)
	if err != nil {
		t.Fatalf("load route after lease takeover: %v", err)
	}

	if record.LeaseOwner == nil || *record.LeaseOwner != replacementOwner {
		t.Errorf(
			"lease owner = %v, want %q",
			record.LeaseOwner,
			replacementOwner,
		)
	}

	if record.ObservedState != "pending" {
		t.Errorf(
			"observed state = %q, want pending",
			record.ObservedState,
		)
	}
}

func TestDeploymentRouteReconcilerIntegrationDoesNotPublishAfterLeaseLostDuringVerification(
	t *testing.T,
) {
	fixture := newDeploymentRouteReconcileIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	claim := createClaimedReadyDeploymentRoute(
		t,
		ctx,
		fixture,
	)

	const replacementOwner =
		"other-controller:00000000-0000-4000-8000-000000000001"

	adapter := &deploymentRouteReconcileScriptedAdapter{
		verifyRouteActiveFn: func(
			ctx context.Context,
			expected caddyRouteConfig,
		) error {
			updateContext, updateCancel := context.WithTimeout(
				ctx,
				3*time.Second,
			)
			defer updateCancel()

			_, err := fixture.pool.Exec(
				updateContext,
				`
					UPDATE deployment_routes
					SET
						lease_owner = $2,
						lease_expires_at =
							clock_timestamp() + interval '2 minutes'
					WHERE id = $1::uuid
				`,
				claim.Route.ID,
				replacementOwner,
			)
			if err != nil {
				return fmt.Errorf(
					"simulate lease takeover during verification: %w",
					err,
				)
			}

			return nil
		},
	}

	reconciler := newDeploymentRouteReconcilerForTest(
		fixture,
		adapter,
	)

	if err := reconciler.reconcileClaim(ctx, claim); err != nil {
		t.Fatalf(
			"reconcile after lease takeover during verification: %v",
			err,
		)
	}

	if len(adapter.ensureRouteCalls) != 1 ||
		len(adapter.verifyActiveCalls) != 1 {
		t.Fatal("expected one mutation and one verification before lease takeover")
	}

	record, err := loadDeploymentRoute(
		ctx,
		fixture.pool,
		fixture.projectID,
		fixture.repositoryID,
		fixture.deploymentID,
	)
	if err != nil {
		t.Fatalf("load route after verification lease takeover: %v", err)
	}

	if record.ObservedState != "pending" {
		t.Errorf(
			"observed state = %q, want pending",
			record.ObservedState,
		)
	}

	if record.ObservedGeneration != nil {
		t.Errorf(
			"observed generation = %d, want nil",
			*record.ObservedGeneration,
		)
	}

	if record.LeaseOwner == nil || *record.LeaseOwner != replacementOwner {
		t.Errorf(
			"lease owner = %v, want %q",
			record.LeaseOwner,
			replacementOwner,
		)
	}

	previewAddress, err := fixture.loadPreviewAddress(
		ctx,
		fixture.deploymentID,
	)
	if err != nil {
		t.Fatalf("load preview address after lease takeover: %v", err)
	}

	if previewAddress != "" {
		t.Errorf(
			"preview address = %q, want empty",
			previewAddress,
		)
	}
}

func TestDeploymentRouteReconcilerIntegrationSchedulesRetryAfterContextCancellation(
	t *testing.T,
) {
	fixture := newDeploymentRouteReconcileIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)

	claim := createClaimedIneligibleDeploymentRoute(
		t,
		ctx,
		fixture,
	)

	adapter := &deploymentRouteReconcileScriptedAdapter{
		removeRouteFn: func(
			context.Context,
			string,
			string,
		) (caddyRouteMutationResult, error) {
			cancel()

			return caddyRouteMutationResult{},
				errors.New("simulated adapter failure during shutdown")
		},
	}

	reconciler := newDeploymentRouteReconcilerForTest(
		fixture,
		adapter,
	)

	if err := reconciler.reconcileClaim(ctx, claim); err != nil {
		t.Fatalf(
			"reconcile after adapter failure and context cancellation: %v",
			err,
		)
	}

	if len(adapter.removeRouteCalls) != 1 {
		t.Errorf(
			"RemoveRoute calls = %d, want 1",
			len(adapter.removeRouteCalls),
		)
	}

	if len(adapter.verifyRemovedCalls) != 0 {
		t.Errorf(
			"VerifyRouteRemoved calls = %d, want 0",
			len(adapter.verifyRemovedCalls),
		)
	}

	checkContext, checkCancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer checkCancel()

	record := assertDeploymentRouteReconcileObservation(
		t,
		checkContext,
		fixture,
		"needs_reconciliation",
		"",
		false,
	)

	if record.LastErrorCode == nil ||
		*record.LastErrorCode != "caddy_route_invalid" {
		t.Errorf(
			"last error code = %v, want caddy_route_invalid",
			record.LastErrorCode,
		)
	}
}

func TestDeploymentRouteReconcilerIntegrationConvergesAgainstPinnedCaddy(
	t *testing.T,
) {
	fixture := newDeploymentRouteReconcileIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	claim := createClaimedReadyDeploymentRoute(
		t,
		ctx,
		fixture,
	)

	integration := startCaddyAdminIntegration(t)

	adapter := caddyRouteMutator{
		Client:        integration.client,
		ServerName:    "nexvarin",
		PreviewSuffix: "preview.example.test",
	}

	reconciler := newDeploymentRouteReconcilerForTest(
		fixture,
		adapter,
	)

	if err := reconciler.reconcileClaim(ctx, claim); err != nil {
		t.Fatalf(
			"reconcile deployment route against pinned Caddy: %v",
			err,
		)
	}

	assertDeploymentRouteReconcileObservation(
		t,
		ctx,
		fixture,
		"active",
		claim.Route.Hostname,
		true,
	)

	serverRoutes, _ := integration.readServerRoutes(t)

	if len(serverRoutes) != 2 {
		t.Fatalf(
			"Caddy server route count = %d, want 2",
			len(serverRoutes),
		)
	}

	namespaceRoutes := caddyAdminIntegrationNamespaceRoutes(
		t,
		serverRoutes,
	)

	if len(namespaceRoutes) != 1 {
		t.Fatalf(
			"NEXVARIN namespace route count = %d, want 1",
			len(namespaceRoutes),
		)
	}

	installedRoute, ok := namespaceRoutes[0].(map[string]any)
	if !ok {
		t.Fatalf(
			"installed route has unexpected type: %T",
			namespaceRoutes[0],
		)
	}

	expectedRouteID := caddyRouteIDPrefix + claim.Route.ID

	if installedRoute["@id"] != expectedRouteID {
		t.Errorf(
			"installed route ID = %v, want %q",
			installedRoute["@id"],
			expectedRouteID,
		)
	}

	matchers, ok := installedRoute["match"].([]any)
	if !ok || len(matchers) != 1 {
		t.Fatalf(
			"installed route matchers = %v, want exactly one matcher",
			installedRoute["match"],
		)
	}

	matcher, ok := matchers[0].(map[string]any)
	if !ok {
		t.Fatalf(
			"installed route matcher has unexpected type: %T",
			matchers[0],
		)
	}

	hosts, ok := matcher["host"].([]any)
	if !ok || len(hosts) != 1 || hosts[0] != claim.Route.Hostname {
		t.Errorf(
			"installed route hosts = %v, want %q",
			matcher["host"],
			claim.Route.Hostname,
		)
	}

	handlers, ok := installedRoute["handle"].([]any)
	if !ok || len(handlers) != 1 {
		t.Fatalf(
			"installed route handlers = %v, want exactly one handler",
			installedRoute["handle"],
		)
	}

	handler, ok := handlers[0].(map[string]any)
	if !ok {
		t.Fatalf(
			"installed route handler has unexpected type: %T",
			handlers[0],
		)
	}

	if handler["handler"] != "reverse_proxy" {
		t.Errorf(
			"installed route handler = %v, want reverse_proxy",
			handler["handler"],
		)
	}

	upstreams, ok := handler["upstreams"].([]any)
	if !ok || len(upstreams) != 1 {
		t.Fatalf(
			"installed route upstreams = %v, want exactly one upstream",
			handler["upstreams"],
		)
	}

	upstream, ok := upstreams[0].(map[string]any)
	if !ok {
		t.Fatalf(
			"installed route upstream has unexpected type: %T",
			upstreams[0],
		)
	}

	if upstream["dial"] != "127.0.0.1:31001" {
		t.Errorf(
			"installed upstream = %v, want %q",
			upstream["dial"],
			"127.0.0.1:31001",
		)
	}

	operatorRoutePresent := false
	for _, route := range serverRoutes {
		if route["@id"] == caddyAdminIntegrationOperatorRouteID {
			operatorRoutePresent = true
			break
		}
	}

	if !operatorRoutePresent {
		t.Error("Caddy operator-owned route was not preserved")
	}
}
