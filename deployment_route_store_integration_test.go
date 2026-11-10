
package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type deploymentRouteStoreIntegrationFixture struct {
	*operationStoreTestFixture
	deploymentID string
}

func newDeploymentRouteStoreIntegrationFixture(
	t *testing.T,
) *deploymentRouteStoreIntegrationFixture {
	t.Helper()

	base := newOperationStoreTestFixture(t)

	fixture := &deploymentRouteStoreIntegrationFixture{
		operationStoreTestFixture: base,
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()

		if _, err := base.pool.Exec(
			ctx,
			`DELETE FROM deployment_routes WHERE project_id = $1::uuid`,
			base.projectID,
		); err != nil {
			t.Errorf("clean up deployment route records: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	err := base.pool.QueryRow(ctx, `
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
	`,
		base.projectID,
		base.repositoryID,
		base.requestedBy,
	).Scan(&fixture.deploymentID)
	if err != nil {
		t.Fatalf("create deployment route fixture: %v", err)
	}

	return fixture
}

func (f *deploymentRouteStoreIntegrationFixture) createRoute(
	ctx context.Context,
	previewSuffix string,
) (createDeploymentRouteResult, error) {
	return createDeploymentRoute(
		ctx,
		f.pool,
		createDeploymentRouteInput{
			ProjectID:     f.projectID,
			RepositoryID:  f.repositoryID,
			DeploymentID:  f.deploymentID,
			PreviewSuffix: previewSuffix,
		},
	)
}

func TestCreateDeploymentRouteIntegrationCreatesAndLoadsPendingRoute(
	t *testing.T,
) {
	fixture := newDeploymentRouteStoreIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	result, err := fixture.createRoute(ctx, "preview.example.test")
	if err != nil {
		t.Fatalf("create deployment route: %v", err)
	}

	if result.Replayed {
		t.Fatal("new deployment route was marked as replayed")
	}

	expectedHostname := "d-" +
		strings.ReplaceAll(fixture.deploymentID, "-", "") +
		".preview.example.test"

	route := result.Route

	if route.ID == "" {
		t.Fatal("created route has an empty ID")
	}
	if route.ProjectID != fixture.projectID {
		t.Errorf("project ID = %q, want %q", route.ProjectID, fixture.projectID)
	}
	if route.RepositoryID != fixture.repositoryID {
		t.Errorf("repository ID = %q, want %q", route.RepositoryID, fixture.repositoryID)
	}
	if route.DeploymentID != fixture.deploymentID {
		t.Errorf("deployment ID = %q, want %q", route.DeploymentID, fixture.deploymentID)
	}
	if route.Hostname != expectedHostname {
		t.Errorf("hostname = %q, want %q", route.Hostname, expectedHostname)
	}
	if route.DesiredState != "active" {
		t.Errorf("desired state = %q, want active", route.DesiredState)
	}
	if route.ObservedState != "pending" {
		t.Errorf("observed state = %q, want pending", route.ObservedState)
	}
	if route.DesiredGeneration != 1 {
		t.Errorf("desired generation = %d, want 1", route.DesiredGeneration)
	}
	if route.ObservedGeneration != nil {
		t.Errorf("observed generation = %v, want nil", route.ObservedGeneration)
	}
	if route.ReconcileAttemptCount != 0 {
		t.Errorf("reconcile attempt count = %d, want 0", route.ReconcileAttemptCount)
	}
	if route.LeaseOwner != nil || route.LeaseExpiresAt != nil {
		t.Error("new route unexpectedly has a reconciliation lease")
	}
	if route.LastErrorCode != nil || route.LastReconciledAt != nil {
		t.Error("new route unexpectedly has reconciliation history")
	}
	if route.NextReconcileAt.IsZero() {
		t.Error("new route has an empty next reconciliation time")
	}

	loaded, err := loadDeploymentRoute(
		ctx,
		fixture.pool,
		fixture.projectID,
		fixture.repositoryID,
		fixture.deploymentID,
	)
	if err != nil {
		t.Fatalf("load created deployment route: %v", err)
	}

	if loaded.ID != route.ID || loaded.Hostname != route.Hostname {
		t.Errorf(
			"loaded route = (%q, %q), want (%q, %q)",
			loaded.ID,
			loaded.Hostname,
			route.ID,
			route.Hostname,
		)
	}

	var count int
	if err := fixture.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM deployment_routes
		WHERE deployment_id = $1::uuid
	`, fixture.deploymentID).Scan(&count); err != nil {
		t.Fatalf("count deployment routes: %v", err)
	}
	if count != 1 {
		t.Errorf("deployment route count = %d, want 1", count)
	}
}

func TestCreateDeploymentRouteIntegrationReplaysOriginalHostname(
	t *testing.T,
) {
	fixture := newDeploymentRouteStoreIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	first, err := fixture.createRoute(ctx, "PREVIEW.Example.Test")
	if err != nil {
		t.Fatalf("create initial deployment route: %v", err)
	}
	if first.Replayed {
		t.Fatal("initial deployment route was marked as replayed")
	}

	second, err := fixture.createRoute(ctx, "changed.example.test")
	if err != nil {
		t.Fatalf("replay deployment route: %v", err)
	}

	if !second.Replayed {
		t.Error("existing deployment route was not marked as replayed")
	}
	if second.Route.ID != first.Route.ID {
		t.Errorf(
			"replayed route ID = %q, want original ID %q",
			second.Route.ID,
			first.Route.ID,
		)
	}
	if second.Route.Hostname != first.Route.Hostname {
		t.Errorf(
			"replayed hostname = %q, want original hostname %q",
			second.Route.Hostname,
			first.Route.Hostname,
		)
	}
	if second.Route.Hostname != strings.ToLower(second.Route.Hostname) {
		t.Errorf("persisted hostname is not lowercase: %q", second.Route.Hostname)
	}

	var count int
	if err := fixture.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM deployment_routes
		WHERE deployment_id = $1::uuid
	`, fixture.deploymentID).Scan(&count); err != nil {
		t.Fatalf("count replayed deployment route: %v", err)
	}
	if count != 1 {
		t.Errorf("deployment route count = %d, want 1", count)
	}
}

func TestCreateDeploymentRouteIntegrationConcurrentRequestsConverge(
	t *testing.T,
) {
	fixture := newDeploymentRouteStoreIntegrationFixture(t)

	const callers = 6

	type outcome struct {
		result createDeploymentRouteResult
		err    error
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		25*time.Second,
	)
	defer cancel()

	start := make(chan struct{})
	results := make(chan outcome, callers)

	for i := range callers {
		suffix := fmt.Sprintf("r%d.preview.example.test", i)

		go func(previewSuffix string) {
			<-start

			result, err := fixture.createRoute(ctx, previewSuffix)
			results <- outcome{
				result: result,
				err:    err,
			}
		}(suffix)
	}

	close(start)

	var (
		routeID       string
		hostname      string
		createdCount  int
		replayedCount int
	)

	for range callers {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent deployment route creation: %v", result.err)
		}

		if routeID == "" {
			routeID = result.result.Route.ID
			hostname = result.result.Route.Hostname
		} else {
			if result.result.Route.ID != routeID {
				t.Errorf(
					"concurrent route ID = %q, want shared ID %q",
					result.result.Route.ID,
					routeID,
				)
			}
			if result.result.Route.Hostname != hostname {
				t.Errorf(
					"concurrent hostname = %q, want shared hostname %q",
					result.result.Route.Hostname,
					hostname,
				)
			}
		}

		if result.result.Replayed {
			replayedCount++
		} else {
			createdCount++
		}
	}

	if createdCount != 1 || replayedCount != callers-1 {
		t.Errorf(
			"created count = %d, replayed count = %d; want 1 and %d",
			createdCount,
			replayedCount,
			callers-1,
		)
	}

	var count int
	if err := fixture.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM deployment_routes
		WHERE deployment_id = $1::uuid
	`, fixture.deploymentID).Scan(&count); err != nil {
		t.Fatalf("count concurrent deployment routes: %v", err)
	}
	if count != 1 {
		t.Errorf("persisted route count = %d, want 1", count)
	}
}

func TestLoadDeploymentRouteIntegrationEnforcesCompleteScope(
	t *testing.T,
) {
	fixture := newDeploymentRouteStoreIntegrationFixture(t)
	foreign := newOperationStoreTestFixture(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	if _, err := fixture.createRoute(ctx, "preview.example.test"); err != nil {
		t.Fatalf("create deployment route: %v", err)
	}

	tests := []struct {
		name         string
		projectID    string
		repositoryID string
	}{
		{
			name:         "foreign project",
			projectID:    foreign.projectID,
			repositoryID: fixture.repositoryID,
		},
		{
			name:         "foreign repository",
			projectID:    fixture.projectID,
			repositoryID: foreign.repositoryID,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadDeploymentRoute(
				ctx,
				fixture.pool,
				test.projectID,
				test.repositoryID,
				fixture.deploymentID,
			)

			if !errors.Is(err, errDeploymentRouteNotFound) {
				t.Fatalf(
					"load with %s scope error = %v, want route-not-found",
					test.name,
					errDeploymentRouteNotFound,
				)
			}
		})
	}
}

func TestCreateDeploymentRouteIntegrationRejectsUnavailableDeployments(
	t *testing.T,
) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name: "inactive deployment",
			query: `
				UPDATE deployments
				SET desired_state = 'inactive'
				WHERE id = $1::uuid
			`,
		},
		{
			name: "deleted deployment",
			query: `
				UPDATE deployments
				SET deleted_at = clock_timestamp()
				WHERE id = $1::uuid
			`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDeploymentRouteStoreIntegrationFixture(t)

			ctx, cancel := context.WithTimeout(
				context.Background(),
				15*time.Second,
			)
			defer cancel()

			if _, err := fixture.pool.Exec(
				ctx,
				test.query,
				fixture.deploymentID,
			); err != nil {
				t.Fatalf("make deployment unavailable: %v", err)
			}

			_, err := fixture.createRoute(ctx, "preview.example.test")
			if !errors.Is(err, errDeploymentRouteDeploymentUnavailable) {
				t.Fatalf(
					"create route for unavailable deployment error = %v, want deployment-unavailable",
					err,
				)
			}

			var count int
			if err := fixture.pool.QueryRow(ctx, `
				SELECT count(*)
				FROM deployment_routes
				WHERE deployment_id = $1::uuid
			`, fixture.deploymentID).Scan(&count); err != nil {
				t.Fatalf("count routes for unavailable deployment: %v", err)
			}
			if count != 0 {
				t.Errorf("route count = %d, want 0", count)
			}
		})
	}
}

func TestCreateDeploymentRouteIntegrationRejectsMissingDeployment(
	t *testing.T,
) {
	fixture := newDeploymentRouteStoreIntegrationFixture(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	var missingDeploymentID string
	if err := fixture.pool.QueryRow(
		ctx,
		`SELECT gen_random_uuid()::text`,
	).Scan(&missingDeploymentID); err != nil {
		t.Fatalf("generate missing deployment ID: %v", err)
	}

	_, err := createDeploymentRoute(
		ctx,
		fixture.pool,
		createDeploymentRouteInput{
			ProjectID:     fixture.projectID,
			RepositoryID:  fixture.repositoryID,
			DeploymentID:  missingDeploymentID,
			PreviewSuffix: "preview.example.test",
		},
	)
	if !errors.Is(err, errDeploymentRouteNotFound) {
		t.Fatalf(
			"create route for missing deployment error = %v, want route-not-found",
			err,
		)
	}
}

func TestDeploymentRouteHostnameNormalizesAndRejectsInvalidSuffixes(
	t *testing.T,
) {
	const deploymentID = "123e4567-e89b-12d3-a456-426614174000"

	got, err := deploymentRouteHostname(
		deploymentID,
		"PREVIEW.Example.Test",
	)
	if err != nil {
		t.Fatalf("normalize valid preview suffix: %v", err)
	}

	const want = "d-123e4567e89b12d3a456426614174000.preview.example.test"
	if got != want {
		t.Errorf("hostname = %q, want %q", got, want)
	}

	tooLongTotal := strings.Join([]string{
		strings.Repeat("a", 63),
		strings.Repeat("b", 63),
		strings.Repeat("c", 63),
		strings.Repeat("d", 63),
	}, ".")

	invalidSuffixes := []string{
		"",
		" preview.example.test",
		"preview.example.test ",
		"preview..example.test",
		"-preview.example.test",
		"preview-.example.test",
		"preview_example.test",
		"préview.example.test",
		strings.Repeat("a", 64) + ".example.test",
		tooLongTotal,
	}

	for _, suffix := range invalidSuffixes {
		t.Run(fmt.Sprintf("invalid_%d", len(suffix)), func(t *testing.T) {
			if _, err := deploymentRouteHostname(deploymentID, suffix); err == nil {
				t.Fatalf("hostname accepted invalid suffix %q", suffix)
			}
		})
	}

	if _, err := deploymentRouteHostname(
		"not-a-uuid",
		"preview.example.test",
	); err == nil {
		t.Fatal("hostname accepted an invalid deployment ID")
	}
}
