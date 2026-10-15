package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func createClaimedBuildTaskForSourceTest(
	t *testing.T,
) (*operationStoreTestFixture, operationWorkerTask) {
	t.Helper()

	fixture, claimed := createAndClaimOperationForOutcomeTest(t, 0)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	task, err := loadClaimedOperationTask(
		ctx,
		fixture.pool,
		claimed,
	)
	if err != nil {
		t.Fatalf("load claimed build task: %v", err)
	}

	return fixture, task
}

func TestValidateOperationWorkerSourceURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{
			name: "valid GitHub HTTPS URL",
			url:  "https://github.com/owner/project.git",
		},
		{
			name: "valid GitLab HTTPS URL",
			url:  "https://gitlab.com/owner/project.git",
		},
		{
			name: "valid explicit HTTPS port",
			url:  "https://github.com:443/owner/project.git",
		},
		{
			name: "trims surrounding whitespace",
			url:  " https://github.com/owner/project.git ",
		},
		{
			name:    "empty URL",
			url:     "",
			wantErr: true,
		},
		{
			name:    "malformed URL",
			url:     "not a repository URL",
			wantErr: true,
		},
		{
			name:    "HTTP is rejected",
			url:     "http://github.com/owner/project.git",
			wantErr: true,
		},
		{
			name:    "non-HTTPS scheme is rejected",
			url:     "ssh://github.com/owner/project.git",
			wantErr: true,
		},
		{
			name:    "embedded credentials are rejected",
			url:     "https://user:password@github.com/owner/project.git",
			wantErr: true,
		},
		{
			name:    "query string is rejected",
			url:     "https://github.com/owner/project.git?token=value",
			wantErr: true,
		},
		{
			name:    "fragment is rejected",
			url:     "https://github.com/owner/project.git#main",
			wantErr: true,
		},
		{
			name:    "IPv4 literal is rejected",
			url:     "https://127.0.0.1/owner/project.git",
			wantErr: true,
		},
		{
			name:    "IPv6 literal is rejected",
			url:     "https://[::1]/owner/project.git",
			wantErr: true,
		},
		{
			name:    "localhost is rejected",
			url:     "https://localhost/project.git",
			wantErr: true,
		},
		{
			name:    "localhost subdomain is rejected",
			url:     "https://git.localhost/project.git",
			wantErr: true,
		},
		{
			name:    "local hostname is rejected",
			url:     "https://git.local/project.git",
			wantErr: true,
		},
		{
			name:    "internal hostname is rejected",
			url:     "https://git.internal/project.git",
			wantErr: true,
		},
		{
			name:    "nonstandard port is rejected",
			url:     "https://github.com:8443/owner/project.git",
			wantErr: true,
		},
		{
			name:    "missing repository path is rejected",
			url:     "https://github.com",
			wantErr: true,
		},
		{
			name:    "root-only path is rejected",
			url:     "https://github.com/",
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := validateOperationWorkerSourceURL(test.url)

			if test.wantErr {
				if !errors.Is(err, errOperationWorkerSourceURLInvalid) {
					t.Fatalf(
						"error = %v, want invalid-source-URL error",
						err,
					)
				}
				return
			}

			if err != nil {
				t.Fatalf("validate URL: %v", err)
			}

			if got != strings.TrimSpace(test.url) {
				t.Errorf(
					"normalized URL = %q, want %q",
					got,
					strings.TrimSpace(test.url),
				)
			}
		})
	}
}

func TestLoadOperationWorkerSourceIntegrationLoadsActiveSource(
	t *testing.T,
) {
	fixture, task := createClaimedBuildTaskForSourceTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	source, err := loadOperationWorkerSource(
		ctx,
		fixture.pool,
		task,
	)
	if err != nil {
		t.Fatalf("load operation worker source: %v", err)
	}

	if source.Provider != "generic_git" {
		t.Errorf("provider = %q, want generic_git", source.Provider)
	}
	if source.CanonicalURL != "https://example.invalid/nexvarin-it-source.git" &&
		!strings.HasPrefix(
			source.CanonicalURL,
			"https://example.invalid/nexvarin-it-",
		) {
		t.Errorf("unexpected canonical URL: %q", source.CanonicalURL)
	}
	if source.DisplayName == "" {
		t.Error("display name is empty")
	}
	if source.DefaultBranch != "main" {
		t.Errorf("default branch = %q, want main", source.DefaultBranch)
	}
}

func TestLoadOperationWorkerSourceIntegrationRejectsInactiveRepository(
	t *testing.T,
) {
	fixture, task := createClaimedBuildTaskForSourceTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	_, err := fixture.pool.Exec(ctx, `
		UPDATE repositories
		SET is_active = FALSE
		WHERE id = $1::uuid
	`, task.Claimed.Operation.RepositoryID)
	if err != nil {
		t.Fatalf("deactivate repository: %v", err)
	}

	_, err = loadOperationWorkerSource(ctx, fixture.pool, task)
	if !errors.Is(err, errOperationWorkerSourceUnavailable) {
		t.Fatalf(
			"load inactive repository error = %v, want source-unavailable",
			err,
		)
	}
}

func TestLoadOperationWorkerSourceIntegrationRejectsArchivedProject(
	t *testing.T,
) {
	fixture, task := createClaimedBuildTaskForSourceTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	_, err := fixture.pool.Exec(ctx, `
		UPDATE projects
		SET status = 'archived'
		WHERE id = $1::uuid
	`, task.Claimed.Operation.ProjectID)
	if err != nil {
		t.Fatalf("archive project: %v", err)
	}

	_, err = loadOperationWorkerSource(ctx, fixture.pool, task)
	if !errors.Is(err, errOperationWorkerSourceUnavailable) {
		t.Fatalf(
			"load archived project error = %v, want source-unavailable",
			err,
		)
	}
}

func TestLoadOperationWorkerSourceIntegrationRejectsInvalidStoredURL(
	t *testing.T,
) {
	fixture, task := createClaimedBuildTaskForSourceTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	_, err := fixture.pool.Exec(ctx, `
		UPDATE repositories
		SET canonical_url = 'http://example.invalid/project.git'
		WHERE id = $1::uuid
	`, task.Claimed.Operation.RepositoryID)
	if err != nil {
		t.Fatalf("set invalid repository URL: %v", err)
	}

	_, err = loadOperationWorkerSource(ctx, fixture.pool, task)
	if !errors.Is(err, errOperationWorkerSourceURLInvalid) {
		t.Fatalf(
			"load invalid source URL error = %v, want invalid-source-URL",
			err,
		)
	}
}

func TestLoadOperationWorkerSourceIntegrationRejectsWrongProjectScope(
	t *testing.T,
) {
	fixture, task := createClaimedBuildTaskForSourceTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	task.Claimed.Operation.ProjectID = "00000000-0000-4000-8000-000000000000"

	_, err := loadOperationWorkerSource(ctx, fixture.pool, task)
	if !errors.Is(err, errOperationWorkerSourceUnavailable) {
		t.Fatalf(
			"load mismatched project source error = %v, want source-unavailable",
			err,
		)
	}
}

func TestLoadOperationWorkerSourceRejectsNonBuildOperation(
	t *testing.T,
) {
	fixture, task := createClaimedBuildTaskForSourceTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	task.Request.OperationType = operationTypeDeploy

	_, err := loadOperationWorkerSource(ctx, fixture.pool, task)
	if err == nil ||
		!strings.Contains(err.Error(), "requires a build operation") {
		t.Fatalf(
			"load non-build source error = %v, want build-operation validation",
			err,
		)
	}
}
