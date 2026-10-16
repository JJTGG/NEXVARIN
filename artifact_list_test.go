package main

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"
)

func createArtifactListTestBuild(
	t *testing.T,
	fixture *operationStoreTestFixture,
) string {
	t.Helper()

	result, err := fixture.create(
		"artifact-list-build",
		buildOperationRequestForTest(strings.Repeat("a", 40)),
	)
	if err != nil {
		t.Fatalf("create build operation: %v", err)
	}

	return result.Operation.ID
}

func insertArtifactListTestArtifact(
	t *testing.T,
	fixture *operationStoreTestFixture,
	buildOperationID string,
	digestCharacter string,
	status string,
	imageReference string,
	createdAt time.Time,
) string {
	t.Helper()

	if len(digestCharacter) != 1 ||
		!strings.Contains("abcdef0123456789", digestCharacter) {
		t.Fatalf("invalid test digest character %q", digestCharacter)
	}

	revision := strings.Repeat("a", 40)
	digest := "sha256:" + strings.Repeat(digestCharacter, 64)
	artifactReference := "test-artifact-" + digestCharacter

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	var artifactID string

	err := fixture.pool.QueryRow(
		ctx,
		`
			INSERT INTO artifacts (
				project_id,
				repository_id,
				build_operation_id,
				source_revision,
				artifact_kind,
				artifact_reference,
				content_digest,
				verification_status,
				verified_at,
				metadata,
				created_at
			)
			VALUES (
				$1::uuid,
				$2::uuid,
				$3::uuid,
				$4,
				'oci_image',
				$5,
				$6,
				$7,
				CASE
					WHEN $7 = 'verified' THEN now()
					ELSE NULL
				END,
				jsonb_build_object('imageReference', $8),
				$9
			)
			RETURNING id::text
		`,
		fixture.projectID,
		fixture.repositoryID,
		buildOperationID,
		revision,
		artifactReference,
		digest,
		status,
		imageReference,
		createdAt,
	).Scan(&artifactID)
	if err != nil {
		t.Fatalf("insert test artifact: %v", err)
	}

	return artifactID
}

func listRepositoryArtifactsForTest(
	t *testing.T,
	fixture *operationStoreTestFixture,
	limit int,
	cursor string,
) (artifactListResult, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	return listRepositoryArtifacts(
		ctx,
		fixture.pool,
		fixture.projectID,
		fixture.repositoryID,
		limit,
		cursor,
	)
}

func TestListRepositoryArtifactsIntegrationPaginatesAndFiltersVerified(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)
	buildOperationID := createArtifactListTestBuild(t, fixture)

	// Identical creation timestamps exercise the artifact ID tie-breaker.
	fixedTimestamp := time.Date(
		2026, time.October, 16,
		0, 0, 0, 0,
		time.UTC,
	)

	var expectedIDs []string

	for index, digestCharacter := range []string{"a", "b", "c"} {
		imageReference := "localhost/nexvarin/build-" +
			strings.Repeat(digestCharacter, 8) + ":latest"

		artifactID := insertArtifactListTestArtifact(
			t,
			fixture,
			buildOperationID,
			digestCharacter,
			"verified",
			imageReference,
			fixedTimestamp,
		)

		expectedIDs = append(expectedIDs, artifactID)
	}

	// This record must not appear in the listing because it is not verified.
	insertArtifactListTestArtifact(
		t,
		fixture,
		buildOperationID,
		"d",
		"pending",
		"",
		fixedTimestamp,
	)

	sort.Slice(expectedIDs, func(i, j int) bool {
		return expectedIDs[i] > expectedIDs[j]
	})

	firstPage, err := listRepositoryArtifactsForTest(
		t,
		fixture,
		2,
		"",
	)
	if err != nil {
		t.Fatalf("list first artifact page: %v", err)
	}

	if len(firstPage.Items) != 2 {
		t.Fatalf(
			"first page item count = %d, want 2",
			len(firstPage.Items),
		)
	}

	if firstPage.NextCursor == nil || *firstPage.NextCursor == "" {
		t.Fatal("first page should include a next cursor")
	}

	for i, item := range firstPage.Items {
		if item.ID != expectedIDs[i] {
			t.Errorf(
				"first page item %d ID = %q, want %q",
				i,
				item.ID,
				expectedIDs[i],
			)
		}

		if item.VerificationStatus != "verified" {
			t.Errorf(
				"item %q status = %q, want verified",
				item.ID,
				item.VerificationStatus,
			)
		}

		if item.VerifiedAt.IsZero() {
			t.Errorf("artifact %q has no verification timestamp", item.ID)
		}

		if item.ImageReference == "" {
			t.Errorf("artifact %q has no image reference", item.ID)
		}

		if item.ProjectID != fixture.projectID {
			t.Errorf("artifact project ID = %q", item.ProjectID)
		}

		if item.RepositoryID != fixture.repositoryID {
			t.Errorf("artifact repository ID = %q", item.RepositoryID)
		}
	}

	secondPage, err := listRepositoryArtifactsForTest(
		t,
		fixture,
		2,
		*firstPage.NextCursor,
	)
	if err != nil {
		t.Fatalf("list second artifact page: %v", err)
	}

	if len(secondPage.Items) != 1 {
		t.Fatalf(
			"second page item count = %d, want 1",
			len(secondPage.Items),
		)
	}

	if secondPage.Items[0].ID != expectedIDs[2] {
		t.Errorf(
			"second page item ID = %q, want %q",
			secondPage.Items[0].ID,
			expectedIDs[2],
		)
	}

	if secondPage.NextCursor != nil {
		t.Error("last page should not include a next cursor")
	}

	seen := make(map[string]bool)

	for _, item := range append(firstPage.Items, secondPage.Items...) {
		if seen[item.ID] {
			t.Errorf("artifact %q appeared on multiple pages", item.ID)
		}
		seen[item.ID] = true
	}

	if len(seen) != len(expectedIDs) {
		t.Errorf(
			"unique artifact count = %d, want %d",
			len(seen),
			len(expectedIDs),
		)
	}

	if seen[""] {
		t.Error("listing included an artifact with an empty ID")
	}
}

func TestListRepositoryArtifactsIntegrationReturnsEmptyList(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	result, err := listRepositoryArtifactsForTest(
		t,
		fixture,
		20,
		"",
	)
	if err != nil {
		t.Fatalf("list artifacts in empty repository: %v", err)
	}

	if len(result.Items) != 0 {
		t.Errorf("item count = %d, want 0", len(result.Items))
	}

	if result.NextCursor != nil {
		t.Error("empty result should not include a next cursor")
	}
}

func TestListRepositoryArtifactsRejectsInvalidArguments(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	tests := []struct {
		name       string
		projectID  string
		repository string
		limit      int
		cursor     string
		wantErr    error
	}{
		{
			name:       "zero limit",
			projectID:  fixture.projectID,
			repository: fixture.repositoryID,
			limit:      0,
			wantErr:    errArtifactListInvalidLimit,
		},
		{
			name:       "limit above maximum",
			projectID:  fixture.projectID,
			repository: fixture.repositoryID,
			limit:      101,
			wantErr:    errArtifactListInvalidLimit,
		},
		{
			name:       "invalid project ID",
			projectID:  "not-a-uuid",
			repository: fixture.repositoryID,
			limit:      20,
		},
		{
			name:       "invalid repository ID",
			projectID:  fixture.projectID,
			repository: "not-a-uuid",
			limit:      20,
		},
		{
			name:       "invalid cursor",
			projectID:  fixture.projectID,
			repository: fixture.repositoryID,
			limit:      20,
			cursor:     "not-a-cursor",
			wantErr:    errArtifactListInvalidCursor,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(
				context.Background(),
				10*time.Second,
			)
			defer cancel()

			_, err := listRepositoryArtifacts(
				ctx,
				fixture.pool,
				test.projectID,
				test.repository,
				test.limit,
				test.cursor,
			)

			if err == nil {
				t.Fatal("expected invalid argument error")
			}

			if test.wantErr != nil &&
				!errors.Is(err, test.wantErr) {
				t.Errorf(
					"error = %v, want %v",
					err,
					test.wantErr,
				)
			}
		})
	}
}

func TestListRepositoryArtifactsRejectsNilPool(t *testing.T) {
	_, err := listRepositoryArtifacts(
		context.Background(),
		nil,
		"123e4567-e89b-12d3-a456-426614174000",
		"123e4567-e89b-12d3-a456-426614174001",
		20,
		"",
	)

	if err == nil {
		t.Fatal("expected nil database pool to be rejected")
	}
}
