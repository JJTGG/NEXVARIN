package main

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"
)

func listRepositoryOperationsForTest(
	t *testing.T,
	fixture *operationStoreTestFixture,
	limit int,
	cursor string,
) (operationListResult, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	return listRepositoryOperations(
		ctx,
		fixture.pool,
		fixture.projectID,
		fixture.repositoryID,
		limit,
		cursor,
	)
}

func TestListRepositoryOperationsIntegrationPaginatesWithoutDuplicates(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	revisions := []string{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"cccccccccccccccccccccccccccccccccccccccc",
	}

	var operationIDs []string

	for index, revision := range revisions {
		created, err := fixture.create(
			"list-page-"+string(rune('a'+index)),
			buildOperationRequestForTest(revision),
		)
		if err != nil {
			t.Fatalf("create operation %d: %v", index, err)
		}

		operationIDs = append(operationIDs, created.Operation.ID)
	}

	// Force a shared creation timestamp so the test exercises the ID
	// tie-breaker as well as the timestamp-based cursor.
	fixedTimestamp := time.Date(
		2026, time.October, 16,
		0, 0, 0, 0,
		time.UTC,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	_, err := fixture.pool.Exec(
		ctx,
		`
			UPDATE operations
			SET created_at = $2
			WHERE project_id = $1::uuid
		`,
		fixture.projectID,
		fixedTimestamp,
	)
	cancel()

	if err != nil {
		t.Fatalf("set shared operation timestamps: %v", err)
	}

	expected := make([]operationRecord, 0, len(operationIDs))
	for _, operationID := range operationIDs {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		record, err := loadOperationRecord(
			ctx,
			fixture.pool,
			operationID,
		)
		cancel()

		if err != nil {
			t.Fatalf("reload operation %s: %v", operationID, err)
		}

		expected = append(expected, record)
	}

	sort.Slice(expected, func(i, j int) bool {
		if expected[i].CreatedAt.Equal(expected[j].CreatedAt) {
			return expected[i].ID > expected[j].ID
		}
		return expected[i].CreatedAt.After(expected[j].CreatedAt)
	})

	firstPage, err := listRepositoryOperationsForTest(
		t,
		fixture,
		2,
		"",
	)
	if err != nil {
		t.Fatalf("load first page: %v", err)
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

	for i, want := range expected[:2] {
		if firstPage.Items[i].ID != want.ID {
			t.Errorf(
				"first page item %d ID = %q, want %q",
				i,
				firstPage.Items[i].ID,
				want.ID,
			)
		}
	}

	secondPage, err := listRepositoryOperationsForTest(
		t,
		fixture,
		2,
		*firstPage.NextCursor,
	)
	if err != nil {
		t.Fatalf("load second page: %v", err)
	}

	if len(secondPage.Items) != 1 {
		t.Fatalf(
			"second page item count = %d, want 1",
			len(secondPage.Items),
		)
	}

	if secondPage.NextCursor != nil {
		t.Error("last page should not include a next cursor")
	}

	if secondPage.Items[0].ID != expected[2].ID {
		t.Errorf(
			"second page item ID = %q, want %q",
			secondPage.Items[0].ID,
			expected[2].ID,
		)
	}

	seen := make(map[string]bool)

	for _, item := range append(firstPage.Items, secondPage.Items...) {
		if seen[item.ID] {
			t.Errorf("operation %q appeared on multiple pages", item.ID)
		}
		seen[item.ID] = true
	}

	if len(seen) != len(expected) {
		t.Errorf(
			"unique operation count = %d, want %d",
			len(seen),
			len(expected),
		)
	}
}

func TestListRepositoryOperationsIntegrationReturnsEmptyList(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	result, err := listRepositoryOperationsForTest(
		t,
		fixture,
		20,
		"",
	)
	if err != nil {
		t.Fatalf("list operations in empty repository: %v", err)
	}

	if len(result.Items) != 0 {
		t.Errorf("item count = %d, want 0", len(result.Items))
	}

	if result.NextCursor != nil {
		t.Error("empty result should not include a next cursor")
	}
}

func TestListRepositoryOperationsRejectsInvalidArguments(
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
			wantErr:    errOperationListInvalidLimit,
		},
		{
			name:       "limit above maximum",
			projectID:  fixture.projectID,
			repository: fixture.repositoryID,
			limit:      101,
			wantErr:    errOperationListInvalidLimit,
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
			wantErr:    errOperationListInvalidCursor,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(
				context.Background(),
				10*time.Second,
			)
			defer cancel()

			_, err := listRepositoryOperations(
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

			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Errorf(
					"error = %v, want %v",
					err,
					test.wantErr,
				)
			}
		})
	}
}
