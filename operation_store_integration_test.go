package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type operationStoreTestFixture struct {
	pool         *pgxpool.Pool
	projectID    string
	repositoryID string
	requestedBy  string
}

func newOperationStoreTestFixture(t *testing.T) *operationStoreTestFixture {
	t.Helper()

	databaseURL := strings.TrimSpace(os.Getenv("TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set; skipping PostgreSQL integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	if config.ConnConfig.Database != "nexvarin_test" {
		t.Fatalf(
			"refusing to run integration tests against database %q; expected the disposable nexvarin_test database",
			config.ConnConfig.Database,
		)
	}
	config.MaxConns = 8

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("create PostgreSQL pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL integration database: %v", err)
	}

	var randomBytes [8]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		t.Fatalf("generate fixture suffix: %v", err)
	}
	suffix := hex.EncodeToString(randomBytes[:])
	slug := "nexvarin-it-" + suffix

	fixture := &operationStoreTestFixture{
		pool:        pool,
		requestedBy: "integration-test:" + suffix,
	}

	if err := pool.QueryRow(ctx, `
		INSERT INTO projects (slug, name, owner_subject)
		VALUES ($1, $2, $3)
		RETURNING id::text
	`, slug, "NEXVARIN integration "+suffix, fixture.requestedBy).Scan(
		&fixture.projectID,
	); err != nil {
		t.Fatalf("create integration project: %v", err)
	}

	// Clean dependent records in foreign-key-safe order. This cleanup is
	// registered before repository setup so it also runs after later failures.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cleanupCancel()

		statements := []string{
			`DELETE FROM outbox_events WHERE project_id = $1::uuid`,
			`DELETE FROM operation_events WHERE project_id = $1::uuid`,
			`DELETE FROM deployments WHERE project_id = $1::uuid`,
			`DELETE FROM artifacts WHERE project_id = $1::uuid`,
			`DELETE FROM operations WHERE project_id = $1::uuid AND parent_operation_id IS NOT NULL`,
			`DELETE FROM operations WHERE project_id = $1::uuid`,
			`DELETE FROM repositories WHERE project_id = $1::uuid`,
			`DELETE FROM projects WHERE id = $1::uuid`,
		}

		for _, statement := range statements {
			if _, err := pool.Exec(cleanupCtx, statement, fixture.projectID); err != nil {
				t.Errorf("clean up integration fixture: %v", err)
			}
		}
	})

	if err := pool.QueryRow(ctx, `
		INSERT INTO repositories (
			project_id,
			provider,
			canonical_url,
			display_name,
			default_branch
		)
		VALUES ($1::uuid, 'generic_git', $2, $3, 'main')
		RETURNING id::text
	`, fixture.projectID, "https://example.invalid/"+slug+".git", slug).Scan(
		&fixture.repositoryID,
	); err != nil {
		t.Fatalf("create integration repository: %v", err)
	}

	return fixture
}

func (f *operationStoreTestFixture) create(
	key string,
	requestBody []byte,
) (createOperationResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	return createOperation(ctx, f.pool, createOperationInput{
		ProjectID:        f.projectID,
		RepositoryID:     f.repositoryID,
		RequestedBy:      f.requestedBy,
		IdempotencyScope: "integration-test",
		IdempotencyKey:   key,
		RequestBody:      requestBody,
	})
}

func (f *operationStoreTestFixture) counts(
	t *testing.T,
	key string,
) (operations int64, events int64, outbox int64) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := f.pool.QueryRow(ctx, `
		SELECT
			(
				SELECT count(*)
				FROM operations
				WHERE project_id = $1::uuid
				  AND requested_by = $2
				  AND idempotency_scope = 'integration-test'
				  AND idempotency_key = $3
			),
			(
				SELECT count(*)
				FROM operation_events
				WHERE project_id = $1::uuid
			),
			(
				SELECT count(*)
				FROM outbox_events
				WHERE project_id = $1::uuid
			)
	`, f.projectID, f.requestedBy, key).Scan(&operations, &events, &outbox)
	if err != nil {
		t.Fatalf("count operation-store records: %v", err)
	}

	return operations, events, outbox
}

func buildOperationRequestForTest(revision string) []byte {
	return []byte(
		`{"operationType":"build","sourceRevision":"` + revision + `"}`,
	)
}

func assertOperationStoreCounts(
	t *testing.T,
	fixture *operationStoreTestFixture,
	key string,
	wantOperations int64,
	wantEvents int64,
	wantOutbox int64,
) {
	t.Helper()

	operations, events, outbox := fixture.counts(t, key)
	if operations != wantOperations || events != wantEvents || outbox != wantOutbox {
		t.Fatalf(
			"database counts = operations:%d events:%d outbox:%d; want operations:%d events:%d outbox:%d",
			operations,
			events,
			outbox,
			wantOperations,
			wantEvents,
			wantOutbox,
		)
	}
}

func TestCreateOperationIntegrationPersistsOperationAndOutbox(t *testing.T) {
	fixture := newOperationStoreTestFixture(t)
	const key = "create-once"
	revision := strings.Repeat("a", 40)

	result, err := fixture.create(key, buildOperationRequestForTest(revision))
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	if result.Replayed {
		t.Fatal("new operation was marked as replayed")
	}
	if result.Operation.ID == "" {
		t.Fatal("created operation has an empty ID")
	}
	if result.Operation.ProjectID != fixture.projectID {
		t.Errorf("project ID = %q, want %q", result.Operation.ProjectID, fixture.projectID)
	}
	if result.Operation.RepositoryID != fixture.repositoryID {
		t.Errorf("repository ID = %q, want %q", result.Operation.RepositoryID, fixture.repositoryID)
	}
	if result.Operation.OperationType != operationTypeBuild {
		t.Errorf("operation type = %q, want %q", result.Operation.OperationType, operationTypeBuild)
	}
	if result.Operation.Status != operationStatusQueued {
		t.Errorf("operation status = %q, want %q", result.Operation.Status, operationStatusQueued)
	}
	if result.Operation.SourceRevision == nil || *result.Operation.SourceRevision != revision {
		t.Errorf("source revision = %v, want %q", result.Operation.SourceRevision, revision)
	}
	if len(result.Operation.RequestFingerprint) != 64 {
		t.Errorf("request fingerprint length = %d, want 64", len(result.Operation.RequestFingerprint))
	}

	assertOperationStoreCounts(t, fixture, key, 1, 1, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var eventType, toStatus, actor string
	if err := fixture.pool.QueryRow(ctx, `
		SELECT event_type, to_status, actor_subject
		FROM operation_events
		WHERE project_id = $1::uuid AND operation_id = $2::uuid
	`, fixture.projectID, result.Operation.ID).Scan(&eventType, &toStatus, &actor); err != nil {
		t.Fatalf("read initial operation event: %v", err)
	}
	if eventType != "operation.queued" || toStatus != "queued" || actor != fixture.requestedBy {
		t.Errorf("initial event = (%q, %q, %q), want (operation.queued, queued, %q)", eventType, toStatus, actor, fixture.requestedBy)
	}

	var outboxType, dedupeKey, payloadID, payloadType string
	if err := fixture.pool.QueryRow(ctx, `
		SELECT event_type, dedupe_key, payload->>'operationId', payload->>'operationType'
		FROM outbox_events
		WHERE project_id = $1::uuid AND operation_id = $2::uuid
	`, fixture.projectID, result.Operation.ID).Scan(&outboxType, &dedupeKey, &payloadID, &payloadType); err != nil {
		t.Fatalf("read initial outbox event: %v", err)
	}
	if outboxType != "operation.queued" {
		t.Errorf("outbox event type = %q, want operation.queued", outboxType)
	}
	if dedupeKey != "operation:"+result.Operation.ID+":queued" {
		t.Errorf("outbox dedupe key = %q, unexpected value", dedupeKey)
	}
	if payloadID != result.Operation.ID || payloadType != string(operationTypeBuild) {
		t.Errorf("outbox payload = (%q, %q), want operation ID %q and type %q", payloadID, payloadType, result.Operation.ID, operationTypeBuild)
	}
}

func TestCreateOperationIntegrationReplaysEquivalentRequest(t *testing.T) {
	fixture := newOperationStoreTestFixture(t)
	const key = "replay-equivalent"
	revision := strings.Repeat("b", 40)

	first, err := fixture.create(key, buildOperationRequestForTest(revision))
	if err != nil {
		t.Fatalf("create initial operation: %v", err)
	}

	equivalentRequest := []byte(
		"{\n  \"sourceRevision\": \"" + revision + "\",\n  \"operationType\": \"build\"\n}",
	)
	second, err := fixture.create(key, equivalentRequest)
	if err != nil {
		t.Fatalf("replay equivalent request: %v", err)
	}

	if second.Operation.ID != first.Operation.ID {
		t.Errorf("replay ID = %q, want original ID %q", second.Operation.ID, first.Operation.ID)
	}
	if !second.Replayed {
		t.Error("equivalent request was not marked as replayed")
	}
	assertOperationStoreCounts(t, fixture, key, 1, 1, 1)
}

func TestCreateOperationIntegrationRejectsConflictingKeyReuse(t *testing.T) {
	fixture := newOperationStoreTestFixture(t)
	const key = "conflicting-reuse"

	firstRevision := strings.Repeat("c", 40)
	secondRevision := strings.Repeat("d", 40)
	if _, err := fixture.create(key, buildOperationRequestForTest(firstRevision)); err != nil {
		t.Fatalf("create initial operation: %v", err)
	}

	_, err := fixture.create(key, buildOperationRequestForTest(secondRevision))
	if !errors.Is(err, errIdempotencyConflict) {
		t.Fatalf("conflicting request error = %v, want %v", err, errIdempotencyConflict)
	}
	assertOperationStoreCounts(t, fixture, key, 1, 1, 1)
}

func TestCreateOperationIntegrationConcurrentDuplicateRequests(t *testing.T) {
	fixture := newOperationStoreTestFixture(t)
	const key = "concurrent-duplicate"
	request := buildOperationRequestForTest(strings.Repeat("e", 40))

	type outcome struct {
		result createOperationResult
		err    error
	}

	start := make(chan struct{})
	results := make(chan outcome, 2)
	for range 2 {
		go func() {
			<-start
			result, err := fixture.create(key, request)
			results <- outcome{result: result, err: err}
		}()
	}
	close(start)

	var operationID string
	createdCount := 0
	replayedCount := 0

	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent create operation: %v", result.err)
		}
		if operationID == "" {
			operationID = result.result.Operation.ID
		} else if result.result.Operation.ID != operationID {
			t.Errorf("concurrent request returned IDs %q and %q", operationID, result.result.Operation.ID)
		}

		if result.result.Replayed {
			replayedCount++
		} else {
			createdCount++
		}
	}

	if createdCount != 1 || replayedCount != 1 {
		t.Errorf("created count = %d, replayed count = %d; want 1 each", createdCount, replayedCount)
	}
	assertOperationStoreCounts(t, fixture, key, 1, 1, 1)
}

func TestCreateOperationIntegrationOutboxFailureRollsBackAllWrites(t *testing.T) {
	fixture := newOperationStoreTestFixture(t)
	installOutboxFailureTrigger(t, fixture)

	const key = "rollback-on-outbox-failure"
	_, err := fixture.create(key, buildOperationRequestForTest(strings.Repeat("f", 40)))
	if err == nil {
		t.Fatal("create operation error = nil, want simulated outbox insertion failure")
	}

	assertOperationStoreCounts(t, fixture, key, 0, 0, 0)
}

func installOutboxFailureTrigger(
	t *testing.T,
	fixture *operationStoreTestFixture,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dropTrigger := `DROP TRIGGER IF EXISTS nexvarin_test_fail_outbox_insert ON public.outbox_events`
	dropFunction := `DROP FUNCTION IF EXISTS public.nexvarin_test_fail_outbox_insert()`

	// Register cleanup before creating either database object so failed setup
	// also attempts to remove any partially created test objects.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()

		if _, err := fixture.pool.Exec(cleanupCtx, dropTrigger); err != nil {
			t.Errorf("drop outbox failure test trigger: %v", err)
		}
		if _, err := fixture.pool.Exec(cleanupCtx, dropFunction); err != nil {
			t.Errorf("drop outbox failure test function: %v", err)
		}
	})

	if _, err := fixture.pool.Exec(ctx, dropTrigger); err != nil {
		t.Fatalf("remove previous test trigger: %v", err)
	}
	if _, err := fixture.pool.Exec(ctx, dropFunction); err != nil {
		t.Fatalf("remove previous test function: %v", err)
	}

	createFunction := `
		CREATE FUNCTION public.nexvarin_test_fail_outbox_insert()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			IF NEW.event_type = 'operation.queued' THEN
				RAISE EXCEPTION 'simulated outbox insertion failure';
			END IF;
			RETURN NEW;
		END;
		$$
	`
	if _, err := fixture.pool.Exec(ctx, createFunction); err != nil {
		t.Fatalf("create outbox failure test function: %v", err)
	}

	createTrigger := `
		CREATE TRIGGER nexvarin_test_fail_outbox_insert
		BEFORE INSERT ON public.outbox_events
		FOR EACH ROW
		EXECUTE FUNCTION public.nexvarin_test_fail_outbox_insert()
	`
	if _, err := fixture.pool.Exec(ctx, createTrigger); err != nil {
		t.Fatalf("create outbox failure test trigger: %v", err)
	}
}
