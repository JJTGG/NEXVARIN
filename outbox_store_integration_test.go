package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func createOutboxTestEvent(t *testing.T) (
	*operationStoreTestFixture,
	string,
) {
	t.Helper()

	fixture := newOperationStoreTestFixture(t)
	result, err := fixture.create(
		"outbox-"+strings.ToLower(t.Name()),
		buildOperationRequestForTest(strings.Repeat("a", 40)),
	)
	if err != nil {
		t.Fatalf("create operation and outbox event: %v", err)
	}

	return fixture, result.Operation.ID
}

func TestClaimOutboxEventsLeasesPendingEvent(t *testing.T) {
	fixture, operationID := createOutboxTestEvent(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	events, err := claimOutboxEvents(
		ctx,
		fixture.pool,
		"worker-test",
		10,
		3,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim outbox events: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("claimed %d events, want 1", len(events))
	}

	event := events[0]
	if event.OperationID != operationID {
		t.Errorf("operation ID = %q, want %q", event.OperationID, operationID)
	}
	if event.AttemptCount != 1 {
		t.Errorf("attempt count = %d, want 1", event.AttemptCount)
	}
	if !strings.HasPrefix(event.LeaseOwner, "worker-test:") {
		t.Errorf("lease owner %q does not contain worker token", event.LeaseOwner)
	}
	if !event.LeaseExpiresAt.After(time.Now()) {
		t.Error("lease expiration should be in the future")
	}
	if len(event.Payload) == 0 {
		t.Error("claimed event payload is empty")
	}
}

func TestClaimOutboxEventsDoesNotClaimSameActiveLeaseTwice(t *testing.T) {
	fixture, _ := createOutboxTestEvent(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	first, err := claimOutboxEvents(
		ctx,
		fixture.pool,
		"worker-one",
		10,
		3,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("first claim returned %d events, want 1", len(first))
	}

	second, err := claimOutboxEvents(
		ctx,
		fixture.pool,
		"worker-two",
		10,
		3,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("second claim returned %d events, want 0", len(second))
	}
}

func TestAcknowledgeOutboxEventRequiresCurrentLease(t *testing.T) {
	fixture, _ := createOutboxTestEvent(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	events, err := claimOutboxEvents(
		ctx,
		fixture.pool,
		"worker-ack",
		10,
		3,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim event: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("claimed %d events, want 1", len(events))
	}

	event := events[0]

	if err := acknowledgeOutboxEvent(
		ctx,
		fixture.pool,
		event.ID,
		"wrong-worker-token",
	); !errors.Is(err, errOutboxLeaseNotOwned) {
		t.Fatalf("wrong lease error = %v, want lease-not-owned", err)
	}

	if err := acknowledgeOutboxEvent(
		ctx,
		fixture.pool,
		event.ID,
		event.LeaseOwner,
	); err != nil {
		t.Fatalf("acknowledge event with current lease: %v", err)
	}

	var delivered bool
	var leaseOwner *string
	err = fixture.pool.QueryRow(ctx, `
		SELECT delivered_at IS NOT NULL, lease_owner
		FROM outbox_events
		WHERE id = $1::uuid
	`, event.ID).Scan(&delivered, &leaseOwner)
	if err != nil {
		t.Fatalf("read acknowledged event: %v", err)
	}

	if !delivered {
		t.Error("event was not marked delivered")
	}
	if leaseOwner != nil {
		t.Errorf("lease owner after acknowledgement = %q, want NULL", *leaseOwner)
	}
}

func TestRetryOutboxEventMakesItAvailableAgain(t *testing.T) {
	fixture, _ := createOutboxTestEvent(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	first, err := claimOutboxEvents(
		ctx,
		fixture.pool,
		"worker-retry",
		10,
		3,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("first claim returned %d events, want 1", len(first))
	}

	deadLettered, err := retryOrDeadLetterOutboxEvent(
		ctx,
		fixture.pool,
		first[0].ID,
		first[0].LeaseOwner,
		3,
		0,
		"temporary_failure",
	)
	if err != nil {
		t.Fatalf("schedule retry: %v", err)
	}
	if deadLettered {
		t.Fatal("event was dead-lettered before exhausting attempts")
	}

	second, err := claimOutboxEvents(
		ctx,
		fixture.pool,
		"worker-retry-again",
		10,
		3,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim retried event: %v", err)
	}
	if len(second) != 1 {
		t.Fatalf("retry claim returned %d events, want 1", len(second))
	}
	if second[0].ID != first[0].ID {
		t.Errorf("retried event ID = %q, want %q", second[0].ID, first[0].ID)
	}
	if second[0].AttemptCount != 2 {
		t.Errorf("attempt count = %d, want 2", second[0].AttemptCount)
	}
}

func TestRetryOutboxEventDeadLettersAtAttemptLimit(t *testing.T) {
	fixture, _ := createOutboxTestEvent(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	events, err := claimOutboxEvents(
		ctx,
		fixture.pool,
		"worker-final-attempt",
		10,
		1,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim event: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("claimed %d events, want 1", len(events))
	}

	deadLettered, err := retryOrDeadLetterOutboxEvent(
		ctx,
		fixture.pool,
		events[0].ID,
		events[0].LeaseOwner,
		1,
		time.Second,
		"permanent_failure",
	)
	if err != nil {
		t.Fatalf("dead-letter event: %v", err)
	}
	if !deadLettered {
		t.Fatal("event was not dead-lettered at its attempt limit")
	}

	var delivered bool
	var deadLetteredAt *time.Time
	var leaseOwner *string
	err = fixture.pool.QueryRow(ctx, `
		SELECT delivered_at IS NOT NULL, dead_lettered_at, lease_owner
		FROM outbox_events
		WHERE id = $1::uuid
	`, events[0].ID).Scan(&delivered, &deadLetteredAt, &leaseOwner)
	if err != nil {
		t.Fatalf("read dead-lettered event: %v", err)
	}

	if delivered {
		t.Error("dead-lettered event must not be marked delivered")
	}
	if deadLetteredAt == nil {
		t.Error("dead-letter timestamp is NULL")
	}
	if leaseOwner != nil {
		t.Errorf("lease owner after dead-lettering = %q, want NULL", *leaseOwner)
	}
}

func TestClaimOutboxEventsValidatesArguments(t *testing.T) {
	tests := []struct {
		name          string
		workerID      string
		limit         int
		maxAttempts   int
		leaseDuration time.Duration
	}{
		{
			name:          "empty worker",
			workerID:      "",
			limit:         1,
			maxAttempts:   3,
			leaseDuration: time.Minute,
		},
		{
			name:          "zero limit",
			workerID:      "worker",
			limit:         0,
			maxAttempts:   3,
			leaseDuration: time.Minute,
		},
		{
			name:          "limit too large",
			workerID:      "worker",
			limit:         1001,
			maxAttempts:   3,
			leaseDuration: time.Minute,
		},
		{
			name:          "zero attempts",
			workerID:      "worker",
			limit:         1,
			maxAttempts:   0,
			leaseDuration: time.Minute,
		},
		{
			name:          "lease too short",
			workerID:      "worker",
			limit:         1,
			maxAttempts:   3,
			leaseDuration: time.Nanosecond,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := claimOutboxEvents(
				context.Background(),
				nil,
				test.workerID,
				test.limit,
				test.maxAttempts,
				test.leaseDuration,
			)
			if err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
