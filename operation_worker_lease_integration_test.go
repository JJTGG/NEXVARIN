package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func createAndClaimOperationForLeaseTest(
	t *testing.T,
	leaseDuration time.Duration,
) (*operationStoreTestFixture, claimedOperation) {
	t.Helper()

	fixture := newOperationStoreTestFixture(t)
	created, err := fixture.create(
		"lease-renew-"+strings.ToLower(t.Name()),
		buildOperationRequestForTest(strings.Repeat("a", 40)),
	)
	if err != nil {
		t.Fatalf("create operation for lease test: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	claimed, err := claimOperation(
		ctx,
		fixture.pool,
		created.Operation.ID,
		"worker-lease-test",
		leaseDuration,
	)
	if err != nil {
		t.Fatalf("claim operation for lease test: %v", err)
	}

	return fixture, claimed
}

func TestRenewOperationLeaseIntegrationExtendsLease(t *testing.T) {
	fixture, claimed := createAndClaimOperationForLeaseTest(
		t,
		30*time.Second,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	const renewalDuration = 2 * time.Minute

	renewedExpiry, err := renewOperationLease(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		claimed.LeaseOwner,
		renewalDuration,
	)
	if err != nil {
		t.Fatalf("renew operation lease: %v", err)
	}

	if !renewedExpiry.After(claimed.LeaseExpiresAt) {
		t.Errorf(
			"renewed expiry = %v, want after original expiry %v",
			renewedExpiry,
			claimed.LeaseExpiresAt,
		)
	}

	if !renewedExpiry.After(time.Now().Add(90 * time.Second)) {
		t.Errorf(
			"renewed expiry = %v, want at least 90 seconds from now",
			renewedExpiry,
		)
	}

	var status string
	var leaseOwner *string
	var leaseExpiresAt *time.Time

	err = fixture.pool.QueryRow(ctx, `
		SELECT status, lease_owner, lease_expires_at
		FROM operations
		WHERE id = $1::uuid
	`, claimed.Operation.ID).Scan(
		&status,
		&leaseOwner,
		&leaseExpiresAt,
	)
	if err != nil {
		t.Fatalf("read renewed operation lease: %v", err)
	}

	if status != string(operationStatusRunning) {
		t.Errorf("status = %q, want running", status)
	}

	if leaseOwner == nil || *leaseOwner != claimed.LeaseOwner {
		t.Errorf(
			"lease owner = %v, want original lease token",
			leaseOwner,
		)
	}

	if leaseExpiresAt == nil {
		t.Fatal("persisted lease expiry is NULL")
	}

	if !leaseExpiresAt.Equal(renewedExpiry) {
		t.Errorf(
			"persisted expiry = %v, returned expiry = %v",
			*leaseExpiresAt,
			renewedExpiry,
		)
	}
}

func TestRenewOperationLeaseIntegrationRejectsExpiredLease(
	t *testing.T,
) {
	fixture, claimed := createAndClaimOperationForLeaseTest(
		t,
		time.Minute,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	_, err := fixture.pool.Exec(ctx, `
		UPDATE operations
		SET lease_expires_at =
			clock_timestamp() - interval '1 second'
		WHERE id = $1::uuid
		  AND lease_owner = $2
	`, claimed.Operation.ID, claimed.LeaseOwner)
	if err != nil {
		t.Fatalf("expire operation lease: %v", err)
	}

	var expiredAt time.Time
	err = fixture.pool.QueryRow(ctx, `
		SELECT lease_expires_at
		FROM operations
		WHERE id = $1::uuid
	`, claimed.Operation.ID).Scan(&expiredAt)
	if err != nil {
		t.Fatalf("read expired lease: %v", err)
	}

	if _, err := renewOperationLease(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		claimed.LeaseOwner,
		time.Minute,
	); !errors.Is(err, errOperationLeaseNotOwned) {
		t.Fatalf(
			"renew expired lease error = %v, want lease-not-owned",
			err,
		)
	}

	var status string
	var leaseOwner *string
	var leaseExpiresAt time.Time

	err = fixture.pool.QueryRow(ctx, `
		SELECT status, lease_owner, lease_expires_at
		FROM operations
		WHERE id = $1::uuid
	`, claimed.Operation.ID).Scan(
		&status,
		&leaseOwner,
		&leaseExpiresAt,
	)
	if err != nil {
		t.Fatalf("read operation after rejected renewal: %v", err)
	}

	if status != string(operationStatusRunning) {
		t.Errorf("status = %q, want running", status)
	}

	if leaseOwner == nil || *leaseOwner != claimed.LeaseOwner {
		t.Errorf(
			"lease owner = %v, want original lease token",
			leaseOwner,
		)
	}

	if !leaseExpiresAt.Equal(expiredAt) {
		t.Errorf(
			"lease expiry changed after rejected renewal: before %v, after %v",
			expiredAt,
			leaseExpiresAt,
		)
	}
}

func TestRenewOperationLeaseIntegrationRejectsWrongOwner(
	t *testing.T,
) {
	fixture, claimed := createAndClaimOperationForLeaseTest(
		t,
		time.Minute,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	wrongOwner := claimed.LeaseOwner
	last := len(wrongOwner) - 1

	if wrongOwner[last] == '0' {
		wrongOwner = wrongOwner[:last] + "1"
	} else {
		wrongOwner = wrongOwner[:last] + "0"
	}

	if _, err := renewOperationLease(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		wrongOwner,
		time.Minute,
	); !errors.Is(err, errOperationLeaseNotOwned) {
		t.Fatalf(
			"renew with wrong owner error = %v, want lease-not-owned",
			err,
		)
	}

	var leaseOwner *string

	err := fixture.pool.QueryRow(ctx, `
		SELECT lease_owner
		FROM operations
		WHERE id = $1::uuid
	`, claimed.Operation.ID).Scan(&leaseOwner)
	if err != nil {
		t.Fatalf("read lease owner after rejected renewal: %v", err)
	}

	if leaseOwner == nil || *leaseOwner != claimed.LeaseOwner {
		t.Errorf(
			"persisted lease owner = %v, want original lease token",
			leaseOwner,
		)
	}
}

func TestRenewOperationLeaseIntegrationRejectsCompletedOperation(
	t *testing.T,
) {
	fixture, claimed := createAndClaimOperationForLeaseTest(
		t,
		time.Minute,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	completed, err := completeOperation(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		claimed.LeaseOwner,
	)
	if err != nil {
		t.Fatalf("complete operation: %v", err)
	}

	if completed.Status != operationStatusSucceeded {
		t.Fatalf(
			"completed status = %q, want succeeded",
			completed.Status,
		)
	}

	if _, err := renewOperationLease(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		claimed.LeaseOwner,
		time.Minute,
	); !errors.Is(err, errOperationOutcomeStateConflict) {
		t.Fatalf(
			"renew completed operation error = %v, want state conflict",
			err,
		)
	}

	var status string
	var leaseOwner *string
	var leaseExpiresAt *time.Time

	err = fixture.pool.QueryRow(ctx, `
		SELECT status, lease_owner, lease_expires_at
		FROM operations
		WHERE id = $1::uuid
	`, claimed.Operation.ID).Scan(
		&status,
		&leaseOwner,
		&leaseExpiresAt,
	)
	if err != nil {
		t.Fatalf("read completed operation: %v", err)
	}

	if status != string(operationStatusSucceeded) {
		t.Errorf("status = %q, want succeeded", status)
	}

	if leaseOwner != nil {
		t.Errorf("lease owner = %q, want NULL", *leaseOwner)
	}

	if leaseExpiresAt != nil {
		t.Errorf("lease expiry = %v, want NULL", *leaseExpiresAt)
	}
}

func TestRenewOperationLeaseIntegrationRejectsShortDuration(
	t *testing.T,
) {
	fixture, claimed := createAndClaimOperationForLeaseTest(
		t,
		time.Minute,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	_, err := renewOperationLease(
		ctx,
		fixture.pool,
		claimed.Operation.ID,
		claimed.LeaseOwner,
		time.Nanosecond,
	)
	if err == nil {
		t.Fatal("renew with sub-microsecond duration error = nil")
	}

	if !strings.Contains(err.Error(), "at least one microsecond") {
		t.Fatalf(
			"short duration error = %q, want minimum-duration validation",
			err,
		)
	}
}
