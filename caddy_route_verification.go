package main

import (
	"context"
	"errors"
	"fmt"
)

var errCaddyRouteVerificationMismatch = errors.New(
	"Caddy route read-back does not match the desired state",
)

// VerifyRouteActive performs a fresh Caddy Admin API read and returns nil
// only when the exact NEXVARIN route is present in the active configuration.
//
// The existing snapshot parser validates the unique namespace anchor,
// exact managed-route structure, hostname matcher, reverse proxy handler,
// and single upstream. Inventory validation additionally rejects duplicate
// identities and conflicting routes within the namespace.
//
// This method can also be called after an uncertain mutation outcome. It
// never submits a mutation or treats a successful HTTP response as proof
// that a route is active.
func (mutator caddyRouteMutator) VerifyRouteActive(
	ctx context.Context,
	expected caddyRouteConfig,
) error {
	suffix, err := mutator.validateVerificationRoute(expected)
	if err != nil {
		return err
	}

	snapshot, err := mutator.readVerificationSnapshot(ctx, suffix)
	if err != nil {
		return err
	}

	if err := snapshot.Inventory.validateExpectedRoute(expected); err != nil {
		return err
	}

	actual, exists := snapshot.Inventory.ByID[expected.ID]
	if !exists {
		return fmt.Errorf(
			"%w: expected route ID %q is absent from the active namespace",
			errCaddyRouteVerificationMismatch,
			expected.ID,
		)
	}

	if !actual.Managed ||
		actual.ID != expected.ID ||
		actual.Hostname != expected.Hostname ||
		actual.UpstreamAddress != expected.UpstreamAddress {
		return fmt.Errorf(
			"%w: route %q does not match its expected hostname and upstream",
			errCaddyRouteVerificationMismatch,
			expected.ID,
		)
	}

	hostnameEntry, exists := snapshot.Inventory.ByHostname[expected.Hostname]
	if !exists || hostnameEntry.ID != expected.ID {
		return fmt.Errorf(
			"%w: hostname %q is not uniquely associated with route %q",
			errCaddyRouteVerificationMismatch,
			expected.Hostname,
			expected.ID,
		)
	}

	return nil
}

// VerifyRouteRemoved performs a fresh Caddy Admin API read and returns nil
// only when the expected stable route ID and hostname are absent from the
// active NEXVARIN namespace.
//
// Inventory validation rejects duplicate route identities, duplicate
// managed hostnames, and foreign routes whose matchers could still claim
// the expected hostname. A successful HTTP DELETE response alone is not
// accepted as evidence of removal.
func (mutator caddyRouteMutator) VerifyRouteRemoved(
	ctx context.Context,
	routeID string,
	hostname string,
) error {
	suffix, err := normalizeDeploymentRoutePreviewSuffix(
		mutator.PreviewSuffix,
	)
	if err != nil {
		return fmt.Errorf(
			"%w: configured preview suffix is invalid",
			errCaddyRouteConfigInvalid,
		)
	}

	if err := validateCaddyRouteMutationIdentity(
		routeID,
		hostname,
		suffix,
	); err != nil {
		return err
	}

	snapshot, err := mutator.readVerificationSnapshot(ctx, suffix)
	if err != nil {
		return err
	}

	expected := caddyRouteConfig{
		ID:       routeID,
		Hostname: hostname,
	}

	if err := snapshot.Inventory.validateExpectedRoute(expected); err != nil {
		return err
	}

	if _, exists := snapshot.Inventory.ByID[routeID]; exists {
		return fmt.Errorf(
			"%w: route ID %q is still present in the active namespace",
			errCaddyRouteVerificationMismatch,
			routeID,
		)
	}

	if _, exists := snapshot.Inventory.ByHostname[hostname]; exists {
		return fmt.Errorf(
			"%w: hostname %q is still present in the active namespace",
			errCaddyRouteVerificationMismatch,
			hostname,
		)
	}

	return nil
}

func (mutator caddyRouteMutator) validateVerificationRoute(
	expected caddyRouteConfig,
) (string, error) {
	suffix, err := normalizeDeploymentRoutePreviewSuffix(
		mutator.PreviewSuffix,
	)
	if err != nil {
		return "", fmt.Errorf(
			"%w: configured preview suffix is invalid",
			errCaddyRouteConfigInvalid,
		)
	}

	if err := validateCaddyRouteMutationIdentity(
		expected.ID,
		expected.Hostname,
		suffix,
	); err != nil {
		return "", err
	}

	if !validCaddyRouteUpstreamAddress(expected.UpstreamAddress) {
		return "", fmt.Errorf(
			"%w: expected upstream is not a canonical loopback address",
			errCaddyRouteConfigInvalid,
		)
	}

	return suffix, nil
}

// readVerificationSnapshot obtains a new active-configuration snapshot.
// A failed or ambiguous read-back cannot establish the external state,
// so it is classified as an unknown outcome for the caller.
func (mutator caddyRouteMutator) readVerificationSnapshot(
	ctx context.Context,
	previewSuffix string,
) (caddyRouteSnapshot, error) {
	var empty caddyRouteSnapshot

	if ctx == nil {
		return empty, fmt.Errorf(
			"%w: request context is required",
			errCaddyAdminInvalidRequest,
		)
	}

	snapshot, err := mutator.readSnapshot(ctx, previewSuffix)
	if err != nil {
		return empty, fmt.Errorf(
			"%w: unable to establish active Caddy route state: %v",
			errCaddyAdminOutcomeUnknown,
			err,
		)
	}

	return snapshot, nil
}
