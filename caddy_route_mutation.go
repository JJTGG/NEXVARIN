package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

var errCaddyRouteMutationPreconditionFailed = errors.New(
	"Caddy route mutation precondition failed",
)

type caddyRouteMutationKind string

const (
	caddyRouteMutationUnchanged caddyRouteMutationKind = "unchanged"
	caddyRouteMutationAdded     caddyRouteMutationKind = "added"
	caddyRouteMutationReplaced  caddyRouteMutationKind = "replaced"
	caddyRouteMutationRemoved   caddyRouteMutationKind = "removed"
)

type caddyRouteMutationResult struct {
	Kind       caddyRouteMutationKind
	Path       string
	StatusCode int
	Changed    bool
}

type caddyRouteMutator struct {
	Client        *caddyAdminClient
	ServerName    string
	PreviewSuffix string
}

// EnsureRoute installs the expected route when it is absent, or replaces
// an existing route when its stable identity and hostname match but its
// upstream has drifted.
//
// Each call reads one complete server-routes snapshot and uses only that
// snapshot's namespace path, array index, and ETag. It never retries a
// mutation. The caller must revalidate database state and obtain a fresh
// snapshot before attempting another mutation after a conflict or an
// uncertain outcome.
func (mutator caddyRouteMutator) EnsureRoute(
	ctx context.Context,
	expected caddyRouteConfig,
) (caddyRouteMutationResult, error) {
	var empty caddyRouteMutationResult

	suffix, body, err := mutator.validateExpectedRoute(expected)
	if err != nil {
		return empty, err
	}

	snapshot, err := mutator.readSnapshot(ctx, suffix)
	if err != nil {
		return empty, err
	}

	if err := snapshot.Inventory.validateExpectedRoute(expected); err != nil {
		return empty, err
	}

	method := http.MethodPost
	path := snapshot.Namespace.RoutesPath
	kind := caddyRouteMutationAdded

	if existing, exists := snapshot.Inventory.ByID[expected.ID]; exists {
		if existing.Hostname != expected.Hostname {
			return empty, fmt.Errorf(
				"%w: route ID %q belongs to hostname %q",
				errCaddyRouteIdentityConflict,
				expected.ID,
				existing.Hostname,
			)
		}

		if existing.UpstreamAddress == expected.UpstreamAddress {
			return caddyRouteMutationResult{
				Kind:    caddyRouteMutationUnchanged,
				Path:    path,
				Changed: false,
			}, nil
		}

		method = http.MethodPatch
		path = path + "/" + strconv.Itoa(existing.Index)
		kind = caddyRouteMutationReplaced
	}

	return mutator.submitMutation(
		ctx,
		method,
		path,
		body,
		snapshot.ETag,
		kind,
	)
}

// RemoveRoute removes only the route identified by the expected stable
// NEXVARIN route ID and hostname. It does not remove a different route
// that happens to use the same hostname.
func (mutator caddyRouteMutator) RemoveRoute(
	ctx context.Context,
	routeID string,
	hostname string,
) (caddyRouteMutationResult, error) {
	var empty caddyRouteMutationResult

	suffix, err := normalizeDeploymentRoutePreviewSuffix(
		mutator.PreviewSuffix,
	)
	if err != nil {
		return empty, fmt.Errorf(
			"%w: configured preview suffix is invalid",
			errCaddyRouteConfigInvalid,
		)
	}

	if err := validateCaddyRouteMutationIdentity(
		routeID,
		hostname,
		suffix,
	); err != nil {
		return empty, err
	}

	snapshot, err := mutator.readSnapshot(ctx, suffix)
	if err != nil {
		return empty, err
	}

	expected := caddyRouteConfig{
		ID:       routeID,
		Hostname: hostname,
	}

	if err := snapshot.Inventory.validateExpectedRoute(expected); err != nil {
		return empty, err
	}

	existing, exists := snapshot.Inventory.ByID[routeID]
	if !exists {
		return caddyRouteMutationResult{
			Kind:    caddyRouteMutationUnchanged,
			Path:    snapshot.Namespace.RoutesPath,
			Changed: false,
		}, nil
	}

	if existing.Hostname != hostname {
		return empty, fmt.Errorf(
			"%w: route ID %q belongs to hostname %q",
			errCaddyRouteIdentityConflict,
			routeID,
			existing.Hostname,
		)
	}

	path := snapshot.Namespace.RoutesPath +
		"/" +
		strconv.Itoa(existing.Index)

	return mutator.submitMutation(
		ctx,
		http.MethodDelete,
		path,
		nil,
		snapshot.ETag,
		caddyRouteMutationRemoved,
	)
}

func (mutator caddyRouteMutator) validateExpectedRoute(
	expected caddyRouteConfig,
) (string, []byte, error) {
	var emptyBody []byte

	suffix, err := normalizeDeploymentRoutePreviewSuffix(
		mutator.PreviewSuffix,
	)
	if err != nil {
		return "", emptyBody, fmt.Errorf(
			"%w: configured preview suffix is invalid",
			errCaddyRouteConfigInvalid,
		)
	}

	if err := validateCaddyRouteMutationIdentity(
		expected.ID,
		expected.Hostname,
		suffix,
	); err != nil {
		return "", emptyBody, err
	}

	if !validCaddyRouteUpstreamAddress(expected.UpstreamAddress) {
		return "", emptyBody, fmt.Errorf(
			"%w: expected upstream is not a canonical loopback address",
			errCaddyRouteConfigInvalid,
		)
	}

	body, err := marshalCaddyRouteConfig(expected)
	if err != nil {
		return "", emptyBody, err
	}

	return suffix, body, nil
}

func validateCaddyRouteMutationIdentity(
	routeID string,
	hostname string,
	previewSuffix string,
) error {
	if !strings.HasPrefix(routeID, caddyRouteIDPrefix) {
		return fmt.Errorf(
			"%w: route ID is not a NEXVARIN route identity",
			errCaddyRouteConfigInvalid,
		)
	}

	routeUUID := strings.TrimPrefix(routeID, caddyRouteIDPrefix)
	normalizedUUID, err := normalizeOperationUUID(routeUUID)
	if err != nil ||
		normalizedUUID != routeUUID ||
		routeID != caddyRouteIDPrefix+normalizedUUID {
		return fmt.Errorf(
			"%w: route ID is not canonical",
			errCaddyRouteConfigInvalid,
		)
	}

	if !validCaddyPreviewHostname(hostname, previewSuffix) {
		return fmt.Errorf(
			"%w: hostname is not beneath the configured preview suffix",
			errCaddyRouteConfigInvalid,
		)
	}

	return nil
}

func (mutator caddyRouteMutator) readSnapshot(
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

	if mutator.Client == nil {
		return empty, errCaddyAdminClientNotConfigured
	}

	serverRoutesPath, err := caddyAdminServerRoutesPath(
		mutator.ServerName,
	)
	if err != nil {
		return empty, err
	}

	response, err := mutator.Client.do(
		ctx,
		http.MethodGet,
		serverRoutesPath,
		nil,
		"",
	)
	if err != nil {
		return empty, err
	}

	return parseCaddyAdminServerRoutesResponse(
		response,
		mutator.ServerName,
		previewSuffix,
	)
}

func (mutator caddyRouteMutator) submitMutation(
	ctx context.Context,
	method string,
	path string,
	body []byte,
	etag string,
	kind caddyRouteMutationKind,
) (caddyRouteMutationResult, error) {
	var empty caddyRouteMutationResult

	response, err := mutator.Client.do(
		ctx,
		method,
		path,
		body,
		etag,
	)
	if err != nil {
		// In particular, preserve unknown-outcome errors from the
		// Admin API client. A timeout or server error must not cause
		// this mutation to be replayed automatically.
		return empty, err
	}

	if response.StatusCode == http.StatusPreconditionFailed {
		return empty, fmt.Errorf(
			"%w: Caddy rejected the snapshot ETag for %s %s",
			errCaddyRouteMutationPreconditionFailed,
			method,
			path,
		)
	}

	if response.StatusCode < http.StatusOK ||
		response.StatusCode >= http.StatusMultipleChoices {
		return empty, fmt.Errorf(
			"Caddy route mutation %s %s returned HTTP %d",
			method,
			path,
			response.StatusCode,
		)
	}

	return caddyRouteMutationResult{
		Kind:       kind,
		Path:       path,
		StatusCode: response.StatusCode,
		Changed:    true,
	}, nil
}
