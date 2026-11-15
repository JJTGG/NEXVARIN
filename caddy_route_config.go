package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
)

const (
	caddyRouteNamespaceID = "nexvarin-preview-route-namespace"
	caddyRouteIDPrefix    = "nexvarin-route-"
)

var (
	errCaddyRouteConfigInvalid = errors.New(
		"Caddy route configuration is invalid",
	)
	errCaddyRouteAnchorMissing = errors.New(
		"Caddy NEXVARIN namespace anchor is missing",
	)
	errCaddyRouteAnchorInvalid = errors.New(
		"Caddy NEXVARIN namespace anchor is invalid",
	)
	errCaddyRouteIdentityConflict = errors.New(
		"Caddy route identity conflict",
	)
	errCaddyRouteHostnameConflict = errors.New(
		"Caddy route hostname conflict",
	)
)

type caddyRouteSnapshot struct {
	ServerRoutesPath string
	ETag             string
	ServerRoutes     []json.RawMessage
	Namespace        caddyRouteNamespace
	Inventory        caddyRouteInventory
}

type caddyRouteNamespace struct {
	ParentRouteIndex int
	HandlerIndex     int
	RoutesPath       string
	Routes           []json.RawMessage
}

type caddyRouteConfig struct {
	ID              string
	Hostname        string
	UpstreamAddress string
}

type caddyRouteInventoryEntry struct {
	Index           int
	ID              string
	Hostname        string
	UpstreamAddress string
	Managed         bool
	MatchesAllHosts bool
	HostPatterns    []string
	Raw             json.RawMessage
}

type caddyRouteInventory struct {
	Entries    []caddyRouteInventoryEntry
	ByID       map[string]caddyRouteInventoryEntry
	ByHostname map[string]caddyRouteInventoryEntry
}

type caddyRouteJSON struct {
	ID     string                  `json:"@id"`
	Match  []caddyRouteMatcherJSON `json:"match"`
	Handle []caddyRouteHandlerJSON `json:"handle"`
}

type caddyRouteMatcherJSON struct {
	Host []string `json:"host"`
}

type caddyRouteHandlerJSON struct {
	Handler   string                   `json:"handler"`
	Upstreams []caddyRouteUpstreamJSON `json:"upstreams"`
}

type caddyRouteUpstreamJSON struct {
	Dial string `json:"dial"`
}

// caddyAdminServerRoutesPath builds the configuration path for one
// operator-selected HTTP server. Server names are configuration, never
// request data, and must remain a single safe path segment.
func caddyAdminServerRoutesPath(serverName string) (string, error) {
	if serverName == "" ||
		strings.TrimSpace(serverName) != serverName ||
		serverName == "." ||
		serverName == ".." {
		return "", fmt.Errorf(
			"%w: Caddy HTTP server name is invalid",
			errCaddyRouteConfigInvalid,
		)
	}

	for _, character := range serverName {
		if !isCaddyServerNameCharacter(character) {
			return "", fmt.Errorf(
				"%w: Caddy HTTP server name contains an unsupported character",
				errCaddyRouteConfigInvalid,
			)
		}
	}

	return "/config/apps/http/servers/" + serverName + "/routes", nil
}

func isCaddyServerNameCharacter(character rune) bool {
	return (character >= 'a' && character <= 'z') ||
		(character >= 'A' && character <= 'Z') ||
		(character >= '0' && character <= '9') ||
		character == '-' ||
		character == '_' ||
		character == '.'
}

// parseCaddyAdminServerRoutesResponse validates a complete server-routes
// response and discovers the NEXVARIN namespace using that exact snapshot.
// The ETag and all array indices returned here belong to this snapshot only.
func parseCaddyAdminServerRoutesResponse(
	response caddyAdminResponse,
	serverName string,
	previewSuffix string,
) (caddyRouteSnapshot, error) {
	var empty caddyRouteSnapshot

	routesPath, err := caddyAdminServerRoutesPath(serverName)
	if err != nil {
		return empty, err
	}

	if response.StatusCode != http.StatusOK {
		return empty, fmt.Errorf(
			"%w: Caddy server-routes request returned HTTP %d",
			errCaddyRouteConfigInvalid,
			response.StatusCode,
		)
	}

	if response.ETag == "" ||
		strings.TrimSpace(response.ETag) != response.ETag {
		return empty, fmt.Errorf(
			"%w: Caddy server-routes response has no usable ETag",
			errCaddyRouteConfigInvalid,
		)
	}

	var serverRoutes []json.RawMessage
	if err := json.Unmarshal(response.Body, &serverRoutes); err != nil {
		return empty, fmt.Errorf(
			"%w: decode Caddy server-routes array: %v",
			errCaddyRouteConfigInvalid,
			err,
		)
	}

	if serverRoutes == nil {
		return empty, fmt.Errorf(
			"%w: Caddy server-routes response is not a JSON array",
			errCaddyRouteConfigInvalid,
		)
	}

	namespace, err := discoverCaddyRouteNamespace(
		serverRoutes,
		routesPath,
		previewSuffix,
	)
	if err != nil {
		return empty, err
	}

	inventory, err := inspectCaddyRouteInventory(namespace.Routes)
	if err != nil {
		return empty, err
	}

	return caddyRouteSnapshot{
		ServerRoutesPath: routesPath,
		ETag:             response.ETag,
		ServerRoutes:     append([]json.RawMessage(nil), serverRoutes...),
		Namespace:        namespace,
		Inventory:        inventory,
	}, nil
}

// discoverCaddyRouteNamespace finds exactly one NEXVARIN anchor among the
// top-level server routes. It does not search or mutate other HTTP servers.
func discoverCaddyRouteNamespace(
	serverRoutes []json.RawMessage,
	serverRoutesPath string,
	previewSuffix string,
) (caddyRouteNamespace, error) {
	var empty caddyRouteNamespace

	suffix, err := normalizeDeploymentRoutePreviewSuffix(previewSuffix)
	if err != nil {
		return empty, fmt.Errorf(
			"%w: configured preview suffix is invalid",
			errCaddyRouteConfigInvalid,
		)
	}

	if serverRoutes == nil {
		return empty, fmt.Errorf(
			"%w: server routes must be a JSON array",
			errCaddyRouteConfigInvalid,
		)
	}

	totalAnchorIDs := 0
	var discovered []caddyRouteNamespace

	for routeIndex, routeRaw := range serverRoutes {
		routeFields, err := decodeCaddyRouteJSONObject(routeRaw)
		if err != nil {
			return empty, fmt.Errorf(
				"%w: server route at index %d is not an object",
				errCaddyRouteConfigInvalid,
				routeIndex,
			)
		}

		anchorCount, err := countCaddyRouteNamespaceIDs(routeRaw, 0)
		if err != nil {
			return empty, fmt.Errorf(
				"%w: inspect server route at index %d",
				errCaddyRouteConfigInvalid,
				routeIndex,
			)
		}
		if anchorCount == 0 {
			continue
		}

		totalAnchorIDs += anchorCount

		handleRaw, exists := routeFields["handle"]
		if !exists {
			continue
		}

		var handlers []json.RawMessage
		if err := json.Unmarshal(handleRaw, &handlers); err != nil ||
			handlers == nil {
			continue
		}

		directAnchorCount := 0

		for handlerIndex, handlerRaw := range handlers {
			handlerFields, err := decodeCaddyRouteJSONObject(handlerRaw)
			if err != nil {
				continue
			}

			idRaw, exists := handlerFields["@id"]
			if !exists {
				continue
			}

			var handlerID string
			if err := json.Unmarshal(idRaw, &handlerID); err != nil ||
				handlerID != caddyRouteNamespaceID {
				continue
			}

			directAnchorCount++

			handlerTypeRaw, exists := handlerFields["handler"]
			if !exists {
				continue
			}

			var handlerType string
			if err := json.Unmarshal(handlerTypeRaw, &handlerType); err != nil ||
				handlerType != "subroute" {
				continue
			}

			childRoutesRaw, exists := handlerFields["routes"]
			if !exists {
				continue
			}

			var childRoutes []json.RawMessage
			if err := json.Unmarshal(childRoutesRaw, &childRoutes); err != nil ||
				childRoutes == nil {
				continue
			}

			if err := validateCaddyRouteParentPreviewMatcher(
				routeFields,
				suffix,
			); err != nil {
				return empty, err
			}

			discovered = append(discovered, caddyRouteNamespace{
				ParentRouteIndex: routeIndex,
				HandlerIndex:     handlerIndex,
				RoutesPath: fmt.Sprintf(
					"%s/%d/handle/%d/routes",
					serverRoutesPath,
					routeIndex,
					handlerIndex,
				),
				Routes: append([]json.RawMessage(nil), childRoutes...),
			})
		}

		if directAnchorCount != 1 || anchorCount != 1 {
			return empty, fmt.Errorf(
				"%w: namespace ID is duplicated or is not a direct top-level route handler",
				errCaddyRouteAnchorInvalid,
			)
		}
	}

	if totalAnchorIDs == 0 {
		return empty, errCaddyRouteAnchorMissing
	}

	if totalAnchorIDs != 1 || len(discovered) != 1 {
		return empty, fmt.Errorf(
			"%w: expected exactly one valid namespace anchor",
			errCaddyRouteAnchorInvalid,
		)
	}

	return discovered[0], nil
}

func validateCaddyRouteParentPreviewMatcher(
	routeFields map[string]json.RawMessage,
	previewSuffix string,
) error {
	matchRaw, exists := routeFields["match"]
	if !exists {
		return fmt.Errorf(
			"%w: namespace parent route has no host matcher",
			errCaddyRouteAnchorInvalid,
		)
	}

	var matchers []json.RawMessage
	if err := json.Unmarshal(matchRaw, &matchers); err != nil ||
		matchers == nil {
		return fmt.Errorf(
			"%w: namespace parent route matchers are malformed",
			errCaddyRouteAnchorInvalid,
		)
	}

	hostMatcherCount := 0
	var parentHosts []string

	for _, matcherRaw := range matchers {
		matcherFields, err := decodeCaddyRouteJSONObject(matcherRaw)
		if err != nil {
			return fmt.Errorf(
				"%w: namespace parent matcher is malformed",
				errCaddyRouteAnchorInvalid,
			)
		}

		hostRaw, exists := matcherFields["host"]
		if !exists {
			continue
		}

		hostMatcherCount++

		var hosts []string
		if err := json.Unmarshal(hostRaw, &hosts); err != nil ||
			hosts == nil {
			return fmt.Errorf(
				"%w: namespace parent host matcher is malformed",
				errCaddyRouteAnchorInvalid,
			)
		}

		parentHosts = hosts
	}

	expectedHost := "*." + previewSuffix
	if hostMatcherCount != 1 ||
		len(parentHosts) != 1 ||
		parentHosts[0] != expectedHost {
		return fmt.Errorf(
			"%w: namespace parent host matcher does not match the configured preview suffix",
			errCaddyRouteAnchorInvalid,
		)
	}

	return nil
}

func decodeCaddyRouteJSONObject(
	raw json.RawMessage,
) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage

	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, errors.New("JSON value is not an object")
	}

	return fields, nil
}

func countCaddyRouteNamespaceIDs(
	raw json.RawMessage,
	depth int,
) (int, error) {
	if depth > 128 {
		return 0, errors.New("Caddy configuration nesting exceeds the inspection limit")
	}

	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, err
	}

	return countCaddyRouteNamespaceIDsInValue(value, depth)
}

func countCaddyRouteNamespaceIDsInValue(
	value any,
	depth int,
) (int, error) {
	if depth > 128 {
		return 0, errors.New("Caddy configuration nesting exceeds the inspection limit")
	}

	count := 0

	switch typed := value.(type) {
	case map[string]any:
		if id, ok := typed["@id"].(string); ok &&
			id == caddyRouteNamespaceID {
			count++
		}

		for _, child := range typed {
			childCount, err := countCaddyRouteNamespaceIDsInValue(
				child,
				depth+1,
			)
			if err != nil {
				return 0, err
			}
			count += childCount
		}

	case []any:
		for _, child := range typed {
			childCount, err := countCaddyRouteNamespaceIDsInValue(
				child,
				depth+1,
			)
			if err != nil {
				return 0, err
			}
			count += childCount
		}
	}

	return count, nil
}

// inspectCaddyRouteInventory parses the owned namespace without changing it.
// Unknown route identities are retained as foreign entries. Known NEXVARIN
// identities must have the exact supported route representation.
func inspectCaddyRouteInventory(
	routes []json.RawMessage,
) (caddyRouteInventory, error) {
	inventory := caddyRouteInventory{
		Entries:    make([]caddyRouteInventoryEntry, 0, len(routes)),
		ByID:       make(map[string]caddyRouteInventoryEntry),
		ByHostname: make(map[string]caddyRouteInventoryEntry),
	}

	for index, routeRaw := range routes {
		routeFields, err := decodeCaddyRouteJSONObject(routeRaw)
		if err != nil {
			return caddyRouteInventory{}, fmt.Errorf(
				"%w: namespace route at index %d is not an object",
				errCaddyRouteConfigInvalid,
				index,
			)
		}

		var routeID string
		if idRaw, exists := routeFields["@id"]; exists {
			if err := json.Unmarshal(idRaw, &routeID); err != nil ||
				routeID == "" {
				return caddyRouteInventory{}, fmt.Errorf(
					"%w: namespace route at index %d has an invalid identity",
					errCaddyRouteConfigInvalid,
					index,
				)
			}
		}

		hostPatterns, matchesAllHosts, err := inspectCaddyRouteHostPatterns(
			routeFields,
		)
		if err != nil {
			return caddyRouteInventory{}, fmt.Errorf(
				"%w: inspect namespace route at index %d",
				errCaddyRouteConfigInvalid,
				index,
			)
		}

		entry := caddyRouteInventoryEntry{
			Index:           index,
			ID:              routeID,
			MatchesAllHosts: matchesAllHosts,
			HostPatterns:    hostPatterns,
			Raw:             append(json.RawMessage(nil), routeRaw...),
		}

		if strings.HasPrefix(routeID, caddyRouteIDPrefix) {
			config, err := decodeCaddyManagedRoute(routeRaw)
			if err != nil {
				return caddyRouteInventory{}, fmt.Errorf(
					"%w: NEXVARIN route at index %d is malformed",
					errCaddyRouteConfigInvalid,
					index,
				)
			}

			entry.Managed = true
			entry.Hostname = config.Hostname
			entry.UpstreamAddress = config.UpstreamAddress

			if _, exists := inventory.ByID[config.ID]; exists {
				return caddyRouteInventory{}, fmt.Errorf(
					"%w: duplicate NEXVARIN route ID %q",
					errCaddyRouteIdentityConflict,
					config.ID,
				)
			}

			if _, exists := inventory.ByHostname[config.Hostname]; exists {
				return caddyRouteInventory{}, fmt.Errorf(
					"%w: duplicate NEXVARIN route hostname %q",
					errCaddyRouteHostnameConflict,
					config.Hostname,
				)
			}

			inventory.ByID[config.ID] = entry
			inventory.ByHostname[config.Hostname] = entry
		}

		inventory.Entries = append(inventory.Entries, entry)
	}

	return inventory, nil
}

func inspectCaddyRouteHostPatterns(
	routeFields map[string]json.RawMessage,
) ([]string, bool, error) {
	matchRaw, exists := routeFields["match"]
	if !exists {
		return nil, true, nil
	}

	var matchers []json.RawMessage
	if err := json.Unmarshal(matchRaw, &matchers); err != nil ||
		matchers == nil {
		return nil, false, errors.New("route matchers are malformed")
	}

	if len(matchers) == 0 {
		return nil, true, nil
	}

	var (
		hostPatterns []string
		hasHostMatch bool
		matchesAll   bool
	)

	for _, matcherRaw := range matchers {
		matcherFields, err := decodeCaddyRouteJSONObject(matcherRaw)
		if err != nil {
			return nil, false, errors.New("route matcher is not an object")
		}

		hostRaw, exists := matcherFields["host"]
		if !exists {
			matchesAll = true
			continue
		}

		hasHostMatch = true

		var hosts []string
		if err := json.Unmarshal(hostRaw, &hosts); err != nil ||
			hosts == nil {
			return nil, false, errors.New("route host matcher is malformed")
		}

		if len(hosts) == 0 {
			matchesAll = true
			continue
		}

		for _, host := range hosts {
			normalized := strings.ToLower(host)
			if strings.TrimSpace(host) != host ||
				!validCaddyRouteHostPattern(normalized) {
				// Unknown host syntax cannot safely be proven disjoint
				// from a desired preview hostname.
				matchesAll = true
				continue
			}

			hostPatterns = append(hostPatterns, normalized)
		}
	}

	if !hasHostMatch {
		matchesAll = true
	}

	return hostPatterns, matchesAll, nil
}

func validCaddyRouteHostPattern(pattern string) bool {
	if pattern == "*" {
		return true
	}

	if strings.HasPrefix(pattern, "*.") {
		return validDeploymentRouteDNSName(strings.TrimPrefix(pattern, "*."))
	}

	return validDeploymentRouteDNSName(pattern)
}

// validateExpectedRoute checks whether a desired route can safely coexist
// with the current namespace inventory. An existing matching route ID may
// have an obsolete upstream; that is drift, not an identity conflict.
func (inventory caddyRouteInventory) validateExpectedRoute(
	expected caddyRouteConfig,
) error {
	if existing, exists := inventory.ByID[expected.ID]; exists &&
		existing.Hostname != expected.Hostname {
		return fmt.Errorf(
			"%w: route ID %q is already associated with hostname %q",
			errCaddyRouteIdentityConflict,
			expected.ID,
			existing.Hostname,
		)
	}

	for _, entry := range inventory.Entries {
		if entry.ID == expected.ID {
			continue
		}

		if entry.MatchesAllHosts {
			return fmt.Errorf(
				"%w: namespace route at index %d may match hostname %q",
				errCaddyRouteHostnameConflict,
				entry.Index,
				expected.Hostname,
			)
		}

		for _, pattern := range entry.HostPatterns {
			if caddyRouteHostPatternOverlaps(
				pattern,
				expected.Hostname,
			) {
				return fmt.Errorf(
					"%w: namespace route at index %d may already claim hostname %q",
					errCaddyRouteHostnameConflict,
					entry.Index,
					expected.Hostname,
				)
			}
		}
	}

	return nil
}

func caddyRouteHostPatternOverlaps(
	pattern string,
	hostname string,
) bool {
	pattern = strings.ToLower(pattern)
	hostname = strings.ToLower(hostname)

	if pattern == "*" || pattern == hostname {
		return true
	}

	if strings.HasPrefix(pattern, "*.") {
		suffix := strings.TrimPrefix(pattern, "*.")
		return hostname != suffix &&
			strings.HasSuffix(hostname, "."+suffix)
	}

	return false
}

// newCaddyRouteConfig derives the stable Caddy route identity from the
// immutable database route UUID. The hostname and upstream must already
// represent the current verified deployment state.
func newCaddyRouteConfig(
	route deploymentRouteRecord,
	upstreamAddress string,
	previewSuffix string,
) (caddyRouteConfig, error) {
	var empty caddyRouteConfig

	routeID, err := normalizeOperationUUID(route.ID)
	if err != nil || routeID != route.ID {
		return empty, fmt.Errorf(
			"%w: deployment route UUID is invalid or not canonical",
			errCaddyRouteConfigInvalid,
		)
	}

	suffix, err := normalizeDeploymentRoutePreviewSuffix(previewSuffix)
	if err != nil {
		return empty, fmt.Errorf(
			"%w: configured preview suffix is invalid",
			errCaddyRouteConfigInvalid,
		)
	}

	if !validCaddyPreviewHostname(route.Hostname, suffix) {
		return empty, fmt.Errorf(
			"%w: deployment route hostname is not beneath the configured preview suffix",
			errCaddyRouteConfigInvalid,
		)
	}

	if !validCaddyRouteUpstreamAddress(upstreamAddress) {
		return empty, fmt.Errorf(
			"%w: deployment upstream is not a canonical 127.0.0.1 host-port address",
			errCaddyRouteConfigInvalid,
		)
	}

	return caddyRouteConfig{
		ID:              caddyRouteIDPrefix + routeID,
		Hostname:        route.Hostname,
		UpstreamAddress: upstreamAddress,
	}, nil
}

func validCaddyPreviewHostname(
	hostname string,
	previewSuffix string,
) bool {
	return hostname != "" &&
		strings.TrimSpace(hostname) == hostname &&
		hostname == strings.ToLower(hostname) &&
		validDeploymentRouteDNSName(hostname) &&
		strings.HasSuffix(hostname, "."+previewSuffix)
}

func validCaddyRouteUpstreamAddress(value string) bool {
	if value == "" || strings.TrimSpace(value) != value {
		return false
	}

	host, portText, err := net.SplitHostPort(value)
	if err != nil || host != "127.0.0.1" {
		return false
	}

	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return false
	}

	return net.JoinHostPort(host, strconv.Itoa(port)) == value
}

// marshalCaddyRouteConfig creates the only route shape NEXVARIN is allowed
// to install. Unknown Caddy fields are never copied into this object.
func marshalCaddyRouteConfig(
	config caddyRouteConfig,
) ([]byte, error) {
	if !strings.HasPrefix(config.ID, caddyRouteIDPrefix) ||
		!validDeploymentRouteDNSName(config.Hostname) ||
		!validCaddyRouteUpstreamAddress(config.UpstreamAddress) {
		return nil, fmt.Errorf(
			"%w: expected Caddy route configuration is invalid",
			errCaddyRouteConfigInvalid,
		)
	}

	route := caddyRouteJSON{
		ID: config.ID,
		Match: []caddyRouteMatcherJSON{
			{
				Host: []string{config.Hostname},
			},
		},
		Handle: []caddyRouteHandlerJSON{
			{
				Handler: "reverse_proxy",
				Upstreams: []caddyRouteUpstreamJSON{
					{
						Dial: config.UpstreamAddress,
					},
				},
			},
		},
	}

	body, err := json.Marshal(route)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: encode expected Caddy route configuration: %v",
			errCaddyRouteConfigInvalid,
			err,
		)
	}

	return body, nil
}

// decodeCaddyManagedRoute accepts only the exact NEXVARIN route structure.
// Additional matchers, handlers, upstreams, or unrelated fields are rejected.
func decodeCaddyManagedRoute(
	raw json.RawMessage,
) (caddyRouteConfig, error) {
	var empty caddyRouteConfig

	routeFields, err := decodeCaddyRouteJSONObject(raw)
	if err != nil ||
		!caddyRouteJSONObjectHasExactKeys(
			routeFields,
			"@id",
			"match",
			"handle",
		) {
		return empty, errCaddyRouteConfigInvalid
	}

	var routeID string
	if err := json.Unmarshal(routeFields["@id"], &routeID); err != nil ||
		!strings.HasPrefix(routeID, caddyRouteIDPrefix) {
		return empty, errCaddyRouteConfigInvalid
	}

	uuid := strings.TrimPrefix(routeID, caddyRouteIDPrefix)
	normalizedUUID, err := normalizeOperationUUID(uuid)
	if err != nil || normalizedUUID != uuid ||
		routeID != caddyRouteIDPrefix+normalizedUUID {
		return empty, errCaddyRouteConfigInvalid
	}

	var matchers []json.RawMessage
	if err := json.Unmarshal(routeFields["match"], &matchers); err != nil ||
		len(matchers) != 1 {
		return empty, errCaddyRouteConfigInvalid
	}

	matcherFields, err := decodeCaddyRouteJSONObject(matchers[0])
	if err != nil ||
		!caddyRouteJSONObjectHasExactKeys(matcherFields, "host") {
		return empty, errCaddyRouteConfigInvalid
	}

	var hosts []string
	if err := json.Unmarshal(matcherFields["host"], &hosts); err != nil ||
		len(hosts) != 1 {
		return empty, errCaddyRouteConfigInvalid
	}

	hostname := hosts[0]
	if hostname != strings.ToLower(hostname) ||
		!validDeploymentRouteDNSName(hostname) {
		return empty, errCaddyRouteConfigInvalid
	}

	var handlers []json.RawMessage
	if err := json.Unmarshal(routeFields["handle"], &handlers); err != nil ||
		len(handlers) != 1 {
		return empty, errCaddyRouteConfigInvalid
	}

	handlerFields, err := decodeCaddyRouteJSONObject(handlers[0])
	if err != nil ||
		!caddyRouteJSONObjectHasExactKeys(
			handlerFields,
			"handler",
			"upstreams",
		) {
		return empty, errCaddyRouteConfigInvalid
	}

	var handlerType string
	if err := json.Unmarshal(handlerFields["handler"], &handlerType); err != nil ||
		handlerType != "reverse_proxy" {
		return empty, errCaddyRouteConfigInvalid
	}

	var upstreams []json.RawMessage
	if err := json.Unmarshal(handlerFields["upstreams"], &upstreams); err != nil ||
		len(upstreams) != 1 {
		return empty, errCaddyRouteConfigInvalid
	}

	upstreamFields, err := decodeCaddyRouteJSONObject(upstreams[0])
	if err != nil ||
		!caddyRouteJSONObjectHasExactKeys(upstreamFields, "dial") {
		return empty, errCaddyRouteConfigInvalid
	}

	var upstreamAddress string
	if err := json.Unmarshal(upstreamFields["dial"], &upstreamAddress); err != nil ||
		!validCaddyRouteUpstreamAddress(upstreamAddress) {
		return empty, errCaddyRouteConfigInvalid
	}

	return caddyRouteConfig{
		ID:              routeID,
		Hostname:        hostname,
		UpstreamAddress: upstreamAddress,
	}, nil
}

func caddyRouteJSONObjectHasExactKeys(
	fields map[string]json.RawMessage,
	keys ...string,
) bool {
	if len(fields) != len(keys) {
		return false
	}

	for _, key := range keys {
		if _, exists := fields[key]; !exists {
			return false
		}
	}

	return true
}
