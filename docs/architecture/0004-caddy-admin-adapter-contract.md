
Caddy Admin Adapter Contract

Project: NEXVARIN

Parent Decision: ADR 0003 — Caddy Ingress and Route Reconciliation

Revision: 0.1

Date: 2026-11-12

Status: Proposed

Scope: Initial Caddy Admin API integration

## 1. Purpose

This document defines the implementation contract between the NEXVARIN ingress controller and the Caddy Admin API.

It specifies:

- Caddy configuration ownership.
- The NEXVARIN route namespace.
- The Admin API communication boundary.
- Conditional configuration mutations.
- Route identity and verification.
- Concurrency and uncertain-outcome recovery.
- Integration and acceptance tests.

This contract implements the architecture established by ADR 0003. It does not replace or weaken that decision.

## 2. Core Invariants

The adapter must preserve these invariants:

1. PostgreSQL is authoritative for desired route state.
2. Caddy's active configuration is authoritative for observed external configuration.
3. NEXVARIN modifies only its explicitly owned route namespace.
4. No ordinary route operation replaces the entire Caddy configuration.
5. Every mutation is conditional on a previously observed configuration version.
6. Every successful mutation is followed by active-configuration read-back.
7. A database lease is not an external Caddy fencing token.
8. A route is not reported active until its exact hostname and upstream have been verified.
9. A stale generation cannot publish a current preview hostname.
10. Unknown or operator-owned configuration is never silently overwritten.

## 3. Caddy Configuration Ownership

### 3.1 Existing HTTP Server

NEXVARIN must operate inside an existing operator-configured Caddy HTTP server.

It must not create another server using listener addresses already assigned to the existing public HTTP or HTTPS server.

The operator or controlled bootstrap process owns:

- The HTTP server identity.
- Listener addresses.
- TLS configuration.
- Server-level options.
- The parent route position.
- The NEXVARIN namespace anchor.

NEXVARIN owns only the nested route array inside its reserved anchor.

### 3.2 Namespace Anchor

The initial bootstrap configuration must provide one reserved `subroute` handler with this stable identifier:

`nexvarin-preview-route-namespace`

Illustrative native Caddy JSON:

```json
{
  "match": [
    {
      "host": [
        "*.preview.example.test"
      ]
    }
  ],
  "handle": [
    {
      "handler": "subroute",
      "@id": "nexvarin-preview-route-namespace",
      "routes": []
    }
  ]
}
```

The preview suffix shown here is a test example, not a final public domain decision.

The parent route must be positioned by the operator so that the preview-host matcher is evaluated before any overlapping catch-all route.

The `subroute` handler's nested `routes` array is the exclusive NEXVARIN mutation namespace.

The controller must not:

- Create the namespace anchor automatically.
- Change its parent matcher.
- Move the parent route.
- Change its parent handler.
- Modify sibling handlers.
- Change the containing HTTP server's listeners.
- Remove or recreate the anchor during routine reconciliation.

At startup, the adapter must verify that:

- Exactly one anchor with the reserved ID exists.
- Its handler type is `subroute`.
- Its nested routes value is an array.
- Its parent host matcher is consistent with the configured preview suffix.
- The anchor is inside the configured HTTP server.

A missing, duplicated, malformed, or conflicting anchor is a configuration error.

The controller must fail closed and report the problem. It must not attempt to repair the operator-owned parent configuration.

## 4. NEXVARIN Route Representation

### 4.1 Stable Identity

Each route's stable Caddy identity is derived from its immutable PostgreSQL route UUID:

`nexvarin-route-<route-uuid>`

The identifier must not depend on a mutable array index.

The database hostname remains the authoritative hostname for that route.

### 4.2 Route Configuration

A route is represented by a Caddy HTTP route object containing:

- Its stable `@id`.
- An exact hostname matcher.
- A reverse proxy handler.
- Exactly one upstream dial address.

Illustrative JSON:

```json
{
  "@id": "nexvarin-route-00000000-0000-4000-8000-000000000001",
  "match": [
    {
      "host": [
        "app.preview.example.test"
      ]
    }
  ],
  "handle": [
    {
      "handler": "reverse_proxy",
      "upstreams": [
        {
          "dial": "127.0.0.1:31001"
        }
      ]
    }
  ]
}
```

The upstream must be obtained from the current verified deployment record.

It must pass the existing canonical loopback upstream validation.

The adapter must not accept an arbitrary user-supplied upstream address.

The route object must not introduce additional handlers, matchers, redirects, header transformations, or unrelated configuration.

### 4.3 Identity and Conflict Rules

Within the NEXVARIN namespace:

- A route ID must occur at most once.
- A hostname must be associated with at most one route ID.
- An existing route with the same ID and hostname but an obsolete upstream may be replaced with the expected NEXVARIN route object.
- An existing route ID associated with a different hostname is an identity conflict.
- A hostname used by a different route ID is a hostname conflict.
- Duplicate route identities or ambiguous matches must stop reconciliation.

The adapter must not take over or delete a route merely because it shares a hostname with a NEXVARIN route.

## 5. Admin API Communication

### 5.1 Transport

The initial adapter communicates with Caddy over a permissioned Unix-domain socket.

The Caddy configuration must use the Unix socket administration endpoint.

The ingress process must use a Go HTTP transport with a custom Unix-socket dialer.

The HTTP transport must:

- Connect only to the configured socket path.
- Disable environment-based HTTP proxies.
- Reject redirects.
- Apply bounded connection and request timeouts.
- Bound response-body reads.
- Preserve the exact ETag response header.
- Avoid automatic retries of mutating requests.
- Return structured status and transport errors.

The controller must not silently fall back to TCP when the socket is missing, inaccessible, or unavailable.

### 5.2 Socket Permissions

Caddy must be able to create and replace its configured socket path.

The ingress controller must have only the filesystem permissions required to connect to that socket.

The service configuration must account for the runtime directory lifecycle, service identities, and shared group permissions.

An empty file created manually is not a substitute for a socket bound by Caddy.

### 5.3 Configuration Path

The controller reads the configured server's routes array:

`/config/apps/http/servers/<server-name>/routes`

The server name is trusted process configuration, not request data.

The controller parses this response and locates the namespace anchor by its reserved `@id`.

It then locates the nested subroute handler and its child routes array.

Array indices are derived from the latest configuration snapshot. They must never be persisted as route identities.

## 6. Conditional Mutation Protocol

### 6.1 Read Snapshot

Before every mutation, the adapter must:

1. Request the configured server's routes array.
2. Capture the complete response body.
3. Capture the `ETag` response header.
4. Verify the ETag is present and usable.
5. Locate the unique NEXVARIN namespace anchor.
6. Locate the nested route array.
7. Recompute the required operation using the current route contents.

The ETag must be retained exactly as returned by Caddy.

The adapter must not construct, normalize, or synthesize an ETag.

A missing ETag is a fatal compatibility condition for mutation. The adapter must fail closed.

### 6.2 Add

A missing route is appended using Caddy's array append operation:

`POST <namespace-routes-path>/...`

The request body is the complete expected route object.

The request must include:

`If-Match: <ETag from the parent routes snapshot>`

### 6.3 Replace

An existing route with the correct stable identity and hostname but an obsolete upstream is replaced at its current array index:

`PATCH <namespace-routes-path>/<index>`

PATCH must send the complete replacement route object.

The request must include the ETag obtained from the snapshot used to calculate the index.

### 6.4 Remove

An existing route that must no longer exist is removed at its current array index:

`DELETE <namespace-routes-path>/<index>`

The request must include the ETag obtained from the same configuration snapshot used to locate that index.

### 6.5 No Full Configuration Replacement

The adapter must never use:

`POST /load`

for route creation, replacement, removal, or routine recovery.

It must never delete the entire HTTP server, the entire server routes array, or configuration outside the NEXVARIN namespace.

### 6.6 Precondition Failure

HTTP 412 means the ETag no longer matches the configuration scope.

On HTTP 412:

1. Do not retry the old mutation.
2. Discard the old ETag.
3. Discard every array index derived from the old snapshot.
4. Re-read the configuration.
5. Revalidate the database claim and current desired generation.
6. Recompute the required mutation.
7. Submit a new conditional mutation if still required.

The controller must bound repeated conflicts. The initial implementation should permit no more than three mutation attempts for one reconciliation pass.

If the conflict limit is reached, leave the route due for another reconciliation attempt and record an appropriate error.

## 7. External Mutation and Read-Back

A successful HTTP response is not sufficient evidence of route activation or removal.

After a mutation, the adapter must read the current NEXVARIN namespace again.

### 7.1 Activation Verification

An active observation is valid only when read-back confirms:

- Exactly one route with the expected stable route ID.
- Exactly one exact hostname matcher equal to the database hostname.
- The expected reverse proxy handler.
- Exactly one upstream.
- An upstream dial address exactly equal to the current verified deployment upstream.
- No conflicting route with the same hostname in the NEXVARIN namespace.

Only this verified state may be reported to the reconciliation store as `active`.

### 7.2 Removal Verification

A removed observation is valid only when read-back confirms:

- The expected route ID is absent from the NEXVARIN namespace.
- No duplicate route identity remains.
- No route with the same hostname remains in that namespace.

The controller must not infer removal from an HTTP 2xx response alone.

### 7.3 Uncertain Outcomes

Transport errors, request timeouts, connection resets, and server errors may leave the outcome uncertain.

The adapter must not blindly replay the mutation.

It must:

1. Read the current namespace configuration.
2. Inspect the target route by stable identity.
3. Compare the actual route against the exact desired route object.
4. Treat an exact match as a verified external state candidate.
5. If the target state is absent or different, revalidate the database claim and recompute from a fresh ETag.
6. If read-back is unavailable or ambiguous, return an unknown-outcome error and leave the route for later reconciliation.

A timed-out request may still complete after the client stops waiting.

All subsequent mutations must therefore use a fresh ETag. Concurrent requests based on the same obsolete ETag must not be replayed without recomputing the current state.

An uncertain outcome must never directly cause a preview-address projection update.

## 8. Database Coordination

The adapter does not own database state transitions.

The ingress controller coordinates the adapter with the existing deployment route reconciliation store.

Before each external mutation, the controller must revalidate:

- Lease ownership.
- Lease expiry.
- Current desired generation.
- Current route desired state.
- Current deployment eligibility.
- Current verified upstream address.

A lease renewal must be performed before operations that may consume a significant portion of the lease duration.

The controller must not hold PostgreSQL row locks across an external Caddy API call.

After verified read-back, the controller invokes the database reconciliation success recorder.

The store remains responsible for locking the deployment before the route and validating the current desired generation and deployment eligibility before recording convergence.

The store is also responsible for updating `preview_address` atomically with the confirmed route observation.

If the desired generation changes during an external operation, the observation must not be treated as convergence of the newer generation.

The controller must leave the route eligible for reconciliation.

A database lease is not an external fencing token. It cannot prevent an already-issued Caddy request from completing after lease expiry.

Conditional Caddy writes, current-generation checks, read-back, and durable reconciliation must work together to recover from this condition.

## 9. Error Classification

The adapter must distinguish at least:

- `caddy_unavailable`: Caddy cannot be reached.
- `caddy_config_conflict`: ETag precondition failed repeatedly.
- `caddy_config_anchor_missing`: the reserved namespace anchor is absent.
- `caddy_config_anchor_invalid`: the anchor does not have the expected structure.
- `caddy_route_identity_conflict`: a route ID is duplicated or associated with the wrong hostname.
- `caddy_route_hostname_conflict`: the hostname is already claimed by another route.
- `caddy_route_invalid`: the route object cannot be safely interpreted or verified.
- `caddy_outcome_unknown`: the external mutation outcome cannot be established.

Error codes must remain compatible with the repository's existing error-code validation rules.

The controller must not log the complete Caddy configuration or sensitive configuration values.

Logs should include route ID, desired generation, operation type, HTTP status, and a bounded error classification.

## 10. Drift Detection Boundary

The initial adapter must support reading the complete NEXVARIN-owned route array.

This is required for a later periodic inventory reconciliation.

The drift detector will compare:

- Durable PostgreSQL route records.
- Current deployment eligibility.
- Current desired route generation.
- The NEXVARIN-owned Caddy route inventory.

It may repair missing desired routes and remove orphaned NEXVARIN-owned route entries.

It must never remove routes outside the NEXVARIN namespace.

A route with an unknown or non-NEXVARIN identity must be preserved.

## 11. Version and Compatibility

The initial Caddy integration candidate is pinned to:

`v2.11.9`

The exact version must be verified by the integration test environment.

The release artifact must be obtained from the official Caddy release distribution and verified against its published checksums and signatures.

Caddy documents ETag and If-Match configuration concurrency protection as experimental.

The implementation must not assume that this behavior is a permanent API guarantee.

A Caddy version upgrade must run the full adapter compatibility suite before being accepted.

If the pinned version does not provide the required ETag and If-Match behavior, route mutations must remain disabled rather than silently falling back to unguarded writes.

## 12. Required Tests

The adapter is not accepted until tests demonstrate:

1. Unix-domain socket connectivity.
2. Failure when socket permissions deny access.
3. No TCP fallback.
4. Successful configuration snapshot retrieval.
5. ETag extraction and preservation.
6. Namespace anchor discovery by stable ID.
7. Failure on missing or duplicated anchors.
8. Route append through the nested route array.
9. Route replacement through the nested route array.
10. Route removal through the nested route array.
11. Correct If-Match behavior for all mutations.
12. A stale ETag produces HTTP 412.
13. A 412 causes a fresh read and recomputation.
14. Concurrent writers cannot silently overwrite each other.
15. Exact hostname and upstream verification.
16. Duplicate route identity detection.
17. Hostname conflict detection.
18. Uncertain mutation outcome recovery by read-back.
19. A delayed mutation cannot cause stale database convergence.
20. Operator-owned routes outside the namespace remain unchanged.
21. Caddy restart restores the intended persisted configuration.
22. An invalid or missing anchor causes the controller to fail closed.

The integration suite must execute against the exact pinned Caddy binary, not only a mocked HTTP server.

## 13. Implementation Sequence

Implementation proceeds in separate, reviewable changes:

1. Add the Caddy Admin API HTTP client and transport tests.
2. Add the pinned Caddy integration-test harness.
3. Add namespace discovery and route configuration validation.
4. Add conditional route mutation operations.
5. Add active-configuration read-back verification.
6. Add the controller orchestration and database coordination.
7. Add `ingress` process wiring to `main.go`.
8. Add periodic inventory drift detection.
9. Validate the full deployment-to-preview lifecycle.

Each implementation change must preserve the existing one-file-per-commit review boundary where practical.

## 14. References

Caddy Admin API:
https://caddyserver.com/docs/api

Caddy global options:
https://caddyserver.com/docs/caddyfile/options

Caddy HTTP server API:
https://pkg.go.dev/github.com/caddyserver/caddy/v2/modules/caddyhttp

Official Caddy releases:
https://github.com/caddyserver/caddy/releases

NEXVARIN ADR 0003:
docs/architecture/0003-caddy-ingress-and-route-reconciliation.md
