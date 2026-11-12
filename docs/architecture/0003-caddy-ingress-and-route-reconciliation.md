
ADR 0003: Caddy Ingress and Route Reconciliation

Project: NEXVARIN

Decision: Host-managed Caddy with durable route reconciliation

Revision: 0.2

Date: 2026-11-12

Status: Proposed

Scope: Initial controlled preview ingress

## 1. Context

NEXVARIN deploys application containers through rootless Podman.

The runtime verifies application readiness and persists a canonical loopback upstream address in the deployment record.

The upstream address is distinct from the public preview address. A deployment becoming ready does not establish that a preview route exists or is reachable.

The database now includes a durable deployment route resource, introduced by migration `000006_add_deployment_routes.sql`. Route ownership, desired and observed generations, reconciliation leases, retry scheduling, and database-side invalidation when deployment eligibility changes are implemented.

The Go reconciliation store supports claiming due routes, renewing and validating leases, recording external observations, rejecting stale or ineligible convergence, and updating the deployment preview-address projection atomically with a route observation.

The initial implementation also establishes a hostname-only representation for `deployments.preview_address`. It does not establish a public URL scheme or prove DNS resolution, TLS availability, or external reachability.

The Caddy integration is not yet implemented. In particular, the repository does not yet contain the Caddy Admin API adapter, the dedicated ingress controller process, active-configuration read-back, or periodic Caddy drift detection. The current process entry point supports only the `api` and `worker` process kinds.

The database reconciliation store is therefore a foundation for the controller, not evidence that Caddy currently contains or serves a route. The controller must only record an active or removed observation after verifying the corresponding external configuration state.

Caddy is the selected initial ingress and TLS candidate. Its integration must preserve the separation between desired deployment state, observed runtime state, desired ingress state, and observed ingress state.

The ingress integration must recover safely after process restarts, uncertain API responses, configuration conflicts, lease expiration, and partial completion.

## 2. Architectural Decisions

### 2.1 Network Topology

Caddy will run as a host-managed Linux service in the host network namespace for the initial deployment vertical slice.

The initial Podman runtime publishes application ports exclusively on the host loopback interface.

Caddy will proxy to the exact verified upstream address persisted for the deployment, using the canonical `127.0.0.1:port` form.

Application ports must not be exposed on public host interfaces merely to make them reachable from Caddy.

An isolated Caddy container is not compatible with the current loopback upstream contract because its loopback interface refers to its own network namespace.

Changing Caddy to a containerized or otherwise isolated network topology requires a separate architecture decision and verified connectivity, isolation, and recovery tests.

### 2.2 Durable Route Resource

Ingress route state is represented independently from runtime deployment state.

The durable deployment route resource is stored in PostgreSQL.

The initial scope supports one preview route per deployment.

Each route has:

- A stable database identity.
- A stable Caddy route identity derived from the route UUID.
- An owning project and repository.
- An associated deployment.
- A canonical, uniquely allocated hostname.
- A desired route state.
- An observed route state.
- A monotonically increasing desired generation.
- An observed generation.
- Reconciliation attempt and scheduling metadata.
- An optional controller lease and last reconciliation error.

Route hostnames must be allocated by trusted NEXVARIN control-plane logic beneath an operator-configured preview suffix.

V1 must not accept arbitrary externally supplied hostnames as trusted route identities.

The database enforces one route per deployment and global hostname uniqueness. A hostname remains reserved while its route record exists, including when the route is inactive or removed. Route identity and hostname are immutable.

The initial route identifier convention is:

`nexvarin-route-<route-uuid>`

The route resource must not duplicate the deployment's upstream address as an independent source of truth. Its target is derived from the associated deployment record.

The deployment `preview_address` field is a nullable API-facing projection. It may be populated only after the current desired route generation has been confirmed active against Caddy and the associated deployment remains eligible.

The current representation contract for `preview_address` is:

- Store the canonical lowercase hostname only.
- Do not store `http://` or `https://`.
- Do not append a port, path, or query string.
- Do not treat the field as a complete public URL.
- Do not treat a non-null value alone as proof that Caddy currently serves the route.

The public URL scheme and final externally visible URL contract remain deferred.

The projection is updated in the same PostgreSQL transaction as the route observation. A confirmed current-generation activation may publish the hostname. A completed observation that does not justify a current active route clears it.

An unresolved or failed external mutation does not, by itself, prove that a previously confirmed route has disappeared. The last confirmed projection may therefore remain while reconciliation is unresolved. Consumers must not use `preview_address` as the sole source of truth for current route availability; observed state and generation must also be considered.

### 2.3 Route State and Activation Eligibility

Route desired state and observed state remain separate.

The initial desired states are:

`active`

`inactive`

The initial observed states are:

`pending`

`active`

`removing`

`removed`

`needs_reconciliation`

The desired generation advances when route desired state changes or when deployment eligibility inputs change.

The observed generation records the generation associated with the latest external observation persisted by the controller.

An observation may be factually correct for an older generation without proving convergence of the current desired generation. Such an observation must not be treated as current convergence. The generation mismatch must leave the route eligible for further reconciliation.

A route may be installed only when all of the following conditions hold:

- The route's desired state is `active`.
- The route hostname is allocated and validated.
- The associated deployment's `desired_state` is `active`.
- The associated deployment's `observed_state` is `ready`.
- The associated deployment has a non-null, valid `upstream_address`.
- The deployment has not been deleted.
- The route belongs to the expected project and repository.
- The upstream address used for the Caddy mutation matches the current verified upstream persisted for that deployment.

The initial upstream address format is canonical `127.0.0.1:port`.

Legacy ready deployments without a verified upstream address must never become routable.

Runtime readiness alone must never activate a Caddy route.

A route becomes observed active only after the controller has confirmed that the intended route identity, hostname, and exact upstream are present in Caddy's active configuration.

A successful database write or successful Caddy API response alone is insufficient evidence of route activation.

A route must not be marked converged for its current generation when:

- Its desired generation has advanced since the controller claim.
- Its desired state is no longer active.
- Its deployment is no longer eligible.
- Its current upstream differs from the upstream used for the attempted mutation.
- The intended route is missing from Caddy despite being required by current desired state.

These cases must remain eligible for reconciliation. A stale external observation may be recorded as factual state for its claimed generation, but it must not publish a current preview hostname.

### 2.4 Dedicated Ingress Controller

Caddy configuration must be managed by a dedicated NEXVARIN ingress controller process.

The dedicated process is not yet implemented. The current process entry point accepts only `api` and `worker`; an `ingress` process kind and its runtime wiring remain required.

The ingress controller is responsible for:

- Discovering routes requiring reconciliation.
- Claiming due routes through durable PostgreSQL leases.
- Rechecking current route and deployment state before external mutation.
- Validating route eligibility against durable database state.
- Creating, updating, and removing owned Caddy routes.
- Confirming applied configuration through active-configuration read-back.
- Recording observed route state.
- Maintaining the deployment preview-address projection.
- Recovering from uncertain Caddy API outcomes.
- Detecting and repairing configuration drift.

The general build and deployment execution worker must not receive access to the Caddy administration interface.

The ingress controller must not execute application builds or manage application containers.

The database remains the authoritative source for desired route state. Caddy's active configuration is observed external state.

The route reconciliation store uses durable database polling. Transactional outbox events may later be introduced to wake the controller, but they must not become the authoritative representation of route state.

A periodic reconciliation sweep must independently discover:

- Unapplied desired changes.
- Routes whose leases have expired.
- Routes whose observed generation differs from their desired generation.
- Missing or stale Caddy routes within the NEXVARIN-owned namespace.
- Configuration drift between PostgreSQL desired state and Caddy active state.

Duplicate event delivery must not create duplicate routes or unsafe repeated configuration changes.

### 2.5 Caddy Configuration Management

NEXVARIN will manage only a clearly owned route namespace within a preconfigured Caddy HTTP server.

Each route will have a stable Caddy route identifier derived from its durable NEXVARIN route identity.

The ingress controller must not replace the entire Caddy configuration as part of ordinary route activation or removal.

In particular, ordinary route operations must not use `POST /load` to overwrite the active configuration.

The controller must use targeted Caddy Admin API configuration operations.

Where supported by the pinned Caddy version, configuration mutations must use optimistic concurrency:

1. Read the relevant configuration scope and its ETag.
2. Prepare the intended route change from that returned state.
3. Submit the targeted mutation with the matching `If-Match` header.
4. On HTTP 412, read the latest configuration and recompute the change.
5. Do not blindly replay a mutation prepared against stale configuration.

On an uncertain timeout or lost response, inspect the active configuration before deciding whether to retry.

A successful API response alone is insufficient evidence that the expected route is durably represented in the control plane.

Caddy documents ETag and `If-Match` protection for concurrent `/config/` operations as experimental and subject to change. The implementation must therefore pin a tested Caddy version and include compatibility tests for ETag handling, targeted mutation semantics, and precondition failures.

If the expected concurrency protection is unavailable, the controller must not silently fall back to unguarded concurrent configuration writes.

The controller must never delete, replace, or rewrite Caddy configuration outside its explicitly owned route namespace.

### 2.6 Administration and Security Boundary

Caddy's administrative API is a privileged control surface capable of changing the active server configuration.

It must never be exposed through the public application ingress.

The initial deployment must expose Caddy administration through a permissioned Unix-domain socket rather than relying on an unrestricted local TCP administration endpoint.

Operating-system permissions must grant the dedicated ingress controller access to that socket.

The general deployment worker, application containers, and build workloads must not receive access to it.

The Caddy process and ingress controller must run under appropriately restricted service identities.

The ingress controller's database access should be limited to the records and state transitions required for route reconciliation.

An application workload must never be able to modify ingress configuration through the Caddy administration interface.

### 2.7 Activation, Deactivation, and Recovery

Route activation and removal are durable workflows, not incidental side effects of application deployment.

The database claim returned to a controller is a point-in-time snapshot. It is useful for planning reconciliation but is not authorization to perform an external mutation.

Before changing Caddy, the controller must recheck current route generation, desired state, deployment eligibility, and verified upstream. A stale claim must not authorize a new mutation.

For activation:

1. Persist the intended route identity and desired active state.
2. Claim the due route generation.
3. Recheck current route and deployment state.
4. Confirm that the associated deployment satisfies all route eligibility requirements.
5. Read the relevant owned Caddy configuration scope and its ETag.
6. Apply the route through a targeted Caddy configuration mutation.
7. Read back and verify the exact route identity, hostname, and upstream mapping in active Caddy configuration.
8. Record the active observation against the claimed generation.
9. In the same database transaction, persist the observed route state and populate `preview_address` with the canonical hostname only if the current generation and deployment eligibility still match.

The database success recorder cannot independently inspect Caddy. The controller is responsible for supplying an `active` observation only after external read-back has verified the intended route.

For deactivation:

1. Persist the desired inactive state before attempting removal.
2. Claim the resulting desired generation.
3. Recheck the current desired state and route ownership.
4. Remove only the corresponding NEXVARIN-owned route.
5. Confirm that the route is absent from active Caddy configuration.
6. Record the removed observation against the claimed generation.
7. Clear the `preview_address` projection in the same database transaction as the confirmed removal observation.

A route may also need removal when its associated deployment becomes ineligible, is stopped, or is deleted. A route must not remain intentionally active for a deployment that no longer satisfies the activation contract.

If route removal cannot be confirmed, the system must preserve the unresolved state and require reconciliation rather than reporting successful cleanup.

Routine application shutdown, deletion, or replacement must not intentionally proceed while its ingress route remains active.

An explicit emergency shutdown may override normal ordering when stopping a workload is necessary to contain an immediate security or operational threat. That action must be recorded and must leave the route marked for reconciliation.

The controller must handle uncertain Caddy API outcomes by observing active configuration before retrying.

A controller restart must reconcile durable desired route records against Caddy's active configuration.

Caddy's persisted configuration and restart-resume mechanism provide useful process recovery, but PostgreSQL remains authoritative for NEXVARIN's desired route state.

On startup, the controller must repair missing desired routes and remove stale routes within its owned namespace.

### 2.8 Configuration Ownership, Leases, and Concurrency

NEXVARIN must preserve Caddy configuration owned by the operator or other explicitly registered integrations.

Only the NEXVARIN-owned server or route namespace may be modified by the initial controller.

Route updates must be scoped to the affected route.

Configuration collisions must not cause unrelated routes to disappear.

If another administrator or process changes the configuration concurrently, the ingress controller must detect the conflict and reconcile from a fresh configuration snapshot.

A full configuration replacement is reserved for an explicit bootstrap or controlled recovery procedure, never ordinary per-route lifecycle operations.

PostgreSQL leases coordinate route claims among NEXVARIN controllers. They are not external fencing tokens understood by Caddy.

A lease does not prevent a Caddy request that was already issued from completing after the lease expires. Consequently, database lease ownership alone cannot guarantee external mutation ordering.

The controller must enforce the following safeguards:

- Serialize mutations that affect the same Caddy configuration scope.
- Check current lease ownership and desired generation immediately before an external mutation.
- Renew leases before operations that may take a significant amount of time.
- Use ETag and `If-Match` protection for the relevant Caddy configuration scope where supported.
- Recheck the latest desired route generation before persisting an observation.
- Never report a stale claim as current-generation convergence.
- After lease loss or an uncertain external result, read the active Caddy configuration and reconcile against the latest database state.
- Never assume that cancellation or timeout of an HTTP request proves the mutation did not complete.

ETag protection prevents a mutation based on a stale Caddy configuration snapshot. It does not replace database generation checks or controller serialization.

The database transaction that records an observation must lock the deployment row before the route row. Deployment update triggers also acquire the deployment row before updating its associated route. The controller's persistence path must preserve this lock order to avoid deadlocks with concurrent deployment eligibility changes.

Caddy configuration mutations and PostgreSQL state transitions cannot be made one atomic transaction. The controller must explicitly handle the interval between external mutation and database observation recording through read-back, generation validation, and recoverable reconciliation.

## 3. Deliberately Deferred Decisions

The following remain outside this decision:

- The final public preview domain and hostname format.
- Public DNS automation and domain ownership verification.
- The final public preview URL scheme and URL construction contract.
- Production TLS certificate issuer and deployment configuration.
- Multi-node or highly available ingress.
- Multiple independent Caddy instances.
- Arbitrary customer-owned domains.
- Public execution of untrusted workloads.
- Production availability and recovery objectives.
- Advanced traffic splitting, canary releases, and weighted routing.

These decisions must be revisited when implementation requirements justify them.

## 4. Initial Acceptance Boundary

The initial ingress implementation is not accepted until tests demonstrate that:

- Caddy runs in the host network namespace and can reach verified loopback upstreams.
- Application ports remain unpublished on public host interfaces.
- The Caddy administration interface is not publicly reachable.
- Only the dedicated ingress controller can access the administration socket.
- A route cannot be activated for an inactive, deleted, unready, or upstream-less deployment.
- Legacy ready records without upstream addresses remain ineligible.
- Hostname ownership and uniqueness are enforced durably.
- Route creation and removal are idempotent.
- Route claim leases prevent multiple controllers from concurrently owning the same unexpired claim.
- Expired leases can be reclaimed safely.
- Stale generations cannot be recorded as current-generation convergence.
- Concurrent deployment updates and reconciliation completion preserve the required database lock order.
- `preview_address` is published only for a confirmed current-generation active route.
- `preview_address` contains the canonical hostname only, without a URL scheme.
- Confirmed removal clears the preview-address projection.
- Ineligible, stale, or missing active-route observations do not publish a preview hostname.
- A failed or uncertain external operation does not get misrepresented as confirmed absence.
- The controller can update one route without overwriting unrelated Caddy configuration.
- Concurrent configuration changes are detected rather than silently overwritten.
- A Caddy HTTP 412 response causes a fresh read and recomputation, not a blind stale retry.
- An uncertain API response is reconciled against active configuration before another mutation is attempted.
- A request that completes after lease expiry cannot cause a stale generation to be reported as converged.
- Controller restarts recover pending route changes.
- Periodic drift detection repairs missing desired routes and stale NEXVARIN-owned routes without touching unrelated routes.
- Routine runtime cleanup does not report completion while an ingress route remains active.
- The preview-address projection and route observation state are updated consistently.

The PostgreSQL route schema, route reconciliation storage, lease handling, generation validation, and preview projection have integration coverage. These database-level tests do not satisfy the Caddy adapter or end-to-end acceptance requirements above.

## 5. Consequences

This decision introduces a separate ingress controller and durable route resource.

That increases the amount of control-plane state and testing required, but avoids coupling application deployment success to an external configuration mutation.

The implemented PostgreSQL foundation provides durable route identity, desired and observed generations, leases, retry scheduling, and conditional preview-address projection.

The remaining Caddy integration introduces a separate external consistency boundary. PostgreSQL and Caddy cannot participate in one atomic transaction, so the controller must use targeted mutations, read-back verification, generation checks, and recovery reconciliation.

The host-network topology preserves the existing verified loopback upstream contract and avoids exposing application ports publicly.

The dedicated administration boundary reduces the risk that build or deployment execution code can modify the public ingress configuration.

The architecture does not guarantee production security, public DNS correctness, TLS availability, or multi-tenant isolation without further implementation and testing.

## 6. Change Policy

Changes to the ingress topology, route ownership model, Caddy administration boundary, or reconciliation guarantees must be supported by implementation evidence or a demonstrated requirement.

Material changes require a subsequent architecture decision record.

Implementation details may be refined without changing this ADR when they preserve its route ownership, security, concurrency, and recovery guarantees.

## Governing Principle

A healthy application is not a live preview route.

A successful configuration request is not proof of a live preview route.

A non-null preview hostname is not proof of DNS resolution, TLS availability, or external reachability.

Durable desired state, current deployment eligibility, verified active configuration, and recoverable reconciliation must agree before NEXVARIN reports ingress activation.

## 7. References

Caddy Admin API:
https://caddyserver.com/docs/api

Caddy API tutorial:
https://caddyserver.com/docs/api-tutorial

Caddy global options and administration endpoint security:
https://caddyserver.com/docs/caddyfile/options

Caddy architecture:
https://caddyserver.com/docs/architecture
