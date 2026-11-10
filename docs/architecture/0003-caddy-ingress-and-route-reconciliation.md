ADR 0003: Caddy Ingress and Route Reconciliation

Project: NEXVARIN

Decision: Host-managed Caddy with durable route reconciliation

Revision: 0.1

Date: 2026-11-10

Status: Proposed

Scope: Initial controlled preview ingress

1. Context

NEXVARIN currently deploys application containers through rootless Podman.

The runtime verifies application readiness and persists a canonical loopback upstream address in the deployment record.

The upstream address is distinct from the public preview address. A deployment becoming ready does not establish that a preview route exists or is reachable.

The initial database schema already includes preview_address, but the current application does not implement ingress configuration, route ownership, route reconciliation, or route removal.

Caddy is the selected initial ingress and TLS candidate. Its integration must preserve the separation between desired deployment state, observed runtime state, desired ingress state, and observed ingress state.

The ingress integration must also recover safely after process restarts, uncertain API responses, configuration conflicts, and partial completion.

2. Architectural Decisions

2.1 Network Topology

Caddy will run as a host-managed Linux service in the host network namespace for the initial deployment vertical slice.

The initial Podman runtime publishes application ports exclusively on the host loopback interface.

Caddy will proxy to the exact verified upstream address persisted for the deployment, using the canonical 127.0.0.1:port form.

Application ports must not be exposed on public host interfaces merely to make them reachable from Caddy.

An isolated Caddy container is not compatible with the current loopback upstream contract because its loopback interface refers to its own network namespace.

Changing Caddy to a containerized or otherwise isolated network topology requires a separate architecture decision and verified connectivity, isolation, and recovery tests.

2.2 Durable Route Resource

Ingress route state must be represented independently from runtime deployment state.

The initial implementation will introduce a durable deployment route resource, stored in PostgreSQL.

The initial scope supports one preview route per deployment.

Each route must have:

A stable database identity.

A stable Caddy route identity.

An owning project and repository.

An associated deployment.

A canonical, uniquely allocated hostname.

A desired route state.

An observed route state.

Creation and update timestamps.

Reconciliation metadata sufficient to identify unresolved changes.

Route hostnames must be allocated by trusted NEXVARIN control-plane logic beneath an operator-configured preview suffix.

V1 must not accept arbitrary externally supplied hostnames as trusted route identities.

A unique database constraint must prevent two active route records from claiming the same hostname.

The route resource must not duplicate the deployment's upstream address as an independent source of truth. Its target is derived from the associated deployment record.

The deployment preview_address field remains a nullable API-facing projection. It may be populated only after the corresponding ingress route has been applied and verified. It must not be treated as proof of route existence by itself.

2.3 Route State and Activation Eligibility

Route desired state and observed state must remain separate.

The initial desired states are:

active
inactive

The initial observed states are:

pending
active
removing
removed
needs_reconciliation

A route may be installed only when all of the following conditions hold:

The route's desired state is active.

The route hostname is allocated and validated.

The associated deployment's desired_state is active.

The associated deployment's observed_state is ready.

The associated deployment has a non-null, valid upstream_address.

The deployment has not been deleted.

The route belongs to the expected project and repository.

Legacy ready deployments without a verified upstream address must never become routable.

Runtime readiness alone must never activate a Caddy route.

A route becomes observed active only after the controller has confirmed that the intended route identity, hostname, and upstream are present in Caddy's active configuration.

2.4 Dedicated Ingress Controller

Caddy configuration must be managed by a dedicated NEXVARIN ingress controller process.

The ingress controller is responsible for:

Discovering routes requiring reconciliation.

Validating route eligibility against durable database state.

Creating, updating, and removing owned Caddy routes.

Confirming applied configuration.

Recording observed route state.

Recovering from uncertain Caddy API outcomes.

Detecting and repairing configuration drift.

The general build and deployment execution worker must not receive access to the Caddy administration interface.

The ingress controller must not execute application builds or manage application containers.

The database remains the authoritative source for desired route state. Caddy's active configuration is observed external state.

Transactional outbox events may wake the controller, but they must not become the authoritative representation of route state.

A periodic reconciliation sweep must independently discover unapplied desired changes, expired controller leases, and configuration drift.

Duplicate event delivery must not create duplicate routes or unsafe repeated configuration changes.

2.5 Caddy Configuration Management

NEXVARIN will manage only a clearly owned route namespace within a preconfigured Caddy HTTP server.

Each route will have a stable Caddy route identifier derived from its durable NEXVARIN route identity.

The ingress controller must not replace the entire Caddy configuration as part of ordinary route activation or removal.

In particular, ordinary route operations must not use POST /load to overwrite the active configuration.

The controller must use targeted Caddy Admin API configuration operations.

Where supported by the pinned Caddy version, configuration mutations must use optimistic concurrency:

Read the relevant configuration scope and its ETag.

Prepare the intended route change from that returned state.

Submit the targeted mutation with the matching If-Match header.

On HTTP 412, read the latest configuration and recompute the change. Do not blindly replay a stale mutation.

On an uncertain timeout or lost response, inspect the active configuration before deciding whether to retry.

A successful API response alone is insufficient evidence that the expected route is durably represented in the control plane.

Caddy documents ETag and If-Match protection for concurrent configuration changes as experimental. Therefore, the implementation must pin a tested Caddy version and include compatibility tests for ETag handling and precondition failures.

If the expected concurrency protection is unavailable, the controller must not silently fall back to unguarded concurrent configuration writes.

The controller must never delete, replace, or rewrite Caddy configuration outside its explicitly owned route namespace.

2.6 Administration and Security Boundary

Caddy's administrative API is a privileged control surface capable of changing the active server configuration.

It must never be exposed through the public application ingress.

The initial deployment must expose Caddy administration through a permissioned Unix-domain socket rather than relying on an unrestricted local TCP administration endpoint.

Operating-system permissions must grant the dedicated ingress controller access to that socket.

The general deployment worker, application containers, and build workloads must not receive access to it.

The Caddy process and ingress controller must run under appropriately restricted service identities.

The ingress controller's database access should be limited to the records and state transitions required for route reconciliation.

An application workload must never be able to modify ingress configuration through the Caddy administration interface.

2.7 Activation, Deactivation, and Recovery

Route activation and removal are durable workflows, not incidental side effects of application deployment.

For activation:

Persist the intended route identity and desired active state.

Confirm that the associated deployment satisfies all route eligibility requirements.

Apply the route through a targeted Caddy configuration mutation.

Read back and verify the expected hostname and upstream mapping.

Persist the observed active state.

Populate preview_address only after successful route verification.

For deactivation:

Persist the desired inactive state before attempting removal.

Remove the corresponding NEXVARIN-owned route.

Confirm that the route is absent from active Caddy configuration.

Persist the observed removed state.

Clear the preview_address projection after removal is confirmed.

Routine application shutdown, deletion, or replacement must not intentionally proceed while its ingress route remains active.

If route removal cannot be confirmed, the system must preserve the unresolved state and require reconciliation rather than reporting successful cleanup.

An explicit emergency shutdown may override normal ordering when stopping a workload is necessary to contain an immediate security or operational threat. That action must be recorded and must leave the route marked for reconciliation.

The controller must handle uncertain Caddy API outcomes by observing active configuration before retrying.

A controller restart must reconcile durable desired route records against Caddy's active configuration.

Caddy's persisted configuration and restart-resume mechanism provide useful process recovery, but PostgreSQL remains authoritative for NEXVARIN's desired route state.

On startup, the controller must repair missing desired routes and remove stale routes within its owned namespace.

2.8 Configuration Ownership and Concurrency

NEXVARIN must preserve Caddy configuration owned by the operator or other explicitly registered integrations.

Only the NEXVARIN-owned server or route namespace may be modified by the initial controller.

Route updates must be scoped to the affected route.

Configuration collisions must not cause unrelated routes to disappear.

If another administrator or process changes the configuration concurrently, the ingress controller must detect the conflict and reconcile from a fresh configuration snapshot.

A full configuration replacement is reserved for an explicit bootstrap or controlled recovery procedure, never ordinary per-route lifecycle operations.

3. Deliberately Deferred Decisions

The following remain outside this decision:

The final public preview domain and hostname format.

Public DNS automation and domain ownership verification.

Production TLS certificate issuer and deployment configuration.

Multi-node or highly available ingress.

Multiple independent Caddy instances.

Arbitrary customer-owned domains.

Public execution of untrusted workloads.

Production availability and recovery objectives.

Advanced traffic splitting, canary releases, and weighted routing.

These decisions must be revisited when implementation requirements justify them.

4. Initial Acceptance Boundary

The initial ingress implementation is not accepted until tests demonstrate that:

Caddy runs in the host network namespace and can reach verified loopback upstreams.

Application ports remain unpublished on public host interfaces.

The Caddy administration interface is not publicly reachable.

Only the dedicated ingress controller can access the administration socket.

A route cannot be activated for an inactive, deleted, unready, or upstream-less deployment.

Legacy ready records without upstream addresses remain ineligible.

Hostname ownership and uniqueness are enforced durably.

Route creation and removal are idempotent.

The controller can update one route without overwriting unrelated Caddy configuration.

Concurrent configuration changes are detected rather than silently overwritten.

A Caddy HTTP 412 response causes a fresh read and recomputation, not a blind stale retry.

An uncertain API response is reconciled against active configuration before another mutation is attempted.

Controller restarts recover pending route changes.

Stale NEXVARIN-owned routes are removed without touching unrelated routes.

Routine runtime cleanup does not report completion while an ingress route remains active.

The preview_address projection agrees with the confirmed route state.

5. Consequences

This decision introduces a separate ingress controller and durable route resource.

That increases the amount of control-plane state and testing required, but avoids coupling application deployment success to an external configuration mutation.

It provides an explicit recovery boundary between PostgreSQL and Caddy.

The host-network topology preserves the existing verified loopback upstream contract and avoids exposing application ports publicly.

The dedicated administration boundary reduces the risk that build or deployment execution code can modify the public ingress configuration.

The architecture does not guarantee production security, public DNS correctness, TLS availability, or multi-tenant isolation without further implementation and testing.

6. Change Policy

Changes to the ingress topology, route ownership model, Caddy administration boundary, or reconciliation guarantees must be supported by implementation evidence or a demonstrated requirement.

Material changes require a subsequent architecture decision record.

Governing Principle

A healthy application is not a live preview route. A live preview route is not proven by a successful configuration request. Durable desired state, verified active configuration, and recoverable reconciliation must agree before NEXVARIN reports ingress activation.

7. References

Caddy Admin API:
https://caddyserver.com/docs/api

Caddy global options and administration endpoint security:
https://caddyserver.com/docs/caddyfile/options

Caddy architecture:
https://caddyserver.com/docs/architecture
