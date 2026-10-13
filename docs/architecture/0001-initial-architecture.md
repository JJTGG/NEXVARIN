ADR 0001: Initial Architecture and System Boundaries

Project: NEXVARIN

Decision: Initial implementation architecture

Revision: 0.1

Date: 2026-10-14

Status: Accepted baseline; technology candidates remain subject to verification

Scope: First deployment/build vertical slice

1. Context

NEXVARIN is an independent infrastructure engineering project under the TGG HUB ecosystem.

Its architecture consists of independent infrastructure systems connected through explicit contracts and shared foundations where justified.

The initial implementation must demonstrate a controlled deployment/build lifecycle before NEXVARIN expands into public hosting, arbitrary customer workloads, multi-node scheduling, billing, or additional infrastructure systems.

The first implementation must prioritize resource ownership, durable operation tracking, safe retries, artifact integrity, controlled execution, and recovery from incomplete operations.

2. Architectural Decisions

2.1 Modular Control Plane

Begin with a small, modular control-plane codebase rather than a distributed network of microservices.

The initial system consists of:

A web console.

A control API.

A background worker.

A PostgreSQL database.

A controlled build and deployment execution environment.

These components may run as separate processes while sharing a repository and common contracts.

Additional service boundaries must be justified by requirements for isolation, security, reliability, scaling, or independent operation.

2.2 Language Allocation

TypeScript: Web console and client-side application logic.

Go: Control API, background worker, and infrastructure integration code.

SQL: Persistent data structures, constraints, indexes, and migrations.

OpenAPI: Language-independent HTTP API contract.

The initial project should avoid introducing additional programming languages or frameworks without a concrete requirement.

2.3 Durable State

PostgreSQL is the proposed source of truth for NEXVARIN's local control-plane records.

The initial data model will cover:

Project ownership.

Registered repositories.

Operations and idempotency records.

Deployment records.

Build and artifact metadata.

Outbox events.

Relevant administrative audit events.

Large artifacts and operational log bodies should be stored outside ordinary control-plane tables.

Desired configuration, observed external state, and operation history must remain distinguishable.

2.4 Durable Operations and Asynchronous Work

Long-running operations must be represented by persistent operation records.

The control API must not report final success merely because a request was accepted or a provider accepted an API call.

When a database transaction changes local state and schedules corresponding background work, the state change and outbox entry must be committed atomically.

Workers must support safe retries, stable operation identity, bounded resource consumption, and reconciliation after uncertain external outcomes.

An external provider operation is not assumed to participate in the local PostgreSQL transaction.

2.5 Initial Build System

BuildKit is the candidate build engine.

The first implementation will build only controlled source repositories. Repository identity and the selected source revision must be validated before execution.

Build environments must have explicit resource limits and must not receive unnecessary production credentials or privileged host access.

The build system must preserve the relationship between the source revision, build operation, and resulting artifact.

2.6 Artifact Identity

Application build outputs should use an appropriate OCI-compatible artifact representation.

The control plane must record a stable content digest and verify retrieved artifact content against its expected digest where applicable.

Artifact identity, build provenance, deployment identity, and release assignment must not be treated as interchangeable concepts.

2.7 Initial Runtime

Podman rootless is the initial runtime candidate.

Compatibility testing must verify the interaction between the build output, image transfer or storage, runtime execution, networking, resource limits, process lifecycle, and cleanup.

Rootless operation alone is not considered proof of sufficient isolation for arbitrary untrusted workloads.

2.8 Initial Ingress

Caddy is the initial ingress and TLS candidate.

The first implementation uses a controlled local preview environment. The routing integration must establish how configuration is updated, validated, reconciled, and restored after failure.

The control-plane administration interface must not be exposed publicly as part of the initial slice.

2.9 API Contracts

The control API uses REST/JSON with an OpenAPI contract.

The API must define request validation, authorization expectations, error responses, idempotency behavior, operation inspection, and cancellation semantics.

Persistent operation state is authoritative for the recorded workflow. External state must still be verified when necessary.

2.10 Security Boundary

The initial deployment system is operator-controlled and restricted to registered repositories under our control.

Public registration, arbitrary repositories, and arbitrary untrusted builds are excluded.

Authentication, authorization, repository credential handling, worker privileges, runtime permissions, network access, and secret storage must be established before the corresponding functionality is exposed.

The build worker must not be granted production credentials or privileged runtime-management access by default.

2.11 Bootstrap and Recovery

The initial implementation targets a controlled Linux test environment.

Bootstrap and recovery procedures must remain available independently of the web console and ordinary deployment workflow.

The platform must preserve enough operation history and resource metadata to investigate partial success after a process restart or external service failure.

2.12 Observability and Usage

Operational logging, health checks, and audit records are required to verify the first slice.

Usage accounting must distinguish actual resource consumption from estimated cost and customer-billable usage.

A full metering pipeline, commercial billing engine, and production cost-enforcement service are outside the initial implementation scope.

3. Deliberately Deferred Decisions

The following are not finalized by this ADR:

Final runtime and container-isolation architecture.

Public workload execution and multi-tenant isolation.

Artifact registry or object-storage provider.

Production authentication and secrets-management implementation.

Multi-node scheduling and capacity provisioning.

Message broker adoption.

Public DNS, domain automation, and production TLS deployment.

Commercial pricing, billing, and subscription management.

Independent database, storage, and backend products.

Production availability and recovery objectives.

Each decision should be revisited when an implementation requirement makes it necessary.

4. Initial Acceptance Boundary

The first vertical slice must demonstrate that NEXVARIN can:

Accept an authorized request for a valid, registered source revision.

Create durable operation and deployment records.

Handle repeated requests idempotently.

Recover queued or interrupted work without blindly duplicating side effects.

Build controlled source code within the defined security boundary.

Identify and verify the resulting artifact.

Deploy the artifact to a controlled preview environment.

Verify runtime health before activating the preview route.

Reconcile uncertain or partially completed operations.

Record failures, sensitive actions, and cleanup outcomes without exposing credentials.

The detailed acceptance checklist is maintained in the first build specification.

5. Consequences

This architecture minimizes initial implementation complexity while establishing durable foundations for later systems.

It keeps the first implementation small enough to test locally, while preserving explicit boundaries between control logic, background work, artifact handling, runtime execution, and provider-specific behavior.

The design does not guarantee portability, production security, autonomous recovery, or multi-tenant safety merely because these boundaries are documented. Those properties must be verified through implementation and testing.

6. Change Policy

Changes to these architectural decisions must be justified by requirements or evidence.

Material changes to the control-plane boundary, resource model, security assumptions, persistence strategy, or provider interface should be recorded in a subsequent architecture decision record.

The presence of an existing technology in the proposed stack is not sufficient reason to retain it when tests show that it fails a necessary requirement.

Governing Principle

Build the smallest system that can prove the intended deployment lifecycle correctly, securely, and recoverably.

Expand the architecture only when demonstrated requirements justify the additional capability or complexity.

