# NEXVARIN

**Independent infrastructure. Shared foundations. Self-managed operations.**

NEXVARIN is an independent infrastructure engineering project under
the TGG HUB ecosystem.

Its mission is to design, build, and operate a collection of
interoperable infrastructure systems that provide greater ownership,
control, reliability, and operational autonomy.

NEXVARIN aims to reduce unnecessary dependence on external
infrastructure platforms while progressively developing the ability
to manage, maintain, monitor, and recover its own systems wherever
technically and economically practical.

> Build infrastructure we can understand, control, verify, and
> progressively teach to manage itself.

## Project Status

- **Phase:** Architecture and implementation preparation
- **Project definition:** Q1–Q10 locked
- **Initial build specification:** v0.1 locked
- **Implementation:** Not yet started
- **Production infrastructure:** Not yet established
- **Technology stack:** Proposed baseline; compatibility verification
  remains necessary

NEXVARIN does not yet provide production-ready infrastructure.
Planned capabilities must not be interpreted as implemented features.

## Vision

Build an independently operated infrastructure ecosystem whose
systems can cooperate through explicit contracts while retaining
their own purposes, implementations, and lifecycles.

NEXVARIN aims to progressively provide the capabilities needed to
build, deploy, execute, store, secure, observe, maintain, and recover
software systems.

Its long-term direction includes bounded autonomous operations,
practical dependency independence, and infrastructure that can
verify its own state and recover from defined failures.

## Scope

The planned infrastructure domains include:

- **Deployment and builds:** Source integration, isolated build
  execution, artifact management, releases, and rollback.
- **Compute and networking:** Workload execution, resource
  scheduling, capacity management, routing, domains, and connectivity.
- **Backend and databases:** Application APIs, database services,
  identity integration, and data operations.
- **Storage and asynchronous work:** Persistent data, object storage,
  queues, scheduled jobs, and durable workflows.
- **Identity and security:** Authentication, authorization, workload
  identity, secrets, tenant isolation, and auditability.
- **Operations and reliability:** Observability, resource
  reconciliation, backups, restoration, and recovery.
- **Usage and economics:** Resource measurement, usage attribution,
  quotas, cost reconciliation, and future billing capabilities where
  appropriate.

These are planned domains, not a declaration that every system will
be built immediately.

## Architecture Principles

### 1. Independent Systems

Each system retains a defined purpose, lifecycle, implementation,
and set of requirements.

Systems may cooperate without being forced into one tightly coupled
product.

### 2. Shared Foundations

Common contracts may support identity, authorization, resource
ownership, operation tracking, observability, and usage accounting
where sharing provides a demonstrated benefit.

### 3. Operational Autonomy

Systems should progressively learn to detect failures, reconcile
state, enforce policies, perform routine maintenance, and recover
from supported failure scenarios.

Autonomous actions must remain bounded, authorized, observable,
and verifiable.

### 4. Reduced External Dependency

NEXVARIN aims to favor self-hostable components, replaceable
provider interfaces, portable data formats, and documented recovery
paths where practical.

External dependencies will be identified and managed rather than
assumed to disappear completely.

### 5. Security by Design

Least privilege, workload isolation, controlled credentials, explicit
network boundaries, and auditable administrative actions must be
considered from the beginning.

### 6. Explicit State and Recovery

Resources must have identifiable ownership, lifecycle state, and
operation history.

Desired state must be distinguished from observed reality.
Uncertain outcomes must be reconciled before being reported as
successful.

### 7. Measurable Resource Usage

Observed consumption, internal usage accounting, provider costs,
customer-billable usage, and enforceable limits are distinct concepts.

Their relationships must be explicit and auditable.

### 8. Evidence-Driven Engineering

Design decisions must be supported by relevant evidence, explicit
requirements, adversarial review, and repeatable tests.

Capabilities must earn claims of reliability, security, portability,
and autonomy through demonstrated results.

## Engineering Method

NEXVARIN follows this method:

**Investigate → Extract → Red team → Enhance → Apply**

1. **Investigate:** Examine existing systems, documentation,
   architecture, interfaces, and evidence.
2. **Extract:** Identify useful mechanisms, limitations, and
   underlying requirements.
3. **Red team:** Challenge assumptions and examine failure paths,
   security risks, and unintended consequences.
4. **Enhance:** Develop improvements supported by the findings.
5. **Apply:** Decide what to adopt, adapt, reject, test further, or
   keep independent.

Further investigation should be triggered by a concrete unresolved
question, not by an obligation to research indefinitely.

## Initial Implementation

The first implementation target is a controlled deployment and
build lifecycle.

The initial scope is designed to demonstrate that NEXVARIN can:

1. Accept an authorized source revision from a registered repository.
2. Record a durable operation and its identity.
3. Build the source in a controlled environment.
4. identify and verify the resulting artifact.
5. Deploy that artifact to a preview environment.
6. Verify runtime health before activating its preview route.
7. Record the outcome and reconcile incomplete operations.

The first implementation will use controlled source code in a test
environment. Public registration, arbitrary untrusted builds,
production multi-tenancy, and multi-node scheduling are outside
the initial scope.

### Proposed Technology Baseline

| Area | Candidate |
|---|---|
| Web console | Next.js, React, TypeScript |
| Control API | Go |
| Durable state | PostgreSQL |
| API contracts | OpenAPI |
| Build engine | BuildKit |
| Artifact format | OCI-compatible artifacts |
| Initial runtime | Podman rootless, pending verification |
| Initial ingress | Caddy, pending integration testing |
| Telemetry | OpenTelemetry |

These technologies are candidates for the initial implementation,
not a permanent commitment for every future NEXVARIN system.

## Relationship with TGG HUB

TGG HUB is the umbrella ecosystem under which NEXVARIN and other
independent projects exist.

NEXVARIN has its own identity, architecture, goals, and development
lifecycle. Its infrastructure may eventually support other TGG HUB
projects and external users.

Projects within TGG HUB remain standalone. Membership in the
ecosystem does not automatically make every project dependent on
NEXVARIN.

## Five-Year Direction

NEXVARIN aims to become an independently operated infrastructure
ecosystem whose systems can:

- Operate through clear and maintainable interfaces.
- Manage resources with traceable ownership and lifecycle state.
- Recover from defined failures through tested procedures.
- Automate routine operations within explicit safety boundaries.
- Explain resource consumption and infrastructure costs.
- Reduce unnecessary dependencies while preserving practical
  replacement and recovery paths.
- Provide dependable capabilities to independent projects without
  forcing unnecessary architectural coupling.

Progress will be judged by working capabilities, verified guarantees,
operational evidence, and sustainable maintenance—not simply by the
number of systems or features created.

## Current Priorities

1. Establish the repository and foundational project structure.
2. Implement the control API and durable operation model.
3. Add the initial PostgreSQL schema and transactional outbox.
4. Implement the worker and controlled build lifecycle.
5. Validate artifact integrity, deployment health, and reconciliation.
6. Verify the acceptance criteria before expanding the scope.

## Licensing and Contributions

The project license and contribution policy have not yet been
established.

Until a license is added, no open-source reuse permission should
be assumed.

## Governing Principle

NEXVARIN seeks to build infrastructure that its operators can
understand, control, verify, improve, and progressively teach to
manage itself responsibly.

It will prioritize explicit boundaries, recoverability, security,
and sustainable operation over premature complexity or unsupported
claims.