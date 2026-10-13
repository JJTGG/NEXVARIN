ADR 0002: Operation Lifecycle, Idempotency, and Recovery

Project: NEXVARIN

Revision: 0.1

Date: 2026-10-14

Status: Accepted baseline, pending implementation validation

Scope: Initial deployment/build vertical slice

1. Purpose

Define the lifecycle rules for durable operations, background work, retries, cancellation, idempotency, and recovery after uncertain outcomes.

The database records operation state, but persisted state alone does not guarantee that an external side effect occurred exactly once. The API and worker must follow this contract.

2. Operation States

The initial schema defines these states:

queued: accepted and waiting to be processed.

running: a worker holds an active operation lease.

retry_wait: another execution attempt is permitted after a delay.

cancel_requested: cancellation has been requested while work may still be in progress.

needs_reconciliation: the external outcome is uncertain and must be investigated.

succeeded: the intended outcome has been verified.

failed: execution has definitively failed and automatic execution retries are exhausted or inappropriate.

cancelled: cancellation is confirmed and no unresolved side effect remains.

The application must validate every state transition. A database status constraint only validates the state value; it does not enforce the transition graph.

3. Permitted Transitions

Current statePermitted next stateRequired conditionqueuedrunningA worker claims the operation and obtains a lease.queuedcancelledWork has not started and no external side effect occurred.runningsucceededThe intended outcome is verified.runningfailedFailure is definitive and no unresolved side effect remains.runningretry_waitFailure is retryable and repeating the action is safe.runningcancel_requestedCancellation is requested while the worker holds its lease.runningneeds_reconciliationThe outcome cannot safely be determined from local state.retry_waitrunningRetry time has arrived and another attempt is allowed.retry_waitfailedRetry policy is exhausted or a definitive permanent failure is established.retry_waitcancelledCancellation is confirmed before another attempt starts.cancel_requestedcancelledWork is stopped and no unresolved side effect remains.cancel_requestedsucceededThe intended outcome completed before cancellation took effect.cancel_requestedfailedThe outcome is definitively failed and resolved.cancel_requestedneeds_reconciliationThe cancellation outcome cannot yet be established.needs_reconciliationsucceededEvidence confirms successful completion.needs_reconciliationfailedEvidence confirms definitive failure and no unresolved side effect.needs_reconciliationcancelledEvidence confirms cancellation and no unresolved side effect.needs_reconciliationretry_waitEvidence proves the previous action did not take effect, or a safe repeat is otherwise established. 

Terminal states are succeeded, failed, and cancelled. A terminal operation is not reopened. Corrective work or compensation must use a separate operation, linked through parent_operation_id where appropriate.

A reconciliation operation should be separately recorded and linked to the operation being investigated. Reconciliation must not be treated as proof that repeating the original action is safe.

4. Operation Creation and Idempotency

The API must authenticate the caller and authorize the requested project and repository before creating an operation.

For an accepted operation, one PostgreSQL transaction must:

Establish or retrieve the idempotency record.

Insert the operation record.

Insert the corresponding initial outbox event.

Record any required operation-history or audit event.

The operation and initial outbox event must commit together or not at all.

The database uniqueness scope is the project, requesting subject, idempotency scope, and idempotency key.

For repeated requests:

Same scope, same key, same request fingerprint: return the original operation identity and current state.

Same scope and key, different fingerprint: reject the request as a conflict.

Failed transaction: leave neither a partially created operation nor a dispatch event.

The request fingerprint must be calculated from a canonical representation of the validated, normalized request. Secret values must not be included in persisted request payloads or logs.

Idempotency prevents duplicate logical request creation. It does not independently guarantee exactly-once execution by an external system.

5. Worker Claims and Leases

Workers must claim work using transactionally safe coordination. When concurrent workers are possible, PostgreSQL locking techniques such as FOR UPDATE SKIP LOCKED may be used, subject to implementation testing.

A worker claiming an operation must atomically establish:

The operation status is running.

The lease owner is recorded.

The lease expiry is set.

The execution attempt count is updated according to the retry policy.

The start time is recorded when appropriate.

The operation event is appended.

Lease renewal must verify the current lease owner and the validity of the worker's claim. A stale worker must not be allowed to overwrite a newer worker's state.

When a lease expires, NEXVARIN must not infer that the external operation failed. Expiry establishes that ownership is no longer trustworthy; it does not establish what happened outside PostgreSQL.

The recovery process must inspect the operation type and available evidence before deciding whether to resume, retry, terminate, or reconcile it.

6. Retry Policy

Each operation has a bounded execution-attempt budget.

Retries must use a documented delay policy, a maximum attempt count, and a distinction between transient and permanent errors. Backoff and jitter may be introduced where appropriate to avoid repeated synchronized failures.

An operation may enter retry_wait only when a subsequent execution is safe.

If the execution outcome is uncertain, the operation must enter needs_reconciliation rather than being blindly retried.

Reconciliation attempts must be tracked independently when necessary so that exhausting the execution retry budget does not prevent investigation of an uncertain result.

A failed operation may be followed by a new attempt or compensating operation only through an explicit workflow that preserves history and maintains idempotency.

7. Cancellation

A cancellation request is a request to stop work, not evidence that work has stopped.

For queued work, cancellation can complete immediately only if the worker has not started an external side effect. Any pending outbox event must subsequently be safely acknowledged as a no-op or otherwise resolved; it must not launch cancelled work.

For running work, the worker must receive or discover the cancellation request and attempt a safe stop.

The operation may become cancelled only after confirming that the relevant work has stopped and the resulting external state is known. If success or failure occurred before cancellation took effect, record the verified outcome instead. If uncertainty remains, require reconciliation.

8. Transactional Outbox

Outbox delivery is at-least-once. Duplicate delivery must not create duplicate logical operations or cause unsafe repeated external actions.

Each event must have a stable identity and a unique deduplication key.

Workers must use expiring leases, bounded retries, and durable acknowledgement. Acknowledgement means the event's processing decision has been durably recorded; it does not mean that a deployment is healthy or a build has succeeded.

The consumer must verify the associated operation state before executing work. Events for already cancelled or terminal operations must not launch new side effects.

A dead-lettered event is not automatically equivalent to a failed operation. The system must record the dispatch failure and determine whether the operation can safely be retried or requires reconciliation.

9. External Side Effects and Reconciliation

PostgreSQL transactions cannot atomically include an arbitrary runtime, build engine, registry, or ingress provider.

Before invoking an external action, the worker must persist enough information to identify the intended action and associate subsequent evidence with its operation.

Where available, provider idempotency keys and stable external resource identifiers should be used. Their actual guarantees must be verified rather than assumed.

After a timeout, process crash, lost response, or expired lease, the worker must inspect observable external state before repeating a non-idempotent action.

Reconciliation must compare desired state, recorded operation history, and observed external state. It must reach a verified conclusion or preserve the unresolved state for further investigation.

10. Operation History and Audit

Important transitions must append operation events in the same transaction as the corresponding local state change.

Security-sensitive actions, authorization failures, retries, cancellation decisions, reconciliation outcomes, and cleanup results must be auditable.

History records should be append-only through application write paths and permissions. Sensitive credentials, tokens, secret-bearing request bodies, and unnecessary personal information must never be written to event details or error summaries.

Errors returned to API clients must not disclose internal credentials, host details, or sensitive provider responses.

11. Acceptance Tests

The operation implementation is not accepted until tests demonstrate:

Operation and outbox insertion are atomic.

Repeated requests with the same key and fingerprint return the same logical operation.

Reuse of a key with a different fingerprint is rejected.

Unauthorized callers cannot create, inspect, cancel, or alter another owner's operations.

Concurrent workers cannot validly hold the same operation lease.

A stale lease holder cannot overwrite state after ownership changes.

Worker termination after claiming work is recoverable.

Expired leases do not automatically trigger unsafe external retries.

Retry budgets and retry delays are enforced.

Cancellation does not report completion before the outcome is established.

Duplicate outbox delivery does not duplicate logical side effects.

Dead-lettered dispatch is distinguishable from operation failure.

Uncertain provider outcomes enter reconciliation.

Terminal operations cannot be reopened by ordinary execution.

History records transitions and outcomes without exposing secrets.

12. Change Policy

Changes to these lifecycle rules must be supported by implementation evidence, failure testing, or a demonstrated requirement. Material changes require a subsequent architecture decision record.

Governing principle: Acknowledgement is not completion, lease expiry is not failure, and retry is not safe until the prior outcome is understood or the repeated action is proven safe.

