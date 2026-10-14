package main

type operationStatus string

const (
	operationStatusQueued               operationStatus = "queued"
	operationStatusRunning              operationStatus = "running"
	operationStatusRetryWait             operationStatus = "retry_wait"
	operationStatusCancelRequested       operationStatus = "cancel_requested"
	operationStatusNeedsReconciliation  operationStatus = "needs_reconciliation"
	operationStatusSucceeded             operationStatus = "succeeded"
	operationStatusFailed                operationStatus = "failed"
	operationStatusCancelled             operationStatus = "cancelled"
)

// operationStatusTransitions defines the permitted state changes.
//
// The transition graph alone does not establish that a change is safe.
// Callers must also enforce transaction boundaries, lease ownership,
// cancellation evidence, and reconciliation requirements.
var operationStatusTransitions = map[operationStatus][]operationStatus{
	operationStatusQueued: {
		operationStatusRunning,
		operationStatusCancelled,
	},
	operationStatusRunning: {
		operationStatusSucceeded,
		operationStatusFailed,
		operationStatusRetryWait,
		operationStatusCancelRequested,
		operationStatusNeedsReconciliation,
	},
	operationStatusRetryWait: {
		operationStatusRunning,
		operationStatusFailed,
		operationStatusCancelled,
	},
	operationStatusCancelRequested: {
		operationStatusCancelled,
		operationStatusSucceeded,
		operationStatusFailed,
		operationStatusNeedsReconciliation,
	},
	operationStatusNeedsReconciliation: {
		operationStatusSucceeded,
		operationStatusFailed,
		operationStatusCancelled,
		operationStatusRetryWait,
	},
	operationStatusSucceeded: nil,
	operationStatusFailed:    nil,
	operationStatusCancelled: nil,
}

func isKnownOperationStatus(status operationStatus) bool {
	_, ok := operationStatusTransitions[status]
	return ok
}

func isTerminalOperationStatus(status operationStatus) bool {
	switch status {
	case operationStatusSucceeded,
		operationStatusFailed,
		operationStatusCancelled:
		return true
	default:
		return false
	}
}

// canTransitionOperationStatus reports whether a state change is permitted.
// Same-state comparisons are not transitions; handle idempotent no-ops
// separately when integrating this rule into database operations.
func canTransitionOperationStatus(
	current operationStatus,
	next operationStatus,
) bool {
	if current == next {
		return false
	}

	allowedNext, known := operationStatusTransitions[current]
	if !known {
		return false
	}

	for _, candidate := range allowedNext {
		if candidate == next {
			return true
		}
	}

	return false
}