package main

import "testing"

func TestIsKnownOperationStatus(t *testing.T) {
	tests := []struct {
		name   string
		status operationStatus
		want   bool
	}{
		{"queued", operationStatusQueued, true},
		{"running", operationStatusRunning, true},
		{"retry wait", operationStatusRetryWait, true},
		{"cancel requested", operationStatusCancelRequested, true},
		{"needs reconciliation", operationStatusNeedsReconciliation, true},
		{"succeeded", operationStatusSucceeded, true},
		{"failed", operationStatusFailed, true},
		{"cancelled", operationStatusCancelled, true},
		{"unknown", operationStatus("unknown"), false},
		{"empty", operationStatus(""), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isKnownOperationStatus(test.status); got != test.want {
				t.Errorf("isKnownOperationStatus(%q) = %t, want %t",
					test.status, got, test.want)
			}
		})
	}
}

func TestIsTerminalOperationStatus(t *testing.T) {
	tests := []struct {
		name   string
		status operationStatus
		want   bool
	}{
		{"queued", operationStatusQueued, false},
		{"running", operationStatusRunning, false},
		{"retry wait", operationStatusRetryWait, false},
		{"cancel requested", operationStatusCancelRequested, false},
		{"needs reconciliation", operationStatusNeedsReconciliation, false},
		{"succeeded", operationStatusSucceeded, true},
		{"failed", operationStatusFailed, true},
		{"cancelled", operationStatusCancelled, true},
		{"unknown", operationStatus("unknown"), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isTerminalOperationStatus(test.status); got != test.want {
				t.Errorf("isTerminalOperationStatus(%q) = %t, want %t",
					test.status, got, test.want)
			}
		})
	}
}

func TestCanTransitionOperationStatusAllowed(t *testing.T) {
	tests := []struct {
		name    string
		current operationStatus
		next    operationStatus
	}{
		{"queued to running", operationStatusQueued, operationStatusRunning},
		{"queued to cancelled", operationStatusQueued, operationStatusCancelled},

		{"running to succeeded", operationStatusRunning, operationStatusSucceeded},
		{"running to failed", operationStatusRunning, operationStatusFailed},
		{"running to retry wait", operationStatusRunning, operationStatusRetryWait},
		{"running to cancel requested", operationStatusRunning, operationStatusCancelRequested},
		{"running to reconciliation", operationStatusRunning, operationStatusNeedsReconciliation},

		{"retry wait to running", operationStatusRetryWait, operationStatusRunning},
		{"retry wait to failed", operationStatusRetryWait, operationStatusFailed},
		{"retry wait to cancelled", operationStatusRetryWait, operationStatusCancelled},

		{"cancel requested to cancelled", operationStatusCancelRequested, operationStatusCancelled},
		{"cancel requested to succeeded", operationStatusCancelRequested, operationStatusSucceeded},
		{"cancel requested to failed", operationStatusCancelRequested, operationStatusFailed},
		{"cancel requested to reconciliation", operationStatusCancelRequested, operationStatusNeedsReconciliation},

		{"reconciliation to succeeded", operationStatusNeedsReconciliation, operationStatusSucceeded},
		{"reconciliation to failed", operationStatusNeedsReconciliation, operationStatusFailed},
		{"reconciliation to cancelled", operationStatusNeedsReconciliation, operationStatusCancelled},
		{"reconciliation to retry wait", operationStatusNeedsReconciliation, operationStatusRetryWait},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !canTransitionOperationStatus(test.current, test.next) {
				t.Errorf("transition %q -> %q was rejected, want allowed",
					test.current, test.next)
			}
		})
	}
}

func TestCanTransitionOperationStatusRejectsInvalidTransitions(t *testing.T) {
	tests := []struct {
		name    string
		current operationStatus
		next    operationStatus
	}{
		{"same state", operationStatusQueued, operationStatusQueued},
		{"queued to succeeded", operationStatusQueued, operationStatusSucceeded},
		{"queued to retry wait", operationStatusQueued, operationStatusRetryWait},
		{"running to queued", operationStatusRunning, operationStatusQueued},
		{"retry wait to reconciliation", operationStatusRetryWait, operationStatusNeedsReconciliation},
		{"cancel requested to retry wait", operationStatusCancelRequested, operationStatusRetryWait},
		{"reconciliation to running", operationStatusNeedsReconciliation, operationStatusRunning},
		{"unknown current state", operationStatus("unknown"), operationStatusRunning},
		{"unknown next state", operationStatusRunning, operationStatus("unknown")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if canTransitionOperationStatus(test.current, test.next) {
				t.Errorf("transition %q -> %q was allowed, want rejected",
					test.current, test.next)
			}
		})
	}
}

func TestTerminalOperationStatusesCannotTransition(t *testing.T) {
	terminalStatuses := []operationStatus{
		operationStatusSucceeded,
		operationStatusFailed,
		operationStatusCancelled,
	}

	allStatuses := []operationStatus{
		operationStatusQueued,
		operationStatusRunning,
		operationStatusRetryWait,
		operationStatusCancelRequested,
		operationStatusNeedsReconciliation,
		operationStatusSucceeded,
		operationStatusFailed,
		operationStatusCancelled,
	}

	for _, current := range terminalStatuses {
		for _, next := range allStatuses {
			t.Run(string(current)+" to "+string(next), func(t *testing.T) {
				if canTransitionOperationStatus(current, next) {
					t.Errorf("terminal status %q transitioned to %q",
						current, next)
				}
			})
		}
	}
}
