package main

import (
	"context"
	"testing"
)

type operationExecutorMuxTestExecutor struct {
	result   operationExecutionResult
	calls    int
	lastTask operationWorkerTask
}

func (executor *operationExecutorMuxTestExecutor) Execute(
	_ context.Context,
	task operationWorkerTask,
) operationExecutionResult {
	executor.calls++
	executor.lastTask = task

	return executor.result
}

func TestOperationExecutorMuxRoutesToMatchingExecutor(
	t *testing.T,
) {
	buildExecutor := &operationExecutorMuxTestExecutor{
		result: operationExecutionResult{
			Disposition: operationExecutionSucceeded,
		},
	}
	deployExecutor := &operationExecutorMuxTestExecutor{
		result: operationExecutionResult{
			Disposition: operationExecutionRetry,
			ErrorCode:   "temporary_deploy_failure",
			ErrorSummary: "Deployment could not start.",
		},
	}

	mux := newOperationExecutorMux(map[operationType]operationExecutor{
		operationTypeBuild:  buildExecutor,
		operationTypeDeploy: deployExecutor,
	})

	task := operationWorkerTask{
		Request: normalizedOperationRequest{
			OperationType: operationTypeBuild,
		},
	}

	result := mux.Execute(context.Background(), task)

	if result.Disposition != operationExecutionSucceeded {
		t.Fatalf(
			"disposition = %q, want succeeded",
			result.Disposition,
		)
	}
	if buildExecutor.calls != 1 {
		t.Errorf(
			"build executor calls = %d, want 1",
			buildExecutor.calls,
		)
	}
	if deployExecutor.calls != 0 {
		t.Errorf(
			"deploy executor calls = %d, want 0",
			deployExecutor.calls,
		)
	}
	if buildExecutor.lastTask.Request.OperationType != operationTypeBuild {
		t.Error("build executor received the wrong operation type")
	}
}

func TestOperationExecutorMuxFailsWhenExecutorIsMissing(
	t *testing.T,
) {
	mux := newOperationExecutorMux(nil)

	result := mux.Execute(
		context.Background(),
		operationWorkerTask{
			Request: normalizedOperationRequest{
				OperationType: operationTypeBuild,
			},
		},
	)

	if result.Disposition != operationExecutionFailed {
		t.Errorf(
			"disposition = %q, want failed",
			result.Disposition,
		)
	}
	if result.ErrorCode != "executor_not_configured" {
		t.Errorf(
			"error code = %q, want executor_not_configured",
			result.ErrorCode,
		)
	}
	if result.ErrorSummary == "" {
		t.Error("error summary is empty")
	}
	if result.RetryDelay != 0 {
		t.Errorf(
			"retry delay = %v, want zero",
			result.RetryDelay,
		)
	}
}

func TestOperationExecutorMuxCopiesRegistrationMap(
	t *testing.T,
) {
	original := &operationExecutorMuxTestExecutor{
		result: operationExecutionResult{
			Disposition: operationExecutionSucceeded,
		},
	}
	replacement := &operationExecutorMuxTestExecutor{
		result: operationExecutionResult{
			Disposition: operationExecutionFailed,
			ErrorCode:   "replacement_executor",
			ErrorSummary: "Replacement executor was called.",
		},
	}

	registrations := map[operationType]operationExecutor{
		operationTypeBuild: original,
	}
	mux := newOperationExecutorMux(registrations)

	// Changing the caller's map must not silently replace a live route.
	registrations[operationTypeBuild] = replacement

	result := mux.Execute(
		context.Background(),
		operationWorkerTask{
			Request: normalizedOperationRequest{
				OperationType: operationTypeBuild,
			},
		},
	)

	if result.Disposition != operationExecutionSucceeded {
		t.Errorf(
			"disposition = %q, want succeeded",
			result.Disposition,
		)
	}
	if original.calls != 1 {
		t.Errorf("original executor calls = %d, want 1", original.calls)
	}
	if replacement.calls != 0 {
		t.Errorf(
			"replacement executor calls = %d, want 0",
			replacement.calls,
		)
	}
}
