package main

import "context"

// operationExecutorMux routes each operation to the component
// responsible for executing that operation type.
//
// The worker runner stays independent of the build, deployment,
// cleanup, and reconciliation implementations.
type operationExecutorMux struct {
	executors map[operationType]operationExecutor
}

func newOperationExecutorMux(
	executors map[operationType]operationExecutor,
) operationExecutorMux {
	registered := make(
		map[operationType]operationExecutor,
		len(executors),
	)

	for operationTypeValue, executor := range executors {
		registered[operationTypeValue] = executor
	}

	return operationExecutorMux{
		executors: registered,
	}
}

// Execute sends a task to its registered executor.
//
// An operation without a registered executor fails safely without
// starting an external action. Once a real executor is implemented,
// it can be registered here without changing the worker runner.
func (mux operationExecutorMux) Execute(
	ctx context.Context,
	task operationWorkerTask,
) operationExecutionResult {
	executor := mux.executors[task.Request.OperationType]
	if executor == nil {
		return operationExecutionResult{
			Disposition:  operationExecutionFailed,
			ErrorCode:    "executor_not_configured",
			ErrorSummary: "No executor is configured for this operation type.",
		}
	}

	return executor.Execute(ctx, task)
}

var _ operationExecutor = operationExecutorMux{}
