package main

import (
	"context"
	"errors"
)

var errOperationWorkerDeployRuntimeOutcomeUnknown = errors.New(
	"deployment runtime outcome is unknown",
)

// operationWorkerDeployRuntimeRequest contains the information
// required to start an application from a verified OCI artifact.
type operationWorkerDeployRuntimeRequest struct {
	// OperationID identifies the persisted deploy operation.
	OperationID string

	// DeploymentID identifies the durable deployment record.
	DeploymentID string

	// ArtifactPath must come from artifact-file verification.
	ArtifactPath string

	// ArtifactDigest identifies the complete OCI archive file.
	ArtifactDigest string

	// ImageReference identifies the image stored in the archive.
	ImageReference string

	// ApplicationPort is the validated port the application listens
	// on inside its container.
	ApplicationPort int
}

// operationWorkerDeployRuntimeResult describes the instance
// created or recovered by the runtime.
type operationWorkerDeployRuntimeResult struct {
	// RuntimeInstanceReference is the runtime's stable identifier
	// for the application instance.
	RuntimeInstanceReference string
}

// operationWorkerDeployRuntime is the boundary between NEXVARIN's
// deployment executor and the application runtime.
//
// Implementations must be idempotent for the same operation and
// deployment IDs. Repeated attempts must not create duplicate
// application instances.
//
// An ordinary error must mean the implementation knows no runtime
// side effect remains active. If a side effect might have occurred
// but its outcome cannot be confirmed, the implementation must
// wrap errOperationWorkerDeployRuntimeOutcomeUnknown.
//
// Port publishing, application health checks, and preview routing
// are implemented by subsequent runtime changes.
type operationWorkerDeployRuntime interface {
	Deploy(
		ctx context.Context,
		request operationWorkerDeployRuntimeRequest,
	) (operationWorkerDeployRuntimeResult, error)
}
