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

	// UpstreamAddress is the host:port through which the local ingress
	// adapter can reach the application. For the initial Podman runtime,
	// this must be a verified loopback-published address.
	// This address alone does not prove application readiness.
	UpstreamAddress string

	// ReadinessVerified is true only when the runtime has successfully
	// completed its configured application readiness probe.
	ReadinessVerified bool
}

// operationWorkerDeployRuntime is the boundary between NEXVARIN's
// deployment executor and the application runtime.
//
// Implementations must be idempotent for the same operation and
// deployment IDs. Repeated attempts must not create duplicate
// application instances.
//
// UpstreamAddress must only identify an address confirmed to belong
// to the requested runtime instance and its published application port.
// For the initial local ingress design, the address must be loopback-bound.
//
// ReadinessVerified must be true only after the configured application
// readiness probe succeeds. An instance may exist while readiness
// remains unverified; the executor must not persist that instance as
// ready or activate its preview route.
//
// An ordinary error must mean the implementation knows no runtime
// side effect remains active. If a side effect might have occurred
// but its outcome cannot be confirmed, the implementation must
// wrap errOperationWorkerDeployRuntimeOutcomeUnknown.
type operationWorkerDeployRuntime interface {
	Deploy(
		ctx context.Context,
		request operationWorkerDeployRuntimeRequest,
	) (operationWorkerDeployRuntimeResult, error)
}
