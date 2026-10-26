package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestOperationWorkerDeployRunsThroughRunnerIntegration(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)

	runtime := &operationWorkerDeployExecutorTestRuntime{
		result: operationWorkerDeployRuntimeResult{
			RuntimeInstanceReference: "test-worker-runtime-instance-001",
			ReadinessVerified:        true,
		},
	}

	artifact := insertDeployArtifactForTest(
		t,
		fixture,
		"verified",
	)

	artifact, artifactDirectory :=
		prepareOperationWorkerDeployExecutorTestArtifact(
			t,
			fixture,
			artifact,
		)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	// The artifact fixture creates a queued build operation. Complete that
	// setup operation so the worker's next claim targets the deployment.
	buildClaim, err := claimOperation(
		ctx,
		fixture.pool,
		artifact.BuildOperationID,
		"worker-deploy-runner-setup",
		time.Minute,
	)
	if err != nil {
		t.Fatalf("claim fixture build operation: %v", err)
	}

	if _, err := completeOperation(
		ctx,
		fixture.pool,
		buildClaim.Operation.ID,
		buildClaim.LeaseOwner,
	); err != nil {
		t.Fatalf("complete fixture build operation: %v", err)
	}

	requestBody, err := json.Marshal(struct {
		OperationType   operationType `json:"operationType"`
		ArtifactID      string        `json:"artifactId"`
		ApplicationPort int           `json:"applicationPort"`
	}{
		OperationType:   operationTypeDeploy,
		ArtifactID:      artifact.ID,
		ApplicationPort: 8080,
	})
	if err != nil {
		t.Fatalf("encode deployment request: %v", err)
	}

	created, err := fixture.create(
		"deploy-runner-"+artifact.ID,
		requestBody,
	)
	if err != nil {
		t.Fatalf("create queued deployment operation: %v", err)
	}

	deployExecutor := operationWorkerDeployExecutor{
		Pool:              fixture.pool,
		ArtifactDirectory: artifactDirectory,
		Runtime:           runtime,
	}

	executor := newOperationExecutorMux(
		map[operationType]operationExecutor{
			operationTypeDeploy: deployExecutor,
		},
	)

	worker := operationWorkerRunner{
		Pool:              fixture.pool,
		WorkerID:          "worker-deploy-runner-integration",
		Executor:          executor,
		LeaseDuration:     time.Minute,
		HeartbeatInterval: time.Second,
		PollInterval:      10 * time.Millisecond,
	}

	processed, err := worker.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run deployment worker: %v", err)
	}
	if !processed {
		t.Fatal("worker processed = false, want true")
	}

	if runtime.calls != 1 {
		t.Fatalf("runtime calls = %d, want 1", runtime.calls)
	}

	if runtime.request.OperationID != created.Operation.ID {
		t.Errorf(
			"runtime operation ID = %q, want %q",
			runtime.request.OperationID,
			created.Operation.ID,
		)
	}

	if runtime.request.DeploymentID == "" {
		t.Fatal("runtime received an empty deployment ID")
	}

	if runtime.request.ArtifactPath != artifact.ArtifactReference {
		t.Errorf(
			"runtime artifact path = %q, want %q",
			runtime.request.ArtifactPath,
			artifact.ArtifactReference,
		)
	}

	if runtime.request.ArtifactDigest != artifact.ContentDigest {
		t.Errorf(
			"runtime artifact digest = %q, want %q",
			runtime.request.ArtifactDigest,
			artifact.ContentDigest,
		)
	}

	if runtime.request.ImageReference != artifact.Metadata.ImageReference {
		t.Errorf(
			"runtime image reference = %q, want %q",
			runtime.request.ImageReference,
			artifact.Metadata.ImageReference,
		)
	}

	if runtime.request.ApplicationPort != 8080 {
		t.Errorf(
			"runtime application port = %d, want 8080",
			runtime.request.ApplicationPort,
		)
	}

	var (
		operationStatusValue string
		leaseOwner           *string
		finishedAt           *time.Time
		errorCode            *string
		errorSummary         *string
	)

	err = fixture.pool.QueryRow(ctx, `
		SELECT
			status,
			lease_owner,
			finished_at,
			error_code,
			error_summary
		FROM operations
		WHERE id = $1::uuid
	`, created.Operation.ID).Scan(
		&operationStatusValue,
		&leaseOwner,
		&finishedAt,
		&errorCode,
		&errorSummary,
	)
	if err != nil {
		t.Fatalf("read persisted deployment operation: %v", err)
	}

	if operationStatusValue != string(operationStatusSucceeded) {
		t.Errorf(
			"operation status = %q, want succeeded",
			operationStatusValue,
		)
	}

	if leaseOwner != nil {
		t.Errorf(
			"operation lease owner = %q, want NULL",
			*leaseOwner,
		)
	}

	if finishedAt == nil {
		t.Error("operation finished_at is NULL")
	}

	if errorCode != nil {
		t.Errorf(
			"operation error code = %q, want NULL",
			*errorCode,
		)
	}

	if errorSummary != nil {
		t.Errorf(
			"operation error summary = %q, want NULL",
			*errorSummary,
		)
	}

	var (
		persistedDeploymentID string
		observedState         string
		runtimeReference      string
		persistedArtifactID   string
	)

	err = fixture.pool.QueryRow(ctx, `
		SELECT
			id::text,
			observed_state,
			COALESCE(runtime_instance_ref, ''),
			artifact_id::text
		FROM deployments
		WHERE id = $1::uuid
		  AND project_id = $2::uuid
		  AND repository_id = $3::uuid
		  AND last_operation_id = $4::uuid
	`,
		runtime.request.DeploymentID,
		fixture.projectID,
		fixture.repositoryID,
		created.Operation.ID,
	).Scan(
		&persistedDeploymentID,
		&observedState,
		&runtimeReference,
		&persistedArtifactID,
	)
	if err != nil {
		t.Fatalf("read persisted deployment record: %v", err)
	}

	if persistedDeploymentID != runtime.request.DeploymentID {
		t.Errorf(
			"deployment ID = %q, want %q",
			persistedDeploymentID,
			runtime.request.DeploymentID,
		)
	}

	if observedState != "ready" {
		t.Errorf(
			"deployment observed state = %q, want ready after verified readiness",
			observedState,
		)
	}

	if runtimeReference != runtime.result.RuntimeInstanceReference {
		t.Errorf(
			"runtime reference = %q, want %q",
			runtimeReference,
			runtime.result.RuntimeInstanceReference,
		)
	}

	if persistedArtifactID != artifact.ID {
		t.Errorf(
			"deployment artifact ID = %q, want %q",
			persistedArtifactID,
			artifact.ID,
		)
	}

	var successEventCount int

	err = fixture.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM operation_events
		WHERE operation_id = $1::uuid
		  AND event_type = 'operation.succeeded'
	`, created.Operation.ID).Scan(&successEventCount)
	if err != nil {
		t.Fatalf("count deployment success events: %v", err)
	}

	if successEventCount != 1 {
		t.Errorf(
			"deployment success event count = %d, want 1",
			successEventCount,
		)
	}
}
