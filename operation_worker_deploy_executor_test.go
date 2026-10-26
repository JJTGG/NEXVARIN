package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type operationWorkerDeployExecutorTestRuntime struct {
	result  operationWorkerDeployRuntimeResult
	err     error
	calls   int
	request operationWorkerDeployRuntimeRequest
}

func (runtime *operationWorkerDeployExecutorTestRuntime) Deploy(
	_ context.Context,
	request operationWorkerDeployRuntimeRequest,
) (operationWorkerDeployRuntimeResult, error) {
	runtime.calls++
	runtime.request = request

	return runtime.result, runtime.err
}

func prepareOperationWorkerDeployExecutorTestArtifact(
	t *testing.T,
	fixture *operationStoreTestFixture,
	artifact operationWorkerDeployArtifact,
) (operationWorkerDeployArtifact, string) {
	t.Helper()

	root := t.TempDir()

	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatalf("make artifact root private: %v", err)
	}

	digestDirectory := filepath.Join(root, "sha256")
	if err := os.Mkdir(digestDirectory, 0o700); err != nil {
		t.Fatalf("create artifact digest directory: %v", err)
	}

	stagingPath := filepath.Join(root, "staging.oci.tar")
	writeTestOCIArchive(
		t,
		stagingPath,
		validTestOCIArchiveEntries(),
	)

	archiveMetadata, err := inspectOperationWorkerOCIArchive(
		stagingPath,
	)
	if err != nil {
		t.Fatalf("inspect test OCI archive: %v", err)
	}

	digestHex := strings.TrimPrefix(
		archiveMetadata.ArchiveDigest,
		"sha256:",
	)

	artifactPath := filepath.Join(
		digestDirectory,
		digestHex+".oci.tar",
	)

	if err := os.Rename(stagingPath, artifactPath); err != nil {
		t.Fatalf("publish test OCI archive: %v", err)
	}

	if err := os.Chmod(artifactPath, 0o600); err != nil {
		t.Fatalf("make test artifact private: %v", err)
	}

	artifact.ArtifactReference = artifactPath
	artifact.ContentDigest = archiveMetadata.ArchiveDigest
	artifact.Metadata = operationWorkerBuildArtifactMetadata{
		Format:             "oci-archive-tar",
		ImageReference:     artifact.Metadata.ImageReference,
		ContentDigestScope: "complete-archive-file",
		ArchiveDigest:      archiveMetadata.ArchiveDigest,
		OCIIndexDigest:     archiveMetadata.IndexDigest,
		SourceProvider:     "generic_git",
		SourceRevision:     artifact.SourceRevision,
		BuildOperationID:   artifact.BuildOperationID,
	}

	metadataJSON, err := json.Marshal(artifact.Metadata)
	if err != nil {
		t.Fatalf("encode updated artifact metadata: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	result, err := fixture.pool.Exec(ctx, `
		UPDATE artifacts
		SET
			artifact_reference = $2,
			content_digest = $3,
			metadata = $4::jsonb
		WHERE id = $1::uuid
		  AND verification_status = 'verified'
	`,
		artifact.ID,
		artifact.ArtifactReference,
		artifact.ContentDigest,
		string(metadataJSON),
	)
	if err != nil {
		t.Fatalf("update test artifact record: %v", err)
	}

	if result.RowsAffected() != 1 {
		t.Fatalf(
			"updated artifact rows = %d, want 1",
			result.RowsAffected(),
		)
	}

	return artifact, root
}

func newOperationWorkerDeployExecutorTestFixture(
	t *testing.T,
	runtime *operationWorkerDeployExecutorTestRuntime,
) (
	*operationStoreTestFixture,
	operationWorkerTask,
	operationWorkerDeployArtifact,
	operationWorkerDeployExecutor,
) {
	t.Helper()

	fixture := newOperationStoreTestFixture(t)
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

	task := createClaimedDeployTaskForArtifactTest(
		t,
		fixture,
		artifact.ID,
	)

	executor := operationWorkerDeployExecutor{
		Pool:              fixture.pool,
		ArtifactDirectory: artifactDirectory,
		Runtime:           runtime,
	}

	return fixture, task, artifact, executor
}

func TestOperationWorkerDeployExecutorIntegrationSucceeds(
	t *testing.T,
) {
	runtime := &operationWorkerDeployExecutorTestRuntime{
		result: operationWorkerDeployRuntimeResult{
			RuntimeInstanceReference: "test-runtime-instance-001",
			ReadinessVerified:        true,
		},
	}

	fixture, task, artifact, executor :=
		newOperationWorkerDeployExecutorTestFixture(t, runtime)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	result := executor.Execute(ctx, task)

	if result.Disposition != operationExecutionSucceeded {
		t.Fatalf(
			"disposition = %q, want succeeded; code=%q summary=%q",
			result.Disposition,
			result.ErrorCode,
			result.ErrorSummary,
		)
	}

	if runtime.calls != 1 {
		t.Fatalf("runtime calls = %d, want 1", runtime.calls)
	}

	if runtime.request.OperationID != task.Claimed.Operation.ID {
		t.Errorf(
			"runtime operation ID = %q, want %q",
			runtime.request.OperationID,
			task.Claimed.Operation.ID,
		)
	}

	if runtime.request.DeploymentID == "" {
		t.Error("runtime received an empty deployment ID")
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

	// The runner normally records the executor's outcome.
	record, err := recordOperationExecutionResult(
		ctx,
		fixture.pool,
		task,
		result,
	)
	if err != nil {
		t.Fatalf("record successful operation result: %v", err)
	}

	if record.Status != operationStatusSucceeded {
		t.Errorf(
			"operation status = %q, want succeeded",
			record.Status,
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
	`,
		runtime.request.DeploymentID,
		fixture.projectID,
		fixture.repositoryID,
	).Scan(
		&persistedDeploymentID,
		&observedState,
		&runtimeReference,
		&persistedArtifactID,
	)
	if err != nil {
		t.Fatalf("read persisted deployment result: %v", err)
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
			"observed state = %q, want ready after verified readiness",
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
			"persisted artifact ID = %q, want %q",
			persistedArtifactID,
			artifact.ID,
		)
	}
}

func TestOperationWorkerDeployExecutorIntegrationReconcilesUnverifiedReadiness(
	t *testing.T,
) {
	runtime := &operationWorkerDeployExecutorTestRuntime{
		result: operationWorkerDeployRuntimeResult{
			RuntimeInstanceReference: "test-runtime-unready-instance-001",
			ReadinessVerified:        false,
		},
	}

	fixture, task, artifact, executor :=
		newOperationWorkerDeployExecutorTestFixture(t, runtime)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	result := executor.Execute(ctx, task)

	if result.Disposition != operationExecutionNeedsReconciliation {
		t.Fatalf(
			"disposition = %q, want needs_reconciliation; code=%q summary=%q",
			result.Disposition,
			result.ErrorCode,
			result.ErrorSummary,
		)
	}

	if result.ErrorCode != "deployment_readiness_unverified" {
		t.Errorf(
			"error code = %q, want deployment_readiness_unverified",
			result.ErrorCode,
		)
	}

	if runtime.calls != 1 {
		t.Fatalf("runtime calls = %d, want 1", runtime.calls)
	}

	if runtime.request.DeploymentID == "" {
		t.Fatal("runtime received an empty deployment ID")
	}

	// The runner normally records the executor's outcome. The operation
	// must not become successful while the runtime is not verified ready.
	record, err := recordOperationExecutionResult(
		ctx,
		fixture.pool,
		task,
		result,
	)
	if err != nil {
		t.Fatalf("record unverified-readiness result: %v", err)
	}

	if record.Status != operationStatusNeedsReconciliation {
		t.Errorf(
			"operation status = %q, want needs_reconciliation",
			record.Status,
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
	`,
		runtime.request.DeploymentID,
		fixture.projectID,
		fixture.repositoryID,
	).Scan(
		&persistedDeploymentID,
		&observedState,
		&runtimeReference,
		&persistedArtifactID,
	)
	if err != nil {
		t.Fatalf("read deployment with unverified readiness: %v", err)
	}

	if persistedDeploymentID != runtime.request.DeploymentID {
		t.Errorf(
			"deployment ID = %q, want %q",
			persistedDeploymentID,
			runtime.request.DeploymentID,
		)
	}

	if observedState != "deploying" {
		t.Errorf(
			"observed state = %q, want deploying until readiness is verified",
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
			"persisted artifact ID = %q, want %q",
			persistedArtifactID,
			artifact.ID,
		)
	}
}

func TestOperationWorkerDeployExecutorIntegrationRetriesKnownRuntimeFailure(
	t *testing.T,
) {
	runtime := &operationWorkerDeployExecutorTestRuntime{
		err: errors.New("simulated runtime failure before side effects"),
	}

	fixture, task, _, executor :=
		newOperationWorkerDeployExecutorTestFixture(t, runtime)

	executor.RetryDelay = 27 * time.Second

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	result := executor.Execute(ctx, task)

	if result.Disposition != operationExecutionRetry {
		t.Fatalf(
			"disposition = %q, want retry; code=%q",
			result.Disposition,
			result.ErrorCode,
		)
	}

	if result.ErrorCode != "deployment_runtime_failed" {
		t.Errorf(
			"error code = %q, want deployment_runtime_failed",
			result.ErrorCode,
		)
	}

	if result.RetryDelay != executor.RetryDelay {
		t.Errorf(
			"retry delay = %v, want %v",
			result.RetryDelay,
			executor.RetryDelay,
		)
	}

	if runtime.calls != 1 {
		t.Errorf("runtime calls = %d, want 1", runtime.calls)
	}

	record, err := recordOperationExecutionResult(
		ctx,
		fixture.pool,
		task,
		result,
	)
	if err != nil {
		t.Fatalf("record retryable operation result: %v", err)
	}

	if record.Status != operationStatusRetryWait {
		t.Errorf(
			"operation status = %q, want retry_wait",
			record.Status,
		)
	}

	var (
		deploymentCount  int
		observedState    string
		runtimeReference string
	)

	err = fixture.pool.QueryRow(ctx, `
		SELECT
			count(*),
			COALESCE(MAX(observed_state), ''),
			COALESCE(MAX(runtime_instance_ref), '')
		FROM deployments
		WHERE project_id = $1::uuid
		  AND repository_id = $2::uuid
		  AND last_operation_id = $3::uuid
	`,
		fixture.projectID,
		fixture.repositoryID,
		task.Claimed.Operation.ID,
	).Scan(
		&deploymentCount,
		&observedState,
		&runtimeReference,
	)
	if err != nil {
		t.Fatalf("read deployment after runtime failure: %v", err)
	}

	if deploymentCount != 1 {
		t.Errorf(
			"deployment count = %d, want 1",
			deploymentCount,
		)
	}

	if observedState != "pending" {
		t.Errorf(
			"observed state = %q, want pending",
			observedState,
		)
	}

	if runtimeReference != "" {
		t.Errorf(
			"runtime reference = %q, want empty",
			runtimeReference,
		)
	}
}

func TestOperationWorkerDeployExecutorIntegrationReconcilesUnknownOutcome(
	t *testing.T,
) {
	runtime := &operationWorkerDeployExecutorTestRuntime{
		err: errOperationWorkerDeployRuntimeOutcomeUnknown,
	}

	fixture, task, _, executor :=
		newOperationWorkerDeployExecutorTestFixture(t, runtime)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	result := executor.Execute(ctx, task)

	if result.Disposition != operationExecutionNeedsReconciliation {
		t.Fatalf(
			"disposition = %q, want needs_reconciliation; code=%q",
			result.Disposition,
			result.ErrorCode,
		)
	}

	if result.ErrorCode != "deployment_outcome_unknown" {
		t.Errorf(
			"error code = %q, want deployment_outcome_unknown",
			result.ErrorCode,
		)
	}

	if runtime.calls != 1 {
		t.Errorf("runtime calls = %d, want 1", runtime.calls)
	}

	record, err := recordOperationExecutionResult(
		ctx,
		fixture.pool,
		task,
		result,
	)
	if err != nil {
		t.Fatalf("record uncertain operation result: %v", err)
	}

	if record.Status != operationStatusNeedsReconciliation {
		t.Errorf(
			"operation status = %q, want needs_reconciliation",
			record.Status,
		)
	}

	var (
		deploymentCount  int
		observedState    string
		runtimeReference string
	)

	err = fixture.pool.QueryRow(ctx, `
		SELECT
			count(*),
			COALESCE(MAX(observed_state), ''),
			COALESCE(MAX(runtime_instance_ref), '')
		FROM deployments
		WHERE project_id = $1::uuid
		  AND repository_id = $2::uuid
		  AND last_operation_id = $3::uuid
	`,
		fixture.projectID,
		fixture.repositoryID,
		task.Claimed.Operation.ID,
	).Scan(
		&deploymentCount,
		&observedState,
		&runtimeReference,
	)
	if err != nil {
		t.Fatalf("read deployment after uncertain outcome: %v", err)
	}

	if deploymentCount != 1 {
		t.Errorf(
			"deployment count = %d, want 1",
			deploymentCount,
		)
	}

	if observedState != "pending" {
		t.Errorf(
			"observed state = %q, want pending until reconciled",
			observedState,
		)
	}

	if runtimeReference != "" {
		t.Errorf(
			"runtime reference = %q, want empty until confirmed",
			runtimeReference,
		)
	}
}
