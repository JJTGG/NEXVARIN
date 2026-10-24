package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	operationWorkerDeployPodmanCommandTimeout = 2 * time.Minute
	operationWorkerDeployPodmanOutputLimit    = 4 << 20

	operationWorkerDeployPodmanReadinessTimeout       = 20 * time.Second
	operationWorkerDeployPodmanReadinessProbeTimeout  = 2 * time.Second
	operationWorkerDeployPodmanReadinessProbeInterval = 250 * time.Millisecond

	operationWorkerDeployPodmanOperationLabel       = "io.nexvarin.operation-id"
	operationWorkerDeployPodmanDeploymentLabel      = "io.nexvarin.deployment-id"
	operationWorkerDeployPodmanDigestLabel          = "io.nexvarin.artifact-digest"
	operationWorkerDeployPodmanImageLabel           = "io.nexvarin.image-reference"
	operationWorkerDeployPodmanApplicationPortLabel = "io.nexvarin.application-port"

	operationWorkerDeployPodmanNamePrefix = "nexvarin-deploy-"
)

var errOperationWorkerDeployPodmanCommandInterrupted = errors.New(
	"Podman command was interrupted",
)

// operationWorkerDeployPodmanRuntime runs verified OCI artifacts through
// a local rootless Podman installation.
//
// Deployments with a validated application port publish that container port
// on a dynamically assigned host port bound to 127.0.0.1. The runtime confirms
// the binding and waits for a successful HTTP response before reporting
// deployment success. A zero application port retains isolated mode for
// internal adapter tests; API-created deploy requests cannot use that mode.
// Preview routing is not activated by this adapter.
type operationWorkerDeployPodmanRuntime struct {
	PodmanPath     string
	CommandTimeout time.Duration
}

type operationWorkerDeployPodmanContainer struct {
	ID     string `json:"Id"`
	Config struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	State struct {
		Running bool   `json:"Running"`
		Status  string `json:"Status"`
	} `json:"State"`
}

func (runtime operationWorkerDeployPodmanRuntime) Deploy(
	ctx context.Context,
	request operationWorkerDeployRuntimeRequest,
) (operationWorkerDeployRuntimeResult, error) {
	if ctx == nil {
		return operationWorkerDeployRuntimeResult{}, errors.New(
			"Podman deployment context is required",
		)
	}

	if err := runtime.validateRequest(request); err != nil {
		return operationWorkerDeployRuntimeResult{}, err
	}

	if ctx.Err() != nil {
		return operationWorkerDeployRuntimeResult{}, ctx.Err()
	}

	// Verify the actual Podman host is rootless before inspecting or
	// changing any deployment container.
	if err := runtime.verifyRootless(ctx); err != nil {
		return operationWorkerDeployRuntimeResult{}, err
	}

	containerName := operationWorkerDeployPodmanContainerName(
		request.DeploymentID,
	)

	container, exists, err := runtime.inspectContainer(
		ctx,
		containerName,
	)
	if err != nil {
		return operationWorkerDeployRuntimeResult{}, unknownPodmanDeployOutcome(
			"the existing container state could not be confirmed",
		)
	}

	if exists {
		if !runtime.containerMatches(container, request) {
			return operationWorkerDeployRuntimeResult{}, unknownPodmanDeployOutcome(
				"the deterministic container name belongs to an unexpected container",
			)
		}

		if container.State.Running {
			return runtime.readyResultForContainer(
				ctx,
				containerName,
				request,
				container,
			)
		}

		return runtime.startAndConfirm(
			ctx,
			containerName,
			request,
			container,
		)
	}

	// Loading an image does not start an application container. A failed
	// load can therefore be retried without creating a duplicate instance.
	_, exitCode, err := runtime.runPodman(
		ctx,
		"load",
		"--input",
		request.ArtifactPath,
	)
	if err != nil || exitCode != 0 {
		return operationWorkerDeployRuntimeResult{}, errors.New(
			"Podman could not load the verified OCI artifact",
		)
	}

	arguments := []string{
		"run",
		"--detach",
		"--name",
		containerName,

		"--label",
		operationWorkerDeployPodmanOperationLabel + "=" + request.OperationID,
		"--label",
		operationWorkerDeployPodmanDeploymentLabel + "=" + request.DeploymentID,
		"--label",
		operationWorkerDeployPodmanDigestLabel + "=" + request.ArtifactDigest,
		"--label",
		operationWorkerDeployPodmanImageLabel + "=" + request.ImageReference,

		"--pull=never",
	}

	if request.ApplicationPort == 0 {
		arguments = append(arguments, "--network=none")
	} else {
		arguments = append(
			arguments,
			"--network=slirp4netns",
			"--publish",
			fmt.Sprintf("127.0.0.1::%d/tcp", request.ApplicationPort),
			"--label",
			operationWorkerDeployPodmanApplicationPortLabel+"="+strconv.Itoa(
				request.ApplicationPort,
			),
		)
	}

	arguments = append(
		arguments,
		"--cap-drop=ALL",
		"--security-opt=no-new-privileges",
		"--pids-limit=256",
		"--memory=512m",
		"--cpus=1",
		"--tmpfs",
		"/tmp:rw,noexec,nosuid,nodev,size=64m",
		request.ImageReference,
	)

	_, exitCode, err = runtime.runPodman(ctx, arguments...)
	if err != nil {
		if errors.Is(
			err,
			errOperationWorkerDeployPodmanCommandInterrupted,
		) {
			return operationWorkerDeployRuntimeResult{},
				unknownPodmanDeployOutcome(
					"container startup was interrupted before its result could be confirmed",
				)
		}

		return operationWorkerDeployRuntimeResult{}, errors.New(
			"Podman could not execute the deployment command",
		)
	}

	if exitCode != 0 {
		// Podman can fail after creating a container. Inspect the
		// deterministic name before deciding whether a retry is safe.
		container, exists, inspectErr := runtime.inspectContainer(
			ctx,
			containerName,
		)
		if inspectErr != nil {
			return operationWorkerDeployRuntimeResult{},
				unknownPodmanDeployOutcome(
					"container creation failed and its outcome could not be inspected",
				)
		}

		if !exists {
			return operationWorkerDeployRuntimeResult{}, errors.New(
				"Podman could not create the deployment container",
			)
		}

		if !runtime.containerMatches(container, request) {
			return operationWorkerDeployRuntimeResult{},
				unknownPodmanDeployOutcome(
					"an unexpected container appeared during deployment",
				)
		}

		return runtime.startAndConfirm(
			ctx,
			containerName,
			request,
			container,
		)
	}

	container, exists, err = runtime.inspectContainer(
		ctx,
		containerName,
	)
	if err != nil || !exists {
		return operationWorkerDeployRuntimeResult{},
			unknownPodmanDeployOutcome(
				"container creation succeeded but the resulting state could not be confirmed",
			)
	}

	if !runtime.containerMatches(container, request) {
		return operationWorkerDeployRuntimeResult{},
			unknownPodmanDeployOutcome(
				"the created container did not match the requested deployment",
			)
	}

	if !container.State.Running {
		return operationWorkerDeployRuntimeResult{},
			unknownPodmanDeployOutcome(
				"the container did not remain running after startup",
			)
	}

	return runtime.readyResultForContainer(
		ctx,
		containerName,
		request,
		container,
	)
}

func (runtime operationWorkerDeployPodmanRuntime) validateRequest(
	request operationWorkerDeployRuntimeRequest,
) error {
	if strings.TrimSpace(runtime.PodmanPath) != runtime.PodmanPath ||
		runtime.PodmanPath == "" ||
		!filepath.IsAbs(runtime.PodmanPath) {
		return errors.New(
			"Podman executable must be an absolute path",
		)
	}

	operationID, err := normalizeOperationUUID(request.OperationID)
	if err != nil || operationID != request.OperationID {
		return errors.New("deployment operation ID is invalid")
	}

	deploymentID, err := normalizeOperationUUID(request.DeploymentID)
	if err != nil || deploymentID != request.DeploymentID {
		return errors.New("deployment ID is invalid")
	}

	if request.ArtifactPath == "" ||
		strings.TrimSpace(request.ArtifactPath) != request.ArtifactPath ||
		!filepath.IsAbs(request.ArtifactPath) {
		return errors.New(
			"deployment artifact path must be absolute",
		)
	}

	if !validOperationWorkerDeployPodmanDigest(
		request.ArtifactDigest,
	) {
		return errors.New("deployment artifact digest is invalid")
	}

	if !validOperationWorkerDeployPodmanImageReference(
		request.ImageReference,
	) {
		return errors.New("deployment image reference is invalid")
	}

	if request.ApplicationPort < 0 ||
		request.ApplicationPort > maxOperationApplicationPort {
		return errors.New(
			"deployment application port must be zero for isolated mode or between 1 and 65535",
		)
	}

	return nil
}

// verifyRootless requires Podman to explicitly report that its host is
// running rootless. Unknown output, a rootful host, or a failed info command
// all block deployment before any container operation is attempted.
func (runtime operationWorkerDeployPodmanRuntime) verifyRootless(
	ctx context.Context,
) error {
	output, exitCode, err := runtime.runPodman(
		ctx,
		"info",
		"--format",
		"{{.Host.Security.Rootless}}",
	)
	if err != nil || exitCode != 0 {
		return errors.New(
			"Podman rootless mode could not be verified",
		)
	}

	if strings.TrimSpace(string(output)) != "true" {
		return errors.New(
			"Podman must be running in rootless mode",
		)
	}

	return nil
}

func validOperationWorkerDeployPodmanDigest(value string) bool {
	const prefix = "sha256:"

	if !strings.HasPrefix(value, prefix) {
		return false
	}

	digest := strings.TrimPrefix(value, prefix)
	if len(digest) != 64 {
		return false
	}

	decoded, err := hex.DecodeString(digest)
	if err != nil {
		return false
	}

	return hex.EncodeToString(decoded) == digest
}

func validOperationWorkerDeployPodmanImageReference(
	value string,
) bool {
	const (
		prefix = "localhost/nexvarin/build-"
		suffix = ":latest"
	)

	if !strings.HasPrefix(value, prefix) ||
		!strings.HasSuffix(value, suffix) {
		return false
	}

	buildOperationID := value[len(prefix) : len(value)-len(suffix)]

	normalizedID, err := normalizeOperationUUID(buildOperationID)
	if err != nil || normalizedID != buildOperationID {
		return false
	}

	expected, err := operationWorkerBuildImageReference(
		buildOperationID,
	)

	return err == nil && expected == value
}

func operationWorkerDeployPodmanContainerName(
	deploymentID string,
) string {
	compactID := strings.ReplaceAll(deploymentID, "-", "")

	return operationWorkerDeployPodmanNamePrefix + compactID
}

func (runtime operationWorkerDeployPodmanRuntime) inspectContainer(
	ctx context.Context,
	name string,
) (*operationWorkerDeployPodmanContainer, bool, error) {
	_, exitCode, err := runtime.runPodman(
		ctx,
		"container",
		"exists",
		name,
	)
	if err != nil {
		return nil, false, err
	}

	switch exitCode {
	case 1:
		return nil, false, nil
	case 0:
		// Continue to inspect the existing container.
	default:
		return nil, false, errors.New(
			"Podman could not establish whether the container exists",
		)
	}

	output, exitCode, err := runtime.runPodman(
		ctx,
		"container",
		"inspect",
		name,
	)
	if err != nil {
		return nil, true, err
	}

	if exitCode != 0 {
		return nil, true, errors.New(
			"Podman could not inspect the existing container",
		)
	}

	var containers []operationWorkerDeployPodmanContainer

	if err := json.Unmarshal(output, &containers); err != nil {
		return nil, true, errors.New(
			"Podman returned invalid container inspection data",
		)
	}

	if len(containers) != 1 {
		return nil, true, errors.New(
			"Podman returned an unexpected number of inspected containers",
		)
	}

	return &containers[0], true, nil
}

func (runtime operationWorkerDeployPodmanRuntime) containerMatches(
	container *operationWorkerDeployPodmanContainer,
	request operationWorkerDeployRuntimeRequest,
) bool {
	if container == nil {
		return false
	}

	labels := container.Config.Labels
	expectedApplicationPort := ""
	if request.ApplicationPort > 0 {
		expectedApplicationPort = strconv.Itoa(request.ApplicationPort)
	}

	return labels[operationWorkerDeployPodmanOperationLabel] ==
		request.OperationID &&
		labels[operationWorkerDeployPodmanDeploymentLabel] ==
			request.DeploymentID &&
		labels[operationWorkerDeployPodmanDigestLabel] ==
			request.ArtifactDigest &&
		labels[operationWorkerDeployPodmanImageLabel] ==
			request.ImageReference &&
		labels[operationWorkerDeployPodmanApplicationPortLabel] ==
			expectedApplicationPort
}

func (runtime operationWorkerDeployPodmanRuntime) startAndConfirm(
	ctx context.Context,
	name string,
	request operationWorkerDeployRuntimeRequest,
	existing *operationWorkerDeployPodmanContainer,
) (operationWorkerDeployRuntimeResult, error) {
	if runtime.containerMatches(existing, request) &&
		existing.State.Running {
		return runtime.readyResultForContainer(
			ctx,
			name,
			request,
			existing,
		)
	}

	_, _, startErr := runtime.runPodman(
		ctx,
		"start",
		name,
	)
	if errors.Is(
		startErr,
		errOperationWorkerDeployPodmanCommandInterrupted,
	) {
		return operationWorkerDeployRuntimeResult{},
			unknownPodmanDeployOutcome(
				"container start was interrupted before its result could be confirmed",
			)
	}

	// Even if Podman reports a start error, inspect again. The container
	// might have started before the command returned.
	container, exists, inspectErr := runtime.inspectContainer(
		ctx,
		name,
	)
	if inspectErr != nil {
		return operationWorkerDeployRuntimeResult{},
			unknownPodmanDeployOutcome(
				"the result of restarting the deployment container could not be confirmed",
			)
	}

	if !exists {
		return operationWorkerDeployRuntimeResult{}, errors.New(
			"deployment container disappeared before startup was confirmed",
		)
	}

	if !runtime.containerMatches(container, request) {
		return operationWorkerDeployRuntimeResult{},
			unknownPodmanDeployOutcome(
				"the container identity changed during startup",
			)
	}

	if !container.State.Running {
		return operationWorkerDeployRuntimeResult{},
			unknownPodmanDeployOutcome(
				"the container is not running after the start attempt",
			)
	}

	return runtime.readyResultForContainer(
		ctx,
		name,
		request,
		container,
	)
}

func (runtime operationWorkerDeployPodmanRuntime) readyResultForContainer(
	ctx context.Context,
	name string,
	request operationWorkerDeployRuntimeRequest,
	container *operationWorkerDeployPodmanContainer,
) (operationWorkerDeployRuntimeResult, error) {
	result, err := runtime.resultForContainer(container)
	if err != nil {
		return operationWorkerDeployRuntimeResult{}, err
	}

	if request.ApplicationPort == 0 {
		return result, nil
	}

	hostPort, err := runtime.publishedApplicationHostPort(
		ctx,
		name,
		request.ApplicationPort,
	)
	if err != nil {
		return operationWorkerDeployRuntimeResult{},
			unknownPodmanDeployOutcome(
				"the published application port could not be verified: " + err.Error(),
			)
	}

	if err := runtime.waitForApplicationHTTP(ctx, hostPort); err != nil {
		return operationWorkerDeployRuntimeResult{},
			unknownPodmanDeployOutcome(
				"application HTTP readiness could not be confirmed: " + err.Error(),
			)
	}

	// Readiness is asserted only after the loopback HTTP probe succeeds.
	result.ReadinessVerified = true

	return result, nil
}

// publishedApplicationHostPort accepts Podman's documented mapping form
// ("8080/tcp -> 127.0.0.1:12345") and a bare host address, but rejects
// wildcard, non-loopback, malformed, or ambiguous bindings.
func (runtime operationWorkerDeployPodmanRuntime) publishedApplicationHostPort(
	ctx context.Context,
	containerName string,
	applicationPort int,
) (string, error) {
	output, exitCode, err := runtime.runPodman(
		ctx,
		"port",
		containerName,
		strconv.Itoa(applicationPort)+"/tcp",
	)
	if err != nil {
		return "", errors.New("Podman port inspection failed")
	}

	if exitCode != 0 {
		return "", errors.New("Podman did not report a published application port")
	}

	lines := make([]string, 0, 1)
	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			lines = append(lines, line)
		}
	}

	if len(lines) != 1 {
		return "", errors.New("expected exactly one published application port binding")
	}

	binding := lines[0]
	if strings.Contains(binding, "->") {
		parts := strings.Split(binding, "->")
		if len(parts) != 2 ||
			strings.TrimSpace(parts[0]) != strconv.Itoa(applicationPort)+"/tcp" {
			return "", errors.New("Podman returned an unexpected port mapping")
		}

		binding = strings.TrimSpace(parts[1])
	}

	host, hostPort, err := net.SplitHostPort(binding)
	if err != nil {
		return "", errors.New("Podman returned a malformed host port binding")
	}

	if host != "127.0.0.1" {
		return "", errors.New("application port is not bound exclusively to 127.0.0.1")
	}

	portNumber, err := strconv.Atoi(hostPort)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return "", errors.New("Podman returned an invalid dynamically assigned host port")
	}

	return hostPort, nil
}

// waitForApplicationHTTP probes only the loopback host binding. It bypasses
// environment proxies and does not follow redirects, so readiness cannot be
// satisfied by a proxy or a redirect to a different host.
func (runtime operationWorkerDeployPodmanRuntime) waitForApplicationHTTP(
	ctx context.Context,
	hostPort string,
) error {
	target := "http://" + net.JoinHostPort("127.0.0.1", hostPort) + "/"

	dialer := &net.Dialer{
		Timeout: operationWorkerDeployPodmanReadinessProbeTimeout,
	}
	transport := &http.Transport{
		Proxy:       nil,
		DialContext: dialer.DialContext,
	}
	defer transport.CloseIdleConnections()

	client := &http.Client{
		Transport: transport,
		Timeout:   operationWorkerDeployPodmanReadinessProbeTimeout,
		CheckRedirect: func(
			_ *http.Request,
			_ []*http.Request,
		) error {
			return http.ErrUseLastResponse
		},
	}

	deadline := time.Now().Add(operationWorkerDeployPodmanReadinessTimeout)
	lastResult := "the HTTP server did not become ready before the deadline"

	for {
		if ctx.Err() != nil {
			return fmt.Errorf("readiness probe context ended: %w", ctx.Err())
		}

		if !time.Now().Before(deadline) {
			break
		}

		probeContext, cancelProbe := context.WithTimeout(
			ctx,
			operationWorkerDeployPodmanReadinessProbeTimeout,
		)

		request, err := http.NewRequestWithContext(
			probeContext,
			http.MethodGet,
			target,
			nil,
		)
		if err != nil {
			cancelProbe()
			return fmt.Errorf("create HTTP readiness request: %w", err)
		}

		response, requestErr := client.Do(request)
		if requestErr != nil {
			lastResult = requestErr.Error()
			cancelProbe()
		} else {
			_, readErr := io.Copy(
				io.Discard,
				io.LimitReader(response.Body, 4096),
			)
			closeErr := response.Body.Close()
			statusCode := response.StatusCode
			cancelProbe()

			if statusCode >= http.StatusOK &&
				statusCode < http.StatusMultipleChoices &&
				readErr == nil &&
				closeErr == nil {
				return nil
			}

			lastResult = fmt.Sprintf(
				"status=%d response_read_error=%v response_close_error=%v",
				statusCode,
				readErr,
				closeErr,
			)
		}

		if ctx.Err() != nil {
			return fmt.Errorf("readiness probe context ended: %w", ctx.Err())
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		delay := operationWorkerDeployPodmanReadinessProbeInterval
		if remaining < delay {
			delay = remaining
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("readiness probe context ended: %w", ctx.Err())
		case <-timer.C:
		}
	}

	return fmt.Errorf(
		"HTTP readiness failed at %s: %s",
		target,
		lastResult,
	)
}

func (runtime operationWorkerDeployPodmanRuntime) resultForContainer(
	container *operationWorkerDeployPodmanContainer,
) (operationWorkerDeployRuntimeResult, error) {
	if container == nil ||
		!validDeployRuntimeInstanceReference(container.ID) {
		return operationWorkerDeployRuntimeResult{},
			unknownPodmanDeployOutcome(
				"Podman did not provide a valid container instance reference",
			)
	}

	return operationWorkerDeployRuntimeResult{
		RuntimeInstanceReference: container.ID,
	}, nil
}

func unknownPodmanDeployOutcome(reason string) error {
	return fmt.Errorf(
		"%w: %s",
		errOperationWorkerDeployRuntimeOutcomeUnknown,
		reason,
	)
}

func (runtime operationWorkerDeployPodmanRuntime) runPodman(
	ctx context.Context,
	arguments ...string,
) ([]byte, int, error) {
	if ctx == nil {
		return nil, -1, errors.New(
			"Podman command context is required",
		)
	}

	if runtime.PodmanPath == "" ||
		!filepath.IsAbs(runtime.PodmanPath) {
		return nil, -1, errors.New(
			"Podman executable path is invalid",
		)
	}

	timeout := runtime.CommandTimeout
	if timeout <= 0 {
		timeout = operationWorkerDeployPodmanCommandTimeout
	}

	commandContext, cancel := context.WithTimeout(
		ctx,
		timeout,
	)
	defer cancel()

	command := exec.CommandContext(
		commandContext,
		runtime.PodmanPath,
		arguments...,
	)

	output := operationWorkerDeployPodmanLimitedOutput{
		limit: operationWorkerDeployPodmanOutputLimit,
	}

	command.Stdout = &output
	command.Stderr = io.Discard

	err := command.Run()

	if commandContext.Err() != nil {
		return output.data, -1, fmt.Errorf(
			"%w: %v",
			errOperationWorkerDeployPodmanCommandInterrupted,
			commandContext.Err(),
		)
	}

	if err == nil {
		return output.data, 0, nil
	}

	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return output.data, exitError.ExitCode(), nil
	}

	return output.data, -1, errors.New(
		"could not start the Podman command",
	)
}

type operationWorkerDeployPodmanLimitedOutput struct {
	data  []byte
	limit int
}

func (output *operationWorkerDeployPodmanLimitedOutput) Write(
	value []byte,
) (int, error) {
	length := len(value)
	remaining := output.limit - len(output.data)

	if remaining > 0 {
		if len(value) > remaining {
			value = value[:remaining]
		}

		output.data = append(output.data, value...)
	}

	// Report the full write so the child process can finish even when
	// command output exceeds the amount we retain in memory.
	return length, nil
}

var _ operationWorkerDeployRuntime = operationWorkerDeployPodmanRuntime{}
