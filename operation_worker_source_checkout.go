package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

const operationWorkerSourceCheckoutTimeout = 2 * time.Minute

// operationWorkerSourceCheckout represents a temporary, verified checkout.
// The caller owns the directory and must clean it up when finished.
type operationWorkerSourceCheckout struct {
	directory string
	revision  string
}

// Directory returns the working directory containing the checked-out source.
func (checkout operationWorkerSourceCheckout) Directory() string {
	return checkout.directory
}

// Revision returns the verified Git commit identifier.
func (checkout operationWorkerSourceCheckout) Revision() string {
	return checkout.revision
}

// Cleanup removes the temporary checkout and its Git metadata.
func (checkout operationWorkerSourceCheckout) Cleanup() error {
	if checkout.directory == "" {
		return nil
	}

	if err := os.RemoveAll(checkout.directory); err != nil {
		return fmt.Errorf("remove temporary source checkout: %w", err)
	}

	return nil
}

// checkoutOperationWorkerSource retrieves the exact requested commit from
// a previously validated, registered repository.
//
// This initial implementation supports unauthenticated HTTPS Git access.
// It is intended for operator-controlled repositories, not arbitrary
// untrusted customer repositories. Network-level egress restrictions,
// resource isolation, and private-repository credentials are separate
// requirements before broader use.
func checkoutOperationWorkerSource(
	ctx context.Context,
	source operationWorkerSource,
	revision string,
) (operationWorkerSourceCheckout, error) {
	if ctx == nil {
		return operationWorkerSourceCheckout{}, errors.New(
			"source checkout context is required",
		)
	}

	if !isValidGitRevision(revision) {
		return operationWorkerSourceCheckout{}, errors.New(
			"source revision must be a 40- or 64-character lowercase hexadecimal identifier",
		)
	}

	switch source.Provider {
	case "github", "gitlab", "generic_git":
		// Supported registered provider types.
	default:
		return operationWorkerSourceCheckout{}, errors.New(
			"source provider is not supported",
		)
	}

	canonicalURL, err := validateOperationWorkerSourceURL(
		source.CanonicalURL,
	)
	if err != nil {
		return operationWorkerSourceCheckout{}, err
	}

	checkoutContext, cancel := context.WithTimeout(
		ctx,
		operationWorkerSourceCheckoutTimeout,
	)
	defer cancel()

	directory, err := os.MkdirTemp("", "nexvarin-source-")
	if err != nil {
		return operationWorkerSourceCheckout{}, fmt.Errorf(
			"create temporary source directory: %w",
			err,
		)
	}

	checkout := operationWorkerSourceCheckout{
		directory: directory,
		revision:  revision,
	}

	cleanupOnError := func(cause error) (
		operationWorkerSourceCheckout,
		error,
	) {
		if cleanupErr := checkout.Cleanup(); cleanupErr != nil {
			return operationWorkerSourceCheckout{}, errors.Join(
				cause,
				cleanupErr,
			)
		}

		return operationWorkerSourceCheckout{}, cause
	}

	// Git uses SHA-1 by default. Initialize SHA-256 repositories explicitly
	// when the requested revision uses a 64-character object identifier.
	initArgs := []string{"init", "--quiet"}
	if len(revision) == 64 {
		initArgs = append(initArgs, "--object-format=sha256")
	}

	if _, err := runOperationWorkerSourceGit(
		checkoutContext,
		directory,
		initArgs...,
	); err != nil {
		return cleanupOnError(fmt.Errorf(
			"initialize temporary Git repository: %w",
			err,
		))
	}

	if _, err := runOperationWorkerSourceGit(
		checkoutContext,
		directory,
		"remote",
		"add",
		"origin",
		canonicalURL,
	); err != nil {
		return cleanupOnError(fmt.Errorf(
			"configure source repository: %w",
			err,
		))
	}

	// Fetch only the requested commit and omit tags. No shell is used, and
	// the revision is validated before being passed to Git.
	if _, err := runOperationWorkerSourceGit(
		checkoutContext,
		directory,
		"fetch",
		"--no-tags",
		"--depth=1",
		"origin",
		revision,
	); err != nil {
		return cleanupOnError(fmt.Errorf(
			"fetch requested source revision: %w",
			err,
		))
	}

	if _, err := runOperationWorkerSourceGit(
		checkoutContext,
		directory,
		"checkout",
		"--quiet",
		"--detach",
		"--force",
		"FETCH_HEAD",
	); err != nil {
		return cleanupOnError(fmt.Errorf(
			"check out requested source revision: %w",
			err,
		))
	}

	output, err := runOperationWorkerSourceGit(
		checkoutContext,
		directory,
		"rev-parse",
		"--verify",
		"HEAD",
	)
	if err != nil {
		return cleanupOnError(fmt.Errorf(
			"verify checked-out source revision: %w",
			err,
		))
	}

	actualRevision := strings.TrimSpace(string(output))
	if actualRevision != revision {
		return cleanupOnError(errors.New(
			"checked-out Git revision does not match the requested revision",
		))
	}

	return checkout, nil
}

// runOperationWorkerSourceGit invokes Git without a shell, disables system
// and user configuration, prevents interactive credential prompts, disables
// hooks, and restricts Git transport protocols to HTTPS.
//
// These controls reduce accidental execution and configuration surprises;
// they do not replace OS-level network isolation or a build sandbox.
func runOperationWorkerSourceGit(
	ctx context.Context,
	directory string,
	args ...string,
) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("Git command context is required")
	}
	if strings.TrimSpace(directory) == "" {
		return nil, errors.New("Git working directory is required")
	}
	if len(args) == 0 {
		return nil, errors.New("Git command is required")
	}

	commandArgs := []string{
		"-C",
		directory,
		"-c",
		"core.hooksPath=" + os.DevNull,
		"-c",
		"protocol.file.allow=never",
	}
	commandArgs = append(commandArgs, args...)

	command := exec.CommandContext(
		ctx,
		"git",
		commandArgs...,
	)

	// Deliberately do not inherit arbitrary system or user Git settings.
	// The temporary directory also serves as an isolated HOME directory.
	command.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + directory,
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_TERMINAL_PROMPT=0",
		"GIT_ASKPASS=" + os.DevNull,
		"GIT_LFS_SKIP_SMUDGE=1",
		"GIT_ALLOW_PROTOCOL=https",
		"GIT_NO_REPLACE_OBJECTS=1",
		"LC_ALL=C",
	}

	var stdout bytes.Buffer

	command.Stdout = &stdout
	command.Stderr = io.Discard

	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf(
				"Git %s interrupted: %w",
				args[0],
				ctx.Err(),
			)
		}

		return nil, fmt.Errorf(
			"Git %s failed: %w",
			args[0],
			err,
		)
	}

	return stdout.Bytes(), nil
}
