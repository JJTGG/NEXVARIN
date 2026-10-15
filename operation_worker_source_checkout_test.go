package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func installFakeOperationWorkerGit(
	t *testing.T,
	actualRevision string,
	failCommand string,
) string {
	t.Helper()

	binDirectory := t.TempDir()
	logPath := filepath.Join(binDirectory, "git-calls.log")
	gitPath := filepath.Join(binDirectory, "git")

	script := "#!/bin/sh\nset -eu\n"
	script += "printf '%s\\n' \"$*\" >> " + shellQuote(logPath) + "\n"

	if failCommand == "fetch" {
		script += `
case "$*" in
	*" fetch --no-tags --depth=1 origin "*) exit 23 ;;
esac
`
	}

	script += `
case "$*" in
	*" rev-parse --verify HEAD"*)
		printf '%s\n' ` + shellQuote(actualRevision) + `
		;;
esac

exit 0
`

	if err := os.WriteFile(gitPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake Git executable: %v", err)
	}

	// The production helper invokes Git through PATH. The fake executable
	// records the requested arguments without accessing a network.
	t.Setenv("PATH", binDirectory)

	return logPath
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func readFakeOperationWorkerGitLog(t *testing.T, path string) string {
	t.Helper()

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fake Git log: %v", err)
	}

	return string(content)
}

func temporaryDirectoryFromGitLog(t *testing.T, log string) string {
	t.Helper()

	firstLine := strings.SplitN(strings.TrimSpace(log), "\n", 2)[0]
	if !strings.HasPrefix(firstLine, "-C ") {
		t.Fatalf("unexpected first Git invocation: %q", firstLine)
	}

	arguments := strings.TrimPrefix(firstLine, "-C ")
	index := strings.Index(arguments, " -c ")
	if index < 0 {
		t.Fatalf("could not find Git working-directory boundary: %q", firstLine)
	}

	return arguments[:index]
}

func TestCheckoutOperationWorkerSourceVerifiesRequestedRevision(
	t *testing.T,
) {
	revision := strings.Repeat("a", 40)
	logPath := installFakeOperationWorkerGit(t, revision, "")

	checkout, err := checkoutOperationWorkerSource(
		context.Background(),
		operationWorkerSource{
			Provider:     "generic_git",
			CanonicalURL: "https://example.com/owner/repo.git",
		},
		revision,
	)
	if err != nil {
		t.Fatalf("checkout operation source: %v", err)
	}

	t.Cleanup(func() {
		if err := checkout.Cleanup(); err != nil {
			t.Errorf("clean up source checkout: %v", err)
		}
	})

	if checkout.Directory() == "" {
		t.Fatal("checkout directory is empty")
	}

	if checkout.Revision() != revision {
		t.Errorf(
			"checkout revision = %q, want %q",
			checkout.Revision(),
			revision,
		)
	}

	log := readFakeOperationWorkerGitLog(t, logPath)

	expectedCalls := []string{
		"init --quiet",
		"remote add origin https://example.com/owner/repo.git",
		"fetch --no-tags --depth=1 origin " + revision,
		"checkout --quiet --detach --force FETCH_HEAD",
		"rev-parse --verify HEAD",
	}

	for _, expected := range expectedCalls {
		if !strings.Contains(log, expected) {
			t.Errorf("Git log does not contain %q:\n%s", expected, log)
		}
	}

	directory := checkout.Directory()

	if err := checkout.Cleanup(); err != nil {
		t.Fatalf("clean up source checkout: %v", err)
	}

	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Errorf("checkout directory still exists or could not be checked: %v", err)
	}
}

func TestCheckoutOperationWorkerSourceRejectsRevisionMismatch(
	t *testing.T,
) {
	requestedRevision := strings.Repeat("a", 40)
	actualRevision := strings.Repeat("b", 40)
	logPath := installFakeOperationWorkerGit(t, actualRevision, "")

	_, err := checkoutOperationWorkerSource(
		context.Background(),
		operationWorkerSource{
			Provider:     "generic_git",
			CanonicalURL: "https://example.com/owner/repo.git",
		},
		requestedRevision,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "does not match the requested revision") {
		t.Fatalf("checkout error = %v, want revision mismatch", err)
	}

	log := readFakeOperationWorkerGitLog(t, logPath)
	directory := temporaryDirectoryFromGitLog(t, log)

	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Errorf("failed checkout directory still exists or could not be checked: %v", err)
	}
}

func TestCheckoutOperationWorkerSourceCleansUpAfterFetchFailure(
	t *testing.T,
) {
	revision := strings.Repeat("c", 40)
	logPath := installFakeOperationWorkerGit(t, revision, "fetch")

	_, err := checkoutOperationWorkerSource(
		context.Background(),
		operationWorkerSource{
			Provider:     "generic_git",
			CanonicalURL: "https://example.com/owner/repo.git",
		},
		revision,
	)
	if err == nil || !strings.Contains(err.Error(), "fetch requested source revision") {
		t.Fatalf("checkout error = %v, want fetch failure", err)
	}

	log := readFakeOperationWorkerGitLog(t, logPath)
	directory := temporaryDirectoryFromGitLog(t, log)

	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Errorf("failed checkout directory still exists or could not be checked: %v", err)
	}

	if !strings.Contains(log, "fetch --no-tags --depth=1 origin "+revision) {
		t.Errorf("Git did not receive the requested revision:\n%s", log)
	}
}

func TestCheckoutOperationWorkerSourceUsesSHA256RepositoryFormat(
	t *testing.T,
) {
	revision := strings.Repeat("d", 64)
	logPath := installFakeOperationWorkerGit(t, revision, "")

	checkout, err := checkoutOperationWorkerSource(
		context.Background(),
		operationWorkerSource{
			Provider:     "generic_git",
			CanonicalURL: "https://example.com/owner/repo.git",
		},
		revision,
	)
	if err != nil {
		t.Fatalf("checkout SHA-256 revision: %v", err)
	}
	defer func() {
		if err := checkout.Cleanup(); err != nil {
			t.Errorf("clean up SHA-256 checkout: %v", err)
		}
	}()

	if checkout.Revision() != revision {
		t.Errorf("checkout revision = %q, want %q", checkout.Revision(), revision)
	}

	log := readFakeOperationWorkerGitLog(t, logPath)
	if !strings.Contains(log, "init --quiet --object-format=sha256") {
		t.Errorf("Git repository was not initialized with SHA-256 format:\n%s", log)
	}
}

func TestCheckoutOperationWorkerSourceRejectsInvalidInputs(
	t *testing.T,
) {
	tests := []struct {
		name     string
		source   operationWorkerSource
		revision string
	}{
		{
			name: "invalid revision",
			source: operationWorkerSource{
				Provider:     "generic_git",
				CanonicalURL: "https://example.com/owner/repo.git",
			},
			revision: "not-a-commit",
		},
		{
			name: "unsupported provider",
			source: operationWorkerSource{
				Provider:     "unknown",
				CanonicalURL: "https://example.com/owner/repo.git",
			},
			revision: strings.Repeat("a", 40),
		},
		{
			name: "invalid repository URL",
			source: operationWorkerSource{
				Provider:     "generic_git",
				CanonicalURL: "http://example.com/owner/repo.git",
			},
			revision: strings.Repeat("a", 40),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// A fake executable is installed to confirm the checkout
			// helper rejects bad inputs before invoking Git.
			logPath := installFakeOperationWorkerGit(
				t,
				test.revision,
				"",
			)

			_, err := checkoutOperationWorkerSource(
				context.Background(),
				test.source,
				test.revision,
			)
			if err == nil {
				t.Fatal("expected invalid input to be rejected")
			}

			if _, logErr := os.Stat(logPath); !os.IsNotExist(logErr) {
				t.Errorf(
					"Git was invoked or its log could not be checked: %v",
					logErr,
				)
			}
		})
	}
}

func TestRunOperationWorkerSourceGitReportsMissingGit(
	t *testing.T,
) {
	emptyPath := t.TempDir()
	t.Setenv("PATH", emptyPath)

	directory := t.TempDir()
	_, err := runOperationWorkerSourceGit(
		context.Background(),
		directory,
		"version",
	)
	if err == nil {
		t.Fatal("expected an error when Git is unavailable")
	}

	if !strings.Contains(err.Error(), "git") &&
		!strings.Contains(err.Error(), "executable file") {
		t.Errorf("unexpected missing-Git error: %v", err)
	}
}

func TestOperationWorkerSourceCheckoutTestHelpers(t *testing.T) {
	if got := shellQuote("plain"); got != "'plain'" {
		t.Errorf("shellQuote(plain) = %q", got)
	}

	if got := shellQuote("it's"); got != "'it'\\''s'" {
		t.Errorf("shellQuote with apostrophe = %q", got)
	}

	if got := fmt.Sprint(strings.Repeat("a", 40)); len(got) != 40 {
		t.Errorf("unexpected revision test value length: %d", len(got))
	}
}
