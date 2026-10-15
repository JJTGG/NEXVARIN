package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	errOperationWorkerSourceUnavailable = errors.New(
		"registered operation source is unavailable",
	)
	errOperationWorkerSourceURLInvalid = errors.New(
		"registered repository URL is invalid",
	)
)

type operationWorkerSource struct {
	Provider      string
	CanonicalURL  string
	DisplayName   string
	DefaultBranch string
}

// loadOperationWorkerSource loads the active repository associated with a
// claimed build task. The project and repository must match the IDs recorded
// in the claimed operation.
func loadOperationWorkerSource(
	ctx context.Context,
	pool *pgxpool.Pool,
	task operationWorkerTask,
) (operationWorkerSource, error) {
	var empty operationWorkerSource

	if ctx == nil {
		return empty, errors.New("worker context is required")
	}
	if pool == nil {
		return empty, errors.New("database pool is required")
	}
	if task.Request.OperationType != operationTypeBuild {
		return empty, errors.New(
			"repository source loading requires a build operation",
		)
	}

	projectID, err := normalizeOperationUUID(
		task.Claimed.Operation.ProjectID,
	)
	if err != nil {
		return empty, fmt.Errorf("project ID: %w", err)
	}

	repositoryID, err := normalizeOperationUUID(
		task.Claimed.Operation.RepositoryID,
	)
	if err != nil {
		return empty, fmt.Errorf("repository ID: %w", err)
	}

	var source operationWorkerSource

	err = pool.QueryRow(ctx, `
		SELECT
			r.provider,
			r.canonical_url,
			r.display_name,
			r.default_branch
		FROM projects p
		JOIN repositories r
		  ON r.project_id = p.id
		WHERE p.id = $1::uuid
		  AND r.id = $2::uuid
		  AND p.status = 'active'
		  AND r.is_active = TRUE
		FOR SHARE OF p, r
	`, projectID, repositoryID).Scan(
		&source.Provider,
		&source.CanonicalURL,
		&source.DisplayName,
		&source.DefaultBranch,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return empty, errOperationWorkerSourceUnavailable
	}
	if err != nil {
		return empty, fmt.Errorf(
			"load registered operation source: %w",
			err,
		)
	}

	switch source.Provider {
	case "github", "gitlab", "generic_git":
		// Supported repository provider types.
	default:
		return empty, errOperationWorkerSourceUnavailable
	}

	if strings.TrimSpace(source.DisplayName) == "" ||
		strings.TrimSpace(source.DefaultBranch) == "" {
		return empty, errOperationWorkerSourceUnavailable
	}

	source.CanonicalURL, err = validateOperationWorkerSourceURL(
		source.CanonicalURL,
	)
	if err != nil {
		return empty, err
	}

	return source, nil
}

// validateOperationWorkerSourceURL accepts HTTPS repository URLs without
// embedded credentials, query parameters, fragments, or literal IP hosts.
//
// This is URL validation, not a complete SSRF defence. Before arbitrary
// customer repositories are supported, source retrieval must also enforce
// network egress restrictions and prevent access to private infrastructure.
func validateOperationWorkerSourceURL(
	rawURL string,
) (string, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "", errOperationWorkerSourceURLInvalid
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", errOperationWorkerSourceURLInvalid
	}

	if !strings.EqualFold(parsed.Scheme, "https") ||
		parsed.Opaque != "" ||
		parsed.User != nil ||
		parsed.RawQuery != "" ||
		parsed.Fragment != "" ||
		parsed.RawFragment != "" {
		return "", errOperationWorkerSourceURLInvalid
	}

	host := parsed.Hostname()
	if host == "" || net.ParseIP(host) != nil {
		return "", errOperationWorkerSourceURLInvalid
	}

	normalizedHost := strings.ToLower(
		strings.TrimSuffix(host, "."),
	)

	if normalizedHost == "localhost" ||
		strings.HasSuffix(normalizedHost, ".localhost") ||
		strings.HasSuffix(normalizedHost, ".local") ||
		strings.HasSuffix(normalizedHost, ".internal") {
		return "", errOperationWorkerSourceURLInvalid
	}

	if port := parsed.Port(); port != "" && port != "443" {
		return "", errOperationWorkerSourceURLInvalid
	}

	if strings.HasSuffix(parsed.Host, ":") {
		return "", errOperationWorkerSourceURLInvalid
	}

	if strings.Trim(parsed.Path, "/") == "" {
		return "", errOperationWorkerSourceURLInvalid
	}

	return rawURL, nil
}
