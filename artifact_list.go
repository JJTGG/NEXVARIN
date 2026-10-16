package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	errArtifactListInvalidLimit = errors.New(
		"artifact list limit must be between 1 and 100",
	)
	errArtifactListInvalidCursor = errors.New(
		"artifact list cursor is invalid",
	)
)

type artifactListItem struct {
	ID                 string    `json:"id"`
	ProjectID          string    `json:"projectId"`
	RepositoryID       string    `json:"repositoryId"`
	BuildOperationID   string    `json:"buildOperationId"`
	SourceRevision     string    `json:"sourceRevision"`
	ArtifactKind       string    `json:"artifactKind"`
	ContentDigest      string    `json:"contentDigest"`
	VerificationStatus string    `json:"verificationStatus"`
	VerifiedAt         time.Time `json:"verifiedAt"`
	CreatedAt          time.Time `json:"createdAt"`
	ImageReference     string    `json:"imageReference,omitempty"`
}

type artifactListResult struct {
	Items      []artifactListItem `json:"items"`
	NextCursor *string            `json:"nextCursor"`
}

type artifactListCursor struct {
	CreatedAt time.Time `json:"createdAt"`
	ID        string    `json:"id"`
}

// listRepositoryArtifacts returns verified artifacts in reverse
// creation order. The cursor identifies the last returned artifact,
// allowing the caller to request the next page.
func listRepositoryArtifacts(
	ctx context.Context,
	pool *pgxpool.Pool,
	projectID string,
	repositoryID string,
	limit int,
	cursorValue string,
) (artifactListResult, error) {
	if pool == nil {
		return artifactListResult{}, errors.New(
			"database pool is required",
		)
	}

	if limit < 1 || limit > 100 {
		return artifactListResult{}, errArtifactListInvalidLimit
	}

	normalizedProjectID, err := normalizeOperationUUID(projectID)
	if err != nil {
		return artifactListResult{}, fmt.Errorf(
			"projectId: %w",
			err,
		)
	}

	projectUUID, err := operationUUIDValue(normalizedProjectID)
	if err != nil {
		return artifactListResult{}, fmt.Errorf(
			"projectId: %w",
			err,
		)
	}

	normalizedRepositoryID, err := normalizeOperationUUID(repositoryID)
	if err != nil {
		return artifactListResult{}, fmt.Errorf(
			"repositoryId: %w",
			err,
		)
	}

	repositoryUUID, err := operationUUIDValue(normalizedRepositoryID)
	if err != nil {
		return artifactListResult{}, fmt.Errorf(
			"repositoryId: %w",
			err,
		)
	}

	query := `
		SELECT
			id::text,
			project_id::text,
			repository_id::text,
			build_operation_id::text,
			source_revision,
			artifact_kind,
			content_digest,
			verification_status,
			verified_at,
			created_at,
			COALESCE(metadata->>'imageReference', '')
		FROM artifacts
		WHERE project_id = $1::uuid
		  AND repository_id = $2::uuid
		  AND verification_status = 'verified'
	`

	args := []any{
		projectUUID,
		repositoryUUID,
	}

	if cursorValue != "" {
		cursor, err := decodeArtifactListCursor(cursorValue)
		if err != nil {
			return artifactListResult{}, err
		}

		cursorUUID, err := operationUUIDValue(cursor.ID)
		if err != nil {
			return artifactListResult{}, errArtifactListInvalidCursor
		}

		query += `
		  AND (created_at, id) < ($3::timestamptz, $4::uuid)
		`

		args = append(args, cursor.CreatedAt, cursorUUID)
	}

	query += fmt.Sprintf(
		"\n ORDER BY created_at DESC, id DESC\n LIMIT $%d",
		len(args)+1,
	)

	args = append(args, limit+1)

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return artifactListResult{}, fmt.Errorf(
			"query repository artifacts: %w",
			err,
		)
	}
	defer rows.Close()

	items := make([]artifactListItem, 0, limit+1)

	for rows.Next() {
		var item artifactListItem

		if err := rows.Scan(
			&item.ID,
			&item.ProjectID,
			&item.RepositoryID,
			&item.BuildOperationID,
			&item.SourceRevision,
			&item.ArtifactKind,
			&item.ContentDigest,
			&item.VerificationStatus,
			&item.VerifiedAt,
			&item.CreatedAt,
			&item.ImageReference,
		); err != nil {
			return artifactListResult{}, fmt.Errorf(
				"scan repository artifact: %w",
				err,
			)
		}

		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return artifactListResult{}, fmt.Errorf(
			"iterate repository artifacts: %w",
			err,
		)
	}

	result := artifactListResult{
		Items: items,
	}

	if len(items) <= limit {
		return result, nil
	}

	result.Items = items[:limit]

	nextCursor, err := encodeArtifactListCursor(
		result.Items[len(result.Items)-1],
	)
	if err != nil {
		return artifactListResult{}, err
	}

	result.NextCursor = &nextCursor

	return result, nil
}

func encodeArtifactListCursor(
	item artifactListItem,
) (string, error) {
	cursor := artifactListCursor{
		CreatedAt: item.CreatedAt.UTC(),
		ID:        item.ID,
	}

	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf(
			"encode artifact list cursor: %w",
			err,
		)
	}

	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeArtifactListCursor(
	value string,
) (artifactListCursor, error) {
	if value == "" || len(value) > 1024 {
		return artifactListCursor{}, errArtifactListInvalidCursor
	}

	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return artifactListCursor{}, errArtifactListInvalidCursor
	}

	var cursor artifactListCursor

	if err := json.Unmarshal(payload, &cursor); err != nil {
		return artifactListCursor{}, errArtifactListInvalidCursor
	}

	if cursor.CreatedAt.IsZero() ||
		cursor.CreatedAt.Year() < 1 ||
		cursor.CreatedAt.Year() > 9999 {
		return artifactListCursor{}, errArtifactListInvalidCursor
	}

	normalizedID, err := normalizeOperationUUID(cursor.ID)
	if err != nil {
		return artifactListCursor{}, errArtifactListInvalidCursor
	}

	cursor.CreatedAt = cursor.CreatedAt.UTC()
	cursor.ID = normalizedID

	return cursor, nil
}
