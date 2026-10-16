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
	errOperationListInvalidLimit = errors.New(
		"operation list limit must be between 1 and 100",
	)
	errOperationListInvalidCursor = errors.New(
		"operation list cursor is invalid",
	)
)

type operationListResult struct {
	Items      []operationRecord `json:"items"`
	NextCursor *string           `json:"nextCursor"`
}

type operationListCursor struct {
	CreatedAt time.Time `json:"createdAt"`
	ID        string    `json:"id"`
}

// listRepositoryOperations returns operations in reverse creation order.
// The cursor uses the last returned record as the next page boundary.
func listRepositoryOperations(
	ctx context.Context,
	pool *pgxpool.Pool,
	projectID string,
	repositoryID string,
	limit int,
	cursorValue string,
) (operationListResult, error) {
	if pool == nil {
		return operationListResult{}, errors.New(
			"database pool is required",
		)
	}

	if limit < 1 || limit > 100 {
		return operationListResult{}, errOperationListInvalidLimit
	}

	normalizedProjectID, err := normalizeOperationUUID(projectID)
	if err != nil {
		return operationListResult{}, fmt.Errorf(
			"projectId: %w",
			err,
		)
	}

	projectUUID, err := operationUUIDValue(normalizedProjectID)
	if err != nil {
		return operationListResult{}, fmt.Errorf(
			"projectId: %w",
			err,
		)
	}

	normalizedRepositoryID, err := normalizeOperationUUID(repositoryID)
	if err != nil {
		return operationListResult{}, fmt.Errorf(
			"repositoryId: %w",
			err,
		)
	}

	repositoryUUID, err := operationUUIDValue(normalizedRepositoryID)
	if err != nil {
		return operationListResult{}, fmt.Errorf(
			"repositoryId: %w",
			err,
		)
	}

	query := `
		SELECT ` + operationRecordColumns + `
		FROM operations
		WHERE project_id = $1::uuid
		  AND repository_id = $2::uuid
	`

	args := []any{projectUUID, repositoryUUID}

	if cursorValue != "" {
		cursor, err := decodeOperationListCursor(cursorValue)
		if err != nil {
			return operationListResult{}, err
		}

		cursorUUID, err := operationUUIDValue(cursor.ID)
		if err != nil {
			return operationListResult{}, errOperationListInvalidCursor
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
		return operationListResult{}, fmt.Errorf(
			"query repository operations: %w",
			err,
		)
	}
	defer rows.Close()

	items := make([]operationRecord, 0, limit+1)

	for rows.Next() {
		record, err := scanOperationRecord(rows)
		if err != nil {
			return operationListResult{}, fmt.Errorf(
				"scan repository operation: %w",
				err,
			)
		}

		items = append(items, record)
	}

	if err := rows.Err(); err != nil {
		return operationListResult{}, fmt.Errorf(
			"iterate repository operations: %w",
			err,
		)
	}

	result := operationListResult{
		Items: items,
	}

	if len(items) <= limit {
		return result, nil
	}

	result.Items = items[:limit]

	nextCursor, err := encodeOperationListCursor(
		result.Items[len(result.Items)-1],
	)
	if err != nil {
		return operationListResult{}, err
	}

	result.NextCursor = &nextCursor

	return result, nil
}

func encodeOperationListCursor(record operationRecord) (string, error) {
	cursor := operationListCursor{
		CreatedAt: record.CreatedAt.UTC(),
		ID:        record.ID,
	}

	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf(
			"encode operation list cursor: %w",
			err,
		)
	}

	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeOperationListCursor(
	value string,
) (operationListCursor, error) {
	if value == "" || len(value) > 1024 {
		return operationListCursor{}, errOperationListInvalidCursor
	}

	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return operationListCursor{}, errOperationListInvalidCursor
	}

	var cursor operationListCursor
	if err := json.Unmarshal(payload, &cursor); err != nil {
		return operationListCursor{}, errOperationListInvalidCursor
	}

	if cursor.CreatedAt.IsZero() ||
		cursor.CreatedAt.Year() < 1 ||
		cursor.CreatedAt.Year() > 9999 {
		return operationListCursor{}, errOperationListInvalidCursor
	}

	normalizedID, err := normalizeOperationUUID(cursor.ID)
	if err != nil {
		return operationListCursor{}, errOperationListInvalidCursor
	}

	cursor.CreatedAt = cursor.CreatedAt.UTC()
	cursor.ID = normalizedID

	return cursor, nil
}
