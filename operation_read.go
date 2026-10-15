package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var errOperationRecordNotFound = errors.New(
	"operation record was not found",
)

// loadOperationRecord retrieves the latest persisted state of an operation.
// Callers must authorize access before returning the record to an API client.
func loadOperationRecord(
	ctx context.Context,
	pool *pgxpool.Pool,
	operationID string,
) (operationRecord, error) {
	if pool == nil {
		return operationRecord{}, errors.New(
			"database pool is required",
		)
	}

	normalizedOperationID, err := normalizeOperationUUID(operationID)
	if err != nil {
		return operationRecord{}, fmt.Errorf(
			"operationId: %w",
			err,
		)
	}

	operationUUID, err := operationUUIDValue(
		normalizedOperationID,
	)
	if err != nil {
		return operationRecord{}, fmt.Errorf(
			"operationId: %w",
			err,
		)
	}

	record, err := scanOperationRecord(pool.QueryRow(
		ctx,
		`
			SELECT `+operationRecordColumns+`
			FROM operations
			WHERE id = $1::uuid
		`,
		operationUUID,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return operationRecord{}, errOperationRecordNotFound
	}
	if err != nil {
		return operationRecord{}, fmt.Errorf(
			"load operation record: %w",
			err,
		)
	}

	return record, nil
}
