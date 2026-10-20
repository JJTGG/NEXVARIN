package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

type operationType string

const (
	operationTypeBuild     operationType = "build"
	operationTypeDeploy    operationType = "deploy"
	operationTypeCleanup   operationType = "cleanup"
	operationTypeReconcile operationType = "reconcile"
)

const (
	minOperationApplicationPort = 1
	maxOperationApplicationPort = 65535
)

// normalizedOperationRequest contains only the fields that belong to
// the selected operation type.
//
// Field order is intentional: encoding this struct produces a stable
// representation for computing the request fingerprint.
type normalizedOperationRequest struct {
	OperationType     operationType `json:"operationType"`
	SourceRevision    string        `json:"sourceRevision,omitempty"`
	ArtifactID        string        `json:"artifactId,omitempty"`
	ApplicationPort   int           `json:"applicationPort,omitempty"`
	DeploymentID      string        `json:"deploymentId,omitempty"`
	ParentOperationID string        `json:"parentOperationId,omitempty"`
}

func validOperationApplicationPort(port int) bool {
	return port >= minOperationApplicationPort &&
		port <= maxOperationApplicationPort
}

// normalizeOperationRequest validates a supported operation request,
// normalizes its fields, and returns its canonical JSON representation
// and SHA-256 fingerprint.
//
// The fingerprint is suitable for comparing normalized requests. It
// does not implement idempotency by itself; the database transaction
// must enforce the idempotency key's uniqueness and replay rules.
func normalizeOperationRequest(
	raw []byte,
) (normalizedOperationRequest, []byte, string, error) {
	var empty normalizedOperationRequest

	if len(bytes.TrimSpace(raw)) == 0 {
		return empty, nil, "", errors.New("request body is required")
	}

	var envelope struct {
		OperationType json.RawMessage `json:"operationType"`
	}

	if err := json.Unmarshal(raw, &envelope); err != nil {
		return empty, nil, "", fmt.Errorf("invalid JSON request: %w", err)
	}

	if len(envelope.OperationType) == 0 {
		return empty, nil, "", errors.New("operationType is required")
	}

	var requestedType operationType
	if err := json.Unmarshal(envelope.OperationType, &requestedType); err != nil {
		return empty, nil, "", errors.New("operationType must be a string")
	}

	var normalized normalizedOperationRequest

	switch requestedType {
	case operationTypeBuild:
		var request struct {
			OperationType  operationType `json:"operationType"`
			SourceRevision string        `json:"sourceRevision"`
		}

		if err := decodeStrictOperationJSON(raw, &request); err != nil {
			return empty, nil, "", err
		}

		if request.OperationType != operationTypeBuild {
			return empty, nil, "", errors.New("operationType must be build")
		}

		if !isValidGitRevision(request.SourceRevision) {
			return empty, nil, "", errors.New(
				"sourceRevision must be a 40- or 64-character lowercase hexadecimal commit identifier",
			)
		}

		normalized = normalizedOperationRequest{
			OperationType:  operationTypeBuild,
			SourceRevision: request.SourceRevision,
		}

	case operationTypeDeploy:
		var request struct {
			OperationType   operationType `json:"operationType"`
			ArtifactID      string        `json:"artifactId"`
			ApplicationPort int           `json:"applicationPort"`
		}

		if err := decodeStrictOperationJSON(raw, &request); err != nil {
			return empty, nil, "", err
		}

		if request.OperationType != operationTypeDeploy {
			return empty, nil, "", errors.New("operationType must be deploy")
		}

		artifactID, err := normalizeOperationUUID(request.ArtifactID)
		if err != nil {
			return empty, nil, "", fmt.Errorf("artifactId: %w", err)
		}

		if !validOperationApplicationPort(request.ApplicationPort) {
			return empty, nil, "", errors.New(
				"applicationPort must be an integer between 1 and 65535",
			)
		}

		normalized = normalizedOperationRequest{
			OperationType:   operationTypeDeploy,
			ArtifactID:      artifactID,
			ApplicationPort: request.ApplicationPort,
		}

	case operationTypeCleanup:
		var request struct {
			OperationType operationType `json:"operationType"`
			DeploymentID  string        `json:"deploymentId"`
		}

		if err := decodeStrictOperationJSON(raw, &request); err != nil {
			return empty, nil, "", err
		}

		if request.OperationType != operationTypeCleanup {
			return empty, nil, "", errors.New("operationType must be cleanup")
		}

		deploymentID, err := normalizeOperationUUID(request.DeploymentID)
		if err != nil {
			return empty, nil, "", fmt.Errorf("deploymentId: %w", err)
		}

		normalized = normalizedOperationRequest{
			OperationType: operationTypeCleanup,
			DeploymentID:  deploymentID,
		}

	case operationTypeReconcile:
		var request struct {
			OperationType     operationType `json:"operationType"`
			ParentOperationID string        `json:"parentOperationId"`
		}

		if err := decodeStrictOperationJSON(raw, &request); err != nil {
			return empty, nil, "", err
		}

		if request.OperationType != operationTypeReconcile {
			return empty, nil, "", errors.New("operationType must be reconcile")
		}

		parentID, err := normalizeOperationUUID(request.ParentOperationID)
		if err != nil {
			return empty, nil, "", fmt.Errorf("parentOperationId: %w", err)
		}

		normalized = normalizedOperationRequest{
			OperationType:     operationTypeReconcile,
			ParentOperationID: parentID,
		}

	default:
		return empty, nil, "", errors.New("unsupported operationType")
	}

	canonicalJSON, err := json.Marshal(normalized)
	if err != nil {
		return empty, nil, "", fmt.Errorf(
			"encode normalized operation request: %w",
			err,
		)
	}

	digest := sha256.Sum256(canonicalJSON)

	return normalized, canonicalJSON, hex.EncodeToString(digest[:]), nil
}

func decodeStrictOperationJSON(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("invalid operation request: %w", err)
	}

	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("request must contain exactly one JSON value")
		}

		return fmt.Errorf("invalid trailing JSON data: %w", err)
	}

	return nil
}

func isValidGitRevision(revision string) bool {
	if len(revision) != 40 && len(revision) != 64 {
		return false
	}

	for _, character := range revision {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}

	return true
}

func normalizeOperationUUID(value string) (string, error) {
	if len(value) != 36 ||
		value[8] != '-' ||
		value[13] != '-' ||
		value[18] != '-' ||
		value[23] != '-' {
		return "", errors.New("must be a UUID in canonical hyphenated form")
	}

	compact := strings.ReplaceAll(value, "-", "")

	if _, err := hex.DecodeString(compact); err != nil {
		return "", errors.New("must contain only hexadecimal UUID characters")
	}

	return strings.ToLower(value), nil
}
