package main

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestNormalizeOperationRequestValid(t *testing.T) {
	revision40 := strings.Repeat("a", 40)
	revision64 := strings.Repeat("b", 64)
	artifactID := "123e4567-e89b-12d3-a456-426614174000"
	upperArtifactID := "123E4567-E89B-12D3-A456-426614174000"
	deploymentID := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	parentOperationID := "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

	tests := []struct {
		name          string
		request       string
		want          normalizedOperationRequest
		wantCanonical string
	}{
		{
			name:    "build with 40 character revision",
			request: `{"operationType":"build","sourceRevision":"` + revision40 + `"}`,
			want: normalizedOperationRequest{
				OperationType:  operationTypeBuild,
				SourceRevision: revision40,
			},
			wantCanonical: `{"operationType":"build","sourceRevision":"` + revision40 + `"}`,
		},
		{
			name:    "build with 64 character revision",
			request: `{"operationType":"build","sourceRevision":"` + revision64 + `"}`,
			want: normalizedOperationRequest{
				OperationType:  operationTypeBuild,
				SourceRevision: revision64,
			},
			wantCanonical: `{"operationType":"build","sourceRevision":"` + revision64 + `"}`,
		},
		{
			name:    "deploy normalizes uppercase UUID and stores application port",
			request: `{"operationType":"deploy","artifactId":"` + upperArtifactID + `","applicationPort":8080}`,
			want: normalizedOperationRequest{
				OperationType:   operationTypeDeploy,
				ArtifactID:      artifactID,
				ApplicationPort: 8080,
			},
			wantCanonical: `{"operationType":"deploy","artifactId":"` + artifactID + `","applicationPort":8080}`,
		},
		{
			name:    "deploy accepts lowest valid application port",
			request: `{"operationType":"deploy","artifactId":"` + artifactID + `","applicationPort":1}`,
			want: normalizedOperationRequest{
				OperationType:   operationTypeDeploy,
				ArtifactID:      artifactID,
				ApplicationPort: 1,
			},
			wantCanonical: `{"operationType":"deploy","artifactId":"` + artifactID + `","applicationPort":1}`,
		},
		{
			name:    "deploy accepts highest valid application port",
			request: `{"operationType":"deploy","artifactId":"` + artifactID + `","applicationPort":65535}`,
			want: normalizedOperationRequest{
				OperationType:   operationTypeDeploy,
				ArtifactID:      artifactID,
				ApplicationPort: 65535,
			},
			wantCanonical: `{"operationType":"deploy","artifactId":"` + artifactID + `","applicationPort":65535}`,
		},
		{
			name:    "cleanup",
			request: `{"operationType":"cleanup","deploymentId":"` + deploymentID + `"}`,
			want: normalizedOperationRequest{
				OperationType: operationTypeCleanup,
				DeploymentID:  deploymentID,
			},
			wantCanonical: `{"operationType":"cleanup","deploymentId":"` + deploymentID + `"}`,
		},
		{
			name:    "reconcile",
			request: `{"operationType":"reconcile","parentOperationId":"` + parentOperationID + `"}`,
			want: normalizedOperationRequest{
				OperationType:     operationTypeReconcile,
				ParentOperationID: parentOperationID,
			},
			wantCanonical: `{"operationType":"reconcile","parentOperationId":"` + parentOperationID + `"}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, canonical, fingerprint, err := normalizeOperationRequest(
				[]byte(test.request),
			)
			if err != nil {
				t.Fatalf("normalizeOperationRequest() error = %v", err)
			}

			if got != test.want {
				t.Errorf(
					"normalized request = %#v, want %#v",
					got,
					test.want,
				)
			}

			if string(canonical) != test.wantCanonical {
				t.Errorf(
					"canonical JSON = %s, want %s",
					canonical,
					test.wantCanonical,
				)
			}

			digest := sha256.Sum256(canonical)
			wantFingerprint := hex.EncodeToString(digest[:])

			if fingerprint != wantFingerprint {
				t.Errorf(
					"fingerprint = %q, want SHA-256 %q",
					fingerprint,
					wantFingerprint,
				)
			}

			if len(fingerprint) != sha256.Size*2 {
				t.Errorf(
					"fingerprint length = %d, want %d hexadecimal characters",
					len(fingerprint),
					sha256.Size*2,
				)
			}
		})
	}
}

func TestNormalizeOperationRequestFingerprintIsStable(t *testing.T) {
	revision := strings.Repeat("c", 40)
	first := `{"operationType":"build","sourceRevision":"` + revision + `"}`
	second := "{\n  \"sourceRevision\": \"" + revision + "\",\n  \"operationType\": \"build\"\n}"

	_, firstCanonical, firstFingerprint, err := normalizeOperationRequest(
		[]byte(first),
	)
	if err != nil {
		t.Fatalf("normalize first request: %v", err)
	}

	_, secondCanonical, secondFingerprint, err := normalizeOperationRequest(
		[]byte(second),
	)
	if err != nil {
		t.Fatalf("normalize second request: %v", err)
	}

	if string(firstCanonical) != string(secondCanonical) {
		t.Errorf(
			"canonical JSON differs for equivalent requests: %s != %s",
			firstCanonical,
			secondCanonical,
		)
	}

	if firstFingerprint != secondFingerprint {
		t.Errorf(
			"fingerprints differ for equivalent requests: %s != %s",
			firstFingerprint,
			secondFingerprint,
		)
	}
}

func TestNormalizeOperationRequestRejectsInvalidRequests(t *testing.T) {
	revision := strings.Repeat("a", 40)
	invalidRevisionLength := strings.Repeat("a", 39)
	uppercaseRevision := strings.Repeat("A", 40)
	nonHexRevision := strings.Repeat("g", 40)
	validUUID := "123e4567-e89b-12d3-a456-426614174000"

	tests := []struct {
		name    string
		request string
	}{
		{
			name:    "empty body",
			request: "",
		},
		{
			name:    "whitespace body",
			request: " \n\t ",
		},
		{
			name:    "malformed JSON",
			request: `{"operationType":`,
		},
		{
			name:    "missing operation type",
			request: `{}`,
		},
		{
			name:    "operation type is not a string",
			request: `{"operationType":123}`,
		},
		{
			name:    "top-level array",
			request: `[]`,
		},
		{
			name:    "top-level null",
			request: `null`,
		},
		{
			name:    "unsupported operation type",
			request: `{"operationType":"unknown"}`,
		},
		{
			name:    "build missing revision",
			request: `{"operationType":"build"}`,
		},
		{
			name:    "build revision too short",
			request: `{"operationType":"build","sourceRevision":"` + invalidRevisionLength + `"}`,
		},
		{
			name:    "build uppercase revision",
			request: `{"operationType":"build","sourceRevision":"` + uppercaseRevision + `"}`,
		},
		{
			name:    "build non-hex revision",
			request: `{"operationType":"build","sourceRevision":"` + nonHexRevision + `"}`,
		},
		{
			name:    "build rejects unknown field",
			request: `{"operationType":"build","sourceRevision":"` + revision + `","artifactId":"` + validUUID + `"}`,
		},
		{
			name:    "deploy missing artifact ID",
			request: `{"operationType":"deploy","applicationPort":8080}`,
		},
		{
			name:    "deploy missing application port",
			request: `{"operationType":"deploy","artifactId":"` + validUUID + `"}`,
		},
		{
			name:    "deploy rejects zero application port",
			request: `{"operationType":"deploy","artifactId":"` + validUUID + `","applicationPort":0}`,
		},
		{
			name:    "deploy rejects negative application port",
			request: `{"operationType":"deploy","artifactId":"` + validUUID + `","applicationPort":-1}`,
		},
		{
			name:    "deploy rejects application port above maximum",
			request: `{"operationType":"deploy","artifactId":"` + validUUID + `","applicationPort":65536}`,
		},
		{
			name:    "deploy rejects fractional application port",
			request: `{"operationType":"deploy","artifactId":"` + validUUID + `","applicationPort":8080.5}`,
		},
		{
			name:    "deploy rejects string application port",
			request: `{"operationType":"deploy","artifactId":"` + validUUID + `","applicationPort":"8080"}`,
		},
		{
			name:    "deploy rejects boolean application port",
			request: `{"operationType":"deploy","artifactId":"` + validUUID + `","applicationPort":true}`,
		},
		{
			name:    "deploy rejects null application port",
			request: `{"operationType":"deploy","artifactId":"` + validUUID + `","applicationPort":null}`,
		},
		{
			name:    "deploy UUID missing hyphens",
			request: `{"operationType":"deploy","artifactId":"123e4567e89b12d3a456426614174000","applicationPort":8080}`,
		},
		{
			name:    "deploy UUID has non-hex character",
			request: `{"operationType":"deploy","artifactId":"123e4567-e89b-12d3-a456-42661417400g","applicationPort":8080}`,
		},
		{
			name:    "deploy rejects unknown field",
			request: `{"operationType":"deploy","artifactId":"` + validUUID + `","applicationPort":8080,"hostPort":9000}`,
		},
		{
			name:    "cleanup missing deployment ID",
			request: `{"operationType":"cleanup"}`,
		},
		{
			name:    "reconcile missing parent operation ID",
			request: `{"operationType":"reconcile"}`,
		},
		{
			name:    "multiple JSON values",
			request: `{"operationType":"build","sourceRevision":"` + revision + `"} {}`,
		},
		{
			name:    "trailing non-JSON data",
			request: `{"operationType":"build","sourceRevision":"` + revision + `"} trailing`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, err := normalizeOperationRequest([]byte(test.request))
			if err == nil {
				t.Fatal("normalizeOperationRequest() error = nil, want an error")
			}
		})
	}
}
