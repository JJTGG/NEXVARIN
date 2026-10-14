package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

const operationAuthTestSubject = "test-operator"

func TestValidOperationBearerToken(t *testing.T) {
	tests := []struct {
		name  string
		token string
		want  bool
	}{
		{
			name:  "32 valid characters",
			token: strings.Repeat("a", minimumOperationBearerTokenBytes),
			want:  true,
		},
		{
			name:  "valid token with trailing padding",
			token: strings.Repeat("a", minimumOperationBearerTokenBytes-1) + "=",
			want:  true,
		},
		{
			name:  "too short",
			token: strings.Repeat("a", minimumOperationBearerTokenBytes-1),
			want:  false,
		},
		{
			name:  "leading whitespace",
			token: " " + strings.Repeat("a", minimumOperationBearerTokenBytes),
			want:  false,
		},
		{
			name:  "trailing whitespace",
			token: strings.Repeat("a", minimumOperationBearerTokenBytes) + " ",
			want:  false,
		},
		{
			name:  "unsupported character",
			token: strings.Repeat("a", minimumOperationBearerTokenBytes-1) + ":",
			want:  false,
		},
		{
			name:  "padding before token characters",
			token: "=" + strings.Repeat("a", minimumOperationBearerTokenBytes),
			want:  false,
		},
		{
			name: "characters after padding",
			token: strings.Repeat("a", minimumOperationBearerTokenBytes-1) +
				"=b",
			want: false,
		},
		{
			name:  "empty token",
			token: "",
			want:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := validOperationBearerToken(test.token); got != test.want {
				t.Errorf("validOperationBearerToken() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestNewStaticOperationBearerAuthenticatorValidatesConfiguration(t *testing.T) {
	validToken := strings.Repeat("a", minimumOperationBearerTokenBytes)

	tests := []struct {
		name    string
		token   string
		subject string
		wantErr bool
	}{
		{
			name:    "valid configuration",
			token:   validToken,
			subject: operationAuthTestSubject,
		},
		{
			name:    "empty token",
			token:   "",
			subject: operationAuthTestSubject,
			wantErr: true,
		},
		{
			name:    "empty subject",
			token:   validToken,
			subject: "",
			wantErr: true,
		},
		{
			name:    "whitespace subject",
			token:   validToken,
			subject: "   ",
			wantErr: true,
		},
		{
			name:    "subject containing a null byte",
			token:   validToken,
			subject: "operator\x00suffix",
			wantErr: true,
		},
		{
			name:    "subject containing invalid UTF-8",
			token:   validToken,
			subject: string([]byte{0xff}),
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authenticator, err := newStaticOperationBearerAuthenticator(
				test.token,
				test.subject,
			)

			if test.wantErr {
				if err == nil {
					t.Fatal("constructor error = nil, want an error")
				}
				if authenticator != nil {
					t.Error("constructor returned an authenticator for invalid configuration")
				}
				return
			}

			if err != nil {
				t.Fatalf("construct authenticator: %v", err)
			}
			if authenticator == nil {
				t.Fatal("constructor returned a nil authenticator")
			}
		})
	}
}

func TestStaticOperationBearerAuthenticatorAuthenticatesConfiguredToken(t *testing.T) {
	token := strings.Repeat("a", minimumOperationBearerTokenBytes)

	authenticator, err := newStaticOperationBearerAuthenticator(
		token,
		operationAuthTestSubject,
	)
	if err != nil {
		t.Fatalf("construct authenticator: %v", err)
	}

	principal, err := authenticator.Authenticate(context.Background(), token)
	if err != nil {
		t.Fatalf("authenticate configured token: %v", err)
	}
	if principal.Subject != operationAuthTestSubject {
		t.Errorf(
			"authenticated subject = %q, want %q",
			principal.Subject,
			operationAuthTestSubject,
		)
	}

	_, err = authenticator.Authenticate(
		context.Background(),
		strings.Repeat("b", minimumOperationBearerTokenBytes),
	)
	if !errors.Is(err, errOperationUnauthenticated) {
		t.Errorf(
			"wrong-token error = %v, want %v",
			err,
			errOperationUnauthenticated,
		)
	}

	var nilAuthenticator *staticOperationBearerAuthenticator
	if _, err := nilAuthenticator.Authenticate(context.Background(), token); err == nil {
		t.Error("nil authenticator returned no error")
	}
}

func TestLoadOperationHTTPAuthenticatorFromEnvironment(t *testing.T) {
	validToken := strings.Repeat("a", minimumOperationBearerTokenBytes)

	tests := []struct {
		name    string
		token   string
		subject string
		wantNil bool
		wantErr bool
	}{
		{
			name:    "both variables absent",
			wantNil: true,
		},
		{
			name:    "token without subject",
			token:   validToken,
			wantErr: true,
		},
		{
			name:    "subject without token",
			subject: operationAuthTestSubject,
			wantErr: true,
		},
		{
			name:    "valid configuration",
			token:   validToken,
			subject: operationAuthTestSubject,
		},
		{
			name:    "invalid token",
			token:   "short",
			subject: operationAuthTestSubject,
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("NEXVARIN_API_TOKEN", test.token)
			t.Setenv("NEXVARIN_API_SUBJECT", test.subject)

			authenticator, err := loadOperationHTTPAuthenticatorFromEnvironment()

			if test.wantErr {
				if err == nil {
					t.Fatal("configuration error = nil, want an error")
				}
				if authenticator != nil {
					t.Error("invalid configuration returned an authenticator")
				}
				return
			}

			if err != nil {
				t.Fatalf("load authenticator: %v", err)
			}

			if test.wantNil {
				if authenticator != nil {
					t.Error("expected no authenticator when both variables are absent")
				}
				return
			}

			if authenticator == nil {
				t.Fatal("valid configuration returned a nil authenticator")
			}

			principal, err := authenticator.Authenticate(
				context.Background(),
				validToken,
			)
			if err != nil {
				t.Fatalf("authenticate loaded token: %v", err)
			}
			if principal.Subject != operationAuthTestSubject {
				t.Errorf(
					"loaded subject = %q, want %q",
					principal.Subject,
					operationAuthTestSubject,
				)
			}
		})
	}
}

func TestPostgresOperationCreationAuthorizerEnforcesProjectOwnershipAndRepositoryScope(
	t *testing.T,
) {
	fixture := newOperationStoreTestFixture(t)
	authorizer := newPostgresOperationCreationAuthorizer(fixture.pool)

	if authorizer == nil {
		t.Fatal("authorizer is nil for a valid database pool")
	}

	tests := []struct {
		name         string
		subject      string
		projectID    string
		repositoryID string
		wantErr      error
	}{
		{
			name:         "project owner and matching repository",
			subject:      fixture.requestedBy,
			projectID:    fixture.projectID,
			repositoryID: fixture.repositoryID,
		},
		{
			name:         "different subject cannot access project",
			subject:      "another-operator",
			projectID:    fixture.projectID,
			repositoryID: fixture.repositoryID,
			wantErr:      errOperationScopeNotFound,
		},
		{
			name:         "repository outside requested project",
			subject:      fixture.requestedBy,
			projectID:    "123e4567-e89b-12d3-a456-426614174000",
			repositoryID: fixture.repositoryID,
			wantErr:      errOperationScopeNotFound,
		},
		{
			name:         "repository does not exist in project",
			subject:      fixture.requestedBy,
			projectID:    fixture.projectID,
			repositoryID: "123e4567-e89b-12d3-a456-426614174001",
			wantErr:      errOperationScopeNotFound,
		},
		{
			name:         "invalid project identifier",
			subject:      fixture.requestedBy,
			projectID:    "not-a-uuid",
			repositoryID: fixture.repositoryID,
			wantErr:      errOperationScopeNotFound,
		},
		{
			name:         "invalid subject is forbidden",
			subject:      "   ",
			projectID:    fixture.projectID,
			repositoryID: fixture.repositoryID,
			wantErr:      errOperationCreationForbidden,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(
				context.Background(),
				5*time.Second,
			)
			defer cancel()

			err := authorizer.AuthorizeCreateOperation(
				ctx,
				operationHTTPPrincipal{Subject: test.subject},
				test.projectID,
				test.repositoryID,
			)

			if test.wantErr == nil {
				if err != nil {
					t.Fatalf("authorize project operation: %v", err)
				}
				return
			}

			if !errors.Is(err, test.wantErr) {
				t.Fatalf(
					"authorization error = %v, want %v",
					err,
					test.wantErr,
				)
			}
		})
	}
}

func TestPostgresOperationCreationAuthorizerFailsClosedWithoutPool(t *testing.T) {
	if authorizer := newPostgresOperationCreationAuthorizer(nil); authorizer != nil {
		t.Fatal("authorizer constructor returned a non-nil value for a nil pool")
	}

	var authorizer *postgresOperationCreationAuthorizer
	err := authorizer.AuthorizeCreateOperation(
		context.Background(),
		operationHTTPPrincipal{Subject: operationAuthTestSubject},
		"123e4567-e89b-12d3-a456-426614174000",
		"123e4567-e89b-12d3-a456-426614174001",
	)
	if err == nil {
		t.Fatal("nil authorizer returned no error")
	}
}
