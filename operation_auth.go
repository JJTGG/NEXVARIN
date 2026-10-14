package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const minimumOperationBearerTokenBytes = 32

// staticOperationBearerAuthenticator provides the initial, single-operator
// authentication boundary for the controlled NEXVARIN vertical slice.
//
// The token must be provided out of band through environment configuration.
// It is never logged or persisted. This implementation is not intended to
// serve as the final credential system for a multi-user public platform.
type staticOperationBearerAuthenticator struct {
	token     []byte
	principal operationHTTPPrincipal
}

var _ operationHTTPAuthenticator = (*staticOperationBearerAuthenticator)(nil)

// newStaticOperationBearerAuthenticator validates the configured credential
// and binds it to the trusted subject used by project ownership checks.
func newStaticOperationBearerAuthenticator(
	token string,
	subject string,
) (*staticOperationBearerAuthenticator, error) {
	if !validOperationBearerToken(token) {
		return nil, errors.New(
			"operation bearer token must contain at least 32 bytes of valid token characters",
		)
	}

	if !validOperationSubject(subject) {
		return nil, errors.New(
			"operation bearer subject must be a non-empty valid subject",
		)
	}

	return &staticOperationBearerAuthenticator{
		token: []byte(token),
		principal: operationHTTPPrincipal{
			Subject: subject,
		},
	}, nil
}

// loadOperationHTTPAuthenticatorFromEnvironment loads the initial
// single-operator credential. When neither variable is set, it returns
// nil without error so the API can run health endpoints while operation
// creation remains closed. Partial or invalid configuration is an error.
func loadOperationHTTPAuthenticatorFromEnvironment() (
	operationHTTPAuthenticator,
	error,
) {
	token := os.Getenv("NEXVARIN_API_TOKEN")
	subject := os.Getenv("NEXVARIN_API_SUBJECT")

	if token == "" && subject == "" {
		return nil, nil
	}

	if token == "" || subject == "" {
		return nil, errors.New(
			"NEXVARIN_API_TOKEN and NEXVARIN_API_SUBJECT must be configured together",
		)
	}

	authenticator, err := newStaticOperationBearerAuthenticator(
		token,
		subject,
	)
	if err != nil {
		return nil, err
	}

	return authenticator, nil
}

func validOperationBearerToken(token string) bool {
	if len(token) < minimumOperationBearerTokenBytes ||
		strings.TrimSpace(token) != token {
		return false
	}

	paddingStarted := false
	tokenCharacters := 0

	for index := 0; index < len(token); index++ {
		character := token[index]

		if character == '=' {
			if tokenCharacters == 0 {
				return false
			}

			paddingStarted = true
			continue
		}

		if paddingStarted {
			return false
		}

		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			strings.ContainsRune("-._~+/", rune(character)) {
			tokenCharacters++
			continue
		}

		return false
	}

	return tokenCharacters > 0
}

func (a *staticOperationBearerAuthenticator) Authenticate(
	_ context.Context,
	bearerToken string,
) (operationHTTPPrincipal, error) {
	if a == nil || len(a.token) == 0 {
		return operationHTTPPrincipal{}, errors.New(
			"operation authentication is not configured",
		)
	}

	if subtle.ConstantTimeCompare(
		[]byte(bearerToken),
		a.token,
	) != 1 {
		return operationHTTPPrincipal{}, errOperationUnauthenticated
	}

	return a.principal, nil
}

// postgresOperationCreationAuthorizer grants operation creation only when
// the authenticated subject owns the requested project and the repository
// belongs to that project. Missing and non-owned scopes are intentionally
// indistinguishable to avoid exposing other operators' resource identifiers.
type postgresOperationCreationAuthorizer struct {
	pool *pgxpool.Pool
}

var _ operationCreationAuthorizer = (*postgresOperationCreationAuthorizer)(nil)

func newPostgresOperationCreationAuthorizer(
	pool *pgxpool.Pool,
) *postgresOperationCreationAuthorizer {
	if pool == nil {
		return nil
	}

	return &postgresOperationCreationAuthorizer{
		pool: pool,
	}
}

func (a *postgresOperationCreationAuthorizer) AuthorizeCreateOperation(
	ctx context.Context,
	principal operationHTTPPrincipal,
	projectID string,
	repositoryID string,
) error {
	if a == nil || a.pool == nil {
		return errors.New(
			"operation authorization is not configured",
		)
	}

	if !validOperationSubject(principal.Subject) {
		return errOperationCreationForbidden
	}

	normalizedProjectID, err := normalizeOperationUUID(projectID)
	if err != nil {
		return errOperationScopeNotFound
	}

	normalizedRepositoryID, err := normalizeOperationUUID(repositoryID)
	if err != nil {
		return errOperationScopeNotFound
	}

	var authorized int

	err = a.pool.QueryRow(
		ctx,
		`
			SELECT 1
			FROM projects AS p
			JOIN repositories AS r
			  ON r.project_id = p.id
			WHERE p.id = $1::uuid
			  AND r.id = $2::uuid
			  AND p.owner_subject = $3
		`,
		normalizedProjectID,
		normalizedRepositoryID,
		principal.Subject,
	).Scan(&authorized)

	if errors.Is(err, pgx.ErrNoRows) {
		return errOperationScopeNotFound
	}

	if err != nil {
		return fmt.Errorf(
			"verify project ownership and repository scope: %w",
			err,
		)
	}

	return nil
}
