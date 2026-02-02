// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"
	"errors"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Authentication errors (PUBLIC - shown to users)
var (
	ErrNotAuthenticated = utils.NewPublicError(errors.New("not authenticated"))
	ErrForbidden        = utils.NewPublicError(errors.New("forbidden"))
	ErrInvalidToken     = utils.NewPublicError(errors.New("invalid token"))
)

// AuthenticationType represents the type of authenticator to use.
type AuthenticationType string

const (
	// AuthenticationTypeDisabled bypasses authentication (for development/testing).
	AuthenticationTypeDisabled AuthenticationType = "disabled"

	// AuthenticationTypeJWT uses JWT tokens (Google IAP, etc.).
	AuthenticationTypeJWT AuthenticationType = "jwt"

	// AuthenticationTypeBearer uses opaque bearer tokens with Okta integration.
	AuthenticationTypeBearer AuthenticationType = "bearer"
)

// IAuthenticator defines the framework-agnostic interface for authentication and authorization.
// Implementations can use different authentication methods (JWT, Bearer tokens, etc.)
// while providing a consistent interface for the API layer.
type IAuthenticator interface {
	// Authenticate validates a token and returns the authenticated principal.
	// clientIP is the remote IP address of the client making the request, used for IP allowlisting.
	// Supports both IPv4 and IPv6 addresses.
	// Returns the principal and nil error on success.
	// Returns nil principal and an AuthError on failure.
	Authenticate(ctx context.Context, token string, clientIP string) (*Principal, error)
}

// TokenInfo contains metadata about the authentication token.
// For bearer tokens, all fields are populated.
// For JWT tokens, only Type is meaningful (ID/CreatedAt/ExpiresAt are nil/zero).
type TokenInfo struct {
	// ID is the unique identifier for the token (zero UUID for JWT)
	ID uuid.UUID

	// CreatedAt is when the token was created (nil for JWT auth)
	CreatedAt *time.Time

	// ExpiresAt is when the token expires (nil for JWT auth)
	ExpiresAt *time.Time
}

// Principal represents an authenticated user or service account.
// This is returned by all authenticators regardless of the auth method.
type Principal struct {
	// Token contains metadata about the authentication token
	Token TokenInfo

	// Claims contains arbitrary authentication claims from the auth provider.
	// This is implementation-agnostic and can be populated by any authenticator.
	// For JWT: contains token claims (sub, email, groups, etc.)
	// For Bearer: can contain additional metadata if needed
	// For other auth types: can store relevant auth context
	Claims map[string]interface{}
}

// GetAuthMethod returns a string describing the authentication method used by the principal.
// This is used for metrics and logging to distinguish between different auth types.
func (p *Principal) GetAuthMethod() string {
	if p.Claims != nil {
		return "jwt"
	}
	return "unknown"
}

// AuthConfig holds configuration for authentication/authorization.
type AuthConfig struct {
	// Disabled allows bypassing authentication entirely (for development/testing).
	Disabled bool

	// Header specifies the HTTP header to read the authentication token from.
	// Common values: "Authorization", "X-Auth-Token".
	Header string

	// Audience specifies the expected audience for JWT tokens (JWT auth only).
	Audience string

	// Issuer specifies the expected issuer for JWT tokens (JWT auth only).
	Issuer string
}

// AuthError represents an authentication error with an HTTP status code.
type AuthError struct {
	Message    string
	StatusCode int
	Err        error
}

func (e *AuthError) Error() string {
	if e.Err != nil {
		return e.Message + ": " + e.Err.Error()
	}
	return e.Message
}

func (e *AuthError) Unwrap() error {
	return e.Err
}
