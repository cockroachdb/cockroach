// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"
	"time"

	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

	// Authorize checks if the principal has the required permissions for an endpoint.
	// Returns nil on success, or an error if authorization fails.
	// This method also records authorization metrics.
	Authorize(ctx context.Context, principal *Principal, requirement *AuthorizationRequirement, endpoint string) error
}

// AuthorizationRequirement defines what authorization is needed for an endpoint.
type AuthorizationRequirement struct {
	// RequiredPermissions specifies permissions that the principal must have.
	// These are just permission names (NOT including scope).
	// If multiple permissions are specified, the principal must have ALL of them (AND logic).
	//
	// Format: Simple permission strings like "clusters:create", "scim:manage-user", "tokens:mine:view"
	// Examples:
	//   - "clusters:create" - Permission to create clusters (scope checked by service layer)
	//   - "scim:manage-user" - Permission to manage users via SCIM
	//   - "tokens:mine:view" - Permission to view own tokens
	//   - "admin" - Admin permissions
	//
	// Note: Middleware only checks if the permission exists. Services are responsible
	// for validating scope using principal.HasPermissionScoped().
	RequiredPermissions []string

	// AnyOf allows OR logic for permissions. If specified, the principal must have
	// at least ONE of the permissions in the list.
	AnyOf []string
}

// TokenInfo contains metadata about the authentication token.
// For bearer tokens, all fields are populated from the database.
// For JWT tokens, ID is a zero UUID, and CreatedAt/ExpiresAt are populated from iat/exp claims.
type TokenInfo struct {
	// ID is the unique identifier for the token (zero UUID for JWT)
	ID uuid.UUID

	// Type indicates whether this is a user or service account token
	Type authmodels.TokenType

	// CreatedAt is when the token was created (from iat claim for JWT)
	CreatedAt *time.Time

	// ExpiresAt is when the token expires (from exp claim for JWT)
	ExpiresAt *time.Time
}

// Principal represents an authenticated user or service account.
// This is returned by all authenticators regardless of the auth method.
type Principal struct {
	// Token contains metadata about the authentication token
	Token TokenInfo

	// UserID is set for user tokens
	UserID *uuid.UUID

	// ServiceAccountID is set for service account tokens
	ServiceAccountID *uuid.UUID

	// DelegatedFrom is set for service accounts that inherit permissions from a user principal.
	// This is the UserID of the user principal from which permissions are inherited.
	DelegatedFrom      *uuid.UUID
	DelegatedFromEmail string

	// User contains full user information (for bearer auth)
	User *authmodels.User

	// ServiceAccount contains full service account information (for bearer auth)
	ServiceAccount *authmodels.ServiceAccount

	// Permissions contains all permissions for this principal (unified for both users and service accounts)
	Permissions []authmodels.Permission

	// Claims contains arbitrary authentication claims from the auth provider.
	// This is implementation-agnostic and can be populated by any authenticator.
	// For JWT: contains token claims (sub, email, groups, etc.)
	// For Bearer: can contain additional metadata if needed
	// For other auth types: can store relevant auth context
	Claims map[string]interface{}
}

// HasPermission checks if the principal has a specific permission (any scope).
// This is used by middleware to verify the user can attempt this action.
// Supports wildcard: if the principal has a permission with value "*", it matches any permission.
// Example: HasPermission("clusters:create") returns true if the user has this permission
// with any scope, or if the user has the "*" wildcard permission.
func (p *Principal) HasPermission(permission string) bool {
	for _, perm := range p.Permissions {
		permValue := perm.GetPermission()
		// Wildcard permission grants access to everything (used by disabled authenticator)
		if permValue == "*" {
			return true
		}
		if permValue == permission {
			return true
		}
	}
	return false
}

// HasPermissionScoped checks if the principal has a permission for a specific scope.
// This is used by services to enforce scope-based access control.
// Supports directional wildcard matching: "*" in the principal scope matches any required scope.
// Example: HasPermissionScoped("clusters:create", "gcp-engineering")
func (p *Principal) HasPermissionScoped(permission, scope string) bool {
	for _, perm := range p.Permissions {
		// Check permission match
		if perm.GetPermission() != "*" && perm.GetPermission() != permission {
			continue
		}

		permScope := perm.GetScope()
		// Directional scope check:
		// principal "*" grants any scope, but requested "*" does NOT match narrow principal scopes.
		if permScope == "*" || permScope == scope {
			return true
		}
	}

	return false
}

// HasAnyPermission checks if the principal has any of the specified permissions (OR logic).
func (p *Principal) HasAnyPermission(permissions []string) bool {
	for _, perm := range permissions {
		if p.HasPermission(perm) {
			return true
		}
	}
	return false
}

// HasAllPermissions checks if the principal has all of the specified permissions (AND logic).
func (p *Principal) HasAllPermissions(permissions []string) bool {
	for _, perm := range permissions {
		if !p.HasPermission(perm) {
			return false
		}
	}
	return true
}

// GetAuthMethod returns a string describing the authentication method used by the principal.
// This is used for metrics and logging to distinguish between different auth types.
func (p *Principal) GetAuthMethod() string {
	if p.Token.Type != "" {
		return string(p.Token.Type) // "user" or "service-account"
	}
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
