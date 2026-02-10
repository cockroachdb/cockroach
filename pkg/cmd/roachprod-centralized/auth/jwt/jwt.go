// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwt

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/idtoken"
)

// JWTAuthenticator implements IAuthenticator using Google ID tokens (JWT).
// This is the existing authentication method used for Google-based authentication.
type JWTAuthenticator struct {
	config      auth.AuthConfig
	authService authtypes.IService
	metrics     authtypes.IAuthMetricsRecorder
}

// NewJWTAuthenticator creates a new JWT-based authenticator.
func NewJWTAuthenticator(
	config auth.AuthConfig, authService authtypes.IService, metrics authtypes.IAuthMetricsRecorder,
) *JWTAuthenticator {
	return &JWTAuthenticator{
		config:      config,
		authService: authService,
		metrics:     metrics,
	}
}

// Authenticate validates a JWT token and returns the authenticated principal.
// This is a framework-agnostic implementation that returns (*Principal, error).
// Service errors are returned directly for the controller layer to map to HTTP status codes.
func (a *JWTAuthenticator) Authenticate(
	ctx context.Context, token string, _ string,
) (*auth.Principal, error) {
	start := timeutil.Now()

	if token == "" {
		a.metrics.RecordAuthentication("error", "none", timeutil.Since(start))
		return nil, authtypes.ErrNotAuthenticated
	}

	// Validate the JWT token
	payload, err := a.validateToken(ctx, token)
	if err != nil {
		a.metrics.RecordAuthentication("error", "jwt", timeutil.Since(start))
		return nil, errors.CombineErrors(authtypes.ErrInvalidToken, err)
	}

	// Extract email and name from claims
	var email, fullName string
	if emailClaim, ok := payload.Claims["email"].(string); ok {
		email = emailClaim
	}
	if nameClaim, ok := payload.Claims["name"].(string); ok {
		fullName = nameClaim
	}

	// Extract token timestamps from JWT claims
	var createdAt, expiresAt *time.Time
	if iat, ok := payload.Claims["iat"].(float64); ok {
		t := timeutil.Unix(int64(iat), 0)
		createdAt = &t
	}
	if exp, ok := payload.Claims["exp"].(float64); ok {
		t := timeutil.Unix(int64(exp), 0)
		expiresAt = &t
	}

	// Create principal with User fields populated from JWT claims
	// Note: JWT auth doesn't use database-backed users, so we create a lightweight User struct
	principal := &auth.Principal{
		Token: auth.TokenInfo{
			ID:        uuid.UUID{}, // Zero UUID for JWT tokens
			Type:      authmodels.TokenTypeUser,
			CreatedAt: createdAt,
			ExpiresAt: expiresAt,
		},
		Claims: payload.Claims, // Store claims for additional context
		UserID: &uuid.UUID{},   // Zero UUID for JWT tokens
		User: &authmodels.User{
			ID:       uuid.UUID{}, // Zero UUID for JWT tokens
			Email:    email,
			FullName: fullName,
			Active:   true, // JWT validation implies active user
		},
		// Grant wildcard permission - matches any permission check
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{
				ID:         uuid.UUID{},
				UserID:     uuid.UUID{},
				Scope:      "*",
				Permission: "*", // Wildcard permission grants access to everything
			},
		},
	}

	a.metrics.RecordAuthentication("success", "jwt", timeutil.Since(start))
	return principal, nil
}

// validateToken validates a Google ID token and returns the payload.
func (a *JWTAuthenticator) validateToken(
	ctx context.Context, token string,
) (*idtoken.Payload, error) {
	// Validate the token with Google's idtoken library
	payload, err := idtoken.Validate(ctx, token, a.config.Audience)
	if err != nil {
		return nil, errors.Wrap(err, "failed to validate token")
	}

	// Verify issuer if configured
	if a.config.Issuer != "" {
		if payload.Issuer != a.config.Issuer {
			return nil, errors.Newf("invalid issuer: expected %s, got %s",
				a.config.Issuer, payload.Issuer)
		}
	}

	return payload, nil
}

// Authorize checks if the principal has the required permissions for an endpoint.
// Returns nil on success, or authtypes.ErrForbidden if authorization fails.
// This method also records authorization metrics.
func (a *JWTAuthenticator) Authorize(
	ctx context.Context,
	principal *auth.Principal,
	requirement *auth.AuthorizationRequirement,
	endpoint string,
) error {
	start := timeutil.Now()
	authMethod := principal.GetAuthMethod()

	// Check AnyOf permissions (OR logic)
	if len(requirement.AnyOf) > 0 && !principal.HasAnyPermission(requirement.AnyOf) {
		a.metrics.RecordAuthzDecision("deny", "missing_permission", endpoint, authMethod)
		a.metrics.RecordAuthzLatency(endpoint, timeutil.Since(start))
		return authtypes.ErrForbidden
	}

	// Check required permissions (AND logic)
	if len(requirement.RequiredPermissions) > 0 && !principal.HasAllPermissions(requirement.RequiredPermissions) {
		a.metrics.RecordAuthzDecision("deny", "missing_permission", endpoint, authMethod)
		a.metrics.RecordAuthzLatency(endpoint, timeutil.Since(start))
		return authtypes.ErrForbidden
	}

	a.metrics.RecordAuthzDecision("allow", "", endpoint, authMethod)
	a.metrics.RecordAuthzLatency(endpoint, timeutil.Since(start))
	return nil
}
