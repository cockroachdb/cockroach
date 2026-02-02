// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwt

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/idtoken"
)

// JWTAuthenticator implements IAuthenticator using Google ID tokens (JWT).
// This is the existing authentication method used for Google-based authentication.
type JWTAuthenticator struct {
	config auth.AuthConfig
}

// NewJWTAuthenticator creates a new JWT-based authenticator.
func NewJWTAuthenticator(config auth.AuthConfig) *JWTAuthenticator {
	return &JWTAuthenticator{
		config: config,
	}
}

// Authenticate validates a JWT token and returns the authenticated principal.
// This is a framework-agnostic implementation that returns (*Principal, error).
// Service errors are returned directly for the controller layer to map to HTTP status codes.
func (a *JWTAuthenticator) Authenticate(
	ctx context.Context, token string, _ string,
) (*auth.Principal, error) {

	if token == "" {
		return nil, auth.ErrNotAuthenticated
	}

	// Validate the JWT token
	payload, err := a.validateToken(ctx, token)
	if err != nil {
		return nil, errors.CombineErrors(auth.ErrInvalidToken, err)
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
			CreatedAt: createdAt,
			ExpiresAt: expiresAt,
		},
		Claims: payload.Claims, // Store claims for additional context
	}

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
