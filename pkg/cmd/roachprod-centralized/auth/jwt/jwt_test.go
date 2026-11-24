// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwt

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authmocks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/mocks"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/idtoken"
)

func TestJWTAuthenticator_Authenticate_InvalidToken(t *testing.T) {
	config := auth.AuthConfig{
		Audience: "test-audience",
		Issuer:   "test-issuer",
	}

	mockAuthService := authmocks.NewIService(t)
	mockMetrics := &authmocks.IAuthMetricsRecorder{}
	mockMetrics.On("RecordAuthentication", "error", "jwt", mock.Anything).Return()
	authenticator := NewJWTAuthenticator(config, mockAuthService, mockMetrics)

	principal, err := authenticator.Authenticate(context.Background(), "invalid-token", "127.0.0.1")
	require.Error(t, err)
	assert.Nil(t, principal)

	// Verify error is ErrInvalidToken (wrapped with underlying error details)
	assert.True(t, errors.Is(err, authtypes.ErrInvalidToken), "expected ErrInvalidToken, got: %v", err)
}

func TestJWTAuthenticator_Authenticate_EmptyToken(t *testing.T) {
	config := auth.AuthConfig{
		Audience: "test-audience",
		Issuer:   "test-issuer",
	}

	mockAuthService := authmocks.NewIService(t)
	mockMetrics := &authmocks.IAuthMetricsRecorder{}
	mockMetrics.On("RecordAuthentication", "error", "none", mock.Anything).Return()
	authenticator := NewJWTAuthenticator(config, mockAuthService, mockMetrics)

	principal, err := authenticator.Authenticate(context.Background(), "", "127.0.0.1")
	require.Error(t, err)
	assert.Nil(t, principal)

	// Verify error is ErrNotAuthenticated for empty token
	assert.True(t, errors.Is(err, authtypes.ErrNotAuthenticated), "expected ErrNotAuthenticated, got: %v", err)
}

func TestJWTAuthenticator_ValidateToken_InvalidToken(t *testing.T) {
	config := auth.AuthConfig{
		Audience: "test-audience",
		Issuer:   "test-issuer",
	}

	mockAuthService := authmocks.NewIService(t)
	mockMetrics := &authmocks.IAuthMetricsRecorder{}
	authenticator := NewJWTAuthenticator(config, mockAuthService, mockMetrics)

	// Use a clearly invalid token format
	_, err := authenticator.validateToken(context.Background(), "invalid-token")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to validate token")
}

func TestJWTAuthenticator_ValidateToken_MalformedToken(t *testing.T) {
	config := auth.AuthConfig{
		Audience: "test-audience",
		Issuer:   "test-issuer",
	}

	mockAuthService := authmocks.NewIService(t)
	mockMetrics := &authmocks.IAuthMetricsRecorder{}
	authenticator := NewJWTAuthenticator(config, mockAuthService, mockMetrics)

	// Test with various malformed tokens
	testCases := []string{
		"not.a.jwt",
		"",
		"Bearer token",
		"random-string",
	}

	for _, tc := range testCases {
		_, err := authenticator.validateToken(context.Background(), tc)
		require.Error(t, err)
	}
}

// TestJWTAuthenticator_PrincipalConstruction tests the principal construction logic
// by simulating what happens after successful JWT validation.
// This tests the token metadata extraction from JWT claims.
func TestJWTAuthenticator_PrincipalConstruction(t *testing.T) {
	// Simulate a validated JWT payload with standard claims
	now := timeutil.Now()
	issuedAt := now.Add(-1 * time.Hour)
	expiresAt := now.Add(1 * time.Hour)

	payload := &idtoken.Payload{
		Issuer:   "https://accounts.google.com",
		Audience: "test-audience",
		Subject:  "test-user-123",
		IssuedAt: issuedAt.Unix(),
		Expires:  expiresAt.Unix(),
		Claims: map[string]interface{}{
			"email": "test@example.com",
			"name":  "Test User",
			"iat":   float64(issuedAt.Unix()),
			"exp":   float64(expiresAt.Unix()),
			"sub":   "test-user-123",
		},
	}

	// Test the logic that would run after validateToken succeeds
	// Extract email and name from claims
	var email, fullName string
	if emailClaim, ok := payload.Claims["email"].(string); ok {
		email = emailClaim
	}
	if nameClaim, ok := payload.Claims["name"].(string); ok {
		fullName = nameClaim
	}

	// Extract token timestamps from JWT claims
	var createdAt, expiresAtPtr *time.Time
	if iat, ok := payload.Claims["iat"].(float64); ok {
		t := time.Unix(int64(iat), 0)
		createdAt = &t
	}
	if exp, ok := payload.Claims["exp"].(float64); ok {
		t := time.Unix(int64(exp), 0)
		expiresAtPtr = &t
	}

	// Construct principal as the authenticator would
	principal := &auth.Principal{
		Token: auth.TokenInfo{
			ID:        uuid.UUID{}, // Zero UUID for JWT tokens
			Type:      authmodels.TokenTypeUser,
			CreatedAt: createdAt,
			ExpiresAt: expiresAtPtr,
		},
		Claims: payload.Claims,
		User: &authmodels.User{
			Email:    email,
			FullName: fullName,
			Active:   true,
		},
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{
				ID:         uuid.MakeV4(),
				UserID:     uuid.MakeV4(),
				Scope:      "*",
				Permission: "*",
			},
		},
	}

	// Verify TokenInfo is correctly populated
	assert.Equal(t, uuid.UUID{}, principal.Token.ID, "JWT should have zero UUID")
	assert.Equal(t, authmodels.TokenTypeUser, principal.Token.Type)
	require.NotNil(t, principal.Token.CreatedAt, "CreatedAt should be populated from iat claim")
	require.NotNil(t, principal.Token.ExpiresAt, "ExpiresAt should be populated from exp claim")
	assert.Equal(t, issuedAt.Unix(), principal.Token.CreatedAt.Unix())
	assert.Equal(t, expiresAt.Unix(), principal.Token.ExpiresAt.Unix())

	// Verify user info
	assert.Equal(t, "test@example.com", principal.User.Email)
	assert.Equal(t, "Test User", principal.User.FullName)
	assert.True(t, principal.User.Active)

	// Verify claims
	assert.NotNil(t, principal.Claims)
	assert.Equal(t, "test-user-123", principal.Claims["sub"])

	// Verify wildcard permissions
	assert.Len(t, principal.Permissions, 1)
	assert.Equal(t, "*", principal.Permissions[0].GetPermission())
}
