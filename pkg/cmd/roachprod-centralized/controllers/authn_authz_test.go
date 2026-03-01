// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package controllers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

// TestAuthenticationResult_GetAssociatedStatusCode tests the error-to-HTTP-status mapping
func TestAuthenticationResult_GetAssociatedStatusCode(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		expectedCode int
	}{
		{"ErrNotAuthenticated", authtypes.ErrNotAuthenticated, http.StatusUnauthorized},
		{"ErrInvalidToken", authtypes.ErrInvalidToken, http.StatusUnauthorized},
		{"ErrTokenExpired", authtypes.ErrTokenExpired, http.StatusUnauthorized},
		{"ErrUserNotProvisioned", authtypes.ErrUserNotProvisioned, http.StatusUnauthorized},
		{"ErrUserDeactivated", authtypes.ErrUserDeactivated, http.StatusUnauthorized},
		{"ErrServiceAccountDisabled", authtypes.ErrServiceAccountDisabled, http.StatusUnauthorized},
		{"ErrServiceAccountNotFound", authtypes.ErrServiceAccountNotFound, http.StatusUnauthorized},
		{"ErrUserNotFound", authtypes.ErrUserNotFound, http.StatusUnauthorized},
		{"ErrIPNotAllowed", authtypes.ErrIPNotAllowed, http.StatusForbidden},
		{"unknown error defaults to 401", errors.New("some unknown error"), http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &AuthenticationErrorResult{err: tt.err}
			assert.Equal(t, tt.expectedCode, result.GetAssociatedStatusCode())
		})
	}
}

// TestAuthenticationResult_GetError tests that public errors are returned correctly
func TestAuthenticationResult_GetError(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		expectedMsg string
	}{
		{
			name:        "PublicError is returned as-is",
			err:         authtypes.ErrNotAuthenticated, // Already a PublicError
			expectedMsg: "not authenticated",
		},
		{
			name:        "ErrTokenExpired returns its public message",
			err:         authtypes.ErrTokenExpired,
			expectedMsg: "token expired",
		},
		{
			name:        "ErrIPNotAllowed returns its public message",
			err:         authtypes.ErrIPNotAllowed,
			expectedMsg: "client IP not allowed for this service account",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &AuthenticationErrorResult{err: tt.err}
			assert.Contains(t, result.GetError().Error(), tt.expectedMsg)
		})
	}
}

// TestAuthenticationResult_GetError_PrivateErrorNotDisclosed tests that internal errors
// (like database connection details) are not disclosed to clients
func TestAuthenticationResult_GetError_PrivateErrorNotDisclosed(t *testing.T) {
	// Simulate an internal error that should NOT be disclosed to clients
	internalErr := errors.New("database connection failed: pq: connection refused to 10.0.0.5:5432")

	result := &AuthenticationErrorResult{err: internalErr}
	publicErr := result.GetError()

	// The public error should NOT contain internal details
	assert.NotContains(t, publicErr.Error(), "database")
	assert.NotContains(t, publicErr.Error(), "pq:")
	assert.NotContains(t, publicErr.Error(), "10.0.0.5")
	assert.NotContains(t, publicErr.Error(), "connection")

	// Should return a generic unauthorized message instead
	assert.Contains(t, publicErr.Error(), "unauthorized")
}

// mockAuthenticator is a test helper that implements auth.IAuthenticator
type mockAuthenticator struct {
	authenticateFn func(ctx context.Context, token, ip string) (*auth.Principal, error)
	authorizeFn    func(ctx context.Context, principal *auth.Principal, requirement *auth.AuthorizationRequirement, endpoint string) error
}

func (m *mockAuthenticator) Authenticate(
	ctx context.Context, token, ip string,
) (*auth.Principal, error) {
	return m.authenticateFn(ctx, token, ip)
}

func (m *mockAuthenticator) Authorize(
	ctx context.Context,
	principal *auth.Principal,
	requirement *auth.AuthorizationRequirement,
	endpoint string,
) error {
	if m.authorizeFn != nil {
		return m.authorizeFn(ctx, principal, requirement, endpoint)
	}

	// Default: perform actual permission checks
	// Check AnyOf permissions (OR logic)
	if len(requirement.AnyOf) > 0 && !principal.HasAnyPermission(requirement.AnyOf) {
		return authtypes.ErrForbidden
	}

	// Check required permissions (AND logic)
	if len(requirement.RequiredPermissions) > 0 && !principal.HasAllPermissions(requirement.RequiredPermissions) {
		return authtypes.ErrForbidden
	}

	return nil
}

// TestAuthMiddleware_EmptyToken tests that empty token returns 401
func TestAuthMiddleware_EmptyToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Create mock authenticator that returns ErrNotAuthenticated for empty token
	mockAuth := &mockAuthenticator{
		authenticateFn: func(ctx context.Context, token, ip string) (*auth.Principal, error) {
			if token == "" {
				return nil, authtypes.ErrNotAuthenticated
			}
			return nil, nil
		},
	}

	ctrl := &Controller{}
	middleware := ctrl.AuthMiddleware(mockAuth, "Authorization")

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/api/test", nil)
	// No Authorization header

	middleware(c)

	assert.True(t, c.IsAborted())
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// TestAuthMiddleware_ValidToken tests that valid token stores principal in context
func TestAuthMiddleware_ValidToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	expectedPrincipal := &auth.Principal{
		User: &authmodels.User{Email: "test@example.com"},
	}

	mockAuth := &mockAuthenticator{
		authenticateFn: func(ctx context.Context, token, ip string) (*auth.Principal, error) {
			return expectedPrincipal, nil
		},
	}

	ctrl := &Controller{}
	middleware := ctrl.AuthMiddleware(mockAuth, "Authorization")

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/api/test", nil)
	c.Request.Header.Set("Authorization", "Bearer valid-token")

	middleware(c)

	assert.False(t, c.IsAborted())

	// Verify principal is stored in context
	principal, exists := GetPrincipal(c)
	assert.True(t, exists)
	assert.Equal(t, "test@example.com", principal.User.Email)
}

// TestAuthMiddleware_BearerPrefixStripping tests that "Bearer " prefix is stripped
func TestAuthMiddleware_BearerPrefixStripping(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var capturedToken string
	mockAuth := &mockAuthenticator{
		authenticateFn: func(ctx context.Context, token, ip string) (*auth.Principal, error) {
			capturedToken = token
			return &auth.Principal{}, nil
		},
	}

	ctrl := &Controller{}
	middleware := ctrl.AuthMiddleware(mockAuth, "Authorization")

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/api/test", nil)
	c.Request.Header.Set("Authorization", "Bearer my-token-value")

	middleware(c)

	// Token should have "Bearer " prefix stripped
	assert.Equal(t, "my-token-value", capturedToken)
}

// TestAuthzMiddleware_NoRequirement tests that nil requirement allows access
func TestAuthzMiddleware_NoRequirement(t *testing.T) {
	gin.SetMode(gin.TestMode)

	mockAuth := &mockAuthenticator{}
	ctrl := &Controller{}
	middleware := ctrl.AuthzMiddleware(mockAuth, nil)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/api/test", nil)

	middleware(c)

	assert.False(t, c.IsAborted())
}

// TestAuthzMiddleware_NoPrincipal tests that missing principal returns 401
func TestAuthzMiddleware_NoPrincipal(t *testing.T) {
	gin.SetMode(gin.TestMode)

	mockAuth := &mockAuthenticator{}
	ctrl := &Controller{}
	middleware := ctrl.AuthzMiddleware(mockAuth, &auth.AuthorizationRequirement{
		RequiredPermissions: []string{"clusters:create"},
	})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/api/test", nil)
	// No principal set in context

	middleware(c)

	assert.True(t, c.IsAborted())
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// TestAuthzMiddleware_HasRequiredPermission tests that principal with permission is allowed
func TestAuthzMiddleware_HasRequiredPermission(t *testing.T) {
	gin.SetMode(gin.TestMode)

	principal := &auth.Principal{
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{Permission: "clusters:create"},
		},
	}

	mockAuth := &mockAuthenticator{}
	ctrl := &Controller{}
	middleware := ctrl.AuthzMiddleware(mockAuth, &auth.AuthorizationRequirement{
		RequiredPermissions: []string{"clusters:create"},
	})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/api/test", nil)
	SetPrincipal(c, principal)

	middleware(c)

	assert.False(t, c.IsAborted())
}

// TestAuthzMiddleware_MissingRequiredPermission tests that principal without permission is denied
func TestAuthzMiddleware_MissingRequiredPermission(t *testing.T) {
	gin.SetMode(gin.TestMode)

	principal := &auth.Principal{
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{Permission: "clusters:read"},
		},
	}

	mockAuth := &mockAuthenticator{}
	ctrl := &Controller{}
	middleware := ctrl.AuthzMiddleware(mockAuth, &auth.AuthorizationRequirement{
		RequiredPermissions: []string{"clusters:create"},
	})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/api/test", nil)
	SetPrincipal(c, principal)

	middleware(c)

	assert.True(t, c.IsAborted())
	assert.Equal(t, http.StatusForbidden, w.Code)
}

// TestAuthzMiddleware_AnyOfPermissions tests OR logic for permissions
func TestAuthzMiddleware_AnyOfPermissions(t *testing.T) {
	gin.SetMode(gin.TestMode)

	principal := &auth.Principal{
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{Permission: "clusters:read"},
		},
	}

	mockAuth := &mockAuthenticator{}
	ctrl := &Controller{}
	middleware := ctrl.AuthzMiddleware(mockAuth, &auth.AuthorizationRequirement{
		AnyOf: []string{"clusters:create", "clusters:read", "clusters:delete"},
	})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/api/test", nil)
	SetPrincipal(c, principal)

	middleware(c)

	assert.False(t, c.IsAborted()) // Has "clusters:read"
}

// TestAuthzMiddleware_AnyOfPermissions_Denied tests OR logic when no match
func TestAuthzMiddleware_AnyOfPermissions_Denied(t *testing.T) {
	gin.SetMode(gin.TestMode)

	principal := &auth.Principal{
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{Permission: "clusters:list"},
		},
	}

	mockAuth := &mockAuthenticator{}
	ctrl := &Controller{}
	middleware := ctrl.AuthzMiddleware(mockAuth, &auth.AuthorizationRequirement{
		AnyOf: []string{"clusters:create", "clusters:read", "clusters:delete"},
	})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/api/test", nil)
	SetPrincipal(c, principal)

	middleware(c)

	assert.True(t, c.IsAborted())
	assert.Equal(t, http.StatusForbidden, w.Code)
}

// TestAuthzMiddleware_AllPermissionsRequired tests AND logic
func TestAuthzMiddleware_AllPermissionsRequired(t *testing.T) {
	gin.SetMode(gin.TestMode)

	principal := &auth.Principal{
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{Permission: "clusters:create"},
			&authmodels.UserPermission{Permission: "clusters:read"},
		},
	}

	mockAuth := &mockAuthenticator{}
	ctrl := &Controller{}
	middleware := ctrl.AuthzMiddleware(mockAuth, &auth.AuthorizationRequirement{
		RequiredPermissions: []string{"clusters:create", "clusters:read"},
	})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/api/test", nil)
	SetPrincipal(c, principal)

	middleware(c)

	assert.False(t, c.IsAborted())
}

// TestAuthzMiddleware_AllPermissionsRequired_MissingOne tests AND logic when missing one
func TestAuthzMiddleware_AllPermissionsRequired_MissingOne(t *testing.T) {
	gin.SetMode(gin.TestMode)

	principal := &auth.Principal{
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{Permission: "clusters:create"},
			// Missing clusters:read
		},
	}

	mockAuth := &mockAuthenticator{}
	ctrl := &Controller{}
	middleware := ctrl.AuthzMiddleware(mockAuth, &auth.AuthorizationRequirement{
		RequiredPermissions: []string{"clusters:create", "clusters:read"},
	})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/api/test", nil)
	SetPrincipal(c, principal)

	middleware(c)

	assert.True(t, c.IsAborted())
	assert.Equal(t, http.StatusForbidden, w.Code)
}
