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
		{"ErrNotAuthenticated", auth.ErrNotAuthenticated, http.StatusUnauthorized},
		{"ErrInvalidToken", auth.ErrInvalidToken, http.StatusUnauthorized},
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
			err:         auth.ErrNotAuthenticated, // Already a PublicError
			expectedMsg: "not authenticated",
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
}

func (m *mockAuthenticator) Authenticate(
	ctx context.Context, token, ip string,
) (*auth.Principal, error) {
	return m.authenticateFn(ctx, token, ip)
}

// TestAuthMiddleware_EmptyToken tests that empty token returns 401
func TestAuthMiddleware_EmptyToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Create mock authenticator that returns ErrNotAuthenticated for empty token
	mockAuth := &mockAuthenticator{
		authenticateFn: func(ctx context.Context, token, ip string) (*auth.Principal, error) {
			if token == "" {
				return nil, auth.ErrNotAuthenticated
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

	expectedPrincipal := &auth.Principal{}

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
	_, exists := GetPrincipal(c)
	assert.True(t, exists)
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
