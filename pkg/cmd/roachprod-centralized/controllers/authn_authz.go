// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package controllers

import (
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
)

// Context keys for storing authentication/authorization data
const (
	PrincipalContextKey = "principal" // Stores *auth.Principal
)

// AuthenticationErrorResult is a result DTO for authentication middleware errors.
// It wraps authentication errors to prevent leaking internal details to clients.
type AuthenticationErrorResult struct {
	err error // The original error from authentication
}

// GetData returns nil as authentication errors don't return data.
func (r *AuthenticationErrorResult) GetData() interface{} {
	return nil
}

// GetError returns a public error safe for client consumption.
// If the original error is already a PublicError, it's returned as-is.
// Otherwise, a generic error is derived from the HTTP status code.
func (r *AuthenticationErrorResult) GetError() error {
	// If the error is already a PublicError, return it as-is
	var publicErr *utils.PublicError
	if errors.As(r.err, &publicErr) {
		return publicErr
	}

	// For internal errors, return a generic message to avoid leaking details
	return utils.NewPublicError(ErrUnauthorized)
}

// GetAssociatedStatusCode returns the HTTP status code for the authentication error.
func (r *AuthenticationErrorResult) GetAssociatedStatusCode() int {

	// Authentication errors -> 401 Unauthorized
	switch {
	case errors.Is(r.err, auth.ErrNotAuthenticated),
		errors.Is(r.err, auth.ErrInvalidToken):
		return http.StatusUnauthorized
	}

	return http.StatusUnauthorized // Default for unknown auth errors
}

// AuthorizationErrorResult is a result DTO for authorization middleware errors.
// It wraps authorization errors to prevent leaking permission details to clients.
type AuthorizationErrorResult struct {
	err error
}

// GetData returns nil as authorization errors don't return data.
func (r *AuthorizationErrorResult) GetData() interface{} {
	return nil
}

// GetError returns a public error safe for client consumption.
func (r *AuthorizationErrorResult) GetError() error {
	// If the error is already a PublicError, return it as-is
	var publicErr *utils.PublicError
	if errors.As(r.err, &publicErr) {
		return publicErr
	}
	return utils.NewPublicError(ErrForbidden)
}

// GetAssociatedStatusCode returns the HTTP status code for the authorization error.
func (r *AuthorizationErrorResult) GetAssociatedStatusCode() int {
	return http.StatusForbidden
}

// AuthenticationType is the type of authentication required for a controller.
type AuthenticationType string

const (
	// AuthenticationTypeNone is the authentication type that does not
	// check authentication.
	AuthenticationTypeNone AuthenticationType = "none"
	// AuthenticationTypeRequired is the type of authentication that requires
	// authentication.
	AuthenticationTypeRequired AuthenticationType = "required"
)

// AuthMiddleware creates a Gin middleware that wraps a framework-agnostic authenticator.
// The authenticator handles all authentication logic including disabled mode (DisabledAuthenticator).
func (ctrl *Controller) AuthMiddleware(
	authenticator auth.IAuthenticator, headerName string,
) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Extract token from configured header
		authHeader := c.GetHeader(headerName)

		// Extract token value
		var token string
		if authHeader != "" {
			// Remove "Bearer " prefix if present
			token = strings.TrimPrefix(authHeader, "Bearer ")
			token = strings.TrimSpace(token)
		}

		// Authenticate using the framework-agnostic authenticator
		// The authenticator will handle validation and return appropriate errors:
		// - DisabledAuthenticator will always succeed regardless of token
		// - JWT/Bearer authenticators will validate the token and return service errors on failure
		principal, err := authenticator.Authenticate(c.Request.Context(), token, c.ClientIP())
		if err != nil {
			// Log the detailed internal error
			ctrl.GetRequestLogger(c).Error("authentication failed", "error", err.Error())

			// Pass the original error to AuthenticationErrorResult
			// It will derive the appropriate HTTP status code and public error message
			ctrl.Render(c, &AuthenticationErrorResult{err: err})
			c.Abort()
			return
		}

		// Store principal in context
		SetPrincipal(c, principal)

		// Set legacy session keys for compatibility with existing controllers
		setLegacySessionKeys(c, principal)

		c.Next()
	}
}

// SetPrincipal stores the authenticated principal in the Gin context.
func SetPrincipal(c *gin.Context, principal *auth.Principal) {
	c.Set(PrincipalContextKey, principal)
}

// GetPrincipal retrieves the authenticated principal from the Gin context.
func GetPrincipal(c *gin.Context) (*auth.Principal, bool) {
	value, exists := c.Get(PrincipalContextKey)
	if !exists {
		return nil, false
	}
	principal, ok := value.(*auth.Principal)
	return principal, ok
}

// setLegacySessionKeys sets legacy session keys for backward compatibility.
func setLegacySessionKeys(c *gin.Context, principal *auth.Principal) {
	// For JWT auth or any auth type with Claims
	if principal.Claims != nil {
		// Fallback to Claims if User is not populated
		if email, ok := principal.Claims["email"].(string); ok {
			c.Set(SessionUserEmail, email)
		}
		if sub, ok := principal.Claims["sub"].(string); ok {
			c.Set(SessionUserID, sub)
		}
	}
}
