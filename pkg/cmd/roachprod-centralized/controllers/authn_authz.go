// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package controllers

import (
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
)

// Context keys for storing authentication/authorization data
const (
	PrincipalContextKey = "principal" // Stores *auth.Principal
	TokenContextKey     = "token"     // Stores *authmodels.ApiToken (bearer auth only)
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
	case errors.Is(r.err, authtypes.ErrNotAuthenticated),
		errors.Is(r.err, authtypes.ErrInvalidToken),
		errors.Is(r.err, authtypes.ErrTokenExpired),
		errors.Is(r.err, authtypes.ErrUserNotProvisioned),
		errors.Is(r.err, authtypes.ErrUserDeactivated),
		errors.Is(r.err, authtypes.ErrServiceAccountDisabled),
		errors.Is(r.err, authtypes.ErrServiceAccountNotFound),
		errors.Is(r.err, authtypes.ErrUserNotFound):
		return http.StatusUnauthorized

	// Authorization errors -> 403 Forbidden
	case errors.Is(r.err, authtypes.ErrIPNotAllowed):
		return http.StatusForbidden
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

// AuthzMiddleware creates authorization middleware based on requirements.
// This middleware must run AFTER AuthMiddleware so that the Principal is available.
func (ctrl *Controller) AuthzMiddleware(
	authenticator auth.IAuthenticator, requirement *auth.AuthorizationRequirement,
) gin.HandlerFunc {
	return func(c *gin.Context) {
		// If no authorization requirement, allow access
		if requirement == nil {
			c.Next()
			return
		}

		// Get the authenticated principal from context
		principal, exists := GetPrincipal(c)
		if !exists {
			// If we get here, authentication middleware didn't run or failed
			ctrl.GetRequestLogger(c).Error("authorization check failed: no principal in context")
			ctrl.Render(c, &AuthenticationErrorResult{err: ErrUnauthorized})
			c.Abort()
			return
		}

		// Use the authenticator to check authorization (it will handle metrics recording)
		if err := authenticator.Authorize(c.Request.Context(), principal, requirement, c.FullPath()); err != nil {
			ctrl.GetRequestLogger(c).Warn("authorization denied",
				"principal_type", principal.GetAuthMethod(),
				"endpoint", c.FullPath())
			ctrl.Render(c, &AuthorizationErrorResult{err: err})
			c.Abort()
			return
		}

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

// GetToken retrieves the validated token from the Gin context.
// Note: This is only populated for bearer token authentication.
func GetToken(c *gin.Context) (*authmodels.ApiToken, bool) {
	value, exists := c.Get(TokenContextKey)
	if !exists {
		return nil, false
	}
	token, ok := value.(*authmodels.ApiToken)
	return token, ok
}

// setLegacySessionKeys sets legacy session keys for backward compatibility.
func setLegacySessionKeys(c *gin.Context, principal *auth.Principal) {
	// For bearer auth with user tokens
	if principal.User != nil {
		c.Set(SessionUserEmail, principal.User.Email)
		c.Set(SessionUserID, principal.User.OktaUserID)
		return
	}

	// For bearer auth with service account tokens
	if principal.ServiceAccount != nil {
		c.Set(SessionUserEmail, principal.ServiceAccount.Name)
		c.Set(SessionUserID, principal.ServiceAccount.ID.String())
		return
	}

	// For JWT auth where User/ServiceAccount are not populated
	if principal.Claims != nil {
		if email, ok := principal.Claims["email"].(string); ok {
			c.Set(SessionUserEmail, email)
		}
		if sub, ok := principal.Claims["sub"].(string); ok {
			c.Set(SessionUserID, sub)
		}
	}
}
