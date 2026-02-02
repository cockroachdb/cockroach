// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/errors"
)

const (
	// ControllerPath is the base path for authentication endpoints.
	ControllerPath = "/v1/auth"
)

// Request/Response types

// TokenInfo contains metadata about the authentication token.
type TokenInfo struct {
	ID         string `json:"id"`
	Token      string `json:"token"`
	Type       string `json:"type"`
	Status     string `json:"status,omitempty"`       // e.g., "valid", "expired", "revoked"
	CreatedAt  string `json:"created_at,omitempty"`   // ISO 8601 format, omitted for JWT auth
	ExpiresAt  string `json:"expires_at,omitempty"`   // ISO 8601 format, omitted for JWT auth
	LastUsedAt string `json:"last_used_at,omitempty"` // ISO 8601 format, omitted for JWT auth
}

// WhoAmIResponse contains information about the current authenticated principal.
type WhoAmIResponse struct {
	User        *UserInfo        `json:"user,omitempty"`
	Permissions []PermissionInfo `json:"permissions"`
	Token       TokenInfo        `json:"token"`
}

// UserInfo contains user details.
type UserInfo struct {
	ID     string `json:"id"`
	Email  string `json:"email"`
	Name   string `json:"name"`
	Active bool   `json:"active"`
}

// PermissionInfo contains permission details.
type PermissionInfo struct {
	Provider   string `json:"provider"`
	Account    string `json:"account"`
	Permission string `json:"permission"`
}

// Error handling types

// AuthResultError represents the standard error for authentication responses.
type AuthResultError struct {
	Error error
}

// GetError returns the error from the AuthResultError.
func (dto *AuthResultError) GetError() error {
	return dto.Error
}

// GetData returns nil for error responses.
func (dto *AuthResultError) GetData() any {
	return nil
}

// GetAssociatedStatusCode returns the HTTP status code for authentication errors.
func (dto *AuthResultError) GetAssociatedStatusCode() int {
	err := dto.GetError()
	switch {
	case err == nil:
		return http.StatusOK
	// Auth-specific error mappings
	case errors.Is(err, auth.ErrNotAuthenticated):
		return http.StatusUnauthorized
	case errors.Is(err, auth.ErrInvalidToken):
		return http.StatusUnauthorized
	default:
		// Fall back to generic error handling
		return controllers.GetGenericStatusCode(err)
	}
}

// AuthResult is the standard result type for authentication controller responses.
type AuthResult struct {
	AuthResultError
	Data any `json:"data,omitempty"`
}

// GetData returns the data from the result.
func (dto *AuthResult) GetData() any {
	return dto.Data
}

// NewAuthResult creates a new AuthResult with data and error.
// This is a simpler alternative to the FromService pattern.
func NewAuthResult(data any, err error) *AuthResult {
	return &AuthResult{
		AuthResultError: AuthResultError{Error: err},
		Data:            data,
	}
}

// ==================== Builder Functions ====================
// These functions convert service models to DTOs without the weird
// empty struct creation pattern.

// BuildWhoAmIResponse converts a principal to a WhoAmI response DTO.
func BuildWhoAmIResponse(principal *auth.Principal) WhoAmIResponse {
	// Build token info
	tokenInfo := TokenInfo{
		ID: principal.Token.ID.String(),
	}
	if principal.Token.CreatedAt != nil {
		tokenInfo.CreatedAt = principal.Token.CreatedAt.Format("2006-01-02T15:04:05Z07:00")
	}
	if principal.Token.ExpiresAt != nil {
		tokenInfo.ExpiresAt = principal.Token.ExpiresAt.Format("2006-01-02T15:04:05Z07:00")
	}

	response := WhoAmIResponse{
		Token: tokenInfo,
	}

	return response
}
