// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/bindings/stripe"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/errors"
)

const (
	// ControllerPath is the base path for authentication endpoints.
	ControllerPath = "/v1/auth"
)

// Request/Response types

// ExchangeOktaTokenRequest is the request body for exchanging an Okta token.
type ExchangeOktaTokenRequest struct {
	OktaIDToken string `json:"okta_id_token" binding:"required"`
}

// ExchangeOktaTokenResponse is the response for successful token exchange.
type ExchangeOktaTokenResponse struct {
	Token     string `json:"token"`
	ExpiresAt string `json:"expires_at"`
}

// TokenInfo contains metadata about the authentication token.
type TokenInfo struct {
	ID         string `json:"id"`
	Token      string `json:"token"`
	Type       string `json:"type"`
	Status     string `json:"status,omitempty"`       // e.g., "valid", "expired", "revoked"
	CreatedAt  string `json:"created_at,omitempty"`   // ISO 8601 format, from iat/exp claim for JWT
	ExpiresAt  string `json:"expires_at,omitempty"`   // ISO 8601 format, from iat/exp claim for JWT
	LastUsedAt string `json:"last_used_at,omitempty"` // ISO 8601 format, not populated for JWT
}

// WhoAmIResponse contains information about the current authenticated principal.
type WhoAmIResponse struct {
	User           *UserInfo           `json:"user,omitempty"`
	ServiceAccount *ServiceAccountInfo `json:"service_account,omitempty"`
	Permissions    []PermissionInfo    `json:"permissions"`
	Token          TokenInfo           `json:"token"`
}

// UserInfo contains user details.
type UserInfo struct {
	ID     string `json:"id"`
	Email  string `json:"email"`
	Name   string `json:"name"`
	Active bool   `json:"active"`
}

// ServiceAccountInfo contains service account details.
type ServiceAccountInfo struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Enabled     bool   `json:"enabled"`
}

// PermissionInfo contains permission details.
type PermissionInfo struct {
	Scope      string `json:"scope"`
	Permission string `json:"permission"`
}

// RevokeTokenResponse is the response for successful token revocation.
type RevokeTokenResponse struct {
	Message string `json:"message"`
}

// InputListTokensDTO is input data transfer object that handles requests parameters
// for the auth tokens list controller.
// Note: user_id and service_account_id are intentionally omitted from this DTO.
// These fields are set implicitly based on the authenticated principal and should
// not be controllable by users to prevent privilege escalation.
type InputListTokensDTO struct {
	TokenSuffix stripe.FilterValue[string]                 `stripe:"suffix" validate:"omitempty,max=50,min=3"`
	TokenType   stripe.FilterValue[authmodels.TokenType]   `stripe:"type" validate:"omitempty,oneof=user service-account"`
	Status      stripe.FilterValue[authmodels.TokenStatus] `stripe:"status" validate:"omitempty,oneof=valid revoked disabled"`
	CreatedAt   stripe.FilterValue[time.Time]              `stripe:"created_at" validate:"omitempty"`
	ExpiresAt   stripe.FilterValue[time.Time]              `stripe:"expires_at" validate:"omitempty"`
}

// ToFilterSet converts the InputListTokensDTO to a filters.FilterSet for use by the service layer
func (dto *InputListTokensDTO) ToFilterSet() filtertypes.FilterSet {
	return stripe.ToFilterSet(*dto)
}

// ToServiceInputListTokensDTO converts the InputListTokensDTO data transfer object
// to the auth service InputListTokensDTO.
func (dto *InputListTokensDTO) ToServiceInputListTokensDTO() authtypes.InputListTokensDTO {
	return authtypes.InputListTokensDTO{
		Filters: dto.ToFilterSet(),
	}
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
	case errors.Is(err, authtypes.ErrNotAuthenticated),
		errors.Is(err, authtypes.ErrInvalidToken),
		errors.Is(err, authtypes.ErrTokenExpired),
		errors.Is(err, authtypes.ErrInvalidTokenType):
		return http.StatusUnauthorized
	case errors.Is(err, authtypes.ErrUserNotProvisioned),
		errors.Is(err, authtypes.ErrUserDeactivated):
		return http.StatusForbidden
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

// TokensListResult is the output data transfer object for listing tokens.
// It embeds PaginationMetadata to automatically implement IPaginatedResult.
type TokensListResult struct {
	AuthResultError
	controllers.PaginationMetadata
	Data []TokenInfo `json:"data,omitempty"`
}

// GetData returns the data from the TokensListResult.
func (dto *TokensListResult) GetData() any {
	return dto.Data
}

// NewTokensListResult creates a new TokensListResult with pagination metadata.
func NewTokensListResult(
	tokens []*authmodels.ApiToken,
	totalCount int,
	pagination *filtertypes.PaginationParams,
	err error,
) *TokensListResult {
	result := &TokensListResult{}
	result.AuthResultError.Error = err

	if err != nil {
		return result
	}

	result.Data = BuildTokenInfoList(tokens)
	result.TotalCount = totalCount
	result.Count = len(tokens)
	result.StartIndex = 1
	if pagination != nil {
		result.StartIndex = pagination.StartIndex
	}

	return result
}

// ==================== Builder Functions ====================
// These functions convert service models to DTOs without the weird
// empty struct creation pattern.

// BuildTokenInfoList converts API tokens to TokenInfo DTOs.
func BuildTokenInfoList(tokens []*authmodels.ApiToken) []TokenInfo {
	if tokens == nil {
		return nil
	}

	result := make([]TokenInfo, 0, len(tokens))
	for _, token := range tokens {
		tokenInfo := TokenInfo{
			ID:     token.ID.String(),
			Type:   string(token.TokenType),
			Token:  token.TokenSuffix,
			Status: string(token.Status),
		}
		tokenInfo.CreatedAt = token.CreatedAt.Format("2006-01-02T15:04:05Z07:00")
		tokenInfo.ExpiresAt = token.ExpiresAt.Format("2006-01-02T15:04:05Z07:00")
		if token.LastUsedAt != nil {
			tokenInfo.LastUsedAt = token.LastUsedAt.Format("2006-01-02T15:04:05Z07:00")
		}
		result = append(result, tokenInfo)
	}
	return result
}

// BuildExchangeOktaTokenResponse converts token and token string to response DTO.
func BuildExchangeOktaTokenResponse(
	token *authmodels.ApiToken, tokenString string,
) ExchangeOktaTokenResponse {
	return ExchangeOktaTokenResponse{
		Token:     tokenString,
		ExpiresAt: token.ExpiresAt.Format("2006-01-02T15:04:05Z07:00"),
	}
}

// BuildRevokeTokenResponse creates a success response for token revocation.
func BuildRevokeTokenResponse() RevokeTokenResponse {
	return RevokeTokenResponse{
		Message: "token revoked successfully",
	}
}

// BuildWhoAmIResponse converts a principal to a WhoAmI response DTO.
func BuildWhoAmIResponse(principal *auth.Principal) WhoAmIResponse {
	// Build token info
	tokenInfo := TokenInfo{
		ID:   principal.Token.ID.String(),
		Type: string(principal.Token.Type),
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

	// Populate user info if this is a user token
	if principal.User != nil {
		response.User = &UserInfo{
			ID:     principal.User.ID.String(),
			Email:  principal.User.Email,
			Name:   principal.User.FullName,
			Active: principal.User.Active,
		}
	}
	if principal.ServiceAccount != nil {
		response.ServiceAccount = &ServiceAccountInfo{
			ID:          principal.ServiceAccount.ID.String(),
			Name:        principal.ServiceAccount.Name,
			Description: principal.ServiceAccount.Description,
			Enabled:     principal.ServiceAccount.Enabled,
		}
	}

	// Map permissions
	response.Permissions = make([]PermissionInfo, 0, len(principal.Permissions))
	for _, perm := range principal.Permissions {
		response.Permissions = append(response.Permissions, PermissionInfo{
			Scope:      perm.GetScope(),
			Permission: perm.GetPermission(),
		})
	}

	return response
}
