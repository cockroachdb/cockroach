// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/bindings/stripe"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const (
	// ControllerPath is the base path for service account endpoints.
	ControllerPath = "/v1/service-accounts"
)

// Request/Response types

// InputListDTO is input data transfer object that handles requests parameters
// for the service accounts list controller.
type InputListDTO struct {
	Name stripe.FilterValue[string] `stripe:"name" validate:"omitempty,max=50,min=3"`
}

// ToFilterSet converts the InputGetAllDTO to a filters.FilterSet for use by the service layer
func (dto *InputListDTO) ToFilterSet() filtertypes.FilterSet {
	return stripe.ToFilterSet(*dto)
}

// ToServiceInputGetAllDTO converts the InputGetAllDTO data transfer object
// to a clusters' service InputGetAllDTO.
func (dto *InputListDTO) ToServiceInputGetAllDTO() authtypes.InputListServiceAccountsDTO {
	return authtypes.InputListServiceAccountsDTO{
		Filters: dto.ToFilterSet(),
	}
}

// CreateServiceAccountRequest is the request body for creating a service account.
type CreateServiceAccountRequest struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
	Orphan      bool   `json:"orphan"`
}

// UpdateServiceAccountRequest is the request body for updating a service account.
type UpdateServiceAccountRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Enabled     *bool  `json:"enabled"`
}

// ToServiceDTO converts the controller request to a service DTO.
// Only non-empty string fields are included in the update.
func (req *UpdateServiceAccountRequest) ToServiceDTO() authtypes.UpdateServiceAccountDTO {
	dto := authtypes.UpdateServiceAccountDTO{}
	if req.Name != "" {
		dto.Name = &req.Name
	}
	if req.Description != "" {
		dto.Description = &req.Description
	}
	if req.Enabled != nil {
		dto.Enabled = req.Enabled
	}
	return dto
}

// ServiceAccountResponse is the response for service account operations.
type ServiceAccountResponse struct {
	ID            string  `json:"id"`
	Name          string  `json:"name"`
	Description   string  `json:"description"`
	Enabled       bool    `json:"enabled"`
	DelegatedFrom *string `json:"delegated_from,omitempty"`
	CreatedAt     string  `json:"created_at"`
	UpdatedAt     string  `json:"updated_at"`
}

// DeleteResponse represents the response for successful deletion.
type DeleteResponse struct {
	Message string `json:"message"`
}

// MintTokenRequest is the request body for minting a new service account token.
type MintTokenRequest struct {
	TTLDays int `json:"ttl_days" binding:"omitempty,min=1,max=365"`
}

// MintTokenResponse is the response for minting a token.
type MintTokenResponse struct {
	Token     string `json:"token"`
	TokenID   string `json:"token_id"`
	ExpiresAt string `json:"expires_at"`
}

// RevokeTokenResponse represents the response for successful token revocation.
type RevokeTokenResponse struct {
	Message string `json:"message"`
}

// Origins request/response types

// AddOriginRequest is the request body for adding an IP origin restriction.
type AddOriginRequest struct {
	CIDR        string `json:"cidr" binding:"required"`
	Description string `json:"description"`
}

// InputListOriginsDTO is input data transfer object that handles request parameters
// for the service account origins list controller.
type InputListOriginsDTO struct {
	CIDR        stripe.FilterValue[string] `stripe:"cidr" validate:"omitempty,max=50"`
	Description stripe.FilterValue[string] `stripe:"description" validate:"omitempty,max=200"`
}

// ToFilterSet converts the InputListOriginsDTO to a filters.FilterSet for use by the service layer
func (dto *InputListOriginsDTO) ToFilterSet() filtertypes.FilterSet {
	return stripe.ToFilterSet(*dto)
}

// ToServiceInputListOriginsDTO converts the InputListOriginsDTO data transfer object
// to an auth service InputListServiceAccountOriginsDTO.
func (dto *InputListOriginsDTO) ToServiceInputListOriginsDTO() authtypes.InputListServiceAccountOriginsDTO {
	return authtypes.InputListServiceAccountOriginsDTO{
		Filters: dto.ToFilterSet(),
	}
}

// OriginResponse is the response for origin operations.
type OriginResponse struct {
	ID          string `json:"id"`
	CIDR        string `json:"cidr"`
	Description string `json:"description"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

// OriginsListResult is the output data transfer object for listing origins.
// It embeds PaginationMetadata to automatically implement IPaginatedResult.
type OriginsListResult struct {
	ServiceAccountsResultError
	controllers.PaginationMetadata
	Data []OriginResponse `json:"data,omitempty"`
}

// GetData returns the data from the OriginsListResult.
func (dto *OriginsListResult) GetData() any {
	return dto.Data
}

// NewOriginsListResult creates a new OriginsListResult with pagination metadata.
func NewOriginsListResult(
	origins []*auth.ServiceAccountOrigin,
	totalCount int,
	pagination *filtertypes.PaginationParams,
	err error,
) *OriginsListResult {
	result := &OriginsListResult{}
	result.ServiceAccountsResultError.Error = err

	if err != nil {
		return result
	}

	result.Data = BuildOriginsList(origins)
	result.TotalCount = totalCount
	result.Count = len(origins)
	result.StartIndex = 1
	if pagination != nil {
		result.StartIndex = pagination.StartIndex
	}

	return result
}

// RemoveOriginResponse represents the response for successful origin removal.
type RemoveOriginResponse struct {
	Message string `json:"message"`
}

// BuildOriginResponse converts an origin model to a response DTO.
func BuildOriginResponse(origin *auth.ServiceAccountOrigin) OriginResponse {
	return OriginResponse{
		ID:          origin.ID.String(),
		CIDR:        origin.CIDR,
		Description: origin.Description,
		CreatedAt:   origin.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:   origin.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}
}

// BuildOriginsList converts a slice of origin models to response DTOs.
func BuildOriginsList(origins []*auth.ServiceAccountOrigin) []OriginResponse {
	responses := make([]OriginResponse, len(origins))
	for i, origin := range origins {
		responses[i] = BuildOriginResponse(origin)
	}
	return responses
}

// BuildRemoveOriginResponse creates a success response for origin removal.
func BuildRemoveOriginResponse() RemoveOriginResponse {
	return RemoveOriginResponse{
		Message: "origin removed successfully",
	}
}

// Permissions request/response types

// InputListPermissionsDTO is the input data transfer object for listing permissions.
type InputListPermissionsDTO struct {
	Scope      stripe.FilterValue[string] `stripe:"scope" validate:"omitempty,max=255"`
	Permission stripe.FilterValue[string] `stripe:"permission" validate:"omitempty,max=100"`
}

// ToFilterSet converts the InputListPermissionsDTO to a filters.FilterSet for use by the service layer.
func (dto *InputListPermissionsDTO) ToFilterSet() filtertypes.FilterSet {
	return stripe.ToFilterSet(*dto)
}

// ToServiceInputListPermissionsDTO converts the controller DTO to a service DTO.
func (dto *InputListPermissionsDTO) ToServiceInputListPermissionsDTO() authtypes.InputListServiceAccountPermissionsDTO {
	return authtypes.InputListServiceAccountPermissionsDTO{
		Filters: dto.ToFilterSet(),
	}
}

// AddPermissionRequest is the request body for adding a single permission.
type AddPermissionRequest struct {
	Scope      string `json:"scope" binding:"required"`
	Permission string `json:"permission" binding:"required"`
}

// ReplacePermissionsRequest is the request body for replacing all permissions.
type ReplacePermissionsRequest struct {
	Permissions []AddPermissionRequest `json:"permissions" binding:"required"`
}

// PermissionResponse is the response for permission operations.
type PermissionResponse struct {
	ID         string `json:"id"`
	Scope      string `json:"scope"`
	Permission string `json:"permission"`
	CreatedAt  string `json:"created_at"`
}

// PermissionsListResponse is the response for listing permissions.
type PermissionsListResponse struct {
	Permissions []PermissionResponse `json:"permissions"`
	Count       int                  `json:"count"`
}

// PermissionsListResult is the output data transfer object for listing permissions with pagination.
type PermissionsListResult struct {
	ServiceAccountsResultError
	controllers.PaginationMetadata
	Data []PermissionResponse `json:"data,omitempty"`
}

// GetData returns the data from the PermissionsListResult.
func (dto *PermissionsListResult) GetData() any {
	return dto.Data
}

// NewPermissionsListResult creates a new PermissionsListResult with pagination metadata.
func NewPermissionsListResult(
	permissions []*auth.ServiceAccountPermission,
	totalCount int,
	pagination *filtertypes.PaginationParams,
	err error,
) *PermissionsListResult {
	result := &PermissionsListResult{}
	result.ServiceAccountsResultError.Error = err

	if err != nil {
		return result
	}

	// Build response DTOs
	responses := make([]PermissionResponse, len(permissions))
	for i, perm := range permissions {
		responses[i] = BuildPermissionResponse(perm)
	}

	result.Data = responses
	result.TotalCount = totalCount
	result.Count = len(permissions)
	result.StartIndex = 1
	if pagination != nil {
		result.StartIndex = pagination.StartIndex
	}

	return result
}

// RemovePermissionResponse represents the response for successful permission removal.
type RemovePermissionResponse struct {
	Message string `json:"message"`
}

// ReplacePermissionsResponse represents the response for successful permissions replacement.
type ReplacePermissionsResponse struct {
	Message string `json:"message"`
}

// BuildPermissionResponse converts a permission model to a response DTO.
func BuildPermissionResponse(perm *auth.ServiceAccountPermission) PermissionResponse {
	return PermissionResponse{
		ID:         perm.ID.String(),
		Scope:      perm.Scope,
		Permission: perm.Permission,
		CreatedAt:  perm.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}
}

// BuildPermissionsListResponse converts a slice of permission models to a response DTO.
func BuildPermissionsListResponse(perms []*auth.ServiceAccountPermission) PermissionsListResponse {
	responses := make([]PermissionResponse, len(perms))
	for i, perm := range perms {
		responses[i] = BuildPermissionResponse(perm)
	}
	return PermissionsListResponse{
		Permissions: responses,
		Count:       len(responses),
	}
}

// BuildRemovePermissionResponse creates a success response for permission removal.
func BuildRemovePermissionResponse() RemovePermissionResponse {
	return RemovePermissionResponse{
		Message: "permission removed successfully",
	}
}

// BuildReplacePermissionsResponse creates a success response for permissions replacement.
func BuildReplacePermissionsResponse() ReplacePermissionsResponse {
	return ReplacePermissionsResponse{
		Message: "permissions replaced successfully",
	}
}

// Error handling types

// ServiceAccountsResultError represents the standard error for service account responses.
type ServiceAccountsResultError struct {
	Error error
}

// GetError returns the error from the ServiceAccountsResultError.
func (dto *ServiceAccountsResultError) GetError() error {
	return dto.Error
}

// GetData returns nil for error responses.
func (dto *ServiceAccountsResultError) GetData() any {
	return nil
}

// GetAssociatedStatusCode returns the HTTP status code for service account errors.
func (dto *ServiceAccountsResultError) GetAssociatedStatusCode() int {
	err := dto.GetError()
	switch {
	case err == nil:
		return http.StatusOK
	// Service account-specific error mappings
	case errors.Is(err, authtypes.ErrServiceAccountNotFound):
		return http.StatusNotFound
	case errors.Is(err, authtypes.ErrServiceAccountDisabled):
		return http.StatusForbidden
	// Token-related errors
	case errors.Is(err, authtypes.ErrInvalidToken):
		return http.StatusUnauthorized
	case errors.Is(err, authtypes.ErrTokenExpired):
		return http.StatusUnauthorized
	case errors.Is(err, authtypes.ErrTokenNotFound):
		return http.StatusNotFound
	// Authentication errors
	case errors.Is(err, authtypes.ErrNotAuthenticated):
		return http.StatusUnauthorized
	case errors.Is(err, authtypes.ErrUserNotProvisioned):
		return http.StatusForbidden
	case errors.Is(err, authtypes.ErrUserDeactivated):
		return http.StatusForbidden
	// Business rule violations
	case errors.Is(err, authtypes.ErrSACreationNotAllowedFromOrphanSA):
		return http.StatusForbidden
	case errors.Is(err, authtypes.ErrNonOrphanSAPermissionModification):
		return http.StatusForbidden
	default:
		// Fall back to generic error handling
		return controllers.GetGenericStatusCode(err)
	}
}

// ServiceAccountsResult is the standard result type for service account controller responses.
type ServiceAccountsResult struct {
	ServiceAccountsResultError
	Data any `json:"data,omitempty"`
}

// GetData returns the data from the result.
func (dto *ServiceAccountsResult) GetData() any {
	return dto.Data
}

// NewServiceAccountsResult creates a new ServiceAccountsResult with data and error.
func NewServiceAccountsResult(data any, err error) *ServiceAccountsResult {
	return &ServiceAccountsResult{
		ServiceAccountsResultError: ServiceAccountsResultError{Error: err},
		Data:                       data,
	}
}

// ServiceAccountsListResult is the output data transfer object for listing service accounts.
// It embeds PaginationMetadata to automatically implement IPaginatedResult.
type ServiceAccountsListResult struct {
	ServiceAccountsResultError
	controllers.PaginationMetadata
	Data []ServiceAccountResponse `json:"data,omitempty"`
}

// GetData returns the data from the ServiceAccountsListResult.
func (dto *ServiceAccountsListResult) GetData() any {
	return dto.Data
}

// NewServiceAccountsListResult creates a new ServiceAccountsListResult with pagination metadata.
func NewServiceAccountsListResult(
	accounts []*auth.ServiceAccount,
	totalCount int,
	pagination *filtertypes.PaginationParams,
	err error,
) *ServiceAccountsListResult {
	result := &ServiceAccountsListResult{}
	result.ServiceAccountsResultError.Error = err

	if err != nil {
		return result
	}

	result.Data = BuildServiceAccountList(accounts)
	result.TotalCount = totalCount
	result.Count = len(accounts)
	result.StartIndex = 1
	if pagination != nil {
		result.StartIndex = pagination.StartIndex
	}

	return result
}

// Builder functions for converting service models to response DTOs

// BuildServiceAccountResponse converts a service account model to a response DTO.
func BuildServiceAccountResponse(sa *auth.ServiceAccount) ServiceAccountResponse {
	resp := ServiceAccountResponse{
		ID:          sa.ID.String(),
		Name:        sa.Name,
		Description: sa.Description,
		Enabled:     sa.Enabled,
		CreatedAt:   sa.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:   sa.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}
	if sa.DelegatedFrom != nil {
		delegatedFromStr := sa.DelegatedFrom.String()
		resp.DelegatedFrom = &delegatedFromStr
	}
	return resp
}

// BuildServiceAccountList converts a slice of service account models to response DTOs.
func BuildServiceAccountList(accounts []*auth.ServiceAccount) []ServiceAccountResponse {
	responses := make([]ServiceAccountResponse, len(accounts))
	for i, sa := range accounts {
		responses[i] = BuildServiceAccountResponse(sa)
	}
	return responses
}

// BuildDeleteResponse creates a success response for deletion.
func BuildDeleteResponse() DeleteResponse {
	return DeleteResponse{
		Message: "service account deleted successfully",
	}
}

// BuildMintTokenResponse creates a response for token minting.
func BuildMintTokenResponse(
	tokenID uuid.UUID, tokenString string, expiresAt *time.Time,
) MintTokenResponse {
	return MintTokenResponse{
		Token:     tokenString,
		TokenID:   tokenID.String(),
		ExpiresAt: expiresAt.Format("2006-01-02T15:04:05Z07:00"),
	}
}

// BuildRevokeTokenResponse creates a success response for token revocation.
func BuildRevokeTokenResponse() RevokeTokenResponse {
	return RevokeTokenResponse{
		Message: "token revoked successfully",
	}
}

// TokenListResponse is the response for token listing operations within service accounts.
type TokenListResponse struct {
	ID          string  `json:"id"`
	TokenSuffix string  `json:"token_suffix"`
	TokenType   string  `json:"token_type"`
	Status      string  `json:"status"`
	CreatedAt   string  `json:"created_at"`
	ExpiresAt   string  `json:"expires_at"`
	LastUsedAt  *string `json:"last_used_at,omitempty"`
}

// TokensListResult is the output data transfer object for listing service account tokens.
type TokensListResult struct {
	ServiceAccountsResultError
	controllers.PaginationMetadata
	Data []TokenListResponse `json:"data,omitempty"`
}

// GetData returns the data from the TokensListResult.
func (dto *TokensListResult) GetData() any {
	return dto.Data
}

// NewTokensListResult creates a new TokensListResult with pagination metadata.
func NewTokensListResult(
	tokens []*auth.ApiToken, totalCount int, pagination *filtertypes.PaginationParams, err error,
) *TokensListResult {
	result := &TokensListResult{}
	result.ServiceAccountsResultError.Error = err

	if err != nil {
		return result
	}

	result.Data = BuildTokenList(tokens)
	result.TotalCount = totalCount
	result.Count = len(tokens)
	result.StartIndex = 1
	if pagination != nil {
		result.StartIndex = pagination.StartIndex
	}

	return result
}

// BuildTokenListResponse converts a token model to a response DTO.
func BuildTokenListResponse(token *auth.ApiToken) TokenListResponse {
	resp := TokenListResponse{
		ID:          token.ID.String(),
		TokenSuffix: token.TokenSuffix,
		TokenType:   string(token.TokenType),
		Status:      string(token.Status),
		CreatedAt:   token.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		ExpiresAt:   token.ExpiresAt.Format("2006-01-02T15:04:05Z07:00"),
	}
	if token.LastUsedAt != nil {
		lastUsed := token.LastUsedAt.Format("2006-01-02T15:04:05Z07:00")
		resp.LastUsedAt = &lastUsed
	}
	return resp
}

// BuildTokenList converts a slice of token models to response DTOs.
func BuildTokenList(tokens []*auth.ApiToken) []TokenListResponse {
	responses := make([]TokenListResponse, len(tokens))
	for i, token := range tokens {
		responses[i] = BuildTokenListResponse(token)
	}
	return responses
}
