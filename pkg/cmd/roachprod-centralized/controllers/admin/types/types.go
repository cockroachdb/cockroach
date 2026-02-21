// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/bindings/stripe"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const (
	// ControllerPath is the base path for admin endpoints.
	ControllerPath = "/v1/admin"
)

// TokenInfo represents token information for admin view.
type TokenInfo struct {
	ID               string  `json:"id"`
	TokenType        string  `json:"token_type"`
	UserID           *string `json:"user_id,omitempty"`
	ServiceAccountID *string `json:"service_account_id,omitempty"`
	Status           string  `json:"status"`
	ExpiresAt        string  `json:"expires_at"`
	CreatedAt        string  `json:"created_at"`
	LastUsedAt       *string `json:"last_used_at,omitempty"`
	TokenSuffix      string  `json:"token_suffix"`
}

// RevokeTokenResponse is the response for successful token revocation.
type RevokeTokenResponse struct {
	Message string `json:"message"`
}

// AdminResultError represents the standard error for admin responses.
type AdminResultError struct {
	Error error
}

// GetError returns the error from the AdminResultError.
func (dto *AdminResultError) GetError() error {
	return dto.Error
}

// GetData returns nil for error responses.
func (dto *AdminResultError) GetData() any {
	return nil
}

// GetAssociatedStatusCode returns the HTTP status code for admin errors.
func (dto *AdminResultError) GetAssociatedStatusCode() int {
	err := dto.GetError()
	switch {
	case err == nil:
		return http.StatusOK
	// Auth-specific error mappings
	case errors.Is(err, authtypes.ErrNotAuthenticated):
		return http.StatusUnauthorized
	case errors.Is(err, authtypes.ErrInvalidToken):
		return http.StatusNotFound
	default:
		// Fall back to generic error handling
		return controllers.GetGenericStatusCode(err)
	}
}

// AdminResult is the standard result type for admin controller responses.
type AdminResult struct {
	AdminResultError
	Data any `json:"data,omitempty"`
}

// GetData returns the data from the result.
func (dto *AdminResult) GetData() any {
	return dto.Data
}

// NewAdminResult creates a new AdminResult with data and error.
func NewAdminResult(data any, err error) *AdminResult {
	return &AdminResult{
		AdminResultError: AdminResultError{Error: err},
		Data:             data,
	}
}

// TokensListResult is the output data transfer object for listing tokens.
// It embeds PaginationMetadata to automatically implement IPaginatedResult.
type TokensListResult struct {
	AdminResultError
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
	result.AdminResultError.Error = err

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

// BuildTokenInfoList converts API tokens to TokenInfo DTOs for admin view.
func BuildTokenInfoList(tokens []*authmodels.ApiToken) []TokenInfo {
	if tokens == nil {
		return nil
	}

	result := make([]TokenInfo, 0, len(tokens))
	for _, t := range tokens {
		tokenInfo := TokenInfo{
			ID:          t.ID.String(),
			TokenType:   string(t.TokenType),
			Status:      string(t.Status),
			ExpiresAt:   t.ExpiresAt.Format("2006-01-02T15:04:05Z07:00"),
			CreatedAt:   t.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
			TokenSuffix: t.TokenSuffix,
		}

		if t.UserID != nil {
			userID := t.UserID.String()
			tokenInfo.UserID = &userID
		}

		if t.ServiceAccountID != nil {
			saID := t.ServiceAccountID.String()
			tokenInfo.ServiceAccountID = &saID
		}

		if t.LastUsedAt != nil {
			lastUsed := t.LastUsedAt.Format("2006-01-02T15:04:05Z07:00")
			tokenInfo.LastUsedAt = &lastUsed
		}

		result = append(result, tokenInfo)
	}
	return result
}

// BuildRevokeTokenResponse creates a success response for token revocation.
func BuildRevokeTokenResponse() RevokeTokenResponse {
	return RevokeTokenResponse{
		Message: "token revoked successfully",
	}
}

// InputListTokensDTO is input data transfer object that handles requests parameters
// for the service accounts list controller.
type InputListTokensDTO struct {
	TokenSuffix      stripe.FilterValue[string]                 `stripe:"suffix" validate:"omitempty,max=50,min=3"`
	TokenType        stripe.FilterValue[authmodels.TokenType]   `stripe:"type" validate:"omitempty,oneof=user service-account"`
	UserID           stripe.FilterValue[*uuid.UUID]             `stripe:"user_id" validate:"omitempty,uuid"`
	ServiceAccountID stripe.FilterValue[*uuid.UUID]             `stripe:"service_account_id" validate:"omitempty,uuid"`
	Status           stripe.FilterValue[authmodels.TokenStatus] `stripe:"status" validate:"omitempty,oneof=valid revoked disabled"`
	CreatedAt        stripe.FilterValue[time.Time]              `stripe:"created_at" validate:"omitempty"`
	ExpiresAt        stripe.FilterValue[time.Time]              `stripe:"expires_at" validate:"omitempty"`
}

// ToFilterSet converts the InputListTokensDTO to a filters.FilterSet for use by the service layer
func (dto *InputListTokensDTO) ToFilterSet() filtertypes.FilterSet {
	return stripe.ToFilterSet(*dto)
}

// ToServiceInputGetAllDTO converts the InputGetAllDTO data transfer object
// to a clusters' service InputGetAllDTO.
func (dto *InputListTokensDTO) ToServiceInputListTokensDTO() authtypes.InputListTokensDTO {
	return authtypes.InputListTokensDTO{
		Filters: dto.ToFilterSet(),
	}
}

// ==================== Group Permissions ====================

// GroupPermissionResponse is the response for group permission operations.
type GroupPermissionResponse struct {
	ID         string `json:"id"`
	GroupName  string `json:"group_name"`
	Scope      string `json:"scope"`
	Permission string `json:"permission"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
}

// GroupPermissionsListResponse is the response for listing group permissions.
type GroupPermissionsListResponse struct {
	Permissions []GroupPermissionResponse `json:"permissions"`
	Count       int                       `json:"count"`
}

// CreateGroupPermissionRequest is the request body for creating a group permission.
type CreateGroupPermissionRequest struct {
	GroupName  string `json:"group_name" binding:"required"`
	Scope      string `json:"scope" binding:"required"`
	Permission string `json:"permission" binding:"required"`
}

// UpdateGroupPermissionRequest is the request body for updating a group permission.
type UpdateGroupPermissionRequest struct {
	GroupName  string `json:"group_name"`
	Scope      string `json:"scope"`
	Permission string `json:"permission"`
}

// ReplaceGroupPermissionsRequest is the request body for replacing all group permissions.
type ReplaceGroupPermissionsRequest struct {
	Permissions []CreateGroupPermissionRequest `json:"permissions" binding:"required"`
}

// DeleteGroupPermissionResponse represents the response for successful permission deletion.
type DeleteGroupPermissionResponse struct {
	Message string `json:"message"`
}

// ReplaceGroupPermissionsResponse represents the response for successful permissions replacement.
type ReplaceGroupPermissionsResponse struct {
	Message string `json:"message"`
}

// BuildGroupPermissionResponse converts a GroupPermission model to a response DTO.
func BuildGroupPermissionResponse(permission *authmodels.GroupPermission) GroupPermissionResponse {
	return GroupPermissionResponse{
		ID:         permission.ID.String(),
		GroupName:  permission.GroupName,
		Scope:      permission.Scope,
		Permission: permission.Permission,
		CreatedAt:  permission.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:  permission.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}
}

// BuildGroupPermissionsListResponse converts a slice of GroupPermission models to a response DTO.
func BuildGroupPermissionsListResponse(
	permissions []*authmodels.GroupPermission,
) GroupPermissionsListResponse {
	responses := make([]GroupPermissionResponse, len(permissions))
	for i, perm := range permissions {
		responses[i] = BuildGroupPermissionResponse(perm)
	}
	return GroupPermissionsListResponse{
		Permissions: responses,
		Count:       len(responses),
	}
}

// BuildDeleteGroupPermissionResponse creates a success response for permission deletion.
func BuildDeleteGroupPermissionResponse() DeleteGroupPermissionResponse {
	return DeleteGroupPermissionResponse{
		Message: "group permission deleted successfully",
	}
}

// BuildReplaceGroupPermissionsResponse creates a success response for permissions replacement.
func BuildReplaceGroupPermissionsResponse() ReplaceGroupPermissionsResponse {
	return ReplaceGroupPermissionsResponse{
		Message: "group permissions replaced successfully",
	}
}

// InputListGroupPermissionsDTO is input data transfer object that handles request parameters
// for the group permissions list controller.
type InputListGroupPermissionsDTO struct {
	GroupName  stripe.FilterValue[string] `stripe:"group_name" validate:"omitempty,max=255"`
	Scope      stripe.FilterValue[string] `stripe:"scope" validate:"omitempty,max=255"`
	Permission stripe.FilterValue[string] `stripe:"permission" validate:"omitempty,max=100"`
}

// ToFilterSet converts the InputListGroupPermissionsDTO to a filters.FilterSet for use by the service layer.
func (dto *InputListGroupPermissionsDTO) ToFilterSet() filtertypes.FilterSet {
	return stripe.ToFilterSet(*dto)
}

// GroupPermissionsListResult is the output data transfer object for listing group permissions.
// It embeds PaginationMetadata to automatically implement IPaginatedResult.
type GroupPermissionsListResult struct {
	AdminResultError
	controllers.PaginationMetadata
	Data []GroupPermissionResponse `json:"data,omitempty"`
}

// GetData returns the data from the GroupPermissionsListResult.
func (dto *GroupPermissionsListResult) GetData() any {
	return dto.Data
}

// NewGroupPermissionsListResult creates a new GroupPermissionsListResult with pagination metadata.
func NewGroupPermissionsListResult(
	permissions []*authmodels.GroupPermission,
	totalCount int,
	pagination *filtertypes.PaginationParams,
	err error,
) *GroupPermissionsListResult {
	result := &GroupPermissionsListResult{}
	result.AdminResultError.Error = err

	if err != nil {
		return result
	}

	// Convert to response DTOs
	responses := make([]GroupPermissionResponse, len(permissions))
	for i, perm := range permissions {
		responses[i] = BuildGroupPermissionResponse(perm)
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

// ==================== User Management ====================

// UserResponse represents a user in admin API responses.
type UserResponse struct {
	ID          string  `json:"id"`
	OktaUserID  string  `json:"okta_user_id"`
	Email       string  `json:"email"`
	FullName    string  `json:"full_name,omitempty"`
	SlackHandle string  `json:"slack_handle,omitempty"`
	Active      bool    `json:"active"`
	CreatedAt   string  `json:"created_at"`
	UpdatedAt   string  `json:"updated_at"`
	LastLoginAt *string `json:"last_login_at,omitempty"`
}

// BuildUserResponse converts an auth.User model to a UserResponse DTO.
func BuildUserResponse(user *authmodels.User) UserResponse {
	resp := UserResponse{
		ID:          user.ID.String(),
		OktaUserID:  user.OktaUserID,
		Email:       user.Email,
		FullName:    user.FullName,
		SlackHandle: user.SlackHandle,
		Active:      user.Active,
		CreatedAt:   user.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:   user.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}
	if user.LastLoginAt != nil {
		lastLogin := user.LastLoginAt.Format("2006-01-02T15:04:05Z07:00")
		resp.LastLoginAt = &lastLogin
	}
	return resp
}

// CreateUserRequest is the request body for creating a user.
type CreateUserRequest struct {
	OktaUserID  string `json:"okta_user_id" binding:"required"`
	Email       string `json:"email" binding:"required,email"`
	FullName    string `json:"full_name"`
	SlackHandle string `json:"slack_handle,omitempty"`
	Active      *bool  `json:"active"` // Defaults to true if nil
}

// ReplaceUserRequest is the request body for replacing a user (PUT).
type ReplaceUserRequest struct {
	OktaUserID string `json:"okta_user_id" binding:"required"`
	Email      string `json:"email" binding:"required,email"`
	FullName   string `json:"full_name"`
	Active     bool   `json:"active"`
}

// PatchUserRequest is the request body for patching a user (PATCH).
type PatchUserRequest struct {
	OktaUserID  *string `json:"okta_user_id,omitempty"`
	Email       *string `json:"email,omitempty"`
	FullName    *string `json:"full_name,omitempty"`
	SlackHandle *string `json:"slack_handle,omitempty"`
	Active      *bool   `json:"active,omitempty"`
}

// InputListUsersDTO is input data transfer object for listing users with filters.
type InputListUsersDTO struct {
	Email      stripe.FilterValue[string] `stripe:"email" validate:"omitempty,max=255"`
	OktaUserID stripe.FilterValue[string] `stripe:"okta_user_id" validate:"omitempty,max=255"`
	Active     stripe.FilterValue[bool]   `stripe:"active" validate:"omitempty"`
	FullName   stripe.FilterValue[string] `stripe:"full_name" validate:"omitempty,max=255"`
}

// ToFilterSet converts the InputListUsersDTO to a filters.FilterSet for use by the service layer.
func (dto *InputListUsersDTO) ToFilterSet() filtertypes.FilterSet {
	return stripe.ToFilterSet(*dto)
}

// UsersListResult is the output data transfer object for listing users.
// It embeds PaginationMetadata to automatically implement IPaginatedResult.
type UsersListResult struct {
	AdminResultError
	controllers.PaginationMetadata
	Data []UserResponse `json:"data,omitempty"`
}

// GetData returns the data from the UsersListResult.
func (dto *UsersListResult) GetData() any {
	return dto.Data
}

// NewUsersListResult creates a new UsersListResult with pagination metadata.
func NewUsersListResult(
	users []*authmodels.User, totalCount int, pagination *filtertypes.PaginationParams, err error,
) *UsersListResult {
	result := &UsersListResult{}
	result.AdminResultError.Error = err

	if err != nil {
		return result
	}

	responses := make([]UserResponse, len(users))
	for i, user := range users {
		responses[i] = BuildUserResponse(user)
	}

	result.Data = responses
	result.TotalCount = totalCount
	result.Count = len(users)
	result.StartIndex = 1
	if pagination != nil {
		result.StartIndex = pagination.StartIndex
	}

	return result
}

// DeleteUserResponse represents the response for successful user deletion.
type DeleteUserResponse struct {
	Message string `json:"message"`
}

// BuildDeleteUserResponse creates a success response for user deletion.
func BuildDeleteUserResponse() DeleteUserResponse {
	return DeleteUserResponse{
		Message: "user deleted successfully",
	}
}

// ==================== Group Management ====================

// GroupResponse represents a group in admin API responses.
type GroupResponse struct {
	ID          string           `json:"id"`
	ExternalID  *string          `json:"external_id"`
	DisplayName string           `json:"display_name"`
	Members     []GroupMemberDTO `json:"members,omitempty"`
	CreatedAt   string           `json:"created_at"`
	UpdatedAt   string           `json:"updated_at"`
}

// GroupMemberDTO represents a group member in admin API responses.
type GroupMemberDTO struct {
	ID        string `json:"id"`
	UserID    string `json:"user_id"`
	CreatedAt string `json:"created_at"`
}

// BuildGroupResponse converts an auth.Group model and its members to a GroupResponse DTO.
func BuildGroupResponse(group *authmodels.Group, members []*authmodels.GroupMember) GroupResponse {
	memberDTOs := make([]GroupMemberDTO, len(members))
	for i, m := range members {
		memberDTOs[i] = GroupMemberDTO{
			ID:        m.ID.String(),
			UserID:    m.UserID.String(),
			CreatedAt: m.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		}
	}

	return GroupResponse{
		ID:          group.ID.String(),
		ExternalID:  group.ExternalID,
		DisplayName: group.DisplayName,
		Members:     memberDTOs,
		CreatedAt:   group.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:   group.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}
}

// BuildGroupResponseNoMembers converts an auth.Group model to a GroupResponse DTO without members.
func BuildGroupResponseNoMembers(group *authmodels.Group) GroupResponse {
	return GroupResponse{
		ID:          group.ID.String(),
		ExternalID:  group.ExternalID,
		DisplayName: group.DisplayName,
		Members:     nil,
		CreatedAt:   group.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:   group.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}
}

// CreateGroupRequest is the request body for creating a group.
type CreateGroupRequest struct {
	ExternalID  *string  `json:"external_id" binding:"required"`
	DisplayName string   `json:"display_name" binding:"required"`
	MemberIDs   []string `json:"member_ids,omitempty"`
}

// ReplaceGroupRequest is the request body for replacing a group (PUT).
type ReplaceGroupRequest struct {
	ExternalID  *string  `json:"external_id" binding:"required"`
	DisplayName string   `json:"display_name" binding:"required"`
	MemberIDs   []string `json:"member_ids"`
}

// PatchGroupRequest is the request body for patching a group (PATCH).
type PatchGroupRequest struct {
	ExternalID    *string  `json:"external_id,omitempty"`
	DisplayName   *string  `json:"display_name,omitempty"`
	AddMembers    []string `json:"add_members,omitempty"`
	RemoveMembers []string `json:"remove_members,omitempty"`
}

// InputListGroupsDTO is input data transfer object for listing groups with filters.
type InputListGroupsDTO struct {
	DisplayName stripe.FilterValue[string] `stripe:"display_name" validate:"omitempty,max=255"`
	ExternalID  stripe.FilterValue[string] `stripe:"external_id" validate:"omitempty,max=255"`
}

// ToFilterSet converts the InputListGroupsDTO to a filters.FilterSet for use by the service layer.
func (dto *InputListGroupsDTO) ToFilterSet() filtertypes.FilterSet {
	return stripe.ToFilterSet(*dto)
}

// GroupsListResult is the output data transfer object for listing groups.
// It embeds PaginationMetadata to automatically implement IPaginatedResult.
type GroupsListResult struct {
	AdminResultError
	controllers.PaginationMetadata
	Data []GroupResponse `json:"data,omitempty"`
}

// GetData returns the data from the GroupsListResult.
func (dto *GroupsListResult) GetData() any {
	return dto.Data
}

// NewGroupsListResult creates a new GroupsListResult with pagination metadata.
func NewGroupsListResult(
	groups []*authmodels.Group, totalCount int, pagination *filtertypes.PaginationParams, err error,
) *GroupsListResult {
	result := &GroupsListResult{}
	result.AdminResultError.Error = err

	if err != nil {
		return result
	}

	responses := make([]GroupResponse, len(groups))
	for i, group := range groups {
		responses[i] = BuildGroupResponseNoMembers(group)
	}

	result.Data = responses
	result.TotalCount = totalCount
	result.Count = len(groups)
	result.StartIndex = 1
	if pagination != nil {
		result.StartIndex = pagination.StartIndex
	}

	return result
}

// DeleteGroupResponse represents the response for successful group deletion.
type DeleteGroupResponse struct {
	Message string `json:"message"`
}

// BuildDeleteGroupResponse creates a success response for group deletion.
func BuildDeleteGroupResponse() DeleteGroupResponse {
	return DeleteGroupResponse{
		Message: "group deleted successfully",
	}
}
