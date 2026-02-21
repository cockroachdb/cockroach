// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"context"
	"time"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	TaskServiceName = "auth"
)

const (
	// SCIM and admin User Management Permissions
	PermissionScimManageUser = TaskServiceName + ":scim:manage-user"

	// Service Account Permissions
	PermissionServiceAccountCreate    = TaskServiceName + ":service-accounts:create"
	PermissionServiceAccountViewAll   = TaskServiceName + ":service-accounts:view:all"
	PermissionServiceAccountViewOwn   = TaskServiceName + ":service-accounts:view:own"
	PermissionServiceAccountUpdateAll = TaskServiceName + ":service-accounts:update:all"
	PermissionServiceAccountUpdateOwn = TaskServiceName + ":service-accounts:update:own"
	PermissionServiceAccountDeleteAll = TaskServiceName + ":service-accounts:delete:all"
	PermissionServiceAccountDeleteOwn = TaskServiceName + ":service-accounts:delete:own"

	// Service Account Token Minting Permissions
	PermissionServiceAccountMintAll = TaskServiceName + ":service-accounts:mint:all"
	PermissionServiceAccountMintOwn = TaskServiceName + ":service-accounts:mint:own"

	// Token Permissions
	PermissionTokensViewAll   = TaskServiceName + ":tokens:view:all"
	PermissionTokensViewOwn   = TaskServiceName + ":tokens:view:own"
	PermissionTokensRevokeOwn = TaskServiceName + ":tokens:revoke:own"
	PermissionTokensRevokeAll = TaskServiceName + ":tokens:revoke:all"
)

// IService defines the interface for the authentication service.
// It covers auth business logic consumed by controllers and the bearer
// authenticator. Lifecycle methods (RegisterTasks, StartService, etc.)
// live in the base services.IService interface; metrics recording lives
// in IAuthMetricsRecorder.
type IService interface {
	// --- Authentication ---

	AuthenticateToken(ctx context.Context, l *logger.Logger, tokenString string, clientIP string) (*pkgauth.Principal, error)

	// --- Okta Token Exchange ---

	ExchangeOktaToken(context.Context, *logger.Logger, string) (*auth.ApiToken, string, error)

	// --- Token Self-Service ---

	ListSelfTokens(context.Context, *logger.Logger, *pkgauth.Principal, InputListTokensDTO) ([]*auth.ApiToken, int, error)
	RevokeSelfToken(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID) error

	// --- Token Administration ---

	ListAllTokens(context.Context, *logger.Logger, *pkgauth.Principal, InputListTokensDTO) ([]*auth.ApiToken, int, error)
	RevokeUserToken(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID, uuid.UUID) error

	// --- User Management (SCIM + admin) ---

	ListUsers(context.Context, *logger.Logger, *pkgauth.Principal, InputListUsersDTO) ([]*auth.User, int, error)
	GetUser(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID) (*auth.User, error)
	CreateUser(context.Context, *logger.Logger, *pkgauth.Principal, *auth.User) error
	ReplaceUser(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID, ReplaceUserInput) (*auth.User, error)
	PatchUser(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID, PatchUserInput) (*auth.User, error)
	DeleteUser(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID) error

	// --- Service Account Management ---

	ListServiceAccounts(context.Context, *logger.Logger, *pkgauth.Principal, InputListServiceAccountsDTO) ([]*auth.ServiceAccount, int, error)
	GetServiceAccount(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID) (*auth.ServiceAccount, error)
	CreateServiceAccount(context.Context, *logger.Logger, *pkgauth.Principal, *auth.ServiceAccount, bool) error
	UpdateServiceAccount(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID, UpdateServiceAccountDTO) (*auth.ServiceAccount, error)
	DeleteServiceAccount(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID) error
	ListServiceAccountPermissions(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID, InputListServiceAccountPermissionsDTO) ([]*auth.ServiceAccountPermission, int, error)
	UpdateServiceAccountPermissions(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID, []*auth.ServiceAccountPermission) error
	AddServiceAccountPermission(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID, *auth.ServiceAccountPermission) error
	RemoveServiceAccountPermission(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID, uuid.UUID) error
	AddServiceAccountOrigin(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID, *auth.ServiceAccountOrigin) error
	ListServiceAccountOrigins(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID, InputListServiceAccountOriginsDTO) ([]*auth.ServiceAccountOrigin, int, error)
	RemoveServiceAccountOrigin(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID, uuid.UUID) error
	ListServiceAccountTokens(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID, InputListServiceAccountTokensDTO) ([]*auth.ApiToken, int, error)
	MintServiceAccountToken(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID, time.Duration) (*auth.ApiToken, string, error)
	RevokeServiceAccountToken(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID, uuid.UUID) error

	// --- Group Permissions (Admin) ---

	ListGroupPermissions(context.Context, *logger.Logger, *pkgauth.Principal, InputListGroupPermissionsDTO) ([]*auth.GroupPermission, int, error)
	CreateGroupPermission(context.Context, *logger.Logger, *pkgauth.Principal, *auth.GroupPermission) error
	UpdateGroupPermission(context.Context, *logger.Logger, *pkgauth.Principal, *auth.GroupPermission) error
	DeleteGroupPermission(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID) error
	ReplaceGroupPermissions(context.Context, *logger.Logger, *pkgauth.Principal, []*auth.GroupPermission) error

	// --- Groups (SCIM + admin) ---

	ListGroups(context.Context, *logger.Logger, *pkgauth.Principal, InputListGroupsDTO) ([]*auth.Group, int, error)
	GetGroup(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID) (*auth.Group, error)
	GetGroupByExternalID(context.Context, *logger.Logger, *pkgauth.Principal, string) (*auth.Group, error)
	GetGroupWithMembers(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID) (*auth.Group, []*auth.GroupMember, error)
	CreateGroupWithMembers(context.Context, *logger.Logger, *pkgauth.Principal, CreateGroupInput) (*auth.Group, []*auth.GroupMember, error)
	ReplaceGroup(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID, ReplaceGroupInput) (*auth.Group, []*auth.GroupMember, error)
	PatchGroup(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID, PatchGroupInput) (*PatchGroupOutput, error)
	DeleteGroup(context.Context, *logger.Logger, *pkgauth.Principal, uuid.UUID) error

	// --- Token Cleanup (internal, used by background tasks) ---

	CleanupRevokedAndExpiredTokens(ctx context.Context, l *logger.Logger, retention time.Duration) (int, error)
}

// IAuthMetricsRecorder records authentication and authorization metrics.
// Implemented by the auth Service. Consumed by the bearer authenticator
// separately from IService to keep metrics concerns out of the business
// logic interface.
type IAuthMetricsRecorder interface {
	RecordAuthentication(result, authMethod string, latency time.Duration)
	RecordAuthzDecision(result, reason, endpoint, provider string)
	RecordAuthzLatency(endpoint string, latency time.Duration)
}

// Options configures the auth service.
type Options struct {
	CleanupInterval          time.Duration // Default: 24h
	ExpiredTokensRetention   time.Duration // Default: 24h
	CollectMetrics           bool          // Enable Prometheus metrics collection
	StatisticsUpdateInterval time.Duration // How often to update gauge metrics (default: 30s)
	BootstrapSCIMToken       string        // If set and no service accounts exist, creates a bootstrap SCIM service account with this token
	TokenLastUsedBufferSize  int           // Buffer size for async token last-used updates (default: 256)
}

// InputListServiceAccountsDTO is the data transfer object to get all service accounts.
type InputListServiceAccountsDTO struct {
	Filters filtertypes.FilterSet `json:"filters,omitempty"`
}

// NewInputListServiceAccountsDTO creates a new InputListServiceAccountsDTO with proper defaults.
func NewInputListServiceAccountsDTO() InputListServiceAccountsDTO {
	return InputListServiceAccountsDTO{Filters: *filters.NewFilterSet()}
}

// InputListTokensDTO is the data transfer object to get all tokens.
type InputListTokensDTO struct {
	Filters filtertypes.FilterSet `json:"filters,omitempty"`
}

// NewInputListTokensDTO creates a new InputListTokensDTO with proper defaults.
func NewInputListTokensDTO() InputListTokensDTO {
	return InputListTokensDTO{Filters: *filters.NewFilterSet()}
}

type InputListUsersDTO struct {
	Filters filtertypes.FilterSet `json:"filters,omitempty"`
}

// NewInputListUsersDTO creates a new InputListUsersDTO with proper defaults.
func NewInputListUsersDTO() InputListUsersDTO {
	return InputListUsersDTO{Filters: *filters.NewFilterSet()}
}

// UpdateServiceAccountDTO represents the fields that can be updated on a service account.
type UpdateServiceAccountDTO struct {
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
	Enabled     *bool   `json:"enabled,omitempty"`
}

// InputListGroupsDTO is the data transfer object to list groups.
type InputListGroupsDTO struct {
	Filters filtertypes.FilterSet `json:"filters,omitempty"`
}

// NewInputListGroupsDTO creates a new InputListGroupsDTO with proper defaults.
func NewInputListGroupsDTO() InputListGroupsDTO {
	return InputListGroupsDTO{Filters: *filters.NewFilterSet()}
}

// GroupMemberOperation represents a single add/remove operation for group membership.
type GroupMemberOperation struct {
	Op     string    `json:"op"`      // "add" or "remove"
	UserID uuid.UUID `json:"user_id"` // The user to add or remove
}

// CreateGroupInput is the input for creating a new group with optional initial members.
type CreateGroupInput struct {
	ExternalID  *string     `json:"external_id"`
	DisplayName string      `json:"display_name"`
	Members     []uuid.UUID `json:"members,omitempty"` // Initial member user IDs
}

// GroupPatchOperation represents a single SCIM patch operation for a group.
type GroupPatchOperation struct {
	Op      string                 `json:"op"`                // "add", "remove", or "replace"
	Path    string                 `json:"path"`              // "displayName", "externalId", "members", etc.
	Value   any                    `json:"value,omitempty"`   // The value to set/add/remove (for add, replace operations)
	Filters *filtertypes.FilterSet `json:"filters,omitempty"` // Filter to identify members for remove operations
}

// PatchGroupInput is the input for patching a group (SCIM or admin).
// All operations are executed in a single transaction.
type PatchGroupInput struct {
	Operations []GroupPatchOperation `json:"operations"`
}

// PatchGroupOutput is the output from patching a group.
type PatchGroupOutput struct {
	Group   *auth.Group         `json:"group"`
	Members []*auth.GroupMember `json:"members"`
}

// ReplaceGroupInput is the input for replacing a group (PUT).
type ReplaceGroupInput struct {
	ExternalID  *string     `json:"external_id"`
	DisplayName string      `json:"display_name"`
	Members     []uuid.UUID `json:"members"` // Complete list of members
}

// User DTOs

// ReplaceUserInput is the input for replacing a user (SCIM PUT).
type ReplaceUserInput struct {
	ExternalID string `json:"external_id"` // Maps to OktaUserID
	UserName   string `json:"user_name"`   // Maps to Email
	FullName   string `json:"full_name"`
	Active     bool   `json:"active"`
}

// UserPatchOperation represents a single SCIM patch operation for a user.
type UserPatchOperation struct {
	Op    string `json:"op"`              // "add", "remove", or "replace"
	Path  string `json:"path"`            // "active", "userName", "name.formatted", etc.
	Value any    `json:"value,omitempty"` // The value to set/add/remove
}

// PatchUserInput is the input for patching a user (SCIM PATCH).
type PatchUserInput struct {
	Operations []UserPatchOperation `json:"operations"`
}

// InputListGroupPermissionsDTO is the data transfer object to list group permissions.
type InputListGroupPermissionsDTO struct {
	Filters filtertypes.FilterSet `json:"filters,omitempty"`
}

// NewInputListGroupPermissionsDTO creates a new InputListGroupPermissionsDTO with proper defaults.
func NewInputListGroupPermissionsDTO() InputListGroupPermissionsDTO {
	return InputListGroupPermissionsDTO{Filters: *filters.NewFilterSet()}
}

// InputListServiceAccountOriginsDTO is the data transfer object to list service account origins.
type InputListServiceAccountOriginsDTO struct {
	Filters filtertypes.FilterSet `json:"filters,omitempty"`
}

// NewInputListServiceAccountOriginsDTO creates a new InputListServiceAccountOriginsDTO with proper defaults.
func NewInputListServiceAccountOriginsDTO() InputListServiceAccountOriginsDTO {
	return InputListServiceAccountOriginsDTO{Filters: *filters.NewFilterSet()}
}

// InputListServiceAccountPermissionsDTO is the data transfer object to list service account permissions.
type InputListServiceAccountPermissionsDTO struct {
	Filters filtertypes.FilterSet `json:"filters,omitempty"`
}

// NewInputListServiceAccountPermissionsDTO creates a new InputListServiceAccountPermissionsDTO with proper defaults.
func NewInputListServiceAccountPermissionsDTO() InputListServiceAccountPermissionsDTO {
	return InputListServiceAccountPermissionsDTO{Filters: *filters.NewFilterSet()}
}

// InputListServiceAccountTokensDTO is the data transfer object to list service account tokens.
type InputListServiceAccountTokensDTO struct {
	Filters filtertypes.FilterSet `json:"filters,omitempty"`
}

// NewInputListServiceAccountTokensDTO creates a new InputListServiceAccountTokensDTO with proper defaults.
func NewInputListServiceAccountTokensDTO() InputListServiceAccountTokensDTO {
	return InputListServiceAccountTokensDTO{Filters: *filters.NewFilterSet()}
}
