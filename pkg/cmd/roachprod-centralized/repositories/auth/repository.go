// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// ErrNotFound is returned when a requested resource is not found.
var ErrNotFound = errors.New("not found")

// GroupMemberOperation represents an add or remove operation for group membership.
// Used by UpdateGroupWithMembers for atomic SCIM PATCH operations.
type GroupMemberOperation struct {
	Op     string    // "add" or "remove"
	UserID uuid.UUID // The user to add or remove
}

// Statistics returns counts of auth entities for metrics.
type Statistics struct {
	UsersActive             int
	UsersInactive           int
	Groups                  int
	ServiceAccountsEnabled  int
	ServiceAccountsDisabled int
	TokensByTypeAndStatus   map[string]map[string]int // type -> status -> count
}

// IAuthRepository defines the interface for authentication and authorization data persistence.
type IAuthRepository interface {
	// Users
	CreateUser(context.Context, *logger.Logger, *auth.User) error
	GetUser(context.Context, *logger.Logger, uuid.UUID) (*auth.User, error)
	GetUserByOktaID(context.Context, *logger.Logger, string) (*auth.User, error)
	// GetUserByEmail retrieves a user by email address.
	// Returns ErrNotFound if no user with this email exists.
	GetUserByEmail(context.Context, *logger.Logger, string) (*auth.User, error)
	UpdateUser(context.Context, *logger.Logger, *auth.User) error
	ListUsers(context.Context, *logger.Logger, filtertypes.FilterSet) ([]*auth.User, int, error)
	// DeleteUser performs a hard delete (permanent removal) of a user.
	// This is used for SCIM DELETE /scim/v2/Users/:id requests.
	// For soft deletes (deactivation), use the service's DeactivateUser method instead.
	DeleteUser(context.Context, *logger.Logger, uuid.UUID) error
	// DeactivateUser atomically sets user.active=false and revokes all their tokens.
	// This is used for SCIM PATCH /scim/v2/Users/:id with {"active": false}.
	DeactivateUser(context.Context, *logger.Logger, uuid.UUID) error

	// Service Accounts
	CreateServiceAccount(context.Context, *logger.Logger, *auth.ServiceAccount) error
	GetServiceAccount(context.Context, *logger.Logger, uuid.UUID) (*auth.ServiceAccount, error)
	UpdateServiceAccount(context.Context, *logger.Logger, *auth.ServiceAccount) error
	ListServiceAccounts(context.Context, *logger.Logger, filtertypes.FilterSet) ([]*auth.ServiceAccount, int, error)
	DeleteServiceAccount(context.Context, *logger.Logger, uuid.UUID) error

	// Service Account Origins
	AddServiceAccountOrigin(context.Context, *logger.Logger, *auth.ServiceAccountOrigin) error
	RemoveServiceAccountOrigin(context.Context, *logger.Logger, uuid.UUID) error
	ListServiceAccountOrigins(context.Context, *logger.Logger, uuid.UUID, filtertypes.FilterSet) ([]*auth.ServiceAccountOrigin, int, error)

	// Service Account Permissions
	ListServiceAccountPermissions(context.Context, *logger.Logger, uuid.UUID, filtertypes.FilterSet) ([]*auth.ServiceAccountPermission, int, error)
	UpdateServiceAccountPermissions(context.Context, *logger.Logger, uuid.UUID, []*auth.ServiceAccountPermission) error
	AddServiceAccountPermission(context.Context, *logger.Logger, *auth.ServiceAccountPermission) error
	GetServiceAccountPermission(context.Context, *logger.Logger, uuid.UUID) (*auth.ServiceAccountPermission, error)
	RemoveServiceAccountPermission(context.Context, *logger.Logger, uuid.UUID) error

	// Tokens
	ListAllTokens(context.Context, *logger.Logger, filtertypes.FilterSet) ([]*auth.ApiToken, int, error)
	GetToken(context.Context, *logger.Logger, uuid.UUID) (*auth.ApiToken, error)
	CreateToken(context.Context, *logger.Logger, *auth.ApiToken) error
	GetTokenByHash(context.Context, *logger.Logger, string) (*auth.ApiToken, error)
	UpdateTokenLastUsed(context.Context, *logger.Logger, uuid.UUID) error
	RevokeToken(context.Context, *logger.Logger, uuid.UUID) error

	// CleanupTokens deletes tokens that have been in the specified status beyond retention.
	// For TokenStatusValid (expired): deletes where ExpiresAt < now - retention
	// For TokenStatusRevoked: deletes where UpdatedAt < now - retention
	// Returns the number of tokens deleted.
	CleanupTokens(context.Context, *logger.Logger, auth.TokenStatus, time.Duration) (int, error)

	// Groups (SCIM-managed)
	GetGroup(context.Context, *logger.Logger, uuid.UUID) (*auth.Group, error)
	GetGroupByExternalID(context.Context, *logger.Logger, string) (*auth.Group, error)
	UpdateGroup(context.Context, *logger.Logger, *auth.Group) error
	DeleteGroup(context.Context, *logger.Logger, uuid.UUID) error
	ListGroups(context.Context, *logger.Logger, filtertypes.FilterSet) ([]*auth.Group, int, error)

	// Group Members
	// GetGroupMembers returns all members of a group.
	GetGroupMembers(context.Context, *logger.Logger, uuid.UUID, filtertypes.FilterSet) ([]*auth.GroupMember, int, error)
	// CreateGroupWithMembers atomically creates a group and adds initial members in a single transaction.
	CreateGroupWithMembers(context.Context, *logger.Logger, *auth.Group, []uuid.UUID) error
	// UpdateGroupWithMembers atomically updates a group and applies member operations in a single transaction.
	// If group is non-nil, it will be updated. Member operations are applied regardless.
	UpdateGroupWithMembers(context.Context, *logger.Logger, *auth.Group, []GroupMemberOperation) error

	// GetUserPermissionsFromGroups returns permissions for a user by joining
	// group_members → groups → group_permissions in a single query.
	GetUserPermissionsFromGroups(context.Context, *logger.Logger, uuid.UUID) ([]*auth.GroupPermission, error)

	// Group Permissions (maps group names to permissions)
	ListGroupPermissions(context.Context, *logger.Logger, filtertypes.FilterSet) ([]*auth.GroupPermission, int, error)
	GetPermissionsForGroups(context.Context, *logger.Logger, []string) ([]*auth.GroupPermission, error)
	CreateGroupPermission(context.Context, *logger.Logger, *auth.GroupPermission) error
	UpdateGroupPermission(context.Context, *logger.Logger, *auth.GroupPermission) error
	DeleteGroupPermission(context.Context, *logger.Logger, uuid.UUID) error
	ReplaceGroupPermissions(context.Context, *logger.Logger, []*auth.GroupPermission) error

	// GetStatistics returns current counts for metrics gauges.
	GetStatistics(context.Context, *logger.Logger) (*Statistics, error)
}
