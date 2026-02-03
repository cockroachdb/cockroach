// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// ErrNotFound is returned when a requested resource is not found.
var ErrNotFound = errors.New("not found")

// GroupMemberOperation represents an add or remove operation for group membership.
// Used by PatchGroupMembers for atomic SCIM PATCH operations.
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
	GetUser(context.Context, *logger.Logger, uuid.UUID) (*auth.User, error)
	GetUserByOktaID(context.Context, *logger.Logger, string) (*auth.User, error)

	// Service Accounts
	GetServiceAccount(context.Context, *logger.Logger, uuid.UUID) (*auth.ServiceAccount, error)

	// Service Account Origins
	ListServiceAccountOrigins(context.Context, *logger.Logger, uuid.UUID, filtertypes.FilterSet) ([]*auth.ServiceAccountOrigin, int, error)

	// Tokens
	ListAllTokens(context.Context, *logger.Logger, filtertypes.FilterSet) ([]*auth.ApiToken, int, error)
	GetToken(context.Context, *logger.Logger, uuid.UUID) (*auth.ApiToken, error)
	CreateToken(context.Context, *logger.Logger, *auth.ApiToken) error
	GetTokenByHash(context.Context, *logger.Logger, string) (*auth.ApiToken, error)
	UpdateTokenLastUsed(context.Context, *logger.Logger, uuid.UUID) error
	RevokeToken(context.Context, *logger.Logger, uuid.UUID) error

	// GetUserPermissionsFromGroups returns permissions for a user by joining
	// group_members → groups → group_permissions in a single query.
	GetUserPermissionsFromGroups(context.Context, *logger.Logger, uuid.UUID) ([]*auth.GroupPermission, error)

	// Permissions
	ListServiceAccountPermissions(context.Context, *logger.Logger, uuid.UUID, filtertypes.FilterSet) ([]*auth.ServiceAccountPermission, int, error)
	// GetServiceAccountPermission retrieves a single permission by ID.
	GetServiceAccountPermission(context.Context, *logger.Logger, uuid.UUID) (*auth.ServiceAccountPermission, error)
}
