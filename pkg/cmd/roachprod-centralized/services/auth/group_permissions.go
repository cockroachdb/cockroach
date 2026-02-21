// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	rauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// ListGroupPermissions retrieves all group permissions with filtering/pagination (admin operation).
// Returns ErrUnauthorized if principal lacks permission.
func (s *Service) ListGroupPermissions(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	input authtypes.InputListGroupPermissionsDTO,
) ([]*auth.GroupPermission, int, error) {
	// Validate filters if present
	if !input.Filters.IsEmpty() {
		if err := input.Filters.Validate(); err != nil {
			return nil, 0, err
		}
	}

	return s.repo.ListGroupPermissions(ctx, l, input.Filters)
}

// CreateGroupPermission creates a new group permission (admin operation).
// Returns ErrUnauthorized if principal lacks permission.
func (s *Service) CreateGroupPermission(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	permission *auth.GroupPermission,
) error {
	// Set timestamps at service layer
	now := timeutil.Now()
	permission.CreatedAt = now
	permission.UpdatedAt = now

	if err := s.repo.CreateGroupPermission(ctx, l, permission); err != nil {
		return err
	}

	s.auditEvent(ctx, l, principal, AuditGroupPermissionCreated, "success", map[string]interface{}{
		"permission_id": permission.ID.String(),
		"group_name":    permission.GroupName,
		"scope":         permission.Scope,
		"permission":    permission.Permission,
	})

	return nil
}

// UpdateGroupPermission updates a group permission (admin operation).
// Returns ErrUnauthorized if principal lacks permission.
func (s *Service) UpdateGroupPermission(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	permission *auth.GroupPermission,
) error {
	// Update timestamp at service layer
	permission.UpdatedAt = timeutil.Now()

	if err := s.repo.UpdateGroupPermission(ctx, l, permission); err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return authtypes.ErrGroupPermissionNotFound
		}
		return err
	}

	s.auditEvent(ctx, l, principal, AuditGroupPermissionUpdated, "success", map[string]interface{}{
		"permission_id": permission.ID.String(),
		"group_name":    permission.GroupName,
	})

	return nil
}

// DeleteGroupPermission deletes a group permission (admin operation).
// Returns ErrUnauthorized if principal lacks permission.
func (s *Service) DeleteGroupPermission(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, id uuid.UUID,
) error {
	if err := s.repo.DeleteGroupPermission(ctx, l, id); err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return authtypes.ErrGroupPermissionNotFound
		}
		return err
	}

	s.auditEvent(ctx, l, principal, AuditGroupPermissionDeleted, "success", map[string]interface{}{
		"permission_id": id.String(),
	})

	return nil
}

// ReplaceGroupPermissions replaces all group permissions atomically (admin operation).
// Returns ErrUnauthorized if principal lacks permission.
func (s *Service) ReplaceGroupPermissions(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	permissions []*auth.GroupPermission,
) error {
	// Set timestamps at service layer
	now := timeutil.Now()
	for _, permission := range permissions {
		permission.CreatedAt = now
		permission.UpdatedAt = now
	}

	if err := s.repo.ReplaceGroupPermissions(ctx, l, permissions); err != nil {
		return err
	}

	s.auditEvent(ctx, l, principal, AuditGroupPermissionsReplaced, "success", map[string]interface{}{
		"permission_count": len(permissions),
	})

	return nil
}
