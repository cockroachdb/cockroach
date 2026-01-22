// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// GetUserPermissions returns the user's permissions resolved from their group memberships.
// This dynamically resolves permissions by joining group_members → groups → group_permissions.
func (s *Service) GetUserPermissions(
	ctx context.Context, l *logger.Logger, _ *pkgauth.Principal, userID uuid.UUID,
) ([]*auth.UserPermission, error) {
	// Single query joining group_members → groups → group_permissions
	groupPermissions, err := s.repo.GetUserPermissionsFromGroups(ctx, l, userID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get user permissions from groups")
	}

	if len(groupPermissions) == 0 {
		return nil, nil
	}

	// Convert GroupPermissions to UserPermissions
	permissions := make([]*auth.UserPermission, 0, len(groupPermissions))
	for _, gp := range groupPermissions {
		permissions = append(permissions, &auth.UserPermission{
			ID:         uuid.MakeV4(),
			UserID:     userID,
			Provider:   gp.Provider,
			Account:    gp.Account,
			Permission: gp.Permission,
		})
	}

	return permissions, nil
}
