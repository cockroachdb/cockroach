// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	rauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// AuthenticateToken validates a bearer token and returns the fully-loaded principal.
// This combines: token validation, user/SA lookup, status checks, and permission resolution.
// Used by the bearer authenticator as a single entry point for authentication.
func (s *Service) AuthenticateToken(
	ctx context.Context, l *logger.Logger, tokenString string, clientIP string,
) (*pkgauth.Principal, error) {
	start := timeutil.Now()

	// 1. Validate token (includes IP check for SAs)
	token, err := s.validateToken(ctx, l, tokenString, clientIP)
	if err != nil {
		// Record metrics based on error type
		if errors.Is(err, authtypes.ErrTokenExpired) {
			s.metrics.RecordTokenValidation("expired", timeutil.Since(start))
		} else if errors.Is(err, authtypes.ErrInvalidToken) {
			s.metrics.RecordTokenValidation("revoked", timeutil.Since(start))
		} else if errors.Is(err, authtypes.ErrIPNotAllowed) {
			s.metrics.RecordTokenValidation("ip_blocked", timeutil.Since(start))
		} else {
			s.metrics.RecordTokenValidation("invalid", timeutil.Since(start))
		}
		return nil, err
	}

	// 2. Build principal with token info
	principal := &pkgauth.Principal{
		Token: pkgauth.TokenInfo{
			ID:        token.ID,
			Type:      token.TokenType,
			CreatedAt: &token.CreatedAt,
			ExpiresAt: &token.ExpiresAt,
		},
	}

	// 3. Load user or service account with permissions
	switch token.TokenType {
	case authmodels.TokenTypeUser:
		if token.UserID == nil {
			s.metrics.RecordTokenValidation("invalid", timeutil.Since(start))
			return nil, errors.New("user token missing user ID")
		}
		if err := s.loadUserPrincipal(ctx, l, principal, *token.UserID); err != nil {
			// Record user_inactive metric if user is deactivated
			if errors.Is(err, authtypes.ErrUserDeactivated) {
				s.metrics.RecordTokenValidation("user_inactive", timeutil.Since(start))
			} else {
				s.metrics.RecordTokenValidation("invalid", timeutil.Since(start))
			}
			return nil, err
		}
	case authmodels.TokenTypeServiceAccount:
		if token.ServiceAccountID == nil {
			s.metrics.RecordTokenValidation("invalid", timeutil.Since(start))
			return nil, errors.New("service account token missing service account ID")
		}
		if err := s.loadServiceAccountPrincipal(ctx, l, principal, *token.ServiceAccountID); err != nil {
			// Service account disabled counts as inactive
			if errors.Is(err, authtypes.ErrServiceAccountDisabled) || errors.Is(err, authtypes.ErrUserDeactivated) {
				s.metrics.RecordTokenValidation("user_inactive", timeutil.Since(start))
			} else {
				s.metrics.RecordTokenValidation("invalid", timeutil.Since(start))
			}
			return nil, err
		}
	default:
		return nil, authtypes.ErrInvalidTokenType
	}

	// Record successful token validation
	s.metrics.RecordTokenValidation("success", timeutil.Since(start))
	return principal, nil
}

// loadUserPrincipal loads the user and their permissions into the principal.
func (s *Service) loadUserPrincipal(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, userID uuid.UUID,
) error {
	// Get user from repository
	user, err := s.repo.GetUser(ctx, l, userID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return authtypes.ErrUserNotProvisioned
		}
		return errors.Wrap(err, "failed to get user")
	}

	// Check if user is active
	if !user.Active {
		return authtypes.ErrUserDeactivated
	}

	principal.UserID = &user.ID
	principal.User = user

	// Load permissions from groups (single query joining group_members → groups → group_permissions)
	groupPerms, err := s.repo.GetUserPermissionsFromGroups(ctx, l, userID)
	if err != nil {
		return errors.Wrap(err, "failed to get user permissions from groups")
	}

	// Convert GroupPermissions to Permission interface (via UserPermission).
	// These are ephemeral permission objects used only within this request's
	// principal; they are never persisted, so they use a zero UUID.
	permissions := make([]authmodels.Permission, len(groupPerms))
	for i, gp := range groupPerms {
		permissions[i] = &authmodels.UserPermission{
			ID:         uuid.UUID{},
			UserID:     userID,
			Scope:      gp.Scope,
			Permission: gp.Permission,
		}
	}
	principal.Permissions = permissions

	return nil
}

// loadServiceAccountPrincipal loads the service account and their permissions into the principal.
func (s *Service) loadServiceAccountPrincipal(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, saID uuid.UUID,
) error {
	// Get service account from repository
	sa, err := s.repo.GetServiceAccount(ctx, l, saID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return authtypes.ErrServiceAccountNotFound
		}
		return errors.Wrap(err, "failed to get service account")
	}

	// Check if service account is enabled
	if !sa.Enabled {
		return authtypes.ErrServiceAccountDisabled
	}

	principal.ServiceAccountID = &sa.ID
	principal.ServiceAccount = sa

	if sa.DelegatedFrom != nil {
		// Load user info for delegated from user
		user, err := s.repo.GetUser(ctx, l, *sa.DelegatedFrom)
		if err != nil {
			if errors.Is(err, rauth.ErrNotFound) {
				return authtypes.ErrUserNotProvisioned
			}
			return errors.Wrap(err, "failed to get user")
		}

		if !user.Active {
			return authtypes.ErrUserDeactivated
		}
		principal.DelegatedFrom = &user.ID
		principal.DelegatedFromEmail = user.Email
	}

	// If the service account inherits permissions from a user principal,
	// load those permissions, else load its own permissions.
	var permissions []authmodels.Permission
	if sa.DelegatedFrom != nil {
		saPerms, err := s.repo.GetUserPermissionsFromGroups(ctx, l, *principal.DelegatedFrom)
		if err != nil {
			return errors.Wrap(err, "failed to get user permissions from groups")
		}

		// Ephemeral permission objects for this request's principal; never
		// persisted, so they use a zero UUID.
		permissions = make([]authmodels.Permission, len(saPerms))
		for i, gp := range saPerms {
			permissions[i] = &authmodels.UserPermission{
				ID:         uuid.UUID{},
				UserID:     *sa.DelegatedFrom,
				Scope:      gp.Scope,
				Permission: gp.Permission,
			}
		}
	} else {
		// Load service account permissions
		saPerms, _, err := s.repo.ListServiceAccountPermissions(ctx, l, saID, *filters.NewFilterSet())
		if err != nil {
			return errors.Wrap(err, "failed to get service account permissions")
		}

		permissions = make([]authmodels.Permission, len(saPerms))
		for i, p := range saPerms {
			permissions[i] = p
		}
	}

	principal.Permissions = permissions

	return nil
}
