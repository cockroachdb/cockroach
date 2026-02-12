// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"
	"strings"
	"time"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	rauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// CreateServiceAccount creates a new service account with validation and audit logging.
func (s *Service) CreateServiceAccount(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	sa *auth.ServiceAccount,
	orphan bool,
) error {
	// Validate input
	if err := validateServiceAccount(sa, false); err != nil {
		return err
	}

	// Set timestamps at service layer
	now := timeutil.Now()
	sa.CreatedAt = now
	sa.UpdatedAt = now

	// Set enabled to true by default if not explicitly set
	if !sa.Enabled {
		sa.Enabled = true
	}

	// Set created_by from principal
	if principal != nil {
		if principal.UserID != nil {
			sa.CreatedBy = principal.UserID
		} else if principal.ServiceAccountID != nil {
			sa.CreatedBy = principal.ServiceAccountID
		}
	}

	// If the service account to be created is not orphan, we set DelegatedFrom
	// to the creating principal's user ID, so that the service account inherits
	// permissions from that user principal.
	if !orphan {
		// To create a non-orphan service account, we need a service principal
		// to be the creator and delegator.
		// The service principal can be either:
		// 1. a user, and DelegatedFrom is set to that user ID.
		// 2. a service account, which can be either:
		//   a. a delegated service account with a DelegatedFrom user principal,
		//      in which case DelegatedFrom is set to the principal's DelegatedFrom
		//      user ID.
		//   b. an orphan service account without a DelegatedFrom user principal,
		//      in which case we reject the creation request because orphan SAs
		//      are not delegators.
		switch {
		case principal == nil:
			return types.ErrSACreationNotAllowedFromOrphanSA
		case principal.UserID != nil:
			sa.DelegatedFrom = principal.UserID
		case principal.ServiceAccountID != nil && principal.DelegatedFrom != nil:
			sa.DelegatedFrom = principal.DelegatedFrom
		case principal.ServiceAccountID != nil && principal.DelegatedFrom == nil:
			return types.ErrSACreationNotAllowedFromOrphanSA
		}
	}

	// Create in repository
	if err := s.repo.CreateServiceAccount(ctx, l, sa); err != nil {
		s.auditEvent(ctx, l, principal, AuditSACreated, "error", map[string]interface{}{
			"error": err.Error(),
		})
		return errors.Wrap(err, "failed to create service account")
	}

	// Audit successful creation
	metadata := map[string]interface{}{
		"sa_id":  sa.ID.String(),
		"orphan": orphan,
	}
	if sa.DelegatedFrom != nil {
		metadata["delegated_from"] = sa.DelegatedFrom.String()
	}
	s.auditEvent(ctx, l, principal, AuditSACreated, "success", metadata)

	// Record metrics for successful creation
	s.metrics.RecordServiceAccountCreated()

	return nil
}

// GetServiceAccount retrieves a service account by ID.
// Enforces ownership: principals with :view:own can only view service accounts they created.
func (s *Service) GetServiceAccount(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, id uuid.UUID,
) (*auth.ServiceAccount, error) {
	sa, err := s.repo.GetServiceAccount(ctx, l, id)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return nil, types.ErrServiceAccountNotFound
		}
		return nil, errors.Wrap(err, "failed to get service account")
	}

	// Principal has permission to view all service accounts
	if principal.HasPermission(types.PermissionServiceAccountViewAll) {
		return sa, nil
	}

	// Check ownership
	if !isOwnedByPrincipal(sa, principal) {
		return nil, types.ErrServiceAccountNotFound
	}

	return sa, nil
}

// ListServiceAccounts lists all service accounts with optional filters.
// Enforces ownership: principals with :view:own can only list service accounts they created.
func (s *Service) ListServiceAccounts(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	input types.InputListServiceAccountsDTO,
) ([]*auth.ServiceAccount, int, error) {
	filterSet := input.Filters

	// Principal has permission to view all service accounts
	if principal.HasPermission(types.PermissionServiceAccountViewAll) {
		return s.repo.ListServiceAccounts(ctx, l, filterSet)
	}

	// Wrap user-provided filters into a subgroup, then add mandatory
	// ownership filter at the top level. No sanitization needed —
	// the outer AND ensures the ownership constraint regardless of what
	// the user's filters contain.
	filterSet.NestFiltersAsSubGroup()
	filterSet.SetLogic(filtertypes.LogicAnd)

	// Set ownership filter
	if principal.UserID != nil {

		// If the principal is a user, they can see service accounts they created
		// unless they have the :all permission, which is handled above.
		filterSet.AddFilter("CreatedBy", filtertypes.OpEqual, *principal.UserID)

	} else if principal.ServiceAccountID != nil {

		// Either the service account principal is orphan and only sees
		// its own service accounts, or it has a DelegatedFrom user principal,
		// and can see service accounts created by that user.
		ownerIds := []*uuid.UUID{principal.ServiceAccountID}
		if principal.DelegatedFrom != nil {
			ownerIds = append(ownerIds, principal.DelegatedFrom)
		}
		filterSet.AddFilter("CreatedBy", filtertypes.OpIn, ownerIds)

	} else {
		// Principal has no ID - return empty list
		return []*auth.ServiceAccount{}, 0, nil
	}

	return s.repo.ListServiceAccounts(ctx, l, filterSet)
}

// UpdateServiceAccount updates a service account with validation and audit logging.
// Enforces ownership: principals with :update:own can only update service accounts they created.
func (s *Service) UpdateServiceAccount(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	id uuid.UUID,
	updates types.UpdateServiceAccountDTO,
) (*auth.ServiceAccount, error) {
	// Get existing service account
	oldSA, err := s.repo.GetServiceAccount(ctx, l, id)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return nil, types.ErrServiceAccountNotFound
		}
		return nil, errors.Wrap(err, "failed to get service account")
	}

	// Principal has permission to update all service accounts
	if !principal.HasPermission(types.PermissionServiceAccountUpdateAll) {
		if !isOwnedByPrincipal(oldSA, principal) {
			return nil, types.ErrServiceAccountNotFound
		}
	}

	// Create a copy for tracking changes
	newSA := *oldSA

	// Apply updates (only update fields that were provided)
	if updates.Name != nil {
		newSA.Name = *updates.Name
	}
	if updates.Description != nil {
		newSA.Description = *updates.Description
	}
	if updates.Enabled != nil {
		newSA.Enabled = *updates.Enabled
	}

	// Validate the updated service account
	if err := validateServiceAccount(&newSA, true); err != nil {
		return nil, err
	}

	// Compute changed fields for audit
	changedFields := computeChangedFieldsSA(oldSA, &newSA)

	// Update timestamp at service layer
	newSA.UpdatedAt = timeutil.Now()

	// Update in repository
	if err := s.repo.UpdateServiceAccount(ctx, l, &newSA); err != nil {
		s.auditEvent(ctx, l, principal, AuditSAUpdated, "error", map[string]interface{}{
			"sa_id": id.String(),
			"error": err.Error(),
		})
		return nil, errors.Wrap(err, "failed to update service account")
	}

	// Audit successful update
	s.auditEvent(ctx, l, principal, AuditSAUpdated, "success", map[string]interface{}{
		"sa_id":          id.String(),
		"changed_fields": changedFields,
	})

	return &newSA, nil
}

// DeleteServiceAccount performs a hard delete of a service account.
// Enforces ownership: principals with :delete:own can only delete service accounts they created.
func (s *Service) DeleteServiceAccount(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, id uuid.UUID,
) error {
	// Get service account
	sa, err := s.repo.GetServiceAccount(ctx, l, id)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return types.ErrServiceAccountNotFound
		}
		return errors.Wrap(err, "failed to get service account")
	}

	// Principal only has permission to delete their own service accounts,
	// so we check ownership.
	if !principal.HasPermission(types.PermissionServiceAccountDeleteAll) {
		if !isOwnedByPrincipal(sa, principal) {
			return types.ErrServiceAccountNotFound
		}
	}

	// Delete from repository
	if err := s.repo.DeleteServiceAccount(ctx, l, id); err != nil {
		s.auditEvent(ctx, l, principal, AuditSADeleted, "error", map[string]interface{}{
			"sa_id": id.String(),
			"error": err.Error(),
		})
		return errors.Wrap(err, "failed to delete service account")
	}

	// Audit successful deletion
	s.auditEvent(ctx, l, principal, AuditSADeleted, "success", map[string]interface{}{
		"sa_id": id.String(),
	})

	// Record metrics for successful deletion
	s.metrics.RecordServiceAccountDeleted()

	return nil
}

// Service Account Token Operations

// MintServiceAccountToken creates a new token for a service account.
// Enforces ownership: principals with :mint:own can only mint tokens for SAs they created.
func (s *Service) MintServiceAccountToken(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	saID uuid.UUID,
	ttl time.Duration,
) (*auth.ApiToken, string, error) {
	// Validate TTL
	if ttl == 0 {
		ttl = types.TokenDefaultTTLServiceAccount
	}
	if err := validateTTL(ttl); err != nil {
		return nil, "", err
	}

	sa, err := s.repo.GetServiceAccount(ctx, l, saID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return nil, "", types.ErrServiceAccountNotFound
		}
		return nil, "", errors.Wrap(err, "failed to get service account")
	}
	if !sa.Enabled {
		return nil, "", types.ErrServiceAccountDisabled
	}

	// If the principal only has permission to mint tokens for their own service accounts,
	// we check ownership.
	if !principal.HasPermission(types.PermissionServiceAccountMintAll) {
		if !isOwnedByPrincipal(sa, principal) {
			return nil, "", types.ErrServiceAccountNotFound
		}
	}

	// Check token limit for this service account
	filterSet := filters.NewFilterSet().
		AddFilter("ServiceAccountID", filtertypes.OpEqual, saID).
		AddFilter("Status", filtertypes.OpEqual, auth.TokenStatusValid)
	validTokens, _, err := s.repo.ListAllTokens(ctx, l, *filterSet)
	if err != nil {
		return nil, "", errors.Wrap(err, "failed to count tokens")
	}
	if len(validTokens) >= types.MaxTokensPerServiceAccount {
		return nil, "", types.ErrTokenLimitExceeded
	}

	// Generate a new opaque token
	tokenStr, tokenHash, tokenSuffix, err := s.generateToken(auth.TokenTypeServiceAccount)
	if err != nil {
		return nil, "", err
	}

	// 6. Create token record
	now := timeutil.Now()
	token := &auth.ApiToken{
		ID:               uuid.MakeV4(),
		TokenHash:        tokenHash,
		TokenSuffix:      tokenSuffix,
		TokenType:        auth.TokenTypeServiceAccount,
		ServiceAccountID: &sa.ID,
		Status:           auth.TokenStatusValid,
		CreatedAt:        now,
		UpdatedAt:        now,
		ExpiresAt:        now.Add(ttl),
	}

	if err := s.repo.CreateToken(ctx, l, token); err != nil {
		return nil, "", errors.Wrap(err, "failed to persist token")
	}

	// 7. Audit log the token issuance
	s.auditEvent(ctx, l, principal, AuditTokenIssued, "success", map[string]interface{}{
		"token_id":     token.ID.String(),
		"token_suffix": token.TokenSuffix,
		"sa_id":        saID.String(),
		"ttl_seconds":  int64(ttl.Seconds()),
	})

	// Record metrics for successful token issuance
	s.metrics.RecordTokenIssued("service-account")

	return token, tokenStr, nil
}

// RevokeServiceAccountToken revokes a token belonging to a specific service account.
// This is used by the service-accounts controller when revoking tokens for a SA.
// Returns ErrTokenNotFound if token doesn't exist or doesn't belong to the SA.
func (s *Service) RevokeServiceAccountToken(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	saID uuid.UUID,
	tokenID uuid.UUID,
) error {
	// Get the service account to check authorization
	sa, err := s.repo.GetServiceAccount(ctx, l, saID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return types.ErrServiceAccountNotFound
		}
		return errors.Wrap(err, "failed to get service account")
	}

	// If the principal only has permission to revoke tokens for their own service accounts,
	// we check ownership.
	if !principal.HasPermission(types.PermissionServiceAccountUpdateAll) {
		if !isOwnedByPrincipal(sa, principal) {
			return types.ErrServiceAccountNotFound
		}
	}

	// Get the token
	token, err := s.repo.GetToken(ctx, l, tokenID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return types.ErrTokenNotFound
		}
		return errors.Wrap(err, "failed to get token")
	}

	// Verify token belongs to this service account
	if token.TokenType != auth.TokenTypeServiceAccount || token.ServiceAccountID == nil || *token.ServiceAccountID != saID {
		return types.ErrTokenNotFound
	}

	// Revoke the token
	if err := s.repo.RevokeToken(ctx, l, tokenID); err != nil {
		s.auditEvent(ctx, l, principal, AuditTokenRevoked, "error", map[string]interface{}{
			"token_id":     tokenID.String(),
			"token_suffix": token.TokenSuffix,
			"sa_id":        saID.String(),
			"error":        err.Error(),
		})
		return err
	}

	s.auditEvent(ctx, l, principal, AuditTokenRevoked, "success", map[string]interface{}{
		"token_id":     tokenID.String(),
		"token_suffix": token.TokenSuffix,
		"sa_id":        saID.String(),
	})

	// Record metrics for successful revocation
	s.metrics.RecordTokenRevoked("admin")

	return nil
}

// Service Account Origins

// AddServiceAccountOrigin adds an allowed origin (IP CIDR) to a service account.
// Enforces ownership: principals with :update:own can only modify SAs they created.
func (s *Service) AddServiceAccountOrigin(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	saID uuid.UUID,
	origin *auth.ServiceAccountOrigin,
) error {
	// Validate CIDR
	if err := validateCIDR(origin.CIDR); err != nil {
		return errors.Wrap(err, "invalid origin CIDR")
	}

	// Get service account to check authorization
	sa, err := s.repo.GetServiceAccount(ctx, l, saID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return types.ErrServiceAccountNotFound
		}
		return errors.Wrap(err, "failed to get service account")
	}

	// If the principal only has permission to update their own service accounts,
	// we check ownership.
	if !principal.HasPermission(types.PermissionServiceAccountUpdateAll) {
		if !isOwnedByPrincipal(sa, principal) {
			return types.ErrServiceAccountNotFound
		}
	}

	// Generate UUID for the origin
	origin.ID = uuid.MakeV4()
	origin.ServiceAccountID = saID

	// Set timestamps at service layer
	now := timeutil.Now()
	origin.CreatedAt = now
	origin.UpdatedAt = now

	if err := s.repo.AddServiceAccountOrigin(ctx, l, origin); err != nil {
		return err
	}

	s.auditEvent(ctx, l, principal, AuditSAOriginAdded, "success", map[string]interface{}{
		"sa_id":     saID.String(),
		"origin_id": origin.ID.String(),
		"cidr":      origin.CIDR,
	})

	return nil
}

// ListServiceAccountOrigins lists all allowed origins for a service account.
// Enforces ownership: principals with :view:own can only view origins for SAs they created.
func (s *Service) ListServiceAccountOrigins(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	saID uuid.UUID,
	input types.InputListServiceAccountOriginsDTO,
) ([]*auth.ServiceAccountOrigin, int, error) {
	// Get service account to check authorization
	sa, err := s.repo.GetServiceAccount(ctx, l, saID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return nil, 0, types.ErrServiceAccountNotFound
		}
		return nil, 0, errors.Wrap(err, "failed to get service account")
	}

	// If the principal only has permission to view their own service accounts,
	// we check ownership.
	if !principal.HasPermission(types.PermissionServiceAccountViewAll) {
		if !isOwnedByPrincipal(sa, principal) {
			return nil, 0, types.ErrServiceAccountNotFound
		}
	}

	return s.repo.ListServiceAccountOrigins(ctx, l, saID, input.Filters)
}

// ListServiceAccountTokens lists all tokens for a service account.
// Enforces ownership: principals with :view:own can only view tokens for SAs they created.
func (s *Service) ListServiceAccountTokens(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	saID uuid.UUID,
	input types.InputListServiceAccountTokensDTO,
) ([]*auth.ApiToken, int, error) {
	// Get service account to check authorization
	sa, err := s.repo.GetServiceAccount(ctx, l, saID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return nil, 0, types.ErrServiceAccountNotFound
		}
		return nil, 0, errors.Wrap(err, "failed to get service account")
	}

	// If the principal only has permission to view their own service accounts,
	// we check ownership.
	if !principal.HasPermission(types.PermissionServiceAccountViewAll) {
		if !isOwnedByPrincipal(sa, principal) {
			return nil, 0, types.ErrServiceAccountNotFound
		}
	}

	// Add mandatory filter to scope tokens to this service account
	filterSet := input.Filters
	filterSet.NestFiltersAsSubGroup()
	filterSet.SetLogic(filtertypes.LogicAnd)
	filterSet.AddFilter("ServiceAccountID", filtertypes.OpEqual, saID)

	return s.repo.ListAllTokens(ctx, l, filterSet)
}

// RemoveServiceAccountOrigin removes an allowed origin from a service account.
// Enforces ownership: principals with :update:own can only modify SAs they created.
// Takes both saID and originID to ensure intent and prevent accidental deletion.
func (s *Service) RemoveServiceAccountOrigin(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	saID uuid.UUID,
	originID uuid.UUID,
) error {
	// Get service account to check authorization
	sa, err := s.repo.GetServiceAccount(ctx, l, saID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return types.ErrServiceAccountNotFound
		}
		return errors.Wrap(err, "failed to get service account")
	}

	// If the principal only has permission to update their own service accounts,
	// we check ownership.
	if !principal.HasPermission(types.PermissionServiceAccountUpdateAll) {
		if !isOwnedByPrincipal(sa, principal) {
			return types.ErrServiceAccountNotFound
		}
	}

	// Verify the origin belongs to the specified service account
	origins, _, err := s.repo.ListServiceAccountOrigins(ctx, l, saID, *filters.NewFilterSet())
	if err != nil {
		return errors.Wrap(err, "failed to list origins")
	}

	found := false
	for _, origin := range origins {
		if origin.ID == originID {
			found = true
			break
		}
	}
	if !found {
		return types.ErrOriginNotFound
	}

	if err := s.repo.RemoveServiceAccountOrigin(ctx, l, originID); err != nil {
		return err
	}

	s.auditEvent(ctx, l, principal, AuditSAOriginRemoved, "success", map[string]interface{}{
		"sa_id":     saID.String(),
		"origin_id": originID.String(),
	})

	return nil
}

// Service Account Permissions

// ListServiceAccountPermissions retrieves all permissions for a service account with filtering/sorting/pagination.
// Enforces ownership: principals with :view:own can only view permissions for SAs they created.
func (s *Service) ListServiceAccountPermissions(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	saID uuid.UUID,
	input types.InputListServiceAccountPermissionsDTO,
) ([]*auth.ServiceAccountPermission, int, error) {
	// Get service account to check authorization
	sa, err := s.repo.GetServiceAccount(ctx, l, saID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return nil, 0, types.ErrServiceAccountNotFound
		}
		return nil, 0, errors.Wrap(err, "failed to get service account")
	}

	// If the principal only has permission to view their own service accounts,
	// we check ownership.
	if !principal.HasPermission(types.PermissionServiceAccountViewAll) {
		if !isOwnedByPrincipal(sa, principal) {
			return nil, 0, types.ErrServiceAccountNotFound
		}
	}

	if sa.DelegatedFrom != nil {
		// Service account inherits permissions from a user principal.
		// Return an empty list to avoid confusion, as permissions cannot be
		// directly managed on such service accounts.
		return []*auth.ServiceAccountPermission{}, 0, nil
	}

	return s.repo.ListServiceAccountPermissions(ctx, l, saID, input.Filters)
}

// UpdateServiceAccountPermissions replaces all permissions for a service account.
// Enforces ownership: principals with :update:own can only modify SAs they created.
func (s *Service) UpdateServiceAccountPermissions(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	saID uuid.UUID,
	permissions []*auth.ServiceAccountPermission,
) error {
	// Get service account to check authorization
	sa, err := s.repo.GetServiceAccount(ctx, l, saID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return types.ErrServiceAccountNotFound
		}
		return errors.Wrap(err, "failed to get service account")
	}

	// If the principal only has permission to update their own service accounts,
	// we check ownership.
	if !principal.HasPermission(types.PermissionServiceAccountUpdateAll) {
		if !isOwnedByPrincipal(sa, principal) {
			return types.ErrServiceAccountNotFound
		}
	}

	if sa.DelegatedFrom != nil {
		// Service account inherits permissions from a user principal.
		// Disallow direct permission management to avoid confusion.
		return types.ErrNonOrphanSAPermissionModification
	}

	// Privilege escalation check: you can only grant permissions you have
	for _, perm := range permissions {
		if !canGrantPermission(principal, perm) {
			s.auditEvent(ctx, l, principal, AuditSAPermissionsReplaced, "denied", map[string]interface{}{
				"sa_id":      saID.String(),
				"scope":      perm.Scope,
				"permission": perm.Permission,
				"reason":     "privilege_escalation",
			})
			return types.ErrPermissionEscalation
		}
	}

	if err := s.repo.UpdateServiceAccountPermissions(ctx, l, saID, permissions); err != nil {
		return err
	}

	s.auditEvent(ctx, l, principal, AuditSAPermissionsReplaced, "success", map[string]interface{}{
		"sa_id":            saID.String(),
		"permission_count": len(permissions),
	})

	return nil
}

// AddServiceAccountPermission adds a single permission to a service account.
// Enforces ownership: principals with :update:own can only modify SAs they created.
func (s *Service) AddServiceAccountPermission(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	saID uuid.UUID,
	permission *auth.ServiceAccountPermission,
) error {
	// Get service account to check authorization
	sa, err := s.repo.GetServiceAccount(ctx, l, saID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return types.ErrServiceAccountNotFound
		}
		return errors.Wrap(err, "failed to get service account")
	}

	// If the principal only has permission to update their own service accounts,
	// we check ownership.
	if !principal.HasPermission(types.PermissionServiceAccountUpdateAll) {
		if !isOwnedByPrincipal(sa, principal) {
			return types.ErrServiceAccountNotFound
		}
	}

	if sa.DelegatedFrom != nil {
		// Service account inherits permissions from a user principal.
		// Disallow direct permission management to avoid confusion.
		return types.ErrNonOrphanSAPermissionModification
	}

	// Privilege escalation check: you can only grant permissions you have
	if !canGrantPermission(principal, permission) {
		s.auditEvent(ctx, l, principal, AuditSAPermissionAdded, "denied", map[string]interface{}{
			"sa_id":      saID.String(),
			"scope":      permission.Scope,
			"permission": permission.Permission,
			"reason":     types.ErrPermissionEscalation.Error(),
		})
		return types.ErrPermissionEscalation
	}

	// Generate UUID and set timestamps
	permission.ID = uuid.MakeV4()
	permission.ServiceAccountID = saID
	permission.CreatedAt = timeutil.Now()

	// Check for duplicate permission
	existing, _, err := s.repo.ListServiceAccountPermissions(ctx, l, saID, *filters.NewFilterSet())
	if err != nil {
		return errors.Wrap(err, "failed to check existing permissions")
	}
	for _, p := range existing {
		if p.Scope == permission.Scope && p.Permission == permission.Permission {
			return types.ErrPermissionAlreadyExists
		}
	}

	if err := s.repo.AddServiceAccountPermission(ctx, l, permission); err != nil {
		return err
	}

	s.auditEvent(ctx, l, principal, AuditSAPermissionAdded, "success", map[string]interface{}{
		"sa_id":         saID.String(),
		"permission_id": permission.ID.String(),
		"scope":         permission.Scope,
		"permission":    permission.Permission,
	})

	return nil
}

// RemoveServiceAccountPermission removes a single permission from a service account.
// Enforces ownership: principals with :update:own can only modify SAs they created.
func (s *Service) RemoveServiceAccountPermission(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	saID uuid.UUID,
	permissionID uuid.UUID,
) error {
	// Get service account to check authorization
	sa, err := s.repo.GetServiceAccount(ctx, l, saID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return types.ErrServiceAccountNotFound
		}
		return errors.Wrap(err, "failed to get service account")
	}

	// If the principal only has permission to update their own service accounts,
	// we check ownership.
	if !principal.HasPermission(types.PermissionServiceAccountUpdateAll) {
		if !isOwnedByPrincipal(sa, principal) {
			return types.ErrServiceAccountNotFound
		}
	}

	if sa.DelegatedFrom != nil {
		// Service account inherits permissions from a user principal.
		// Disallow direct permission management to avoid confusion.
		return types.ErrNonOrphanSAPermissionModification
	}

	// Verify permission exists and belongs to this SA
	permission, err := s.repo.GetServiceAccountPermission(ctx, l, permissionID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return types.ErrPermissionNotFound
		}
		return errors.Wrap(err, "failed to get permission")
	}
	if permission.ServiceAccountID != saID {
		return types.ErrPermissionNotFound
	}

	if err := s.repo.RemoveServiceAccountPermission(ctx, l, permissionID); err != nil {
		return err
	}

	s.auditEvent(ctx, l, principal, AuditSAPermissionRemoved, "success", map[string]interface{}{
		"sa_id":         saID.String(),
		"permission_id": permissionID.String(),
	})

	return nil
}

// Helper functions

// canGrantPermission checks if a principal can grant a specific permission to a service account.
// Rules:
// 1. Principal must have the permission (or a higher-scoped version)
// 2. :all scope can grant :own, but :own cannot grant :all
// 3. For scoped permissions, principal must have access to that scope (or wildcard)
func canGrantPermission(
	principal *pkgauth.Principal, permission *auth.ServiceAccountPermission,
) bool {
	permStr := permission.Permission
	scope := permission.Scope

	// Check if principal has exact permission with matching or broader scope
	if principal.HasPermissionScoped(permStr, scope) {
		return true
	}

	// Check scope hierarchy: if granting :own and principal doesn't have :own,
	// check if principal has :all
	if strings.HasSuffix(permStr, ":own") {
		allVersion := strings.TrimSuffix(permStr, ":own") + ":all"
		if principal.HasPermissionScoped(allVersion, scope) {
			return true
		}
	}

	return false
}

// isOwnedByPrincipal checks if a service account was created by the given principal.
func isOwnedByPrincipal(sa *auth.ServiceAccount, principal *pkgauth.Principal) bool {
	if sa.CreatedBy == nil {
		return false
	}
	if principal.UserID != nil && *sa.CreatedBy == *principal.UserID {
		return true
	}
	if principal.ServiceAccountID != nil && *sa.CreatedBy == *principal.ServiceAccountID {
		return true
	}
	if principal.DelegatedFrom != nil && *sa.CreatedBy == *principal.DelegatedFrom {
		return true
	}
	return false
}

// computeChangedFieldsSA compares two service account objects and returns list of changed field names.
// Used for audit logging. Returns field names only, no values.
func computeChangedFieldsSA(old, new *auth.ServiceAccount) []string {
	var changed []string

	if old.Name != new.Name {
		changed = append(changed, "name")
	}
	if old.Description != new.Description {
		changed = append(changed, "description")
	}
	if old.Enabled != new.Enabled {
		changed = append(changed, "enabled")
	}

	return changed
}
