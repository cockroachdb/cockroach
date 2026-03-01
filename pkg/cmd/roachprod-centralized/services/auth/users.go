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

// ListUsers lists all users (admin/SCIM operation).
func (s *Service) ListUsers(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	inputDTO authtypes.InputListUsersDTO,
) ([]*auth.User, int, error) {
	start := timeutil.Now()

	users, total, err := s.repo.ListUsers(ctx, l, inputDTO.Filters)
	if err != nil {
		s.metrics.RecordSCIMRequest("list_users", "error", timeutil.Since(start))
		return nil, 0, err
	}

	s.metrics.RecordSCIMRequest("list_users", "success", timeutil.Since(start))
	return users, total, nil
}

// GetUser retrieves a user by ID (SCIM GET /scim/v2/Users/:id).
// Returns ErrUserNotFound if the user does not exist.
func (s *Service) GetUser(
	ctx context.Context, l *logger.Logger, _ *pkgauth.Principal, id uuid.UUID,
) (*auth.User, error) {
	start := timeutil.Now()

	// Get user from repository
	user, err := s.repo.GetUser(ctx, l, id)
	if err != nil {
		s.metrics.RecordSCIMRequest("get_user", "error", timeutil.Since(start))
		if errors.Is(err, rauth.ErrNotFound) {
			return nil, authtypes.ErrUserNotFound
		}
		return nil, errors.Wrap(err, "failed to get user")
	}

	s.metrics.RecordSCIMRequest("get_user", "success", timeutil.Since(start))
	// No audit logging for read operations
	return user, nil
}

// CreateUser creates a new user (SCIM POST /scim/v2/Users).
// Validates input, checks for duplicates, sets timestamps, and audits the operation.
func (s *Service) CreateUser(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, user *auth.User,
) error {
	start := timeutil.Now()

	// 1. Validate input
	if err := validateUser(user, false); err != nil {
		s.metrics.RecordSCIMRequest("create_user", "error", timeutil.Since(start))
		return err
	}

	// 2. Check for duplicate email
	existingUser, err := s.repo.GetUserByEmail(ctx, l, user.Email)
	if err != nil && !errors.Is(err, rauth.ErrNotFound) {
		s.metrics.RecordSCIMRequest("create_user", "error", timeutil.Since(start))
		return errors.Wrap(err, "failed to check for duplicate email")
	}
	if existingUser != nil {
		s.metrics.RecordSCIMRequest("create_user", "error", timeutil.Since(start))
		return authtypes.ErrDuplicateUser
	}

	// 3. Check for duplicate Okta ID
	existingUser, err = s.repo.GetUserByOktaID(ctx, l, user.OktaUserID)
	if err != nil && !errors.Is(err, rauth.ErrNotFound) {
		s.metrics.RecordSCIMRequest("create_user", "error", timeutil.Since(start))
		return errors.Wrap(err, "failed to check for duplicate okta_user_id")
	}
	if existingUser != nil {
		s.metrics.RecordSCIMRequest("create_user", "error", timeutil.Since(start))
		return authtypes.ErrDuplicateOktaID
	}

	// 4. Set ID and timestamps at service layer
	user.ID = uuid.MakeV4()
	now := timeutil.Now()
	user.CreatedAt = now
	user.UpdatedAt = now

	// 5. Create user in repository
	if err := s.repo.CreateUser(ctx, l, user); err != nil {
		s.metrics.RecordSCIMRequest("create_user", "error", timeutil.Since(start))
		return errors.Wrap(err, "failed to create user")
	}

	// 6. Record metrics
	s.metrics.RecordSCIMRequest("create_user", "success", timeutil.Since(start))
	s.metrics.RecordSCIMUserProvisioned()

	// 7. Audit the operation
	s.auditEvent(ctx, l, principal, AuditSCIMUserCreated, "success", map[string]interface{}{
		"user_id": user.ID.String(),
	})

	return nil
}

// ReplaceUser replaces a user's attributes (SCIM PUT /scim/v2/Users/:id).
// All business logic is contained in this method - the controller just passes the DTO.
func (s *Service) ReplaceUser(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	userID uuid.UUID,
	input authtypes.ReplaceUserInput,
) (*auth.User, error) {
	start := timeutil.Now()

	// 1. Get existing user
	user, err := s.GetUser(ctx, l, principal, userID)
	if err != nil {
		s.metrics.RecordSCIMRequest("replace_user", "error", timeutil.Since(start))
		return nil, err
	}

	wasDeactivated := user.Active && !input.Active
	wasReactivated := !user.Active && input.Active

	// 2. Apply updates from input
	if input.ExternalID != "" {
		user.OktaUserID = input.ExternalID
	}
	user.Email = input.UserName
	user.FullName = input.FullName
	user.Active = input.Active
	user.UpdatedAt = timeutil.Now()

	// 3. Validate the updated user
	if err := validateUser(user, true); err != nil {
		s.metrics.RecordSCIMRequest("replace_user", "error", timeutil.Since(start))
		return nil, err
	}

	// 4. Update user in repository
	if err := s.repo.UpdateUser(ctx, l, user); err != nil {
		s.metrics.RecordSCIMRequest("replace_user", "error", timeutil.Since(start))
		return nil, errors.Wrap(err, "failed to update user")
	}

	// 5. Record metrics
	s.metrics.RecordSCIMRequest("replace_user", "success", timeutil.Since(start))
	if wasDeactivated {
		s.metrics.RecordSCIMUserDeactivated()
	}
	if wasReactivated {
		s.metrics.RecordSCIMUserReactivated()
	}

	// 6. Audit the operation
	s.auditEvent(ctx, l, principal, AuditSCIMUserUpdated, "success", map[string]interface{}{
		"user_id": user.ID.String(),
	})
	if wasDeactivated {
		s.auditEvent(ctx, l, principal, AuditSCIMUserDeactivated, "success", map[string]interface{}{
			"user_id": userID.String(),
		})
	}
	if wasReactivated {
		s.auditEvent(ctx, l, principal, AuditSCIMUserReactivated, "success", map[string]interface{}{
			"user_id": userID.String(),
		})
	}

	return user, nil
}

// PatchUser applies patch operations to a user (SCIM PATCH /scim/v2/Users/:id).
// All business logic is contained in this method - the controller just passes the DTO.
func (s *Service) PatchUser(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	userID uuid.UUID,
	input authtypes.PatchUserInput,
) (*auth.User, error) {
	start := timeutil.Now()

	// 1. Get existing user
	user, err := s.GetUser(ctx, l, principal, userID)
	if err != nil {
		s.metrics.RecordSCIMRequest("patch_user", "error", timeutil.Since(start))
		return nil, err
	}

	wasDeactivated := false
	wasReactivated := false

	// 2. Process patch operations
	for _, op := range input.Operations {
		switch {
		case op.Op == "replace" && op.Path == "active":
			if active, ok := op.Value.(bool); ok {
				if user.Active && !active {
					wasDeactivated = true
				}
				if !user.Active && active {
					wasReactivated = true
				}
				user.Active = active
			}
		case op.Op == "replace" && op.Path == "userName":
			if userName, ok := op.Value.(string); ok {
				user.Email = userName
			}
		case op.Op == "replace" && op.Path == "name.formatted":
			if formatted, ok := op.Value.(string); ok {
				user.FullName = formatted
			}
		case op.Op == "replace" && op.Path == "externalId":
			if externalID, ok := op.Value.(string); ok {
				user.OktaUserID = externalID
			}
		}
	}

	// 3. Update user
	user.UpdatedAt = timeutil.Now()
	if err := s.repo.UpdateUser(ctx, l, user); err != nil {
		s.metrics.RecordSCIMRequest("patch_user", "error", timeutil.Since(start))
		return nil, errors.Wrap(err, "failed to update user")
	}

	// 4. Record metrics
	s.metrics.RecordSCIMRequest("patch_user", "success", timeutil.Since(start))
	if wasDeactivated {
		s.metrics.RecordSCIMUserDeactivated()
	}
	if wasReactivated {
		s.metrics.RecordSCIMUserReactivated()
	}

	// 5. Audit
	s.auditEvent(ctx, l, principal, AuditSCIMUserUpdated, "success", map[string]interface{}{
		"user_id": user.ID.String(),
	})
	if wasDeactivated {
		s.auditEvent(ctx, l, principal, AuditSCIMUserDeactivated, "success", map[string]interface{}{
			"user_id": userID.String(),
		})
	}
	if wasReactivated {
		s.auditEvent(ctx, l, principal, AuditSCIMUserReactivated, "success", map[string]interface{}{
			"user_id": user.ID.String(),
		})
	}

	return user, nil
}

// DeleteUser performs a hard delete (permanent removal) of a user.
// (SCIM DELETE /scim/v2/Users/:id)
func (s *Service) DeleteUser(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, id uuid.UUID,
) error {
	// 1. Check if user exists
	_, err := s.GetUser(ctx, l, principal, id)
	if err != nil {
		return err // Already wrapped by GetUser
	}

	// 2. Delete user from repository
	if err := s.repo.DeleteUser(ctx, l, id); err != nil {
		return errors.Wrap(err, "failed to delete user")
	}

	// 3. Audit the operation
	s.auditEvent(ctx, l, principal, AuditSCIMUserDeleted, "success", map[string]interface{}{
		"user_id": id.String(),
	})

	return nil
}

// ExchangeOktaToken exchanges an Okta access token for a roachprod opaque token.
func (s *Service) ExchangeOktaToken(
	ctx context.Context, l *logger.Logger, oktaToken string,
) (*auth.ApiToken, string, error) {
	start := timeutil.Now()
	var oktaUserID string
	var token *auth.ApiToken
	var returnErr error

	// Audit and metrics on exit
	defer func() {
		finishDetails := make(map[string]interface{})
		if returnErr != nil {
			// Error case
			finishDetails["error"] = returnErr.Error()
			if oktaUserID != "" {
				finishDetails["okta_user_id"] = oktaUserID
			}
			s.auditEvent(ctx, l, nil, AuditOktaExchangeFinish, "error", finishDetails)
			s.metrics.RecordOktaExchange("error", timeutil.Since(start))
		} else if token != nil {
			// Success case
			finishDetails["user_id"] = token.UserID.String()
			finishDetails["token_id"] = token.ID.String()
			finishDetails["token_suffix"] = token.TokenSuffix
			s.auditEvent(ctx, l, nil, AuditOktaExchangeFinish, "success", finishDetails)
			s.metrics.RecordOktaExchange("success", timeutil.Since(start))
			s.metrics.RecordTokenIssued("user")
		}
	}()

	// 0. Check if validator is configured
	if s.tokenValidator == nil {
		returnErr = errors.New("Okta token validator not configured - call WithTokenValidator during service initialization")
		return nil, "", returnErr
	}

	// 1. Validate Okta token
	var err error
	oktaUserID, _, err = s.tokenValidator(ctx, oktaToken)
	if err != nil {
		returnErr = errors.Wrap(err, "invalid okta token")
		return nil, "", returnErr
	}

	// Audit start event (after we have oktaUserID)
	startDetails := map[string]interface{}{
		"okta_user_id": oktaUserID,
	}
	s.auditEvent(ctx, l, nil, AuditOktaExchangeStart, "success", startDetails)

	// 2. Find user in DB
	user, err := s.repo.GetUserByOktaID(ctx, l, oktaUserID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			s.metrics.RecordUserNotProvisioned()
			returnErr = authtypes.ErrUserNotProvisioned
			return nil, "", returnErr
		}
		returnErr = errors.Wrap(err, "failed to lookup user")
		return nil, "", returnErr
	}
	if !user.Active {
		returnErr = authtypes.ErrUserDeactivated
		return nil, "", returnErr
	}

	// 3. Generate opaque token
	tokenStr, tokenHash, tokenSuffix, err := s.generateToken(auth.TokenTypeUser)
	if err != nil {
		returnErr = err
		return nil, "", returnErr
	}

	// 4. Create token record
	now := timeutil.Now()
	token = &auth.ApiToken{
		ID:          uuid.MakeV4(),
		TokenHash:   tokenHash,
		TokenSuffix: tokenSuffix,
		TokenType:   auth.TokenTypeUser,
		UserID:      &user.ID,
		Status:      auth.TokenStatusValid,
		CreatedAt:   now,
		UpdatedAt:   now,
		ExpiresAt:   now.Add(authtypes.TokenDefaultTTLUser),
	}

	if err := s.repo.CreateToken(ctx, l, token); err != nil {
		returnErr = errors.Wrap(err, "failed to persist token")
		return nil, "", returnErr
	}

	s.auditEvent(ctx, l, &pkgauth.Principal{UserID: &user.ID}, AuditTokenIssued, "success", startDetails)

	return token, tokenStr, nil
}
