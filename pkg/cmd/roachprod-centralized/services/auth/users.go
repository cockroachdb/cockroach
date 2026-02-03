// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"

	authpkg "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	rauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

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

	s.auditEvent(ctx, l, &authpkg.Principal{UserID: &user.ID}, AuditTokenIssued, "success", startDetails)

	return token, tokenStr, nil
}
