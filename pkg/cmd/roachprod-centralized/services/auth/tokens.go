// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"net"

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

// validateToken validates an opaque token.
// For service account tokens, enforces IP allowlist if configured.
func (s *Service) validateToken(
	ctx context.Context, l *logger.Logger, tokenString string, clientIP string,
) (*auth.ApiToken, error) {
	// 1. Hash the token
	tokenHash := hashToken(tokenString)

	// 2. Lookup in DB
	token, err := s.repo.GetTokenByHash(ctx, l, tokenHash)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			// Audit the failed validation (no token found)
			s.auditEvent(ctx, l, nil, AuditTokenValidationFailed, "deny", map[string]interface{}{
				"reason": "token_not_found",
			})
			return nil, types.ErrInvalidToken
		}
		return nil, errors.Wrap(err, "failed to lookup token")
	}

	// 3. Check status
	if token.Status != auth.TokenStatusValid {
		s.auditEvent(ctx, l, nil, AuditTokenValidationFailed, "deny", map[string]interface{}{
			"reason":       "invalid_status",
			"token_id":     token.ID.String(),
			"token_suffix": token.TokenSuffix,
			"status":       string(token.Status),
		})
		return nil, types.ErrInvalidToken
	}

	// 4. Check expiry
	if timeutil.Now().After(token.ExpiresAt) {
		s.auditEvent(ctx, l, nil, AuditTokenValidationFailed, "deny", map[string]interface{}{
			"reason":       "expired",
			"token_id":     token.ID.String(),
			"token_suffix": token.TokenSuffix,
		})
		return nil, types.ErrTokenExpired
	}

	// 5. IP allowlist enforcement for service account tokens
	if token.TokenType == auth.TokenTypeServiceAccount && token.ServiceAccountID != nil {
		// Get allowed origins for this service account
		origins, _, err := s.repo.ListServiceAccountOrigins(ctx, l, *token.ServiceAccountID, *filters.NewFilterSet())
		if err != nil {
			l.Warn(
				"failed to get service account origins",
				"sa_id", token.ServiceAccountID.String(),
				"error", err,
			)
			return nil, errors.Wrap(err, "failed to get service account origins")
		}

		// If origins are configured, enforce allowlist
		if len(origins) > 0 {
			allowed := false
			clientIPParsed := net.ParseIP(clientIP)
			if clientIPParsed == nil {
				s.auditEvent(ctx, l, nil, AuditTokenValidationFailed, "deny", map[string]interface{}{
					"reason":       "invalid_client_ip",
					"token_id":     token.ID.String(),
					"token_suffix": token.TokenSuffix,
					"sa_id":        token.ServiceAccountID.String(),
				})
				return nil, types.ErrIPNotAllowed
			}

			for _, origin := range origins {
				_, ipNet, err := net.ParseCIDR(origin.CIDR)
				if err != nil {
					// Log but continue checking other origins
					l.Warn(
						"invalid CIDR in service account origin",
						"origin_id", origin.ID.String(),
						"cidr", origin.CIDR,
						"error", err,
					)
					continue
				}
				if ipNet.Contains(clientIPParsed) {
					allowed = true
					break
				}
			}

			if !allowed {
				s.auditEvent(ctx, l, nil, AuditTokenValidationFailed, "deny", map[string]interface{}{
					"reason":       "ip_not_allowed",
					"token_id":     token.ID.String(),
					"token_suffix": token.TokenSuffix,
					"sa_id":        token.ServiceAccountID.String(),
				})
				return nil, types.ErrIPNotAllowed
			}
		}
	}

	// 6. Queue async last_used_at update
	select {
	case s.tokenLastUsedCh <- token.ID:
	default:
		l.Warn(
			"token last_used_at update channel full, dropping update",
			"token_id", token.ID.String(),
		)
	}

	// Audit successful token validation
	s.auditEvent(ctx, l, nil, AuditAuthnValidated, "success", map[string]interface{}{
		"token_id":     token.ID.String(),
		"token_suffix": token.TokenSuffix,
		"token_type":   string(token.TokenType),
	})

	return token, nil
}

// ListAllTokens lists all tokens (admin operation).
func (s *Service) ListAllTokens(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	inputDTO types.InputListTokensDTO,
) ([]*auth.ApiToken, int, error) {
	return s.repo.ListAllTokens(ctx, l, inputDTO.Filters)
}

// ListSelfTokens lists all tokens belonging to the authenticated principal.
func (s *Service) ListSelfTokens(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	inputDTO types.InputListTokensDTO,
) ([]*auth.ApiToken, int, error) {
	// Start with the filters from the input DTO
	filterSet := inputDTO.Filters

	// Wrap user-provided filters into a subgroup, then add mandatory
	// ownership filter at the top level. No sanitization needed —
	// the outer AND ensures the ownership constraint regardless of what
	// the user's filters contain.
	filterSet.NestFiltersAsSubGroup()
	filterSet.SetLogic(filtertypes.LogicAnd)

	// Add mandatory filter to restrict tokens to the authenticated principal
	switch principal.Token.Type {
	case auth.TokenTypeUser:

		filterSet.AddFilter("UserID", filtertypes.OpEqual, principal.UserID)

	case auth.TokenTypeServiceAccount:
		filterSet.AddFilter("ServiceAccountID", filtertypes.OpEqual, principal.ServiceAccountID)

	default:
		return nil, 0, errors.New("invalid principal token type")
	}

	// Get all tokens filtered by principal's user ID or service account ID (plus any additional filters)
	tokens, totalCount, err := s.repo.ListAllTokens(ctx, l, filterSet)
	if err != nil {
		return nil, 0, err
	}

	return tokens, totalCount, nil
}

// RevokeSelfToken revokes a token belonging to the authenticated principal.
// Returns ErrTokenNotFound if token doesn't exist or doesn't belong to principal.
func (s *Service) RevokeSelfToken(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, tokenID uuid.UUID,
) error {
	// Get the token
	token, err := s.repo.GetToken(ctx, l, tokenID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return types.ErrTokenNotFound
		}
		return errors.Wrap(err, "failed to get token")
	}

	// Check ownership - don't leak existence if not owned
	switch token.TokenType {
	case auth.TokenTypeUser:
		if token.UserID == nil || principal.UserID == nil || *token.UserID != *principal.UserID {
			return types.ErrTokenNotFound
		}
	case auth.TokenTypeServiceAccount:

		// Service accounts that are DelegatedFrom a user principal should maybe
		// be able to see tokens created by that user as well?
		// We currently have a limitation that prevents us from grouping filters
		// with OR logic, so we only allow seeing tokens created by the service account
		// itself as a precaution.
		if token.ServiceAccountID == nil || principal.ServiceAccountID == nil || *token.ServiceAccountID != *principal.ServiceAccountID {
			return types.ErrTokenNotFound
		}

	default:
		return types.ErrTokenNotFound
	}

	// Revoke the token
	if err := s.repo.RevokeToken(ctx, l, tokenID); err != nil {
		s.auditEvent(ctx, l, principal, AuditTokenRevoked, "error", map[string]interface{}{
			"token_id":     tokenID.String(),
			"token_suffix": token.TokenSuffix,
			"error":        err.Error(),
		})
		return err
	}

	s.auditEvent(ctx, l, principal, AuditTokenRevoked, "success", map[string]interface{}{
		"token_id":     tokenID.String(),
		"token_suffix": token.TokenSuffix,
		"scope":        "self",
	})

	// Record metrics for successful revocation
	s.metrics.RecordTokenRevoked("self")

	return nil
}

// RevokeUserToken revokes a token belonging to a specific user.
// This is an admin operation that requires PermissionScimManageUser.
// Returns ErrTokenNotFound if token doesn't exist or doesn't belong to the user.
func (s *Service) RevokeUserToken(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	userID uuid.UUID,
	tokenID uuid.UUID,
) error {

	// Get the user to verify it exists
	user, err := s.repo.GetUser(ctx, l, userID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return types.ErrUserNotFound
		}
		return errors.Wrap(err, "failed to get user")
	}

	// Get the token
	token, err := s.repo.GetToken(ctx, l, tokenID)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return types.ErrTokenNotFound
		}
		return errors.Wrap(err, "failed to get token")
	}

	// Verify token belongs to this user
	if token.TokenType != auth.TokenTypeUser || token.UserID == nil || *token.UserID != userID {
		return types.ErrTokenNotFound
	}

	// Revoke the token
	if err := s.repo.RevokeToken(ctx, l, tokenID); err != nil {
		s.auditEvent(ctx, l, principal, AuditTokenRevoked, "error", map[string]interface{}{
			"token_id":     tokenID.String(),
			"token_suffix": token.TokenSuffix,
			"user_id":      userID.String(),
			"error":        err.Error(),
		})
		return err
	}

	s.auditEvent(ctx, l, principal, AuditTokenRevoked, "success", map[string]interface{}{
		"token_id":     tokenID.String(),
		"token_suffix": token.TokenSuffix,
		"user_id":      user.ID.String(),
		"user_email":   user.Email,
	})

	// Record metrics for successful revocation
	s.metrics.RecordTokenRevoked("admin")

	return nil
}

// Token generation helpers

// generateToken generates a new opaque token, its hash, and a displayable suffix.
// Format: rp$<type>$1$<random>
// The random portion uses base62 encoding (alphanumeric) for URL safety.
// Returns: (tokenString, tokenHash, tokenSuffix, error)
func (s *Service) generateToken(principalType auth.TokenType) (string, string, string, error) {
	// Generate random bytes using crypto/rand and encode as base62 (alphanumeric only)
	b := make([]byte, types.TokenEntropyLength)
	const base62Chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	for i := range b {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(base62Chars))))
		if err != nil {
			return "", "", "", errors.Wrap(err, "failed to generate random token")
		}
		b[i] = base62Chars[num.Int64()]
	}

	typeStr := types.TokenTypeUser
	if principalType == auth.TokenTypeServiceAccount {
		typeStr = types.TokenTypeSA
	}

	tokenString := fmt.Sprintf("%s$%s$%s$%s", types.TokenPrefix, typeStr, types.TokenVersion, string(b))
	tokenHash := hashToken(tokenString)

	// Compute suffix for audit logging: "rp$<type>$1$****<last8chars>"
	// This allows displaying a recognizable token suffix without exposing the full token.
	// With 43 chars of base62 and revealing 8, we still have 35 unknown chars ≈ 208 bits of entropy.
	suffix := computeTokenSuffix(tokenString, typeStr)

	return tokenString, tokenHash, suffix, nil
}

// hashToken computes the SHA-256 hash of the token string.
func hashToken(token string) string {
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}

// computeTokenSuffix creates a displayable suffix for audit logs.
// Format: "rp$<type>$1$****<last8chars>" where last8chars are from the random portion.
func computeTokenSuffix(token string, tokenType string) string {
	// Token format: rp$<type>$1$<random>
	// We want: rp$<type>$1$****<last8>
	if len(token) < 8 {
		return token
	}
	last8 := token[len(token)-8:]
	return fmt.Sprintf("%s$%s$1$****%s", types.TokenPrefix, tokenType, last8)
}
