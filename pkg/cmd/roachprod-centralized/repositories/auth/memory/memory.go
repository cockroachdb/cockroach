// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memory

import (
	"context"
	"log/slog"
	"reflect"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	rauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/auth"
	filters "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	memoryfilters "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/memory"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// MemAuthRepo is an in-memory implementation of the auth repository.
type MemAuthRepo struct {
	lock syncutil.Mutex

	users                     map[uuid.UUID]*auth.User
	serviceAccounts           map[uuid.UUID]*auth.ServiceAccount
	saOrigins                 map[uuid.UUID][]*auth.ServiceAccountOrigin     // SA ID -> Origins
	tokens                    map[uuid.UUID]*auth.ApiToken                   // Token ID -> Token
	tokensByHash              map[string]*auth.ApiToken                      // Token hash -> Token (for O(1) lookups)
	serviceAccountPermissions map[uuid.UUID][]*auth.ServiceAccountPermission // SA ID -> Perms

	// Groups (SCIM-managed)
	groups           map[uuid.UUID]*auth.Group         // Group ID -> Group
	groupMembers     map[uuid.UUID][]*auth.GroupMember // Group ID -> Members
	groupPermissions []*auth.GroupPermission
}

// NewAuthRepository creates a new in-memory auth repository.
func NewAuthRepository() *MemAuthRepo {
	return &MemAuthRepo{
		users:                     make(map[uuid.UUID]*auth.User),
		serviceAccounts:           make(map[uuid.UUID]*auth.ServiceAccount),
		saOrigins:                 make(map[uuid.UUID][]*auth.ServiceAccountOrigin),
		tokens:                    make(map[uuid.UUID]*auth.ApiToken),
		tokensByHash:              make(map[string]*auth.ApiToken),
		serviceAccountPermissions: make(map[uuid.UUID][]*auth.ServiceAccountPermission),
		groups:                    make(map[uuid.UUID]*auth.Group),
		groupMembers:              make(map[uuid.UUID][]*auth.GroupMember),
		groupPermissions:          make([]*auth.GroupPermission, 0),
	}
}

// Ensure MemAuthRepo implements IAuthRepository
var _ rauth.IAuthRepository = &MemAuthRepo{}

// Users

func (r *MemAuthRepo) GetUser(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) (*auth.User, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if u, ok := r.users[id]; ok {
		return u, nil
	}
	return nil, rauth.ErrNotFound
}

func (r *MemAuthRepo) GetUserByOktaID(
	ctx context.Context, l *logger.Logger, oktaID string,
) (*auth.User, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	for _, u := range r.users {
		if u.OktaUserID == oktaID {
			return u, nil
		}
	}
	return nil, rauth.ErrNotFound
}

// Service Accounts

func (r *MemAuthRepo) GetServiceAccount(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) (*auth.ServiceAccount, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if sa, ok := r.serviceAccounts[id]; ok {
		return sa, nil
	}
	return nil, rauth.ErrNotFound
}

func (r *MemAuthRepo) ListServiceAccountOrigins(
	ctx context.Context, l *logger.Logger, saID uuid.UUID, filterSet filtertypes.FilterSet,
) ([]*auth.ServiceAccountOrigin, int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Get origins for this service account
	origins := r.saOrigins[saID]

	// 1. Apply filters
	evaluator := filters.NewMemoryFilterEvaluatorWithTypeHint(
		reflect.TypeOf(auth.ServiceAccountOrigin{}),
	)
	var filteredOrigins []*auth.ServiceAccountOrigin

	for _, origin := range origins {
		// If no filters, include all origins
		if filterSet.IsEmpty() {
			filteredOrigins = append(filteredOrigins, origin)
			continue
		}

		matches, err := evaluator.Evaluate(origin, &filterSet)
		if err != nil {
			l.Error(
				"Error filtering origin, continuing with other origins",
				slog.String("origin_id", origin.ID.String()),
				slog.Any("error", err),
			)
			continue
		}
		if matches {
			filteredOrigins = append(filteredOrigins, origin)
		}
	}

	totalCount := len(filteredOrigins)

	// 2. Apply sorting (with created_at as default)
	_ = memoryfilters.SortByField(filteredOrigins, filterSet.Sort, "created_at")

	// 3. Apply pagination
	filteredOrigins = memoryfilters.ApplyPagination(filteredOrigins, filterSet.Pagination)

	return filteredOrigins, totalCount, nil
}

// Tokens

func (r *MemAuthRepo) CreateToken(
	ctx context.Context, l *logger.Logger, token *auth.ApiToken,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.tokens[token.ID] = token
	r.tokensByHash[token.TokenHash] = token
	return nil
}

func (r *MemAuthRepo) GetTokenByHash(
	ctx context.Context, l *logger.Logger, hash string,
) (*auth.ApiToken, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if t, ok := r.tokensByHash[hash]; ok {
		return t, nil
	}
	return nil, rauth.ErrNotFound
}

func (r *MemAuthRepo) GetToken(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) (*auth.ApiToken, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if t, ok := r.tokens[id]; ok {
		return t, nil
	}
	return nil, rauth.ErrNotFound
}

func (r *MemAuthRepo) UpdateTokenLastUsed(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if t, ok := r.tokens[id]; ok {
		now := timeutil.Now()
		t.LastUsedAt = &now
		return nil
	}
	return rauth.ErrNotFound
}

func (r *MemAuthRepo) RevokeToken(ctx context.Context, l *logger.Logger, id uuid.UUID) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if t, ok := r.tokens[id]; ok {
		t.Status = auth.TokenStatusRevoked
		t.UpdatedAt = timeutil.Now()
		return nil
	}
	return rauth.ErrNotFound
}

func (r *MemAuthRepo) ListAllTokens(
	ctx context.Context, l *logger.Logger, filterSet filtertypes.FilterSet,
) ([]*auth.ApiToken, int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// 1. Apply filters
	evaluator := filters.NewMemoryFilterEvaluatorWithTypeHint(
		reflect.TypeOf(auth.ApiToken{}),
	)
	var filteredTokens []*auth.ApiToken

	for _, token := range r.tokens {
		// If no filters, include all tokens
		if filterSet.IsEmpty() {
			filteredTokens = append(filteredTokens, token)
			continue
		}

		matches, err := evaluator.Evaluate(token, &filterSet)
		if err != nil {
			l.Error(
				"Error filtering token, continuing with other tokens",
				slog.String("token_id", token.ID.String()),
				slog.Any("error", err),
			)
			continue
		}
		if matches {
			filteredTokens = append(filteredTokens, token)
		}
	}

	totalCount := len(filteredTokens)

	// 2. Apply sorting (with created_at descending as default)
	_ = memoryfilters.SortByField(filteredTokens, filterSet.Sort, "created_at")

	// 3. Apply pagination
	filteredTokens = memoryfilters.ApplyPagination(filteredTokens, filterSet.Pagination)

	return filteredTokens, totalCount, nil
}

// CleanupTokens removes tokens based on status and retention period.
func (r *MemAuthRepo) CleanupTokens(
	ctx context.Context, l *logger.Logger, status auth.TokenStatus, retention time.Duration,
) (int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	cutoff := timeutil.Now().Add(-retention)
	var toDelete []uuid.UUID

	for id, token := range r.tokens {
		if token.Status != status {
			continue
		}

		switch status {
		case auth.TokenStatusValid:
			if token.ExpiresAt.Before(cutoff) {
				toDelete = append(toDelete, id)
			}
		case auth.TokenStatusRevoked:
			if token.UpdatedAt.Before(cutoff) {
				toDelete = append(toDelete, id)
			}
		}
	}

	for _, id := range toDelete {
		tokenHash := r.tokens[id].TokenHash
		delete(r.tokens, id)
		delete(r.tokensByHash, tokenHash)
	}

	return len(toDelete), nil
}

// Permissions

func (r *MemAuthRepo) GetUserPermissionsFromGroups(
	ctx context.Context, l *logger.Logger, userID uuid.UUID,
) ([]*auth.GroupPermission, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Get user's group memberships
	var userGroupNames []string
	for _, members := range r.groupMembers {
		for _, m := range members {
			if m.UserID == userID {
				if g, ok := r.groups[m.GroupID]; ok {
					userGroupNames = append(userGroupNames, g.DisplayName)
				}
			}
		}
	}

	if len(userGroupNames) == 0 {
		return nil, nil
	}

	// Build set for O(1) lookups
	groupSet := make(map[string]struct{}, len(userGroupNames))
	for _, g := range userGroupNames {
		groupSet[g] = struct{}{}
	}

	// Find matching permissions and deduplicate by provider+account+permission
	seen := make(map[string]struct{})
	var result []*auth.GroupPermission
	for _, m := range r.groupPermissions {
		if _, ok := groupSet[m.GroupName]; ok {
			key := m.Provider + ":" + m.Account + ":" + m.Permission
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				result = append(result, m)
			}
		}
	}

	// Sort by provider, account, permission
	slices.SortFunc(result, func(a, b *auth.GroupPermission) int {
		if a.Provider != b.Provider {
			if a.Provider < b.Provider {
				return -1
			}
			return 1
		}
		if a.Account != b.Account {
			if a.Account < b.Account {
				return -1
			}
			return 1
		}
		if a.Permission < b.Permission {
			return -1
		}
		if a.Permission > b.Permission {
			return 1
		}
		return 0
	})

	return result, nil
}

func (r *MemAuthRepo) ListServiceAccountPermissions(
	ctx context.Context, l *logger.Logger, saID uuid.UUID, filterSet filtertypes.FilterSet,
) ([]*auth.ServiceAccountPermission, int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Get all permissions for this service account
	allPerms := r.serviceAccountPermissions[saID]
	if allPerms == nil {
		allPerms = []*auth.ServiceAccountPermission{}
	}

	// 1. Apply filters
	evaluator := filters.NewMemoryFilterEvaluatorWithTypeHint(
		reflect.TypeOf(auth.ServiceAccountPermission{}),
	)
	var filteredPerms []*auth.ServiceAccountPermission

	for _, perm := range allPerms {
		// If no filters, include all permissions
		if filterSet.IsEmpty() {
			filteredPerms = append(filteredPerms, perm)
			continue
		}

		matches, err := evaluator.Evaluate(perm, &filterSet)
		if err != nil {
			l.Error(
				"Error filtering service account permission, continuing with other permissions",
				slog.String("permission_id", perm.ID.String()),
				slog.Any("error", err),
			)
			continue
		}
		if matches {
			filteredPerms = append(filteredPerms, perm)
		}
	}

	totalCount := len(filteredPerms)

	// 2. Apply sorting (with provider as default)
	_ = memoryfilters.SortByField(filteredPerms, filterSet.Sort, "provider")

	// 3. Apply pagination
	filteredPerms = memoryfilters.ApplyPagination(filteredPerms, filterSet.Pagination)

	return filteredPerms, totalCount, nil
}

func (r *MemAuthRepo) GetServiceAccountPermission(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) (*auth.ServiceAccountPermission, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	for _, perms := range r.serviceAccountPermissions {
		for _, p := range perms {
			if p.ID == id {
				return p, nil
			}
		}
	}
	return nil, rauth.ErrNotFound
}

// GetStatistics returns current counts for metrics gauges.
func (r *MemAuthRepo) GetStatistics(
	ctx context.Context, l *logger.Logger,
) (*rauth.Statistics, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	stats := &rauth.Statistics{
		TokensByTypeAndStatus: make(map[string]map[string]int),
	}

	// Count users by active status
	for _, user := range r.users {
		if user.Active {
			stats.UsersActive++
		} else {
			stats.UsersInactive++
		}
	}

	// Count groups
	stats.Groups = len(r.groups)

	// Count service accounts by enabled status
	for _, sa := range r.serviceAccounts {
		if sa.Enabled {
			stats.ServiceAccountsEnabled++
		} else {
			stats.ServiceAccountsDisabled++
		}
	}

	// Count tokens by type and status
	for _, token := range r.tokens {
		tokenType := string(token.TokenType)
		status := string(token.Status)

		if stats.TokensByTypeAndStatus[tokenType] == nil {
			stats.TokensByTypeAndStatus[tokenType] = make(map[string]int)
		}
		stats.TokensByTypeAndStatus[tokenType][status]++
	}

	return stats, nil
}
