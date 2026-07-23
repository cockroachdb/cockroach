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
	"github.com/cockroachdb/errors"
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

func (r *MemAuthRepo) CreateUser(ctx context.Context, l *logger.Logger, user *auth.User) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Check for duplicate Okta ID
	for _, u := range r.users {
		if u.OktaUserID == user.OktaUserID {
			return errors.New("user with this Okta ID already exists")
		}
	}

	r.users[user.ID] = user
	return nil
}

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

func (r *MemAuthRepo) GetUserByEmail(
	ctx context.Context, l *logger.Logger, email string,
) (*auth.User, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	for _, u := range r.users {
		if u.Email == email {
			return u, nil
		}
	}
	return nil, rauth.ErrNotFound
}

func (r *MemAuthRepo) UpdateUser(ctx context.Context, l *logger.Logger, user *auth.User) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if _, ok := r.users[user.ID]; !ok {
		return errors.New("user not found")
	}
	r.users[user.ID] = user
	return nil
}

func (r *MemAuthRepo) ListUsers(
	ctx context.Context, l *logger.Logger, filterSet filtertypes.FilterSet,
) ([]*auth.User, int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// 1. Apply filters
	evaluator := filters.NewMemoryFilterEvaluatorWithTypeHint(
		reflect.TypeOf(auth.User{}),
	)
	var filteredUsers []*auth.User

	for _, user := range r.users {
		// If no filters, include all users
		if filterSet.IsEmpty() {
			filteredUsers = append(filteredUsers, user)
			continue
		}

		matches, err := evaluator.Evaluate(user, &filterSet)
		if err != nil {
			l.Error(
				"Error filtering user, continuing with other users",
				slog.String("user_id", user.ID.String()),
				slog.Any("error", err),
			)
			continue
		}
		if matches {
			filteredUsers = append(filteredUsers, user)
		}
	}

	totalCount := len(filteredUsers)

	// 2. Apply sorting (with email as default)
	_ = memoryfilters.SortByField(filteredUsers, filterSet.Sort, "email")

	// 3. Apply pagination
	filteredUsers = memoryfilters.ApplyPagination(filteredUsers, filterSet.Pagination)

	return filteredUsers, totalCount, nil
}

func (r *MemAuthRepo) DeleteUser(ctx context.Context, l *logger.Logger, id uuid.UUID) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Cascade delete: remove group memberships (ON DELETE CASCADE on group_members.user_id)
	for groupID, members := range r.groupMembers {
		var remaining []*auth.GroupMember
		for _, m := range members {
			if m.UserID != id {
				remaining = append(remaining, m)
			}
		}
		r.groupMembers[groupID] = remaining
	}

	// Cascade delete: remove user's tokens (ON DELETE CASCADE on api_tokens.user_id)
	for tokenID, token := range r.tokens {
		if token.UserID != nil && *token.UserID == id {
			delete(r.tokensByHash, token.TokenHash)
			delete(r.tokens, tokenID)
		}
	}

	// Cascade delete: remove service accounts delegated from this user
	// (ON DELETE CASCADE on service_accounts.delegated_from)
	for saID, sa := range r.serviceAccounts {
		if sa.DelegatedFrom != nil && *sa.DelegatedFrom == id {
			// Also delete SA's tokens, origins, and permissions
			for tokenID, token := range r.tokens {
				if token.ServiceAccountID != nil && *token.ServiceAccountID == saID {
					delete(r.tokensByHash, token.TokenHash)
					delete(r.tokens, tokenID)
				}
			}
			delete(r.saOrigins, saID)
			delete(r.serviceAccountPermissions, saID)
			delete(r.serviceAccounts, saID)
		}
	}

	delete(r.users, id)
	return nil
}

func (r *MemAuthRepo) DeactivateUser(ctx context.Context, l *logger.Logger, id uuid.UUID) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	// 1. Get the user
	user, ok := r.users[id]
	if !ok {
		return rauth.ErrNotFound
	}

	// 2. Set user to inactive
	user.Active = false
	user.UpdatedAt = timeutil.Now()

	// 3. Revoke all user's tokens
	now := timeutil.Now()
	for _, t := range r.tokens {
		if t.UserID != nil && *t.UserID == id && t.Status == auth.TokenStatusValid {
			t.Status = auth.TokenStatusRevoked
			t.UpdatedAt = now
		}
	}

	return nil
}

// Service Accounts

func (r *MemAuthRepo) CreateServiceAccount(
	ctx context.Context, l *logger.Logger, sa *auth.ServiceAccount,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.serviceAccounts[sa.ID] = sa
	return nil
}

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

func (r *MemAuthRepo) UpdateServiceAccount(
	ctx context.Context, l *logger.Logger, sa *auth.ServiceAccount,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if _, ok := r.serviceAccounts[sa.ID]; !ok {
		return rauth.ErrNotFound
	}
	r.serviceAccounts[sa.ID] = sa
	return nil
}

func (r *MemAuthRepo) ListServiceAccounts(
	ctx context.Context, l *logger.Logger, filterSet filtertypes.FilterSet,
) ([]*auth.ServiceAccount, int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// 1. Apply filters
	evaluator := filters.NewMemoryFilterEvaluatorWithTypeHint(
		reflect.TypeOf(auth.ServiceAccount{}),
	)
	var filteredSAs []*auth.ServiceAccount

	for _, sa := range r.serviceAccounts {
		// If no filters, include all service accounts
		if filterSet.IsEmpty() {
			filteredSAs = append(filteredSAs, sa)
			continue
		}

		matches, err := evaluator.Evaluate(sa, &filterSet)
		if err != nil {
			l.Error(
				"Error filtering service account, continuing with other service accounts",
				slog.String("service_account_id", sa.ID.String()),
				slog.Any("error", err),
			)
			continue
		}
		if matches {
			filteredSAs = append(filteredSAs, sa)
		}
	}

	totalCount := len(filteredSAs)

	// 2. Apply sorting (with name as default)
	_ = memoryfilters.SortByField(filteredSAs, filterSet.Sort, "name")

	// 3. Apply pagination
	filteredSAs = memoryfilters.ApplyPagination(filteredSAs, filterSet.Pagination)

	return filteredSAs, totalCount, nil
}

func (r *MemAuthRepo) DeleteServiceAccount(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Cascade delete: remove SA's tokens (ON DELETE CASCADE on api_tokens.service_account_id)
	for tokenID, token := range r.tokens {
		if token.ServiceAccountID != nil && *token.ServiceAccountID == id {
			delete(r.tokensByHash, token.TokenHash)
			delete(r.tokens, tokenID)
		}
	}

	// Cascade delete: remove SA's origins (ON DELETE CASCADE on service_account_origins.service_account_id)
	delete(r.saOrigins, id)

	// Cascade delete: remove SA's permissions (ON DELETE CASCADE on service_account_permissions.service_account_id)
	delete(r.serviceAccountPermissions, id)

	delete(r.serviceAccounts, id)
	return nil
}

// Service Account Origins

func (r *MemAuthRepo) AddServiceAccountOrigin(
	ctx context.Context, l *logger.Logger, origin *auth.ServiceAccountOrigin,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Check for duplicate CIDR (matches database UNIQUE constraint)
	for _, existing := range r.saOrigins[origin.ServiceAccountID] {
		if existing.CIDR == origin.CIDR {
			return errors.New("origin with this CIDR already exists for this service account")
		}
	}

	r.saOrigins[origin.ServiceAccountID] = append(r.saOrigins[origin.ServiceAccountID], origin)
	return nil
}

func (r *MemAuthRepo) RemoveServiceAccountOrigin(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	for saID, origins := range r.saOrigins {
		for i, origin := range origins {
			if origin.ID == id {
				r.saOrigins[saID] = append(origins[:i], origins[i+1:]...)
				return nil
			}
		}
	}
	return nil
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

	// Find matching permissions and deduplicate by scope+permission
	seen := make(map[string]struct{})
	var result []*auth.GroupPermission
	for _, m := range r.groupPermissions {
		if _, ok := groupSet[m.GroupName]; ok {
			key := m.Scope + "\x00" + m.Permission
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				result = append(result, m)
			}
		}
	}

	// Sort by scope, permission
	slices.SortFunc(result, func(a, b *auth.GroupPermission) int {
		if a.Scope != b.Scope {
			if a.Scope < b.Scope {
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

// Permissions

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

	// 2. Apply sorting (with scope as default)
	_ = memoryfilters.SortByField(filteredPerms, filterSet.Sort, "scope")

	// 3. Apply pagination
	filteredPerms = memoryfilters.ApplyPagination(filteredPerms, filterSet.Pagination)

	return filteredPerms, totalCount, nil
}

func (r *MemAuthRepo) UpdateServiceAccountPermissions(
	ctx context.Context,
	l *logger.Logger,
	saID uuid.UUID,
	permissions []*auth.ServiceAccountPermission,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.serviceAccountPermissions[saID] = permissions
	return nil
}

func (r *MemAuthRepo) AddServiceAccountPermission(
	ctx context.Context, l *logger.Logger, perm *auth.ServiceAccountPermission,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.serviceAccountPermissions[perm.ServiceAccountID] = append(r.serviceAccountPermissions[perm.ServiceAccountID], perm)
	return nil
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

func (r *MemAuthRepo) RemoveServiceAccountPermission(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	for saID, perms := range r.serviceAccountPermissions {
		for i, p := range perms {
			if p.ID == id {
				r.serviceAccountPermissions[saID] = append(perms[:i], perms[i+1:]...)
				return nil
			}
		}
	}
	return rauth.ErrNotFound
}

// Groups (SCIM-managed)

func (r *MemAuthRepo) GetGroup(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) (*auth.Group, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if g, ok := r.groups[id]; ok {
		return g, nil
	}
	return nil, rauth.ErrNotFound
}

func (r *MemAuthRepo) GetGroupByExternalID(
	ctx context.Context, l *logger.Logger, externalID string,
) (*auth.Group, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	for _, g := range r.groups {
		if g.ExternalID != nil && *g.ExternalID == externalID {
			return g, nil
		}
	}
	return nil, rauth.ErrNotFound
}

func (r *MemAuthRepo) UpdateGroup(ctx context.Context, l *logger.Logger, group *auth.Group) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if _, ok := r.groups[group.ID]; !ok {
		return rauth.ErrNotFound
	}
	r.groups[group.ID] = group
	return nil
}

func (r *MemAuthRepo) DeleteGroup(ctx context.Context, l *logger.Logger, id uuid.UUID) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if _, ok := r.groups[id]; !ok {
		return rauth.ErrNotFound
	}
	delete(r.groups, id)
	delete(r.groupMembers, id)
	return nil
}

func (r *MemAuthRepo) ListGroups(
	ctx context.Context, l *logger.Logger, filterSet filtertypes.FilterSet,
) ([]*auth.Group, int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// 1. Apply filters
	evaluator := filters.NewMemoryFilterEvaluatorWithTypeHint(
		reflect.TypeOf(auth.Group{}),
	)
	var filteredGroups []*auth.Group

	for _, group := range r.groups {
		// If no filters, include all groups
		if filterSet.IsEmpty() {
			filteredGroups = append(filteredGroups, group)
			continue
		}

		matches, err := evaluator.Evaluate(group, &filterSet)
		if err != nil {
			l.Error(
				"Error filtering group, continuing with other groups",
				slog.String("group_id", group.ID.String()),
				slog.Any("error", err),
			)
			continue
		}
		if matches {
			filteredGroups = append(filteredGroups, group)
		}
	}

	totalCount := len(filteredGroups)

	// 2. Apply sorting (with display_name as default)
	_ = memoryfilters.SortByField(filteredGroups, filterSet.Sort, "display_name")

	// 3. Apply pagination
	filteredGroups = memoryfilters.ApplyPagination(filteredGroups, filterSet.Pagination)

	return filteredGroups, totalCount, nil
}

// Group Members

func (r *MemAuthRepo) GetGroupMembers(
	ctx context.Context, l *logger.Logger, groupID uuid.UUID, filterSet filtertypes.FilterSet,
) ([]*auth.GroupMember, int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Get members for this group
	members := r.groupMembers[groupID]

	// 1. Apply filters
	evaluator := filters.NewMemoryFilterEvaluatorWithTypeHint(
		reflect.TypeOf(auth.GroupMember{}),
	)
	var filteredMembers []*auth.GroupMember

	for _, member := range members {
		// If no filters, include all groups
		if filterSet.IsEmpty() {
			filteredMembers = append(filteredMembers, member)
			continue
		}

		matches, err := evaluator.Evaluate(member, &filterSet)
		if err != nil {
			l.Error(
				"Error filtering group members, continuing with other members",
				slog.String("group_id", groupID.String()),
				slog.String("group_member_id", member.ID.String()),
				slog.Any("error", err),
			)
			continue
		}
		if matches {
			filteredMembers = append(filteredMembers, member)
		}
	}

	totalCount := len(filteredMembers)

	// 2. Apply sorting (with display_name as default)
	_ = memoryfilters.SortByField(filteredMembers, filterSet.Sort, "created_at")

	// 3. Apply pagination
	filteredMembers = memoryfilters.ApplyPagination(filteredMembers, filterSet.Pagination)

	return filteredMembers, totalCount, nil
}

// CreateGroupWithMembers atomically creates a group and adds initial members.
func (r *MemAuthRepo) CreateGroupWithMembers(
	ctx context.Context, l *logger.Logger, group *auth.Group, memberIDs []uuid.UUID,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Create the group
	r.groups[group.ID] = group

	// Add members
	for _, userID := range memberIDs {
		member := &auth.GroupMember{
			ID:        uuid.MakeV4(),
			GroupID:   group.ID,
			UserID:    userID,
			CreatedAt: timeutil.Now(),
		}
		r.groupMembers[group.ID] = append(r.groupMembers[group.ID], member)
	}

	return nil
}

// UpdateGroupWithMembers atomically updates a group and applies member operations.
func (r *MemAuthRepo) UpdateGroupWithMembers(
	ctx context.Context, l *logger.Logger, group *auth.Group, operations []rauth.GroupMemberOperation,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Verify group exists
	if _, ok := r.groups[group.ID]; !ok {
		return rauth.ErrNotFound
	}

	// Update the group if provided
	if group != nil {
		r.groups[group.ID] = group
	}

	// Apply member operations
	for _, op := range operations {
		switch op.Op {
		case "add":
			alreadyMember := false
			for _, m := range r.groupMembers[group.ID] {
				if m.UserID == op.UserID {
					alreadyMember = true
					break
				}
			}
			if !alreadyMember {
				member := &auth.GroupMember{
					ID:        uuid.MakeV4(),
					GroupID:   group.ID,
					UserID:    op.UserID,
					CreatedAt: timeutil.Now(),
				}
				r.groupMembers[group.ID] = append(r.groupMembers[group.ID], member)
			}
		case "remove":
			for i, m := range r.groupMembers[group.ID] {
				if m.UserID == op.UserID {
					r.groupMembers[group.ID] = append(r.groupMembers[group.ID][:i], r.groupMembers[group.ID][i+1:]...)
					break
				}
			}
		default:
			return errors.Newf("unsupported operation: %s", op.Op)
		}
	}

	return nil
}

// Group Permissions

func (r *MemAuthRepo) ListGroupPermissions(
	ctx context.Context, l *logger.Logger, filterSet filtertypes.FilterSet,
) ([]*auth.GroupPermission, int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// 1. Apply filters
	evaluator := filters.NewMemoryFilterEvaluatorWithTypeHint(
		reflect.TypeOf(auth.GroupPermission{}),
	)
	var filteredPerms []*auth.GroupPermission

	for _, perm := range r.groupPermissions {
		// If no filters, include all permissions
		if filterSet.IsEmpty() {
			filteredPerms = append(filteredPerms, perm)
			continue
		}

		matches, err := evaluator.Evaluate(perm, &filterSet)
		if err != nil {
			l.Error(
				"Error filtering group permission, continuing with other permissions",
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

	// 2. Apply sorting (with group_name as default)
	_ = memoryfilters.SortByField(filteredPerms, filterSet.Sort, "group_name")

	// 3. Apply pagination
	filteredPerms = memoryfilters.ApplyPagination(filteredPerms, filterSet.Pagination)

	return filteredPerms, totalCount, nil
}

func (r *MemAuthRepo) GetPermissionsForGroups(
	ctx context.Context, l *logger.Logger, groups []string,
) ([]*auth.GroupPermission, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if len(groups) == 0 {
		return nil, nil
	}

	// Build a set for O(1) lookups
	groupSet := make(map[string]struct{}, len(groups))
	for _, g := range groups {
		groupSet[g] = struct{}{}
	}

	var result []*auth.GroupPermission
	for _, m := range r.groupPermissions {
		if _, ok := groupSet[m.GroupName]; ok {
			result = append(result, m)
		}
	}

	// Sort by group_name, scope, permission
	slices.SortFunc(result, func(a, b *auth.GroupPermission) int {
		if a.GroupName != b.GroupName {
			if a.GroupName < b.GroupName {
				return -1
			}
			return 1
		}
		if a.Scope != b.Scope {
			if a.Scope < b.Scope {
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

func (r *MemAuthRepo) CreateGroupPermission(
	ctx context.Context, l *logger.Logger, perm *auth.GroupPermission,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Check for duplicates
	for _, m := range r.groupPermissions {
		if m.ID == perm.ID {
			return errors.New("group permission with this ID already exists")
		}
	}

	r.groupPermissions = append(r.groupPermissions, perm)
	return nil
}

func (r *MemAuthRepo) UpdateGroupPermission(
	ctx context.Context, l *logger.Logger, perm *auth.GroupPermission,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	for i, m := range r.groupPermissions {
		if m.ID == perm.ID {
			r.groupPermissions[i] = perm
			return nil
		}
	}

	return rauth.ErrNotFound
}

func (r *MemAuthRepo) DeleteGroupPermission(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	for i, m := range r.groupPermissions {
		if m.ID == id {
			r.groupPermissions = append(r.groupPermissions[:i], r.groupPermissions[i+1:]...)
			return nil
		}
	}

	return rauth.ErrNotFound
}

func (r *MemAuthRepo) ReplaceGroupPermissions(
	ctx context.Context, l *logger.Logger, perms []*auth.GroupPermission,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Replace all permissions atomically
	r.groupPermissions = make([]*auth.GroupPermission, len(perms))
	copy(r.groupPermissions, perms)
	return nil
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
