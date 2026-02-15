// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memory

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	rauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMemoryRepository_UserCRUD tests the full lifecycle of user operations
func TestMemoryRepository_UserCRUD(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")
	ctx := context.Background()

	// Create
	user := &auth.User{
		ID:         uuid.MakeV4(),
		Email:      "test@example.com",
		OktaUserID: "okta-123",
		FullName:   "Test User",
		Active:     true,
	}
	err := repo.CreateUser(ctx, l, user)
	require.NoError(t, err)

	// Get by ID
	fetched, err := repo.GetUser(ctx, l, user.ID)
	require.NoError(t, err)
	assert.Equal(t, user.Email, fetched.Email)
	assert.Equal(t, user.FullName, fetched.FullName)

	// Get by OktaID
	fetched, err = repo.GetUserByOktaID(ctx, l, "okta-123")
	require.NoError(t, err)
	assert.Equal(t, user.ID, fetched.ID)

	// Get by Email
	fetched, err = repo.GetUserByEmail(ctx, l, "test@example.com")
	require.NoError(t, err)
	assert.Equal(t, user.ID, fetched.ID)

	// Update
	user.FullName = "Updated Name"
	err = repo.UpdateUser(ctx, l, user)
	require.NoError(t, err)

	fetched, err = repo.GetUser(ctx, l, user.ID)
	require.NoError(t, err)
	assert.Equal(t, "Updated Name", fetched.FullName)

	// Delete
	err = repo.DeleteUser(ctx, l, user.ID)
	require.NoError(t, err)

	_, err = repo.GetUser(ctx, l, user.ID)
	assert.True(t, errors.Is(err, rauth.ErrNotFound))
}

// TestMemoryRepository_GetUser_NotFound tests that getting a non-existent user returns ErrNotFound
func TestMemoryRepository_GetUser_NotFound(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")

	_, err := repo.GetUser(context.Background(), l, uuid.MakeV4())
	assert.True(t, errors.Is(err, rauth.ErrNotFound))
}

// TestMemoryRepository_CreateUser_DuplicateOktaID tests that duplicate Okta IDs are rejected
func TestMemoryRepository_CreateUser_DuplicateOktaID(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")
	ctx := context.Background()

	user1 := &auth.User{
		ID:         uuid.MakeV4(),
		Email:      "user1@example.com",
		OktaUserID: "okta-123",
	}
	err := repo.CreateUser(ctx, l, user1)
	require.NoError(t, err)

	user2 := &auth.User{
		ID:         uuid.MakeV4(),
		Email:      "user2@example.com",
		OktaUserID: "okta-123", // Same Okta ID
	}
	err = repo.CreateUser(ctx, l, user2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Okta ID already exists")
}

// TestMemoryRepository_ListUsers tests listing users with filters
func TestMemoryRepository_ListUsers(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")
	ctx := context.Background()

	// Create test users
	for i := 0; i < 5; i++ {
		user := &auth.User{
			ID:         uuid.MakeV4(),
			Email:      "user" + string(rune('0'+i)) + "@example.com",
			OktaUserID: "okta-" + string(rune('0'+i)),
			Active:     i%2 == 0, // Alternate active status
		}
		require.NoError(t, repo.CreateUser(ctx, l, user))
	}

	// List all users
	users, count, err := repo.ListUsers(ctx, l, *filters.NewFilterSet())
	require.NoError(t, err)
	assert.Equal(t, 5, count)
	assert.Len(t, users, 5)
}

// TestMemoryRepository_TokenOperations tests the full lifecycle of token operations
func TestMemoryRepository_TokenOperations(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")
	ctx := context.Background()

	userID := uuid.MakeV4()
	tokenID := uuid.MakeV4()

	token := &auth.ApiToken{
		ID:          tokenID,
		TokenType:   auth.TokenTypeUser,
		UserID:      &userID,
		TokenHash:   "hash123",
		TokenSuffix: "rp$user$1$****abcd1234",
		Status:      auth.TokenStatusValid,
		CreatedAt:   timeutil.Now(),
		ExpiresAt:   timeutil.Now().Add(7 * 24 * time.Hour),
	}

	// Create
	err := repo.CreateToken(ctx, l, token)
	require.NoError(t, err)

	// Get by hash
	fetched, err := repo.GetTokenByHash(ctx, l, "hash123")
	require.NoError(t, err)
	assert.Equal(t, tokenID, fetched.ID)

	// Get by ID
	fetched, err = repo.GetToken(ctx, l, tokenID)
	require.NoError(t, err)
	assert.Equal(t, auth.TokenStatusValid, fetched.Status)

	// Update last used
	err = repo.UpdateTokenLastUsed(ctx, l, tokenID)
	require.NoError(t, err)

	fetched, err = repo.GetToken(ctx, l, tokenID)
	require.NoError(t, err)
	assert.NotNil(t, fetched.LastUsedAt)

	// Revoke
	err = repo.RevokeToken(ctx, l, tokenID)
	require.NoError(t, err)

	fetched, err = repo.GetToken(ctx, l, tokenID)
	require.NoError(t, err)
	assert.Equal(t, auth.TokenStatusRevoked, fetched.Status)
}

// TestMemoryRepository_GetTokenByHash_NotFound tests that getting a non-existent token returns ErrNotFound
func TestMemoryRepository_GetTokenByHash_NotFound(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")

	_, err := repo.GetTokenByHash(context.Background(), l, "nonexistent-hash")
	assert.True(t, errors.Is(err, rauth.ErrNotFound))
}

// TestMemoryRepository_ServiceAccountWithOrigins tests service account and origin operations
func TestMemoryRepository_ServiceAccountWithOrigins(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")
	ctx := context.Background()

	saID := uuid.MakeV4()
	sa := &auth.ServiceAccount{
		ID:          saID,
		Name:        "ci-bot",
		Description: "CI automation",
		Enabled:     true,
	}

	// Create SA
	err := repo.CreateServiceAccount(ctx, l, sa)
	require.NoError(t, err)

	// Get SA
	fetched, err := repo.GetServiceAccount(ctx, l, saID)
	require.NoError(t, err)
	assert.Equal(t, "ci-bot", fetched.Name)

	// Add origins
	origin1 := &auth.ServiceAccountOrigin{
		ID:               uuid.MakeV4(),
		ServiceAccountID: saID,
		CIDR:             "10.0.0.0/8",
	}
	err = repo.AddServiceAccountOrigin(ctx, l, origin1)
	require.NoError(t, err)

	origin2 := &auth.ServiceAccountOrigin{
		ID:               uuid.MakeV4(),
		ServiceAccountID: saID,
		CIDR:             "192.168.1.0/24",
	}
	err = repo.AddServiceAccountOrigin(ctx, l, origin2)
	require.NoError(t, err)

	// List origins
	origins, totalCount, err := repo.ListServiceAccountOrigins(ctx, l, saID, *filters.NewFilterSet())
	require.NoError(t, err)
	assert.Len(t, origins, 2)
	assert.Equal(t, 2, totalCount)

	// Remove origin
	err = repo.RemoveServiceAccountOrigin(ctx, l, origin1.ID)
	require.NoError(t, err)

	origins, totalCount, err = repo.ListServiceAccountOrigins(ctx, l, saID, *filters.NewFilterSet())
	require.NoError(t, err)
	assert.Len(t, origins, 1)
	assert.Equal(t, 1, totalCount)
	assert.Equal(t, "192.168.1.0/24", origins[0].CIDR)
}

// TestMemoryRepository_GetServiceAccount_NotFound tests that getting a non-existent SA returns ErrNotFound
func TestMemoryRepository_GetServiceAccount_NotFound(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")

	_, err := repo.GetServiceAccount(context.Background(), l, uuid.MakeV4())
	assert.True(t, errors.Is(err, rauth.ErrNotFound))
}

// TestMemoryRepository_AddServiceAccountOrigin_DuplicateCIDR tests that duplicate CIDRs are rejected
func TestMemoryRepository_AddServiceAccountOrigin_DuplicateCIDR(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")
	ctx := context.Background()

	saID := uuid.MakeV4()
	sa := &auth.ServiceAccount{ID: saID, Name: "test-sa", Enabled: true}
	require.NoError(t, repo.CreateServiceAccount(ctx, l, sa))

	origin1 := &auth.ServiceAccountOrigin{
		ID:               uuid.MakeV4(),
		ServiceAccountID: saID,
		CIDR:             "10.0.0.0/8",
	}
	require.NoError(t, repo.AddServiceAccountOrigin(ctx, l, origin1))

	origin2 := &auth.ServiceAccountOrigin{
		ID:               uuid.MakeV4(),
		ServiceAccountID: saID,
		CIDR:             "10.0.0.0/8", // Same CIDR
	}
	err := repo.AddServiceAccountOrigin(ctx, l, origin2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CIDR already exists")
}

// TestMemoryRepository_ServiceAccountPermissions tests service account permission operations
func TestMemoryRepository_ServiceAccountPermissions(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")
	ctx := context.Background()

	saID := uuid.MakeV4()
	sa := &auth.ServiceAccount{ID: saID, Name: "test-sa", Enabled: true}
	require.NoError(t, repo.CreateServiceAccount(ctx, l, sa))

	// Add permission
	perm := &auth.ServiceAccountPermission{
		ID:               uuid.MakeV4(),
		ServiceAccountID: saID,
		Scope:            "gcp-project1",
		Permission:       "clusters:create",
	}
	err := repo.AddServiceAccountPermission(ctx, l, perm)
	require.NoError(t, err)

	// Get permissions
	perms, totalCount, err := repo.ListServiceAccountPermissions(ctx, l, saID, *filters.NewFilterSet())
	require.NoError(t, err)
	assert.Len(t, perms, 1)
	assert.Equal(t, 1, totalCount)
	assert.Equal(t, "clusters:create", perms[0].Permission)

	// Get single permission
	fetched, err := repo.GetServiceAccountPermission(ctx, l, perm.ID)
	require.NoError(t, err)
	assert.Equal(t, perm.ID, fetched.ID)

	// Remove permission
	err = repo.RemoveServiceAccountPermission(ctx, l, perm.ID)
	require.NoError(t, err)

	perms, totalCount, err = repo.ListServiceAccountPermissions(ctx, l, saID, *filters.NewFilterSet())
	require.NoError(t, err)
	assert.Len(t, perms, 0)
	assert.Equal(t, 0, totalCount)
}

// TestMemoryRepository_DeactivateUser tests that deactivating a user revokes their tokens
func TestMemoryRepository_DeactivateUser(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")
	ctx := context.Background()

	userID := uuid.MakeV4()
	user := &auth.User{
		ID:         userID,
		Email:      "test@example.com",
		OktaUserID: "okta-123",
		Active:     true,
	}
	require.NoError(t, repo.CreateUser(ctx, l, user))

	// Create some tokens
	token1 := &auth.ApiToken{
		ID:        uuid.MakeV4(),
		UserID:    &userID,
		TokenHash: "hash1",
		TokenType: auth.TokenTypeUser,
		Status:    auth.TokenStatusValid,
		ExpiresAt: timeutil.Now().Add(24 * time.Hour),
	}
	require.NoError(t, repo.CreateToken(ctx, l, token1))

	token2 := &auth.ApiToken{
		ID:        uuid.MakeV4(),
		UserID:    &userID,
		TokenHash: "hash2",
		TokenType: auth.TokenTypeUser,
		Status:    auth.TokenStatusValid,
		ExpiresAt: timeutil.Now().Add(24 * time.Hour),
	}
	require.NoError(t, repo.CreateToken(ctx, l, token2))

	// Deactivate user
	err := repo.DeactivateUser(ctx, l, userID)
	require.NoError(t, err)

	// Verify user is deactivated
	fetched, err := repo.GetUser(ctx, l, userID)
	require.NoError(t, err)
	assert.False(t, fetched.Active)

	// Verify tokens are revoked
	t1, err := repo.GetToken(ctx, l, token1.ID)
	require.NoError(t, err)
	assert.Equal(t, auth.TokenStatusRevoked, t1.Status)

	t2, err := repo.GetToken(ctx, l, token2.ID)
	require.NoError(t, err)
	assert.Equal(t, auth.TokenStatusRevoked, t2.Status)
}

// TestMemoryRepository_GroupCRUD tests the full lifecycle of group operations
func TestMemoryRepository_GroupCRUD(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")
	ctx := context.Background()

	groupID := uuid.MakeV4()
	groupExternalID := "eng-team"
	group := &auth.Group{
		ID:          groupID,
		ExternalID:  &groupExternalID,
		DisplayName: "Engineering Team",
	}

	// Create
	err := repo.CreateGroupWithMembers(ctx, l, group, []uuid.UUID{})
	require.NoError(t, err)

	// Get by ID
	fetched, err := repo.GetGroup(ctx, l, groupID)
	require.NoError(t, err)
	assert.Equal(t, "Engineering Team", fetched.DisplayName)

	// Get by ExternalID
	fetched, err = repo.GetGroupByExternalID(ctx, l, "eng-team")
	require.NoError(t, err)
	assert.Equal(t, groupID, fetched.ID)

	// Update
	group.DisplayName = "Engineering"
	err = repo.UpdateGroup(ctx, l, group)
	require.NoError(t, err)

	fetched, err = repo.GetGroup(ctx, l, groupID)
	require.NoError(t, err)
	assert.Equal(t, "Engineering", fetched.DisplayName)

	// Delete
	err = repo.DeleteGroup(ctx, l, groupID)
	require.NoError(t, err)

	_, err = repo.GetGroup(ctx, l, groupID)
	assert.True(t, errors.Is(err, rauth.ErrNotFound))
}

// TestMemoryRepository_GroupMembers tests group membership operations
func TestMemoryRepository_GroupMembers(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")
	ctx := context.Background()

	// Create users
	user1ID := uuid.MakeV4()
	user2ID := uuid.MakeV4()

	// Create group with initial members
	groupID := uuid.MakeV4()
	groupExternalID := "eng-team"
	group := &auth.Group{
		ID:          groupID,
		ExternalID:  &groupExternalID,
		DisplayName: "Engineering",
	}
	err := repo.CreateGroupWithMembers(ctx, l, group, []uuid.UUID{user1ID})
	require.NoError(t, err)

	// Get members
	members, count, err := repo.GetGroupMembers(ctx, l, groupID, *filters.NewFilterSet())
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Len(t, members, 1)
	assert.Equal(t, user1ID, members[0].UserID)

	// Patch members (add user2)
	err = repo.UpdateGroupWithMembers(ctx, l, group, []rauth.GroupMemberOperation{
		{Op: "add", UserID: user2ID},
	})
	require.NoError(t, err)

	members, count, err = repo.GetGroupMembers(ctx, l, groupID, *filters.NewFilterSet())
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	assert.Len(t, members, 2)

	// Patch members (remove user1)
	err = repo.UpdateGroupWithMembers(ctx, l, group, []rauth.GroupMemberOperation{
		{Op: "remove", UserID: user1ID},
	})
	require.NoError(t, err)

	members, count, err = repo.GetGroupMembers(ctx, l, groupID, *filters.NewFilterSet())
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Equal(t, user2ID, members[0].UserID)
}

// TestMemoryRepository_GetUserPermissionsFromGroups tests permission resolution through groups
func TestMemoryRepository_GetUserPermissionsFromGroups(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")
	ctx := context.Background()

	// Create user
	userID := uuid.MakeV4()
	user := &auth.User{ID: userID, Email: "test@example.com", OktaUserID: "okta-123", Active: true}
	require.NoError(t, repo.CreateUser(ctx, l, user))

	// Create group and add user
	groupID := uuid.MakeV4()
	groupExternalID := "eng-team"
	group := &auth.Group{ID: groupID, ExternalID: &groupExternalID, DisplayName: "Engineering"}
	require.NoError(t, repo.CreateGroupWithMembers(ctx, l, group, []uuid.UUID{userID}))

	// Create group permissions
	perm1 := &auth.GroupPermission{
		ID:         uuid.MakeV4(),
		GroupName:  "Engineering",
		Scope:      "gcp-project1",
		Permission: "clusters:create",
	}
	require.NoError(t, repo.CreateGroupPermission(ctx, l, perm1))

	perm2 := &auth.GroupPermission{
		ID:         uuid.MakeV4(),
		GroupName:  "Engineering",
		Scope:      "gcp-project1",
		Permission: "clusters:delete",
	}
	require.NoError(t, repo.CreateGroupPermission(ctx, l, perm2))

	// Get user permissions from groups
	perms, err := repo.GetUserPermissionsFromGroups(ctx, l, userID)
	require.NoError(t, err)
	assert.Len(t, perms, 2)
}

// TestMemoryRepository_GroupPermissionsCRUD tests group permission operations
func TestMemoryRepository_GroupPermissionsCRUD(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")
	ctx := context.Background()

	perm := &auth.GroupPermission{
		ID:         uuid.MakeV4(),
		GroupName:  "admin",
		Scope:      "*",
		Permission: "*",
	}

	// Create
	err := repo.CreateGroupPermission(ctx, l, perm)
	require.NoError(t, err)

	// Get all
	perms, totalCount, err := repo.ListGroupPermissions(ctx, l, *filters.NewFilterSet())
	require.NoError(t, err)
	assert.Len(t, perms, 1)
	assert.Equal(t, 1, totalCount)

	// Update
	perm.Permission = "clusters:*"
	err = repo.UpdateGroupPermission(ctx, l, perm)
	require.NoError(t, err)

	perms, totalCount, err = repo.ListGroupPermissions(ctx, l, *filters.NewFilterSet())
	require.NoError(t, err)
	assert.Equal(t, 1, totalCount)
	assert.Equal(t, "clusters:*", perms[0].Permission)

	// Delete
	err = repo.DeleteGroupPermission(ctx, l, perm.ID)
	require.NoError(t, err)

	perms, totalCount, err = repo.ListGroupPermissions(ctx, l, *filters.NewFilterSet())
	require.NoError(t, err)
	assert.Len(t, perms, 0)
	assert.Equal(t, 0, totalCount)
}

// TestMemoryRepository_ReplaceGroupPermissions tests atomic replacement of all group permissions
func TestMemoryRepository_ReplaceGroupPermissions(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")
	ctx := context.Background()

	// Create initial permissions
	perm1 := &auth.GroupPermission{ID: uuid.MakeV4(), GroupName: "admin", Permission: "old-perm"}
	require.NoError(t, repo.CreateGroupPermission(ctx, l, perm1))

	perms, totalCount, err := repo.ListGroupPermissions(ctx, l, *filters.NewFilterSet())
	require.NoError(t, err)
	assert.Len(t, perms, 1)
	assert.Equal(t, 1, totalCount)

	// Replace with new permissions
	newPerms := []*auth.GroupPermission{
		{ID: uuid.MakeV4(), GroupName: "eng", Permission: "new-perm-1"},
		{ID: uuid.MakeV4(), GroupName: "ops", Permission: "new-perm-2"},
	}
	err = repo.ReplaceGroupPermissions(ctx, l, newPerms)
	require.NoError(t, err)

	perms, totalCount, err = repo.ListGroupPermissions(ctx, l, *filters.NewFilterSet())
	require.NoError(t, err)
	assert.Len(t, perms, 2)
	assert.Equal(t, 2, totalCount)
}

// TestMemoryRepository_GetStatistics tests the statistics gathering functionality
func TestMemoryRepository_GetStatistics(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")
	ctx := context.Background()
	now := timeutil.Now()

	// Create test data
	// Users: 2 active, 1 inactive
	user1 := &auth.User{
		ID:         uuid.MakeV4(),
		Email:      "active1@example.com",
		OktaUserID: "okta-active-1",
		Active:     true,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	user2 := &auth.User{
		ID:         uuid.MakeV4(),
		Email:      "active2@example.com",
		OktaUserID: "okta-active-2",
		Active:     true,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	user3 := &auth.User{
		ID:         uuid.MakeV4(),
		Email:      "inactive@example.com",
		OktaUserID: "okta-inactive",
		Active:     false,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	require.NoError(t, repo.CreateUser(ctx, l, user1))
	require.NoError(t, repo.CreateUser(ctx, l, user2))
	require.NoError(t, repo.CreateUser(ctx, l, user3))

	// Groups: 2 groups
	group1ExternalID := "ext-group-1"
	group1 := &auth.Group{
		ID:          uuid.MakeV4(),
		ExternalID:  &group1ExternalID,
		DisplayName: "Group One",
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	group2ExternalID := "ext-group-2"
	group2 := &auth.Group{
		ID:          uuid.MakeV4(),
		ExternalID:  &group2ExternalID,
		DisplayName: "Group Two",
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	require.NoError(t, repo.CreateGroupWithMembers(ctx, l, group1, []uuid.UUID{}))
	require.NoError(t, repo.CreateGroupWithMembers(ctx, l, group2, []uuid.UUID{}))

	// Service Accounts: 1 enabled, 1 disabled
	sa1 := &auth.ServiceAccount{
		ID:        uuid.MakeV4(),
		Name:      "enabled-sa",
		Enabled:   true,
		CreatedAt: now,
		UpdatedAt: now,
	}
	sa2 := &auth.ServiceAccount{
		ID:        uuid.MakeV4(),
		Name:      "disabled-sa",
		Enabled:   false,
		CreatedAt: now,
		UpdatedAt: now,
	}
	require.NoError(t, repo.CreateServiceAccount(ctx, l, sa1))
	require.NoError(t, repo.CreateServiceAccount(ctx, l, sa2))

	// Tokens: various types and statuses
	// 2 user tokens (1 valid, 1 revoked)
	token1 := &auth.ApiToken{
		ID:          uuid.MakeV4(),
		TokenHash:   "hash1",
		TokenSuffix: "suf1",
		TokenType:   auth.TokenTypeUser,
		UserID:      &user1.ID,
		Status:      auth.TokenStatusValid,
		CreatedAt:   now,
		UpdatedAt:   now,
		ExpiresAt:   now.Add(24 * time.Hour),
	}
	token2 := &auth.ApiToken{
		ID:          uuid.MakeV4(),
		TokenHash:   "hash2",
		TokenSuffix: "suf2",
		TokenType:   auth.TokenTypeUser,
		UserID:      &user2.ID,
		Status:      auth.TokenStatusRevoked,
		CreatedAt:   now,
		UpdatedAt:   now,
		ExpiresAt:   now.Add(24 * time.Hour),
	}
	// 1 service account token (valid)
	token3 := &auth.ApiToken{
		ID:               uuid.MakeV4(),
		TokenHash:        "hash3",
		TokenSuffix:      "suf3",
		TokenType:        auth.TokenTypeServiceAccount,
		ServiceAccountID: &sa1.ID,
		Status:           auth.TokenStatusValid,
		CreatedAt:        now,
		UpdatedAt:        now,
		ExpiresAt:        now.Add(24 * time.Hour),
	}
	require.NoError(t, repo.CreateToken(ctx, l, token1))
	require.NoError(t, repo.CreateToken(ctx, l, token2))
	require.NoError(t, repo.CreateToken(ctx, l, token3))

	// Get statistics
	stats, err := repo.GetStatistics(ctx, l)
	require.NoError(t, err)
	require.NotNil(t, stats)

	// Verify user counts
	assert.Equal(t, 2, stats.UsersActive, "should have 2 active users")
	assert.Equal(t, 1, stats.UsersInactive, "should have 1 inactive user")

	// Verify group count
	assert.Equal(t, 2, stats.Groups, "should have 2 groups")

	// Verify service account counts
	assert.Equal(t, 1, stats.ServiceAccountsEnabled, "should have 1 enabled service account")
	assert.Equal(t, 1, stats.ServiceAccountsDisabled, "should have 1 disabled service account")

	// Verify token counts by type and status
	require.NotNil(t, stats.TokensByTypeAndStatus)
	assert.Equal(t, 1, stats.TokensByTypeAndStatus[string(auth.TokenTypeUser)][string(auth.TokenStatusValid)],
		"should have 1 valid user token")
	assert.Equal(t, 1, stats.TokensByTypeAndStatus[string(auth.TokenTypeUser)][string(auth.TokenStatusRevoked)],
		"should have 1 revoked user token")
	assert.Equal(t, 1, stats.TokensByTypeAndStatus[string(auth.TokenTypeServiceAccount)][string(auth.TokenStatusValid)],
		"should have 1 valid service account token")
}

// TestMemoryRepository_GetStatistics_Empty tests statistics with empty repository
func TestMemoryRepository_GetStatistics_Empty(t *testing.T) {
	repo := NewAuthRepository()
	l := logger.NewLogger("error")
	ctx := context.Background()

	stats, err := repo.GetStatistics(ctx, l)
	require.NoError(t, err)
	require.NotNil(t, stats)

	assert.Equal(t, 0, stats.UsersActive)
	assert.Equal(t, 0, stats.UsersInactive)
	assert.Equal(t, 0, stats.Groups)
	assert.Equal(t, 0, stats.ServiceAccountsEnabled)
	assert.Equal(t, 0, stats.ServiceAccountsDisabled)
	require.NotNil(t, stats.TokensByTypeAndStatus)
	assert.Empty(t, stats.TokensByTypeAndStatus)
}
