// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	rauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/database"
	crdbmigrator "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/database/cockroachdb"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	rpconfig "github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

var (
	testDBOnce         sync.Once
	testDB             *gosql.DB
	testDBErr          error
	authMigrationsOnce sync.Once
	authMigrationsErr  error
)

// isTableNotExistsError checks if the error is a "relation does not exist" error.
func isTableNotExistsError(err error) bool {
	if err == nil {
		return false
	}
	// PostgreSQL/CockroachDB error code 42P01 is "undefined_table"
	return strings.Contains(err.Error(), "does not exist") ||
		strings.Contains(err.Error(), "42P01")
}

// getTestDB returns a shared database connection for all tests.
// The connection is created once and reused across all tests.
func getTestDB() (*gosql.DB, error) {
	testDBOnce.Do(func() {
		testDB, testDBErr = database.NewConnection(context.Background(), database.ConnectionConfig{
			URL: fmt.Sprintf(
				"postgresql://%s@localhost:%d/defaultdb?sslmode=require",
				"roachprod:cockroachdb",
				rpconfig.DefaultOpenPortStart,
			),
			MaxConns:    10,
			MaxIdleTime: 300, // 5 minutes
		})
	})
	return testDB, testDBErr
}

func createAuthTables(t *testing.T, db *gosql.DB) {
	t.Helper()

	const repoName = "auth_integration_tests"
	ctx := context.Background()
	authMigrationsOnce.Do(func() {
		// Reset schema for test isolation - drop all auth tables and migration history.
		// This ensures tests run against the current consolidated schema.
		// Drop tables in dependency order (children first, then parents).
		dropStatements := []string{
			"DROP TABLE IF EXISTS group_members CASCADE",
			"DROP TABLE IF EXISTS group_permissions CASCADE",
			"DROP TABLE IF EXISTS api_tokens CASCADE",
			"DROP TABLE IF EXISTS service_account_permissions CASCADE",
			"DROP TABLE IF EXISTS service_account_origins CASCADE",
			"DROP TABLE IF EXISTS service_accounts CASCADE",
			"DROP TABLE IF EXISTS groups CASCADE",
			"DROP TABLE IF EXISTS users CASCADE",
		}
		for _, stmt := range dropStatements {
			if _, err := db.Exec(stmt); err != nil {
				authMigrationsErr = fmt.Errorf("failed to drop table: %s: %w", stmt, err)
				return
			}
		}
		// Clear migration history for this repository
		if _, err := db.Exec("DELETE FROM schema_migrations WHERE repository = $1", repoName); err != nil {
			// Ignore error if schema_migrations doesn't exist yet
			if !isTableNotExistsError(err) {
				authMigrationsErr = fmt.Errorf("failed to clear migration history: %w", err)
				return
			}
		}

		authMigrationsErr = database.RunMigrationsForRepository(
			ctx,
			logger.DefaultLogger,
			db,
			repoName,
			GetAuthMigrations(),
			crdbmigrator.NewMigrator(),
		)
	})
	require.NoError(t, authMigrationsErr)

	// Use DELETE instead of TRUNCATE - faster for small test datasets
	// Delete in FK dependency order (children first, then parents)
	_, err := db.Exec(`
		DELETE FROM group_members;
		DELETE FROM group_permissions;
		DELETE FROM api_tokens;
		DELETE FROM service_account_permissions;
		DELETE FROM service_account_origins;
		DELETE FROM service_accounts;
		DELETE FROM groups;
		DELETE FROM users;
	`)
	require.NoError(t, err)
}

// TestCRDBAuthRepo_Users groups all user-related tests.
func TestCRDBAuthRepo_Users(t *testing.T) {
	db, err := getTestDB()
	if err != nil {
		skip.IgnoreLint(t, "Database not configured for testing")
	}
	createAuthTables(t, db)
	repo := NewAuthRepository(db)
	ctx := context.Background()

	t.Run("CRUD", func(t *testing.T) {
		now := timeutil.Now()
		user := &auth.User{
			ID:          uuid.MakeV4(),
			OktaUserID:  "okta-crud-" + uuid.MakeV4().Short().String(),
			Email:       "crud-" + uuid.MakeV4().Short().String() + "@example.com",
			SlackHandle: "testuser",
			FullName:    "Test User",
			Active:      true,
			CreatedAt:   now,
			UpdatedAt:   now,
		}

		err := repo.CreateUser(ctx, logger.DefaultLogger, user)
		require.NoError(t, err)

		// Get by ID
		retrievedUser, err := repo.GetUser(ctx, logger.DefaultLogger, user.ID)
		require.NoError(t, err)
		require.Equal(t, user.ID, retrievedUser.ID)
		require.Equal(t, user.OktaUserID, retrievedUser.OktaUserID)
		require.Equal(t, user.Email, retrievedUser.Email)
		require.Equal(t, user.SlackHandle, retrievedUser.SlackHandle)
		require.Equal(t, user.FullName, retrievedUser.FullName)
		require.Equal(t, user.Active, retrievedUser.Active)

		// Get by Okta ID
		byOkta, err := repo.GetUserByOktaID(ctx, logger.DefaultLogger, user.OktaUserID)
		require.NoError(t, err)
		require.Equal(t, user.ID, byOkta.ID)

		// Get by Email
		byEmail, err := repo.GetUserByEmail(ctx, logger.DefaultLogger, user.Email)
		require.NoError(t, err)
		require.Equal(t, user.ID, byEmail.ID)

		// Update user
		user.Email = "updated-" + uuid.MakeV4().Short().String() + "@example.com"
		user.Active = false
		user.UpdatedAt = timeutil.Now()
		err = repo.UpdateUser(ctx, logger.DefaultLogger, user)
		require.NoError(t, err)

		// Verify update
		updated, err := repo.GetUser(ctx, logger.DefaultLogger, user.ID)
		require.NoError(t, err)
		require.Equal(t, user.Email, updated.Email)
		require.False(t, updated.Active)

		// Delete user
		err = repo.DeleteUser(ctx, logger.DefaultLogger, user.ID)
		require.NoError(t, err)

		// Verify deletion
		_, err = repo.GetUser(ctx, logger.DefaultLogger, user.ID)
		require.ErrorIs(t, err, rauth.ErrNotFound)
	})

	t.Run("Deactivate", func(t *testing.T) {
		now := timeutil.Now()
		user := &auth.User{
			ID:         uuid.MakeV4(),
			OktaUserID: "okta-deactivate-" + uuid.MakeV4().Short().String(),
			Email:      "deactivate-" + uuid.MakeV4().Short().String() + "@example.com",
			Active:     true,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		require.NoError(t, repo.CreateUser(ctx, logger.DefaultLogger, user))

		// Create a token for the user
		token := &auth.ApiToken{
			ID:          uuid.MakeV4(),
			TokenHash:   "hash-deactivate-" + uuid.MakeV4().Short().String(),
			TokenSuffix: "12345678",
			TokenType:   auth.TokenTypeUser,
			UserID:      &user.ID,
			Status:      auth.TokenStatusValid,
			CreatedAt:   now,
			ExpiresAt:   now.Add(24 * time.Hour),
		}
		require.NoError(t, repo.CreateToken(ctx, logger.DefaultLogger, token))

		// Deactivate user (should set active=false and revoke tokens)
		err := repo.DeactivateUser(ctx, logger.DefaultLogger, user.ID)
		require.NoError(t, err)

		// Verify user is inactive
		deactivated, err := repo.GetUser(ctx, logger.DefaultLogger, user.ID)
		require.NoError(t, err)
		require.False(t, deactivated.Active)

		// Verify token is revoked
		revokedToken, err := repo.GetToken(ctx, logger.DefaultLogger, token.ID)
		require.NoError(t, err)
		require.Equal(t, auth.TokenStatusRevoked, revokedToken.Status)

		// Test deactivating non-existent user
		err = repo.DeactivateUser(ctx, logger.DefaultLogger, uuid.MakeV4())
		require.ErrorIs(t, err, rauth.ErrNotFound)
	})

	t.Run("ListWithFilters", func(t *testing.T) {
		// Create test users with unique identifiers
		now := timeutil.Now()
		prefix := uuid.MakeV4().Short().String()
		users := []*auth.User{
			{
				ID:         uuid.MakeV4(),
				OktaUserID: "okta-filter-1-" + prefix,
				Email:      "alpha-" + prefix + "@example.com",
				Active:     true,
				CreatedAt:  now,
				UpdatedAt:  now,
			},
			{
				ID:         uuid.MakeV4(),
				OktaUserID: "okta-filter-2-" + prefix,
				Email:      "beta-" + prefix + "@example.com",
				Active:     false,
				CreatedAt:  now,
				UpdatedAt:  now,
			},
			{
				ID:         uuid.MakeV4(),
				OktaUserID: "okta-filter-3-" + prefix,
				Email:      "gamma-" + prefix + "@example.com",
				Active:     true,
				CreatedAt:  now,
				UpdatedAt:  now,
			},
		}

		for _, u := range users {
			require.NoError(t, repo.CreateUser(ctx, logger.DefaultLogger, u))
		}

		// Test filter by active status - count active users we just created
		activeFilter := *filters.NewFilterSet().AddFilter("Active", filtertypes.OpEqual, true)
		activeUsers, _, err := repo.ListUsers(ctx, logger.DefaultLogger, activeFilter)
		require.NoError(t, err)
		// At least 2 active users from this test
		require.GreaterOrEqual(t, len(activeUsers), 2)
	})
}

// TestCRDBAuthRepo_ServiceAccounts groups all service account-related tests.
func TestCRDBAuthRepo_ServiceAccounts(t *testing.T) {
	db, err := getTestDB()
	if err != nil {
		skip.IgnoreLint(t, "Database not configured for testing")
	}
	createAuthTables(t, db)
	repo := NewAuthRepository(db)
	ctx := context.Background()

	t.Run("CRUD", func(t *testing.T) {
		now := timeutil.Now()
		creator := &auth.User{
			ID:         uuid.MakeV4(),
			OktaUserID: "okta-sa-creator-" + uuid.MakeV4().Short().String(),
			Email:      "sa-creator-" + uuid.MakeV4().Short().String() + "@example.com",
			Active:     true,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		require.NoError(t, repo.CreateUser(ctx, logger.DefaultLogger, creator))

		sa := &auth.ServiceAccount{
			ID:          uuid.MakeV4(),
			Name:        "test-sa-" + uuid.MakeV4().Short().String(),
			Description: "Test service account",
			CreatedBy:   &creator.ID,
			CreatedAt:   now,
			UpdatedAt:   now,
			Enabled:     true,
		}
		require.NoError(t, repo.CreateServiceAccount(ctx, logger.DefaultLogger, sa))

		// Get service account
		retrieved, err := repo.GetServiceAccount(ctx, logger.DefaultLogger, sa.ID)
		require.NoError(t, err)
		require.Equal(t, sa.ID, retrieved.ID)
		require.Equal(t, sa.Name, retrieved.Name)
		require.Equal(t, sa.Description, retrieved.Description)
		require.NotNil(t, retrieved.CreatedBy)
		require.Equal(t, creator.ID, *retrieved.CreatedBy)
		require.True(t, retrieved.Enabled)

		// Update service account
		sa.Name = "updated-sa-" + uuid.MakeV4().Short().String()
		sa.Enabled = false
		sa.UpdatedAt = timeutil.Now()
		err = repo.UpdateServiceAccount(ctx, logger.DefaultLogger, sa)
		require.NoError(t, err)

		// Verify update
		updated, err := repo.GetServiceAccount(ctx, logger.DefaultLogger, sa.ID)
		require.NoError(t, err)
		require.Equal(t, sa.Name, updated.Name)
		require.False(t, updated.Enabled)

		// Delete service account
		err = repo.DeleteServiceAccount(ctx, logger.DefaultLogger, sa.ID)
		require.NoError(t, err)

		// Verify deletion
		_, err = repo.GetServiceAccount(ctx, logger.DefaultLogger, sa.ID)
		require.ErrorIs(t, err, rauth.ErrNotFound)
	})

	t.Run("Origins", func(t *testing.T) {
		now := timeutil.Now()
		sa := &auth.ServiceAccount{
			ID:        uuid.MakeV4(),
			Name:      "test-sa-origins-" + uuid.MakeV4().Short().String(),
			CreatedAt: now,
			UpdatedAt: now,
			Enabled:   true,
		}
		require.NoError(t, repo.CreateServiceAccount(ctx, logger.DefaultLogger, sa))

		// Add origins
		origin1 := &auth.ServiceAccountOrigin{
			ID:               uuid.MakeV4(),
			ServiceAccountID: sa.ID,
			CIDR:             "192.168.1.0/24",
			Description:      "Office network",
			CreatedAt:        now,
			UpdatedAt:        now,
		}
		err := repo.AddServiceAccountOrigin(ctx, logger.DefaultLogger, origin1)
		require.NoError(t, err)

		origin2 := &auth.ServiceAccountOrigin{
			ID:               uuid.MakeV4(),
			ServiceAccountID: sa.ID,
			CIDR:             "10.0.0.0/8",
			Description:      "Internal network",
			CreatedAt:        now,
			UpdatedAt:        now,
		}
		err = repo.AddServiceAccountOrigin(ctx, logger.DefaultLogger, origin2)
		require.NoError(t, err)

		// List origins
		origins, totalCount, err := repo.ListServiceAccountOrigins(ctx, logger.DefaultLogger, sa.ID, *filters.NewFilterSet())
		require.NoError(t, err)
		require.Len(t, origins, 2)
		require.Equal(t, 2, totalCount)

		// Remove an origin
		err = repo.RemoveServiceAccountOrigin(ctx, logger.DefaultLogger, origin1.ID)
		require.NoError(t, err)

		// Verify removal
		remainingOrigins, totalCount, err := repo.ListServiceAccountOrigins(ctx, logger.DefaultLogger, sa.ID, *filters.NewFilterSet())
		require.NoError(t, err)
		require.Len(t, remainingOrigins, 1)
		require.Equal(t, 1, totalCount)
		require.Equal(t, origin2.CIDR, remainingOrigins[0].CIDR)
	})

	t.Run("Permissions", func(t *testing.T) {
		now := timeutil.Now()
		sa := &auth.ServiceAccount{
			ID:        uuid.MakeV4(),
			Name:      "test-sa-perms-" + uuid.MakeV4().Short().String(),
			CreatedAt: now,
			UpdatedAt: now,
			Enabled:   true,
		}
		require.NoError(t, repo.CreateServiceAccount(ctx, logger.DefaultLogger, sa))

		// Add a permission
		perm1 := &auth.ServiceAccountPermission{
			ID:               uuid.MakeV4(),
			ServiceAccountID: sa.ID,
			Scope:            "gcp-engineering",
			Permission:       "admin",
			CreatedAt:        now,
		}
		err := repo.AddServiceAccountPermission(ctx, logger.DefaultLogger, perm1)
		require.NoError(t, err)

		// Get permission by ID
		retrieved, err := repo.GetServiceAccountPermission(ctx, logger.DefaultLogger, perm1.ID)
		require.NoError(t, err)
		require.Equal(t, perm1.ID, retrieved.ID)
		require.Equal(t, perm1.Scope, retrieved.Scope)
		require.Equal(t, perm1.Permission, retrieved.Permission)

		// Get all permissions for service account
		perms, totalCount, err := repo.ListServiceAccountPermissions(ctx, logger.DefaultLogger, sa.ID, *filters.NewFilterSet())
		require.NoError(t, err)
		require.Len(t, perms, 1)
		require.Equal(t, 1, totalCount)
		require.Equal(t, perm1.ID, perms[0].ID)

		// Update permissions (replace all)
		newPerms := []*auth.ServiceAccountPermission{
			{
				ID:               uuid.MakeV4(),
				ServiceAccountID: sa.ID,
				Scope:            "aws-staging",
				Permission:       "read",
				CreatedAt:        now,
			},
			{
				ID:               uuid.MakeV4(),
				ServiceAccountID: sa.ID,
				Scope:            "gcp-engineering",
				Permission:       "write",
				CreatedAt:        now,
			},
		}
		err = repo.UpdateServiceAccountPermissions(ctx, logger.DefaultLogger, sa.ID, newPerms)
		require.NoError(t, err)

		// Verify update replaced old permissions
		updatedPerms, totalCount, err := repo.ListServiceAccountPermissions(ctx, logger.DefaultLogger, sa.ID, *filters.NewFilterSet())
		require.NoError(t, err)
		require.Len(t, updatedPerms, 2)
		require.Equal(t, 2, totalCount)

		// Remove a permission
		err = repo.RemoveServiceAccountPermission(ctx, logger.DefaultLogger, newPerms[0].ID)
		require.NoError(t, err)

		// Verify removal
		remainingPerms, totalCount, err := repo.ListServiceAccountPermissions(ctx, logger.DefaultLogger, sa.ID, *filters.NewFilterSet())
		require.NoError(t, err)
		require.Len(t, remainingPerms, 1)
		require.Equal(t, 1, totalCount)
		require.Equal(t, newPerms[1].ID, remainingPerms[0].ID)

		// Test removing non-existent permission
		err = repo.RemoveServiceAccountPermission(ctx, logger.DefaultLogger, uuid.MakeV4())
		require.ErrorIs(t, err, rauth.ErrNotFound)
	})
}

// TestCRDBAuthRepo_Tokens groups all token-related tests.
func TestCRDBAuthRepo_Tokens(t *testing.T) {
	db, err := getTestDB()
	if err != nil {
		skip.IgnoreLint(t, "Database not configured for testing")
	}
	createAuthTables(t, db)
	repo := NewAuthRepository(db)
	ctx := context.Background()

	t.Run("CRUD", func(t *testing.T) {
		now := timeutil.Now()
		user := &auth.User{
			ID:         uuid.MakeV4(),
			OktaUserID: "okta-token-" + uuid.MakeV4().Short().String(),
			Email:      "token-" + uuid.MakeV4().Short().String() + "@example.com",
			Active:     true,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		require.NoError(t, repo.CreateUser(ctx, logger.DefaultLogger, user))

		tokenHash := "hash-" + uuid.MakeV4().Short().String()
		token := &auth.ApiToken{
			ID:          uuid.MakeV4(),
			TokenHash:   tokenHash,
			TokenSuffix: "abc123xy",
			TokenType:   auth.TokenTypeUser,
			UserID:      &user.ID,
			Status:      auth.TokenStatusValid,
			CreatedAt:   now,
			ExpiresAt:   now.Add(24 * time.Hour),
		}
		err := repo.CreateToken(ctx, logger.DefaultLogger, token)
		require.NoError(t, err)

		// Get token by hash
		byHash, err := repo.GetTokenByHash(ctx, logger.DefaultLogger, tokenHash)
		require.NoError(t, err)
		require.Equal(t, token.ID, byHash.ID)
		require.Equal(t, token.TokenHash, byHash.TokenHash)
		require.Equal(t, token.TokenSuffix, byHash.TokenSuffix)
		require.Equal(t, token.Status, byHash.Status)

		// Get token by ID
		byID, err := repo.GetToken(ctx, logger.DefaultLogger, token.ID)
		require.NoError(t, err)
		require.Equal(t, token.ID, byID.ID)

		// Update last used
		time.Sleep(10 * time.Millisecond) // Ensure time difference
		err = repo.UpdateTokenLastUsed(ctx, logger.DefaultLogger, token.ID)
		require.NoError(t, err)

		// Verify last_used_at was updated
		updated, err := repo.GetToken(ctx, logger.DefaultLogger, token.ID)
		require.NoError(t, err)
		require.NotNil(t, updated.LastUsedAt)

		// Revoke token
		err = repo.RevokeToken(ctx, logger.DefaultLogger, token.ID)
		require.NoError(t, err)

		// Verify token was revoked
		revoked, err := repo.GetToken(ctx, logger.DefaultLogger, token.ID)
		require.NoError(t, err)
		require.Equal(t, auth.TokenStatusRevoked, revoked.Status)

		// Test get non-existent token
		_, err = repo.GetTokenByHash(ctx, logger.DefaultLogger, "non-existent-hash")
		require.ErrorIs(t, err, rauth.ErrNotFound)
	})

	t.Run("ListAll", func(t *testing.T) {
		now := timeutil.Now()
		user := &auth.User{
			ID:         uuid.MakeV4(),
			OktaUserID: "okta-list-tokens-" + uuid.MakeV4().Short().String(),
			Email:      "listtokens-" + uuid.MakeV4().Short().String() + "@example.com",
			Active:     true,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		require.NoError(t, repo.CreateUser(ctx, logger.DefaultLogger, user))

		// Create multiple tokens
		tokens := []*auth.ApiToken{
			{
				ID:          uuid.MakeV4(),
				TokenHash:   "hash-list-1-" + uuid.MakeV4().Short().String(),
				TokenSuffix: "suffix1x",
				TokenType:   auth.TokenTypeUser,
				UserID:      &user.ID,
				Status:      auth.TokenStatusValid,
				CreatedAt:   now,
				ExpiresAt:   now.Add(24 * time.Hour),
			},
			{
				ID:          uuid.MakeV4(),
				TokenHash:   "hash-list-2-" + uuid.MakeV4().Short().String(),
				TokenSuffix: "suffix2x",
				TokenType:   auth.TokenTypeUser,
				UserID:      &user.ID,
				Status:      auth.TokenStatusRevoked,
				CreatedAt:   now.Add(-1 * time.Hour),
				ExpiresAt:   now.Add(23 * time.Hour),
			},
		}

		for _, tok := range tokens {
			require.NoError(t, repo.CreateToken(ctx, logger.DefaultLogger, tok))
		}

		// List all tokens - should have at least our 2
		emptyFilter := *filters.NewFilterSet()
		allTokens, count, err := repo.ListAllTokens(ctx, logger.DefaultLogger, emptyFilter)
		require.NoError(t, err)
		require.GreaterOrEqual(t, count, 2)
		require.GreaterOrEqual(t, len(allTokens), 2)
	})
}

// TestCRDBAuthRepo_Groups groups all group-related tests.
func TestCRDBAuthRepo_Groups(t *testing.T) {
	db, err := getTestDB()
	if err != nil {
		skip.IgnoreLint(t, "Database not configured for testing")
	}
	createAuthTables(t, db)
	repo := NewAuthRepository(db)
	ctx := context.Background()

	t.Run("CRUD", func(t *testing.T) {
		now := timeutil.Now()
		externalID := "okta-group-" + uuid.MakeV4().Short().String()
		group := &auth.Group{
			ID:          uuid.MakeV4(),
			ExternalID:  &externalID,
			DisplayName: "Engineering-" + uuid.MakeV4().Short().String(),
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		err := repo.CreateGroupWithMembers(ctx, logger.DefaultLogger, group, []uuid.UUID{})
		require.NoError(t, err)

		// Get group by ID
		retrieved, err := repo.GetGroup(ctx, logger.DefaultLogger, group.ID)
		require.NoError(t, err)
		require.Equal(t, group.ID, retrieved.ID)
		require.Equal(t, group.ExternalID, retrieved.ExternalID)
		require.Equal(t, group.DisplayName, retrieved.DisplayName)

		// Get group by external ID
		byExternal, err := repo.GetGroupByExternalID(ctx, logger.DefaultLogger, externalID)
		require.NoError(t, err)
		require.Equal(t, group.ID, byExternal.ID)

		// Update group
		group.DisplayName = "Engineering Team-" + uuid.MakeV4().Short().String()
		group.UpdatedAt = timeutil.Now()
		err = repo.UpdateGroup(ctx, logger.DefaultLogger, group)
		require.NoError(t, err)

		// Verify update
		updated, err := repo.GetGroup(ctx, logger.DefaultLogger, group.ID)
		require.NoError(t, err)
		require.Equal(t, group.DisplayName, updated.DisplayName)

		// Delete group
		err = repo.DeleteGroup(ctx, logger.DefaultLogger, group.ID)
		require.NoError(t, err)

		// Verify deletion
		_, err = repo.GetGroup(ctx, logger.DefaultLogger, group.ID)
		require.ErrorIs(t, err, rauth.ErrNotFound)

		// Test updating non-existent group
		err = repo.UpdateGroup(ctx, logger.DefaultLogger, group)
		require.ErrorIs(t, err, rauth.ErrNotFound)

		// Test deleting non-existent group
		err = repo.DeleteGroup(ctx, logger.DefaultLogger, uuid.MakeV4())
		require.ErrorIs(t, err, rauth.ErrNotFound)
	})

	t.Run("Members", func(t *testing.T) {
		now := timeutil.Now()
		user1 := &auth.User{
			ID:         uuid.MakeV4(),
			OktaUserID: "okta-member-1-" + uuid.MakeV4().Short().String(),
			Email:      "member1-" + uuid.MakeV4().Short().String() + "@example.com",
			Active:     true,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		user2 := &auth.User{
			ID:         uuid.MakeV4(),
			OktaUserID: "okta-member-2-" + uuid.MakeV4().Short().String(),
			Email:      "member2-" + uuid.MakeV4().Short().String() + "@example.com",
			Active:     true,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		require.NoError(t, repo.CreateUser(ctx, logger.DefaultLogger, user1))
		require.NoError(t, repo.CreateUser(ctx, logger.DefaultLogger, user2))

		externalId := "okta-group-members-" + uuid.MakeV4().Short().String()
		group := &auth.Group{
			ID:          uuid.MakeV4(),
			ExternalID:  &externalId,
			DisplayName: "Test Group-" + uuid.MakeV4().Short().String(),
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		require.NoError(t, repo.CreateGroupWithMembers(ctx, logger.DefaultLogger, group, []uuid.UUID{}))

		// Add members using UpdateGroupWithMembers
		operations := []rauth.GroupMemberOperation{
			{Op: "add", UserID: user1.ID},
			{Op: "add", UserID: user2.ID},
		}
		err := repo.UpdateGroupWithMembers(ctx, logger.DefaultLogger, group, operations)
		require.NoError(t, err)

		// Get group members
		members, count, err := repo.GetGroupMembers(ctx, logger.DefaultLogger, group.ID, *filters.NewFilterSet())
		require.NoError(t, err)
		require.Equal(t, 2, count)
		require.Len(t, members, 2)

		// Remove a member
		removeOps := []rauth.GroupMemberOperation{
			{Op: "remove", UserID: user1.ID},
		}
		err = repo.UpdateGroupWithMembers(ctx, logger.DefaultLogger, group, removeOps)
		require.NoError(t, err)

		// Verify removal
		remainingMembers, count, err := repo.GetGroupMembers(ctx, logger.DefaultLogger, group.ID, *filters.NewFilterSet())
		require.NoError(t, err)
		require.Equal(t, 1, count)
		require.Len(t, remainingMembers, 1)
		require.Equal(t, user2.ID, remainingMembers[0].UserID)
	})

	t.Run("CreateWithMembers", func(t *testing.T) {
		now := timeutil.Now()
		user1 := &auth.User{
			ID:         uuid.MakeV4(),
			OktaUserID: "okta-atomic-1-" + uuid.MakeV4().Short().String(),
			Email:      "atomic1-" + uuid.MakeV4().Short().String() + "@example.com",
			Active:     true,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		user2 := &auth.User{
			ID:         uuid.MakeV4(),
			OktaUserID: "okta-atomic-2-" + uuid.MakeV4().Short().String(),
			Email:      "atomic2-" + uuid.MakeV4().Short().String() + "@example.com",
			Active:     true,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		require.NoError(t, repo.CreateUser(ctx, logger.DefaultLogger, user1))
		require.NoError(t, repo.CreateUser(ctx, logger.DefaultLogger, user2))

		// Create group with members atomically
		externalId := "okta-atomic-group-" + uuid.MakeV4().Short().String()
		group := &auth.Group{
			ID:          uuid.MakeV4(),
			ExternalID:  &externalId,
			DisplayName: "Atomic Group-" + uuid.MakeV4().Short().String(),
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		memberIDs := []uuid.UUID{user1.ID, user2.ID}
		err := repo.CreateGroupWithMembers(ctx, logger.DefaultLogger, group, memberIDs)
		require.NoError(t, err)

		// Verify group was created
		retrieved, err := repo.GetGroup(ctx, logger.DefaultLogger, group.ID)
		require.NoError(t, err)
		require.Equal(t, group.DisplayName, retrieved.DisplayName)

		// Verify members were added
		members, count, err := repo.GetGroupMembers(ctx, logger.DefaultLogger, group.ID, *filters.NewFilterSet())
		require.NoError(t, err)
		require.Equal(t, 2, count)
		require.Len(t, members, 2)
	})

	t.Run("UpdateWithMembers", func(t *testing.T) {
		now := timeutil.Now()
		user1 := &auth.User{
			ID:         uuid.MakeV4(),
			OktaUserID: "okta-update-1-" + uuid.MakeV4().Short().String(),
			Email:      "update1-" + uuid.MakeV4().Short().String() + "@example.com",
			Active:     true,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		user2 := &auth.User{
			ID:         uuid.MakeV4(),
			OktaUserID: "okta-update-2-" + uuid.MakeV4().Short().String(),
			Email:      "update2-" + uuid.MakeV4().Short().String() + "@example.com",
			Active:     true,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		require.NoError(t, repo.CreateUser(ctx, logger.DefaultLogger, user1))
		require.NoError(t, repo.CreateUser(ctx, logger.DefaultLogger, user2))

		// Create group with one member
		externalId := "okta-update-group-" + uuid.MakeV4().Short().String()
		group := &auth.Group{
			ID:          uuid.MakeV4(),
			ExternalID:  &externalId,
			DisplayName: "Update Group-" + uuid.MakeV4().Short().String(),
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		err := repo.CreateGroupWithMembers(ctx, logger.DefaultLogger, group, []uuid.UUID{user1.ID})
		require.NoError(t, err)

		// Update group and members atomically
		group.DisplayName = "Updated Group-" + uuid.MakeV4().Short().String()
		group.UpdatedAt = timeutil.Now()
		operations := []rauth.GroupMemberOperation{
			{Op: "add", UserID: user2.ID},
			{Op: "remove", UserID: user1.ID},
		}
		err = repo.UpdateGroupWithMembers(ctx, logger.DefaultLogger, group, operations)
		require.NoError(t, err)

		// Verify group was updated
		updated, err := repo.GetGroup(ctx, logger.DefaultLogger, group.ID)
		require.NoError(t, err)
		require.Equal(t, group.DisplayName, updated.DisplayName)

		// Verify members were updated
		members, count, err := repo.GetGroupMembers(ctx, logger.DefaultLogger, group.ID, *filters.NewFilterSet())
		require.NoError(t, err)
		require.Equal(t, 1, count)
		require.Len(t, members, 1)
		require.Equal(t, user2.ID, members[0].UserID)
	})

	t.Run("UserPermissionsFromGroups", func(t *testing.T) {
		now := timeutil.Now()
		user := &auth.User{
			ID:         uuid.MakeV4(),
			OktaUserID: "okta-perms-user-" + uuid.MakeV4().Short().String(),
			Email:      "perms-" + uuid.MakeV4().Short().String() + "@example.com",
			Active:     true,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		require.NoError(t, repo.CreateUser(ctx, logger.DefaultLogger, user))

		// Create groups with unique names
		group1Name := "Engineering-" + uuid.MakeV4().Short().String()
		group1ExternalID := "okta-eng-" + uuid.MakeV4().Short().String()
		group2Name := "Operations-" + uuid.MakeV4().Short().String()
		group2ExternalID := "okta-ops-" + uuid.MakeV4().Short().String()
		group1 := &auth.Group{
			ID:          uuid.MakeV4(),
			ExternalID:  &group1ExternalID,
			DisplayName: group1Name,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		group2 := &auth.Group{
			ID:          uuid.MakeV4(),
			ExternalID:  &group2ExternalID,
			DisplayName: group2Name,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		require.NoError(t, repo.CreateGroupWithMembers(ctx, logger.DefaultLogger, group1, []uuid.UUID{}))
		require.NoError(t, repo.CreateGroupWithMembers(ctx, logger.DefaultLogger, group2, []uuid.UUID{}))

		// Add user to groups
		err := repo.UpdateGroupWithMembers(ctx, logger.DefaultLogger, group1, []rauth.GroupMemberOperation{
			{Op: "add", UserID: user.ID},
		})
		require.NoError(t, err)
		err = repo.UpdateGroupWithMembers(ctx, logger.DefaultLogger, group2, []rauth.GroupMemberOperation{
			{Op: "add", UserID: user.ID},
		})
		require.NoError(t, err)

		// Create group permissions
		scope1 := "gcp-engineering"
		scope2 := "aws-staging"
		perm1 := &auth.GroupPermission{
			ID:         uuid.MakeV4(),
			GroupName:  group1Name,
			Scope:      scope1,
			Permission: "admin",
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		perm2 := &auth.GroupPermission{
			ID:         uuid.MakeV4(),
			GroupName:  group2Name,
			Scope:      scope2,
			Permission: "deploy",
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		// Add duplicate permission from another group (should be deduplicated)
		perm3 := &auth.GroupPermission{
			ID:         uuid.MakeV4(),
			GroupName:  group2Name,
			Scope:      scope1,
			Permission: "admin", // Same scope+permission as perm1
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		require.NoError(t, repo.CreateGroupPermission(ctx, logger.DefaultLogger, perm1))
		require.NoError(t, repo.CreateGroupPermission(ctx, logger.DefaultLogger, perm2))
		require.NoError(t, repo.CreateGroupPermission(ctx, logger.DefaultLogger, perm3))

		// Get user permissions from groups (should deduplicate)
		userPerms, err := repo.GetUserPermissionsFromGroups(ctx, logger.DefaultLogger, user.ID)
		require.NoError(t, err)
		require.Len(t, userPerms, 2) // Deduplicated: only 2 unique (scope, permission) tuples

		// Verify permissions contain expected values
		permSet := make(map[string]bool)
		for _, p := range userPerms {
			key := p.Scope + ":" + p.Permission
			permSet[key] = true
		}
		require.True(t, permSet[scope1+":admin"])
		require.True(t, permSet[scope2+":deploy"])
	})
}

// TestCRDBAuthRepo_GroupPermissions groups all group permission-related tests.
func TestCRDBAuthRepo_GroupPermissions(t *testing.T) {
	db, err := getTestDB()
	if err != nil {
		skip.IgnoreLint(t, "Database not configured for testing")
	}
	createAuthTables(t, db)
	repo := NewAuthRepository(db)
	ctx := context.Background()

	t.Run("CRUD", func(t *testing.T) {
		now := timeutil.Now()
		groupName := "TestGroup-" + uuid.MakeV4().Short().String()
		perm := &auth.GroupPermission{
			ID:         uuid.MakeV4(),
			GroupName:  groupName,
			Scope:      "gcp-engineering",
			Permission: "viewer",
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		err := repo.CreateGroupPermission(ctx, logger.DefaultLogger, perm)
		require.NoError(t, err)

		// Get all group permissions - should include our permission
		allPerms, totalCount, err := repo.ListGroupPermissions(ctx, logger.DefaultLogger, *filters.NewFilterSet())
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(allPerms), 1)
		require.GreaterOrEqual(t, totalCount, 1)

		// Get permissions for specific groups
		groupPerms, err := repo.GetPermissionsForGroups(ctx, logger.DefaultLogger, []string{groupName})
		require.NoError(t, err)
		require.Len(t, groupPerms, 1)
		require.Equal(t, perm.GroupName, groupPerms[0].GroupName)

		// Update permission
		perm.Permission = "admin"
		perm.UpdatedAt = timeutil.Now()
		err = repo.UpdateGroupPermission(ctx, logger.DefaultLogger, perm)
		require.NoError(t, err)

		// Verify update
		updatedPerms, err := repo.GetPermissionsForGroups(ctx, logger.DefaultLogger, []string{groupName})
		require.NoError(t, err)
		require.Len(t, updatedPerms, 1)
		require.Equal(t, "admin", updatedPerms[0].Permission)

		// Delete permission
		err = repo.DeleteGroupPermission(ctx, logger.DefaultLogger, perm.ID)
		require.NoError(t, err)

		// Verify deletion
		remaining, err := repo.GetPermissionsForGroups(ctx, logger.DefaultLogger, []string{groupName})
		require.NoError(t, err)
		require.Len(t, remaining, 0)

		// Test updating non-existent permission
		err = repo.UpdateGroupPermission(ctx, logger.DefaultLogger, perm)
		require.ErrorIs(t, err, rauth.ErrNotFound)

		// Test deleting non-existent permission
		err = repo.DeleteGroupPermission(ctx, logger.DefaultLogger, uuid.MakeV4())
		require.ErrorIs(t, err, rauth.ErrNotFound)
	})

	t.Run("ReplaceAll", func(t *testing.T) {
		now := timeutil.Now()
		oldGroupName := "OldGroup-" + uuid.MakeV4().Short().String()
		oldPerms := []*auth.GroupPermission{
			{
				ID:         uuid.MakeV4(),
				GroupName:  oldGroupName,
				Scope:      "gcp-old",
				Permission: "viewer",
				CreatedAt:  now,
				UpdatedAt:  now,
			},
		}
		for _, p := range oldPerms {
			require.NoError(t, repo.CreateGroupPermission(ctx, logger.DefaultLogger, p))
		}

		// Replace with new permissions
		newGroup1 := "NewGroup1-" + uuid.MakeV4().Short().String()
		newGroup2 := "NewGroup2-" + uuid.MakeV4().Short().String()
		newPerms := []*auth.GroupPermission{
			{
				ID:         uuid.MakeV4(),
				GroupName:  newGroup1,
				Scope:      "aws-staging",
				Permission: "admin",
				CreatedAt:  now,
				UpdatedAt:  now,
			},
			{
				ID:         uuid.MakeV4(),
				GroupName:  newGroup2,
				Scope:      "gcp-engineering",
				Permission: "editor",
				CreatedAt:  now,
				UpdatedAt:  now,
			},
		}
		err := repo.ReplaceGroupPermissions(ctx, logger.DefaultLogger, newPerms)
		require.NoError(t, err)

		// Verify old permissions are gone
		oldPermsCheck, err := repo.GetPermissionsForGroups(ctx, logger.DefaultLogger, []string{oldGroupName})
		require.NoError(t, err)
		require.Len(t, oldPermsCheck, 0)

		// Verify new permissions exist
		finalPerms, _, err := repo.ListGroupPermissions(ctx, logger.DefaultLogger, *filters.NewFilterSet())
		require.NoError(t, err)
		require.Len(t, finalPerms, 2)

		// Verify new permissions
		groupNames := make(map[string]bool)
		for _, p := range finalPerms {
			groupNames[p.GroupName] = true
		}
		require.True(t, groupNames[newGroup1])
		require.True(t, groupNames[newGroup2])
	})
}

// TestCRDBAuthRepo_GetStatistics tests statistics gathering.
// This test is kept separate as it requires exact counts on clean data.
func TestCRDBAuthRepo_GetStatistics(t *testing.T) {
	db, err := getTestDB()
	if err != nil {
		skip.IgnoreLint(t, "Database not configured for testing")
	}
	createAuthTables(t, db)
	repo := NewAuthRepository(db)
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
	require.NoError(t, repo.CreateUser(ctx, logger.DefaultLogger, user1))
	require.NoError(t, repo.CreateUser(ctx, logger.DefaultLogger, user2))
	require.NoError(t, repo.CreateUser(ctx, logger.DefaultLogger, user3))

	// Groups: 2 groups
	group1ExternalID := "okta-ext-group-1"
	group1 := &auth.Group{
		ID:          uuid.MakeV4(),
		ExternalID:  &group1ExternalID,
		DisplayName: "Group One",
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	group2ExternalID := "okta-ext-group-2"
	group2 := &auth.Group{
		ID:          uuid.MakeV4(),
		ExternalID:  &group2ExternalID,
		DisplayName: "Group Two",
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	require.NoError(t, repo.CreateGroupWithMembers(ctx, logger.DefaultLogger, group1, []uuid.UUID{}))
	require.NoError(t, repo.CreateGroupWithMembers(ctx, logger.DefaultLogger, group2, []uuid.UUID{}))

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
	require.NoError(t, repo.CreateServiceAccount(ctx, logger.DefaultLogger, sa1))
	require.NoError(t, repo.CreateServiceAccount(ctx, logger.DefaultLogger, sa2))

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
	require.NoError(t, repo.CreateToken(ctx, logger.DefaultLogger, token1))
	require.NoError(t, repo.CreateToken(ctx, logger.DefaultLogger, token2))
	require.NoError(t, repo.CreateToken(ctx, logger.DefaultLogger, token3))

	// Get statistics
	stats, err := repo.GetStatistics(ctx, logger.DefaultLogger)
	require.NoError(t, err)
	require.NotNil(t, stats)

	// Verify user counts
	require.Equal(t, 2, stats.UsersActive, "should have 2 active users")
	require.Equal(t, 1, stats.UsersInactive, "should have 1 inactive user")

	// Verify group count
	require.Equal(t, 2, stats.Groups, "should have 2 groups")

	// Verify service account counts
	require.Equal(t, 1, stats.ServiceAccountsEnabled, "should have 1 enabled service account")
	require.Equal(t, 1, stats.ServiceAccountsDisabled, "should have 1 disabled service account")

	// Verify token counts by type and status
	require.NotNil(t, stats.TokensByTypeAndStatus)
	require.Equal(t, 1, stats.TokensByTypeAndStatus[string(auth.TokenTypeUser)][string(auth.TokenStatusValid)],
		"should have 1 valid user token")
	require.Equal(t, 1, stats.TokensByTypeAndStatus[string(auth.TokenTypeUser)][string(auth.TokenStatusRevoked)],
		"should have 1 revoked user token")
	require.Equal(t, 1, stats.TokensByTypeAndStatus[string(auth.TokenTypeServiceAccount)][string(auth.TokenStatusValid)],
		"should have 1 valid service account token")

	// Test with empty database
	createAuthTables(t, db) // Truncate all tables
	stats, err = repo.GetStatistics(ctx, logger.DefaultLogger)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, 0, stats.UsersActive)
	require.Equal(t, 0, stats.UsersInactive)
	require.Equal(t, 0, stats.Groups)
	require.Equal(t, 0, stats.ServiceAccountsEnabled)
	require.Equal(t, 0, stats.ServiceAccountsDisabled)
	require.NotNil(t, stats.TokensByTypeAndStatus)
	require.Empty(t, stats.TokensByTypeAndStatus)
}
