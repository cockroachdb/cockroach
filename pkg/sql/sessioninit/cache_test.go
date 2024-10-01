// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sessioninit_test

import (
	"context"
	gosql "database/sql"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCacheInvalidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	pgURL, cleanupFunc := s.PGUrl(
		t, serverutils.CertsDirPrefix("TestCacheInvalidation"), serverutils.UserPassword("testuser", "abc"),
	)
	defer cleanupFunc()

	// Extract login as a function so that we can call it to populate the cache
	// with real information.
	login := func() {
		thisDB, err := gosql.Open("postgres", pgURL.String())
		require.NoError(t, err)
		var i int
		err = thisDB.QueryRow("SELECT 1").Scan(&i)
		require.NoError(t, err)
		_ = thisDB.Close()
	}

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	getSettingsFromCache := func() ([]sessioninit.SettingsCacheEntry, bool, error) {
		didReadFromSystemTable := false
		settings, err := execCfg.SessionInitCache.GetDefaultSettings(
			ctx,
			s.ClusterSettings(),
			s.InternalDB().(descs.DB),
			username.TestUserName(),
			"defaultdb",
			func(ctx context.Context, ief descs.DB, userName username.SQLUsername, databaseID descpb.ID) ([]sessioninit.SettingsCacheEntry, error) {
				didReadFromSystemTable = true
				return nil, nil
			})
		return settings, didReadFromSystemTable, err
	}
	getAuthInfoFromCache := func() (sessioninit.AuthInfo, bool, error) {
		didReadFromSystemTable := false
		settings := s.ClusterSettings()
		aInfo, err := execCfg.SessionInitCache.GetAuthInfo(
			ctx,
			settings,
			s.InternalDB().(descs.DB),
			username.TestUserName(),
			func(ctx context.Context, f descs.DB, userName username.SQLUsername) (sessioninit.AuthInfo, error) {
				didReadFromSystemTable = true
				return sessioninit.AuthInfo{}, nil
			})
		return aInfo, didReadFromSystemTable, err
	}

	// Create user and warm the cache.
	_, err := db.ExecContext(ctx, "CREATE USER testuser WITH PASSWORD 'abc'")
	require.NoError(t, err)
	login()

	t.Run("default settings cache", func(t *testing.T) {
		for _, stmt := range []string{
			`ALTER ROLE ALL IN DATABASE postgres SET search_path = 'a'`,
			`ALTER ROLE testuser SET search_path = 'b'`,
		} {
			_, err := db.ExecContext(ctx, stmt)
			require.NoError(t, err)
		}

		// Check that the cache initially contains the default settings for testuser.
		login()
		settings, didReadFromSystemTable, err := getSettingsFromCache()
		require.NoError(t, err)
		require.False(t, didReadFromSystemTable)
		require.Contains(t, settings, sessioninit.SettingsCacheEntry{
			SettingsCacheKey: sessioninit.SettingsCacheKey{
				DatabaseID: 0,
				Username:   username.TestUserName(),
			},
			Settings: []string{"search_path=b"},
		})

		// Verify that dropping a database referenced in the default settings table
		// causes the cache to be invalidated.
		_, err = db.ExecContext(ctx, "DROP DATABASE postgres")
		require.NoError(t, err)
		settings, didReadFromSystemTable, err = getSettingsFromCache()
		require.NoError(t, err)
		require.True(t, didReadFromSystemTable)
		require.Empty(t, settings)

		// Verify that adding a new default setting causes the cache to be
		// invalidated. We need to use login() to load "real" data.
		_, err = db.ExecContext(ctx, `ALTER ROLE ALL SET search_path = 'c'`)
		require.NoError(t, err)
		login()
		settings, didReadFromSystemTable, err = getSettingsFromCache()
		require.NoError(t, err)
		require.False(t, didReadFromSystemTable)
		require.Contains(t, settings, sessioninit.SettingsCacheEntry{
			SettingsCacheKey: sessioninit.SettingsCacheKey{
				DatabaseID: 0,
				Username:   username.MakeSQLUsernameFromPreNormalizedString(""),
			},
			Settings: []string{"search_path=c"},
		})

		// Verify that dropping a user referenced in the default settings table
		// causes the cache to be invalidated.
		_, err = db.ExecContext(ctx, "DROP USER testuser")
		require.NoError(t, err)
		settings, didReadFromSystemTable, err = getSettingsFromCache()
		require.NoError(t, err)
		require.True(t, didReadFromSystemTable)
		require.Empty(t, settings)

		// Re-create the user and warm the cache for the next test.
		_, err = db.ExecContext(ctx, "CREATE USER testuser WITH PASSWORD 'abc'")
		require.NoError(t, err)
		login()
	})

	t.Run("auth info cache", func(t *testing.T) {
		// Check that the cache initially contains info for testuser.
		login()
		aInfo, didReadFromSystemTable, err := getAuthInfoFromCache()
		require.NoError(t, err)
		require.False(t, didReadFromSystemTable)
		require.True(t, aInfo.UserExists)
		require.True(t, aInfo.CanLoginSQLRoleOpt)

		// Verify that creating a different user invalidates the cache.
		_, err = db.ExecContext(ctx, "CREATE USER testuser2")
		require.NoError(t, err)
		aInfo, didReadFromSystemTable, err = getAuthInfoFromCache()
		require.NoError(t, err)
		require.True(t, didReadFromSystemTable)

		// Verify that dropping a user invalidates the cache
		_, err = db.ExecContext(ctx, "DROP USER testuser2")
		require.NoError(t, err)
		aInfo, didReadFromSystemTable, err = getAuthInfoFromCache()
		require.NoError(t, err)
		require.True(t, didReadFromSystemTable)

		// Verify that altering VALID UNTIL invalidates the cache
		_, err = db.ExecContext(ctx, "ALTER USER testuser VALID UNTIL '2099-01-01'")
		require.NoError(t, err)
		aInfo, didReadFromSystemTable, err = getAuthInfoFromCache()
		require.NoError(t, err)
		require.True(t, didReadFromSystemTable)

		// Sanity check to make sure the cache is used.
		_, err = db.ExecContext(ctx, "SELECT 1")
		require.NoError(t, err)
		aInfo, didReadFromSystemTable, err = getAuthInfoFromCache()
		require.NoError(t, err)
		require.False(t, didReadFromSystemTable)
	})
}

func TestCacheSingleFlight(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	settings := execCfg.Settings
	c := execCfg.SessionInitCache

	testuser := username.MakeSQLUsernameFromPreNormalizedString("test")

	// Test concurrent table update with read.
	// Outdated data is written back to the cache, verify that the cache is
	// invalidated after.
	var wgForConcurrentReadWrite sync.WaitGroup
	var wgFirstGetAuthInfoCallInProgress sync.WaitGroup
	var wgForTestComplete sync.WaitGroup

	wgForConcurrentReadWrite.Add(1)
	wgFirstGetAuthInfoCallInProgress.Add(1)
	wgForTestComplete.Add(3)

	go func() {
		didReadFromSystemTable := false
		_, err := c.GetAuthInfo(
			ctx, settings,
			execCfg.InternalDB,
			testuser, func(
				ctx context.Context,
				f descs.DB,
				userName username.SQLUsername,
			) (sessioninit.AuthInfo, error) {
				wgFirstGetAuthInfoCallInProgress.Done()
				wgForConcurrentReadWrite.Wait()
				didReadFromSystemTable = true
				return sessioninit.AuthInfo{}, nil
			},
		)
		require.NoError(t, err)
		require.True(t, didReadFromSystemTable)
		wgForTestComplete.Done()
	}()

	// Wait for the first GetAuthInfo call to be in progress (but waiting)
	// before kicking off the next two calls to GetAuthInfo.
	wgFirstGetAuthInfoCallInProgress.Wait()

	// Kick off two extra goroutines to make sure only the first call reads
	// from the system table.
	for i := 0; i < 2; i++ {
		go func() {
			didReadFromSystemTable := false
			_, err := c.GetAuthInfo(
				ctx,
				settings,
				execCfg.InternalDB,
				testuser,
				func(
					ctx context.Context,
					f descs.DB,
					userName username.SQLUsername,
				) (sessioninit.AuthInfo, error) {
					didReadFromSystemTable = true
					return sessioninit.AuthInfo{}, nil
				},
			)
			require.NoError(t, err)
			require.False(t, didReadFromSystemTable)
			wgForTestComplete.Done()
		}()
	}

	_, err := db.Exec("CREATE USER test")
	require.NoError(t, err)

	wgForConcurrentReadWrite.Done()

	// GetAuthInfo should not be using the cache since it is outdated.
	didReadFromSystemTable := false
	_, err = c.GetAuthInfo(
		ctx,
		settings,
		execCfg.InternalDB,
		testuser,
		func(
			ctx context.Context,
			f descs.DB,
			userName username.SQLUsername,
		) (sessioninit.AuthInfo, error) {
			didReadFromSystemTable = true
			return sessioninit.AuthInfo{}, nil
		},
	)

	require.NoError(t, err)
	require.True(t, didReadFromSystemTable)

	wgForTestComplete.Wait()
}
