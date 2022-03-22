// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"math"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/require"
)

func TestCacheInvalidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	settings := s.ExecutorConfig().(sql.ExecutorConfig).Settings
	ie := sql.MakeInternalExecutor(
		ctx,
		s.(*server.TestServer).Server.PGServer().SQLServer,
		sql.MemoryMetrics{},
		s.ExecutorConfig().(sql.ExecutorConfig).Settings,
	)

	pool := mon.NewUnlimitedMonitor(
		context.Background(), "test", mon.MemoryResource,
		nil /* curCount */, nil /* maxHist */, math.MaxInt64, settings,
	)

	testMon := pool.MakeBoundAccount()
	c := sessioninit.NewCache(testMon, s.Stopper())

	testuser := security.MakeSQLUsernameFromPreNormalizedString("test")

	aInfoOriginal := sessioninit.AuthInfo{
		UserExists:  true,
		CanLoginSQL: true,
	}

	aInfo, err := c.GetAuthInfo(ctx, settings, &ie, s.DB(), s.ExecutorConfig().(sql.ExecutorConfig).CollectionFactory, testuser, func(
		ctx context.Context,
		ie sqlutil.InternalExecutor,
		username security.SQLUsername,
	) (sessioninit.AuthInfo, error) {
		return aInfoOriginal, nil
	})
	require.NoError(t, err)
	require.Equal(t, aInfo, aInfoOriginal)

	// aInfo should be cached.
	aInfo, err = c.GetAuthInfo(ctx, settings, &ie, s.DB(), s.ExecutorConfig().(sql.ExecutorConfig).CollectionFactory, testuser, func(
		ctx context.Context,
		ie sqlutil.InternalExecutor,
		username security.SQLUsername,
	) (sessioninit.AuthInfo, error) {
		return sessioninit.AuthInfo{}, nil
	})
	require.NoError(t, err)
	require.Equal(t, aInfo, aInfoOriginal)

	// Update system.users table to invalidate cache.
	db.Exec("CREATE USER test2")

	aInfoNew := sessioninit.AuthInfo{
		UserExists:  true,
		CanLoginSQL: false,
	}

	aInfo, err = c.GetAuthInfo(ctx, settings, &ie, s.DB(), s.ExecutorConfig().(sql.ExecutorConfig).CollectionFactory, testuser, func(
		ctx context.Context,
		ie sqlutil.InternalExecutor,
		username security.SQLUsername,
	) (sessioninit.AuthInfo, error) {
		return aInfoNew, nil
	})
	require.NoError(t, err)
	require.Equal(t, aInfo, aInfoNew)

	// Test concurrent table update with read.
	// Outdated data is written back to the cache, verify that the cache is
	// invalidated after.
	var wg sync.WaitGroup
	var wg2 sync.WaitGroup
	wg.Add(1)
	wg2.Add(2)
	go func() {
		aInfo, err = c.GetAuthInfo(ctx, settings, &ie, s.DB(), s.ExecutorConfig().(sql.ExecutorConfig).CollectionFactory, testuser, func(
			ctx context.Context,
			ie sqlutil.InternalExecutor,
			username security.SQLUsername,
		) (sessioninit.AuthInfo, error) {
			wg.Wait()
			return aInfoNew, nil
		})
		wg2.Done()
		require.NoError(t, err)
		require.Equal(t, aInfo, aInfoNew)
	}()

	// Kick off two extra gorutines to make sure only the first one returns
	// the value due to single flight.
	for i := 0; i < 2; i++ {
		go func() {
			aInfo, err = c.GetAuthInfo(ctx, settings, &ie, s.DB(), s.ExecutorConfig().(sql.ExecutorConfig).CollectionFactory, testuser, func(
				ctx context.Context,
				ie sqlutil.InternalExecutor,
				username security.SQLUsername,
			) (sessioninit.AuthInfo, error) {
				wg2.Wait()
				// This return shouldn't actually be used.
				return sessioninit.AuthInfo{}, nil
			})
			require.NoError(t, err)
			require.Equal(t, aInfo, aInfoNew)
		}()
	}

	db.Exec("CREATE USER test3")
	wg.Done()

	aInfoNew2 := sessioninit.AuthInfo{
		CanLoginDBConsole: true,
	}

	// GetAuthInfo should not be using the cache since it is outdated.
	aInfo, err = c.GetAuthInfo(ctx, settings, &ie, s.DB(), s.ExecutorConfig().(sql.ExecutorConfig).CollectionFactory, testuser, func(
		ctx context.Context,
		ie sqlutil.InternalExecutor,
		username security.SQLUsername,
	) (sessioninit.AuthInfo, error) {
		return aInfoNew2, nil
	})

	require.NoError(t, err)
	require.Equal(t, aInfo, aInfoNew2)
}
