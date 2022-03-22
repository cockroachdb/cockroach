// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sessioninit_test

import (
	"context"
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
	"github.com/stretchr/testify/require"
)

func TestCacheSingleFlight(t *testing.T) {
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

	c := s.ExecutorConfig().(sql.ExecutorConfig).SessionInitCache

	testuser := security.MakeSQLUsernameFromPreNormalizedString("test")

	// Test concurrent table update with read.
	// Outdated data is written back to the cache, verify that the cache is
	// invalidated after.
	var wgForConcurrentReadWrite sync.WaitGroup
	var wgForParallelReadsToSystemTable sync.WaitGroup
	var wgForTestComplete sync.WaitGroup
	wgForConcurrentReadWrite.Add(1)
	wgForParallelReadsToSystemTable.Add(1)
	wgForTestComplete.Add(3)
	go func() {
		didReadFromSystemTable := false
		_, err := c.GetAuthInfo(ctx, settings, &ie, s.DB(), s.ExecutorConfig().(sql.ExecutorConfig).CollectionFactory, testuser, func(
			ctx context.Context,
			ie sqlutil.InternalExecutor,
			username security.SQLUsername,
		) (sessioninit.AuthInfo, error) {
			wgForConcurrentReadWrite.Wait()
			didReadFromSystemTable = true
			return sessioninit.AuthInfo{}, nil
		})
		wgForParallelReadsToSystemTable.Done()
		require.NoError(t, err)
		require.True(t, didReadFromSystemTable)
		wgForTestComplete.Done()
	}()

	// Kick off two extra gorutines to make sure only the first call reads
	// from the system table.
	for i := 0; i < 2; i++ {
		go func() {
			wgForParallelReadsToSystemTable.Wait()
			didReadFromSystemTable := false
			_, err := c.GetAuthInfo(ctx, settings, &ie, s.DB(), s.ExecutorConfig().(sql.ExecutorConfig).CollectionFactory, testuser, func(
				ctx context.Context,
				ie sqlutil.InternalExecutor,
				username security.SQLUsername,
			) (sessioninit.AuthInfo, error) {
				didReadFromSystemTable = true
				return sessioninit.AuthInfo{}, nil
			})
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
	_, err = c.GetAuthInfo(ctx, settings, &ie, s.DB(), s.ExecutorConfig().(sql.ExecutorConfig).CollectionFactory, testuser, func(
		ctx context.Context,
		ie sqlutil.InternalExecutor,
		username security.SQLUsername,
	) (sessioninit.AuthInfo, error) {
		didReadFromSystemTable = true
		return sessioninit.AuthInfo{}, nil
	})

	require.NoError(t, err)
	require.True(t, didReadFromSystemTable)

	wgForTestComplete.Wait()
}
