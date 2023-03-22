// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestDescriptorIDSequenceForSystemTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	notifyOfMigrationErrorHandling := make(chan struct{})
	notifyToProceedWithCheck := make(chan struct{})
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				TenantTestingKnobs: &sql.TenantTestingKnobs{
					BeforeCheckingForDescriptorIDSequence: func(ctx context.Context) {
						notifyOfMigrationErrorHandling <- struct{}{}
						<-notifyToProceedWithCheck
					},
				},
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.V23_1DescIDSequenceForSystemTenant - 2),
				},
			},
		},
	}

	var (
		ctx = context.Background()

		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		s     = tc.Server(0)
		sqlDB = tc.ServerConn(0)
		tdb   = sqlutils.MakeSQLRunner(sqlDB)
	)
	defer tc.Stopper().Stop(ctx)
	require.True(t, s.ExecutorConfig().(sql.ExecutorConfig).Codec.ForSystemTenant())

	getOld := func() int64 {
		ret, err := s.DB().Get(ctx, keys.LegacyDescIDGenerator)
		require.NoError(t, err)
		return ret.ValueInt()
	}
	getNew := func() int64 {
		ret, err := s.DB().Get(ctx, keys.SystemSQLCodec.SequenceKey(keys.DescIDSequenceID))
		require.NoError(t, err)
		return ret.ValueInt()
	}

	// Check that the counters are in the expected initial state.
	initOld := getOld()
	require.NotZero(t, initOld, "legacy counter should not be zero")
	require.Zero(t, getNew(), "sequence should be zero before upgrade")

	// Check that the legacy counter really has been used until now.
	var maxID int64
	tdb.QueryRow(t, `SELECT max(id) FROM system.descriptor`).Scan(&maxID)
	require.Less(t, maxID, initOld, "legacy counter should have been used in all cases")

	// Check that the legacy counter continues to be used.
	tdb.Exec(t, `CREATE TABLE t1 (x INT PRIMARY KEY)`)
	preMigration := getOld()
	require.Less(t, initOld, preMigration, "legacy counter should be bumped")
	require.Zero(t, getNew(), "sequence should be zero before upgrade")

	// Upgrade the cluster to the version prior to the descriptor ID migration.
	tdb.Exec(t, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1DescIDSequenceForSystemTenant-1).String())

	// Descriptor ID creation should throw an error which triggers a retry.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		tdb.Exec(t, `CREATE TABLE t2 (x INT PRIMARY KEY)`)
		wg.Done()
	}()
	<-notifyOfMigrationErrorHandling
	// As of now there is a pending retry which is blocked.

	// Check that the counters remain unchanged.
	require.Equal(t, preMigration, getOld(), "legacy counter should remain unchanged")
	require.Zero(t, getNew(), "sequence should be zero before upgrade")

	// Upgrade the cluster to the version after the descriptor ID migration.
	tdb.Exec(t, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1DescIDSequenceForSystemTenant).String())

	// Check that the sequence now exists.
	var q = fmt.Sprintf(`SELECT count(*) FROM system.descriptor WHERE id = %d`, keys.DescIDSequenceID)
	tdb.CheckQueryResultsRetry(t, q, [][]string{{"1"}})

	// Check that the counters both have the correct value.
	postMigrationNew := getNew()
	require.Equal(t, preMigration, getOld(), "legacy counter should remain unchanged")
	require.Equal(t, preMigration, postMigrationNew, "sequence counter should be set to legacy counter value")

	// Resume the blocked t2 table creation and wait for it to complete.
	notifyToProceedWithCheck <- struct{}{}
	wg.Wait()

	// Check that the sequence counter was used.
	postMigrationTableCreation := getNew()
	require.Equal(t, preMigration, getOld(), "legacy counter should remain unchanged")
	require.Less(t, postMigrationNew, postMigrationTableCreation, "sequence counter should be bumped")

	// Descriptor ID creation should succeed.
	tdb.Exec(t, `CREATE TABLE t3 (x INT PRIMARY KEY)`)

	// Check that the sequence remains in use.
	require.Equal(t, preMigration, getOld(), "legacy counter should remain unchanged")
	require.Less(t, postMigrationTableCreation, getNew(), "sequence counter should be bumped")
}
