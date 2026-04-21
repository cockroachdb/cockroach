// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestIndexSplitAndScatterDisabled verifies that the session variable and
// cluster setting for enable_split_and_scatter_backfill control whether
// splits are created during CREATE INDEX.
func TestIndexSplitAndScatterDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var splitCount atomic.Int64
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				BeforeIndexSplitAndScatter: func(splitPoints [][]byte) {
					splitCount.Add(int64(len(splitPoints)))
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	runner := sqlutils.MakeSQLRunner(sqlDB)
	runner.Exec(t, "CREATE TABLE t_split_test (k INT PRIMARY KEY, v INT)")

	// By default, CREATE INDEX creates splits even on an empty table.
	splitCount.Store(0)
	runner.Exec(t, "CREATE INDEX idx_default ON t_split_test (v)")
	require.Greater(t, splitCount.Load(), int64(0),
		"expected splits by default")

	// Disable via session variable.
	runner.Exec(t, "SET enable_split_and_scatter_backfill = false")
	splitCount.Store(0)
	runner.Exec(t, "CREATE INDEX idx_session_off ON t_split_test (v, k)")
	require.Equal(t, int64(0), splitCount.Load(),
		"expected no splits when session variable is false")

	// Re-enable via session variable.
	runner.Exec(t, "SET enable_split_and_scatter_backfill = true")
	splitCount.Store(0)
	runner.Exec(t, "CREATE INDEX idx_session_on ON t_split_test (k, v)")
	require.Greater(t, splitCount.Load(), int64(0),
		"expected splits when session variable is true")

	// Disable via cluster setting. Use a new connection so the session
	// variable picks up the new default.
	runner.Exec(t, "SET CLUSTER SETTING sql.defaults.split_and_scatter_backfill.enabled = false")
	runner2 := sqlutils.MakeSQLRunner(s.ApplicationLayer().SQLConn(t))
	splitCount.Store(0)
	runner2.Exec(t, "CREATE INDEX idx_cluster_off ON t_split_test (v)")
	require.Equal(t, int64(0), splitCount.Load(),
		"expected no splits when cluster setting is false")
}

// TestIndexSplitAndScatterWithStats tests the creation of indexes on tables with statistics,
// where the splits will be generated using statistics on the table.
func TestIndexSplitAndScatterWithStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// This test can be fairly slow and timeout under race / duress.
	skip.UnderDuress(t)

	testutils.RunTrueAndFalse(t, "StatsCreated", func(t *testing.T, statsExist bool) {
		ctx := context.Background()
		var splitHookEnabled atomic.Bool
		var observedSplitPoints atomic.Int64
		const numNodes = 3
		cluster := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					SQLExecutor: &sql.ExecutorTestingKnobs{
						BeforeIndexSplitAndScatter: func(splitPoints [][]byte) {
							if !splitHookEnabled.Load() {
								return
							}
							observedSplitPoints.Swap(int64(len(splitPoints)))
						},
					},
				},
			},
		})
		defer cluster.Stopper().Stop(ctx)
		runner := sqlutils.MakeSQLRunner(cluster.ServerConn(0))
		// Disable automatic statistics.
		runner.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false")
		// Create and populate the tables.
		runner.Exec(t, "CREATE TABLE multi_column_split (b bool, n uuid PRIMARY KEY)")
		runner.Exec(t, "INSERT INTO multi_column_split (SELECT true, uuid_generate_v1()  FROM generate_series(1, 5000))")
		runner.Exec(t, "INSERT INTO multi_column_split (SELECT false, uuid_generate_v1() FROM generate_series(1, 5000))")
		// Generate statistics for these tables.
		if statsExist {
			runner.Exec(t, "CREATE STATISTICS st FROM multi_column_split")
		}
		// Next create indexes on both tables.
		splitHookEnabled.Store(true)
		observedSplitPoints.Store(0)
		runner.Exec(t, "CREATE INDEX ON multi_column_split (b, n)")
		// Assert that we generated the target number of split points
		// automatically.
		if !statsExist {
			require.Equal(t, int64(1), observedSplitPoints.Load())
		} else {
			expectedCount := sql.PreservedSplitCountMultiple.Get(&cluster.Server(0).ClusterSettings().SV) * numNodes
			require.Greaterf(t, observedSplitPoints.Load(), expectedCount,
				"expected %d split points, got %d", expectedCount, observedSplitPoints.Load())
		}
		splitHookEnabled.Swap(false)
	})

}
