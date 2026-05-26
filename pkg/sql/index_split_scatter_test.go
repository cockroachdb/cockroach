// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestIndexSplitAndScatterSkipsSmallTable verifies that MaybeSplitIndexSpans
// skips splitting when table statistics indicate the table is smaller than
// the table's RangeMaxBytes, and proceeds normally when the table is large
// enough.
func TestIndexSplitAndScatterSkipsSmallTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t, "test takes too long under duress")

	ctx := context.Background()
	var splitCount atomic.Int64
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				BeforeIndexSplitAndScatter: func(splitPoints [][]byte) {
					splitCount.Swap(int64(len(splitPoints)))
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	runner := sqlutils.MakeSQLRunner(sqlDB)

	// Set range_max_bytes to the minimum so we can exceed it with a
	// reasonable amount of data.
	const rangeMaxBytes = 64 << 20 // 64 MB (minimum allowed)
	runner.Exec(t, fmt.Sprintf(
		"ALTER RANGE default CONFIGURE ZONE USING range_max_bytes = %d, range_min_bytes = 1",
		rangeMaxBytes,
	))

	// Wait for the zone config change to propagate through the gossip-based
	// system config cache. rangeMaxBytesForTable reads from this cache, and
	// without this wait the test is flaky (~10% of the time the cache still
	// has the old default of 512 MB).
	testutils.SucceedsSoon(t, func() error {
		cfg := s.SystemConfigProvider().GetSystemConfig()
		if cfg == nil {
			return errors.New("system config not yet available")
		}
		zc, err := cfg.GetZoneConfigForObject(
			s.Codec(), config.ObjectID(keys.RootNamespaceID),
		)
		if err != nil {
			return errors.Wrap(err, "looking up zone config")
		}
		if zc == nil || zc.RangeMaxBytes == nil || *zc.RangeMaxBytes != rangeMaxBytes {
			return errors.New("zone config not yet propagated")
		}
		return nil
	})

	// Disable automatic statistics so we control when stats are refreshed.
	runner.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false")

	// Creating an index with skip_splits_for_small_tables disabled should create splits.
	runner.Exec(t, "CREATE TABLE t_splits (k INT PRIMARY KEY, v INT, payload STRING)")
	runner.Exec(t, "SET CLUSTER SETTING schemachanger.backfiller.skip_splits_for_small_tables.enabled = false")
	splitCount.Store(0)
	runner.Exec(t, "CREATE INDEX idx_disable_skip ON t_splits (v)")
	require.Greater(t, splitCount.Load(), int64(0),
		"expected splits when cluster setting is disabled")
	runner.Exec(t, "SET CLUSTER SETTING schemachanger.backfiller.skip_splits_for_small_tables.enabled = true")

	// Creating an index on table without stats should skip splits.
	splitCount.Store(0)
	runner.Exec(t, "CREATE INDEX idx_no_stats ON t_splits (v)")
	require.Equal(t, int64(0), splitCount.Load(),
		"expected no splits on a table with no stats")

	// Collect stats so the cache knows RowCount=0. Creating an index
	// on the empty table should skip splits.
	runner.Exec(t, "CREATE STATISTICS s FROM t_splits")
	runner.Exec(t, "CREATE INDEX idx_empty ON t_splits (v)")
	require.Equal(t, int64(0), splitCount.Load(),
		"expected no splits on an empty table with stats")

	// Insert data and refresh stats. The estimated table size (~1 MB)
	// is well below the 64 MB RangeMaxBytes, so splits are still skipped.
	runner.Exec(t, "INSERT INTO t_splits SELECT i, i, repeat('x', 1000) FROM generate_series(1, 1000) AS g(i)")
	runner.Exec(t, "CREATE STATISTICS s FROM t_splits")

	runner.Exec(t, "CREATE INDEX idx_small ON t_splits (v, k)")
	require.Equal(t, int64(0), splitCount.Load(),
		"expected no splits on a small table below RangeMaxBytes")

	// Insert enough data to well exceed 64 MB. Use 10k rows with 10 KB
	// payloads (10k × 10 KB ≈ 100 MB) to keep the insert fast while
	// having enough rows for the stats-based split point generator.
	runner.Exec(t, "INSERT INTO t_splits SELECT i, i, repeat('x', 10000) FROM generate_series(1001, 11000) AS g(i)")
	runner.Exec(t, "CREATE STATISTICS s FROM t_splits")

	splitCount.Store(0)
	runner.Exec(t, "CREATE INDEX idx_large ON t_splits (v, k)")
	require.Greater(t, splitCount.Load(), int64(0),
		"expected splits on a large table above RangeMaxBytes")
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
		// Disable the small-table size check so this test exercises the
		// stats-based split point generation.
		runner.Exec(t, "SET CLUSTER SETTING schemachanger.backfiller.skip_splits_for_small_tables.enabled = false")
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
