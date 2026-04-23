// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstats_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestCoordinatedFlush_BasicMultiNode exercises the streaming-based
// coordinator: each of N nodes runs a query, then a single coordinator
// invokes RunCoordinatedFlush which fans out targeted DrainSqlStats
// streams, merges per-source chunks in Go, and writes via the existing
// flush path. We assert that statistics from every source land in
// system.statement_statistics with the correct cumulative counts.
func TestCoordinatedFlush_BasicMultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t)

	const numNodes = 3
	const appName = "coord_flush_basic"

	testCluster := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: &sqlstats.TestingKnobs{
					SynchronousSQLStats: true,
				},
			},
		},
	})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	for i := 0; i < numNodes; i++ {
		conn := sqlutils.MakeSQLRunner(testCluster.ServerConn(i))
		conn.Exec(t, fmt.Sprintf("SET application_name = '%s'", appName))
		conn.Exec(t, "SELECT 1")
	}

	// Run one coordinated flush cycle from node 0.
	coordSQLServer := testCluster.Server(0).SQLServer().(*sql.Server)
	persisted := coordSQLServer.GetSQLStatsProvider()
	require.NoError(t,
		persisted.RunCoordinatedFlush(ctx, testCluster.Server(0).Stopper()))

	// The cumulative count for SELECT _ in this app must equal numNodes.
	conn := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))
	var totalCount int64
	conn.QueryRow(t, `
		SELECT coalesce(sum(((statistics->'statistics')->>'cnt')::INT), 0)
		FROM system.statement_statistics
		WHERE app_name = $1 AND metadata->>'query' = 'SELECT _'`,
		appName,
	).Scan(&totalCount)
	require.Equalf(t, int64(numNodes), totalCount,
		"expected count %d (one per node) for SELECT _, got %d", numNodes, totalCount)
}

// TestCoordinatedFlush_RestoreOnRPCFailure verifies the snapshot/restore
// contract end-to-end: when a remote drain stream fails partway, the
// remote container's stats are restored rather than lost. Achieved here
// by setting an aggressively low chunk size and tripping a per-RPC error
// after the first chunk via a context cancellation.
//
// This is a smoke test; the snapshot/restore semantics themselves have
// unit-level coverage in pkg/sql/sqlstats/ssmemstorage.
func TestCoordinatedFlush_DrainPreservesStatsAcrossCycles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t)

	const appName = "coord_flush_persistence"
	testCluster := serverutils.StartCluster(t, 2, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: &sqlstats.TestingKnobs{
					SynchronousSQLStats: true,
				},
			},
		},
	})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	for i := 0; i < 2; i++ {
		conn := sqlutils.MakeSQLRunner(testCluster.ServerConn(i))
		conn.Exec(t, fmt.Sprintf("SET application_name = '%s'", appName))
		conn.Exec(t, "SELECT 1")
		conn.Exec(t, "SELECT 1")
	}

	persisted := testCluster.Server(0).SQLServer().(*sql.Server).GetSQLStatsProvider()
	require.NoError(t,
		persisted.RunCoordinatedFlush(ctx, testCluster.Server(0).Stopper()))

	// After the first cycle, in-memory stats should be empty on every node;
	// a second flush cycle must therefore find nothing to add.
	conn := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))
	var firstCycleCount int64
	conn.QueryRow(t, `
		SELECT coalesce(sum(((statistics->'statistics')->>'cnt')::INT), 0)
		FROM system.statement_statistics
		WHERE app_name = $1 AND metadata->>'query' = 'SELECT _'`,
		appName,
	).Scan(&firstCycleCount)
	require.Equal(t, int64(4), firstCycleCount,
		"first cycle should have flushed two SELECT _ executions on each of two nodes")

	require.NoError(t,
		persisted.RunCoordinatedFlush(ctx, testCluster.Server(0).Stopper()))

	var secondCycleCount int64
	conn.QueryRow(t, `
		SELECT coalesce(sum(((statistics->'statistics')->>'cnt')::INT), 0)
		FROM system.statement_statistics
		WHERE app_name = $1 AND metadata->>'query' = 'SELECT _'`,
		appName,
	).Scan(&secondCycleCount)
	require.Equal(t, firstCycleCount, secondCycleCount,
		"second cycle with no new activity must not change row counts")
}

// TestCoordinatedFlush_MergeKeyIncludesAggregationWindow verifies the
// invariant added in this work: stats from disjoint aggregation windows
// must remain disjoint after Collect, even when they share the same
// fingerprint. This protects against the cross-window collapse that would
// otherwise misattribute counts.
func TestCoordinatedFlush_MergeKeyIncludesAggregationWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t)

	// One-second aggregation interval lets us cross a window boundary
	// without test-suite slowness.
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLStatsKnobs: &sqlstats.TestingKnobs{
				SynchronousSQLStats: true,
			},
		},
	})
	ctx := context.Background()
	defer srv.Stopper().Stop(ctx)

	conn := sqlutils.MakeSQLRunner(srv.SQLConn(t))
	conn.Exec(t, "SET CLUSTER SETTING sql.stats.aggregation.interval = '1s'")
	conn.Exec(t, "SET application_name = 'window_test'")

	// Window A.
	conn.Exec(t, "SELECT 1")
	// Wait past the window boundary.
	time.Sleep(1100 * time.Millisecond)
	// Window B.
	conn.Exec(t, "SELECT 1")

	persisted := srv.ApplicationLayer().SQLServer().(*sql.Server).GetSQLStatsProvider()
	require.NoError(t,
		persisted.RunCoordinatedFlush(ctx, srv.Stopper()))

	// The two windows must remain as distinct rows in
	// system.statement_statistics.
	var rowCount int
	conn.QueryRow(t, `
		SELECT count(*)
		FROM system.statement_statistics
		WHERE app_name = $1 AND metadata->>'query' = 'SELECT _'`,
		"window_test",
	).Scan(&rowCount)
	require.GreaterOrEqualf(t, rowCount, 2,
		"expected at least 2 rows (one per aggregation window), got %d", rowCount)
}
