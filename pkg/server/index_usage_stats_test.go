// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	gosql "database/sql"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/idxusage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func compareTimeHelper(t *testing.T, expected, actual time.Time, delta time.Duration) {
	diff := actual.Sub(expected)
	require.True(t, diff < delta, "expected delta %s, but found %s", delta, diff)
}

func compareStatsHelper(
	t *testing.T, expected, actual roachpb.IndexUsageStatistics, delta time.Duration,
) {
	compareTimeHelper(t, expected.LastRead, actual.LastRead, delta)
	compareTimeHelper(t, expected.LastWrite, actual.LastWrite, delta)

	// We don't perform deep comparison of time.Time. So we set them to a dummy
	// value before deep equal.
	dummyTime := timeutil.Now()

	expected.LastRead = dummyTime
	expected.LastWrite = dummyTime
	actual.LastRead = dummyTime
	actual.LastWrite = dummyTime

	require.Equal(t, expected, actual)
}

func createIndexStatsIngestedCallback() (
	func(key roachpb.IndexUsageKey),
	chan roachpb.IndexUsageKey,
) {
	// Create a buffered channel so the callback is non-blocking.
	notify := make(chan roachpb.IndexUsageKey, 100)

	cb := func(key roachpb.IndexUsageKey) {
		notify <- key
	}

	return cb, notify
}

func waitForStatsIngestion(
	t *testing.T,
	notify chan roachpb.IndexUsageKey,
	expectedKeys map[roachpb.IndexUsageKey]struct{},
	expectedEventCnt int,
	timeout time.Duration,
) {
	var timer timeutil.Timer
	eventCnt := 0

	timer.Reset(timeout)

	for eventCnt < expectedEventCnt {
		select {
		case key := <-notify:
			if _, ok := expectedKeys[key]; ok {
				eventCnt++
			}
			continue
		case <-timer.C:
			timer.Read = true
			t.Fatalf("expected stats ingestion to complete within %s, but it timed out", timeout)
		}
	}
}

func TestStatusAPIIndexUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	statsIngestionCb, statsIngestionNotifier := createIndexStatsIngestedCallback()

	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				IndexUsageStatsKnobs: &idxusage.TestingKnobs{
					OnIndexUsageStatsProcessedCallback: statsIngestionCb,
				},
			},
		},
	})

	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	firstServer := testCluster.Server(0 /* idx */)
	firstLocalStatsReader := firstServer.SQLServer().(*sql.Server).GetLocalIndexStatistics()

	expectedStatsIndexA := roachpb.IndexUsageStatistics{
		TotalReadCount: 2,
		LastRead:       timeutil.Now(),
	}

	expectedStatsIndexB := roachpb.IndexUsageStatistics{
		TotalReadCount: 2,
		LastRead:       timeutil.Now(),
	}

	firstPgURL, firstServerConnCleanup := sqlutils.PGUrl(
		t, firstServer.ServingSQLAddr(), "CreateConnections" /* prefix */, url.User(security.RootUser))
	defer firstServerConnCleanup()

	firstServerSQLConn, err := gosql.Open("postgres", firstPgURL.String())
	require.NoError(t, err)

	defer func() {
		err := firstServerSQLConn.Close()
		require.NoError(t, err)
	}()

	// Create table on the first node.
	_, err = firstServerSQLConn.Exec("CREATE TABLE t (k INT PRIMARY KEY, a INT, b INT, c INT, INDEX(a), INDEX(b))")
	require.NoError(t, err)

	_, err = firstServerSQLConn.Exec("INSERT INTO t VALUES (1, 10, 100, 0), (2, 20, 200, 0), (3, 30, 300, 0)")
	require.NoError(t, err)

	// We fetch the table ID of the testing table.
	rows, err := firstServerSQLConn.Query("SELECT table_id FROM crdb_internal.tables WHERE name = 't'")
	require.NoError(t, err)
	require.NotNil(t, rows)

	defer func() {
		err := rows.Close()
		require.NoError(t, err)
	}()

	var tableID int
	require.True(t, rows.Next())
	err = rows.Scan(&tableID)
	require.NoError(t, err)
	require.False(t, rows.Next())

	indexKeyA := roachpb.IndexUsageKey{
		TableID: roachpb.TableID(tableID),
		IndexID: 2, // t@t_a_idx
	}

	indexKeyB := roachpb.IndexUsageKey{
		TableID: roachpb.TableID(tableID),
		IndexID: 3, // t@t_b_idx
	}

	err = firstServerSQLConn.Close()
	require.NoError(t, err)

	firstServerConnCleanup()

	// Run some queries on the second node.
	secondServer := testCluster.Server(1 /* idx */)
	secondLocalStatsReader := secondServer.SQLServer().(*sql.Server).GetLocalIndexStatistics()

	secondPgURL, secondServerConnCleanup := sqlutils.PGUrl(
		t, secondServer.ServingSQLAddr(), "CreateConnections" /* prefix */, url.User(security.RootUser))
	defer secondServerConnCleanup()

	secondServerSQLConn, err := gosql.Open("postgres", secondPgURL.String())
	require.NoError(t, err)

	defer func() {
		err := secondServerSQLConn.Close()
		require.NoError(t, err)
	}()

	// Records a non-full scan over t_a_idx.
	_, err = secondServerSQLConn.Exec("SELECT k, a FROM t WHERE a = 0")
	require.NoError(t, err)

	// Records an zigzag join that scans both t_a_idx and t_b_idx.
	_, err = secondServerSQLConn.Exec("SELECT k FROM t WHERE a = 10 AND b = 200")
	require.NoError(t, err)

	// Record a full scan over t_b_idx.
	_, err = secondServerSQLConn.Exec("SELECT * FROM t@t_b_idx")
	require.NoError(t, err)

	// Execute a explain query to ensure no index stats is collected.
	_, err = secondServerSQLConn.Exec("EXPLAIN SELECT k, a FROM t WHERE a = 0")
	require.NoError(t, err)

	// Check local node stats.
	// Fetch stats reader from each individual
	thirdServer := testCluster.Server(2 /* idx */)
	thirdLocalStatsReader := thirdServer.SQLServer().(*sql.Server).GetLocalIndexStatistics()

	// Wait for the stats to be ingested.
	waitForStatsIngestion(t, statsIngestionNotifier, map[roachpb.IndexUsageKey]struct{}{
		indexKeyA: {},
		indexKeyB: {},
	}, /* expectedKeys */ 4 /* expectedEventCnt*/, 5*time.Second /* timeout */)

	// First node should have nothing.
	stats := firstLocalStatsReader.Get(indexKeyA.TableID, indexKeyA.IndexID)
	require.Equal(t, roachpb.IndexUsageStatistics{}, stats, "expecting empty stats on node 1, but found %v", stats)

	stats = firstLocalStatsReader.Get(indexKeyB.TableID, indexKeyB.IndexID)
	require.Equal(t, roachpb.IndexUsageStatistics{}, stats, "expecting empty stats on node 1, but found %v", stats)

	// Third node should have nothing.
	stats = thirdLocalStatsReader.Get(indexKeyA.TableID, indexKeyA.IndexID)
	require.Equal(t, roachpb.IndexUsageStatistics{}, stats, "expecting empty stats on node 3, but found %v", stats)

	stats = thirdLocalStatsReader.Get(indexKeyB.TableID, indexKeyB.IndexID)
	require.Equal(t, roachpb.IndexUsageStatistics{}, stats, "expecting empty stats on node 1, but found %v", stats)

	// Second server should have nonempty local storage.
	stats = secondLocalStatsReader.Get(indexKeyA.TableID, indexKeyA.IndexID)
	compareStatsHelper(t, expectedStatsIndexA, stats, time.Minute)

	stats = secondLocalStatsReader.Get(indexKeyB.TableID, indexKeyB.IndexID)
	compareStatsHelper(t, expectedStatsIndexB, stats, time.Minute)

	// Test cluster-wide RPC.
	var resp serverpb.IndexUsageStatisticsResponse
	err = getStatusJSONProto(thirdServer, "indexusagestatistics", &resp)
	require.NoError(t, err)

	statsEntries := 0
	for _, stats := range resp.Statistics {
		// Skip if the table is not what we expected.
		if stats.Key.TableID != roachpb.TableID(tableID) {
			continue
		}
		statsEntries++
		switch stats.Key.IndexID {
		case indexKeyA.IndexID: // t@t_a_idx
			compareStatsHelper(t, expectedStatsIndexA, stats.Stats, time.Minute)
		case indexKeyB.IndexID: // t@t_b_idx
			compareStatsHelper(t, expectedStatsIndexB, stats.Stats, time.Minute)
		}
	}

	require.True(t, statsEntries == 2, "expect to find two stats entries in RPC response, but found %d", statsEntries)

	// Test disabling subsystem.
	_, err = secondServerSQLConn.Exec("SET CLUSTER SETTING sql.metrics.index_usage_stats.enabled = false")
	require.NoError(t, err)

	// Records a non-full scan, but it shouldn't change the stats since we have
	// stats collection disabled.
	_, err = secondServerSQLConn.Exec("SELECT k, a FROM t WHERE a = 0")
	require.NoError(t, err)

	err = getStatusJSONProto(thirdServer, "indexusagestatistics", &resp)
	require.NoError(t, err)

	statsEntries = 0
	for _, stats := range resp.Statistics {
		// Skip if the table is not what we expected.
		if stats.Key.TableID != roachpb.TableID(tableID) {
			continue
		}
		statsEntries++
		switch stats.Key.IndexID {
		case 2: // t@t_a_idx
			compareStatsHelper(t, expectedStatsIndexA, stats.Stats, time.Minute)
		case 3: // t@t_b_idx
			compareStatsHelper(t, expectedStatsIndexB, stats.Stats, time.Minute)
		}
	}
	require.True(t, statsEntries == 2, "expect to find two stats entries in RPC response, but found %d", statsEntries)
}
