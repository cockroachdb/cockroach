// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql"
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

func TestStatusAPIIndexUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartCluster(t, 4, base.TestClusterArgs{})

	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	firstServer := testCluster.Server(0 /* idx */)
	firstLocalStatsReader := firstServer.SQLServer().(*sql.Server).GetLocalIndexStatistics()

	expectedStatsIndexA := roachpb.IndexUsageStatistics{
		TotalReadCount: 2,
		LastRead:       timeutil.Now(),
	}

	expectedStatsIndexB := roachpb.IndexUsageStatistics{
		TotalReadCount: 1,
		LastRead:       timeutil.Now(),
	}

	expectedStatsIndexPrimary := roachpb.IndexUsageStatistics{
		TotalReadCount: 2,
		LastRead:       timeutil.Now(),
	}

	firstServerSQLConn := firstServer.SQLConn(t)

	// Create table on the first node.
	_, err := firstServerSQLConn.Exec("CREATE TABLE t (k INT PRIMARY KEY, a INT, b INT, c INT, INDEX(a), INDEX(b))")
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

	indexKeyPrimary := roachpb.IndexUsageKey{
		TableID: roachpb.TableID(tableID),
		IndexID: 1, // t@t_pkey
	}

	indexKeyA := roachpb.IndexUsageKey{
		TableID: roachpb.TableID(tableID),
		IndexID: 2, // t@t_a_idx
	}

	indexKeyB := roachpb.IndexUsageKey{
		TableID: roachpb.TableID(tableID),
		IndexID: 3, // t@t_b_idx
	}

	// Run some queries on the second node.
	secondServer := testCluster.Server(1 /* idx */)
	secondLocalStatsReader := secondServer.SQLServer().(*sql.Server).GetLocalIndexStatistics()

	secondServerSQLConn := secondServer.SQLConn(t)

	// Records a non-full scan over t_a_idx.
	_, err = secondServerSQLConn.Exec("SELECT k, a FROM t WHERE a = 0")
	require.NoError(t, err)

	// Records an zigzag join that scans both t_a_idx and t_b_idx.
	_, err = secondServerSQLConn.Exec("SELECT k FROM t WHERE a = 10 AND b = 200")
	require.NoError(t, err)

	// Record an index join and full scan of t_b_idx.
	_, err = secondServerSQLConn.Exec("SELECT * FROM t@t_b_idx")
	require.NoError(t, err)

	// Execute a explain query to ensure no index stats is collected.
	_, err = secondServerSQLConn.Exec("EXPLAIN SELECT k, a FROM t WHERE a = 0")
	require.NoError(t, err)

	// Run some queries on the fourth node.
	fourthServer := testCluster.Server(3 /* idx */)
	fourthLocalStatsReader := fourthServer.SQLServer().(*sql.Server).GetLocalIndexStatistics()

	fourthServerSQLConn := fourthServer.SQLConn(t)

	// Test that total_reads / last_read was not populated by an explicit CREATE INDEX query.
	_, err = fourthServerSQLConn.Exec("CREATE TABLE test(num INT PRIMARY KEY, letter CHAR)")
	require.NoError(t, err)

	_, err = fourthServerSQLConn.Exec("CREATE INDEX ON test(letter)")
	require.NoError(t, err)

	// We fetch the table ID of the testing table.
	fourthNodeRows, err := fourthServerSQLConn.Query("SELECT table_id FROM crdb_internal.tables WHERE name = 'test'")
	require.NoError(t, err)
	require.NotNil(t, fourthNodeRows)

	defer func() {
		err := fourthNodeRows.Close()
		require.NoError(t, err)
	}()

	var testTableID int
	require.True(t, fourthNodeRows.Next())
	err = fourthNodeRows.Scan(&testTableID)
	require.NoError(t, err)
	require.False(t, fourthNodeRows.Next())

	fourthNodeIndexKeyPrimary := roachpb.IndexUsageKey{
		TableID: roachpb.TableID(testTableID),
		IndexID: 1, // test pkey
	}

	fourthNodeindexKeyA := roachpb.IndexUsageKey{
		TableID: roachpb.TableID(testTableID),
		IndexID: 2, // secondary index
	}

	// Check local node stats.
	// Fetch stats reader from each individual
	thirdServer := testCluster.Server(2 /* idx */)
	thirdLocalStatsReader := thirdServer.SQLServer().(*sql.Server).GetLocalIndexStatistics()

	// First node should have nothing.
	stats := firstLocalStatsReader.Get(indexKeyPrimary.TableID, indexKeyPrimary.IndexID)
	require.Equal(t, roachpb.IndexUsageStatistics{}, stats, "expecting empty stats on node 1, but found %v", stats)

	stats = firstLocalStatsReader.Get(indexKeyA.TableID, indexKeyA.IndexID)
	require.Equal(t, roachpb.IndexUsageStatistics{}, stats, "expecting empty stats on node 1, but found %v", stats)

	stats = firstLocalStatsReader.Get(indexKeyB.TableID, indexKeyB.IndexID)
	require.Equal(t, roachpb.IndexUsageStatistics{}, stats, "expecting empty stats on node 1, but found %v", stats)

	// Third node should have nothing.
	stats = firstLocalStatsReader.Get(indexKeyPrimary.TableID, indexKeyPrimary.IndexID)
	require.Equal(t, roachpb.IndexUsageStatistics{}, stats, "expecting empty stats on node 3, but found %v", stats)

	stats = thirdLocalStatsReader.Get(indexKeyA.TableID, indexKeyA.IndexID)
	require.Equal(t, roachpb.IndexUsageStatistics{}, stats, "expecting empty stats on node 3, but found %v", stats)

	stats = thirdLocalStatsReader.Get(indexKeyB.TableID, indexKeyB.IndexID)
	require.Equal(t, roachpb.IndexUsageStatistics{}, stats, "expecting empty stats on node 3, but found %v", stats)

	// Fourth node should have nothing - total_reads and last_read columns should not be populated.
	stats = fourthLocalStatsReader.Get(fourthNodeIndexKeyPrimary.TableID, fourthNodeIndexKeyPrimary.IndexID)
	require.Equal(t, roachpb.IndexUsageStatistics{}, stats, "expecting empty stats on node 4, but found %v", stats)

	stats = fourthLocalStatsReader.Get(fourthNodeindexKeyA.TableID, fourthNodeindexKeyA.IndexID)
	require.Equal(t, roachpb.IndexUsageStatistics{}, stats, "expecting empty stats on node 4, but found %v", stats)

	// Second server should have nonempty local storage.
	stats = secondLocalStatsReader.Get(indexKeyPrimary.TableID, indexKeyPrimary.IndexID)
	compareStatsHelper(t, expectedStatsIndexPrimary, stats, time.Minute)

	stats = secondLocalStatsReader.Get(indexKeyA.TableID, indexKeyA.IndexID)
	compareStatsHelper(t, expectedStatsIndexA, stats, time.Minute)

	stats = secondLocalStatsReader.Get(indexKeyB.TableID, indexKeyB.IndexID)
	compareStatsHelper(t, expectedStatsIndexB, stats, time.Minute)

	// Test cluster-wide RPC.
	var resp serverpb.IndexUsageStatisticsResponse
	err = srvtestutils.GetStatusJSONProto(thirdServer, "indexusagestatistics", &resp)
	require.NoError(t, err)

	statsEntries := 0
	for _, stats := range resp.Statistics {
		// Skip if the table is not what we expected.
		if stats.Key.TableID != roachpb.TableID(tableID) {
			continue
		}
		statsEntries++
		switch stats.Key.IndexID {
		case indexKeyPrimary.IndexID: // t@t_pkey
			compareStatsHelper(t, expectedStatsIndexPrimary, stats.Stats, time.Minute)
		case indexKeyA.IndexID: // t@t_a_idx
			compareStatsHelper(t, expectedStatsIndexA, stats.Stats, time.Minute)
		case indexKeyB.IndexID: // t@t_b_idx
			compareStatsHelper(t, expectedStatsIndexB, stats.Stats, time.Minute)
		}
	}

	require.Equal(t, 3, statsEntries, "expect to find 3 stats entries in RPC response, but found %d", statsEntries)

	// Test disabling subsystem.
	_, err = secondServerSQLConn.Exec("SET CLUSTER SETTING sql.metrics.index_usage_stats.enabled = false")
	require.NoError(t, err)

	// Records a non-full scan, but it shouldn't change the stats since we have
	// stats collection disabled.
	_, err = secondServerSQLConn.Exec("SELECT k, a FROM t WHERE a = 0")
	require.NoError(t, err)

	err = srvtestutils.GetStatusJSONProto(thirdServer, "indexusagestatistics", &resp)
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
	require.Equal(t, 3, statsEntries, "expect to find 3 stats entries in RPC response, but found %d", statsEntries)
}

func TestGetTableID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(sqlDB)

	// Create tables under public and user defined schemas.
	db.Exec(t, `
CREATE DATABASE test_db1;
SET DATABASE=test_db1;
CREATE TABLE test_table (
  k INT PRIMARY KEY,
  a INT,
  b INT,
  INDEX(a)
);
CREATE SCHEMA schema;
CREATE TABLE schema.test_table (
  k INT PRIMARY KEY,
  a INT,
  b INT,
  INDEX(a)
);
`)

	// Create tables under public and user defined schemas under a different database.
	db.Exec(t, `
CREATE DATABASE test_db2;
SET DATABASE=test_db2;
CREATE TABLE test_table (
  k INT PRIMARY KEY,
  a INT,
  b INT,
  INDEX(a)
);
CREATE SCHEMA schema;
CREATE TABLE schema.test_table (
  k INT PRIMARY KEY,
  a INT,
  b INT,
  INDEX(a)
);
`)

	// Get Table IDs.
	userName, err := authserver.UserFromIncomingRPCContext(ctx)
	require.NoError(t, err)

	testCases := []struct {
		database string
		schema   string
		table    string
	}{
		{
			database: "test_db1",
			schema:   "public",
			table:    "test_table",
		},
		{
			database: "test_db1",
			schema:   "schema",
			table:    "test_table",
		},
		{
			database: "test_db2",
			schema:   "public",
			table:    "test_table",
		},
		{
			database: "test_db2",
			schema:   "schema",
			table:    "test_table",
		},
	}

	for _, tc := range testCases {
		tableName := fmt.Sprintf("%s.%s", tc.schema, tc.table)
		tableID, databaseID, err := getIDFromDatabaseAndTableName(ctx, tc.database, tableName, s.InternalExecutor().(*sql.InternalExecutor), userName)
		require.NoError(t, err)

		// Get actual Table ID.
		actualTableID := db.QueryStr(t, `
SELECT table_id, parent_id
FROM crdb_internal.tables 
WHERE database_name=$1 AND schema_name=$2 AND name=$3`,
			tc.database, tc.schema, tc.table)

		// Assert Table ID is correct.
		require.Equal(t, fmt.Sprint(tableID), actualTableID[0][0])
		require.Equal(t, fmt.Sprint(databaseID), actualTableID[0][1])
	}
}
