// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package statusccl

import (
	"context"
	gosql "database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestTenantStatusAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The liveness session might expire before the stress race can finish.
	skip.UnderStressRace(t, "expensive tests")

	ctx := context.Background()

	knobs := tests.CreateTestingKnobs()

	testHelper := newTestTenantHelper(t, 3 /* tenantClusterSize */, knobs)
	defer testHelper.cleanup(ctx, t)

	for _, conn := range []*sqlutils.SQLRunner{
		sqlutils.MakeSQLRunner(testHelper.hostCluster.ServerConn(0)),
		testHelper.testCluster().tenantConn(0),
	} {
		conn.Exec(
			t, "SET CLUSTER SETTING sql.contention.txn_id_cache.max_size = '10MB'",
		)
	}

	t.Run("reset_sql_stats", func(t *testing.T) {
		testResetSQLStatsRPCForTenant(ctx, t, testHelper)
	})

	t.Run("reset_index_usage_stats", func(t *testing.T) {
		testResetIndexUsageStatsRPCForTenant(ctx, t, testHelper)
	})

	t.Run("table_index_stats", func(t *testing.T) {
		testTableIndexStats(ctx, t, testHelper)
	})

	t.Run("tenant_contention_event", func(t *testing.T) {
		testContentionEventsForTenant(ctx, t, testHelper)
	})

	t.Run("tenant_cancel_session", func(t *testing.T) {
		testTenantStatusCancelSession(t, testHelper)
	})

	t.Run("tenant_cancel_query", func(t *testing.T) {
		testTenantStatusCancelQuery(ctx, t, testHelper)
	})

	t.Run("index_usage_stats", func(t *testing.T) {
		testIndexUsageForTenants(t, testHelper)
	})

	t.Run("txn_id_resolution", func(t *testing.T) {
		testTxnIDResolutionRPC(ctx, t, testHelper)
	})

	t.Run("tenant_ranges", func(t *testing.T) {
		testTenantRangesRPC(ctx, t, testHelper)
	})
}

func TestTenantCannotSeeNonTenantStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	serverParams, _ := tests.CreateTestServerParams()
	serverParams.Knobs.SpanConfig = &spanconfig.TestingKnobs{
		ManagerDisableJobCreation: true, // TODO(irfansharif): #74919.
	}
	testCluster := serverutils.StartNewTestCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: serverParams,
	})
	defer testCluster.Stopper().Stop(ctx)

	server := testCluster.Server(0 /* idx */)

	tenant, sqlDB := serverutils.StartTenant(t, server, base.TestTenantArgs{
		TenantID: roachpb.MakeTenantID(10 /* id */),
		TestingKnobs: base.TestingKnobs{
			SQLStatsKnobs: &sqlstats.TestingKnobs{
				AOSTClause: "AS OF SYSTEM TIME '-1us'",
			},
		},
	})

	nonTenant := testCluster.Server(1 /* idx */)

	tenantStatusServer := tenant.StatusServer().(serverpb.SQLStatusServer)

	type testCase struct {
		stmt        string
		fingerprint string
	}

	testCaseTenant := []testCase{
		{stmt: `CREATE DATABASE roachblog_t`},
		{stmt: `SET database = roachblog_t`},
		{stmt: `CREATE TABLE posts_t (id INT8 PRIMARY KEY, body STRING)`},
		{
			stmt:        `INSERT INTO posts_t VALUES (1, 'foo')`,
			fingerprint: `INSERT INTO posts_t VALUES (_, '_')`,
		},
		{stmt: `SELECT * FROM posts_t`},
	}

	for _, stmt := range testCaseTenant {
		_, err := sqlDB.Exec(stmt.stmt)
		require.NoError(t, err)
	}

	err := sqlDB.Close()
	require.NoError(t, err)

	testCaseNonTenant := []testCase{
		{stmt: `CREATE DATABASE roachblog_nt`},
		{stmt: `SET database = roachblog_nt`},
		{stmt: `CREATE TABLE posts_nt (id INT8 PRIMARY KEY, body STRING)`},
		{
			stmt:        `INSERT INTO posts_nt VALUES (1, 'foo')`,
			fingerprint: `INSERT INTO posts_nt VALUES (_, '_')`,
		},
		{stmt: `SELECT * FROM posts_nt`},
	}

	pgURL, cleanupGoDB := sqlutils.PGUrl(
		t, nonTenant.ServingSQLAddr(), "CreateConnections" /* prefix */, url.User(security.RootUser))
	defer cleanupGoDB()
	sqlDB, err = gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)

	for _, stmt := range testCaseNonTenant {
		_, err = sqlDB.Exec(stmt.stmt)
		require.NoError(t, err)
	}
	err = sqlDB.Close()
	require.NoError(t, err)

	request := &serverpb.StatementsRequest{}
	combinedStatsRequest := &serverpb.CombinedStatementsStatsRequest{}
	var tenantStats *serverpb.StatementsResponse
	var tenantCombinedStats *serverpb.StatementsResponse

	// Populate `tenantStats` and `tenantCombinedStats`. The tenant server
	// `Statements` and `CombinedStatements` methods are backed by the
	// sqlinstance system which uses a cache populated through rangefeed
	// for keeping track of SQL pod data. We use `SucceedsSoon` to eliminate
	// race condition with the sqlinstance cache population such as during
	// a stress test.
	testutils.SucceedsSoon(t, func() error {
		tenantStats, err = tenantStatusServer.Statements(ctx, request)
		if err != nil {
			return err
		}
		if tenantStats == nil || len(tenantStats.Statements) == 0 {
			return errors.New("tenant statements are unexpectedly empty")
		}

		tenantCombinedStats, err = tenantStatusServer.CombinedStatementStats(ctx, combinedStatsRequest)
		if tenantCombinedStats == nil || len(tenantCombinedStats.Statements) == 0 {
			return errors.New("tenant combined statements are unexpectedly empty")
		}
		return nil
	})

	path := "/_status/statements"
	var nonTenantStats serverpb.StatementsResponse
	err = serverutils.GetJSONProto(nonTenant, path, &nonTenantStats)
	require.NoError(t, err)

	path = "/_status/combinedstmts"
	var nonTenantCombinedStats serverpb.StatementsResponse
	err = serverutils.GetJSONProto(nonTenant, path, &nonTenantCombinedStats)
	require.NoError(t, err)

	checkStatements := func(t *testing.T, tc []testCase, actual *serverpb.StatementsResponse) {
		t.Helper()
		var expectedStatements []string
		for _, stmt := range tc {
			var expectedStmt = stmt.stmt
			if stmt.fingerprint != "" {
				expectedStmt = stmt.fingerprint
			}
			expectedStatements = append(expectedStatements, expectedStmt)
		}

		var actualStatements []string
		for _, respStatement := range actual.Statements {
			if respStatement.Key.KeyData.Failed {
				// We ignore failed statements here as the INSERT statement can fail and
				// be automatically retried, confusing the test success check.
				continue
			}
			if strings.HasPrefix(respStatement.Key.KeyData.App, catconstants.InternalAppNamePrefix) {
				// We ignore internal queries, these are not relevant for the
				// validity of this test.
				continue
			}
			actualStatements = append(actualStatements, respStatement.Key.KeyData.Query)
		}

		sort.Strings(expectedStatements)
		sort.Strings(actualStatements)

		require.Equal(t, expectedStatements, actualStatements)
	}

	// First we verify that we have expected stats from tenants.
	t.Run("tenant-stats", func(t *testing.T) {
		checkStatements(t, testCaseTenant, tenantStats)
		checkStatements(t, testCaseTenant, tenantCombinedStats)
	})

	// Now we verify the non tenant stats are what we expected.
	t.Run("non-tenant-stats", func(t *testing.T) {
		checkStatements(t, testCaseNonTenant, &nonTenantStats)
		checkStatements(t, testCaseNonTenant, &nonTenantCombinedStats)
	})

	// Now we verify that tenant and non-tenant have no visibility into each other's stats.
	t.Run("overlap", func(t *testing.T) {
		for _, tenantStmt := range tenantStats.Statements {
			for _, nonTenantStmt := range nonTenantStats.Statements {
				require.NotEqual(t, tenantStmt, nonTenantStmt, "expected tenant to have no visibility to non-tenant's statement stats, but found:", nonTenantStmt)
			}
		}

		for _, tenantTxn := range tenantStats.Transactions {
			for _, nonTenantTxn := range nonTenantStats.Transactions {
				require.NotEqual(t, tenantTxn, nonTenantTxn, "expected tenant to have no visibility to non-tenant's transaction stats, but found:", nonTenantTxn)
			}
		}

		for _, tenantStmt := range tenantCombinedStats.Statements {
			for _, nonTenantStmt := range nonTenantCombinedStats.Statements {
				require.NotEqual(t, tenantStmt, nonTenantStmt, "expected tenant to have no visibility to non-tenant's statement stats, but found:", nonTenantStmt)
			}
		}

		for _, tenantTxn := range tenantCombinedStats.Transactions {
			for _, nonTenantTxn := range nonTenantCombinedStats.Transactions {
				require.NotEqual(t, tenantTxn, nonTenantTxn, "expected tenant to have no visibility to non-tenant's transaction stats, but found:", nonTenantTxn)
			}
		}
	})
}

func testResetSQLStatsRPCForTenant(
	ctx context.Context, t *testing.T, testHelper *tenantTestHelper,
) {
	stmts := []string{
		"SELECT 1",
		"SELECT 1, 1",
		"SELECT 1, 1, 1",
	}

	testCluster := testHelper.testCluster()
	controlCluster := testHelper.controlCluster()

	// Disable automatic flush to ensure tests are deterministic.
	testCluster.tenantConn(0 /* idx */).
		Exec(t, "SET CLUSTER SETTING sql.stats.flush.enabled = false")
	controlCluster.tenantConn(0 /* idx */).
		Exec(t, "SET CLUSTER SETTING sql.stats.flush.enabled = false")

	defer func() {
		// Cleanup
		testCluster.tenantConn(0 /* idx */).
			Exec(t, "SET CLUSTER SETTING sql.stats.flush.enabled = true")
		controlCluster.tenantConn(0 /* idx */).
			Exec(t, "SET CLUSTER SETTING sql.stats.flush.enabled = true")

	}()

	for _, flushed := range []bool{false, true} {
		t.Run(fmt.Sprintf("flushed=%t", flushed), func(t *testing.T) {
			// Clears the SQL Stats at the end of each test via builtin.
			defer func() {
				testCluster.tenantConn(randomServer).Exec(t, "SELECT crdb_internal.reset_sql_stats()")
				controlCluster.tenantConn(randomServer).Exec(t, "SELECT crdb_internal.reset_sql_stats()")
			}()

			for _, stmt := range stmts {
				testCluster.tenantConn(randomServer).Exec(t, stmt)
				controlCluster.tenantConn(randomServer).Exec(t, stmt)
			}

			if flushed {
				testCluster.tenantSQLStats(randomServer).Flush(ctx)
				controlCluster.tenantSQLStats(randomServer).Flush(ctx)
			}

			status := testCluster.tenantStatusSrv(randomServer)

			statsPreReset, err := status.Statements(ctx, &serverpb.StatementsRequest{
				Combined: true,
			})
			require.NoError(t, err)

			require.NotEqual(t, 0, len(statsPreReset.Statements),
				"expected to find stats for at least one statement, but found: %d", len(statsPreReset.Statements))
			ensureExpectedStmtFingerprintExistsInRPCResponse(t, stmts, statsPreReset, "test")

			_, err = status.ResetSQLStats(ctx, &serverpb.ResetSQLStatsRequest{
				ResetPersistedStats: true,
			})
			require.NoError(t, err)

			statsPostReset, err := status.Statements(ctx, &serverpb.StatementsRequest{
				Combined: true,
			})
			require.NoError(t, err)

			if !statsPostReset.LastReset.After(statsPreReset.LastReset) {
				t.Fatal("expected to find stats last reset value changed, but didn't")
			}

			for _, txnStatsPostReset := range statsPostReset.Transactions {
				for _, txnStatsPreReset := range statsPreReset.Transactions {
					require.NotEqual(t, txnStatsPostReset, txnStatsPreReset,
						"expected to have reset SQL stats, but still found transaction %+v", txnStatsPostReset)
				}
			}

			for _, stmtStatsPostReset := range statsPostReset.Statements {
				for _, stmtStatsPreReset := range statsPreReset.Statements {
					require.NotEqual(t, stmtStatsPostReset, stmtStatsPreReset,
						"expected to have reset SQL stats, but still found statement %+v", stmtStatsPostReset)
				}
			}

			// Ensures that sql stats reset is isolated by tenant boundary.
			statsFromControlCluster, err :=
				controlCluster.tenantStatusSrv(randomServer).Statements(ctx, &serverpb.StatementsRequest{
					Combined: true,
				})
			require.NoError(t, err)

			ensureExpectedStmtFingerprintExistsInRPCResponse(t, stmts, statsFromControlCluster, "control")
		})
	}
}

func testResetIndexUsageStatsRPCForTenant(
	ctx context.Context, t *testing.T, testHelper *tenantTestHelper,
) {
	testCases := []struct {
		name    string
		resetFn func(helper *tenantTestHelper)
	}{
		{
			name: "sql-cli",
			resetFn: func(helper *tenantTestHelper) {
				// Reset index usage stats using SQL shell built-in.
				testingCluster := helper.testCluster()
				testingCluster.tenantConn(0).Exec(t, "SELECT crdb_internal.reset_index_usage_stats()")
			},
		},
		{
			name: "http",
			resetFn: func(helper *tenantTestHelper) {
				// Reset index usage stats over HTTP on tenant SQL pod 1.
				httpPod1 := helper.testCluster().tenantHTTPClient(t, 1)
				defer httpPod1.Close()
				httpPod1.PostJSON("/_status/resetindexusagestats", &serverpb.ResetIndexUsageStatsRequest{}, &serverpb.ResetIndexUsageStatsResponse{})
			},
		},
	}

	for _, testCase := range testCases {
		testingCluster := testHelper.testCluster()
		controlCluster := testHelper.controlCluster()

		t.Run(testCase.name, func(t *testing.T) {
			var testingTableID, controlTableID string
			for i, cluster := range []tenantCluster{testingCluster, controlCluster} {
				// Create tables and insert data.
				cluster.tenantConn(0).Exec(t, `
CREATE TABLE test (
  k INT PRIMARY KEY,
  a INT,
  b INT,
  INDEX(a)
)
`)

				cluster.tenantConn(0).Exec(t, `
INSERT INTO test
VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)
`)

				// Record scan on primary index.
				cluster.tenantConn(randomServer).
					Exec(t, "SELECT * FROM test")

				// Record scan on secondary index.
				cluster.tenantConn(randomServer).
					Exec(t, "SELECT * FROM test@test_a_idx")
				testTableIDStr := cluster.tenantConn(randomServer).
					QueryStr(t, "SELECT 'test'::regclass::oid")[0][0]

				// Set table ID outside of loop.
				if i == 0 {
					testingTableID = testTableIDStr
				} else {
					controlTableID = testTableIDStr
				}

				query := `
SELECT
  table_id,
  index_id,
  total_reads,
  extract_duration('second', now() - last_read) < 5
FROM
  crdb_internal.index_usage_statistics
WHERE
  table_id = ` + testTableIDStr
				// Assert index usage data was inserted.
				expected := [][]string{
					{testTableIDStr, "1", "1", "true"},
					{testTableIDStr, "2", "1", "true"},
				}
				cluster.tenantConn(randomServer).CheckQueryResults(t, query, expected)
			}

			// Reset index usage stats.
			timePreReset := timeutil.Now()
			status := testingCluster.tenantStatusSrv(randomServer)

			// Reset index usage stats.
			testCase.resetFn(testHelper)

			// Check that last reset time was updated for test cluster.
			resp, err := status.IndexUsageStatistics(ctx, &serverpb.IndexUsageStatisticsRequest{})
			require.NoError(t, err)
			require.True(t, resp.LastReset.After(timePreReset))

			// Ensure tenant data isolation.
			// Check that last reset time was not updated for control cluster.
			status = controlCluster.tenantStatusSrv(randomServer)
			resp, err = status.IndexUsageStatistics(ctx, &serverpb.IndexUsageStatisticsRequest{})
			require.NoError(t, err)
			require.Equal(t, resp.LastReset, time.Time{})

			// Query to fetch index usage stats. We do this instead of sending
			// an RPC request so that we can filter by table id.
			query := `
SELECT
  table_id,
  total_reads,
  last_read
FROM
  crdb_internal.index_usage_statistics
WHERE
  table_id = $1
`

			// Check that index usage stats were reset.
			rows := testingCluster.tenantConn(2).QueryStr(t, query, testingTableID)
			require.NotNil(t, rows)
			for _, row := range rows {
				require.Equal(t, row[1], "0", "expected total reads for table %s to be reset, but got %s",
					row[0], row[1])
				require.Equal(t, row[2], "NULL", "expected last read time for table %s to be reset, but got %s",
					row[0], row[2])
			}

			// Ensure tenant data isolation.
			rows = controlCluster.tenantConn(0).QueryStr(t, query, controlTableID)
			require.NotNil(t, rows)
			for _, row := range rows {
				require.NotEqual(t, row[1], "0", "expected total reads for table %s to not be reset, but got %s", row[0], row[1])
				require.NotEqual(t, row[2], "NULL", "expected last read time for table %s to not be reset, but got %s", row[0], row[2])
			}

			// Cleanup.
			testingCluster.tenantConn(0).Exec(t, "DROP TABLE IF EXISTS test")
			controlCluster.tenantConn(0).Exec(t, "DROP TABLE IF EXISTS test")
		})
	}
}

func testTableIndexStats(ctx context.Context, t *testing.T, testHelper *tenantTestHelper) {
	getTableIndexStats := func(t *testing.T, helper *tenantTestHelper, db string) *serverpb.TableIndexStatsResponse {
		// Get index usage stats using function call.
		cluster := helper.testCluster()
		status := cluster.tenantStatusSrv(randomServer)
		req := &serverpb.TableIndexStatsRequest{Table: "test", Database: db}
		resp, err := status.TableIndexStats(ctx, req)
		require.NoError(t, err)
		return resp
	}

	cluster := testHelper.testCluster()

	timePreCreate := timeutil.Now()

	// Create table on a database.
	cluster.tenantConn(0).Exec(t, `
CREATE DATABASE test_db1;
SET DATABASE=test_db1;
CREATE TABLE test (
  k INT PRIMARY KEY,
  a INT,
  b INT,
  INDEX(a)
);`)

	// Create second table on different database.
	cluster.tenantConn(0).Exec(t, `
CREATE DATABASE test_db2;
SET DATABASE=test_db2;
CREATE TABLE test (
  k INT PRIMARY KEY,
  a INT,
  b INT,
  INDEX(a, b)
);`)

	// Record scan on primary index.
	timePreRead := timeutil.Now()
	cluster.tenantConn(0).Exec(t, `
SET DATABASE=test_db1;
SELECT * FROM test;
`)

	getCreateStmtQuery := `
SELECT indexdef
FROM pg_catalog.pg_indexes
WHERE tablename = 'test' AND indexname = $1`

	// Get index usage stats and assert expected results.
	requireAfter := func(t *testing.T, a, b *time.Time) {
		t.Helper()
		require.NotNil(t, a)
		require.NotNil(t, b)
		require.Truef(t, a.After(*b), "%v is not after %v", a, b)
	}
	requireBetween := func(t *testing.T, before time.Time, ts *time.Time, after time.Time) {
		t.Helper()
		requireAfter(t, ts, &before)
		requireAfter(t, &after, ts)
	}

	t.Run("validate read index", func(t *testing.T) {
		resp := getTableIndexStats(t, testHelper, "test_db1")
		require.Equal(t, uint64(1), resp.Statistics[0].Statistics.Stats.TotalReadCount)
		requireAfter(t, &resp.Statistics[0].Statistics.Stats.LastRead, &timePreRead)
		indexName := resp.Statistics[0].IndexName
		createStmt := cluster.tenantConn(0).QueryStr(t, getCreateStmtQuery, indexName)[0][0]
		print(createStmt)
		require.Equal(t, resp.Statistics[0].CreateStatement, createStmt)
		requireBetween(t, timePreCreate, resp.Statistics[0].CreatedAt, timePreRead)
	})

	t.Run("validate unread index", func(t *testing.T) {
		resp := getTableIndexStats(t, testHelper, "test_db2")
		require.Equal(t, uint64(0), resp.Statistics[0].Statistics.Stats.TotalReadCount)
		require.Equal(t, resp.Statistics[0].Statistics.Stats.LastRead, time.Time{})
		indexName := resp.Statistics[0].IndexName
		cluster.tenantConn(0).Exec(t, `SET DATABASE=test_db2`)
		createStmt := cluster.tenantConn(0).QueryStr(t, getCreateStmtQuery, indexName)[0][0]
		require.Equal(t, resp.Statistics[0].CreateStatement, createStmt)
		requireBetween(t, timePreCreate, resp.Statistics[0].CreatedAt, timePreRead)
	})

	// Test that a subsequent index creation has an appropriate timestamp.
	t.Run("validate CreatedAt for new index", func(t *testing.T) {
		timeBeforeCreateNewIndex := timeutil.Now()
		cluster.tenantConn(0).Exec(t, `
SET DATABASE=test_db2;
CREATE INDEX idx2 ON test (b, a)`)
		timeAfterCreateNewIndex := timeutil.Now()

		resp := getTableIndexStats(t, testHelper, "test_db2")
		var stat serverpb.TableIndexStatsResponse_ExtendedCollectedIndexUsageStatistics
		var found bool
		for _, idx := range resp.Statistics {
			if found = idx.IndexName == "idx2"; found {
				stat = *idx
				break
			}
		}
		require.True(t, found)
		requireBetween(t,
			timeBeforeCreateNewIndex, stat.CreatedAt, timeAfterCreateNewIndex)
	})
}

func ensureExpectedStmtFingerprintExistsInRPCResponse(
	t *testing.T, expectedStmts []string, resp *serverpb.StatementsResponse, clusterType string,
) {
	t.Helper()

	for _, stmt := range expectedStmts {
		fingerprint := strings.Replace(stmt, "1", "_", -1)
		found := false
		for _, foundStmt := range resp.Statements {
			if !strings.Contains(foundStmt.Key.KeyData.App, resp.InternalAppNamePrefix) {
				if fingerprint == foundStmt.Key.KeyData.Query {
					found = true
					break
				}
			}
		}
		require.True(t, found, "expected %s to be found in "+
			"%s tenant cluster, but it was not found", fingerprint, clusterType)
	}
}

func testContentionEventsForTenant(
	ctx context.Context, t *testing.T, testHelper *tenantTestHelper,
) {
	testingCluster := testHelper.testCluster()
	controlledCluster := testHelper.controlCluster()

	sqlutils.CreateTable(
		t,
		testingCluster[0].tenantConn,
		"test",
		"x INT PRIMARY KEY",
		1, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)

	testTableID, err :=
		strconv.Atoi(testingCluster.tenantConn(0).QueryStr(t, "SELECT 'test.test'::regclass::oid")[0][0])
	require.NoError(t, err)

	testingCluster.tenantConn(0).Exec(t, "USE test")
	testingCluster.tenantConn(1).Exec(t, "USE test")

	testingCluster.tenantConn(0).Exec(t, `
BEGIN;
UPDATE test SET x = 100 WHERE x = 1;
`)
	testingCluster.tenantConn(1).Exec(t, `
SET TRACING=on;
BEGIN PRIORITY HIGH;
UPDATE test SET x = 1000 WHERE x = 1;
COMMIT;
SET TRACING=off;
`)
	testingCluster.tenantConn(0).ExpectErr(
		t,
		"^pq: restart transaction.+",
		"COMMIT;",
	)

	resp, err :=
		testingCluster.tenantStatusSrv(2).ListContentionEvents(ctx, &serverpb.ListContentionEventsRequest{})
	require.NoError(t, err)

	require.GreaterOrEqualf(t, len(resp.Events.IndexContentionEvents), 1,
		"expecting at least 1 contention event, but found none")

	found := false
	for _, event := range resp.Events.IndexContentionEvents {
		if event.TableID == descpb.ID(testTableID) && event.IndexID == descpb.IndexID(1) {
			found = true
			break
		}
	}

	require.True(t, found,
		"expect to find contention event for table %d, but found %+v", testTableID, resp)

	resp, err = controlledCluster.tenantStatusSrv(0).ListContentionEvents(ctx, &serverpb.ListContentionEventsRequest{})
	require.NoError(t, err)
	for _, event := range resp.Events.IndexContentionEvents {
		if event.TableID == descpb.ID(testTableID) && event.IndexID == descpb.IndexID(1) {
			t.Errorf("did not expect contention event in controlled cluster, but it was found")
		}
	}

	testutils.SucceedsWithin(t, func() error {
		err = testHelper.testCluster().tenantContentionRegistry(1).FlushEventsForTest(ctx)
		if err != nil {
			return err
		}

		resp := &serverpb.TransactionContentionEventsResponse{}
		testHelper.
			testCluster().
			tenantHTTPClient(t, 1).
			GetJSON("/_status/transactioncontentionevents", resp)

		if len(resp.Events) == 0 {
			return errors.New("expected transaction contention events being populated, " +
				"but it is not")
		}

		return nil
	}, 5*time.Second)
}

func testIndexUsageForTenants(t *testing.T, testHelper *tenantTestHelper) {
	testingCluster := testHelper.testCluster()
	controlledCluster := testHelper.controlCluster()

	testingCluster.tenantConn(0).Exec(t, "USE defaultdb")
	testingCluster.tenantConn(1).Exec(t, "USE defaultdb")
	testingCluster.tenantConn(2).Exec(t, "USE defaultdb")
	testingCluster.tenantConn(0).Exec(t, `CREATE SCHEMA idx_test`)

	testingCluster.tenantConn(0).Exec(t, `
CREATE TABLE idx_test.test (
  k INT PRIMARY KEY,
  a INT,
  b INT,
  INDEX(a)
)
`)

	defer func() {
		testingCluster.tenantConn(0).Exec(t, "DROP TABLE idx_test.test")
	}()

	testingCluster.tenantConn(0).Exec(t, `
INSERT INTO idx_test.test
VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)
`)

	// Record scan on primary index.
	testingCluster.tenantConn(0).Exec(t, "SELECT * FROM idx_test.test")

	// Record scan on secondary index.
	testingCluster.tenantConn(1).Exec(t, "SELECT * FROM idx_test.test@test_a_idx")
	testTableIDStr := testingCluster.tenantConn(2).QueryStr(t, "SELECT 'idx_test.test'::regclass::oid")[0][0]
	testTableID, err := strconv.Atoi(testTableIDStr)
	require.NoError(t, err)

	query := `
SELECT
  table_id,
  index_id,
  total_reads,
  extract_duration('second', now() - last_read) < 5
FROM
  crdb_internal.index_usage_statistics
WHERE
  table_id = $1
`
	actual := testingCluster.tenantConn(2).QueryStr(t, query, testTableID)
	expected := [][]string{
		{testTableIDStr, "1", "1", "true"},
		{testTableIDStr, "2", "1", "true"},
	}

	require.Equal(t, expected, actual)

	// Ensure tenant data isolation.
	actual = controlledCluster.tenantConn(0).QueryStr(t, query, testTableID)
	expected = [][]string{}

	require.Equal(t, expected, actual)
}

func selectClusterSessionIDs(t *testing.T, conn *sqlutils.SQLRunner) []string {
	var sessionIDs []string
	rows := conn.QueryStr(t, "SELECT session_id FROM crdb_internal.cluster_sessions")
	for _, row := range rows {
		sessionIDs = append(sessionIDs, row[0])
	}
	return sessionIDs
}

func testTenantStatusCancelSession(t *testing.T, helper *tenantTestHelper) {
	// Open a SQL session on tenant SQL pod 0.
	sqlPod0 := helper.testCluster().tenantConn(0)
	sqlPod0.Exec(t, "SELECT 1")

	// See the session over HTTP on tenant SQL pod 1.
	httpPod1 := helper.testCluster().tenantHTTPClient(t, 1)
	defer httpPod1.Close()
	listSessionsResp := serverpb.ListSessionsResponse{}
	httpPod1.GetJSON("/_status/sessions", &listSessionsResp)
	var session serverpb.Session
	for _, s := range listSessionsResp.Sessions {
		if s.LastActiveQuery == "SELECT 1" {
			session = s
			break
		}
	}
	require.NotNil(t, session.ID, "session not found")

	// See the session over SQL on tenant SQL pod 0.
	sessionID := hex.EncodeToString(session.ID)
	require.Eventually(t, func() bool {
		return strings.Contains(strings.Join(selectClusterSessionIDs(t, sqlPod0), ","), sessionID)
	}, 5*time.Second, 100*time.Millisecond)

	// Cancel the session over HTTP from tenant SQL pod 1.
	cancelSessionReq := serverpb.CancelSessionRequest{SessionID: session.ID}
	cancelSessionResp := serverpb.CancelSessionResponse{}
	httpPod1.PostJSON("/_status/cancel_session/"+session.NodeID.String(), &cancelSessionReq, &cancelSessionResp)
	require.Equal(t, true, cancelSessionResp.Canceled, cancelSessionResp.Error)

	// No longer see the session over SQL from tenant SQL pod 0.
	// (The SQL client maintains an internal connection pool and automatically reconnects.)
	require.Eventually(t, func() bool {
		return !strings.Contains(strings.Join(selectClusterSessionIDs(t, sqlPod0), ","), sessionID)
	}, 5*time.Second, 100*time.Millisecond)

	// Attempt to cancel the session again over HTTP from tenant SQL pod 1, so that we can see the error message.
	httpPod1.PostJSON("/_status/cancel_session/"+session.NodeID.String(), &cancelSessionReq, &cancelSessionResp)
	require.Equal(t, false, cancelSessionResp.Canceled)
	require.Equal(t, fmt.Sprintf("session ID %s not found", sessionID), cancelSessionResp.Error)
}

func selectClusterQueryIDs(t *testing.T, conn *sqlutils.SQLRunner) []string {
	var queryIDs []string
	rows := conn.QueryStr(t, "SELECT query_id FROM crdb_internal.cluster_queries")
	for _, row := range rows {
		queryIDs = append(queryIDs, row[0])
	}
	return queryIDs
}

func testTenantStatusCancelQuery(ctx context.Context, t *testing.T, helper *tenantTestHelper) {
	// Open a SQL session on tenant SQL pod 0 and start a long-running query.
	sqlPod0 := helper.testCluster().tenantConn(0)
	resultCh := make(chan struct{})
	errorCh := make(chan error)
	defer close(resultCh)
	defer close(errorCh)
	go func() {
		if _, err := sqlPod0.DB.ExecContext(ctx, "SELECT pg_sleep(60)"); err != nil {
			errorCh <- err
		} else {
			resultCh <- struct{}{}
		}
	}()

	// See the query over HTTP on tenant SQL pod 1.
	httpPod1 := helper.testCluster().tenantHTTPClient(t, 1)
	defer httpPod1.Close()
	var listSessionsResp serverpb.ListSessionsResponse
	var query serverpb.ActiveQuery
	require.Eventually(t, func() bool {
		httpPod1.GetJSON("/_status/sessions", &listSessionsResp)
		for _, s := range listSessionsResp.Sessions {
			for _, q := range s.ActiveQueries {
				if q.Sql == "SELECT pg_sleep(60)" {
					query = q
					break
				}
			}
		}
		return query.ID != ""
	}, 10*time.Second, 100*time.Millisecond, "query not found")

	// See the query over SQL on tenant SQL pod 0.
	require.Eventually(t, func() bool {
		return strings.Contains(strings.Join(selectClusterQueryIDs(t, sqlPod0), ","), query.ID)
	}, 10*time.Second, 100*time.Millisecond)

	// Cancel the query over HTTP on tenant SQL pod 1.
	cancelQueryReq := serverpb.CancelQueryRequest{QueryID: query.ID}
	cancelQueryResp := serverpb.CancelQueryResponse{}
	httpPod1.PostJSON("/_status/cancel_query/0", &cancelQueryReq, &cancelQueryResp)
	require.Equal(t, true, cancelQueryResp.Canceled,
		"expected query to be canceled, but encountered unexpected error %s", cancelQueryResp.Error)

	// No longer see the query over SQL on tenant SQL pod 0.
	require.Eventually(t, func() bool {
		return !strings.Contains(strings.Join(selectClusterQueryIDs(t, sqlPod0), ","), query.ID)
	}, 10*time.Second, 100*time.Millisecond,
		"expected query %s to no longer be visible in crdb_internal.cluster_queries", query.ID)

	select {
	case <-resultCh:
		t.Fatalf("Expected long-running query to have been canceled with error.")
	case err := <-errorCh:
		require.Equal(t, "pq: query execution canceled", err.Error())
	}

	// Attempt to cancel the query again over HTTP from tenant SQL pod 1, so that we can see the error message.
	httpPod1.PostJSON("/_status/cancel_query/0", &cancelQueryReq, &cancelQueryResp)
	require.Equal(t, false, cancelQueryResp.Canceled)
	require.Equal(t, fmt.Sprintf("query ID %s not found", query.ID), cancelQueryResp.Error)
}

// testTxnIDResolutionRPC tests the reachability of TxnIDResolution RPC. The
// underlying implementation correctness is tested within
// pkg/sql/contention/txnidcache.
func testTxnIDResolutionRPC(ctx context.Context, t *testing.T, helper *tenantTestHelper) {
	run := func(sqlConn *sqlutils.SQLRunner, status serverpb.SQLStatusServer, coordinatorNodeID int32) {
		sqlConn.Exec(t, "SET application_name='test1'")

		sqlConn.Exec(t, "BEGIN")
		result := sqlConn.QueryStr(t, `
		SELECT
			id
		FROM
			crdb_internal.node_transactions
		WHERE
			application_name = 'test1'`)
		require.Equal(t, 1 /* expected */, len(result),
			"expected only one active txn, but there are %d active txns found", len(result))
		txnID := uuid.FromStringOrNil(result[0][0])
		require.False(t, uuid.Nil.Equal(txnID),
			"expected a valid txnID, but %+v is found", result)
		sqlConn.Exec(t, "COMMIT")

		testutils.SucceedsSoon(t, func() error {
			resp, err := status.TxnIDResolution(ctx, &serverpb.TxnIDResolutionRequest{
				CoordinatorID: strconv.Itoa(int(coordinatorNodeID)),
				TxnIDs:        []uuid.UUID{txnID},
			})
			require.NoError(t, err)
			if len(resp.ResolvedTxnIDs) != 1 {
				return errors.Newf("expected RPC response to have length of 1, but "+
					"it is %d", len(resp.ResolvedTxnIDs))
			}
			require.Equal(t, txnID, resp.ResolvedTxnIDs[0].TxnID,
				"expected to find txn %s on coordinator node %d, but it "+
					"was not", txnID.String(), coordinatorNodeID)
			require.NotEqual(t, roachpb.InvalidTransactionFingerprintID, resp.ResolvedTxnIDs[0].TxnFingerprintID)
			return nil
		})
	}

	t.Run("regular_cluster", func(t *testing.T) {
		status :=
			helper.hostCluster.Server(0 /* idx */).StatusServer().(serverpb.SQLStatusServer)
		sqlConn := helper.hostCluster.ServerConn(0 /* idx */)
		run(sqlutils.MakeSQLRunner(sqlConn), status, 1 /* coordinatorNodeID */)
	})

	t.Run("tenant_cluster", func(t *testing.T) {
		// Select a different tenant status server here so a pod-to-pod RPC will
		// happen.
		status :=
			helper.testCluster().tenantStatusSrv(2 /* idx */)
		sqlConn := helper.testCluster().tenantConn(0 /* idx */)
		run(sqlConn, status, 1 /* coordinatorNodeID */)
	})
}

func testTenantRangesRPC(_ context.Context, t *testing.T, helper *tenantTestHelper) {
	tenantA := helper.testCluster().tenant(0).tenant.TenantStatusServer().(serverpb.TenantStatusServer)
	keyPrefixForA := keys.MakeTenantPrefix(helper.testCluster().tenant(0).tenant.RPCContext().TenantID)
	keyPrefixEndForA := keyPrefixForA.PrefixEnd()

	tenantB := helper.controlCluster().tenant(0).tenant.TenantStatusServer().(serverpb.TenantStatusServer)
	keyPrefixForB := keys.MakeTenantPrefix(helper.controlCluster().tenant(0).tenant.RPCContext().TenantID)
	keyPrefixEndForB := keyPrefixForB.PrefixEnd()

	resp, err := tenantA.TenantRanges(context.Background(), &serverpb.TenantRangesRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, resp.RangesByLocality)
	for localityKey, rangeList := range resp.RangesByLocality {
		require.NotEmpty(t, localityKey)
		for _, r := range rangeList.Ranges {
			assertStartKeyInRange(t, r.Span.StartKey, keyPrefixForA)
			assertEndKeyInRange(t, r.Span.EndKey, keyPrefixForA, keyPrefixEndForA)
		}
	}

	resp, err = tenantB.TenantRanges(context.Background(), &serverpb.TenantRangesRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, resp.RangesByLocality)
	for localityKey, rangeList := range resp.RangesByLocality {
		require.NotEmpty(t, localityKey)
		for _, r := range rangeList.Ranges {
			assertStartKeyInRange(t, r.Span.StartKey, keyPrefixForB)
			assertEndKeyInRange(t, r.Span.EndKey, keyPrefixForB, keyPrefixEndForB)
		}
	}
}

// assertStartKeyInRange compares the pretty printed startKey with the provided
// tenantPrefix key, ensuring that the startKey starts with the tenantPrefix.
func assertStartKeyInRange(t *testing.T, startKey string, tenantPrefix roachpb.Key) {
	require.Truef(t, strings.Index(startKey, tenantPrefix.String()) == 0,
		fmt.Sprintf("start key %s is outside of the tenant's keyspace (prefix: %v)",
			startKey, tenantPrefix.String()))
}

// assertEndKeyInRange compares the pretty printed endKey with the provided
// tenantPrefix and tenantPrefixEnd keys. Ensures that the key starts with
// either the tenantPrefix, or the tenantPrefixEnd (valid as end keys are
// exclusive).
func assertEndKeyInRange(
	t *testing.T, endKey string, tenantPrefix roachpb.Key, tenantPrefixEnd roachpb.Key,
) {
	require.Truef(t,
		strings.Index(endKey, tenantPrefix.String()) == 0 ||
			strings.Index(endKey, tenantPrefixEnd.String()) == 0 ||
			// Possible if the tenant's ranges fall at the end of the entire keyspace
			// range within the cluster.
			endKey == "/Max",
		fmt.Sprintf("end key %s is outside of the tenant's keyspace (prefix: %v, prefixEnd: %v)",
			endKey, tenantPrefix.String(), tenantPrefixEnd.String()))
}
