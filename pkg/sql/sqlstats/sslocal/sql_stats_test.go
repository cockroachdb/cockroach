// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sslocal_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestStmtStatsBulkIngestWithRandomMetadata generates a sequence of random
// serverpb.StatementsResponse_CollectedStatementStatistics that simulates the
// response from RPC fanout, and use a temporary SQLStats object to ingest
// that sequence. This test checks if the metadata are being properly
// updated in the temporary SQLStats object.
func TestStmtStatsBulkIngestWithRandomMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var testData []serverpb.StatementsResponse_CollectedStatementStatistics

	for i := 0; i < 50; i++ {
		var stats serverpb.StatementsResponse_CollectedStatementStatistics
		randomData := sqlstatsutil.GetRandomizedCollectedStatementStatisticsForTest(t)
		stats.Key.KeyData = randomData.Key
		testData = append(testData, stats)
	}

	sqlStats, err := sslocal.NewTempSQLStatsFromExistingStmtStats(testData)
	require.NoError(t, err)

	require.NoError(t,
		sqlStats.IterateStatementStats(
			context.Background(),
			&sqlstats.IteratorOptions{},
			func(
				ctx context.Context,
				statistics *roachpb.CollectedStatementStatistics,
			) error {
				var found bool
				for i := range testData {
					if testData[i].Key.KeyData == statistics.Key {
						found = true
						break
					}
				}
				require.True(t, found, "expected metadata %+v, but not found", statistics.Key)
				return nil
			}))

}

func TestSQLStatsStmtStatsBulkIngest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testData := []struct {
		id    roachpb.StmtFingerprintID
		key   roachpb.StatementStatisticsKey
		stats roachpb.StatementStatistics
	}{
		{
			id: 0,
			key: roachpb.StatementStatisticsKey{
				App:      "app1",
				Query:    "SELECT 1",
				Database: "testdb",
			},
			stats: roachpb.StatementStatistics{
				Count: 7,
			},
		},
		{
			id: 0,
			key: roachpb.StatementStatisticsKey{
				App:      "app0",
				Query:    "SELECT 1",
				Database: "testdb",
			},
			stats: roachpb.StatementStatistics{
				Count: 2,
			},
		},
		{
			id: 1,
			key: roachpb.StatementStatisticsKey{
				App:      "app100",
				Query:    "SELECT 1,1",
				Database: "testdb",
			},
			stats: roachpb.StatementStatistics{
				Count: 31,
			},
		},
		{
			id: 1,
			key: roachpb.StatementStatisticsKey{
				App:      "app0",
				Query:    "SELECT 1,1",
				Database: "testdb",
			},
			stats: roachpb.StatementStatistics{
				Count: 32,
			},
		},
		{
			id: 0,
			key: roachpb.StatementStatisticsKey{
				App:      "app1",
				Query:    "SELECT 1",
				Database: "testdb",
			},
			stats: roachpb.StatementStatistics{
				Count: 33,
			},
		},
		{
			id: 1,
			key: roachpb.StatementStatisticsKey{
				App:      "app100",
				Query:    "SELECT 1,1",
				Database: "testdb",
			},
			stats: roachpb.StatementStatistics{
				Count: 2,
			},
		},
	}

	expectedCount := make(map[string]int64)
	input :=
		make([]serverpb.StatementsResponse_CollectedStatementStatistics, 0, len(testData))

	for i := range testData {
		var stats serverpb.StatementsResponse_CollectedStatementStatistics
		stats.Stats = testData[i].stats
		stats.ID = testData[i].id
		stats.Key.KeyData = testData[i].key
		input = append(input, stats)
		expectedCountKey := testData[i].key.App + testData[i].key.Query
		if count, ok := expectedCount[expectedCountKey]; ok {
			expectedCount[expectedCountKey] = testData[i].stats.Count + count
		} else {
			expectedCount[expectedCountKey] = testData[i].stats.Count
		}
	}

	sqlStats, err := sslocal.NewTempSQLStatsFromExistingStmtStats(input)
	require.NoError(t, err)

	foundStats := make(map[string]int64)
	require.NoError(t,
		sqlStats.IterateStatementStats(
			context.Background(),
			&sqlstats.IteratorOptions{},
			func(
				ctx context.Context,
				statistics *roachpb.CollectedStatementStatistics,
			) error {
				require.Equal(t, "testdb", statistics.Key.Database)
				foundStats[statistics.Key.App+statistics.Key.Query] = statistics.Stats.Count
				return nil
			}))

	require.Equal(t, expectedCount, foundStats)
}

func TestSQLStatsTxnStatsBulkIngest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testData := []struct {
		stats roachpb.CollectedTransactionStatistics
	}{
		{
			stats: roachpb.CollectedTransactionStatistics{
				TransactionFingerprintID: roachpb.TransactionFingerprintID(0),
				App:                      "app1",
				Stats: roachpb.TransactionStatistics{
					Count: 7,
				},
			},
		},
		{
			stats: roachpb.CollectedTransactionStatistics{
				TransactionFingerprintID: roachpb.TransactionFingerprintID(0),
				App:                      "app0",
				Stats: roachpb.TransactionStatistics{
					Count: 2,
				},
			},
		},
		{
			stats: roachpb.CollectedTransactionStatistics{
				TransactionFingerprintID: roachpb.TransactionFingerprintID(1),
				App:                      "app100",
				Stats: roachpb.TransactionStatistics{
					Count: 31,
				},
			},
		},
		{
			stats: roachpb.CollectedTransactionStatistics{
				TransactionFingerprintID: roachpb.TransactionFingerprintID(1),
				App:                      "app0",
				Stats: roachpb.TransactionStatistics{
					Count: 32,
				},
			},
		},
		{
			stats: roachpb.CollectedTransactionStatistics{
				TransactionFingerprintID: roachpb.TransactionFingerprintID(0),
				App:                      "app1",
				Stats: roachpb.TransactionStatistics{
					Count: 33,
				},
			},
		},
		{
			stats: roachpb.CollectedTransactionStatistics{
				TransactionFingerprintID: roachpb.TransactionFingerprintID(1),
				App:                      "app100",
				Stats: roachpb.TransactionStatistics{
					Count: 2,
				},
			},
		},
	}

	expectedCount := make(map[roachpb.TransactionFingerprintID]int64)
	input :=
		make([]serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics, 0, len(testData))

	for i := range testData {
		var stats serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics
		stats.StatsData.Stats = testData[i].stats.Stats
		input = append(input, stats)
		if count, ok := expectedCount[stats.StatsData.TransactionFingerprintID]; ok {
			expectedCount[stats.StatsData.TransactionFingerprintID] = testData[i].stats.Stats.Count + count
		} else {
			expectedCount[stats.StatsData.TransactionFingerprintID] = testData[i].stats.Stats.Count
		}
	}

	sqlStats, err := sslocal.NewTempSQLStatsFromExistingTxnStats(input)
	require.NoError(t, err)

	foundStats := make(map[roachpb.TransactionFingerprintID]int64)
	require.NoError(t,
		sqlStats.IterateTransactionStats(
			context.Background(),
			&sqlstats.IteratorOptions{},
			func(
				ctx context.Context,
				statistics *roachpb.CollectedTransactionStatistics,
			) error {
				foundStats[statistics.TransactionFingerprintID] = statistics.Stats.Count
				return nil
			}))

	require.Equal(t, expectedCount, foundStats)
}

// TestNodeLocalInMemoryViewDoesNotReturnPersistedStats tests the persisted
// statistics is not returned from the in-memory only view after the stats
// are flushed to disk.
func TestNodeLocalInMemoryViewDoesNotReturnPersistedStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	params, _ := tests.CreateTestServerParams()
	cluster := serverutils.StartNewTestCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: params,
	})

	server := cluster.Server(0 /* idx */)

	// Open two connections so that we can run statements without messing up
	// the SQL stats.
	testConn := cluster.ServerConn(0 /* idx */)
	sqlDB := sqlutils.MakeSQLRunner(testConn)
	defer cluster.Stopper().Stop(ctx)

	sqlDB.Exec(t, "SET application_name = 'app1'")
	sqlDB.Exec(t, "SELECT 1 WHERE true")

	sqlDB.CheckQueryResults(t, `
SELECT
  key, count
FROM
  crdb_internal.node_statement_statistics
WHERE
  application_name = 'app1' AND
  key LIKE 'SELECT _ WHERE%'
`, [][]string{{"SELECT _ WHERE _", "1"}})

	server.SQLServer().(*sql.Server).
		GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats).Flush(ctx)

	sqlDB.CheckQueryResults(t, `
SELECT
  key, count
FROM
  crdb_internal.node_statement_statistics
WHERE
  application_name = 'app1'
`, [][]string{})

	sqlDB.Exec(t, "SELECT 1 WHERE 1 = 1")
	sqlDB.CheckQueryResults(t, `
SELECT
  key, count
FROM
  crdb_internal.node_statement_statistics
WHERE
  application_name = 'app1' AND
  key LIKE 'SELECT _ WHERE%'
`, [][]string{{"SELECT _ WHERE _ = _", "1"}})

}

func TestStatementStatsGroupingInExplicitTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLStatsKnobs = &sqlstats.TestingKnobs{
		StubTimeNow: func() time.Time {
			return timeutil.Now()
		},
	}

	statements := []string{
		"SET application_name = 'app1'",
		// Implicit transactions.
		"SELECT 1",
		"SELECT 1",
		"SELECT 1, 1",
		// Single explicit transaction.
		"BEGIN",
		"SELECT 1",
		"SELECT 1, 1",
		"SELECT 1, 1",
		"COMMIT",
		// One explicit transaction with multiple executions.
		"BEGIN",
		"SELECT 1",
		"SELECT 1, 1",
		"SELECT 1, 1",
		"SELECT 1, 1",
		"COMMIT",
		"BEGIN",
		"SELECT 1",
		"SELECT 1, 1",
		"SELECT 1, 1",
		"SELECT 1, 1",
		"COMMIT",
	}

	expectedStatementStatsInExplicitTransaction1 := map[string]int{
		"SELECT _":    1,
		"SELECT _, _": 2,
	}

	expectedStatementStatsInExplicitTransaction2 := map[string]int{
		"SELECT _":    2,
		"SELECT _, _": 6,
	}

	expectedStatementStatsInImplicitTransaction := map[string]int{
		"SELECT _":    2,
		"SELECT _, _": 1,
	}

	cluster := serverutils.StartNewTestCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: params,
	})

	server := cluster.Server(0 /* idx */)

	// Open two connections so that we can run statements without messing up
	// the SQL stats.
	testConn := cluster.ServerConn(0 /* idx */)
	observerConn := cluster.ServerConn(1 /* idx */)

	sqlDB := sqlutils.MakeSQLRunner(testConn)
	observer := sqlutils.MakeSQLRunner(observerConn)
	defer cluster.Stopper().Stop(ctx)

	for _, stmt := range statements {
		sqlDB.Exec(t, stmt)
	}

	for _, flush := range []bool{false, true} {
		if flush {
			server.SQLServer().(*sql.Server).
				GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats).Flush(ctx)
		}

		t.Run(fmt.Sprintf("flushed=%t", flush), func(t *testing.T) {
			var txnFingerprintID1 string
			var txnFingerprintID2 string
			t.Run("check_transaction_fingerprint_ids", func(t *testing.T) {
				transactionFingerprintIDs := observer.QueryStr(t,
					`
SELECT
  encode(fingerprint_id, 'hex'),
  metadata -> 'stmtFingerprintIDs'
FROM
  crdb_internal.transaction_statistics
WHERE
  app_name = 'app1' AND
  jsonb_array_length(metadata -> 'stmtFingerprintIDs') >= 3
ORDER BY
  jsonb_array_length(metadata -> 'stmtFingerprintIDs')
ASC
`)

				require.Equal(t, 2 /* expected */, len(transactionFingerprintIDs),
					"expecting 2 transaction fingerprints to be recorded, but found %d", len(transactionFingerprintIDs))

				txnFingerprintID1 = transactionFingerprintIDs[0][0]
				txnFingerprintID2 = transactionFingerprintIDs[1][0]
			})

			var expectedStatementFingerprints []string
			t.Run("statement_stats_maps_to_correct_transaction_fingerprint_id", func(t *testing.T) {
				expectedStatementFingerprints = testStatementStatsForTransactionFingerprintID(
					t,
					observer,
					txnFingerprintID1,
					expectedStatementStatsInExplicitTransaction1,
				)
				testStatementStatsForTransactionFingerprintID(
					t,
					observer,
					txnFingerprintID2,
					expectedStatementStatsInExplicitTransaction2,
				)
			})

			t.Run("implicit_statement_stats_unaffected_by_explicit_transactions", func(t *testing.T) {
				statementStats := observer.QueryStr(t, `
SELECT
  metadata ->> 'query',
  (statistics -> 'statistics' ->> 'cnt')::INT
FROM
  crdb_internal.statement_statistics
WHERE
  app_name = 'app1' AND
  metadata ->> 'query' IN (
    $1, $2
  ) AND
  encode(transaction_fingerprint_id, 'hex') NOT IN (
    $3, $4
  )
`,
					expectedStatementFingerprints[0],
					expectedStatementFingerprints[1],
					txnFingerprintID1,
					txnFingerprintID2,
				)

				require.Equal(t, 2 /* expected */, len(statementStats),
					"expected 2 statements to be found, but found %v", statementStats)

				for _, row := range statementStats {
					expectedExecCnt, found := expectedStatementStatsInImplicitTransaction[row[0]]
					require.True(t, found, "unexpected statement fingerprint found %s", row[0])
					require.Equal(t, strconv.Itoa(expectedExecCnt), row[1],
						"expected %s to have exec count of %d, but it has exec count of %s",
						row[0], expectedExecCnt, row[1])
				}
			})
		})
	}
}

func testStatementStatsForTransactionFingerprintID(
	t *testing.T,
	observer *sqlutils.SQLRunner,
	txnFingerprintID string,
	expectedStatementStats map[string]int,
) (foundStatementFingerprints []string) {
	t.Run(fmt.Sprintf("txnFingerprintID=%s", txnFingerprintID), func(t *testing.T) {
		statementStats := observer.QueryStr(t, `
SELECT
  encode(fingerprint_id, 'hex'),
  metadata ->> 'query',
  (statistics -> 'statistics' ->> 'cnt')::INT
FROM
  crdb_internal.statement_statistics
WHERE
  app_name = 'app1' AND
  encode(transaction_fingerprint_id, 'hex') = $1
`, txnFingerprintID)

		require.Equal(t, 2 /* expected */, len(statementStats),
			"expected 2 statements to be found, but found %v", statementStats)

		for _, row := range statementStats {
			expectedExecCnt, found := expectedStatementStats[row[1]]
			require.True(t, found, "unexpected statement fingerprint found %s", row[1])
			require.Equal(t, strconv.Itoa(expectedExecCnt), row[2],
				"expected %s to have exec count of %d, but it has exec count of %s:\n",
				row[1], expectedExecCnt, row[2],
			)
			foundStatementFingerprints = append(foundStatementFingerprints, row[1])
		}
	})

	return foundStatementFingerprints
}

func TestLogicalPlanSamplingForExplicitTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	stubTime := &stubTime{}
	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLStatsKnobs.(*sqlstats.TestingKnobs).StubTimeNow = stubTime.Now

	cluster := serverutils.StartNewTestCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: params,
	})
	defer cluster.Stopper().Stop(ctx)

	server := cluster.Server(0 /* idx */)
	sqlStats := server.SQLServer().(*sql.Server).
		GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)

	testConn := cluster.ServerConn(0 /* idx */)
	sqlDB := sqlutils.MakeSQLRunner(testConn)

	sqlDB.Exec(t, "SET CLUSTER SETTING sql.metrics.statement_details.plan_collection.period = '1h'")
	sqlDB.Exec(t, "SET application_name = 'app1'")
	sqlDB.Exec(t, "USE defaultdb")
	appStats := sqlStats.GetApplicationStats("app1")

	tcs := []struct {
		name        string
		implicit    bool
		statements  string
		fingerprint string
	}{
		{
			name:        "implicit_txn",
			implicit:    true,
			statements:  "SELECT 1",
			fingerprint: "SELECT _",
		},
		{
			name:        "explicit_txn",
			implicit:    false,
			statements:  "BEGIN;SELECT 1;COMMIT",
			fingerprint: "SELECT _",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			for _, flushed := range []bool{false, true} {
				t.Run(fmt.Sprintf("flushed=%t", flushed), func(t *testing.T) {
					if flushed {
						sqlStats.Flush(ctx)
					}

					stubTime.setTime(timeutil.Now())
					t.Run("initial_run", func(t *testing.T) {
						require.True(t,
							appStats.ShouldSaveLogicalPlanDesc(
								tc.fingerprint,
								tc.implicit,
								"defaultdb",
							),
							"expected to save logical plan for %s, but it didn't", tc.name)
					})

					t.Run("subsequent_run", func(t *testing.T) {
						sqlDB.Exec(t, tc.statements)

						require.False(t,
							appStats.ShouldSaveLogicalPlanDesc(
								tc.fingerprint,
								tc.implicit,
								"defaultdb",
							),
							"expected to not sample logical plan for subsequent %s, but it did", tc.name,
						)
					})

					t.Run("after_plan_ttl_expires", func(t *testing.T) {
						stubTime.setTime(stubTime.Now().Add(10 * time.Hour))
						require.True(t,
							appStats.ShouldSaveLogicalPlanDesc(
								tc.fingerprint,
								tc.implicit,
								"defaultdb",
							),
							"expected to save logical plan for %s after sampled plan ttl expires, but it didn't", tc.name)
					})

					t.Run("second_subsequent_run", func(t *testing.T) {
						sqlDB.Exec(t, tc.statements)

						require.False(t,
							appStats.ShouldSaveLogicalPlanDesc(
								tc.fingerprint,
								tc.implicit,
								"defaultdb",
							),
							"expected to not sample logical plan for subsequent %s, but it did", tc.name,
						)
					})
				})
			}
		})
	}
}

type stubTime struct {
	syncutil.RWMutex
	t time.Time
}

func (s *stubTime) setTime(t time.Time) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.t = t
}

func (s *stubTime) Now() time.Time {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.t
}
