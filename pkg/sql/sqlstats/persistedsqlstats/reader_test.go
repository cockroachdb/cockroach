// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// Package sqlstats is a subsystem that is responsible for tracking the
// statistics of statements and transactions.

package persistedsqlstats_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestPersistedSQLStatsReadMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStress(t, "the test is too slow to run under stress")

	testCluster, ctx := createCluster(t)
	defer testCluster.Stopper().Stop(ctx)
	_, sqlStats, expectedStmtsNoConst := insertData(t, testCluster)

	verifyInMemoryStmtFingerprints(t, expectedStmtsNoConst, "TestPersistedSQLStatsRead", sqlStats)
	verifyStoredStmtFingerprints(t, expectedStmtsNoConst, sqlStats)
}

func TestPersistedSQLStatsReadDisk(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStress(t, "the test is too slow to run under stress")

	testCluster, ctx := createCluster(t)
	defer testCluster.Stopper().Stop(ctx)
	sqlConn, sqlStats, expectedStmtsNoConst := insertData(t, testCluster)

	sqlStats.Flush(ctx)
	verifyDiskStmtFingerprints(t, sqlConn, expectedStmtsNoConst)
	verifyStoredStmtFingerprints(t, expectedStmtsNoConst, sqlStats)
}

func TestPersistedSQLStatsReadHybrid(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStress(t, "the test is too slow to run under stress")

	testCluster, ctx := createCluster(t)
	defer testCluster.Stopper().Stop(ctx)

	sqlConn, sqlStats, expectedStmtsNoConst := insertData(t, testCluster)
	sqlStats.Flush(ctx)
	// We execute each test queries one more time without flushing the stats.
	// This means that we should see the exact same result as previous subtest
	// except the execution count field will be incremented. We should not
	// be seeing duplicated fields.
	for _, tc := range testQueries {
		sqlConn.Exec(t, tc.query)
		tc.count++
		expectedStmtsNoConst[tc.stmtNoConst]++
	}

	foundQueries := make(map[string]struct{})
	foundTxns := make(map[string]struct{})
	stmtFingerprintIDToQueries := make(map[appstatspb.StmtFingerprintID]string)

	require.NoError(t,
		sqlStats.IterateStatementStats(
			context.Background(),
			sqlstats.IteratorOptions{
				SortedKey:      true,
				SortedAppNames: true,
			},
			func(ctx context.Context, statistics *appstatspb.CollectedStatementStatistics) error {
				if expectedExecCount, ok := expectedStmtsNoConst[statistics.Key.Query]; ok {
					_, ok = foundQueries[statistics.Key.Query]
					require.False(
						t,
						ok,
						"should only found one stats entry for %s, but found more than one", statistics.Key.Query,
					)
					foundQueries[statistics.Key.Query] = struct{}{}
					stmtFingerprintIDToQueries[statistics.ID] = statistics.Key.Query
					require.Equal(t, expectedExecCount, statistics.Stats.Count, "query: %s", statistics.Key.Query)
				}
				return nil
			}))

	require.NoError(t,
		sqlStats.IterateTransactionStats(
			context.Background(),
			sqlstats.IteratorOptions{},
			func(
				ctx context.Context,
				statistics *appstatspb.CollectedTransactionStatistics,
			) error {
				if len(statistics.StatementFingerprintIDs) == 1 {
					if query, ok := stmtFingerprintIDToQueries[statistics.StatementFingerprintIDs[0]]; ok {
						if expectedExecCount, ok := expectedStmtsNoConst[query]; ok {
							_, ok = foundTxns[query]
							require.False(
								t,
								ok,
								"should only found one txn stats entry for %s, but found more than one", query,
							)
							foundTxns[query] = struct{}{}
							require.Equal(t, expectedExecCount, statistics.Stats.Count)
						}
					}
				}
				return nil
			}))

	for expectedStmtFingerprint := range expectedStmtsNoConst {
		_, ok := foundQueries[expectedStmtFingerprint]
		require.True(t, ok, "expected %s to be returned, but it didn't", expectedStmtFingerprint)
	}
}

func insertData(
	t *testing.T, testCluster serverutils.TestClusterInterface,
) (*sqlutils.SQLRunner, *persistedsqlstats.PersistedSQLStats, map[string]int64) {
	server1 := testCluster.Server(0 /* idx */)
	sqlConn := sqlutils.MakeSQLRunner(server1.SQLConn(t))
	sqlStats := server1.SQLServer().(*sql.Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)

	sqlConn.Exec(t, `SET application_name = 'TestPersistedSQLStatsRead'`)
	expectedStmtsNoConst := make(map[string]int64)
	for _, tc := range testQueries {
		expectedStmtsNoConst[tc.stmtNoConst] = tc.count
		for i := int64(0); i < tc.count; i++ {
			sqlConn.Exec(t, tc.query)
		}
	}
	return sqlConn, sqlStats, expectedStmtsNoConst
}

func createCluster(t *testing.T) (serverutils.TestClusterInterface, context.Context) {
	fakeTime := stubTime{
		aggInterval: time.Hour,
	}
	fakeTime.setTime(timeutil.Now())

	knobs := sqlstats.CreateTestingKnobs()
	knobs.StubTimeNow = fakeTime.Now
	testCluster := serverutils.StartCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: knobs,
			},
		},
	})
	ctx := context.Background()
	return testCluster, ctx
}

// Testing same fingerprint having more than one index recommendation and
// checking the aggregation on the crdb_internal.statement_statistics table.
// Testing for issue #85958.
func TestSQLStatsWithMultipleIdxRec(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStressRace(t, "expensive tests")

	fakeTime := stubTime{
		aggInterval: time.Hour,
	}
	fakeTime.setTime(timeutil.Now())

	knobs := sqlstats.CreateTestingKnobs()
	knobs.StubTimeNow = fakeTime.Now
	testCluster := serverutils.StartCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: knobs,
			},
		},
	})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	sqlConn := sqlutils.MakeSQLRunner(testCluster.ServerConn(0 /* idx */))

	sqlConn.Exec(t, "CREATE TABLE t1 (k INT, i INT, f FLOAT, s STRING)")
	sqlConn.Exec(t, "CREATE TABLE t2 (k INT, i INT, s STRING)")

	query := "SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k WHERE t1.i > 3 AND t2.i > 3"
	memorySelect := "SELECT statistics -> 'statistics' ->> 'cnt' as count, " +
		"array_length(index_recommendations, 1) FROM " +
		"crdb_internal.cluster_statement_statistics WHERE metadata ->> 'query' = " +
		"'SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k WHERE (t1.i > _) AND (t2.i > _)'"
	combinedSelect := "SELECT statistics -> 'statistics' ->> 'cnt' as count, " +
		"array_length(index_recommendations, 1) FROM " +
		"crdb_internal.statement_statistics WHERE metadata ->> 'query' = " +
		"'SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k WHERE (t1.i > _) AND (t2.i > _)'"

	// Execute enough times to have index recommendations generated.
	// This will generate two recommendations.
	for i := 0; i < 6; i++ {
		sqlConn.Exec(t, query)
	}
	var cnt int64
	var recs int64
	// It must have the same count 6 on both in-memory and combined tables.
	// This test when there are more than one recommendation, so adding this
	// example that has 2 recommendations;
	sqlConn.QueryRow(t, memorySelect).Scan(&cnt, &recs)
	require.Equal(t, int64(6), cnt)
	require.Equal(t, int64(2), recs)
	sqlConn.QueryRow(t, combinedSelect).Scan(&cnt, &recs)
	require.Equal(t, int64(6), cnt)
	require.Equal(t, int64(2), recs)
}

func verifyInMemoryStmtFingerprints(
	t *testing.T,
	expectedStmtFingerprints map[string]int64,
	appName string,
	sqlStats *persistedsqlstats.PersistedSQLStats,
) {
	foundQueries := make(map[string]struct{})
	foundTxns := make(map[string]struct{})
	stmtFingerprintIDToQueries := make(map[appstatspb.StmtFingerprintID]string)
	require.NoError(t,
		sqlStats.GetApplicationStats(appName, false).IterateStatementStats(
			context.Background(),
			sqlstats.IteratorOptions{},
			func(ctx context.Context, statistics *appstatspb.CollectedStatementStatistics) error {
				if expectedExecCount, ok := expectedStmtFingerprints[statistics.Key.Query]; ok {
					foundQueries[statistics.Key.Query] = struct{}{}
					stmtFingerprintIDToQueries[statistics.ID] = statistics.Key.Query
					require.Equal(t, expectedExecCount, statistics.Stats.Count)
				}
				return nil
			}))

	require.NoError(t,
		sqlStats.IterateTransactionStats(
			context.Background(),
			sqlstats.IteratorOptions{},
			func(
				ctx context.Context,
				statistics *appstatspb.CollectedTransactionStatistics,
			) error {
				if len(statistics.StatementFingerprintIDs) == 1 {
					if query, ok := stmtFingerprintIDToQueries[statistics.StatementFingerprintIDs[0]]; ok {
						if expectedExecCount, ok := expectedStmtFingerprints[query]; ok {
							foundTxns[query] = struct{}{}
							require.Equal(t, expectedExecCount, statistics.Stats.Count)
						}
					}
				}
				return nil
			}))

	for expectedStmtFingerprint := range expectedStmtFingerprints {
		_, ok := foundQueries[expectedStmtFingerprint]
		require.True(t, ok, "expected %s to be returned, but it didn't", expectedStmtFingerprint)
		_, ok = foundTxns[expectedStmtFingerprint]
		require.True(t, ok, "expected %s to be returned, but it didn't", expectedStmtFingerprint)
	}
}

func verifyDiskStmtFingerprints(
	t *testing.T, sqlRunner *sqlutils.SQLRunner, expectedStmtsNoConst map[string]int64,
) {
	require.NotEmpty(t, expectedStmtsNoConst)
	expectedStmtsNoConstCopy := make(map[string]int64, len(expectedStmtsNoConst))
	stmtQuery := `SELECT ss.metadata->>'query' as query,
       ss.statistics->'statistics'->>'cnt' as count,
       ts.execution_count
FROM system.statement_statistics ss
    LEFT JOIN system.transaction_statistics ts ON ss.transaction_fingerprint_id = ts.fingerprint_id
WHERE ss.metadata->>'query' in ( `

	for fingerprint, cnt := range expectedStmtsNoConst {
		stmtQuery += fmt.Sprintf(` '%s' ,`, fingerprint)
		expectedStmtsNoConstCopy[fingerprint] = cnt
	}
	stmtQuery = strings.TrimRight(stmtQuery, ",")
	stmtQuery += " ) "
	rows := sqlRunner.Query(t, stmtQuery)
	defer rows.Close()

	for rows.Next() {
		require.NoError(t, rows.Err())
		var actualStmtNoConst string
		var actualStmtCount, actualTxnCount int64
		require.NoError(t, rows.Scan(&actualStmtNoConst, &actualStmtCount, &actualTxnCount))
		expectedCount, ok := expectedStmtsNoConstCopy[actualStmtNoConst]
		require.True(t, ok, "Fingerprint %s not found in expected", actualStmtNoConst)
		require.Equal(t, expectedCount, actualStmtCount, "statement no const: %s, expected stmt count: %d, actual count: %d", actualStmtNoConst, expectedCount, actualStmtCount)
		require.Equal(t, expectedCount, actualTxnCount, "stmt no const: %s, expected txn count: %d, actual count: %d", actualStmtNoConst, expectedCount, actualTxnCount)
		delete(expectedStmtsNoConstCopy, actualStmtNoConst)
	}

	require.Empty(t, expectedStmtsNoConstCopy)
}

func verifyStoredStmtFingerprints(
	t *testing.T,
	expectedStmtFingerprints map[string]int64,
	sqlStats *persistedsqlstats.PersistedSQLStats,
) {
	foundQueries := make(map[string]struct{})
	foundTxns := make(map[string]struct{})
	stmtFingerprintIDToQueries := make(map[appstatspb.StmtFingerprintID]string)
	require.NoError(t,
		sqlStats.IterateStatementStats(
			context.Background(),
			sqlstats.IteratorOptions{},
			func(ctx context.Context, statistics *appstatspb.CollectedStatementStatistics) error {
				if expectedExecCount, ok := expectedStmtFingerprints[statistics.Key.Query]; ok {
					foundQueries[statistics.Key.Query] = struct{}{}
					stmtFingerprintIDToQueries[statistics.ID] = statistics.Key.Query
					require.Equal(t, expectedExecCount, statistics.Stats.Count)
				}
				return nil
			}))

	require.NoError(t,
		sqlStats.IterateTransactionStats(
			context.Background(),
			sqlstats.IteratorOptions{},
			func(
				ctx context.Context,
				statistics *appstatspb.CollectedTransactionStatistics,
			) error {
				if len(statistics.StatementFingerprintIDs) == 1 {
					if query, ok := stmtFingerprintIDToQueries[statistics.StatementFingerprintIDs[0]]; ok {
						if expectedExecCount, ok := expectedStmtFingerprints[query]; ok {
							foundTxns[query] = struct{}{}
							require.Equal(t, expectedExecCount, statistics.Stats.Count)
						}
					}
				}
				return nil
			}))

	for expectedStmtFingerprint := range expectedStmtFingerprints {
		_, ok := foundQueries[expectedStmtFingerprint]
		require.True(t, ok, "expected %s to be returned, but it didn't", expectedStmtFingerprint)
		_, ok = foundTxns[expectedStmtFingerprint]
		require.True(t, ok, "expected %s to be returned, but it didn't", expectedStmtFingerprint)
	}
}
