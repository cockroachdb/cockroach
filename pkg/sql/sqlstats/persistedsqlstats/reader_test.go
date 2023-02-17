// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// Package sqlstats is a subsystem that is responsible for tracking the
// statistics of statements and transactions.

package persistedsqlstats_test

import (
	"context"
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

func TestPersistedSQLStatsRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	fakeTime := stubTime{
		aggInterval: time.Hour,
	}
	fakeTime.setTime(timeutil.Now())

	testCluster := serverutils.StartNewTestCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: &sqlstats.TestingKnobs{
					StubTimeNow: fakeTime.Now,
					AOSTClause:  "AS OF SYSTEM TIME '-1us'",
				},
			},
		},
	})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	server1 := testCluster.Server(0 /* idx */)
	sqlConn := sqlutils.MakeSQLRunner(testCluster.ServerConn(0 /* idx */))
	sqlStats := server1.SQLServer().(*sql.Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)

	expectedStmtFingerprints := make(map[string]int64)
	for _, tc := range testQueries {
		expectedStmtFingerprints[tc.fingerprint] = tc.count
		for i := int64(0); i < tc.count; i++ {
			sqlConn.Exec(t, tc.query)
		}
	}

	t.Run("in-memory only read", func(t *testing.T) {
		verifyStoredStmtFingerprints(t, expectedStmtFingerprints, sqlStats)
	})

	t.Run("disk only read", func(t *testing.T) {
		sqlStats.Flush(ctx)
		verifyStoredStmtFingerprints(t, expectedStmtFingerprints, sqlStats)
	})

	t.Run("hybrid read", func(t *testing.T) {
		// We execute each test queries one more time without flushing the stats.
		// This means that we should see the exact same result as previous subtest
		// except the execution count field will be incremented. We should not
		// be seeing duplicated fields.
		for _, tc := range testQueries {
			sqlConn.Exec(t, tc.query)
			tc.count++
			expectedStmtFingerprints[tc.fingerprint]++
		}

		foundQueries := make(map[string]struct{})
		foundTxns := make(map[string]struct{})
		stmtFingerprintIDToQueries := make(map[appstatspb.StmtFingerprintID]string)

		require.NoError(t,
			sqlStats.IterateStatementStats(
				context.Background(),
				&sqlstats.IteratorOptions{
					SortedKey:      true,
					SortedAppNames: true,
				},
				func(ctx context.Context, statistics *appstatspb.CollectedStatementStatistics) error {
					if expectedExecCount, ok := expectedStmtFingerprints[statistics.Key.Query]; ok {
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
				&sqlstats.IteratorOptions{},
				func(
					ctx context.Context,
					statistics *appstatspb.CollectedTransactionStatistics,
				) error {
					if len(statistics.StatementFingerprintIDs) == 1 {
						if query, ok := stmtFingerprintIDToQueries[statistics.StatementFingerprintIDs[0]]; ok {
							if expectedExecCount, ok := expectedStmtFingerprints[query]; ok {
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

		for expectedStmtFingerprint := range expectedStmtFingerprints {
			_, ok := foundQueries[expectedStmtFingerprint]
			require.True(t, ok, "expected %s to be returned, but it didn't", expectedStmtFingerprint)
		}
	})
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

	testCluster := serverutils.StartNewTestCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: &sqlstats.TestingKnobs{
					StubTimeNow: fakeTime.Now,
					AOSTClause:  "AS OF SYSTEM TIME '-1us'",
				},
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
			&sqlstats.IteratorOptions{},
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
			&sqlstats.IteratorOptions{},
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
