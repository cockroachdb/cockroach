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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
					StubTimeNow: fakeTime.StubTimeNow,
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
		stmtFingerprintIDToQueries := make(map[roachpb.StmtFingerprintID]string)

		require.NoError(t,
			sqlStats.IterateStatementStats(
				context.Background(),
				&sqlstats.IteratorOptions{
					SortedKey:      true,
					SortedAppNames: true,
				},
				func(ctx context.Context, statistics *roachpb.CollectedStatementStatistics) error {
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
					statistics *roachpb.CollectedTransactionStatistics,
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

func verifyStoredStmtFingerprints(
	t *testing.T,
	expectedStmtFingerprints map[string]int64,
	sqlStats *persistedsqlstats.PersistedSQLStats,
) {
	foundQueries := make(map[string]struct{})
	foundTxns := make(map[string]struct{})
	stmtFingerprintIDToQueries := make(map[roachpb.StmtFingerprintID]string)
	require.NoError(t,
		sqlStats.IterateStatementStats(
			context.Background(),
			&sqlstats.IteratorOptions{},
			func(ctx context.Context, statistics *roachpb.CollectedStatementStatistics) error {
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
				statistics *roachpb.CollectedTransactionStatistics,
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
