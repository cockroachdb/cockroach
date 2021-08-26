// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	query       string
	fingerprint string
	count       int64
}

var testQueries = []testCase{
	{
		query:       "SELECT 1",
		fingerprint: "SELECT _",
		count:       3,
	},
	{
		query:       "SELECT 1, 2, 3",
		fingerprint: "SELECT _, _, _",
		count:       10,
	},
	{
		query:       "SELECT 1, 1 WHERE 1 < 10",
		fingerprint: "SELECT _, _ WHERE _ < _",
		count:       7,
	},
}

func TestSQLStatsFlush(t *testing.T) {
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
				},
			},
		},
	})

	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	firstServer := testCluster.Server(0 /* idx */)
	secondServer := testCluster.Server(1 /* idx */)

	firstPgURL, firstServerConnCleanup := sqlutils.PGUrl(
		t, firstServer.ServingSQLAddr(), "CreateConnections" /* prefix */, url.User(security.RootUser))
	defer firstServerConnCleanup()

	secondPgURL, secondServerConnCleanup := sqlutils.PGUrl(
		t, secondServer.ServingSQLAddr(), "CreateConnections" /* prefix */, url.User(security.RootUser))
	defer secondServerConnCleanup()

	pgFirstSQLConn, err := gosql.Open("postgres", firstPgURL.String())
	require.NoError(t, err)
	firstSQLConn := sqlutils.MakeSQLRunner(pgFirstSQLConn)

	pgSecondSQLConn, err := gosql.Open("postgres", secondPgURL.String())
	require.NoError(t, err)
	secondSQLConn := sqlutils.MakeSQLRunner(pgSecondSQLConn)

	firstServerSQLStats := firstServer.SQLServer().(*sql.Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)
	secondServerSQLStats := secondServer.SQLServer().(*sql.Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)

	defer func() {
		err := pgFirstSQLConn.Close()
		require.NoError(t, err)
		err = pgSecondSQLConn.Close()
		require.NoError(t, err)
	}()
	firstSQLConn.Exec(t, "SET application_name = 'flush_unit_test'")
	secondSQLConn.Exec(t, "SET application_name = 'flush_unit_test'")
	require.NoError(t, err)

	// Regular inserts.
	{
		for _, tc := range testQueries {
			for i := int64(0); i < tc.count; i++ {
				firstSQLConn.Exec(t, tc.query)
			}
		}

		verifyInMemoryStatsCorrectness(t, testQueries, firstServerSQLStats)
		verifyInMemoryStatsEmpty(t, testQueries, secondServerSQLStats)

		firstServerSQLStats.Flush(ctx)
		secondServerSQLStats.Flush(ctx)

		verifyInMemoryStatsEmpty(t, testQueries, firstServerSQLStats)
		verifyInMemoryStatsEmpty(t, testQueries, secondServerSQLStats)

		// For each test case, we verify that it's being properly inserted exactly
		// once and it is exactly executed tc.count number of times.
		for _, tc := range testQueries {
			verifyNumOfInsertedEntries(t, secondSQLConn, tc.fingerprint, firstServer.NodeID(), 1 /* expectedStmtEntryCnt */, 1 /* expectedTxnEntryCtn */)
			verifyInsertedFingerprintExecCount(t, secondSQLConn, tc.fingerprint, fakeTime.getAggTimeTs(), firstServer.NodeID(), tc.count)
		}
	}

	// We insert the same data during the same aggregation window to ensure that
	// no new entries will be created but the statistics is updated.
	{
		for i := range testQueries {
			// Increment the execution count.
			testQueries[i].count++
			for execCnt := int64(0); execCnt < testQueries[i].count; execCnt++ {
				firstSQLConn.Exec(t, testQueries[i].query)
			}
		}
		verifyInMemoryStatsCorrectness(t, testQueries, firstServerSQLStats)
		verifyInMemoryStatsEmpty(t, testQueries, secondServerSQLStats)

		firstServerSQLStats.Flush(ctx)
		secondServerSQLStats.Flush(ctx)

		verifyInMemoryStatsEmpty(t, testQueries, firstServerSQLStats)
		verifyInMemoryStatsEmpty(t, testQueries, secondServerSQLStats)

		for _, tc := range testQueries {
			verifyNumOfInsertedEntries(t, secondSQLConn, tc.fingerprint, firstServer.NodeID(), 1 /* expectedStmtEntryCnt */, 1 /* expectedTxnEntryCtn */)
			// The execution count is doubled here because we execute all of the
			// statements here in the same aggregation interval.
			verifyInsertedFingerprintExecCount(t, secondSQLConn, tc.fingerprint, fakeTime.getAggTimeTs(), firstServer.NodeID(), tc.count+tc.count-1 /* expectedCount */)
		}
	}

	// We change the time to be in a different aggregation window.
	{
		fakeTime.setTime(fakeTime.StubTimeNow().Add(time.Hour * 3))

		for _, tc := range testQueries {
			for i := int64(0); i < tc.count; i++ {
				firstSQLConn.Exec(t, tc.query)
			}
		}
		verifyInMemoryStatsCorrectness(t, testQueries, firstServerSQLStats)
		verifyInMemoryStatsEmpty(t, testQueries, secondServerSQLStats)

		firstServerSQLStats.Flush(ctx)
		secondServerSQLStats.Flush(ctx)

		verifyInMemoryStatsEmpty(t, testQueries, firstServerSQLStats)
		verifyInMemoryStatsEmpty(t, testQueries, secondServerSQLStats)

		for _, tc := range testQueries {
			// We expect exactly 2 entries since we are in a different aggregation window.
			verifyNumOfInsertedEntries(t, secondSQLConn, tc.fingerprint, firstServer.NodeID(), 2 /* expectedStmtEntryCnt */, 2 /* expectedTxnEntryCtn */)
			verifyInsertedFingerprintExecCount(t, secondSQLConn, tc.fingerprint, fakeTime.getAggTimeTs(), firstServer.NodeID(), tc.count)
		}
	}

	// We run queries in a different server and trigger the flush.
	{
		for _, tc := range testQueries {
			for i := int64(0); i < tc.count; i++ {
				secondSQLConn.Exec(t, tc.query)
				require.NoError(t, err)
			}
		}
		verifyInMemoryStatsEmpty(t, testQueries, firstServerSQLStats)
		verifyInMemoryStatsCorrectness(t, testQueries, secondServerSQLStats)

		firstServerSQLStats.Flush(ctx)
		secondServerSQLStats.Flush(ctx)

		verifyInMemoryStatsEmpty(t, testQueries, firstServerSQLStats)
		verifyInMemoryStatsEmpty(t, testQueries, secondServerSQLStats)

		// Ensure that we encode the correct node_id for the new entry and did not
		// accidentally tamper the entries written by another server.
		for _, tc := range testQueries {
			verifyNumOfInsertedEntries(t, firstSQLConn, tc.fingerprint, secondServer.NodeID(), 1 /* expectedStmtEntryCnt */, 1 /* expectedTxnEntryCtn */)
			verifyInsertedFingerprintExecCount(t, firstSQLConn, tc.fingerprint, fakeTime.getAggTimeTs(), secondServer.NodeID(), tc.count)
			verifyNumOfInsertedEntries(t, secondSQLConn, tc.fingerprint, firstServer.NodeID(), 2 /* expectedStmtEntryCnt */, 2 /* expectedTxnEntryCtn */)
			verifyInsertedFingerprintExecCount(t, secondSQLConn, tc.fingerprint, fakeTime.getAggTimeTs(), firstServer.NodeID(), tc.count)
		}
	}
}

type stubTime struct {
	syncutil.RWMutex
	t           time.Time
	aggInterval time.Duration
}

func (s *stubTime) setTime(t time.Time) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.t = t
}

func (s *stubTime) getAggTimeTs() time.Time {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	return s.t.Truncate(s.aggInterval)
}

// StubTimeNow implements the testing knob interface for persistedsqlstats.Provider.
func (s *stubTime) StubTimeNow() time.Time {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.t
}

func verifyInsertedFingerprintExecCount(
	t *testing.T,
	sqlConn *sqlutils.SQLRunner,
	fingerprint string,
	ts time.Time,
	nodeID roachpb.NodeID,
	expectedCount int64,
) {
	row := sqlConn.Query(t,
		`
SELECT
    (S.statistics -> 'statistics' ->> 'cnt')::INT  AS stmtCount,
    (T.statistics -> 'statistics' ->> 'cnt')::INT AS txnCount
FROM
    system.transaction_statistics T,
    system.statement_statistics S
WHERE S.metadata ->> 'query' = $1
	  AND T.aggregated_ts = $2
    AND T.node_id = $3
    AND T.app_name = 'flush_unit_test'
    AND decode(T.metadata -> 'stmtFingerprintIDs' ->> 0, 'hex') = S.fingerprint_id
    AND S.node_id = T.node_id
    AND S.aggregated_ts = T.aggregated_ts
    AND S.app_name = T.app_name
`, fingerprint, ts, nodeID)

	require.True(t, row.Next(), "no stats found for fingerprint: %s", fingerprint)

	var actualTxnExecCnt int64
	var actualStmtExecCnt int64
	err := row.Scan(&actualStmtExecCnt, &actualTxnExecCnt)
	require.NoError(t, err)
	require.Equal(t, expectedCount, actualStmtExecCnt, "fingerprint: %s", fingerprint)
	require.Equal(t, expectedCount, actualTxnExecCnt, "fingerprint: %s", fingerprint)
	require.False(t, row.Next(), "more than one rows found for fingerprint: %s", fingerprint)
	require.NoError(t, row.Close())
}

func verifyNumOfInsertedEntries(
	t *testing.T,
	sqlConn *sqlutils.SQLRunner,
	fingerprint string,
	nodeID roachpb.NodeID,
	expectedStmtEntryCnt, expectedTxnEntryCnt int64,
) {
	row2 := sqlConn.DB.QueryRowContext(context.Background(),
		`
SELECT
  encode(fingerprint_id, 'hex'),
	count(*)
FROM
	system.statement_statistics
WHERE
	metadata ->> 'query' = $1 AND
  node_id = $2 AND
  app_name = 'flush_unit_test'
GROUP BY
  (fingerprint_id, node_id)
`, fingerprint, nodeID)

	var stmtFingerprintID string
	var numOfInsertedStmtEntry int64

	e := row2.Scan(&stmtFingerprintID, &numOfInsertedStmtEntry)
	require.NoError(t, e)
	require.Equal(t, expectedStmtEntryCnt, numOfInsertedStmtEntry, "fingerprint: %s", fingerprint)

	row1 := sqlConn.DB.QueryRowContext(context.Background(), fmt.Sprintf(
		`
SELECT
  count(*)
FROM
  system.transaction_statistics
WHERE
  (metadata -> 'stmtFingerprintIDs' ->> 0) = '%s' AND
  node_id = $1 AND
  app_name = 'flush_unit_test'
GROUP BY
  (fingerprint_id, node_id)
`, stmtFingerprintID), nodeID)

	var numOfInsertedTxnEntry int64
	err := row1.Scan(&numOfInsertedTxnEntry)
	require.NoError(t, err)
	require.Equal(t, expectedTxnEntryCnt, numOfInsertedTxnEntry, "fingerprint: %s", fingerprint)
}

func verifyInMemoryStatsCorrectness(
	t *testing.T, tcs []testCase, statsProvider *persistedsqlstats.PersistedSQLStats,
) {
	for _, tc := range tcs {
		err := statsProvider.SQLStats.IterateStatementStats(context.Background(), &sqlstats.IteratorOptions{}, func(ctx context.Context, statistics *roachpb.CollectedStatementStatistics) error {
			if tc.fingerprint == statistics.Key.Query {
				require.Equal(t, tc.count, statistics.Stats.Count, "fingerprint: %s", tc.fingerprint)
			}
			return nil
		})

		require.NoError(t, err)
	}
}

func verifyInMemoryStatsEmpty(
	t *testing.T, tcs []testCase, statsProvider *persistedsqlstats.PersistedSQLStats,
) {
	for _, tc := range tcs {
		err := statsProvider.SQLStats.IterateStatementStats(context.Background(), &sqlstats.IteratorOptions{}, func(ctx context.Context, statistics *roachpb.CollectedStatementStatistics) error {
			if tc.fingerprint == statistics.Key.Query {
				require.Equal(t, 0 /* expected */, statistics.Stats.Count, "fingerprint: %s", tc.fingerprint)
			}
			return nil
		})

		require.NoError(t, err)
	}
}
