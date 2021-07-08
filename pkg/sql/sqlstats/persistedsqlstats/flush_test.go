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

func verifyInsertedFingerprint(
	t *testing.T,
	sqlConn *gosql.DB,
	fingerprint string,
	ts time.Time,
	nodeID roachpb.NodeID,
	expectedCount int64,
) {
	row := sqlConn.QueryRow(
		`
SELECT
  encode(fingerprint_id, 'hex'),
  count
FROM
  system.statement_statistics
WHERE
  metadata ->> 'query' = $1 AND
  aggregated_ts = $2 AND
  node_id = $3 AND
  app_name = 'flush_unit_test'
`, fingerprint, ts, nodeID)

	var stmtFingerprintID string
	var actualStmtExecCnt int64
	err := row.Scan(&stmtFingerprintID, &actualStmtExecCnt)
	require.NoError(t, err)
	require.Equal(t, expectedCount, actualStmtExecCnt)

	row = sqlConn.QueryRow(fmt.Sprintf(
		`
SELECT
  count
FROM
  system.transaction_statistics
WHERE
  metadata -> 'stmtFingerprintIDs' @> '"%s"' AND
  aggregated_ts = $1 AND
  node_id = $2 AND
  app_name = 'flush_unit_test'
`, stmtFingerprintID), ts, nodeID)

	var actualTxnExecCnt int64
	err = row.Scan(&actualTxnExecCnt)
	require.NoError(t, err)
	require.Equal(t, expectedCount, actualTxnExecCnt, "unexpected additioanl transaction fingerprint: %s", fingerprint)
}

func verifyNumOfInsertedEntries(
	t *testing.T,
	sqlConn *gosql.DB,
	fingerprint string,
	nodeID roachpb.NodeID,
	expectedStmtEntryCnt, expectedTxnEntryCnt int64,
) {
	row := sqlConn.QueryRow(
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

	err := row.Scan(&stmtFingerprintID, &numOfInsertedStmtEntry)
	require.NoError(t, err, "fingerprint: %s", fingerprint)
	require.Equal(t, expectedStmtEntryCnt, numOfInsertedStmtEntry, "fingerprint: %s", fingerprint)

	row = sqlConn.QueryRow(fmt.Sprintf(
		`
SELECT
	encode(fingerprint_id, 'hex'),
  count(*)
FROM
  system.transaction_statistics
WHERE
  (metadata -> 'stmtFingerprintIDs') @> '"%s"' AND
  node_id = $1 AND
  app_name = 'flush_unit_test'
GROUP BY
  (fingerprint_id, node_id)
`, stmtFingerprintID), nodeID)

	var txnFingerprintID string
	var numOfInsertedTxnEntry int64
	err = row.Scan(&txnFingerprintID, &numOfInsertedTxnEntry)
	require.NoError(t, err, "fingerprint: %s", fingerprint)
	require.Equal(t, expectedTxnEntryCnt, numOfInsertedTxnEntry, "fingerprint: %s", fingerprint)
}

func createOnStatsFlushedCallback(
	t *testing.T,
) (onFlushedCb func(error), waitForFlush func(*testing.T, uint64, time.Duration)) {
	flushEventCh := make(chan struct{})
	onFlushedCb = func(err error) {
		require.NoError(t, err)
		flushEventCh <- struct{}{}
	}

	waitForFlush = func(t *testing.T, expectedFlushEventCnt uint64, flushTimeout time.Duration) {
		// Wait for the flush to finish.
		var flushTimer timeutil.Timer
		flushTimer.Reset(flushTimeout)

		var flushEventCnt uint64
		for flushEventCnt < expectedFlushEventCnt {
			select {
			case <-flushEventCh:
				flushEventCnt++
			case <-flushTimer.C:
				t.Fatalf("expected task to complete within %s, but it timed out", flushTimeout)
			}
		}
	}

	return onFlushedCb, waitForFlush
}

func TestSQLStatsFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	fakeTime := stubTime{
		aggInterval: time.Hour,
	}
	fakeTime.setTime(timeutil.Now())

	flushCallback, waitForFlush := createOnStatsFlushedCallback(t)

	testCluster := serverutils.StartNewTestCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: &persistedsqlstats.TestingKnobs{
					OnStatsFlushFinished: flushCallback,
					StubTimeNow:          fakeTime.StubTimeNow,
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

	firstSQLConn, err := gosql.Open("postgres", firstPgURL.String())
	require.NoError(t, err)

	secondSQLConn, err := gosql.Open("postgres", secondPgURL.String())
	require.NoError(t, err)

	firstServerSQLStatsHandle := firstServer.SQLServer().(*sql.Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)
	secondServerSQLStatsHandle := secondServer.SQLServer().(*sql.Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)

	defer func() {
		err := firstSQLConn.Close()
		require.NoError(t, err)
		err = secondSQLConn.Close()
		require.NoError(t, err)
	}()

	testCases := []testCase{
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

	_, err = firstSQLConn.Exec("SET application_name = 'flush_unit_test'")
	require.NoError(t, err)
	_, err = secondSQLConn.Exec("SET application_name = 'flush_unit_test'")
	require.NoError(t, err)

	t.Run("regular insert", func(t *testing.T) {
		for _, tc := range testCases {
			for i := int64(0); i < tc.count; i++ {
				_, err = firstSQLConn.Exec(tc.query)
				require.NoError(t, err)
			}
		}

		var expectedFlushEvent uint64
		expectedFlushEvent += firstServerSQLStatsHandle.StartFlush(ctx)
		expectedFlushEvent += secondServerSQLStatsHandle.StartFlush(ctx)
		waitForFlush(t, expectedFlushEvent, time.Minute*2 /* timeout */)

		// For each test case, we verify that it's being properly inserted exactly
		// once and it is exactly executed tc.count number of times.
		for _, tc := range testCases {
			verifyNumOfInsertedEntries(t, secondSQLConn, tc.fingerprint, firstServer.NodeID(), 1 /* expectedStmtEntryCnt */, 1 /* expectedTxnEntryCtn */)
			verifyInsertedFingerprint(t, secondSQLConn, tc.fingerprint, fakeTime.getAggTimeTs(), firstServer.NodeID(), tc.count)
		}
	})

	// We insert the same data during the same aggregation window to ensure that
	// no new entries will be created but the statistics is updated.
	t.Run("upsert-same-agg-window", func(t *testing.T) {
		for _, tc := range testCases {
			for i := int64(0); i < tc.count; i++ {
				_, err = firstSQLConn.Exec(tc.query)
				require.NoError(t, err)
			}
		}

		var expectedFlushEvent uint64
		expectedFlushEvent += firstServerSQLStatsHandle.StartFlush(ctx)
		expectedFlushEvent += secondServerSQLStatsHandle.StartFlush(ctx)
		waitForFlush(t, expectedFlushEvent, time.Minute*2 /* timeout */)

		for _, tc := range testCases {
			verifyNumOfInsertedEntries(t, secondSQLConn, tc.fingerprint, firstServer.NodeID(), 1 /* expectedStmtEntryCnt */, 1 /* expectedTxnEntryCtn */)
			// The execution count is doubled here because we execute all of the
			// statements here in the same aggregation interval.
			verifyInsertedFingerprint(t, secondSQLConn, tc.fingerprint, fakeTime.getAggTimeTs(), firstServer.NodeID(), tc.count*2)
		}
	})

	t.Run("upsert-different-agg-window", func(t *testing.T) {
		// We change the time to be in a different aggregation window.
		fakeTime.setTime(fakeTime.StubTimeNow().Add(time.Hour * 3))

		for _, tc := range testCases {
			for i := int64(0); i < tc.count; i++ {
				_, err = firstSQLConn.Exec(tc.query)
				require.NoError(t, err)
			}
		}

		var expectedFlushEvent uint64
		expectedFlushEvent += firstServerSQLStatsHandle.StartFlush(ctx)
		expectedFlushEvent += secondServerSQLStatsHandle.StartFlush(ctx)
		waitForFlush(t, expectedFlushEvent, time.Minute*2 /* timeout */)

		for _, tc := range testCases {
			// We expect exactly 2 entries since we are in a different aggregation window.
			verifyNumOfInsertedEntries(t, secondSQLConn, tc.fingerprint, firstServer.NodeID(), 2 /* expectedStmtEntryCnt */, 2 /* expectedTxnEntryCtn */)
			verifyInsertedFingerprint(t, secondSQLConn, tc.fingerprint, fakeTime.getAggTimeTs(), firstServer.NodeID(), tc.count)
		}
	})

	t.Run("upsert-different-node", func(t *testing.T) {
		// We run queries in a different server and trigger the flush.
		for _, tc := range testCases {
			for i := int64(0); i < tc.count; i++ {
				_, err = secondSQLConn.Exec(tc.query)
				require.NoError(t, err)
			}
		}

		var expectedFlushEvent uint64
		expectedFlushEvent += firstServerSQLStatsHandle.StartFlush(ctx)
		expectedFlushEvent += secondServerSQLStatsHandle.StartFlush(ctx)
		waitForFlush(t, expectedFlushEvent, time.Minute*2 /* timeout */)

		// Ensure that we encode the correct node_id for the new entry and did not
		// accidentally tamper the entries written by another server.
		for _, tc := range testCases {
			verifyNumOfInsertedEntries(t, firstSQLConn, tc.fingerprint, secondServer.NodeID(), 1 /* expectedStmtEntryCnt */, 1 /* expectedTxnEntryCtn */)
			verifyInsertedFingerprint(t, firstSQLConn, tc.fingerprint, fakeTime.getAggTimeTs(), secondServer.NodeID(), tc.count)
			verifyNumOfInsertedEntries(t, secondSQLConn, tc.fingerprint, firstServer.NodeID(), 2 /* expectedStmtEntryCnt */, 2 /* expectedTxnEntryCtn */)
			verifyInsertedFingerprint(t, secondSQLConn, tc.fingerprint, fakeTime.getAggTimeTs(), firstServer.NodeID(), tc.count)
		}
	})
}
