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
	"fmt"
	"regexp"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestSQLStatsCompactorNilTestingKnobCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	server, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	statsCompactor := persistedsqlstats.NewStatsCompactor(
		server.ClusterSettings(),
		server.InternalDB().(isql.DB),
		metric.NewCounter(metric.Metadata{}),
		nil, /* knobs */
	)

	// We run the compactor without disabling the follower read. This can possibly
	// fail due to descriptor not found.
	err := statsCompactor.DeleteOldestEntries(ctx)
	if err != nil {
		require.ErrorIs(t, err, catalog.ErrDescriptorNotFound)
	}
}

func TestSQLStatsCompactor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	testCases := []struct {
		stmtCount            int
		maxPersistedRowLimit int
		rowsToDeletePerTxn   int
	}{
		{
			stmtCount:            10,
			maxPersistedRowLimit: 2,
			rowsToDeletePerTxn:   1,
		},
		{
			stmtCount:            10,
			maxPersistedRowLimit: 2,
			rowsToDeletePerTxn:   4,
		},
		{
			stmtCount:            10,
			maxPersistedRowLimit: 2,
			rowsToDeletePerTxn:   128,
		},
		{
			stmtCount:            10,
			maxPersistedRowLimit: 2,
			rowsToDeletePerTxn:   1024,
		},
		{
			stmtCount:            200,
			maxPersistedRowLimit: 40,
			rowsToDeletePerTxn:   1,
		},
		{
			stmtCount:            200,
			maxPersistedRowLimit: 40,
			rowsToDeletePerTxn:   2,
		},
		{
			stmtCount:            200,
			maxPersistedRowLimit: 40,
			rowsToDeletePerTxn:   1024,
		},
		{
			stmtCount:            200,
			maxPersistedRowLimit: 205,
		},
	}

	kvInterceptor := kvScanInterceptor{}
	cleanupInterceptor := cleanupInterceptor{}

	server, conn, _ := serverutils.StartServer(
		t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: &sqlstats.TestingKnobs{
					AOSTClause: "AS OF SYSTEM TIME '-1us'",
					StubTimeNow: func() time.Time {
						return timeutil.Now().Add(-2 * time.Hour)
					},
				},
				Store: &kvserver.StoreTestingKnobs{
					TestingRequestFilter: kvInterceptor.intercept,
				},
			},
		},
	)

	defer server.Stopper().Stop(ctx)

	sqlConn := sqlutils.MakeSQLRunner(conn)
	internalExecutor := server.InternalExecutor().(isql.Executor)

	// Disable automatic flush since the test will handle the flush manually.
	sqlConn.Exec(t, "SET CLUSTER SETTING sql.stats.flush.interval = '24h'")

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("stmtCount=%d/maxPersistedRowLimit=%d/rowsDeletePerTxn=%d",
			tc.stmtCount,
			tc.maxPersistedRowLimit,
			tc.rowsToDeletePerTxn,
		), func(t *testing.T) {
			_, err := internalExecutor.ExecEx(
				ctx,
				"truncate-stmt-stats",
				nil,
				sessiondata.NodeUserSessionDataOverride,
				"TRUNCATE system.statement_statistics",
			)
			require.NoError(t, err)
			_, err = internalExecutor.ExecEx(
				ctx,
				"truncate-txn-stats",
				nil,
				sessiondata.NodeUserSessionDataOverride,
				"TRUNCATE system.transaction_statistics",
			)
			require.NoError(t, err)
			serverSQLStats :=
				server.
					SQLServer().(*sql.Server).
					GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)
			sqlConn.Exec(t,
				"SET CLUSTER SETTING sql.stats.persisted_rows.max = $1",
				tc.maxPersistedRowLimit)

			if tc.rowsToDeletePerTxn > 0 {
				sqlConn.Exec(t,
					"SET CLUSTER SETTING sql.stats.cleanup.rows_to_delete_per_txn = $1",
					tc.rowsToDeletePerTxn)
			} else {
				sqlConn.Exec(t, "RESET CLUSTER SETTING sql.stats.cleanup.rows_to_delete_per_txn")
			}

			generateFingerprints(t, sqlConn, tc.stmtCount)
			serverSQLStats.Flush(ctx)

			statsCompactor := persistedsqlstats.NewStatsCompactor(
				server.ClusterSettings(),
				server.InternalDB().(isql.DB),
				metric.NewCounter(metric.Metadata{}),
				&sqlstats.TestingKnobs{
					AOSTClause:             "AS OF SYSTEM TIME '-1us'",
					OnCleanupStartForShard: cleanupInterceptor.intercept,
					StubTimeNow: func() time.Time {
						return timeutil.Now()
					},
				},
			)

			// Initial compaction should remove the all the oldest entries.
			expectedDeletedStmtFingerprints, expectedDeletedTxnFingerprints :=
				getTopSortedFingerprints(t, sqlConn, tc.maxPersistedRowLimit)

			// Sanity check.
			require.Equal(t, tc.maxPersistedRowLimit, len(expectedDeletedStmtFingerprints))
			require.Equal(t, tc.maxPersistedRowLimit, len(expectedDeletedTxnFingerprints))

			run := func() {
				// The two interceptors (kvInterceptor and cleanupInterceptor) are
				// injected into kvserver and StatsCompactor respectively.
				// The cleanupInterceptor calculates the number of expected "wide scan"
				// that should be issued by the StatsCompactor.
				// The kvInterceptor counts the number of actual "wide scan" KV Request
				// issued.
				kvInterceptor.reset()
				cleanupInterceptor.reset()
				kvInterceptor.enable()
				defer kvInterceptor.disable()

				err := statsCompactor.DeleteOldestEntries(ctx)
				require.NoError(t, err)

				expectedNumberOfWideScans := cleanupInterceptor.getExpectedNumberOfWideScans()
				actualNumberOfWideScans := kvInterceptor.getTotalWideScans()

				require.Equal(t,
					expectedNumberOfWideScans,
					actualNumberOfWideScans,
					"expected %d number of wide scans issued, but %d number of "+
						"wide scan issued", expectedNumberOfWideScans, actualNumberOfWideScans,
				)
			}
			run()

			actualStmtFingerprints, actualTxnFingerprints :=
				getTopSortedFingerprints(t, sqlConn, 0 /* limit */)

			require.GreaterOrEqual(t, tc.maxPersistedRowLimit, len(actualStmtFingerprints))
			require.GreaterOrEqual(t, tc.maxPersistedRowLimit, len(actualTxnFingerprints))

			for fingerprintID := range actualStmtFingerprints {
				for _, deletedFingerprintID := range expectedDeletedStmtFingerprints {
					require.NotEqual(t, deletedFingerprintID, fingerprintID)
				}
			}

			for fingerprintID := range actualTxnFingerprints {
				for _, deletedFingerprintID := range expectedDeletedTxnFingerprints {
					require.NotEqual(t, deletedFingerprintID, fingerprintID)
				}
			}

			// Calling it again should be a noop.
			err = statsCompactor.DeleteOldestEntries(ctx)
			require.NoError(t, err)
			stmtStatsCnt, txnStatsCnt := getPersistedStatsEntry(t, sqlConn)
			require.GreaterOrEqual(t, tc.maxPersistedRowLimit, stmtStatsCnt)
			require.GreaterOrEqual(t, tc.maxPersistedRowLimit, txnStatsCnt)
		})
	}
}

// TestSQLStatsForegroundInterference ensures that the background SQL Stats
// cleanup job does not delete any rows in the current aggregation window. Doing
// so would cause contentions, which can potentially lead to long runtime of the
// SQL Stats cleanup job. We test this behavior by generating some rows in the
// stats system table that are in the current aggregation window and previous
// aggregation window. Before running the SQL Stats compaction, we lower the
// row limit in the stats table so that all thw rows will be deleted by the
// StatsCompactor, if all the generated rows live outside the current
// aggregation window. This test asserts that, since some of generated rows live
// in the current aggregation interval, those rows will not be deleted by the
// StatsCompactor.
func TestSQLStatsForegroundInterference(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var tm atomic.Value
	// Initialize the time to 2 aggregation interval in the past.
	tm.Store(timeutil.Now().Add(-2 * persistedsqlstats.SQLStatsAggregationInterval.Default()))

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLStatsKnobs.(*sqlstats.TestingKnobs).StubTimeNow = func() time.Time {
		return tm.Load().(time.Time)
	}
	s, conn, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	serverSQLStats :=
		s.SQLServer().(*sql.Server).
			GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)

	sqlConn := sqlutils.MakeSQLRunner(conn)
	sqlConn.Exec(t, "SET CLUSTER SETTING sql.stats.persisted_rows.max = 1")

	// Generate some data that are older than the current aggregation window,
	// and then generate some that are within the current aggregation window.
	generateFingerprints(t, sqlConn, 10 /* distinctFingerprints */)
	serverSQLStats.Flush(ctx)

	tm.Store(timeutil.Now())
	generateFingerprints(t, sqlConn, 10 /* distinctFingerprints */)
	serverSQLStats.Flush(ctx)

	statsCompactor := persistedsqlstats.NewStatsCompactor(
		s.ClusterSettings(),
		s.InternalDB().(isql.DB),
		metric.NewCounter(metric.Metadata{}),
		params.Knobs.SQLStatsKnobs.(*sqlstats.TestingKnobs),
	)

	// Run the compactor.
	require.NoError(t, statsCompactor.DeleteOldestEntries(ctx))

	result := sqlConn.QueryStr(t, `
	SELECT count(*)
	FROM system.statement_statistics`)[0][0]

	stmtStatsCount, err := strconv.Atoi(result)
	require.NoError(t, err)
	require.GreaterOrEqual(t, stmtStatsCount, 10,
		"expected at least 10 fingerprints in statement statistics table, "+
			"but only %d is present", stmtStatsCount)

	result = sqlConn.QueryStr(t, `
	SELECT count(*)
	FROM system.transaction_statistics`)[0][0]

	txnStatsCount, err := strconv.Atoi(result)
	require.NoError(t, err)
	require.GreaterOrEqual(t, txnStatsCount, 10,
		"expected at least 10 fingerprints in transaction statistics table, "+
			"but only %d is present", txnStatsCount)
}

func TestSQLStatsCompactionJobMarkedAsAutomatic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: params,
	})
	defer tc.Stopper().Stop(ctx)

	server := tc.Server(0 /* idx */)
	conn := tc.ServerConn(0 /* idx */)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	jobID, err := launchSQLStatsCompactionJob(server)
	require.NoError(t, err)

	// Ensure the sqlstats job is hidden from the SHOW JOBS command.
	sqlDB.CheckQueryResults(
		t,
		"SELECT count(*) FROM [SHOW JOBS] WHERE job_type = '"+jobspb.TypeAutoSQLStatsCompaction.String()+"'",
		[][]string{{"0"}},
	)

	// Ensure the sqlstats job is displayed in SHOW AUTOMATIC JOBS command.
	sqlDB.CheckQueryResults(
		t,
		fmt.Sprintf("SELECT count(*) FROM [SHOW AUTOMATIC JOBS] WHERE job_id = %d", jobID),
		[][]string{{"1"}},
	)
}

func launchSQLStatsCompactionJob(server serverutils.TestServerInterface) (jobspb.JobID, error) {
	return persistedsqlstats.CreateCompactionJob(
		context.Background(), nil /* createdByInfo */, nil, /* txn */
		server.JobRegistry().(*jobs.Registry),
	)
}

func getPersistedStatsEntry(
	t *testing.T, sqlConn *sqlutils.SQLRunner,
) (stmtStatsCnt, txnStatsCnt int) {
	stmt := "SELECT count(*) FROM %s"

	row := sqlConn.QueryRow(t, fmt.Sprintf(stmt, "system.statement_statistics"))
	row.Scan(&stmtStatsCnt)

	row = sqlConn.QueryRow(t, fmt.Sprintf(stmt, "system.transaction_statistics"))
	row.Scan(&txnStatsCnt)

	return stmtStatsCnt, txnStatsCnt
}

func getTopSortedFingerprints(
	t *testing.T, sqlDb *sqlutils.SQLRunner, limit int,
) (stmtFingerprints, txnFingerprints []uint64) {
	query := `
SELECT fingerprint_id
FROM %s
ORDER BY aggregated_ts`

	if limit > 0 {
		query = fmt.Sprintf("%s LIMIT %d", query, limit)
	}

	stmtFingerprints = make([]uint64, 0)
	txnFingerprints = make([]uint64, 0)

	fingerprintIDBuffer := make([]byte, 0, 8)
	rows := sqlDb.Query(t, fmt.Sprintf(query, "system.statement_statistics"))
	for rows.Next() {
		fingerprintIDBuffer = fingerprintIDBuffer[:0]
		require.NoError(t, rows.Scan(&fingerprintIDBuffer))
		_, fingerprintID, err := encoding.DecodeUint64Ascending(fingerprintIDBuffer)
		require.NoError(t, err)
		stmtFingerprints = append(stmtFingerprints, fingerprintID)
	}
	require.NoError(t, rows.Close())

	rows = sqlDb.Query(t, fmt.Sprintf(query, "system.transaction_statistics"))
	for rows.Next() {
		fingerprintIDBuffer = fingerprintIDBuffer[:0]
		require.NoError(t, rows.Scan(&fingerprintIDBuffer))
		_, fingerprintID, err := encoding.DecodeUint64Ascending(fingerprintIDBuffer)
		require.NoError(t, err)
		txnFingerprints = append(txnFingerprints, fingerprintID)
	}
	require.NoError(t, rows.Close())

	return stmtFingerprints, txnFingerprints
}

func generateFingerprints(t *testing.T, sqlConn *sqlutils.SQLRunner, distinctFingerprints int) {
	stmt := "SELECT 1"
	for i := 0; i < distinctFingerprints; i++ {
		sqlConn.Exec(t, stmt)
		// Mutate the stmt to create different fingerprint.
		stmt = fmt.Sprintf("%s, 1", stmt)
	}
}

const (
	stmtStatsTableID = 42
	txnStatsTableID  = 43
)

var kvReqWideScanStartKeyPattern = regexp.MustCompile("/Table/(42|43)/[0-9]{1,2}/[0-9]$")

type kvScanInterceptor struct {
	totalWideScan int64
	enabled       int32
}

func (k *kvScanInterceptor) reset() {
	atomic.StoreInt64(&k.totalWideScan, 0)
}

func (k *kvScanInterceptor) getTotalWideScans() int64 {
	return atomic.LoadInt64(&k.totalWideScan)
}

func (k *kvScanInterceptor) enable() {
	atomic.StoreInt32(&k.enabled, 1)
}

func (k *kvScanInterceptor) disable() {
	atomic.StoreInt32(&k.enabled, 0)
}

func (k *kvScanInterceptor) intercept(_ context.Context, ba *roachpb.BatchRequest) *roachpb.Error {
	if atomic.LoadInt32(&k.enabled) == 0 {
		return nil
	}
	if req, ok := ba.GetArg(roachpb.Scan); ok {
		_, tableID, _ := encoding.DecodeUvarintAscending(req.(*roachpb.ScanRequest).Key)
		if tableID == stmtStatsTableID || tableID == txnStatsTableID {
			prettyKey := roachpb.PrettyPrintKey([]encoding.Direction{}, req.(*roachpb.ScanRequest).Key)

			keyMatchedWideScan := kvReqWideScanStartKeyPattern.MatchString(prettyKey)

			if keyMatchedWideScan {
				atomic.AddInt64(&k.totalWideScan, 1)
			}
		}
	}

	return nil
}

type cleanupInterceptor struct {
	expectedNumberOfWideScans int64
}

func (c *cleanupInterceptor) reset() {
	atomic.StoreInt64(&c.expectedNumberOfWideScans, 0)
}

func (c *cleanupInterceptor) intercept(shardIdx int, existingCountInShard, shardLimit int64) {
	if existingCountInShard > shardLimit {
		atomic.AddInt64(&c.expectedNumberOfWideScans, 1)
	}
}

func (c *cleanupInterceptor) getExpectedNumberOfWideScans() int64 {
	return systemschema.SQLStatsHashShardBucketCount*2 + atomic.LoadInt64(&c.expectedNumberOfWideScans)
}
