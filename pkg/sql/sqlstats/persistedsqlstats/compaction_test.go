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
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
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
	// Change the automatic compaction job to avoid it running during the test.
	// Test creates a new compactor and calls it directly.
	sqlConn.Exec(t, "SET CLUSTER SETTING sql.stats.cleanup.recurrence = '@yearly';")

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

			// The two interceptors (kvInterceptor and cleanupInterceptor) are
			// injected into kvserver and StatsCompactor respectively.
			// The cleanupInterceptor calculates the number of expected "wide scan"
			// that should be issued by the StatsCompactor.
			// The kvInterceptor counts the number of actual "wide scan" KV Request
			// issued.
			kvInterceptor.reset()
			cleanupInterceptor.reset()
			kvInterceptor.enable()

			err = statsCompactor.DeleteOldestEntries(ctx)
			kvInterceptor.disable()
			require.NoError(t, err)

			expectedNumberOfWideScans := cleanupInterceptor.getExpectedNumberOfWideScans()
			actualNumberOfWideScans := kvInterceptor.getTotalWideScans()

			require.Equal(t,
				expectedNumberOfWideScans,
				actualNumberOfWideScans,
				"expected %d number of wide scans issued, but %d number of "+
					"wide scan issued", expectedNumberOfWideScans, actualNumberOfWideScans,
			)

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

func TestSQLStatsCompactionJobMarkedAsAutomatic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()

	t.Logf("starting test server")
	ctx := context.Background()
	server, conn, _ := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(conn)

	t.Logf("launching the stats compaction job")
	jobID, err := launchSQLStatsCompactionJob(server)
	require.NoError(t, err)

	t.Logf("checking the job status")
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

	t.Logf("test complete")
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

func (k *kvScanInterceptor) intercept(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
	if atomic.LoadInt32(&k.enabled) == 0 {
		return nil
	}
	if req, ok := ba.GetArg(kvpb.Scan); ok {
		_, tableID, _ := encoding.DecodeUvarintAscending(req.(*kvpb.ScanRequest).Key)
		if tableID == stmtStatsTableID || tableID == txnStatsTableID {
			prettyKey := roachpb.PrettyPrintKey([]encoding.Direction{}, req.(*kvpb.ScanRequest).Key)

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
