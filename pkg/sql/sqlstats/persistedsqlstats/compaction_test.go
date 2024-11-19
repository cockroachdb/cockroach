// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstats_test

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatstestutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestSQLStatsCompactorNilTestingKnobCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	server := srv.ApplicationLayer()

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

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("stmtCount=%d/maxPersistedRowLimit=%d/rowsDeletePerTxn=%d",
			tc.stmtCount,
			tc.maxPersistedRowLimit,
			tc.rowsToDeletePerTxn,
		), func(t *testing.T) {
			kvInterceptor := newKVScanIntercceptor()
			cleanupInterceptor := cleanupInterceptor{}

			stubSqlStatsTime := timeutil.Now().Add(-2 * time.Hour).Truncate(time.Hour)
			knobs := sqlstats.CreateTestingKnobs()
			knobs.StubTimeNow = func() time.Time {
				return stubSqlStatsTime
			}
			srv, conn, _ := serverutils.StartServer(
				t, base.TestServerArgs{
					Knobs: base.TestingKnobs{
						SQLStatsKnobs: knobs,
						Store: &kvserver.StoreTestingKnobs{
							TestingRequestFilter:      kvInterceptor.intercept,
							DisableLoadBasedSplitting: true,
						},
					},
				},
			)
			defer srv.Stopper().Stop(ctx)
			server := srv.ApplicationLayer()

			sqlConn := sqlutils.MakeSQLRunner(conn)
			internalExecutor := server.InternalExecutor().(isql.Executor)

			func() {
				// Set the codec from the live server, which determines the tenant
				// ID prefix to strip.
				kvInterceptor.codec = server.Codec()
			}()

			// Disable automatic flush since the test will insert mocked data manually.
			sqlConn.Exec(t, "SET CLUSTER SETTING sql.stats.flush.interval = '24h'")
			// Disable activity update flush which also does a scan on the stats table
			sqlConn.Exec(t, "SET CLUSTER SETTING sql.stats.activity.flush.enabled = false")
			// Change the automatic compaction job to avoid it running during the test.
			// Test creates a new compactor and calls it directly.
			sqlConn.Exec(t, "SET CLUSTER SETTING sql.stats.cleanup.recurrence = '@yearly';")

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

			stmts := make([]appstatspb.CollectedStatementStatistics, tc.stmtCount)
			txns := make([]appstatspb.CollectedTransactionStatistics, tc.stmtCount)
			for i := 0; i < tc.stmtCount; i++ {
				stmts[i].ID = appstatspb.StmtFingerprintID(i)
				stmts[i].Key.TransactionFingerprintID = appstatspb.TransactionFingerprintID(i)
				stmts[i].AggregatedTs = stubSqlStatsTime
				txns[i].TransactionFingerprintID = appstatspb.TransactionFingerprintID(i)
				txns[i].AggregatedTs = stubSqlStatsTime
			}
			require.NoError(t, sqlstatstestutil.InsertMockedIntoSystemStmtStats(ctx, internalExecutor, stmts, 1))
			require.NoError(t, sqlstatstestutil.InsertMockedIntoSystemTxnStats(ctx, internalExecutor, txns, 1))

			sqlStatsKnobs := sqlstats.CreateTestingKnobs()
			sqlStatsKnobs.OnCleanupStartForShard = cleanupInterceptor.intercept
			statsCompactor := persistedsqlstats.NewStatsCompactor(
				server.ClusterSettings(),
				server.InternalDB().(isql.DB),
				metric.NewCounter(metric.Metadata{}),
				sqlStatsKnobs,
			)

			// Initial compaction should remove the all the oldest entries.
			expectedDeletedStmtFingerprints, expectedDeletedTxnFingerprints :=
				getTopSortedFingerprints(t, sqlConn, tc.maxPersistedRowLimit)

			expectedCount := int(math.Min(float64(tc.stmtCount), float64(tc.maxPersistedRowLimit)))
			// Sanity check.
			require.Equal(t, expectedCount, len(expectedDeletedStmtFingerprints))
			require.Equal(t, expectedCount, len(expectedDeletedTxnFingerprints))

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
			expectedNumberOfWideScans := cleanupInterceptor.getExpectedNumberOfWideScans()
			require.NoError(t, err)

			actualNumberOfWideScans := kvInterceptor.getTotalWideScans()

			if expectedNumberOfWideScans != actualNumberOfWideScans {
				t.Fatalf("expected %d number of wide scans issued, but %d number of "+
					"wide scan issued\ndetails: %v", expectedNumberOfWideScans,
					actualNumberOfWideScans, kvInterceptor.wideScanDetails)
			}

			actualStmtFingerprints, actualTxnFingerprints :=
				getTopSortedFingerprints(t, sqlConn, 0 /* limit */)

			require.GreaterOrEqual(t, expectedCount, len(actualStmtFingerprints))
			require.GreaterOrEqual(t, expectedCount, len(actualTxnFingerprints))
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
			require.GreaterOrEqual(t, expectedCount, stmtStatsCnt)
			require.GreaterOrEqual(t, expectedCount, txnStatsCnt)
		})
	}
}

func TestSQLStatsCompactionJobMarkedAsAutomatic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var params base.TestServerArgs
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()

	t.Logf("starting test server")
	ctx := context.Background()
	server, conn, _ := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(ctx)
	s := server.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(conn)

	t.Logf("launching the stats compaction job")
	jobID, err := launchSQLStatsCompactionJob(s)
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

func launchSQLStatsCompactionJob(
	server serverutils.ApplicationLayerInterface,
) (jobspb.JobID, error) {
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

const (
	StatementStatisticsTableID   = 42
	TransactionStatisticsTableID = 43
)

// Tables 42 and 43 and are the IDs of the statement and transactions
// tables respectively. See `StatementStatisticsTableID` and
// `TransactionStatisticsTableID`.
var kvReqWideScanStartKeyPattern = regexp.MustCompile(`(/Tenant/\d+)?/Table/((42)|(43))/[0-9]{1,2}/[0-9]$`)

type kvScanInterceptor struct {
	totalWideScan   atomic.Int64
	enabled         int32
	wideScanDetails []string

	// The codec must be set before the interceptor is enabled.
	codec keys.SQLCodec

	mu struct {
		syncutil.Mutex
		// Track the txnIDs of the scans since
		// the txn could be retried.
		txnIDs map[uuid.UUID]struct{}
	}
}

func newKVScanIntercceptor() *kvScanInterceptor {
	k := kvScanInterceptor{}
	k.mu.txnIDs = make(map[uuid.UUID]struct{})
	return &k
}

func (k *kvScanInterceptor) reset() {
	k.totalWideScan.Store(0)
	k.wideScanDetails = []string{}
}

func (k *kvScanInterceptor) getTotalWideScans() int64 {
	return k.totalWideScan.Load()
}

func (k *kvScanInterceptor) enable() {
	atomic.StoreInt32(&k.enabled, 1)
}

func (k *kvScanInterceptor) disable() {
	atomic.StoreInt32(&k.enabled, 0)
}

func (k *kvScanInterceptor) intercept(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
	if atomic.LoadInt32(&k.enabled) == 0 {
		return nil
	}

	req, ok := ba.GetArg(kvpb.Scan)
	if !ok {
		return nil
	}

	_, tableID, err := k.codec.DecodeTablePrefix(req.(*kvpb.ScanRequest).Key)
	if err != nil {
		log.Warningf(ctx, "unable to decode prefix: %v", err)
	}

	// Ensure the request is for one of the sql stats tables.
	if tableID != StatementStatisticsTableID && tableID != TransactionStatisticsTableID {
		return nil
	}

	k.mu.Lock()
	defer k.mu.Unlock()
	// Avoid double counting the same txn.
	if ba.Txn != nil {
		txnID := ba.Txn.ID
		if _, ok := k.mu.txnIDs[txnID]; ok {
			return nil
		}
		k.mu.txnIDs[txnID] = struct{}{}
	}

	prettyKey := roachpb.PrettyPrintKey([]encoding.Direction{}, req.(*kvpb.ScanRequest).Key)
	keyMatchedWideScan := kvReqWideScanStartKeyPattern.MatchString(prettyKey)

	if keyMatchedWideScan {
		k.wideScanDetails = append(k.wideScanDetails, fmt.Sprintf("wide scan in %v", ba))
		k.totalWideScan.Add(1)
	}

	return nil
}

type cleanupInterceptor struct {
	expectedNumberOfWideScans atomic.Int64
}

func (c *cleanupInterceptor) reset() {
	c.expectedNumberOfWideScans.Store(0)
}

func (c *cleanupInterceptor) intercept(_ int, existingCountInShard, shardLimit int64) {
	if existingCountInShard > shardLimit {
		c.expectedNumberOfWideScans.Add(1)
	}
}

func (c *cleanupInterceptor) getExpectedNumberOfWideScans() int64 {
	// The first part of the equation is the number of wide scans that
	// result from getting the shard count for each shard in each table.
	// The second part is the number of wide scans that result from the
	// delete statement, which is only executed if the number of rows
	// in the shard exceeds the shard limit.
	return systemschema.SQLStatsHashShardBucketCount*2 + c.expectedNumberOfWideScans.Load()
}
