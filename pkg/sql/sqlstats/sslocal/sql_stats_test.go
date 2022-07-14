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
	"math"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/outliers"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/jackc/pgx/v4"
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

func TestExplicitTxnFingerprintAccounting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	type tc struct {
		statements          []string
		fingerprints        []string
		curFingerprintCount int64
		implicit            bool
	}

	testCases := []tc{
		{
			statements: []string{
				"SELECT 1",
			},
			fingerprints: []string{
				"SELECT _",
			},
			curFingerprintCount: 2, /* 1 stmt + 1 txn */
			implicit:            true,
		},
		{
			statements: []string{
				"BEGIN",
				"SELECT 1",
				"SELECT 1, 1",
				"COMMIT",
			},
			fingerprints: []string{
				"BEGIN",
				"SELECT _",
				"SELECT _, _",
				"COMMIT",
			},
			curFingerprintCount: 7, /* 4 stmt + 1 txn + prev count */
			implicit:            false,
		},
		{
			statements: []string{
				"BEGIN",
				"SELECT 1",
				"SELECT 1, 1",
				"COMMIT",
			},
			fingerprints: []string{
				"BEGIN",
				"SELECT _",
				"SELECT _, _",
				"COMMIT",
			},
			curFingerprintCount: 7, /* prev count */
			implicit:            false,
		},
		{
			statements: []string{
				"BEGIN",
				"SELECT 1",
				"SELECT 1, 1",
				"SELECT 1, 1, 1",
				"COMMIT",
			},
			fingerprints: []string{
				"BEGIN",
				"SELECT _",
				"SELECT _, _",
				"SELECT _, _, _",
				"COMMIT",
			},
			curFingerprintCount: 13, /* 5 stmt + 1 txn + prev count */
			implicit:            false,
		},
	}

	st := cluster.MakeTestingClusterSettings()
	monitor := mon.NewUnlimitedMonitor(
		context.Background(), "test", mon.MemoryResource,
		nil /* curCount */, nil /* maxHist */, math.MaxInt64, st,
	)

	sqlStats := sslocal.New(
		st,
		sqlstats.MaxMemSQLStatsStmtFingerprints,
		sqlstats.MaxMemSQLStatsTxnFingerprints,
		nil, /* curMemoryBytesCount */
		nil, /* maxMemoryBytesHist */
		outliers.New(st, outliers.NewMetrics()),
		monitor,
		nil, /* reportingSink */
		nil, /* knobs */
	)

	appStats := sqlStats.GetApplicationStats("" /* appName */)
	statsCollector := sslocal.NewStatsCollector(
		st,
		appStats,
		sessionphase.NewTimes(),
		nil, /* knobs */
	)

	recordStats := func(testCase *tc) {
		var txnFingerprintID roachpb.TransactionFingerprintID
		txnFingerprintIDHash := util.MakeFNV64()
		statsCollector.StartTransaction()
		defer func() {
			statsCollector.EndTransaction(ctx, txnFingerprintID)
			require.NoError(t,
				statsCollector.
					RecordTransaction(ctx, txnFingerprintID, sqlstats.RecordedTxnStats{}))
		}()
		for _, fingerprint := range testCase.fingerprints {
			stmtFingerprintID, err := statsCollector.RecordStatement(
				ctx,
				roachpb.StatementStatisticsKey{
					Query:       fingerprint,
					ImplicitTxn: testCase.implicit,
				},
				sqlstats.RecordedStmtStats{},
			)
			require.NoError(t, err)
			txnFingerprintIDHash.Add(uint64(stmtFingerprintID))
		}
		txnFingerprintID = roachpb.TransactionFingerprintID(txnFingerprintIDHash.Sum())
	}

	for _, tc := range testCases {
		recordStats(&tc)
		require.Equal(t, tc.curFingerprintCount, sqlStats.GetTotalFingerprintCount(),
			"testCase: %+v", tc)
	}
}

func TestAssociatingStmtStatsWithTxnFingerprint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	type simulatedTxn struct {
		stmtFingerprints               []string
		expectedStatsCountWhenEnabled  int
		expectedStatsCountWhenDisabled int
	}

	// The test will run these simulated txns serially, stopping to check
	// the cumulative statement stats counts along the way.
	simulatedTxns := []simulatedTxn{
		{
			stmtFingerprints: []string{
				"BEGIN",
				"SELECT _",
				"COMMIT",
			},
			expectedStatsCountWhenEnabled:  3,
			expectedStatsCountWhenDisabled: 3,
		},
		{
			stmtFingerprints: []string{
				"BEGIN",
				"SELECT _",
				"SELECT _, _",
				"COMMIT",
			},
			expectedStatsCountWhenEnabled:  7, // All 4 fingerprints look new, since they belong to a new txn fingerprint.
			expectedStatsCountWhenDisabled: 4, // Only the `SELECT _, _` looks new, since txn fingerprint doesn't matter.
		},
	}

	st := cluster.MakeTestingClusterSettings()
	updater := st.MakeUpdater()
	monitor := mon.NewUnlimitedMonitor(
		context.Background(),
		"test",
		mon.MemoryResource,
		nil,
		nil,
		math.MaxInt64,
		st,
	)

	testutils.RunTrueAndFalse(t, "enabled", func(t *testing.T, enabled bool) {
		// Establish the cluster setting.
		setting := sslocal.AssociateStmtWithTxnFingerprint
		err := updater.Set(ctx, setting.Key(), settings.EncodedValue{
			Value: settings.EncodeBool(enabled),
			Type:  setting.Typ(),
		})
		require.NoError(t, err)

		// Construct the SQL Stats machinery.
		sqlStats := sslocal.New(
			st,
			sqlstats.MaxMemSQLStatsStmtFingerprints,
			sqlstats.MaxMemSQLStatsTxnFingerprints,
			nil,
			nil,
			outliers.New(st, outliers.NewMetrics()),
			monitor,
			nil,
			nil,
		)
		appStats := sqlStats.GetApplicationStats("" /* appName */)
		statsCollector := sslocal.NewStatsCollector(
			st,
			appStats,
			sessionphase.NewTimes(),
			nil, /* knobs */
		)

		for _, txn := range simulatedTxns {
			// Collect stats for the simulated transaction.
			txnFingerprintIDHash := util.MakeFNV64()
			statsCollector.StartTransaction()

			for _, fingerprint := range txn.stmtFingerprints {
				stmtFingerprintID, err := statsCollector.RecordStatement(
					ctx,
					roachpb.StatementStatisticsKey{Query: fingerprint},
					sqlstats.RecordedStmtStats{},
				)
				require.NoError(t, err)
				txnFingerprintIDHash.Add(uint64(stmtFingerprintID))
			}

			transactionFingerprintID := roachpb.TransactionFingerprintID(txnFingerprintIDHash.Sum())
			statsCollector.EndTransaction(ctx, transactionFingerprintID)
			err := statsCollector.RecordTransaction(ctx, transactionFingerprintID, sqlstats.RecordedTxnStats{})
			require.NoError(t, err)

			// Gather the collected stats so that we can assert on them.
			var stats []*roachpb.CollectedStatementStatistics
			err = statsCollector.IterateStatementStats(
				ctx,
				&sqlstats.IteratorOptions{},
				func(_ context.Context, s *roachpb.CollectedStatementStatistics) error {
					stats = append(stats, s)
					return nil
				},
			)
			require.NoError(t, err)

			// Make sure we see the counts we expect.
			expectedCount := txn.expectedStatsCountWhenEnabled
			if !enabled {
				expectedCount = txn.expectedStatsCountWhenDisabled
			}
			require.Equal(t, expectedCount, len(stats), "testCase: %+v, stats: %+v", txn, stats)
		}
	})
}

func TestTxnStatsDiscardedAfterPrematureStatementExecutionAbortion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	server, sqlConn, _ := serverutils.StartServer(t, params)

	defer server.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlConn)

	sqlDB.Exec(t, "CREATE TABLE t AS SELECT generate_series(1, 50)")
	sqlDB.Exec(t, "SET large_full_scan_rows=49")
	sqlDB.Exec(t, "SET disallow_full_table_scans=on")

	// Simulate a premature statement exec abort by violating the full table
	// scan constraint.
	sqlDB.ExpectErr(t,
		".*contains a full table/index scan.*", /* errRe */
		"SELECT * FROM t",                      /* query */
	)
	sqlDB.Exec(t, "SET disallow_full_table_scans=off")

	// Ensure we don't generate transaction stats entry where the list of stmt
	// fingerprint IDs is nil.
	sqlDB.CheckQueryResults(t, `
SELECT
  count(*)
FROM
  crdb_internal.transaction_statistics
WHERE
  jsonb_array_length(metadata -> 'stmtFingerprintIDs') = 0
`,
		[][]string{{"0"}})
}

func TestUnprivilegedUserReset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlConn := sqlutils.MakeSQLRunner(conn)
	sqlConn.Exec(t, "CREATE USER nonAdminUser")

	ie := s.InternalExecutor().(*sql.InternalExecutor)

	_, err := ie.ExecEx(
		ctx,
		"test-reset-sql-stats-as-non-admin-user",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{
			User: username.MakeSQLUsernameFromPreNormalizedString("nonAdminUser"),
		},
		"SELECT crdb_internal.reset_sql_stats()",
	)

	require.Contains(t, err.Error(), "requires admin privilege")
}

func TestTransactionServiceLatencyOnExtendedProtocol(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testData := []*struct {
		query        string
		placeholders []interface{}
		phaseTimes   *sessionphase.Times
	}{
		{
			query:        "SELECT $1::INT8",
			placeholders: []interface{}{1},
			phaseTimes:   nil,
		},
	}

	waitTxnFinish := make(chan struct{})
	currentTestCaseIdx := 0
	const latencyThreshold = time.Second * 5

	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLExecutor = &sql.ExecutorTestingKnobs{
		OnRecordTxnFinish: func(isInternal bool, phaseTimes *sessionphase.Times, stmt string) {
			if !isInternal && testData[currentTestCaseIdx].query == stmt {
				testData[currentTestCaseIdx].phaseTimes = phaseTimes.Clone()
				go func() {
					waitTxnFinish <- struct{}{}
				}()
			}
		},
	}
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	pgURL, cleanupGoDB := sqlutils.PGUrl(
		t, s.ServingSQLAddr(), "StartServer", url.User(username.RootUser))
	defer cleanupGoDB()
	c, err := pgx.Connect(ctx, pgURL.String())
	require.NoError(t, err, "error connecting with pg url")

	for currentTestCaseIdx < len(testData) {
		tc := testData[currentTestCaseIdx]
		// Make extended protocol query
		_ = c.QueryRow(ctx, tc.query, tc.placeholders...)
		require.NoError(t, err, "error scanning row")
		<-waitTxnFinish

		// Ensure test case phase times are populated by query txn.
		require.True(t, tc.phaseTimes != nil)
		// Ensure SessionTransactionStarted variable is populated.
		require.True(t, !tc.phaseTimes.GetSessionPhaseTime(sessionphase.SessionTransactionStarted).IsZero())
		// Ensure compute transaction service latency is within a reasonable threshold.
		require.True(t, tc.phaseTimes.GetTransactionServiceLatency() < latencyThreshold)
		currentTestCaseIdx++
	}
}
