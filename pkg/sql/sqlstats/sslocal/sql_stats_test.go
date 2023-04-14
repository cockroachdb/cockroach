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
	gosql "database/sql"
	"encoding/json"
	"math"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/jackc/pgx/v4"
	"github.com/lib/pq"
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
				statistics *appstatspb.CollectedStatementStatistics,
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
		id    appstatspb.StmtFingerprintID
		key   appstatspb.StatementStatisticsKey
		stats appstatspb.StatementStatistics
	}{
		{
			id: 0,
			key: appstatspb.StatementStatisticsKey{
				App:      "app1",
				Query:    "SELECT 1",
				Database: "testdb",
			},
			stats: appstatspb.StatementStatistics{
				Count: 7,
			},
		},
		{
			id: 0,
			key: appstatspb.StatementStatisticsKey{
				App:      "app0",
				Query:    "SELECT 1",
				Database: "testdb",
			},
			stats: appstatspb.StatementStatistics{
				Count: 2,
			},
		},
		{
			id: 1,
			key: appstatspb.StatementStatisticsKey{
				App:      "app100",
				Query:    "SELECT 1,1",
				Database: "testdb",
			},
			stats: appstatspb.StatementStatistics{
				Count: 31,
			},
		},
		{
			id: 1,
			key: appstatspb.StatementStatisticsKey{
				App:      "app0",
				Query:    "SELECT 1,1",
				Database: "testdb",
			},
			stats: appstatspb.StatementStatistics{
				Count: 32,
			},
		},
		{
			id: 0,
			key: appstatspb.StatementStatisticsKey{
				App:      "app1",
				Query:    "SELECT 1",
				Database: "testdb",
			},
			stats: appstatspb.StatementStatistics{
				Count: 33,
			},
		},
		{
			id: 1,
			key: appstatspb.StatementStatisticsKey{
				App:      "app100",
				Query:    "SELECT 1,1",
				Database: "testdb",
			},
			stats: appstatspb.StatementStatistics{
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
				statistics *appstatspb.CollectedStatementStatistics,
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
		stats appstatspb.CollectedTransactionStatistics
	}{
		{
			stats: appstatspb.CollectedTransactionStatistics{
				TransactionFingerprintID: appstatspb.TransactionFingerprintID(0),
				App:                      "app1",
				Stats: appstatspb.TransactionStatistics{
					Count: 7,
				},
			},
		},
		{
			stats: appstatspb.CollectedTransactionStatistics{
				TransactionFingerprintID: appstatspb.TransactionFingerprintID(0),
				App:                      "app0",
				Stats: appstatspb.TransactionStatistics{
					Count: 2,
				},
			},
		},
		{
			stats: appstatspb.CollectedTransactionStatistics{
				TransactionFingerprintID: appstatspb.TransactionFingerprintID(1),
				App:                      "app100",
				Stats: appstatspb.TransactionStatistics{
					Count: 31,
				},
			},
		},
		{
			stats: appstatspb.CollectedTransactionStatistics{
				TransactionFingerprintID: appstatspb.TransactionFingerprintID(1),
				App:                      "app0",
				Stats: appstatspb.TransactionStatistics{
					Count: 32,
				},
			},
		},
		{
			stats: appstatspb.CollectedTransactionStatistics{
				TransactionFingerprintID: appstatspb.TransactionFingerprintID(0),
				App:                      "app1",
				Stats: appstatspb.TransactionStatistics{
					Count: 33,
				},
			},
		},
		{
			stats: appstatspb.CollectedTransactionStatistics{
				TransactionFingerprintID: appstatspb.TransactionFingerprintID(1),
				App:                      "app100",
				Stats: appstatspb.TransactionStatistics{
					Count: 2,
				},
			},
		},
	}

	expectedCount := make(map[appstatspb.TransactionFingerprintID]int64)
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

	foundStats := make(map[appstatspb.TransactionFingerprintID]int64)
	require.NoError(t,
		sqlStats.IterateTransactionStats(
			context.Background(),
			&sqlstats.IteratorOptions{},
			func(
				ctx context.Context,
				statistics *appstatspb.CollectedTransactionStatistics,
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

	insightsProvider := insights.New(st, insights.NewMetrics())
	sqlStats := sslocal.New(
		st,
		sqlstats.MaxMemSQLStatsStmtFingerprints,
		sqlstats.MaxMemSQLStatsTxnFingerprints,
		nil, /* curMemoryBytesCount */
		nil, /* maxMemoryBytesHist */
		insightsProvider.Writer,
		monitor,
		nil, /* reportingSink */
		nil, /* knobs */
		insightsProvider.LatencyInformation(),
	)

	appStats := sqlStats.GetApplicationStats("" /* appName */, false /* internal */)
	statsCollector := sslocal.NewStatsCollector(
		st,
		appStats,
		sessionphase.NewTimes(),
		nil, /* knobs */
	)

	recordStats := func(testCase *tc) {
		var txnFingerprintID appstatspb.TransactionFingerprintID
		txnFingerprintIDHash := util.MakeFNV64()
		statsCollector.StartTransaction()
		defer func() {
			statsCollector.EndTransaction(ctx, txnFingerprintID)
			require.NoError(t,
				statsCollector.
					RecordTransaction(ctx, txnFingerprintID, sqlstats.RecordedTxnStats{
						SessionData: &sessiondata.SessionData{
							SessionData: sessiondatapb.SessionData{
								UserProto:       username.RootUserName().EncodeProto(),
								UserID:          username.RootUserID,
								Database:        "defaultdb",
								ApplicationName: "appname_findme",
							},
						},
					}))
		}()
		for _, fingerprint := range testCase.fingerprints {
			stmtFingerprintID, err := statsCollector.RecordStatement(
				ctx,
				appstatspb.StatementStatisticsKey{
					Query:       fingerprint,
					ImplicitTxn: testCase.implicit,
				},
				sqlstats.RecordedStmtStats{},
			)
			require.NoError(t, err)
			txnFingerprintIDHash.Add(uint64(stmtFingerprintID))
		}
		txnFingerprintID = appstatspb.TransactionFingerprintID(txnFingerprintIDHash.Sum())
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
		insightsProvider := insights.New(st, insights.NewMetrics())
		sqlStats := sslocal.New(
			st,
			sqlstats.MaxMemSQLStatsStmtFingerprints,
			sqlstats.MaxMemSQLStatsTxnFingerprints,
			nil,
			nil,
			insightsProvider.Writer,
			monitor,
			nil,
			nil,
			insightsProvider.LatencyInformation(),
		)
		appStats := sqlStats.GetApplicationStats("" /* appName */, false /* internal */)
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
					appstatspb.StatementStatisticsKey{Query: fingerprint},
					sqlstats.RecordedStmtStats{},
				)
				require.NoError(t, err)
				txnFingerprintIDHash.Add(uint64(stmtFingerprintID))
			}

			transactionFingerprintID := appstatspb.TransactionFingerprintID(txnFingerprintIDHash.Sum())
			statsCollector.EndTransaction(ctx, transactionFingerprintID)
			err := statsCollector.RecordTransaction(ctx, transactionFingerprintID, sqlstats.RecordedTxnStats{
				SessionData: &sessiondata.SessionData{
					SessionData: sessiondatapb.SessionData{
						UserProto:       username.RootUserName().EncodeProto(),
						UserID:          username.RootUserID,
						Database:        "defaultdb",
						ApplicationName: "appname_findme",
					},
				},
			})
			require.NoError(t, err)

			// Gather the collected stats so that we can assert on them.
			var stats []*appstatspb.CollectedStatementStatistics
			err = statsCollector.IterateStatementStats(
				ctx,
				&sqlstats.IteratorOptions{},
				func(_ context.Context, s *appstatspb.CollectedStatementStatistics) error {
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

func TestFingerprintCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	testServer, sqlConn, _ := serverutils.StartServer(t, params)
	defer func() {
		require.NoError(t, sqlConn.Close())
		testServer.Stopper().Stop(ctx)
	}()

	testConn := sqlutils.MakeSQLRunner(sqlConn)
	testConn.Exec(t, "CREATE TABLE t (v INT)")

	var count int64

	t.Run("test hide constants", func(t *testing.T) {
		testConn.Exec(t, "SET application_name = 'app1'")
		testCases := []struct {
			stmt        string
			fingerprint string
			count       int64
		}{
			{
				stmt:        "SELECT * FROM t WHERE v IN (1)",
				fingerprint: "SELECT * FROM t WHERE v IN (_,)",
				count:       1,
			},
			{
				stmt:        "SELECT * FROM t WHERE v IN (1,2)",
				fingerprint: "SELECT * FROM t WHERE v IN (_, _)",
				count:       1,
			},
			{
				stmt:        "SELECT * FROM t WHERE v IN (1,2,3)",
				fingerprint: "SELECT * FROM t WHERE v IN (_, _, __more1_10__)",
				count:       1,
			},
			{
				stmt:        "SELECT * FROM t WHERE v IN (1,2,3,4,5,6,7,8,9,10,11,12)",
				fingerprint: "SELECT * FROM t WHERE v IN (_, _, __more1_10__)",
				count:       2,
			},
			{
				stmt:        "SELECT * FROM t WHERE v IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14)",
				fingerprint: "SELECT * FROM t WHERE v IN (_, _, __more10_100__)",
				count:       1,
			},
			{
				stmt:        "SELECT * FROM t WHERE v IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)",
				fingerprint: "SELECT * FROM t WHERE v IN (_, _, __more10_100__)",
				count:       2,
			},
			{
				stmt: "SELECT * FROM t WHERE v IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25," +
					"26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58," +
					"59,60,61,62,63,64)",
				fingerprint: "SELECT * FROM t WHERE v IN (_, _, __more10_100__)",
				count:       3,
			},
			{
				stmt: "SELECT * FROM t WHERE v IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25," +
					"26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58," +
					"59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91," +
					"92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110)",
				fingerprint: "SELECT * FROM t WHERE v IN (_, _, __more100__)",
				count:       1,
			},
			{
				stmt: "SELECT * FROM t WHERE v IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25," +
					"26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58," +
					"59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91," +
					"92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118," +
					"119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143," +
					"144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168," +
					"169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193," +
					"194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218," +
					"219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243," +
					"244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268," +
					"269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293," +
					"294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318," +
					"319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343," +
					"344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368," +
					"369,370,371,372,373,374,375,376,377,378,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393," +
					"394,395,396,397,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418," +
					"419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443," +
					"444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,466,467,468," +
					"469,470,471,472,473,474,475,476,477,478,479,480,481,482,483,484,485,486,487,488,489,490,491,492,493," +
					"494,495,496,497,498,499,500,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517,518," +
					"519,520,521,522,523,524,525,526,527,528,529,530,531,532,533,534,535,536,537,538,539,540,541,542,543)",
				fingerprint: "SELECT * FROM t WHERE v IN (_, _, __more500__)",
				count:       1,
			},
			{
				stmt: "SELECT * FROM t WHERE v IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25," +
					"26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58," +
					"59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91," +
					"92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118," +
					"119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143," +
					"144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168," +
					"169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193," +
					"194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218," +
					"219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243," +
					"244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268," +
					"269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293," +
					"294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318," +
					"319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343," +
					"344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368," +
					"369,370,371,372,373,374,375,376,377,378,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393," +
					"394,395,396,397,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418," +
					"419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443," +
					"444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,466,467,468," +
					"469,470,471,472,473,474,475,476,477,478,479,480,481,482,483,484,485,486,487,488,489,490,491,492,493," +
					"494,495,496,497,498,499,500,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517,518," +
					"519,520,521,522,523,524,525,526,527,528,529,530,531,532,533,534,535,536,537,538,539,540,541,542,543," +
					"544,545,546,547,548,549,550,551,552,553,554,555,556,557,558,559,560,561,562,563,564,565,566,567,568," +
					"569,570,571,572,573,574,575,576,577,578,579,580,581,582,583,584,585,586,587,588,589,590,591,592,593," +
					"594,595,596,597,598,599,600,601,602,603,604,605,606,607,608,609,610,611,612,613,614,615,616,617,618," +
					"619,620,621,622,623,624,625,626,627,628,629,630,631,632,633,634,635,636,637,638,639,640,641,642,643," +
					"644,645,646,647,648,649,650,651,652,653,654,655,656,657,658,659,660,661,662,663,664,665,666,667,668," +
					"669,670,671,672,673,674,675,676,677,678,679,680,681,682,683,684,685,686,687,688,689,690,691,692,693," +
					"694,695,696,697,698,699,700,701,702,703,704,705,706,707,708,709,710,711,712,713,714,715,716,717,718," +
					"719,720,721,722,723,724,725,726,727,728,729,730,731,732,733,734,735,736,737,738,739,740,741,742,743," +
					"744,745,746,747,748,749,750,751,752,753,754,755,756,757,758,759,760,761,762,763,764,765,766,767,768," +
					"769,770,771,772,773,774,775,776,777,778,779,780,781,782,783,784,785,786,787,788,789,790,791,792,793," +
					"794,795,796,797,798,799,800,801,802,803,804,805,806,807,808,809,810,811,812,813,814,815,816,817,818," +
					"819,820,821,822,823,824,825,826,827,828,829,830,831,832,833,834,835,836,837,838,839,840,841,842,843," +
					"844,845,846,847,848,849,850,851,852,853,854,855,856,857,858,859,860,861,862,863,864,865,866,867,868," +
					"869,870,871,872,873,874,875,876,877,878,879,880,881,882,883,884,885,886,887,888,889,890,891,892,893," +
					"894,895,896,897,898,899,900,901,902,903,904,905,906,907,908,909,910,911,912,913,914,915,916,917,918," +
					"919,920,921,922,923,924,925,926,927,928,929,930,931,932,933,934,935,936,937,938,939,940,941,942,943," +
					"944,945,946,947,948,949,950,951,952,953,954,955,956,957,958,959,960,961,962,963,964,965,966,967,968," +
					"969,970,971,972,973,974,975,976,977,978,979,980,981,982,983,984,985,986,987,988,989,990)",
				fingerprint: "SELECT * FROM t WHERE v IN (_, _, __more900__)",
				count:       1,
			},
			{
				stmt: "SELECT * FROM t WHERE v IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25," +
					"26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58," +
					"59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91," +
					"92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118," +
					"119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143," +
					"144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168," +
					"169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193," +
					"194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218," +
					"219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243," +
					"244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268," +
					"269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293," +
					"294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318," +
					"319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343," +
					"344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368," +
					"369,370,371,372,373,374,375,376,377,378,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393," +
					"394,395,396,397,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418," +
					"419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443," +
					"444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,466,467,468," +
					"469,470,471,472,473,474,475,476,477,478,479,480,481,482,483,484,485,486,487,488,489,490,491,492,493," +
					"494,495,496,497,498,499,500,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517,518," +
					"519,520,521,522,523,524,525,526,527,528,529,530,531,532,533,534,535,536,537,538,539,540,541,542,543," +
					"544,545,546,547,548,549,550,551,552,553,554,555,556,557,558,559,560,561,562,563,564,565,566,567,568," +
					"569,570,571,572,573,574,575,576,577,578,579,580,581,582,583,584,585,586,587,588,589,590,591,592,593," +
					"594,595,596,597,598,599,600,601,602,603,604,605,606,607,608,609,610,611,612,613,614,615,616,617,618," +
					"619,620,621,622,623,624,625,626,627,628,629,630,631,632,633,634,635,636,637,638,639,640,641,642,643," +
					"644,645,646,647,648,649,650,651,652,653,654,655,656,657,658,659,660,661,662,663,664,665,666,667,668," +
					"669,670,671,672,673,674,675,676,677,678,679,680,681,682,683,684,685,686,687,688,689,690,691,692,693," +
					"694,695,696,697,698,699,700,701,702,703,704,705,706,707,708,709,710,711,712,713,714,715,716,717,718," +
					"719,720,721,722,723,724,725,726,727,728,729,730,731,732,733,734,735,736,737,738,739,740,741,742,743," +
					"744,745,746,747,748,749,750,751,752,753,754,755,756,757,758,759,760,761,762,763,764,765,766,767,768," +
					"769,770,771,772,773,774,775,776,777,778,779,780,781,782,783,784,785,786,787,788,789,790,791,792,793," +
					"794,795,796,797,798,799,800,801,802,803,804,805,806,807,808,809,810,811,812,813,814,815,816,817,818," +
					"819,820,821,822,823,824,825,826,827,828,829,830,831,832,833,834,835,836,837,838,839,840,841,842,843," +
					"844,845,846,847,848,849,850,851,852,853,854,855,856,857,858,859,860,861,862,863,864,865,866,867,868," +
					"869,870,871,872,873,874,875,876,877,878,879,880,881,882,883,884,885,886,887,888,889,890,891,892,893," +
					"894,895,896,897,898,899,900,901,902,903,904,905,906,907,908,909,910,911,912,913,914,915,916,917,918," +
					"919,920,921,922,923,924,925,926,927,928,929,930,931,932,933,934,935,936,937,938,939,940,941,942,943," +
					"944,945,946,947,948,949,950,951,952,953,954,955,956,957,958,959,960,961,962,963,964,965,966,967,968," +
					"969,970,971,972,973,974,975,976,977,978,979,980,981,982,983,984,985,986,987,988,989,990,991,992,993," +
					"994,995,996,997,998,999,1000,1001,1002,1003,1004,1005,1006,1007,1008,1009,1010,1011,1012,1013,1014," +
					"1015,1016,1017,1018,1019,1020,1021,1022,1023,1024,1025,1026,1027,1028,1029,1030,1031,1032,1033,1034," +
					"1035,1036,1037,1038,1039,1040,1041,1042,1043,1044,1045,1046,1047,1048,1049,1050,1051,1052,1053,1054," +
					"1055,1056,1057,1058,1059,1060,1061,1062,1063,1064,1065,1066,1067,1068,1069,1070,1071,1072,1073,1074," +
					"1075,1076,1077,1078,1079,1080,1081,1082,1083,1084,1085,1086,1087,1088,1089,1090,1091,1092,1093,1094," +
					"1095,1096,1097,1098,1099,1100,1101,1102,1103,1104,1105,1106,1107,1108,1109,1110,1111,1112,1113,1114," +
					"1115,1116,1117,1118,1119,1120,1121,1122,1123,1124,1125,1126,1127,1128,1129,1130,1131,1132,1133,1134," +
					"1135,1136,1137,1138,1139,1140,1141,1142,1143,1144,1145,1146,1147,1148,1149,1150,1151,1152,1153,1154," +
					"1155,1156,1157,1158,1159,1160,1161,1162,1163,1164,1165,1166,1167,1168,1169,1170,1171,1172,1173,1174," +
					"1175,1176,1177,1178,1179,1180,1181,1182,1183,1184,1185,1186,1187,1188,1189,1190,1191,1192,1193,1194," +
					"1195,1196,1197,1198,1199,1200,1201,1202,1203,1204,1205,1206,1207,1208,1209,1210,1211,1212,1213,1214," +
					"1215,1216,1217,1218,1219,1220,1221,1222,1223,1224,1225,1226,1227,1228,1229,1230,1231,1232,1233,1234," +
					"1235,1236,1237,1238,1239,1240,1241,1242,1243,1244,1245,1246,1247,1248,1249,1250,1251,1252,1253,1254," +
					"1255,1256,1257,1258,1259,1260,1261,1262,1263,1264,1265,1266,1267,1268,1269,1270,1271,1272,1273,1274," +
					"1275,1276,1277,1278,1279,1280,1281,1282,1283,1284,1285,1286,1287,1288,1289,1290,1291,1292,1293,1294," +
					"1295,1296,1297,1298,1299,1300,1301,1302,1303,1304,1305,1306,1307,1308,1309,1310,1311,1312,1313,1314)",
				fingerprint: "SELECT * FROM t WHERE v IN (_, _, __more1000_plus__)",
				count:       1,
			},
			{
				stmt: "SELECT * FROM t WHERE v IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25," +
					"26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58," +
					"59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91," +
					"92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118," +
					"119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143," +
					"144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168," +
					"169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193," +
					"194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218," +
					"219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243," +
					"244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268," +
					"269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293," +
					"294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318," +
					"319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343," +
					"344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368," +
					"369,370,371,372,373,374,375,376,377,378,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393," +
					"394,395,396,397,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418," +
					"419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443," +
					"444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,466,467,468," +
					"469,470,471,472,473,474,475,476,477,478,479,480,481,482,483,484,485,486,487,488,489,490,491,492,493," +
					"494,495,496,497,498,499,500,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517,518," +
					"519,520,521,522,523,524,525,526,527,528,529,530,531,532,533,534,535,536,537,538,539,540,541,542,543," +
					"544,545,546,547,548,549,550,551,552,553,554,555,556,557,558,559,560,561,562,563,564,565,566,567,568," +
					"569,570,571,572,573,574,575,576,577,578,579,580,581,582,583,584,585,586,587,588,589,590,591,592,593," +
					"594,595,596,597,598,599,600,601,602,603,604,605,606,607,608,609,610,611,612,613,614,615,616,617,618," +
					"619,620,621,622,623,624,625,626,627,628,629,630,631,632,633,634,635,636,637,638,639,640,641,642,643," +
					"644,645,646,647,648,649,650,651,652,653,654,655,656,657,658,659,660,661,662,663,664,665,666,667,668," +
					"669,670,671,672,673,674,675,676,677,678,679,680,681,682,683,684,685,686,687,688,689,690,691,692,693," +
					"694,695,696,697,698,699,700,701,702,703,704,705,706,707,708,709,710,711,712,713,714,715,716,717,718," +
					"719,720,721,722,723,724,725,726,727,728,729,730,731,732,733,734,735,736,737,738,739,740,741,742,743," +
					"744,745,746,747,748,749,750,751,752,753,754,755,756,757,758,759,760,761,762,763,764,765,766,767,768," +
					"769,770,771,772,773,774,775,776,777,778,779,780,781,782,783,784,785,786,787,788,789,790,791,792,793," +
					"794,795,796,797,798,799,800,801,802,803,804,805,806,807,808,809,810,811,812,813,814,815,816,817,818," +
					"819,820,821,822,823,824,825,826,827,828,829,830,831,832,833,834,835,836,837,838,839,840,841,842,843," +
					"844,845,846,847,848,849,850,851,852,853,854,855,856,857,858,859,860,861,862,863,864,865,866,867,868," +
					"869,870,871,872,873,874,875,876,877,878,879,880,881,882,883,884,885,886,887,888,889,890,891,892,893," +
					"894,895,896,897,898,899,900,901,902,903,904,905,906,907,908,909,910,911,912,913,914,915,916,917,918," +
					"919,920,921,922,923,924,925,926,927,928,929,930,931,932,933,934,935,936,937,938,939,940,941,942,943," +
					"944,945,946,947,948,949,950,951,952,953,954,955,956,957,958,959,960,961,962,963,964,965,966,967,968," +
					"969,970,971,972,973,974,975,976,977,978,979,980,981,982,983,984,985,986,987,988,989,990,991,992,993," +
					"994,995,996,997,998,999,1000,1001,1002,1003,1004,1005,1006,1007,1008,1009,1010,1011,1012,1013,1014," +
					"1015,1016,1017,1018,1019,1020,1021,1022,1023,1024,1025,1026,1027,1028,1029,1030,1031,1032,1033,1034," +
					"1035,1036,1037,1038,1039,1040,1041,1042,1043,1044,1045,1046,1047,1048,1049,1050,1051,1052,1053,1054," +
					"1055,1056,1057,1058,1059,1060,1061,1062,1063,1064,1065,1066,1067,1068,1069,1070,1071,1072,1073,1074," +
					"1075,1076,1077,1078,1079,1080,1081,1082,1083,1084,1085,1086,1087,1088,1089,1090,1091,1092,1093,1094," +
					"1095,1096,1097,1098,1099,1100,1101,1102,1103,1104,1105,1106,1107,1108,1109,1110,1111,1112,1113,1114," +
					"1115,1116,1117,1118,1119,1120,1121,1122,1123,1124,1125,1126,1127,1128,1129,1130,1131,1132,1133,1134," +
					"1135,1136,1137,1138,1139,1140,1141,1142,1143,1144,1145,1146,1147,1148,1149,1150,1151,1152,1153,1154," +
					"1155,1156,1157,1158,1159,1160,1161,1162,1163,1164,1165,1166,1167,1168,1169,1170,1171,1172,1173,1174," +
					"1175,1176,1177,1178,1179,1180,1181,1182,1183,1184,1185,1186,1187,1188,1189,1190,1191,1192,1193,1194," +
					"1195,1196,1197,1198,1199,1200,1201,1202,1203,1204,1205,1206,1207,1208,1209,1210,1211,1212,1213,1214," +
					"1215,1216,1217,1218,1219,1220,1221,1222,1223,1224,1225,1226,1227,1228,1229,1230,1231,1232,1233,1234," +
					"1235,1236,1237,1238,1239,1240,1241,1242,1243,1244,1245,1246,1247,1248,1249,1250,1251,1252,1253,1254," +
					"1255,1256,1257,1258,1259,1260,1261,1262,1263,1264,1265,1266,1267,1268,1269,1270,1271,1272,1273,1274," +
					"1275,1276,1277,1278,1279,1280,1281,1282,1283,1284,1285,1286,1287,1288,1289,1290,1291,1292,1293,1294," +
					"1295,1296,1297,1298,1299,1300,1301,1302,1303,1304,1305,1306,1307,1308,1309,1310,1311,1312,1313,1314," +
					"1315,1316,1317,1318,1319,1320,1321,1322,1323,1324,1325,1326,1327,1328,1329,1330,1331,1332,1333,1334," +
					"1335,1336,1337,1338,1339,1340,1341,1342,1343,1344,1345,1346,1347,1348,1349,1350,1351,1352,1353,1354," +
					"1355,1356,1357,1358,1359,1360,1361,1362,1363,1364,1365,1366,1367,1368,1369,1370,1371,1372,1373,1374," +
					"1375,1376,1377,1378,1379,1380,1381,1382,1383,1384,1385,1386,1387,1388,1389,1390,1391,1392,1393,1394," +
					"1395,1396,1397,1398,1399,1400,1401,1402,1403,1404,1405,1406,1407,1408,1409,1410,1411,1412,1413,1414," +
					"1415,1416,1417,1418,1419,1420,1421,1422,1423,1424,1425,1426,1427,1428,1429,1430,1431,1432,1433,1434," +
					"1435,1436,1437,1438,1439,1440,1441,1442,1443,1444,1445,1446,1447,1448,1449,1450,1451,1452,1453,1454," +
					"1455,1456,1457,1458,1459,1460,1461,1462,1463,1464,1465,1466,1467,1468,1469,1470,1471,1472,1473,1474," +
					"1475,1476,1477,1478,1479,1480,1481,1482,1483,1484,1485,1486,1487,1488,1489,1490,1491,1492,1493,1494," +
					"1495,1496,1497,1498,1499,1500,1501,1502,1503,1504,1505,1506,1507,1508,1509,1510,1511,1512,1513,1514," +
					"1515,1516,1517,1518,1519,1520,1521,1522,1523,1524,1525,1526,1527,1528,1529,1530,1531,1532,1533,1534," +
					"1535,1536,1537,1538,1539,1540,1541,1542,1543,1544,1545,1546,1547,1548,1549,1550,1551,1552,1553,1554," +
					"1555,1556,1557,1558,1559,1560,1561,1562,1563,1564,1565,1566,1567,1568,1569,1570,1571,1572,1573,1574," +
					"1575,1576,1577,1578,1579,1580,1581,1582,1583,1584,1585,1586,1587,1588,1589,1590,1591,1592,1593,1594," +
					"1595,1596,1597,1598,1599,1600,1601,1602,1603,1604,1605,1606,1607,1608,1609,1610,1611,1612,1613,1614," +
					"1615,1616,1617,1618,1619,1620,1621,1622,1623,1624,1625,1626,1627,1628,1629,1630,1631,1632,1633,1634," +
					"1635,1636,1637,1638,1639,1640,1641,1642,1643,1644,1645,1646,1647,1648,1649,1650,1651,1652,1653,1654," +
					"1655,1656,1657,1658,1659,1660,1661,1662,1663,1664,1665,1666,1667,1668,1669,1670,1671,1672,1673,1674," +
					"1675,1676,1677,1678,1679,1680,1681,1682,1683,1684,1685,1686,1687,1688,1689,1690,1691,1692,1693,1694," +
					"1695,1696,1697,1698,1699,1700,1701,1702,1703,1704,1705,1706,1707,1708,1709,1710,1711,1712,1713,1714," +
					"1715,1716,1717,1718,1719,1720,1721,1722,1723,1724,1725,1726,1727,1728,1729,1730,1731,1732,1733,1734," +
					"1735,1736,1737,1738,1739,1740,1741,1742,1743,1744,1745,1746,1747,1748,1749,1750,1751,1752,1753,1754," +
					"1755,1756,1757,1758,1759,1760,1761,1762,1763,1764,1765,1766,1767,1768,1769,1770,1771,1772,1773,1774," +
					"1775,1776,1777,1778,1779,1780,1781,1782,1783,1784,1785,1786,1787,1788,1789,1790,1791,1792,1793,1794," +
					"1795,1796,1797,1798,1799,1800,1801,1802,1803,1804,1805,1806,1807,1808,1809,1810,1811,1812,1813,1814," +
					"1815,1816,1817,1818,1819,1820,1821,1822,1823,1824,1825,1826,1827,1828,1829,1830,1831,1832,1833,1834," +
					"1835,1836,1837,1838,1839,1840,1841,1842,1843,1844,1845,1846,1847,1848,1849,1850,1851,1852,1853,1854," +
					"1855,1856,1857,1858,1859,1860,1861,1862,1863,1864,1865,1866,1867,1868,1869,1870,1871,1872,1873,1874," +
					"1875,1876,1877,1878,1879,1880,1881,1882,1883,1884,1885,1886,1887,1888,1889,1890,1891,1892,1893,1894," +
					"1895,1896,1897,1898,1899,1900,1901,1902,1903,1904,1905,1906,1907,1908,1909,1910,1911,1912,1913,1914," +
					"1915,1916,1917,1918,1919,1920,1921,1922,1923,1924,1925,1926,1927,1928,1929,1930,1931,1932,1933,1934," +
					"1935,1936,1937,1938,1939,1940,1941,1942,1943,1944,1945,1946,1947,1948,1949,1950,1951,1952,1953,1954," +
					"1955,1956,1957,1958,1959,1960,1961,1962,1963,1964,1965,1966,1967,1968,1969,1970,1971,1972,1973,1974," +
					"1975,1976,1977,1978,1979,1980,1981,1982,1983,1984,1985,1986,1987,1988,1989,1990,1991,1992,1993,1994," +
					"1995,1996,1997,1998,1999,2000)",
				fingerprint: "SELECT * FROM t WHERE v IN (_, _, __more1000_plus__)",
				count:       2,
			},
		}

		for _, tc := range testCases {
			testConn.Exec(t, tc.stmt)
			rows := testConn.QueryRow(t, "SELECT statistics -> 'statistics' ->> 'cnt' FROM "+
				"CRDB_INTERNAL.STATEMENT_STATISTICS WHERE app_name = 'app1' "+
				"AND metadata ->> 'query'=$1", tc.fingerprint)
			rows.Scan(&count)

			require.Equal(t, tc.count, count)
		}
	})

	t.Run("test hide placeholders", func(t *testing.T) {
		testConn.Exec(t, "SET application_name = 'app2'")
		argsOne := []interface{}{1}
		argsTwo := []interface{}{1, 2}
		testCases := []struct {
			stmt        string
			fingerprint string
			count       int64
			literals    []interface{}
		}{
			{
				stmt:        "SELECT * FROM t WHERE v IN ($1)",
				fingerprint: "SELECT * FROM t WHERE v IN ($1,)",
				count:       1,
				literals:    argsOne,
			},
			{
				stmt:        "SELECT * FROM t WHERE v IN (1)",
				fingerprint: "SELECT * FROM t WHERE v IN (_,)",
				count:       1,
			},
			{
				stmt:        "SELECT * FROM t WHERE v IN ($1, $2)",
				fingerprint: "SELECT * FROM t WHERE v IN ($1, $1)",
				count:       1,
				literals:    argsTwo,
			},
			{
				stmt:        "SELECT * FROM t WHERE v IN ($2, $1)",
				fingerprint: "SELECT * FROM t WHERE v IN ($1, $1)",
				count:       2,
				literals:    argsTwo,
			},
			{
				stmt:        "SELECT * FROM t WHERE v IN (1,2)",
				fingerprint: "SELECT * FROM t WHERE v IN (_, _)",
				count:       1,
			},
		}

		for _, tc := range testCases {
			testConn.Exec(t, tc.stmt, tc.literals...)
			rows := testConn.QueryRow(t, "SELECT statistics -> 'statistics' ->> 'cnt' FROM "+
				"CRDB_INTERNAL.STATEMENT_STATISTICS WHERE app_name = 'app2' "+
				"AND metadata ->> 'query'=$1", tc.fingerprint)
			rows.Scan(&count)

			require.Equal(t, tc.count, count)
		}
	})
}

func TestSQLStatsIdleLatencies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStress(t, "These tests make timing assertions, which may fail under stress.")

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	testCases := []struct {
		name     string
		stmtLats map[string]float64
		txnLat   float64
		ops      func(*testing.T, *gosql.DB)
	}{
		{
			name:     "no latency",
			stmtLats: map[string]float64{"SELECT _": 0},
			txnLat:   0,
			ops: func(t *testing.T, db *gosql.DB) {
				tx, err := db.Begin()
				require.NoError(t, err)
				_, err = tx.Exec("SELECT 1")
				require.NoError(t, err)
				err = tx.Commit()
				require.NoError(t, err)
			},
		},
		{
			name:     "no latency (implicit txn)",
			stmtLats: map[string]float64{"SELECT _": 0},
			txnLat:   0,
			ops: func(t *testing.T, db *gosql.DB) {
				// These 100ms don't count because we're not in an explicit transaction.
				time.Sleep(100 * time.Millisecond)
				_, err := db.Exec("SELECT 1")
				require.NoError(t, err)
			},
		},
		{
			name:     "no latency - prepared statement (implicit txn)",
			stmtLats: map[string]float64{"SELECT $1::INT8": 0},
			txnLat:   0,
			ops: func(t *testing.T, db *gosql.DB) {
				stmt, err := db.Prepare("SELECT $1::INT")
				require.NoError(t, err)
				time.Sleep(100 * time.Millisecond)
				_, err = stmt.Exec(1)
				require.NoError(t, err)
			},
		},
		{
			name:     "simple statement",
			stmtLats: map[string]float64{"SELECT _": 0.1},
			txnLat:   0.2,
			ops: func(t *testing.T, db *gosql.DB) {
				tx, err := db.Begin()
				require.NoError(t, err)
				time.Sleep(100 * time.Millisecond)
				_, err = tx.Exec("SELECT 1")
				require.NoError(t, err)
				time.Sleep(100 * time.Millisecond)
				err = tx.Commit()
				require.NoError(t, err)
			},
		},
		{
			name:     "compound statement",
			stmtLats: map[string]float64{"SELECT _": 0.1, "SELECT count(*) FROM crdb_internal.statement_statistics": 0},
			txnLat:   0.2,
			ops: func(t *testing.T, db *gosql.DB) {
				tx, err := db.Begin()
				require.NoError(t, err)
				time.Sleep(100 * time.Millisecond)
				_, err = tx.Exec("SELECT 1; SELECT count(*) FROM crdb_internal.statement_statistics")
				require.NoError(t, err)
				time.Sleep(100 * time.Millisecond)
				err = tx.Commit()
				require.NoError(t, err)
			},
		},
		{
			name:     "multiple statements - slow generation",
			stmtLats: map[string]float64{"SELECT pg_sleep(_)": 0, "SELECT _": 0},
			txnLat:   0,
			ops: func(t *testing.T, db *gosql.DB) {
				tx, err := db.Begin()
				require.NoError(t, err)
				_, err = tx.Exec("SELECT pg_sleep(1)")
				require.NoError(t, err)
				_, err = tx.Exec("SELECT 1")
				require.NoError(t, err)
				err = tx.Commit()
				require.NoError(t, err)
			},
		},
		{
			name:     "prepared statement",
			stmtLats: map[string]float64{"SELECT $1::INT8": 0.1},
			txnLat:   0.2,
			ops: func(t *testing.T, db *gosql.DB) {
				stmt, err := db.Prepare("SELECT $1::INT")
				require.NoError(t, err)
				tx, err := db.Begin()
				require.NoError(t, err)
				time.Sleep(100 * time.Millisecond)
				_, err = tx.Stmt(stmt).Exec(1)
				require.NoError(t, err)
				time.Sleep(100 * time.Millisecond)
				err = tx.Commit()
				require.NoError(t, err)
			},
		},
		{
			name:     "prepared statement inside transaction",
			stmtLats: map[string]float64{"SELECT $1::INT8": 0.1},
			txnLat:   0.2,
			ops: func(t *testing.T, db *gosql.DB) {
				tx, err := db.Begin()
				require.NoError(t, err)
				stmt, err := tx.Prepare("SELECT $1::INT")
				require.NoError(t, err)
				time.Sleep(100 * time.Millisecond)
				_, err = stmt.Exec(1)
				require.NoError(t, err)
				time.Sleep(100 * time.Millisecond)
				err = tx.Commit()
				require.NoError(t, err)
			},
		},
		{
			name:     "multiple transactions",
			stmtLats: map[string]float64{"SELECT _": 0.1},
			txnLat:   0.2,
			ops: func(t *testing.T, db *gosql.DB) {
				for i := 0; i < 3; i++ {
					tx, err := db.Begin()
					require.NoError(t, err)
					time.Sleep(100 * time.Millisecond)
					_, err = tx.Exec("SELECT 1")
					require.NoError(t, err)
					time.Sleep(100 * time.Millisecond)
					err = tx.Commit()
					require.NoError(t, err)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Make a separate connection to the database, to isolate the stats
			// we'll observe.
			// Note that we're not using pgx here because it *always* prepares
			// statements, and we want to test our client latency measurements
			// both with and without prepared statements.
			dbUrl, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(username.RootUser))
			defer cleanup()
			connector, err := pq.NewConnector(dbUrl.String())
			require.NoError(t, err)
			opsDB := gosql.OpenDB(connector)
			defer func() {
				_ = opsDB.Close()
			}()

			// Set a unique application name for our session, so we can find our stats easily.
			appName := t.Name()
			_, err = opsDB.Exec("SET application_name = $1", appName)
			require.NoError(t, err)

			// Run the test operations.
			tc.ops(t, opsDB)

			// Make looser timing assertions in CI, since we've seen
			// more variability there.
			// - Bazel test runs also use this looser delta.
			// - Goland and `go test` use the tighter delta unless
			//   the crdb_test build tag has been set.
			delta := 0.003
			if buildutil.CrdbTestBuild {
				delta = 0.05
			}

			// Look for the latencies we expect.
			t.Run("stmt", func(t *testing.T) {
				actual := make(map[string]float64)
				rows, err := db.Query(`
					SELECT metadata->>'query', statistics->'statistics'->'idleLat'->'mean'
					  FROM crdb_internal.statement_statistics
					 WHERE app_name = $1`, appName)
				require.NoError(t, err)
				for rows.Next() {
					var query string
					var latency float64
					err = rows.Scan(&query, &latency)
					require.NoError(t, err)
					actual[query] = latency
				}
				require.NoError(t, rows.Err())
				require.InDeltaMapValues(t, tc.stmtLats, actual, delta,
					"expected: %v\nactual: %v", tc.stmtLats, actual)
			})

			t.Run("txn", func(t *testing.T) {
				var actual float64
				row := db.QueryRow(`
					SELECT statistics->'statistics'->'idleLat'->'mean'
					  FROM crdb_internal.transaction_statistics
					 WHERE app_name = $1`, appName)
				err := row.Scan(&actual)
				require.NoError(t, err)
				require.GreaterOrEqual(t, actual, float64(0))
				require.InDelta(t, tc.txnLat, actual, delta)
			})
		})
	}
}

type indexInfo struct {
	name  string
	table string
}

func TestSQLStatsIndexesUsed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	testServer, sqlConn, _ := serverutils.StartServer(t, params)
	defer func() {
		require.NoError(t, sqlConn.Close())
		testServer.Stopper().Stop(ctx)
	}()
	testConn := sqlutils.MakeSQLRunner(sqlConn)
	appName := "indexes-usage"
	testConn.Exec(t, "SET application_name = $1", appName)

	testCases := []struct {
		name          string
		tableCreation string
		statement     string
		fingerprint   string
		indexes       []indexInfo
	}{
		{
			name:          "buildScan",
			tableCreation: "CREATE TABLE t1 (k INT)",
			statement:     "SELECT * FROM t1",
			fingerprint:   "SELECT * FROM t1",
			indexes:       []indexInfo{{name: "t1_pkey", table: "t1"}},
		},
		{
			name:          "buildIndexJoin",
			tableCreation: "CREATE TABLE t2 (x INT, y INT, INDEX x_idx (x))",
			statement:     "SELECT * FROM t2@x_idx",
			fingerprint:   "SELECT * FROM t2@x_idx",
			indexes: []indexInfo{
				{name: "t2_pkey", table: "t2"},
				{name: "x_idx", table: "t2"}},
		},
		{
			name:          "buildLookupJoin",
			tableCreation: "CREATE TABLE t3 (x INT, y INT, INDEX x_idx (x)); CREATE TABLE t4 (u INT, v INT)",
			statement:     "SELECT * FROM t4 INNER LOOKUP JOIN t3 ON u = x",
			fingerprint:   "SELECT * FROM t4 INNER LOOKUP JOIN t3 ON u = x",
			indexes: []indexInfo{
				{name: "t3_pkey", table: "t3"},
				{name: "t4_pkey", table: "t4"},
				{name: "x_idx", table: "t3"}},
		},
		{
			name:          "buildZigZag",
			tableCreation: "CREATE TABLE t5 (a INT, b INT, INDEX a_idx(a), INDEX b_idx(b))",
			statement:     "SELECT * FROM t5@{FORCE_ZIGZAG} WHERE a = 1 AND b = 1",
			fingerprint:   "SELECT * FROM t5@{FORCE_ZIGZAG} WHERE (a = _) AND (b = _)",
			indexes: []indexInfo{
				{name: "a_idx", table: "t5"},
				{name: "b_idx", table: "t5"}},
		},
		{
			name:          "buildInvertedJoin",
			tableCreation: "CREATE TABLE t6 (k INT, j JSON, INVERTED INDEX j_idx (j))",
			statement:     "SELECT * FROM t6 INNER INVERTED JOIN t6 AS t6_2 ON t6.j @> t6_2.j",
			fingerprint:   "SELECT * FROM t6 INNER INVERTED JOIN t6 AS t6_2 ON t6.j @> t6_2.j",
			indexes: []indexInfo{
				{name: "j_idx", table: "t6"},
				{name: "t6_pkey", table: "t6"}},
		},
	}

	var indexesString string
	var indexes []string
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testConn.Exec(t, tc.tableCreation)
			testConn.Exec(t, tc.statement)

			rows := testConn.QueryRow(t, "SELECT statistics -> 'statistics' ->> 'indexes' "+
				"FROM CRDB_INTERNAL.STATEMENT_STATISTICS WHERE app_name = $1 "+
				"AND metadata ->> 'query'=$2", appName, tc.fingerprint)
			rows.Scan(&indexesString)

			err := json.Unmarshal([]byte(indexesString), &indexes)
			require.NoError(t, err)
			require.Equal(t, len(tc.indexes), len(indexes))
			require.Equal(t, tc.indexes, convertIDsToNames(t, testConn, indexes))
		})
	}
}

func convertIDsToNames(t *testing.T, testConn *sqlutils.SQLRunner, indexes []string) []indexInfo {
	var indexesInfo []indexInfo
	var tableName string
	var indexName string
	for _, idx := range indexes {
		tableID := strings.Split(idx, "@")[0]
		idxID := strings.Split(idx, "@")[1]

		rows := testConn.QueryRow(t, "SELECT descriptor_name, index_name FROM "+
			"crdb_internal.table_indexes WHERE descriptor_id =$1 AND index_id=$2", tableID, idxID)
		rows.Scan(&tableName, &indexName)
		indexesInfo = append(indexesInfo, indexInfo{name: indexName, table: tableName})
	}

	sort.Slice(indexesInfo, func(i, j int) bool {
		return indexesInfo[i].name < indexesInfo[j].name
	})
	return indexesInfo
}

func TestSQLStatsLatencyInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	testServer, sqlConn, _ := serverutils.StartServer(t, params)
	defer func() {
		require.NoError(t, sqlConn.Close())
		testServer.Stopper().Stop(ctx)
	}()
	testConn := sqlutils.MakeSQLRunner(sqlConn)
	appName := "latency-info"
	testConn.Exec(t, "SET application_name = $1", appName)
	testConn.Exec(t, "CREATE TABLE t1 (k INT)")

	testCases := []struct {
		name        string
		statement   string
		fingerprint string
		latencyMax  float64
	}{
		{
			name:        "select on table",
			statement:   "SELECT * FROM t1",
			fingerprint: "SELECT * FROM t1",
			latencyMax:  1,
		},
		{
			name:        "select sleep",
			statement:   "SELECT pg_sleep(0.06)",
			fingerprint: "SELECT pg_sleep(_)",
			latencyMax:  0.2,
		},
		{
			name:        "select sleep",
			statement:   "SELECT pg_sleep(0.1)",
			fingerprint: "SELECT pg_sleep(_)",
			latencyMax:  0.2,
		},
		{
			name:        "select sleep",
			statement:   "SELECT pg_sleep(0.07)",
			fingerprint: "SELECT pg_sleep(_)",
			latencyMax:  0.2,
		},
	}

	var min float64
	var max float64
	var p50 float64
	var p90 float64
	var p99 float64
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testConn.Exec(t, tc.statement)

			rows := testConn.QueryRow(t, "SELECT statistics -> 'statistics' -> 'latencyInfo' ->> 'min',"+
				"statistics -> 'statistics' -> 'latencyInfo' ->> 'max',"+
				"statistics -> 'statistics' -> 'latencyInfo' ->> 'p50',"+
				"statistics -> 'statistics' -> 'latencyInfo' ->> 'p90',"+
				"statistics -> 'statistics' -> 'latencyInfo' ->> 'p99' "+
				"FROM CRDB_INTERNAL.STATEMENT_STATISTICS WHERE app_name = $1 "+
				"AND metadata ->> 'query'=$2", appName, "SELECT * FROM t1")
			rows.Scan(&min, &max, &p50, &p90, &p99)

			require.Positive(t, min)
			require.Positive(t, max)
			require.GreaterOrEqual(t, max, min)
			require.LessOrEqual(t, max, tc.latencyMax)
			require.GreaterOrEqual(t, p99, p90)
			require.GreaterOrEqual(t, p90, p50)
			require.LessOrEqual(t, p99, max)
		})
	}
}

func TestSQLStatsRegions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name     string
		locality roachpb.Locality
		expected string
	}{{
		name:     "locality not set",
		locality: roachpb.Locality{},
		expected: `[]`,
	}, {
		name:     "locality set",
		locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east1"}}},
		expected: `["us-east1"]`,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			params, _ := tests.CreateTestServerParams()
			params.Locality = tc.locality
			s, conn, _ := serverutils.StartServer(t, params)
			defer s.Stopper().Stop(ctx)

			db := sqlutils.MakeSQLRunner(conn)
			db.Exec(t, "SET application_name = $1", t.Name())
			db.Exec(t, "SELECT 1")

			row := db.QueryRow(t, `
				SELECT statistics->'statistics'->>'regions'
				  FROM crdb_internal.statement_statistics
				 WHERE app_name = $1`, t.Name())
			var actual string
			row.Scan(&actual)
			require.Equal(t, tc.expected, actual)
		})
	}
}
