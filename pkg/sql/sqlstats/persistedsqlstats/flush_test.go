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
	"math"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	query       string
	stmtNoConst string
	count       int64
}

var testQueries = []testCase{
	{
		query:       "SELECT 1",
		stmtNoConst: "SELECT _",
		count:       3,
	},
	{
		query:       "SELECT 1, 2, 3",
		stmtNoConst: "SELECT _, _, _",
		count:       10,
	},
	{
		query:       "SELECT 1, 1 WHERE 1 < 10",
		stmtNoConst: "SELECT _, _ WHERE _ < _",
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

	testCluster := serverutils.StartCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestDoesNotWorkWithSharedProcessModeButWeDontKnowWhyYet(
				base.TestTenantProbabilistic, 113855, /* issueNumber */
			),
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: &sqlstats.TestingKnobs{
					StubTimeNow: fakeTime.Now,
				},
			},
		},
	})

	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	firstServer := testCluster.Server(0 /* idx */).ApplicationLayer()
	secondServer := testCluster.Server(1 /* idx */).ApplicationLayer()

	pgFirstSQLConn := firstServer.SQLConn(t)
	firstSQLConn := sqlutils.MakeSQLRunner(pgFirstSQLConn)

	pgSecondSQLConn := secondServer.SQLConn(t)
	secondSQLConn := sqlutils.MakeSQLRunner(pgSecondSQLConn)

	firstServerSQLStats := firstServer.SQLServer().(*sql.Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)
	secondServerSQLStats := secondServer.SQLServer().(*sql.Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)

	firstSQLConn.Exec(t, "SET application_name = 'flush_unit_test'")
	secondSQLConn.Exec(t, "SET application_name = 'flush_unit_test'")

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

		sqlInstanceId := base.SQLInstanceID(0)
		if sqlstats.GatewayNodeEnabled.Get(&testCluster.Server(0).ClusterSettings().SV) {
			sqlInstanceId = firstServer.SQLInstanceID()
		}
		// For each test case, we verify that it's being properly inserted exactly
		// once and it is exactly executed tc.count number of times.
		for _, tc := range testQueries {
			verifyNumOfInsertedEntries(t, secondSQLConn, tc.stmtNoConst, sqlInstanceId, 1 /* expectedStmtEntryCnt */, 1 /* expectedTxnEntryCtn */)
			verifyInsertedFingerprintExecCount(t, secondSQLConn, tc.stmtNoConst, fakeTime.getAggTimeTs(), sqlInstanceId, tc.count)
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

		sqlInstanceId := base.SQLInstanceID(0)
		if sqlstats.GatewayNodeEnabled.Get(&testCluster.Server(0).ClusterSettings().SV) {
			sqlInstanceId = firstServer.SQLInstanceID()
		}

		for _, tc := range testQueries {
			verifyNumOfInsertedEntries(t, secondSQLConn, tc.stmtNoConst, sqlInstanceId, 1 /* expectedStmtEntryCnt */, 1 /* expectedTxnEntryCtn */)
			// The execution count is doubled here because we execute all of the
			// statements here in the same aggregation interval.
			verifyInsertedFingerprintExecCount(t, secondSQLConn, tc.stmtNoConst, fakeTime.getAggTimeTs(), sqlInstanceId, tc.count+tc.count-1 /* expectedCount */)
		}
	}

	// We change the time to be in a different aggregation window.
	{
		fakeTime.setTime(fakeTime.Now().Add(time.Hour * 3))

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

		sqlInstanceId := base.SQLInstanceID(0)
		if sqlstats.GatewayNodeEnabled.Get(&testCluster.Server(0).ClusterSettings().SV) {
			sqlInstanceId = firstServer.SQLInstanceID()
		}

		for _, tc := range testQueries {
			// We expect exactly 2 entries since we are in a different aggregation window.
			verifyNumOfInsertedEntries(t, secondSQLConn, tc.stmtNoConst, sqlInstanceId, 2 /* expectedStmtEntryCnt */, 2 /* expectedTxnEntryCtn */)
			verifyInsertedFingerprintExecCount(t, secondSQLConn, tc.stmtNoConst, fakeTime.getAggTimeTs(), sqlInstanceId, tc.count)
		}
	}

	// We run queries in a different server and trigger the flush.
	{
		for _, tc := range testQueries {
			for i := int64(0); i < tc.count; i++ {
				secondSQLConn.Exec(t, tc.query)
			}
		}
		verifyInMemoryStatsEmpty(t, testQueries, firstServerSQLStats)
		verifyInMemoryStatsCorrectness(t, testQueries, secondServerSQLStats)

		firstServerSQLStats.Flush(ctx)
		secondServerSQLStats.Flush(ctx)

		verifyInMemoryStatsEmpty(t, testQueries, firstServerSQLStats)
		verifyInMemoryStatsEmpty(t, testQueries, secondServerSQLStats)

		if sqlstats.GatewayNodeEnabled.Get(&testCluster.Server(0).ClusterSettings().SV) {
			// Ensure that we encode the correct node_id for the new entry and did not
			// accidentally tamper the entries written by another server.
			for _, tc := range testQueries {
				verifyNumOfInsertedEntries(t, firstSQLConn, tc.stmtNoConst, secondServer.SQLInstanceID(), 1 /* expectedStmtEntryCnt */, 1 /* expectedTxnEntryCtn */)
				verifyInsertedFingerprintExecCount(t, firstSQLConn, tc.stmtNoConst, fakeTime.getAggTimeTs(), secondServer.SQLInstanceID(), tc.count)
				verifyNumOfInsertedEntries(t, secondSQLConn, tc.stmtNoConst, firstServer.SQLInstanceID(), 2 /* expectedStmtEntryCnt */, 2 /* expectedTxnEntryCtn */)
				verifyInsertedFingerprintExecCount(t, secondSQLConn, tc.stmtNoConst, fakeTime.getAggTimeTs(), firstServer.SQLInstanceID(), tc.count)
			}
		} else {
			sqlInstanceId := base.SQLInstanceID(0)
			// Ensure that we encode the correct node_id for the new entry and did not
			// accidentally tamper the entries written by another server.
			for _, tc := range testQueries {
				verifyNumOfInsertedEntries(t, firstSQLConn, tc.stmtNoConst, sqlInstanceId, 2 /* expectedStmtEntryCnt */, 2 /* expectedTxnEntryCtn */)
				verifyInsertedFingerprintExecCount(t, firstSQLConn, tc.stmtNoConst, fakeTime.getAggTimeTs(), sqlInstanceId, tc.count*2 /*num of servers*/)
			}
		}
	}
}

func TestSQLStatsInitialDelay(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	initialNextFlushAt := s.SQLServer().(*sql.Server).
		GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats).GetNextFlushAt()

	// Since we introduced jitter in our flush interval, the next flush time
	// is not entirely deterministic. However, we can still have an upperbound
	// on this value.
	maxNextRunAt :=
		timeutil.Now().Add(persistedsqlstats.SQLStatsFlushInterval.Default() * 2)

	require.True(t, maxNextRunAt.After(initialNextFlushAt),
		"expected latest nextFlushAt to be %s, but found %s", maxNextRunAt, initialNextFlushAt)
}

func TestSQLStatsLogDiscardMessage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	fakeTime := stubTime{
		aggInterval: time.Hour,
	}
	fakeTime.setTime(timeutil.Now())

	var params base.TestServerArgs
	params.Knobs.SQLStatsKnobs = &sqlstats.TestingKnobs{
		StubTimeNow: fakeTime.Now,
	}
	srv, conn, _ := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(ctx)

	sqlConn := sqlutils.MakeSQLRunner(conn)

	sqlConn.Exec(t, "SET CLUSTER SETTING sql.stats.flush.minimum_interval = '10m'")
	sqlConn.Exec(t, fmt.Sprintf("SET CLUSTER SETTING sql.metrics.max_mem_stmt_fingerprints=%d", 8))

	for i := 0; i < 20; i++ {
		appName := fmt.Sprintf("logDiscardTestApp%d", i)
		sqlConn.Exec(t, "SET application_name = $1", appName)
		sqlConn.Exec(t, "SELECT 1")
	}

	log.FlushFiles()

	entries, err := log.FetchEntriesFromFiles(
		0,
		math.MaxInt64,
		10000,
		regexp.MustCompile(`statistics discarded due to memory limit. transaction discard count:`),
		log.WithFlattenedSensitiveData,
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(entries), "there should only be 1 log for the initial execution because the test should take less than 1 minute to execute the 20 commands. cnt: %v", entries)

	// lower the time frame to verify log still occurs after the initial one
	sqlConn.Exec(t, "SET CLUSTER SETTING sql.metrics.discarded_stats_log.interval='0.00001ms'")

	for i := 0; i < 20; i++ {
		appName := fmt.Sprintf("logDiscardTestApp2%d", i)
		sqlConn.Exec(t, "SET application_name = $1", appName)
		sqlConn.Exec(t, "SELECT 1")
	}

	log.FlushFiles()

	entries, err = log.FetchEntriesFromFiles(
		0,
		math.MaxInt64,
		10000,
		regexp.MustCompile(`statistics discarded due to memory limit. transaction discard count:`),
		log.WithFlattenedSensitiveData,
	)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(entries), 1, "there should only be 1 log for the initial execution because the test should take less than 1 minute to execute the 20 commands. cnt: %v", entries)
}

func TestSQLStatsMinimumFlushInterval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	fakeTime := stubTime{
		aggInterval: time.Hour,
	}
	fakeTime.setTime(timeutil.Now())

	var params base.TestServerArgs
	params.Knobs.SQLStatsKnobs = &sqlstats.TestingKnobs{
		StubTimeNow: fakeTime.Now,
	}
	srv, conn, _ := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	sqlConn := sqlutils.MakeSQLRunner(conn)

	sqlConn.Exec(t, "SET CLUSTER SETTING sql.stats.flush.minimum_interval = '10m'")
	sqlConn.Exec(t, "SET application_name = 'min_flush_test'")
	sqlConn.Exec(t, "SELECT 1")

	s.SQLServer().(*sql.Server).
		GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats).Flush(ctx)

	sqlConn.CheckQueryResults(t, `
		SELECT count(*)
		FROM system.statement_statistics
		WHERE app_name = 'min_flush_test'
		`, [][]string{{"1"}})

	sqlConn.CheckQueryResults(t, `
		SELECT count(*)
		FROM system.transaction_statistics
		WHERE app_name = 'min_flush_test'
		`, [][]string{{"1"}})

	// Since by default, the minimum flush interval is 10 minutes, a subsequent
	// flush should be no-op.
	s.SQLServer().(*sql.Server).
		GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats).Flush(ctx)

	sqlConn.CheckQueryResults(t, `
		SELECT count(*)
		FROM system.statement_statistics
		WHERE app_name = 'min_flush_test'
		`, [][]string{{"1"}})

	sqlConn.CheckQueryResults(t, `
		SELECT count(*)
		FROM system.transaction_statistics
		WHERE app_name = 'min_flush_test'
		`, [][]string{{"1"}})

	// We manually set the time to past the minimum flush interval, now the flush
	// should succeed.
	fakeTime.setTime(fakeTime.Now().Add(time.Hour))

	s.SQLServer().(*sql.Server).
		GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats).Flush(ctx)

	sqlConn.CheckQueryResults(t, `
		SELECT count(*) > 1
		FROM system.statement_statistics
		WHERE app_name = 'min_flush_test'
		`, [][]string{{"true"}})

}

func TestInMemoryStatsDiscard(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	observer := s.SQLConn(t)

	sqlConn := sqlutils.MakeSQLRunner(conn)
	sqlConn.Exec(t,
		"SET CLUSTER SETTING sql.stats.flush.minimum_interval = '10m'")
	observerConn := sqlutils.MakeSQLRunner(observer)

	t.Run("flush_disabled", func(t *testing.T) {
		sqlConn.Exec(t,
			"SET CLUSTER SETTING sql.stats.flush.force_cleanup.enabled = true")
		sqlConn.Exec(t,
			"SET CLUSTER SETTING sql.stats.flush.enabled = false")

		sqlConn.Exec(t, "SET application_name = 'flush_disabled_test'")
		sqlConn.Exec(t, "SELECT 1")

		observerConn.CheckQueryResults(t, `
		SELECT count(*)
		FROM crdb_internal.statement_statistics
		WHERE app_name = 'flush_disabled_test'
		`, [][]string{{"1"}})

		s.SQLServer().(*sql.Server).
			GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats).Flush(ctx)

		observerConn.CheckQueryResults(t, `
		SELECT count(*)
		FROM crdb_internal.statement_statistics
		WHERE app_name = 'flush_disabled_test'
		`, [][]string{{"0"}})
	})

	t.Run("flush_enabled", func(t *testing.T) {
		// Now turn back SQL Stats flush. If the flush is aborted due to violating
		// minimum flush interval constraint, we should not be clearing in-memory
		// stats.
		sqlConn.Exec(t,
			"SET CLUSTER SETTING sql.stats.flush.enabled = true")
		sqlConn.Exec(t, "SET application_name = 'flush_enabled_test'")
		sqlConn.Exec(t, "SELECT 1")

		observerConn.CheckQueryResults(t, `
		SELECT count(*)
		FROM crdb_internal.statement_statistics
		WHERE app_name = 'flush_enabled_test'
		`, [][]string{{"1"}})

		observerConn.CheckQueryResults(t, `
		SELECT count(*)
		FROM crdb_internal.transaction_statistics
		WHERE app_name = 'flush_enabled_test'
		`, [][]string{{"1"}})

		// First flush should flush everything into the system tables.
		s.SQLServer().(*sql.Server).
			GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats).Flush(ctx)

		observerConn.CheckQueryResults(t, `
		SELECT count(*)
		FROM system.statement_statistics
		WHERE app_name = 'flush_enabled_test'
		`, [][]string{{"1"}})

		observerConn.CheckQueryResults(t, `
		SELECT count(*)
		FROM system.transaction_statistics
		WHERE app_name = 'flush_enabled_test'
		`, [][]string{{"1"}})

		sqlConn.Exec(t, "SELECT 1,1")

		// Second flush should be aborted due to violating the minimum flush
		// interval requirement. Though the data should still remain in-memory.
		s.SQLServer().(*sql.Server).
			GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats).Flush(ctx)

		observerConn.CheckQueryResults(t, `
		SELECT count(*)
		FROM system.statement_statistics
		WHERE app_name = 'flush_enabled_test'
		`, [][]string{{"1"}})

		observerConn.CheckQueryResults(t, `
		SELECT count(*)
		FROM system.transaction_statistics
		WHERE app_name = 'flush_enabled_test'
		`, [][]string{{"1"}})

		observerConn.CheckQueryResults(t, `
		SELECT count(*)
		FROM crdb_internal.statement_statistics
		WHERE app_name = 'flush_enabled_test'
		`, [][]string{{"2"}})

		observerConn.CheckQueryResults(t, `
		SELECT count(*)
		FROM crdb_internal.transaction_statistics
		WHERE app_name = 'flush_enabled_test'
		`, [][]string{{"2"}})
	})
}

func TestSQLStatsGatewayNodeSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	sqlConn := sqlutils.MakeSQLRunner(conn)

	// Gateway Node ID enabled, so should persist the value.
	sqlConn.Exec(t, "SET CLUSTER SETTING sql.metrics.statement_details.gateway_node.enabled = true")
	sqlConn.Exec(t, "SET application_name = 'gateway_enabled'")
	sqlConn.Exec(t, "SELECT 1")
	s.SQLServer().(*sql.Server).
		GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats).Flush(ctx)

	verifyNodeID(t, sqlConn, "SELECT _", true, "gateway_enabled")

	// Gateway Node ID disabled, so shouldn't persist the value on the node_id column, but it should
	// still store the value on the statistics column.
	sqlConn.Exec(t, "SET CLUSTER SETTING sql.metrics.statement_details.gateway_node.enabled = false")
	sqlConn.Exec(t, "SET application_name = 'gateway_disabled'")
	sqlConn.Exec(t, "SELECT 1")
	s.SQLServer().(*sql.Server).
		GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats).Flush(ctx)

	verifyNodeID(t, sqlConn, "SELECT _", false, "gateway_disabled")
}

func countStats(t *testing.T, sqlConn *sqlutils.SQLRunner) (numStmtStats int64, numTxnStats int64) {
	sqlConn.QueryRow(t, `
		SELECT count(*)
		FROM system.statement_statistics`).Scan(&numStmtStats)

	sqlConn.QueryRow(t, `
		SELECT count(*)
		FROM system.transaction_statistics`).Scan(&numTxnStats)

	return numStmtStats, numTxnStats
}

func TestSQLStatsPersistedLimitReached(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var params base.TestServerArgs
	params.Knobs.SQLStatsKnobs = sqlstats.CreateTestingKnobs()
	srv, conn, _ := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	sqlConn := sqlutils.MakeSQLRunner(conn)
	pss := s.SQLServer().(*sql.Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)

	// 1. Flush then count to get the initial number of rows.
	pss.Flush(ctx)
	stmtStatsCount, txnStatsCount := countStats(t, sqlConn)

	// The size check is done at the shard level. Execute enough so all the shards
	// will have a minimum amount of rows.
	additionalStatements := int64(0)
	const minCountByShard = int64(8)
	for smallestStatsCountAcrossAllShards(t, sqlConn) <= minCountByShard {
		appName := fmt.Sprintf("TestSQLStatsPersistedLimitReached%d", additionalStatements)
		sqlConn.Exec(t, `SET application_name = $1`, appName)
		sqlConn.Exec(t, "SELECT 1")
		additionalStatements += 2
		pss.Flush(ctx)
	}

	stmtStatsCountFlush2, txnStatsCountFlush2 := countStats(t, sqlConn)

	// 2. After flushing and counting a second time, we should see at least
	// additionalStatements count or more rows.
	require.GreaterOrEqual(t, stmtStatsCountFlush2-stmtStatsCount, additionalStatements)
	require.GreaterOrEqual(t, txnStatsCountFlush2-txnStatsCount, additionalStatements)

	// 3. Set sql.stats.persisted_rows.max according to the smallest table.
	smallest := smallestStatsCountAcrossAllShards(t, sqlConn)
	require.GreaterOrEqual(t, smallest, minCountByShard, "min count by shard is less than expected: %d", smallest)

	// Calculate the max value to stop the flush. The -1 is used to ensure it's
	// always less than to avoid it possibly being equal.
	maxRows := int(math.Floor(float64(smallest)/1.5)) - 1
	// increase it by the number of shard to get a total table count.
	maxRows *= systemschema.SQLStatsHashShardBucketCount
	sqlConn.Exec(t, fmt.Sprintf("SET CLUSTER SETTING sql.stats.persisted_rows.max=%d", maxRows))

	// 4. Wait for the cluster setting to be applied.
	testutils.SucceedsSoon(t, func() error {
		var appliedSetting int
		row := sqlConn.QueryRow(t, "SHOW CLUSTER SETTING sql.stats.persisted_rows.max")
		row.Scan(&appliedSetting)
		if appliedSetting != maxRows {
			return errors.Newf("waiting for sql.stats.persisted_rows.max to be applied")
		}
		return nil
	})

	// Set table size check interval to a very small value to invalidate the cache.
	sqlConn.Exec(t, "SET CLUSTER SETTING sql.stats.limit_table_size_check.interval='00:00:00'")
	testutils.SucceedsSoon(t, func() error {
		var appliedSetting string
		row := sqlConn.QueryRow(t, "SHOW CLUSTER SETTING sql.stats.limit_table_size_check.interval")
		row.Scan(&appliedSetting)
		if appliedSetting != "00:00:00" {
			return errors.Newf("waiting for sql.stats.limit_table_size_check.interval to be applied: %s", appliedSetting)
		}
		return nil
	})

	// Add new in memory statements to verify that when the check is enabled the
	// stats are not flushed.
	for i := int64(0); i < 20; i++ {
		appName := fmt.Sprintf("TestSQLStatsPersistedLimitReached2nd%d", i)
		sqlConn.Exec(t, `SET application_name = $1`, appName)
		sqlConn.Exec(t, "SELECT 1")
	}

	for _, enforceLimitEnabled := range []bool{true, false} {
		boolStr := strconv.FormatBool(enforceLimitEnabled)
		t.Run("enforce-limit-"+boolStr, func(t *testing.T) {
			sqlConn.Exec(t, "SET CLUSTER SETTING sql.stats.limit_table_size.enabled = "+boolStr)

			pss.Flush(ctx)

			stmtStatsCountFlush3, txnStatsCountFlush3 := countStats(t, sqlConn)

			if enforceLimitEnabled {
				// Assert that neither table has grown in length.
				require.Equal(t, stmtStatsCountFlush3, stmtStatsCountFlush2, "stmt table should not change. original: %d, current: %d", stmtStatsCountFlush2, stmtStatsCountFlush3)
				require.Equal(t, txnStatsCountFlush3, txnStatsCountFlush2, "txn table should not change. original: %d, current: %d", txnStatsCountFlush2, txnStatsCountFlush3)
			} else {
				// Assert that tables were allowed to grow.
				require.Greater(t, stmtStatsCountFlush3, stmtStatsCountFlush2, "stmt table should have grown. original: %d, current: %d", stmtStatsCountFlush2, stmtStatsCountFlush3)
				require.Greater(t, txnStatsCountFlush3, txnStatsCountFlush2, "txn table should have grown. original: %d, current: %d", txnStatsCountFlush2, txnStatsCountFlush3)
			}
		})
	}
}

func TestSQLStatsReadLimitSizeOnLockedTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	sqlConn := sqlutils.MakeSQLRunner(conn)
	sqlConn.Exec(t, `INSERT INTO system.users VALUES ('node', NULL, true, 3); GRANT node TO root`)
	waitForFollowerReadTimestamp(t, sqlConn)
	pss := s.SQLServer().(*sql.Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)

	// It should be false since nothing has flushed. The table will be empty.
	limitReached, err := pss.StmtsLimitSizeReached(ctx)
	require.NoError(t, err)
	require.False(t, limitReached)

	const minNumExpectedStmts = int64(systemschema.SQLStatsHashShardBucketCount * 10)
	// Maximum number of persisted rows less than minNumExpectedStmts/1.5.
	const maxNumPersistedRows = 8
	// Divided minNumExpectedStmts by 2 because the set and select are counted.
	for i := int64(0); i < minNumExpectedStmts/2; i++ {
		appName := fmt.Sprintf("TestSQLStatsPersistedLimitReached2nd%d", i)
		sqlConn.Exec(t, `SET application_name = $1`, appName)
		sqlConn.Exec(t, "SELECT 1")
	}

	pss.Flush(ctx)
	stmtStatsCountFlush, _ := countStats(t, sqlConn)

	// Ensure we have some rows in system.statement_statistics
	require.GreaterOrEqual(t, stmtStatsCountFlush, minNumExpectedStmts)

	// Set sql.stats.persisted_rows.max
	sqlConn.Exec(t, fmt.Sprintf("SET CLUSTER SETTING sql.stats.persisted_rows.max=%d", maxNumPersistedRows))
	testutils.SucceedsSoon(t, func() error {
		var appliedSetting int
		row := sqlConn.QueryRow(t, "SHOW CLUSTER SETTING sql.stats.persisted_rows.max")
		row.Scan(&appliedSetting)
		if appliedSetting != maxNumPersistedRows {
			return errors.Newf("waiting for sql.stats.persisted_rows.max to be applied")
		}
		return nil
	})

	// We need SucceedsSoon here for the follower read timestamp to catch up
	// enough for this state to be reached.
	testutils.SucceedsSoon(t, func() error {
		row := sqlConn.QueryRow(t, "SELECT count_rows() FROM system.statement_statistics AS OF SYSTEM TIME follower_read_timestamp()")
		var rowCount int64
		row.Scan(&rowCount)
		if rowCount < minNumExpectedStmts {
			return errors.Newf("waiting for AOST query to return results")
		}
		return nil
	})

	// It should still return false because it only checks once an hour by default
	// unless the previous run was over the limit.
	limitReached, err = pss.StmtsLimitSizeReached(ctx)
	require.NoError(t, err)
	require.False(t, limitReached)

	// Set table size check interval to .0000001 second. So the next check doesn't
	// use the cached value.
	sqlConn.Exec(t, "SET CLUSTER SETTING sql.stats.limit_table_size_check.interval='.0000001s'")
	testutils.SucceedsSoon(t, func() error {
		var appliedSetting string
		row := sqlConn.QueryRow(t, "SHOW CLUSTER SETTING sql.stats.limit_table_size_check.interval")
		row.Scan(&appliedSetting)
		if appliedSetting != "00:00:00" {
			return errors.Newf("waiting for sql.stats.limit_table_size_check.interval to be applied: %s", appliedSetting)
		}
		return nil
	})

	// Begin a transaction.
	sqlConn.Exec(t, "BEGIN")
	// Lock the table. Create a state of contention.
	sqlConn.Exec(t, "SELECT * FROM system.statement_statistics FOR UPDATE")

	// Ensure that we can read from the table despite it being locked, due to the follower read (AOST).
	// Expect that the number of statements in the table exceeds sql.stats.persisted_rows.max * 1.5
	// (meaning that the limit will be reached) and no error. Loop to make sure that
	// checking it multiple times still returns the correct value.
	for i := 0; i < 3; i++ {
		limitReached, err = pss.StmtsLimitSizeReached(ctx)
		require.NoError(t, err)
		require.True(t, limitReached, "limitReached should be true. Loop :%d", i)
	}

	// Close the transaction.
	sqlConn.Exec(t, "COMMIT")
}

func TestSQLStatsPlanSampling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stubTime := &stubTime{}
	setTime := func(inputTime string) {
		parsedTime, err := time.Parse(time.RFC3339, inputTime)
		require.NoError(t, err)
		stubTime.setTime(parsedTime)
	}

	sqlStatsKnobs := sqlstats.CreateTestingKnobs()
	sqlStatsKnobs.StubTimeNow = stubTime.Now
	var params base.TestServerArgs
	params.Knobs.SQLStatsKnobs = sqlStatsKnobs

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	sqlRun := sqlutils.MakeSQLRunner(conn)

	dbName := "defaultdb"

	appName := fmt.Sprintf("TestSQLStatsPlanSampling_%s", uuid.FastMakeV4().String())
	sqlRun.Exec(t, "SET application_name = $1", appName)

	sqlStats := s.SQLServer().(*sql.Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)
	appStats := sqlStats.GetApplicationStats(appName, false)

	sqlRun.Exec(t, `SET CLUSTER SETTING sql.metrics.statement_details.plan_collection.enabled = true;`)
	sqlRun.Exec(t, `SET CLUSTER SETTING sql.txn_stats.sample_rate = 0;`)

	validateSample := func(fingerprint string, implicitTxn bool, expectedPreviouslySampledState bool, expectedSavePlanForStatsState bool) {

		previouslySampled, savePlanForStats := appStats.ShouldSample(
			fingerprint,
			implicitTxn,
			dbName,
		)

		errMessage := fmt.Sprintf("validate: %s, implicit: %t expected sample before: %t, actual sample before: %t, exptected save plan: %t actual save plan: %t\n",
			fingerprint, implicitTxn, expectedPreviouslySampledState, previouslySampled, expectedSavePlanForStatsState, savePlanForStats)
		require.Equal(t, expectedSavePlanForStatsState, savePlanForStats, errMessage)
		require.Equal(t, expectedPreviouslySampledState, previouslySampled, errMessage)
	}

	setTime("2021-09-20T15:00:00Z")

	// Logical plan should be sampled here, since we have not collected logical plan
	// at all.
	validateSample("SELECT _", true, false, true)

	// Execute the query to trigger a collection of logical plan.
	// (db_name=defaultdb implicitTxn=true fingerprint=SELECT _)
	_, err := conn.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err)

	// Ensure that if a query is to be subsequently executed, it will not cause
	// logical plan sampling.
	validateSample("SELECT _", true, true, false)

	// However, if we are to execute the same statement but under explicit
	// transaction, the plan will still need to be sampled.
	validateSample("SELECT _", false, false, true)

	// Execute the statement under explicit transaction.
	// (db_name=defaultdb implicitTxn=false fingerprint=SELECT _)
	tx, err := conn.BeginTx(ctx, &gosql.TxOptions{})
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Ensure that the subsequent execution of the query will not cause logical plan
	// collection.
	validateSample("SELECT _", false, true, false)

	// Set the time to the future and ensure we will resample the logical plan.
	setTime("2021-09-20T15:05:01Z")

	// If tracing is not enabled the statement will not be sampled. To update the
	// save plan for stats last sampled time the statement must have tracing enabled.
	_, err = conn.ExecContext(ctx, "SET CLUSTER SETTING sql.txn_stats.sample_rate = 1;")
	require.NoError(t, err)

	validateSample("SELECT _", true, true, true)

	// implicit txn
	_, err = conn.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err)
	validateSample("SELECT _", true, true, false)

	// explicit txn
	validateSample("SELECT _", false, true, true)

	tx, err = conn.BeginTx(ctx, &gosql.TxOptions{})
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Ensure that the subsequent execution of the query will not cause logical plan
	// collection.
	validateSample("SELECT _", false, true, false)
}

type stubTime struct {
	syncutil.RWMutex
	t           time.Time
	aggInterval time.Duration
	timeStubbed bool
}

func (s *stubTime) setTime(t time.Time) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()

	s.t = t
	s.timeStubbed = true
}

func (s *stubTime) getAggTimeTs() time.Time {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	return s.t.Truncate(s.aggInterval)
}

// Now implements the testing knob interface for persistedsqlstats.Provider.
func (s *stubTime) Now() time.Time {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()

	if s.timeStubbed {
		return s.t
	}

	return timeutil.Now()
}

func verifyInsertedFingerprintExecCount(
	t *testing.T,
	sqlConn *sqlutils.SQLRunner,
	fingerprint string,
	ts time.Time,
	instanceID base.SQLInstanceID,
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
`, fingerprint, ts, instanceID)

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
	instanceID base.SQLInstanceID,
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
`, fingerprint, instanceID)

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
`, stmtFingerprintID), instanceID)

	var numOfInsertedTxnEntry int64
	err := row1.Scan(&numOfInsertedTxnEntry)
	require.NoError(t, err)
	require.Equal(t, expectedTxnEntryCnt, numOfInsertedTxnEntry, "fingerprint: %s", fingerprint)
}

func verifyInMemoryStatsCorrectness(
	t *testing.T, tcs []testCase, statsProvider *persistedsqlstats.PersistedSQLStats,
) {
	for _, tc := range tcs {
		err := statsProvider.SQLStats.IterateStatementStats(context.Background(), sqlstats.IteratorOptions{}, func(ctx context.Context, statistics *appstatspb.CollectedStatementStatistics) error {
			if tc.stmtNoConst == statistics.Key.Query {
				require.Equal(t, tc.count, statistics.Stats.Count, "fingerprint: %s", tc.stmtNoConst)
			}

			// All the queries should be under 1 minute
			require.Less(t, statistics.Stats.ServiceLat.Mean, 60.0)
			require.Less(t, statistics.Stats.RunLat.Mean, 60.0)
			require.Less(t, statistics.Stats.PlanLat.Mean, 60.0)
			require.Less(t, statistics.Stats.ParseLat.Mean, 60.0)
			require.Less(t, statistics.Stats.IdleLat.Mean, 60.0)
			require.GreaterOrEqual(t, statistics.Stats.ServiceLat.Mean, 0.0)
			require.GreaterOrEqual(t, statistics.Stats.RunLat.Mean, 0.0)
			require.GreaterOrEqual(t, statistics.Stats.PlanLat.Mean, 0.0)
			require.GreaterOrEqual(t, statistics.Stats.ParseLat.Mean, 0.0)
			require.GreaterOrEqual(t, statistics.Stats.IdleLat.Mean, 0.0)
			return nil
		})

		require.NoError(t, err)
	}
}

func verifyInMemoryStatsEmpty(
	t *testing.T, tcs []testCase, statsProvider *persistedsqlstats.PersistedSQLStats,
) {
	for _, tc := range tcs {
		err := statsProvider.SQLStats.IterateStatementStats(context.Background(), sqlstats.IteratorOptions{}, func(ctx context.Context, statistics *appstatspb.CollectedStatementStatistics) error {
			if tc.stmtNoConst == statistics.Key.Query {
				require.Equal(t, 0 /* expected */, statistics.Stats.Count, "fingerprint: %s", tc.stmtNoConst)
			}
			return nil
		})

		require.NoError(t, err)
	}
}

func verifyNodeID(
	t *testing.T,
	sqlConn *sqlutils.SQLRunner,
	fingerprint string,
	gatewayEnabled bool,
	appName string,
) {
	row := sqlConn.DB.QueryRowContext(context.Background(),
		`
SELECT
  node_id,
  statistics -> 'statistics' ->> 'nodes' as nodes
FROM
	system.statement_statistics
WHERE
	metadata ->> 'query' = $1 AND
  app_name = $2
`, fingerprint, appName)

	var gatewayNodeID int64
	var allNodesIds string

	e := row.Scan(&gatewayNodeID, &allNodesIds)
	require.NoError(t, e)
	nodeID := int64(1)
	if !gatewayEnabled {
		nodeID = int64(0)
	}
	require.Equal(t, nodeID, gatewayNodeID, "Gateway NodeID")
	require.Equal(t, "[1]", allNodesIds, "All NodeIDs from statistics")
}

func waitForFollowerReadTimestamp(t *testing.T, sqlConn *sqlutils.SQLRunner) {
	var hlcTimestamp time.Time
	sqlConn.QueryRow(t, `SELECT hlc_to_timestamp(cluster_logical_timestamp())`).Scan(&hlcTimestamp)

	testutils.SucceedsSoon(t, func() error {
		var followerReadTimestamp time.Time
		sqlConn.QueryRow(t, `SELECT follower_read_timestamp()`).Scan(&followerReadTimestamp)
		if followerReadTimestamp.Before(hlcTimestamp) {
			return errors.New("waiting for follower_read_timestamp to be passed hlc timestamp")
		}
		return nil
	})
}

func smallestStatsCountAcrossAllShards(
	t *testing.T, sqlConn *sqlutils.SQLRunner,
) (numStmtStats int64) {
	numStmtStats = math.MaxInt64
	for i := 0; i < systemschema.SQLStatsHashShardBucketCount; i++ {
		var temp int64
		sqlConn.QueryRow(t, `
		SELECT count(*)
		FROM system.statement_statistics
		WHERE crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8 = $1`, i).Scan(&temp)
		if temp < numStmtStats {
			numStmtStats = temp
		}
	}

	return numStmtStats
}
