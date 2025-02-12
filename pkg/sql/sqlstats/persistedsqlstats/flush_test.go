// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstats_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatstestutil"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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

	skip.UnderRace(t)

	fakeTime := stubTime{
		aggInterval: time.Hour,
	}
	fakeTime.setTime(timeutil.Now())

	testCluster := serverutils.StartCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
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

	// We'll use the third server to execute any sql queries we don't want to
	// pollute the in-memory stats of the other 2 servers.
	observerConn := sqlutils.MakeSQLRunner(testCluster.Server(2).SQLConn(t))

	firstServerSQLStats := firstServer.SQLServer().(*sql.Server).GetSQLStatsProvider()
	secondServerSQLStats := secondServer.SQLServer().(*sql.Server).GetSQLStatsProvider()

	firstServerLocalSS := firstServer.SQLServer().(*sql.Server).GetLocalSQLStatsProvider()
	secondServerLocalSS := secondServer.SQLServer().(*sql.Server).GetLocalSQLStatsProvider()

	firstSQLConn.Exec(t, "SET application_name = 'flush_unit_test'")
	secondSQLConn.Exec(t, "SET application_name = 'flush_unit_test'")

	// Regular inserts.
	{
		for _, tc := range testQueries {
			for i := int64(0); i < tc.count; i++ {
				firstSQLConn.Exec(t, tc.query)
			}
		}

		verifyInMemoryStatsCorrectness(t, testQueries, firstServerLocalSS)
		verifyInMemoryStatsEmpty(t, secondServerLocalSS)

		firstServerSQLStats.MaybeFlush(ctx, testCluster.ApplicationLayer(0).AppStopper())
		secondServerSQLStats.MaybeFlush(ctx, testCluster.ApplicationLayer(1).AppStopper())

		verifyInMemoryStatsEmpty(t, firstServerLocalSS)
		verifyInMemoryStatsEmpty(t, secondServerLocalSS)

		sqlInstanceId := base.SQLInstanceID(0)
		if sqlstats.GatewayNodeEnabled.Get(&testCluster.Server(0).ClusterSettings().SV) {
			sqlInstanceId = firstServer.SQLInstanceID()
		}
		// For each test case, we verify that it's being properly inserted exactly
		// once and it is exactly executed tc.count number of times.
		for _, tc := range testQueries {
			verifyNumOfInsertedEntries(t, observerConn, tc.stmtNoConst, sqlInstanceId, 1 /* expectedStmtEntryCnt */, 1 /* expectedTxnEntryCtn */)
			verifyInsertedFingerprintExecCount(t, observerConn, tc.stmtNoConst, fakeTime.getAggTimeTs(), sqlInstanceId, tc.count)
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
		verifyInMemoryStatsCorrectness(t, testQueries, firstServerLocalSS)
		verifyInMemoryStatsEmpty(t, secondServerLocalSS)

		firstServerSQLStats.MaybeFlush(ctx, testCluster.ApplicationLayer(0).AppStopper())
		secondServerSQLStats.MaybeFlush(ctx, testCluster.ApplicationLayer(1).AppStopper())

		verifyInMemoryStatsEmpty(t, firstServerLocalSS)
		verifyInMemoryStatsEmpty(t, secondServerLocalSS)

		sqlInstanceId := base.SQLInstanceID(0)
		if sqlstats.GatewayNodeEnabled.Get(&testCluster.Server(0).ClusterSettings().SV) {
			sqlInstanceId = firstServer.SQLInstanceID()
		}

		for _, tc := range testQueries {
			verifyNumOfInsertedEntries(t, observerConn, tc.stmtNoConst, sqlInstanceId, 1 /* expectedStmtEntryCnt */, 1 /* expectedTxnEntryCtn */)
			// The execution count is doubled here because we execute all of the
			// statements here in the same aggregation interval.
			verifyInsertedFingerprintExecCount(t, observerConn, tc.stmtNoConst, fakeTime.getAggTimeTs(), sqlInstanceId, tc.count+tc.count-1 /* expectedCount */)
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
		verifyInMemoryStatsCorrectness(t, testQueries, firstServerLocalSS)
		verifyInMemoryStatsEmpty(t, secondServerLocalSS)

		firstServerSQLStats.MaybeFlush(ctx, testCluster.ApplicationLayer(0).AppStopper())
		secondServerSQLStats.MaybeFlush(ctx, testCluster.ApplicationLayer(1).AppStopper())

		verifyInMemoryStatsEmpty(t, firstServerLocalSS)
		verifyInMemoryStatsEmpty(t, secondServerLocalSS)

		sqlInstanceId := base.SQLInstanceID(0)
		if sqlstats.GatewayNodeEnabled.Get(&testCluster.Server(0).ClusterSettings().SV) {
			sqlInstanceId = firstServer.SQLInstanceID()
		}

		for _, tc := range testQueries {
			// We expect exactly 2 entries since we are in a different aggregation window.
			verifyNumOfInsertedEntries(t, observerConn, tc.stmtNoConst, sqlInstanceId, 2 /* expectedStmtEntryCnt */, 2 /* expectedTxnEntryCtn */)
			verifyInsertedFingerprintExecCount(t, observerConn, tc.stmtNoConst, fakeTime.getAggTimeTs(), sqlInstanceId, tc.count)
		}
	}

	// We run queries in a different server and trigger the flush.
	{
		for _, tc := range testQueries {
			for i := int64(0); i < tc.count; i++ {
				secondSQLConn.Exec(t, tc.query)
			}
		}
		verifyInMemoryStatsEmpty(t, firstServerLocalSS)
		verifyInMemoryStatsCorrectness(t, testQueries, secondServerLocalSS)

		firstServerSQLStats.MaybeFlush(ctx, testCluster.ApplicationLayer(0).AppStopper())
		secondServerSQLStats.MaybeFlush(ctx, testCluster.ApplicationLayer(1).AppStopper())

		verifyInMemoryStatsEmpty(t, firstServerLocalSS)
		verifyInMemoryStatsEmpty(t, secondServerLocalSS)

		if sqlstats.GatewayNodeEnabled.Get(&testCluster.Server(0).ClusterSettings().SV) {
			// Ensure that we encode the correct node_id for the new entry and did not
			// accidentally tamper the entries written by another server.
			for _, tc := range testQueries {
				verifyNumOfInsertedEntries(t, firstSQLConn, tc.stmtNoConst, secondServer.SQLInstanceID(), 1 /* expectedStmtEntryCnt */, 1 /* expectedTxnEntryCtn */)
				verifyInsertedFingerprintExecCount(t, firstSQLConn, tc.stmtNoConst, fakeTime.getAggTimeTs(), secondServer.SQLInstanceID(), tc.count)
				verifyNumOfInsertedEntries(t, observerConn, tc.stmtNoConst, firstServer.SQLInstanceID(), 2 /* expectedStmtEntryCnt */, 2 /* expectedTxnEntryCtn */)
				verifyInsertedFingerprintExecCount(t, observerConn, tc.stmtNoConst, fakeTime.getAggTimeTs(), firstServer.SQLInstanceID(), tc.count)
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
		GetSQLStatsProvider().GetNextFlushAt()

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
		GetSQLStatsProvider().MaybeFlush(ctx, s.AppStopper())

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
		GetSQLStatsProvider().MaybeFlush(ctx, s.AppStopper())

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
		GetSQLStatsProvider().MaybeFlush(ctx, s.AppStopper())

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
			GetSQLStatsProvider().MaybeFlush(ctx, s.AppStopper())

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
			GetSQLStatsProvider().MaybeFlush(ctx, s.AppStopper())

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
			GetSQLStatsProvider().MaybeFlush(ctx, s.AppStopper())

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
		GetSQLStatsProvider().MaybeFlush(ctx, s.AppStopper())

	verifyNodeID(t, sqlConn, "SELECT _", true, "gateway_enabled")

	// Gateway Node ID disabled, so shouldn't persist the value on the node_id column, but it should
	// still store the value on the statistics column.
	sqlConn.Exec(t, "SET CLUSTER SETTING sql.metrics.statement_details.gateway_node.enabled = false")
	sqlConn.Exec(t, "SET application_name = 'gateway_disabled'")
	sqlConn.Exec(t, "SELECT 1")
	s.SQLServer().(*sql.Server).
		GetSQLStatsProvider().MaybeFlush(ctx, s.AppStopper())

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
	pss := s.SQLServer().(*sql.Server).GetSQLStatsProvider()

	// 1. Flush then count to get the initial number of rows.
	pss.MaybeFlush(ctx, s.AppStopper())
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
		pss.MaybeFlush(ctx, s.AppStopper())
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

			pss.MaybeFlush(ctx, s.AppStopper())

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
	pss := s.SQLServer().(*sql.Server).GetSQLStatsProvider()

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

	pss.MaybeFlush(ctx, s.AppStopper())
	stmtStatsCountFlush, _ := countStats(t, sqlConn)

	// Ensure we have some rows in system.statement_statistics
	require.GreaterOrEqual(t, stmtStatsCountFlush, minNumExpectedStmts)

	persistedsqlstats.SQLStatsMaxPersistedRows.Override(ctx, &s.ClusterSettings().SV, maxNumPersistedRows)

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
	persistedsqlstats.SQLStatsLimitTableCheckInterval.Override(ctx, &s.ClusterSettings().SV, time.Nanosecond)

	// Begin a transaction.
	sqlConn.Exec(t, "BEGIN")
	// Lock the table. Create a state of contention.
	sqlConn.Exec(t, "SELECT * FROM system.statement_statistics FOR UPDATE")

	// Ensure that we can read from the table despite it being locked, due to the follower read (AOST).
	// Expect that the number of statements in the table exceeds sql.stats.persisted_rows.max * 1.5
	// (meaning that the limit will be reached) and no error. Every iteration picks a random shard, and we
	// loop 3 times to make sure that we find at least one shard with a count over the limit. In the wild,
	// we've observed individual shards only having a single statement recorded which makes this check fail
	// otherwise.
	foundLimit := false
	for i := 0; i < 3; i++ {
		limitReached, err = pss.StmtsLimitSizeReached(ctx)
		require.NoError(t, err)
		if limitReached {
			foundLimit = true
		}
	}

	if !foundLimit {
		readStmt := `SELECT crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8, count(*)
      FROM system.statement_statistics
      AS OF SYSTEM TIME follower_read_timestamp()
      GROUP BY crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8`

		sqlConn2 := sqlutils.MakeSQLRunner(s.SQLConn(t))
		rows := sqlConn2.Query(t, readStmt)
		shard := make([]int64, 8)
		count := make([]int64, 8)
		for j := 0; rows.Next(); {
			err := rows.Scan(&shard[j], &count[j])
			require.NoError(t, err)
			j += 1
		}
		t.Fatalf("limitReached should be true. shards: %d counts: %d", shard, count)
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

	appName := fmt.Sprintf("TestSQLStatsPlanSampling_%s", uuid.MakeV4().String())
	sqlRun.Exec(t, "SET application_name = $1", appName)

	sqlStats := s.SQLServer().(*sql.Server).GetSQLStatsProvider()
	appStats := sqlStats.GetApplicationStats(appName)

	sqlRun.Exec(t, `SET CLUSTER SETTING sql.txn_stats.sample_rate = 0;`)

	validateSample := func(fingerprint string, implicitTxn bool, expectedPreviouslySampledState bool) {

		previouslySampled := appStats.StatementSampled(
			fingerprint,
			implicitTxn,
			dbName,
		)

		errMessage := fmt.Sprintf("validate: %s, implicit: %t expected sample before: %t, actual sample before: %t\n",
			fingerprint, implicitTxn, expectedPreviouslySampledState, previouslySampled)
		require.Equal(t, expectedPreviouslySampledState, previouslySampled, errMessage)
	}

	setTime("2021-09-20T15:00:00Z")

	// Logical plan should be sampled here, since we have not collected logical plan
	// at all.
	validateSample("SELECT _", true, false)

	// Execute the query to trigger a collection of logical plan.
	// (db_name=defaultdb implicitTxn=true fingerprint=SELECT _)
	_, err := conn.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err)

	// Ensure that if a query is to be subsequently executed, it will not cause
	// logical plan sampling.
	validateSample("SELECT _", true, true)

	// However, if we are to execute the same statement but under explicit
	// transaction, the plan will still need to be sampled.
	validateSample("SELECT _", false, false)

	// Execute the statement under explicit transaction.
	// (db_name=defaultdb implicitTxn=false fingerprint=SELECT _)
	tx, err := conn.BeginTx(ctx, &gosql.TxOptions{})
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Ensure that the subsequent execution of the query will not cause logical plan
	// collection.
	validateSample("SELECT _", false, true)

	// Set the time to the future and ensure we will resample the logical plan.
	setTime("2021-09-20T15:05:01Z")

	// If tracing is not enabled the statement will not be sampled. To update the
	// save plan for stats last sampled time the statement must have tracing enabled.
	_, err = conn.ExecContext(ctx, "SET CLUSTER SETTING sql.txn_stats.sample_rate = 1;")
	require.NoError(t, err)

	validateSample("SELECT _", true, true)

	// implicit txn
	_, err = conn.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err)
	validateSample("SELECT _", true, true)

	// explicit txn
	validateSample("SELECT _", false, true)

	tx, err = conn.BeginTx(ctx, &gosql.TxOptions{})
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Ensure that the subsequent execution of the query will not cause logical plan
	// collection.
	validateSample("SELECT _", false, true)
}

func TestPersistedSQLStats_Flush(t *testing.T) {
	// This test should guarantee that stats should be persisted even if they were
	// collected between recent flush and before next reset of in-memory stats.
	//
	// ------X--------------X-------------X---------------X-----------X----> t
	//  Add stats (1)     Flush      Add stats (2)      Reset       Flush
	//                                                                ^
	//                            should be flushed with              |
	//                                  next Flush      --------------|
	t.Run("add stats before reset", func(t *testing.T) {
		defer leaktest.AfterTest(t)()
		defer log.Scope(t).Close(t)
		ctx := context.Background()
		var flushedStmtStats int
		var flushedTxnStats int
		appName := "app"

		ch := make(chan struct{})
		defer func() {
			close(ch)
		}()

		init := atomic.Bool{}
		init.Store(false)

		// We create a server without starting it to control adding new stats and flushing them manually.
		srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: &sqlstats.TestingKnobs{
					FlushInterceptor: func(ctx context.Context, stopper *stop.Stopper, aggregatedTs time.Time, stmtStats []*appstatspb.CollectedStatementStatistics, txnStats []*appstatspb.CollectedTransactionStatistics) {
						for _, stmt := range stmtStats {
							if stmt.Key.App == appName {
								flushedStmtStats++
							}
						}

						for _, txn := range txnStats {
							if txn.App == appName {
								flushedTxnStats++
							}
						}
					},
					OnAfterClear: func() {
						if init.Load() {
							init.Store(false)
							go func() {
								ch <- struct{}{}
							}()
						}
					},
				},
			},
		})
		defer srv.Stopper().Stop(ctx)

		sqlConn := sqlutils.MakeSQLRunner(conn)
		sqlConn.Exec(t,
			"SET CLUSTER SETTING sql.stats.limit_table_size.enabled = 'false'")

		sqlStats := srv.ApplicationLayer().SQLServer().(*sql.Server).GetLocalSQLStatsProvider()
		pss := srv.ApplicationLayer().SQLServer().(*sql.Server).GetSQLStatsProvider()
		{
			// Add some stats for the first time. It should add one stmt and one txn stats to in-memory sql stats cache.
			// Add stmt stats.
			randomData := sqlstatstestutil.GetRandomizedCollectedStatementStatisticsForTest(t)
			var stmt serverpb.StatementsResponse_CollectedStatementStatistics
			stmt.Key.KeyData = randomData.Key
			stmtContainer, _, _ := ssmemstorage.NewTempContainerFromExistingStmtStats([]serverpb.StatementsResponse_CollectedStatementStatistics{stmt})
			err := sqlStats.AddAppStats(ctx, appName, stmtContainer)
			require.NoError(t, err)

			// Add txn stats
			var txn serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics
			txn.StatsData = sqlstatstestutil.GetRandomizedCollectedTransactionStatisticsForTest(t)
			txn.StatsData.TransactionFingerprintID = appstatspb.TransactionFingerprintID(42)
			txnContainer, _, _ := ssmemstorage.NewTempContainerFromExistingTxnStats([]serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics{txn})
			err = sqlStats.AddAppStats(ctx, appName, txnContainer)
			require.NoError(t, err)
		}

		init.Store(true)
		// Flush all available stats.
		pss.MaybeFlush(ctx, srv.AppStopper())

		require.Equal(t, 1, flushedStmtStats)
		require.Equal(t, 1, flushedTxnStats)

		// At this moment, Sql Stats is going to be reset and new stmt and txn stats is added to in-memory Sql stats.
		<-ch
		init.Store(false)

		{
			// Add stmt stats second time
			randomData := sqlstatstestutil.GetRandomizedCollectedStatementStatisticsForTest(t)
			var stmt serverpb.StatementsResponse_CollectedStatementStatistics
			stmt.Key.KeyData = randomData.Key
			stmtContainer, _, _ := ssmemstorage.NewTempContainerFromExistingStmtStats([]serverpb.StatementsResponse_CollectedStatementStatistics{stmt})
			err := sqlStats.AddAppStats(ctx, appName, stmtContainer)
			require.NoError(t, err)

			// Add txn stats second time
			var txn serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics
			txn.StatsData = sqlstatstestutil.GetRandomizedCollectedTransactionStatisticsForTest(t)
			txn.StatsData.TransactionFingerprintID = appstatspb.TransactionFingerprintID(42)
			txnContainer, _, _ := ssmemstorage.NewTempContainerFromExistingTxnStats([]serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics{txn})
			err = sqlStats.AddAppStats(ctx, appName, txnContainer)
			require.NoError(t, err)
		}

		// Flush all stats again. This time it should flush all of the stats that happen to be collected right
		// before SQLStats.
		pss.MaybeFlush(ctx, srv.AppStopper())

		require.Equal(t, 2, flushedStmtStats)
		require.Equal(t, 2, flushedTxnStats)
	})
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

func verifyInMemoryStatsCorrectness(t *testing.T, tcs []testCase, statsProvider *sslocal.SQLStats) {
	for _, tc := range tcs {
		err := statsProvider.IterateStatementStats(context.Background(), sqlstats.IteratorOptions{}, func(ctx context.Context, statistics *appstatspb.CollectedStatementStatistics) error {
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

func verifyInMemoryStatsEmpty(t *testing.T, statsProvider *sslocal.SQLStats) {
	// We could be inserting internal statements in the background, so we only check
	// that we have no user queries left in the container.
	fingerprintCount := statsProvider.GetTotalFingerprintCount()
	var count int64
	err := statsProvider.IterateStatementStats(context.Background(), sqlstats.IteratorOptions{},
		func(ctx context.Context, statistics *appstatspb.CollectedStatementStatistics) error {
			// We should have cleared the sql stats containers on flush.
			if statistics.Key.App != "" && !strings.HasPrefix(statistics.Key.App, catconstants.InternalAppNamePrefix) {
				return errors.Newf("unexpected non-internal statement: %s from app %s",
					statistics.Key.Query, statistics.Key.App)
			}
			count++
			return nil
		})
	require.NoError(t, err)

	err = statsProvider.IterateTransactionStats(context.Background(), sqlstats.IteratorOptions{},
		func(ctx context.Context, statistics *appstatspb.CollectedTransactionStatistics) error {
			// We should have cleared the sql stats containers on flush.
			if statistics.App != "" && !strings.HasPrefix(statistics.App, catconstants.InternalAppNamePrefix) {
				return errors.Newf("unexpected non-internal transaction from app %s", statistics.App)
			}
			count++
			return nil
		})

	require.NoError(t, err)

	// More internal statements could have been added since reading the fingerprint
	// count, so we only check that the number of fingerprints is less than the total
	// fingerprints counted after the counter read.
	require.LessOrEqual(t, fingerprintCount, count)
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

func TestSQLStatsFlushDoesntWaitForFlushSigReceiver(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sqlStatsKnobs := sqlstats.CreateTestingKnobs()
	var sqlStmtFlushCount, sqlTxnFlushCount atomic.Int32
	sqlStatsKnobs.OnStmtStatsFlushFinished = func() {
		sqlStmtFlushCount.Add(1)
	}
	sqlStatsKnobs.OnTxnStatsFlushFinished = func() {
		sqlTxnFlushCount.Add(1)
	}
	tc := serverutils.StartCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: sqlStatsKnobs,
			},
		},
	})

	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	ss := tc.ApplicationLayer(0).SQLServer().(*sql.Server).GetSQLStatsProvider()
	flushDoneCh := make(chan struct{})
	ss.SetFlushDoneSignalCh(flushDoneCh)

	// It should not block on the flush signal receiver.
	persistedsqlstats.SQLStatsFlushInterval.Override(ctx, &tc.Server(0).ClusterSettings().SV, 100*time.Millisecond)
	testutils.SucceedsSoon(t, func() error {
		if sqlStmtFlushCount.Load() < 5 || sqlTxnFlushCount.Load() < 5 {
			return errors.New("flush count hasn't been reached yet")
		}
		return nil
	})
}

// TestSQLStatsFlushWorkerDoesntSignalJobOnAbort asserts that the flush
// worker does not signal the sql activity job if the flush was aborted.
func TestSQLStatsFlushWorkerDoesntSignalJobOnAbort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sqlStatsKnobs := sqlstats.CreateTestingKnobs()
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLStatsKnobs: sqlStatsKnobs,
		},
	})

	ctx := context.Background()
	defer ts.Stopper().Stop(ctx)

	ss := ts.ApplicationLayer().SQLServer().(*sql.Server).GetSQLStatsProvider()
	flushDoneCh := make(chan struct{})
	ss.SetFlushDoneSignalCh(flushDoneCh)

	persistedsqlstats.SQLStatsFlushEnabled.Override(ctx, &ts.ClusterSettings().SV, false)
	persistedsqlstats.SQLStatsFlushInterval.Override(ctx, &ts.ClusterSettings().SV, 50*time.Millisecond)
	// The flush is disabled so the operation should finish very quickly.
	// Sleeping for this amount should trigger at least a few flushes.
	time.Sleep(250 * time.Millisecond)
	select {
	case <-flushDoneCh:
		t.Fatal("flush signal should not have been received")
	default:
	}
}

func BenchmarkSQLStatsFlush(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	fakeTime := stubTime{
		aggInterval: time.Hour,
	}
	fakeTime.setTime(timeutil.Now())
	ts, conn, _ := serverutils.StartServer(b, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLStatsKnobs: &sqlstats.TestingKnobs{
				StubTimeNow: fakeTime.Now,
			},
		},
	},
	)
	defer ts.Stop(context.Background())

	sqlStats := ts.SQLServer().(*sql.Server).GetSQLStatsProvider()
	runner := sqlutils.MakeSQLRunner(conn)

	ctx := context.Background()
	const QueryCountScale = int64(5000)
	for iter := 0; iter < b.N; iter++ {
		for _, tc := range testQueries {
			for i := int64(0); i < QueryCountScale; i++ {
				runner.Exec(b, tc.query)
			}
		}
		b.StartTimer()
		sqlStats.MaybeFlush(ctx, ts.Stopper())
		b.StartTimer()
	}
}
