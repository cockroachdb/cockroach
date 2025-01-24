// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	jsonUtil "github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestSqlActivityUpdateJob verifies that the
// job is created.
func TestSqlActivityUpdateJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	stubTime := timeutil.Now().Truncate(time.Hour)
	sqlStatsKnobs := sqlstats.CreateTestingKnobs()
	sqlStatsKnobs.StubTimeNow = func() time.Time { return stubTime }

	// Start the cluster.
	// Disable the job since it is called manually from a new instance to avoid
	// any race conditions.
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Insecure: true,
		Knobs: base.TestingKnobs{
			SQLStatsKnobs: sqlStatsKnobs,
			UpgradeManager: &upgradebase.TestingKnobs{
				DontUseJobs:                       true,
				SkipUpdateSQLActivityJobBootstrap: true,
			}}})
	defer srv.Stopper().Stop(context.Background())
	defer sqlDB.Close()
	ts := srv.ApplicationLayer()

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, `SET CLUSTER SETTING sql.stats.activity.flush.enabled = true;`)

	var count int
	row := db.QueryRow(t, "SELECT count_rows() FROM system.public.transaction_activity")
	row.Scan(&count)
	require.Equal(t, 0, count, "system.transaction_activity: expect:0, actual:%d", count)

	row = db.QueryRow(t, "SELECT count_rows() FROM system.public.statement_activity")
	row.Scan(&count)
	require.Equal(t, 0, count, "system.statement_activity: expect:0, actual:%d", count)

	row = db.QueryRow(t,
		"SELECT count_rows() FROM system.public.jobs WHERE job_type = 'AUTO UPDATE SQL ACTIVITY' and id = 103")
	row.Scan(&count)
	require.Equal(t, 0, count, "jobs: expect:0, actual:%d", count)

	verifyActivityTablesAreEmpty(t, db)

	execCfg := ts.ExecutorConfig().(ExecutorConfig)
	st := cluster.MakeTestingClusterSettings()
	updater := newSqlActivityUpdater(st, execCfg.InternalDB, sqlStatsKnobs)

	require.NoError(t, updater.TransferStatsToActivity(ctx))

	verifyActivityTablesAreEmpty(t, db)

	// Use random name to keep isolated during stress tests.
	rng, _ := randutil.NewTestRand()
	appName := fmt.Sprintf("TestSqlActivityUpdateJob-%d", rng.Int())

	db.Exec(t, "SET SESSION application_name=$1", appName)
	db.Exec(t, "SELECT 1;")

	ts.SQLServer().(*Server).GetSQLStatsProvider().MaybeFlush(ctx, srv.AppStopper())

	db.Exec(t, "SET SESSION application_name=$1", "randomIgnore")

	// Run the updater to add rows to the activity tables.
	// This will use the transfer all scenarios with there only
	// being a few rows.
	require.NoError(t, updater.TransferStatsToActivity(ctx))

	verifyActivityTableContentHelper(t, db, appName)
}

// TestMergeFunctionLogic verifies the merge functions used in the
// SQL statements to verify the data.
func TestMergeFunctionLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	stubTime := timeutil.Now().Truncate(time.Hour)
	sqlStatsKnobs := sqlstats.CreateTestingKnobs()
	sqlStatsKnobs.StubTimeNow = func() time.Time { return stubTime }

	// Start the cluster.
	// Disable the job since it is called manually from a new instance to avoid
	// any race conditions.
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Insecure: true,
		Knobs: base.TestingKnobs{
			SQLStatsKnobs: sqlStatsKnobs,
			UpgradeManager: &upgradebase.TestingKnobs{
				DontUseJobs:                       true,
				SkipUpdateSQLActivityJobBootstrap: true,
			}}})
	defer srv.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, `SET CLUSTER SETTING sql.stats.activity.flush.enabled = true;`)

	appName := "TestMergeFunctionLogic"
	db.Exec(t, "SET SESSION application_name=$1", appName)
	db.Exec(t, "SELECT * FROM system.statement_statistics")
	db.Exec(t, "SELECT * FROM system.statement_statistics")
	db.Exec(t, "SELECT count_rows() FROM system.transaction_statistics")

	srv.SQLServer().(*Server).GetSQLStatsProvider().MaybeFlush(ctx, srv.AppStopper())

	db.Exec(t, "SET SESSION application_name=$1", "randomIgnore")

	var localAggTxnStats appstatspb.TransactionStatistics
	rows := db.Query(t, "SELECT statistics FROM system.public.transaction_statistics WHERE app_name = $1", appName)
	require.NoError(t, rows.Err())
	defer rows.Close()
	for rows.Next() {
		var tempStats appstatspb.TransactionStatistics
		var jsonString string
		require.NoError(t, rows.Scan(&jsonString))
		j, err := jsonUtil.ParseJSON(jsonString)
		require.NoError(t, err)
		require.NoError(t, sqlstatsutil.DecodeTxnStatsStatisticsJSON(j, &tempStats))
		require.Greater(t, tempStats.Count, int64(0), "empty object: json:%s\n obj:%v\n", jsonString, tempStats)
		localAggTxnStats.Add(&tempStats)
	}

	require.Equal(t, int64(3), localAggTxnStats.Count)

	row := db.QueryRow(t, `SELECT merge_transaction_stats(statistics) AS statistics FROM system.public.transaction_statistics WHERE app_name = $1 GROUP BY app_name`, appName)
	var aggSqlTxnStats appstatspb.TransactionStatistics
	var jsonString string
	row.Scan(&jsonString)
	j, err := jsonUtil.ParseJSON(jsonString)
	require.NoError(t, err)
	require.NoError(t, sqlstatsutil.DecodeTxnStatsStatisticsJSON(j, &aggSqlTxnStats))
	require.Equal(t, localAggTxnStats, aggSqlTxnStats)

	var localAggStmtStats appstatspb.StatementStatistics
	rows = db.Query(t, "SELECT statistics FROM system.public.statement_statistics WHERE app_name = $1", appName)
	require.NoError(t, rows.Err())
	defer rows.Close()
	for rows.Next() {
		var tempStats appstatspb.StatementStatistics
		require.NoError(t, rows.Scan(&jsonString))
		j, err = jsonUtil.ParseJSON(jsonString)
		require.NoError(t, err)
		require.NoError(t, sqlstatsutil.DecodeStmtStatsStatisticsJSON(j, &tempStats))
		require.Greater(t, tempStats.Count, int64(0), "empty object: json:%s\n obj:%v\n", jsonString, tempStats)
		localAggStmtStats.Add(&tempStats)
	}
	require.Equal(t, int64(3), localAggTxnStats.Count)

	row = db.QueryRow(t, `SELECT merge_statement_stats(statistics) AS statistics FROM system.public.statement_statistics WHERE app_name = $1 GROUP BY app_name`, appName)
	var aggStmtStat appstatspb.StatementStatistics
	row.Scan(&jsonString)
	j, err = jsonUtil.ParseJSON(jsonString)
	require.NoError(t, err)
	require.NoError(t, sqlstatsutil.DecodeStmtStatsStatisticsJSON(j, &aggStmtStat))
	require.Equal(t, localAggStmtStats, aggStmtStat)

	// Verify metadata logic
	var localAggStmtMeta appstatspb.AggregatedStatementMetadata
	rows = db.Query(t, "SELECT metadata FROM system.public.statement_statistics WHERE app_name = $1", appName)
	require.NoError(t, rows.Err())
	defer rows.Close()
	for rows.Next() {
		var tempStats appstatspb.CollectedStatementStatistics
		require.NoError(t, rows.Scan(&jsonString))
		j, err = jsonUtil.ParseJSON(jsonString)
		require.NoError(t, err)
		require.NoError(t, sqlstatsutil.DecodeStmtStatsMetadataJSON(j, &tempStats))
		require.NotNil(t, tempStats.Key.Query, "empty object: json:%s\n obj:%v\n", jsonString, tempStats)
		localAggStmtMeta.Add(&tempStats)
	}

	require.Equal(t, int64(2), localAggStmtMeta.TotalCount)

	row = db.QueryRow(t, `SELECT merge_stats_metadata(metadata) AS metadata FROM system.public.statement_statistics WHERE app_name = $1 GROUP BY app_name`, appName)
	var aggSqlStmtMeta appstatspb.AggregatedStatementMetadata
	row.Scan(&jsonString)
	j, err = jsonUtil.ParseJSON(jsonString)
	require.NoError(t, err)
	require.NoError(t, sqlstatsutil.DecodeAggregatedMetadataJSON(j, &aggSqlStmtMeta))
	require.Equal(t, localAggStmtMeta, aggSqlStmtMeta)
}

// TestSqlActivityUpdateTopLimitJob verifies that the
// job is created.
func TestSqlActivityUpdateTopLimitJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "test is too slow to run under race")

	stubTime := timeutil.Now().Truncate(time.Hour)
	sqlStatsKnobs := sqlstats.CreateTestingKnobs()
	sqlStatsKnobs.StubTimeNow = func() time.Time { return stubTime }

	// Start the cluster.
	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Insecure: true,
		Knobs: base.TestingKnobs{
			SQLStatsKnobs:    sqlStatsKnobs,
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})

	defer srv.Stopper().Stop(context.Background())
	defer sqlDB.Close()
	ts := srv.ApplicationLayer()

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, `SET CLUSTER SETTING sql.stats.activity.flush.enabled = true;`)

	// Give permission to write to sys tables.
	db.Exec(t, "INSERT INTO system.users VALUES ('node', NULL, true, 3)")
	db.Exec(t, "GRANT node TO root")

	// Make sure all the tables are empty initially.
	db.Exec(t, "DELETE FROM system.public.transaction_activity")
	db.Exec(t, "DELETE FROM system.public.statement_activity")
	db.Exec(t, "DELETE FROM system.public.transaction_statistics")
	db.Exec(t, "DELETE FROM system.public.statement_statistics")

	verifyActivityTablesAreEmpty(t, db)

	execCfg := ts.ExecutorConfig().(ExecutorConfig)
	st := cluster.MakeTestingClusterSettings()
	su := st.MakeUpdater()
	const topLimit = 3
	err := su.Set(ctx, "sql.stats.activity.top.max", settings.EncodedValue{
		Value: settings.EncodeInt(int64(topLimit)),
		Type:  "i",
	})
	require.NoError(t, err)

	updater := newSqlActivityUpdater(st, execCfg.InternalDB, sqlStatsKnobs)

	db.Exec(t, "SET tracing = true;")

	db.Exec(t, "set cluster setting sql.txn_stats.sample_rate  = 1;")

	const appNamePrefix = "TestSqlActivityUpdateTopLimitJob"
	getAppName := func(count int) string {
		return fmt.Sprintf("%s%d", appNamePrefix, count)
	}
	appIndexCount := 0
	updateStatsCount := 0
	for i := 1; i <= 5; i++ {

		// Generate unique rows in the statistics tables.
		// numQueries = per-column-limit * numColumns with some padding since we need more rows than the limit.
		const numQueries = topLimit*6 + 10
		for j := 0; j < numQueries; j++ {
			appIndexCount++
			db.Exec(t, "SET SESSION application_name=$1", getAppName(appIndexCount))
			db.Exec(t, "SELECT 1;")
		}

		db.Exec(t, "SET SESSION application_name=$1", "topTransaction")
		tx := db.Begin(t)
		_, err = tx.Exec("SELECT 1,2")
		require.NoError(t, err)
		_, err = tx.Exec("SELECT 1,2,3")
		require.NoError(t, err)
		err = tx.Commit()
		require.NoError(t, err)

		db.Exec(t, "SET SESSION application_name=$1", "randomIgnore")

		db.Exec(t, "SET CLUSTER SETTING sql.stats.flush.enabled  = true;")
		ts.SQLServer().(*Server).GetSQLStatsProvider().MaybeFlush(ctx, srv.AppStopper())
		db.Exec(t, "SET CLUSTER SETTING sql.stats.flush.enabled  = false;")

		// Run the updater to add rows to the activity tables.
		// This will use the transfer all scenarios with there only
		// being a few rows.
		err = updater.TransferStatsToActivity(ctx)
		require.NoError(t, err)

		verifyTopActivityTableContentHelper(t, db, 500)

		// The max number of queries is number of top columns * max number of
		// queries per a column (6*3=18 for this test, 6*500=3000 default). Most of
		// the top queries are the same in this test so instead of getting 18 it's
		// only 6 to 8 which make test unreliable. The solution is to change the
		// values for each of the columns to make sure that it is always the max.
		// Then run the update multiple times with new top queries each time. This
		// is needed to simulate ORM which generates a lot of unique queries.

		// Execution count.
		for j := 0; j < topLimit; j++ {
			updateStatsCount++
			db.Exec(t, `UPDATE system.public.statement_statistics
			SET statistics =  jsonb_set(jsonb_set(statistics, '{statistics, cnt}', to_jsonb($1::INT)),
			    '{statistics, svcLat, mean}', to_jsonb($2::FLOAT))
			    WHERE app_name = $3;`, 10000+updateStatsCount, 0.0000001, getAppName(updateStatsCount))
		}

		// Service latency time.
		for j := 0; j < topLimit; j++ {
			updateStatsCount++
			db.Exec(t, `UPDATE system.public.statement_statistics
			SET statistics =  jsonb_set(jsonb_set(statistics, '{statistics, cnt}', to_jsonb($1::INT)),
			    '{statistics, svcLat, mean}', to_jsonb($2::FLOAT))
			    WHERE app_name = $3;`, 1, 1000+updateStatsCount, getAppName(updateStatsCount))
		}

		// Total execution time. Needs to be less than individual execution count
		// and service latency, but greater when multiplied together.
		for j := 0; j < topLimit; j++ {
			updateStatsCount++
			db.Exec(t, `UPDATE system.public.statement_statistics
			SET statistics =  jsonb_set(jsonb_set(statistics, '{statistics, cnt}', to_jsonb($1::INT)),
			    '{statistics, svcLat, mean}', to_jsonb($2::FLOAT))
			    WHERE app_name = $3;`, 500+updateStatsCount, 500+updateStatsCount, getAppName(updateStatsCount))
		}

		// Remaining columns don't interact so a loop can be used
		columnsToChangeValues := []string{"{execution_statistics, contentionTime, mean}", "{execution_statistics, cpu_sql_nanos, mean}", "{statistics, latencyInfo, p99}"}
		for _, updateField := range columnsToChangeValues {
			for j := 0; j < topLimit; j++ {
				updateStatsCount++
				db.Exec(t, `UPDATE system.public.statement_statistics
			SET statistics =  jsonb_set(statistics, $1, to_jsonb($2::INT)) 
			WHERE app_name = $3;`, updateField, 10000+updateStatsCount, getAppName(updateStatsCount))
			}
		}

		// Update the transaction stats
		db.Exec(t, `UPDATE system.public.transaction_statistics
			SET statistics =  jsonb_set(jsonb_set(statistics, '{statistics, cnt}', to_jsonb($1::INT)),
			    '{statistics, svcLat, mean}', to_jsonb($2::FLOAT))
			    WHERE app_name = $3;`, 10000, 1, "topTransaction")

		// Help check for primary key conflicts.
		duplicateRowHelper(t, db, getAppName(0))

		// Run the updater to add rows to the activity tables.
		// This will use the transfer all scenarios with there only
		// being a few rows.
		err = updater.TransferStatsToActivity(ctx)
		require.NoError(t, err)

		var httpStmtsResp serverpb.StatementsResponse

		// Hit query endpoint.
		urlPath := fmt.Sprintf("combinedstmts?start=%d", stubTime.Unix())
		require.NoError(t, srvtestutils.GetStatusJSONProtoWithAdminOption(srv, urlPath, &httpStmtsResp, false))
		require.Equal(t, "crdb_internal.statement_activity", httpStmtsResp.StmtsSourceTable)
		require.Equal(t, "crdb_internal.transaction_activity", httpStmtsResp.TxnsSourceTable)

		maxRows := topLimit * 6 // Number of top columns to select from.
		row := db.QueryRow(t,
			`SELECT count_rows() FROM system.public.transaction_activity WHERE app_name LIKE 'TestSqlActivityUpdateTopLimitJob%'`)
		var count int
		row.Scan(&count)
		require.LessOrEqual(t, count, maxRows, "transaction_activity after transfer: actual:%d, max:%d", count, maxRows)

		row = db.QueryRow(t,
			`SELECT count_rows() FROM system.public.statement_activity WHERE app_name LIKE 'TestSqlActivityUpdateTopLimitJob%'`)
		row.Scan(&count)
		require.LessOrEqual(t, count, maxRows, "statement_activity after transfer: actual:%d, max:%d", count, maxRows)

		row = db.QueryRow(t, `SELECT count_rows() FROM system.public.transaction_activity`)
		row.Scan(&count)
		require.LessOrEqual(t, count, maxRows, "transaction_activity after transfer: actual:%d, max:%d", count, maxRows)

		// Verify that if the transaction is in the transaction_activity table that
		// all the stmts for that transaction are in the statement_activity table.
		// This is necessary for the UI on the transaction details page to show
		// the necessary information.
		var stmtIDs [2]string
		rows := db.Query(t, `SELECT json_array_elements_text(metadata->'stmtFingerprintIDs') FROM system.public.transaction_activity where app_name = 'topTransaction' AND json_array_length(metadata->'stmtFingerprintIDs') = 2`)
		defer rows.Close()
		stmtIdCnt := 0
		for rows.Next() {
			require.Less(t, stmtIdCnt, 2)
			require.NoError(t, rows.Scan(&stmtIDs[stmtIdCnt]))
			stmtIdCnt++
		}

		require.Equal(t, 2, stmtIdCnt, "transaction_activity should have 2 stmts ids: actual:%d", stmtIdCnt)
		// Don't check if the statements are on statements_activity
		// because we no longer force them to be there.

		var txnTotalClusterExecutionSeconds float64
		row = db.QueryRow(t, `SELECT sum(
             (statistics -> 'statistics' -> 'svcLat' ->> 'mean')::FLOAT *
             (statistics -> 'statistics' ->> 'cnt')::FLOAT
          ) FROM system.public.transaction_statistics`)
		row.Scan(&txnTotalClusterExecutionSeconds)
		require.Greater(t, txnTotalClusterExecutionSeconds, float64(0), "transaction_statistics txnTotalClusterExecutionSeconds should be greater than 0: %d", txnTotalClusterExecutionSeconds)
		require.Equal(t, httpStmtsResp.TxnsTotalRuntimeSecs, float32(txnTotalClusterExecutionSeconds))

		var stmtTotalClusterExecutionSeconds float64
		row = db.QueryRow(t, `SELECT sum(
             (statistics -> 'statistics' -> 'svcLat' ->> 'mean')::FLOAT *
             (statistics -> 'statistics' ->> 'cnt')::FLOAT
          ) FROM system.public.statement_statistics`)
		row.Scan(&stmtTotalClusterExecutionSeconds)
		require.Greater(t, stmtTotalClusterExecutionSeconds, float64(0), "statement_statistics stmtTotalClusterExecutionSeconds should be greater than 0: %d", stmtTotalClusterExecutionSeconds)
		require.Equal(t, httpStmtsResp.StmtsTotalRuntimeSecs, float32(stmtTotalClusterExecutionSeconds))

		func() {
			var txnTotalClusterExecutionSecondsActivityTbl float64
			rows := db.Query(t, `SELECT distinct(execution_total_cluster_seconds) FROM system.public.transaction_activity`)
			require.NoError(t, rows.Err())
			defer rows.Close()
			distinctCount := 0
			for rows.Next() {
				distinctCount++
				require.NoError(t, rows.Scan(&txnTotalClusterExecutionSecondsActivityTbl))
				require.Equal(t, txnTotalClusterExecutionSeconds, txnTotalClusterExecutionSecondsActivityTbl)
			}
			require.Equal(t, 1, distinctCount)
		}()

		func() {
			var stmtTotalClusterExecutionSecondsActivityTbl float64
			rows := db.Query(t, `SELECT distinct(execution_total_cluster_seconds) FROM system.public.statement_activity`)
			require.NoError(t, rows.Err())
			defer rows.Close()
			distinctCount := 0
			for rows.Next() {
				distinctCount++
				require.NoError(t, rows.Scan(&stmtTotalClusterExecutionSecondsActivityTbl))
				require.Equal(t, stmtTotalClusterExecutionSeconds, stmtTotalClusterExecutionSecondsActivityTbl)
			}

			require.Equal(t, 1, distinctCount)
		}()

	}
}

// TestSqlActivityJobRunsAfterStatsFlush verifies that the
// correct data is updated on current and prior hour when new stats are
// added to either or both of them.
func TestSqlActivityJobRunsAfterStatsFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start the cluster.
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Insecure: true,
		Settings: st,
		Knobs: base.TestingKnobs{
			SQLStatsKnobs:    sqlstats.CreateTestingKnobs(),
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(context.Background())
	defer db.Close()

	_, err := db.ExecContext(ctx, `SET CLUSTER SETTING sql.stats.activity.flush.enabled = true;`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "SET CLUSTER SETTING sql.stats.flush.interval = '100ms'")
	require.NoError(t, err)
	appName := "TestScheduledSQLStatsCompaction"
	_, err = db.ExecContext(ctx, "SET SESSION application_name=$1", appName)
	require.NoError(t, err)

	testutils.SucceedsWithin(t, func() error {
		_, err = db.ExecContext(ctx, "SELECT 1;")
		require.NoError(t, err)

		row := db.QueryRowContext(ctx, "SELECT count_rows() "+
			"FROM system.public.transaction_activity WHERE app_name = $1", appName)
		var count int
		err = row.Scan(&count)
		if err != nil {
			return err
		}
		if count <= 0 {
			return fmt.Errorf("transaction_activity is empty: %d", count)
		}

		row = db.QueryRowContext(ctx, "SELECT count_rows() "+
			"FROM system.public.statement_activity WHERE app_name = $1", appName)
		err = row.Scan(&count)
		if err != nil {
			return err
		}
		if count <= 0 {
			return fmt.Errorf("statement_activity is empty: %d", count)
		}

		return nil
	}, 1*time.Minute)
}

// TestTransactionActivityMetadata verifies the metadata JSON column of system.transaction_activity are
// what we expect it to be. This test was added to address #103618.
func TestTransactionActivityMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stubTime := timeutil.Now().Truncate(time.Hour)
	sqlStatsKnobs := sqlstats.CreateTestingKnobs()
	sqlStatsKnobs.StubTimeNow = func() time.Time { return stubTime }
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Insecure: true,
		Knobs: base.TestingKnobs{
			SQLStatsKnobs: sqlStatsKnobs,
			UpgradeManager: &upgradebase.TestingKnobs{
				DontUseJobs:                       true,
				SkipUpdateSQLActivityJobBootstrap: true,
			},
		},
	})
	defer s.Stopper().Stop(context.Background())
	defer sqlDB.Close()
	ts := s.ApplicationLayer()

	execCfg := ts.ExecutorConfig().(ExecutorConfig)
	st := cluster.MakeTestingClusterSettings()
	updater := newSqlActivityUpdater(st, execCfg.InternalDB, sqlStatsKnobs)

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, `SET CLUSTER SETTING sql.stats.activity.flush.enabled = true;`)
	db.Exec(t, "SET SESSION application_name = 'test_txn_activity_table'")

	// Generate some sql stats data.
	db.Exec(t, "SELECT 1;")

	// Flush and transfer stats.
	var metadataJSON string
	ts.SQLServer().(*Server).GetSQLStatsProvider().MaybeFlush(ctx, s.AppStopper())

	// Ensure that the metadata column contains the populated 'stmtFingerprintIDs' field.
	var metadata struct {
		StmtFingerprintIDs []string `json:"stmtFingerprintIDs,"`
	}

	require.NoError(t, updater.TransferStatsToActivity(ctx))
	db.QueryRow(t, "SELECT metadata FROM system.public.transaction_activity LIMIT 1").Scan(&metadataJSON)
	require.NoError(t, json.Unmarshal([]byte(metadataJSON), &metadata))
	require.NotEmpty(t, metadata.StmtFingerprintIDs)

	// Do the same but testing transferTopStats.
	db.Exec(t, "SELECT crdb_internal.reset_sql_stats()")
	db.Exec(t, "SELECT 1")

	// Flush and transfer top stats.
	ts.SQLServer().(*Server).GetSQLStatsProvider().MaybeFlush(ctx, s.AppStopper())
	require.NoError(t, updater.transferTopStats(ctx, stubTime, 100, 100, 100))

	// Ensure that the metadata column contains the populated 'stmtFingerprintIDs' field.
	db.QueryRow(t, "SELECT metadata FROM system.public.transaction_activity LIMIT 1").Scan(&metadataJSON)
	require.NoError(t, json.Unmarshal([]byte(metadataJSON), &metadata))
	require.NotEmpty(t, metadata.StmtFingerprintIDs)
}

// Verify the cluster setting ignores activity tables when disabled
// 1. Changes app name and execute 2 queries (select _, change app name)
// 2. Check results include the app which should be from activity tables
// 3. Change the app names in the activity tables
// 4. Verify original app name no longer found
// 5. Disable cluster setting
// 6. Verify original app name in result from statistics table
func TestActivityStatusCombineAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stubTime := timeutil.Now().Truncate(time.Hour)
	sqlStatsKnobs := sqlstats.CreateTestingKnobs()
	sqlStatsKnobs.StubTimeNow = func() time.Time { return stubTime }
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Insecure: true,
		Knobs: base.TestingKnobs{
			SQLStatsKnobs: sqlStatsKnobs,
			UpgradeManager: &upgradebase.TestingKnobs{
				DontUseJobs:                       true,
				SkipUpdateSQLActivityJobBootstrap: true,
			},
		},
	})
	defer s.Stopper().Stop(context.Background())
	defer sqlDB.Close()
	ts := s.ApplicationLayer()

	execCfg := ts.ExecutorConfig().(ExecutorConfig)
	st := cluster.MakeTestingClusterSettings()
	updater := newSqlActivityUpdater(st, execCfg.InternalDB, sqlStatsKnobs)

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, `SET CLUSTER SETTING sql.stats.activity.flush.enabled = true;`)
	// Generate a random app name each time to avoid conflicts
	appName := "test_status_api" + uuid.MakeV4().String()
	db.Exec(t, "SET SESSION application_name = $1", appName)

	// Generate some sql stats data.
	db.Exec(t, "SELECT 1;")

	// Switch the app name back so any queries ran after do not get included
	db.Exec(t, "SET SESSION application_name = '$ internal-test'")

	// Flush and transfer stats.
	var metadataJSON string
	ts.SQLServer().(*Server).GetSQLStatsProvider().MaybeFlush(ctx, s.AppStopper())

	// Ensure that the metadata column contains the populated 'stmtFingerprintIDs' field.
	var metadata struct {
		StmtFingerprintIDs []string `json:"stmtFingerprintIDs,"`
	}

	require.NoError(t, updater.TransferStatsToActivity(ctx))
	db.QueryRow(t, "SELECT metadata FROM system.public.transaction_activity LIMIT 1").Scan(&metadataJSON)
	require.NoError(t, json.Unmarshal([]byte(metadataJSON), &metadata))
	require.NotEmpty(t, metadata.StmtFingerprintIDs)

	// Hit query endpoint.
	start := stubTime
	end := stubTime
	var resp serverpb.StatementsResponse
	if err := getStatusJSONProto(s, "combinedstmts", &resp, start, end); err != nil {
		t.Fatal(err)
	}
	require.NotEmpty(t, resp.Transactions)
	require.NotEmpty(t, resp.Statements)

	stmtAppNameCnt := getStmtAppNameCount(resp, appName)
	require.Greater(t, stmtAppNameCnt, 0)

	txnAppNameCnt := getTxnAppNameCnt(resp, appName)
	require.Greater(t, txnAppNameCnt, 0)

	// Grant permission and change the activity table info
	db.Exec(t, "INSERT INTO system.users VALUES ('node', NULL, true, 3)")
	db.Exec(t, "GRANT node TO root")
	db.Exec(t, "UPDATE system.public.statement_activity SET app_name = 'randomapp' where app_name = $1;", appName)
	db.Exec(t, "UPDATE system.public.transaction_activity SET app_name = 'randomapp' where app_name = $1;", appName)

	if err := getStatusJSONProto(s, "combinedstmts", &resp, start, end); err != nil {
		t.Fatal(err)
	}
	// Verify the activity table changes caused the response to change.
	require.NotEmpty(t, resp.Transactions)
	require.NotEmpty(t, resp.Statements)
	appNameChangedStmtCnt := getStmtAppNameCount(resp, appName)
	require.Equal(t, 0, appNameChangedStmtCnt)

	appNameChangedTxnCnt := getTxnAppNameCnt(resp, appName)
	require.Equal(t, 0, appNameChangedTxnCnt)

	// Disable the activity ui cluster setting so it only pull from stats tables
	db.Exec(t, "set cluster setting sql.stats.activity.ui.enabled = false;")

	if err := getStatusJSONProto(s, "combinedstmts", &resp, start, end); err != nil {
		t.Fatal(err)
	}
	require.NotEmpty(t, resp.Transactions)
	require.NotEmpty(t, resp.Statements)

	// These should be the same as the original results.
	uiDisabledStmtAppNameCnt := getStmtAppNameCount(resp, appName)
	require.Equal(t, stmtAppNameCnt, uiDisabledStmtAppNameCnt)
	uiDisabledTxnAppNameCnt := getTxnAppNameCnt(resp, appName)
	require.Equal(t, txnAppNameCnt, uiDisabledTxnAppNameCnt)

	// Enable the activity ui cluster setting
	db.Exec(t, "set cluster setting sql.stats.activity.ui.enabled = true;")

	// Validate same results after the setting is enabled again.
	if err := getStatusJSONProto(s, "combinedstmts", &resp, start, end); err != nil {
		t.Fatal(err)
	}
	require.NotEmpty(t, resp.Transactions)
	require.NotEmpty(t, resp.Statements)
	require.Equal(t, 0, appNameChangedStmtCnt)
	require.Equal(t, 0, appNameChangedTxnCnt)

	// Update all the activity table aggregated_ts to an old time. This allows
	// the check to see if the activity table has data to pass, and causes the
	// current time frame to have no data.
	db.Exec(t, "UPDATE system.public.statement_activity SET aggregated_ts = '2021-09-12 13:00:00.000000 +00:00' where 1=1;")
	db.Exec(t, "UPDATE system.public.transaction_activity SET aggregated_ts = '2021-09-12 13:00:00.000000 +00:00' where 1=1;")

	if err := getStatusJSONProto(s, "combinedstmts", &resp, start, end); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, "crdb_internal.transaction_statistics_persisted", resp.TxnsSourceTable)
	require.Equal(t, "crdb_internal.statement_statistics_persisted", resp.StmtsSourceTable)
	require.NotEmpty(t, resp.Transactions)
	require.NotEmpty(t, resp.Statements)
	require.Greater(t, resp.StmtsTotalRuntimeSecs, float32(0))
	require.Greater(t, resp.TxnsTotalRuntimeSecs, float32(0))
}

type timeMutex struct {
	syncutil.RWMutex
	stubTime time.Time
}

func (mu *timeMutex) setStubTime(time time.Time) {
	mu.Lock()
	defer mu.Unlock()
	mu.stubTime = time
}

func TestFlushToActivityWithDifferentAggTs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var muStubTime timeMutex
	muStubTime.setStubTime(timeutil.Now().Truncate(time.Hour))

	sqlStatsKnobs := sqlstats.CreateTestingKnobs()
	sqlStatsKnobs.StubTimeNow = func() time.Time {
		muStubTime.RLock()
		defer muStubTime.RUnlock()
		return muStubTime.stubTime
	}

	// Start the cluster.
	// Disable the job since it is called manually from a new instance to avoid
	// any race conditions.
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Insecure: true,
		Knobs: base.TestingKnobs{
			SQLStatsKnobs: sqlStatsKnobs,
			UpgradeManager: &upgradebase.TestingKnobs{
				DontUseJobs:                       true,
				SkipUpdateSQLActivityJobBootstrap: true,
			}}})
	defer srv.Stopper().Stop(context.Background())
	defer sqlDB.Close()
	ts := srv.ApplicationLayer()

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, `SET CLUSTER SETTING sql.stats.activity.flush.enabled = true;`)

	// Start with empty activity tables.
	execCfg := ts.ExecutorConfig().(ExecutorConfig)
	st := cluster.MakeTestingClusterSettings()
	updater := newSqlActivityUpdater(st, execCfg.InternalDB, sqlStatsKnobs)
	require.NoError(t, updater.TransferStatsToActivity(ctx))
	verifyActivityTablesAreEmpty(t, db)

	// Use random name to keep isolated during stress tests.
	rng, _ := randutil.NewTestRand()
	appName := fmt.Sprintf("TestFlushToActivityWithDifferentAggTs-%d", rng.Int())

	datadriven.RunTest(t, "testdata/sql_activity_update_job", func(t *testing.T, d *datadriven.TestData) string {
		var buf bytes.Buffer
		timeLayout := "2006-01-02 15:04:05"
		switch d.Cmd {
		case "update-time":
			for _, arg := range d.CmdArgs {
				switch arg.Key {
				case "time":
					time, err := time.Parse(timeLayout, arg.Vals[0])
					require.NoError(t, err)
					muStubTime.setStubTime(timeutil.FromUnixNanos(time.UnixNano()))
				}
			}
		case "update-app":
			for _, arg := range d.CmdArgs {
				switch arg.Key {
				case "ignore":
					useIgnoreApp, err := strconv.ParseBool(arg.Vals[0])
					require.NoError(t, err)
					if useIgnoreApp {
						db.Exec(t, "SET SESSION application_name=$1", "randomIgnore")
					} else {
						db.Exec(t, "SET SESSION application_name=$1", appName)
					}
				}
			}
		case "run-sql":
			useAppName := false
			var rows [][]string
			var err error
			for _, arg := range d.CmdArgs {
				switch arg.Key {
				case "useApp":
					useAppName, err = strconv.ParseBool(arg.Vals[0])
					require.NoError(t, err)
				}
			}
			if useAppName {
				rows = db.QueryStr(t, d.Input, appName)
			} else {
				rows = db.QueryStr(t, d.Input)
			}

			for _, row := range rows {
				fmt.Fprintf(&buf, "%s\n", strings.Join(row, ","))
			}
		case "flush-stats":
			ts.SQLServer().(*Server).GetSQLStatsProvider().MaybeFlush(ctx, srv.AppStopper())
		case "update-top-activity":
			// Populate the Top Activity. This will use the transfer all scenarios
			// with there only being a few rows.
			require.NoError(t, updater.TransferStatsToActivity(ctx))
		}

		return buf.String()
	})
}

// duplicateRowHelper duplicates a single row in each statistics table, but slightly
// changes non-primary key fields to make sure it doesn't cause a conflict that
// breaks upsert because multiple rows have same primary key.
func duplicateRowHelper(t *testing.T, db *sqlutils.SQLRunner, appName string) {
	// primary key for activity table: aggregated_ts, fingerprint_id, app_name
	db.Exec(t, `INSERT INTO system.transaction_statistics (aggregated_ts, fingerprint_id, app_name, node_id, agg_interval, metadata, statistics)
			 (SELECT aggregated_ts, fingerprint_id, app_name, 42, '00:00:01'::INTERVAL, metadata,  statistics FROM system.transaction_statistics where app_name = $1 limit 1)
		`, appName)

	// primary key for activity table: aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name
	db.Exec(t, `INSERT INTO system.statement_statistics (aggregated_ts, fingerprint_id, transaction_fingerprint_id, app_name, node_id, agg_interval, plan_hash, metadata, statistics)
			 (SELECT aggregated_ts, fingerprint_id, transaction_fingerprint_id, app_name, 42, '00:00:01'::INTERVAL, plan_hash, metadata, jsonb_set(statistics, '{index_recommendations}', to_jsonb('["creation : dummy value"]'))  FROM system.statement_statistics where app_name = $1 limit 1)
		`, appName)
}

func verifyActivityTablesAreEmpty(t *testing.T, db *sqlutils.SQLRunner) {
	var count int
	row := db.QueryRow(t, "SELECT count_rows() FROM system.public.transaction_activity")
	row.Scan(&count)
	require.Zero(t, count, "system.transaction_activity after transfer: expect:0, actual:%d", count)

	row = db.QueryRow(t, "SELECT count_rows() FROM system.public.statement_activity")
	row.Scan(&count)
	require.Zero(t, count, "system.statement_activity after transfer: expect:0, actual:%d", count)

	row = db.QueryRow(t, "SELECT count_rows() FROM crdb_internal.transaction_activity")
	row.Scan(&count)
	require.Zero(t, count, "crdb_internal.transaction_activity after transfer: expect:0, actual:%d", count)

	row = db.QueryRow(t, "SELECT count_rows() FROM crdb_internal.statement_activity")
	row.Scan(&count)
	require.Zero(t, count, "crdb_internal.statement_activity after transfer: expect:0, actual:%d", count)
}

func verifyActivityTableContentHelper(t *testing.T, db *sqlutils.SQLRunner, appName string) {
	txnTables := []string{"system.public.transaction_activity", "crdb_internal.transaction_activity"}
	for _, table := range txnTables {
		query := fmt.Sprintf(`SELECT count_rows() 
		FROM system.public.transaction_statistics ts
		INNER JOIN	(SELECT * FROM %s) ta using (fingerprint_id, app_name)
		WHERE app_name = $1 AND
			ta.execution_count = ts.execution_count AND
			ta.execution_total_seconds = ts.total_estimated_execution_time AND
			ta.contention_time_avg_seconds  = ts.contention_time AND
			ta.cpu_sql_avg_nanos = ts.cpu_sql_nanos AND      
			ta.service_latency_avg_seconds = ts.service_latency AND
			ta.statistics = ts.statistics AND
			ta.metadata = ts.metadata`, table)
		row := db.QueryRow(t, query, appName)
		var count int
		row.Scan(&count)
		require.Equal(t, 1, count, "%s after transfer: expect:1, actual:%d, query: %s", table, count, query)
	}

	stmtTables := []string{"system.public.statement_activity", "crdb_internal.statement_activity"}
	for _, table := range stmtTables {
		query := fmt.Sprintf(`SELECT count_rows() 
		FROM system.public.statement_statistics ss
		INNER JOIN	(SELECT * FROM %s) sa using (fingerprint_id, app_name)
		WHERE app_name = $1 AND
			sa.execution_count = ss.execution_count AND
			sa.execution_total_seconds = ss.total_estimated_execution_time AND
			sa.contention_time_avg_seconds  = ss.contention_time AND
			sa.cpu_sql_avg_nanos = ss.cpu_sql_nanos AND      
			sa.service_latency_avg_seconds = ss.service_latency AND
			sa.statistics = ss.statistics`, table)
		row := db.QueryRow(t, query, appName)
		var count int
		row.Scan(&count)
		require.Equal(t, 1, count, "%s after transfer: expect:1, actual:%d, query: %s", table, count, query)

		// Metadata objects are changed because it's an aggregate.
		query = fmt.Sprintf(`SELECT count_rows() 
		FROM (select fingerprint_id, app_name, merge_stats_metadata(metadata) AS metadata FROM system.public.statement_statistics GROUP BY fingerprint_id, app_name) ss
		INNER JOIN	(SELECT * FROM %s) sa using (fingerprint_id, app_name)
		WHERE app_name = $1 AND
		      sa.metadata = ss.metadata`, table)
		row = db.QueryRow(t, query, appName)
		row.Scan(&count)
		require.Equal(t, 1, count, "%s after transfer metadata: expect:1, actual:%d, query: %s", table, count, query)
	}
}

func verifyTopActivityTableContentHelper(t *testing.T, db *sqlutils.SQLRunner, limitCnt int) {
	txnTables := []string{"system.public.transaction_activity", "crdb_internal.transaction_activity"}
	for _, table := range txnTables {
		topColumnNames := []string{" (statistics->'statistics'->>'cnt')::int ",
			" ((statistics->'statistics'->>'cnt')::float)*((statistics->'statistics'->'svcLat'->>'mean')::float) ",
			" COALESCE((statistics->'execution_statistics'->'contentionTime'->>'mean')::float,0) ",
			" COALESCE((statistics->'execution_statistics'->'cpuSQLNanos'->>'mean')::float,0) ",
			" (statistics->'statistics'->'svcLat'->>'mean')::float "}
		for _, column := range topColumnNames {
			query := fmt.Sprintf(`SELECT count_rows()
		FROM 
		    (select * from (select 
		                        aggregated_ts,
		                        fingerprint_id,
		                        app_name,
		                        max(metadata) AS max_metadata,
		                        merge_transaction_stats(statistics) as statistics
					from system.public.transaction_statistics 
						where app_name not like '$ internal%%' and app_name != 'randomIgnore'
						group by aggregated_ts, fingerprint_id, app_name)
				order by %s limit %d) ts
		LEFT JOIN	(SELECT * FROM %s ) ta using (aggregated_ts, fingerprint_id, app_name)
		WHERE ta.fingerprint_id != ts.fingerprint_id OR
			ta.statistics != ts.statistics`, column, limitCnt, table)
			row := db.QueryRow(t, query)

			var count int
			row.Scan(&count)
			require.Zero(t, count, "%s after transfer: expect:0, actual:%d, query: %s", table, count, query)

			if strings.Contains(column, "'statistics'") {
				query = fmt.Sprintf(`SELECT sum(%s :: float) FROM  %s`, column, table)
				row = db.QueryRow(t, query)
				var totalColumnValue float64
				row.Scan(&totalColumnValue)
				require.Greater(t, totalColumnValue, float64(0), "%s after transfer: expect:1, actual:%d, query: %s", table, totalColumnValue, query)
			}
		}
	}

	stmtTables := []string{"system.public.statement_activity", "crdb_internal.statement_activity"}
	for _, table := range stmtTables {
		topColumnNames := []string{" (statistics->'statistics'->>'cnt')::int ",
			" ((statistics->'statistics'->>'cnt')::float)*((statistics->'statistics'->'svcLat'->>'mean')::float) ",
			" COALESCE((statistics->'execution_statistics'->'contentionTime'->>'mean')::float,0) ",
			" COALESCE((statistics->'execution_statistics'->'cpuSQLNanos'->>'mean')::float,0) ",
			" (statistics->'statistics'->'svcLat'->>'mean')::float ",
			" COALESCE((statistics -> 'statistics' -> 'latencyInfo' ->> 'p99')::float, 0)"}

		for _, column := range topColumnNames {
			query := fmt.Sprintf(`SELECT count_rows()
		FROM 
		    (select * from (select 
		                        aggregated_ts,
		                        fingerprint_id,
		                        app_name,
		                        merge_stats_metadata(metadata)    AS merged_metadata,
		                        merge_statement_stats(statistics) as statistics
					from system.public.statement_statistics 
						where app_name not like '$ internal%%' and app_name != 'randomIgnore'
						group by aggregated_ts, fingerprint_id, app_name)
				order by %s limit %d) ts
		LEFT JOIN	(SELECT * FROM %s ) ta using (aggregated_ts, fingerprint_id, app_name)
		WHERE ta.fingerprint_id != ts.fingerprint_id OR
			ta.statistics != ts.statistics OR
		  ta.metadata != ts.merged_metadata `, column, limitCnt, table)
			row := db.QueryRow(t, query)

			var count int
			row.Scan(&count)
			require.Zero(t, count, "%s after transfer: expect:0, actual:%d, query: %s", table, count, query)

			if strings.Contains(column, "'statistics'") && !strings.Contains(column, "'p99'") {
				query = fmt.Sprintf(`SELECT sum(%s :: float) FROM  %s`, column, table)
				row = db.QueryRow(t, query)
				var totalColumnValue float64
				row.Scan(&totalColumnValue)
				require.Greater(t, totalColumnValue, float64(0), "%s after transfer: expect:1, actual:%d, query: %s", table, totalColumnValue, query)
			}
		}
	}
}

func getTxnAppNameCnt(resp serverpb.StatementsResponse, appName string) int {
	txnAppNameCnt := 0
	for _, txn := range resp.Transactions {
		if txn.StatsData.App == appName {
			txnAppNameCnt++
		}
	}
	return txnAppNameCnt
}

func getStmtAppNameCount(resp serverpb.StatementsResponse, appName string) int {
	stmtAppNameCnt := 0
	for _, stmt := range resp.Statements {
		if stmt.Key.KeyData.App == appName {
			stmtAppNameCnt++
		}
	}
	return stmtAppNameCnt
}

func getStatusJSONProto(
	ts serverutils.TestServerInterface,
	path string,
	response protoutil.Message,
	startTime time.Time,
	endTime time.Time,
) error {
	url := fmt.Sprintf("/_status/%s?start=%d&end=%d", path, startTime.Unix(), endTime.Unix())
	return serverutils.GetJSONProto(ts, url, response)
}
