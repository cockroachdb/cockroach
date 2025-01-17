// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// Package sqlstats is a subsystem that is responsible for tracking the
// statistics of statements and transactions.

package persistedsqlstats_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestPersistedSQLStatsReset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStress(t, "the test is too slow to run under stress")

	ctx := context.Background()
	cluster := serverutils.StartCluster(t, 3 /* numNodes */, base.TestClusterArgs{})
	defer cluster.Stopper().Stop(ctx)
	server := cluster.Server(0 /* idx */).ApplicationLayer()

	// Open two connections so that we can run statements without messing up
	// the SQL stats.
	testConn := server.SQLConn(t)
	observerConn := cluster.Server(1).ApplicationLayer().SQLConn(t)

	sqlDB := sqlutils.MakeSQLRunner(testConn)
	observer := sqlutils.MakeSQLRunner(observerConn)

	testCasesForDisk := map[string]string{
		"SELECT _":       "SELECT 1",
		"SELECT _, _":    "SELECT 1, 1",
		"SELECT _, _, _": "SELECT 1, 1, 1",
	}

	testCasesForMem := map[string]string{
		"SELECT _, _":          "SELECT 1, 1",
		"SELECT _, _, _":       "SELECT 1, 1, 1",
		"SELECT _, _, _, _":    "SELECT 1, 1, 1, 1",
		"SELECT _, _, _, _, _": "SELECT 1, 1, 1, 1, 1",
	}

	appName := "controller_test"
	sqlDB.Exec(t, "SET application_name = $1", appName)

	expectedStmtFingerprintToFingerprintID := make(map[string]string)
	for fingerprint, query := range testCasesForDisk {
		// We will populate the fingerprint ID later.
		expectedStmtFingerprintToFingerprintID[fingerprint] = ""
		sqlDB.Exec(t, query)
	}

	sqlStats := server.SQLServer().(*sql.Server).GetSQLStatsProvider()
	sqlStats.MaybeFlush(ctx, cluster.ApplicationLayer(0).AppStopper())

	checkInsertedStmtStatsAndUpdateFingerprintIDs(t, appName, observer, expectedStmtFingerprintToFingerprintID)
	checkInsertedTxnStats(t, appName, observer, expectedStmtFingerprintToFingerprintID)

	// Run few additional queries, so we would also have some SQL stats in-memory.
	for fingerprint, query := range testCasesForMem {
		sqlDB.Exec(t, query)
		if _, ok := expectedStmtFingerprintToFingerprintID[fingerprint]; !ok {
			expectedStmtFingerprintToFingerprintID[fingerprint] = ""
		}
	}

	// Sanity check that we still have the same count since we are still within
	// the same aggregation interval.
	checkInsertedStmtStatsAndUpdateFingerprintIDs(t, appName, observer, expectedStmtFingerprintToFingerprintID)
	checkInsertedTxnStats(t, appName, observer, expectedStmtFingerprintToFingerprintID)

	// Resets cluster wide SQL stats.
	sqlStatsController := server.SQLServer().(*sql.Server).GetSQLStatsController()
	require.NoError(t, sqlStatsController.ResetClusterSQLStats(ctx))

	var count int
	observer.QueryRow(t,
		"SELECT count(*) FROM crdb_internal.statement_statistics WHERE app_name = $1", appName).
		Scan(&count)
	require.Equal(t, 0 /* expected */, count)

	observer.QueryRow(t,
		"SELECT count(*) FROM crdb_internal.transaction_statistics WHERE app_name = $1", appName).
		Scan(&count)
	require.Equal(t, 0 /* expected */, count)
}

func checkInsertedStmtStatsAndUpdateFingerprintIDs(
	t *testing.T,
	appName string,
	observer *sqlutils.SQLRunner,
	expectedStmtFingerprintToFingerprintID map[string]string,
) {
	result := observer.QueryStr(t,
		`
SELECT encode(fingerprint_id, 'hex'), metadata ->> 'query'
FROM crdb_internal.statement_statistics
WHERE app_name = $1`, appName)

	for expectedFingerprint := range expectedStmtFingerprintToFingerprintID {
		var found bool
		for _, row := range result {
			if expectedFingerprint == row[1] {
				found = true

				// Populate fingerprintID.
				expectedStmtFingerprintToFingerprintID[expectedFingerprint] = row[0]
			}
		}
		require.True(t, found, "expect %s to be found, but it was not", expectedFingerprint)
	}
}

func checkInsertedTxnStats(
	t *testing.T,
	appName string,
	observer *sqlutils.SQLRunner,
	expectedStmtFingerprintToFingerprintID map[string]string,
) {
	result := observer.QueryStr(t,
		`
SELECT metadata -> 'stmtFingerprintIDs' ->> 0
FROM crdb_internal.transaction_statistics
WHERE app_name = $1
 AND metadata -> 'stmtFingerprintIDs' ->> 0 IN (
   SELECT encode(fingerprint_id, 'hex')
   FROM crdb_internal.statement_statistics
   WHERE app_name = $2
 )
`, appName, appName)
	for query, fingerprintID := range expectedStmtFingerprintToFingerprintID {
		var found bool
		for _, row := range result {
			if row[0] == fingerprintID {
				found = true
			}
		}
		require.True(t, found,
			`expected %s to be found in txn stats, but it was not.`, query)
	}
}

func TestActivityTablesReset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	sqlDB := sqlutils.MakeSQLRunner(db)
	defer s.Stopper().Stop(context.Background())

	// Disable sql activity flush job.
	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.activity.flush.enabled = false`)

	testutils.SucceedsSoon(t, func() error {
		var enabled bool
		sqlDB.QueryRow(t,
			"SHOW CLUSTER SETTING sql.stats.activity.flush.enabled",
		).Scan(&enabled)
		if enabled == true {
			return errors.Newf("waiting for sql activity job to be disabled")
		}
		return nil
	})

	// Give the query runner privilege to insert into the activity tables.
	sqlDB.Exec(t, "INSERT INTO system.users VALUES ('node', NULL, true, 3)")
	sqlDB.Exec(t, "GRANT node TO root")

	// Insert into system.statement_activity table
	sqlDB.Exec(t, `
		INSERT INTO system.public.statement_activity (aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name,
                                       agg_interval, metadata, statistics, plan, index_recommendations, execution_count,
                                       execution_total_seconds, execution_total_cluster_seconds,
                                       contention_time_avg_seconds,
                                       cpu_sql_avg_nanos,
                                       service_latency_avg_seconds, service_latency_p99_seconds)
		VALUES (
			'2023-06-29 15:00:00+00',
			'\x125167e869920859',
			'\xbd32daa4ef93bf86',
			'\x0fa54115f7caf3e6',
			'activity_tables_reset',
			'01:00:00',
			'{"db": "", "distsql": false, "failed": false, "fullScan": false, "implicitTxn": true, "query": "SELECT id FROM system.jobs", "querySummary": "SELECT id FROM system.jobs", "stmtType": "TypeDML", "vec": true}',
			'{"execution_statistics": {"cnt": 1, "contentionTime": {"mean": 0, "sqDiff": 0}, "cpuSQLNanos": {"mean": 133623, "sqDiff": 0}, "maxDiskUsage": {"mean": 0, "sqDiff": 0}, "maxMemUsage": {"mean": 3.072E+4, "sqDiff": 0}, "mvccIteratorStats": {"blockBytes": {"mean": 28086, "sqDiff": 0}, "blockBytesInCache": {"mean": 0, "sqDiff": 0}, "keyBytes": {"mean": 0, "sqDiff": 0}, "pointCount": {"mean": 6, "sqDiff": 0}, "pointsCoveredByRangeTombstones": {"mean": 0, "sqDiff": 0}, "rangeKeyContainedPoints": {"mean": 0, "sqDiff": 0}, "rangeKeyCount": {"mean": 0, "sqDiff": 0}, "rangeKeySkippedPoints": {"mean": 0, "sqDiff": 0}, "seekCount": {"mean": 2, "sqDiff": 0}, "seekCountInternal": {"mean": 2, "sqDiff": 0}, "stepCount": {"mean": 6, "sqDiff": 0}, "stepCountInternal": {"mean": 6, "sqDiff": 0}, "valueBytes": {"mean": 39, "sqDiff": 0}}, "networkBytes": {"mean": 0, "sqDiff": 0}, "networkMsgs": {"mean": 0, "sqDiff": 0}}, "index_recommendations": [], "statistics": {"bytesRead": {"mean": 432, "sqDiff": 0}, "cnt": 1, "firstAttemptCnt": 1, "idleLat": {"mean": 0, "sqDiff": 0}, "indexes": ["15@4"], "lastErrorCode": "", "lastExecAt": "2023-06-29T15:33:11.042902Z", "latencyInfo": {"max": 0.00142586, "min": 0.00142586, "p50": 0, "p90": 0, "p99": 0}, "maxRetries": 0, "nodes": [3], "numRows": {"mean": 3, "sqDiff": 0}, "ovhLat": {"mean": 0.000012357999999999857, "sqDiff": 0}, "parseLat": {"mean": 0, "sqDiff": 0}, "planGists": ["AgEeCACHDwIAAAMFAgYC"], "planLat": {"mean": 0.000655272, "sqDiff": 0}, "regions": ["us-east1"], "rowsRead": {"mean": 3, "sqDiff": 0}, "rowsWritten": {"mean": 0, "sqDiff": 0}, "runLat": {"mean": 0.00075823, "sqDiff": 0}, "svcLat": {"mean": 0.00142586, "sqDiff": 0}}}',
			'{"Children": [], "Name": ""}',
			'{}',
			1,
			1,
			1,
			1,
			1,
			1,
			1
		)
	`)
	// Insert into system.transaction_activity table
	sqlDB.Exec(t, `
		INSERT INTO system.public.transaction_activity (aggregated_ts, fingerprint_id, app_name, agg_interval, metadata,
 statistics, query, execution_count, execution_total_seconds,
 execution_total_cluster_seconds, contention_time_avg_seconds, 
 cpu_sql_avg_nanos, service_latency_avg_seconds, service_latency_p99_seconds)
		VALUES (
			'2023-06-29 15:00:00+00',
			'\x125167e869920859',
			'activity_tables_reset',
			'01:00:00',
			'{"db": "", "distsql": false, "failed": false, "fullScan": false, "implicitTxn": true, "query": "SELECT id FROM system.jobs", "querySummary": "SELECT id FROM system.jobs", "stmtType": "TypeDML", "vec": true}',
			'{"execution_statistics": {"cnt": 1, "contentionTime": {"mean": 0, "sqDiff": 0}, "cpuSQLNanos": {"mean": 133623, "sqDiff": 0}, "maxDiskUsage": {"mean": 0, "sqDiff": 0}, "maxMemUsage": {"mean": 3.072E+4, "sqDiff": 0}, "mvccIteratorStats": {"blockBytes": {"mean": 28086, "sqDiff": 0}, "blockBytesInCache": {"mean": 0, "sqDiff": 0}, "keyBytes": {"mean": 0, "sqDiff": 0}, "pointCount": {"mean": 6, "sqDiff": 0}, "pointsCoveredByRangeTombstones": {"mean": 0, "sqDiff": 0}, "rangeKeyContainedPoints": {"mean": 0, "sqDiff": 0}, "rangeKeyCount": {"mean": 0, "sqDiff": 0}, "rangeKeySkippedPoints": {"mean": 0, "sqDiff": 0}, "seekCount": {"mean": 2, "sqDiff": 0}, "seekCountInternal": {"mean": 2, "sqDiff": 0}, "stepCount": {"mean": 6, "sqDiff": 0}, "stepCountInternal": {"mean": 6, "sqDiff": 0}, "valueBytes": {"mean": 39, "sqDiff": 0}}, "networkBytes": {"mean": 0, "sqDiff": 0}, "networkMsgs": {"mean": 0, "sqDiff": 0}}, "index_recommendations": [], "statistics": {"bytesRead": {"mean": 432, "sqDiff": 0}, "cnt": 1, "firstAttemptCnt": 1, "idleLat": {"mean": 0, "sqDiff": 0}, "indexes": ["15@4"], "lastErrorCode": "", "lastExecAt": "2023-06-29T15:33:11.042902Z", "latencyInfo": {"max": 0.00142586, "min": 0.00142586, "p50": 0, "p90": 0, "p99": 0}, "maxRetries": 0, "nodes": [3], "numRows": {"mean": 3, "sqDiff": 0}, "ovhLat": {"mean": 0.000012357999999999857, "sqDiff": 0}, "parseLat": {"mean": 0, "sqDiff": 0}, "planGists": ["AgEeCACHDwIAAAMFAgYC"], "planLat": {"mean": 0.000655272, "sqDiff": 0}, "regions": ["us-east1"], "rowsRead": {"mean": 3, "sqDiff": 0}, "rowsWritten": {"mean": 0, "sqDiff": 0}, "runLat": {"mean": 0.00075823, "sqDiff": 0}, "svcLat": {"mean": 0.00142586, "sqDiff": 0}}}',
			'SELECT id FROM system.jobs',
			1,
			1,
			1,
			1,
			1,
			1,
			1
		)
	`)

	// Check that system.{statement|transaction} activity tables both have 1 row.
	var count int
	sqlDB.QueryRow(t, "SELECT count(*) FROM system.statement_activity").Scan(&count)
	require.Equal(t, 1 /* expected */, count)

	sqlDB.QueryRow(t, "SELECT count(*) FROM system.transaction_activity").Scan(&count)
	require.Equal(t, 1 /* expected */, count)

	// Flush the tables.
	sqlDB.QueryRow(t, "SELECT crdb_internal.reset_activity_tables()")

	sqlDB.QueryRow(t, "SELECT count(*) FROM system.statement_activity").Scan(&count)
	require.Equal(t, 0 /* expected */, count)

	sqlDB.QueryRow(t, "SELECT count(*) FROM system.transaction_activity").Scan(&count)
	require.Equal(t, 0 /* expected */, count)
}

func TestInsightsTablesReset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	sqlDB := sqlutils.MakeSQLRunner(db)
	defer s.Stopper().Stop(context.Background())

	// Give the query runner privilege to insert into the insights tables.
	sqlDB.Exec(t, "INSERT INTO system.users VALUES ('node', NULL, true, 3)")
	sqlDB.Exec(t, "GRANT node TO root")

	// Insert into system.statement_execution_insights table
	sqlDB.Exec(t, `
		INSERT INTO system.public.statement_execution_insights (
		                                                        session_id,
		                                                        transaction_id,
		                                                        transaction_fingerprint_id,
		                                                        statement_id,
		                                                        statement_fingerprint_id,
		                                                        problem,
		                                                        causes,
		                                                        query,
		                                                        status,
		                                                        start_time,
		                                                        end_time,
		                                                        full_scan,
		                                                        user_name,
		                                                        app_name,
		                                                        user_priority,
		                                                        database_name,
		                                                        plan_gist,
		                                                        retries,
		                                                        last_retry_reason,
		                                                        execution_node_ids,
		                                                        index_recommendations,
		                                                        implicit_txn,
		                                                        cpu_sql_nanos,
		                                                        error_code,
		                                                        contention_time,
		                                                        contention_info,
		                                                        details
		                                                        )
		VALUES (
		        '178b8f4d507072200000000000000001',
		        '14a07bbc-dfdb-41df-9231-b6235943847d',
		        '\xd98ea7ce7040ab94',
		        '178b8f50ab40d8680000000000000001',
		        '\x125167e869920859',
		        1,
		        '{}',
		        'SELECT a.balance, b.balance FROM insights_workload_table_0 AS a LEFT JOIN insights_workload_table_1 AS b ON a.shared_key = b.shared_key WHERE a.balance < _',
		        1,
		        '2023-10-06 15:47:41',
		        '2023-10-06 15:47:42',
		        't',
		        'root',
		        'insights_tables_reset',
		        'normal',
		        'insights',
		        'AgHmAQIACgAAAAHkAQIACgAAAAMJAgICAAAFBAYE',
		        0,
		        null,
		        '{}',
		        '{"creation : CREATE INDEX ON insights.public.insights_workload_table_0 (balance) STORING (shared_key);"}',
		        't',
		        0,
		        'XXUUU',
		        null,
		        null,
		        '{}'
		)
	`)
	//Insert into system.transaction_execution_insights table
	sqlDB.Exec(t, `
			INSERT INTO system.public.transaction_execution_insights (
			                                                        session_id,
			                                                        transaction_id,
			                                                        transaction_fingerprint_id,
			                                                        stmt_execution_ids,
			                                                        problems,
			                                                        causes,
			                                                        query_summary,
			                                                        status,
			                                                        start_time,
			                                                        end_time,
			                                                        user_name,
			                                                        app_name,
			                                                        user_priority,
			                                                        retries,
			                                                        last_retry_reason,
			                                                        implicit_txn,
			                                                        cpu_sql_nanos,
			                                                        last_error_code,
			                                                        contention_time,
			                                                        contention_info,
			                                                        details
	
	)
			VALUES (
			        '178b8f4d507072200000000000000001',
			        '14a07bbc-dfdb-41df-9231-b6235943847d',
			        '\xd98ea7ce7040ab94',
			        '{"178b8f50ab40d8680000000000000001"}',
			        '{1}',
			        '{}',
			        'SELECT a.balance, b.balance FROM insights_workload_table_0 AS a LEFT JOIN insights_workload_table_1 AS b ON a.shared_key = b.shared_key WHERE a.balance < _',
			        1,
			        '2023-10-06 15:47:41',
			        '2023-10-06 15:47:42',
			        'root',
			        'insights_tables_reset',
			        'normal',
			        0,
			        null,
			        't',
			        0,
			        'XXUUU',
			        null,
			        null,
			        '{}'
			)
		`)

	// Check that system.{statement|transaction}_execution_insights tables both have 1 row.
	var count int
	sqlDB.QueryRow(t, "SELECT count(*) FROM system.statement_execution_insights").Scan(&count)
	require.Equal(t, 1 /* expected */, count)

	sqlDB.QueryRow(t, "SELECT count(*) FROM system.transaction_execution_insights").Scan(&count)
	require.Equal(t, 1 /* expected */, count)

	// Flush the tables.
	sqlDB.QueryRow(t, "SELECT crdb_internal.reset_insights_tables()")

	sqlDB.QueryRow(t, "SELECT count(*) FROM system.statement_execution_insights").Scan(&count)
	require.Equal(t, 0 /* expected */, count)

	sqlDB.QueryRow(t, "SELECT count(*) FROM system.transaction_execution_insights").Scan(&count)
	require.Equal(t, 0 /* expected */, count)
}

func TestStmtStatsEnable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start the cluster. (One node is sufficient; the outliers system is currently in-memory only.)
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	serverutils.SetClusterSetting(t, tc, "sql.metrics.statement_details.enabled", "false")

	sqlConn := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	sqlConn.Exec(t, "SELECT crdb_internal.reset_sql_stats()")

	appName := "TestStmtStatsEnable"
	sqlConn.Exec(t, "SET application_name = $1", appName)

	sqlConn.Exec(t, "SELECT count_rows() FROM crdb_internal.statement_statistics")
	sqlConn.Exec(t, "SELECT count_rows() FROM crdb_internal.transaction_statistics")
	sqlConn.Exec(t, "SELECT count_rows() FROM crdb_internal.statement_statistics WHERE app_name = $1", appName)
	sqlConn.Exec(t, "SELECT count_rows() FROM crdb_internal.transaction_statistics WHERE app_name = $1", appName)

	sqlConn.Exec(t, "SET application_name = $1", "ObserverTestStmtStatsEnable")
	var count int
	sqlConn.QueryRow(t,
		"SELECT count(*) FROM crdb_internal.statement_statistics WHERE app_name = $1", appName).
		Scan(&count)
	require.Equal(t, 0 /* expected */, count)

	sqlConn.QueryRow(t,
		"SELECT count(*) FROM crdb_internal.transaction_statistics WHERE app_name = $1 AND (statistics->'execution_statistics'->>'cnt')::int > 0 ", appName).
		Scan(&count)
	require.Less(t, count, 4, "statement execution stats collection is disabled there should be less than 4 rows. Actual: %d", count)
}
