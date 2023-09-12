// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package integration

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	os.Exit(m.Run())
}

func TestInsightsIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const appName = "TestInsightsIntegration"

	// Start the cluster. (One node is sufficient; the outliers system is currently in-memory only.)
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	args := base.TestClusterArgs{ServerArgs: base.TestServerArgs{Settings: settings}}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)

	// Enable detection by setting a latencyThreshold > 0.
	latencyThreshold := 250 * time.Millisecond
	insights.LatencyThreshold.Override(ctx, &settings.SV, latencyThreshold)

	_, err := conn.ExecContext(ctx, "SET SESSION application_name=$1", appName)
	require.NoError(t, err)

	// See no recorded insights.
	var count int
	var queryText string
	row := conn.QueryRowContext(ctx, "SELECT count(*), coalesce(string_agg(query, ';'),'') "+
		"FROM crdb_internal.cluster_execution_insights where app_name = $1 ", appName)
	err = row.Scan(&count, &queryText)
	require.NoError(t, err)
	require.Equal(t, 0, count, "expect:0, actual:%d, queries:%s", count, queryText)

	queryDelayInSeconds := latencyThreshold.Seconds()
	// Execute a "long-running" statement, running longer than our latencyThreshold.
	_, err = conn.ExecContext(ctx, "SELECT pg_sleep($1)", queryDelayInSeconds)
	require.NoError(t, err)

	// Eventually see one recorded insight.
	testutils.SucceedsWithin(t, func() error {
		row = conn.QueryRowContext(ctx, "SELECT count(*), coalesce(string_agg(query, ';'),'') "+
			"FROM crdb_internal.cluster_execution_insights where app_name = $1 ", appName)
		if err = row.Scan(&count, &queryText); err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("expected 1, but was %d, queryText:%s", count, queryText)
		}
		return nil
	}, 1*time.Second)

	// Verify the table content is valid.
	testutils.SucceedsWithin(t, func() error {
		row = conn.QueryRowContext(ctx, "SELECT "+
			"query, "+
			"status, "+
			"start_time, "+
			"end_time, "+
			"full_scan, "+
			"implicit_txn, "+
			"cpu_sql_nanos, "+
			"COALESCE(error_code, '') error_code "+
			"FROM crdb_internal.node_execution_insights where "+
			"query = $1 and app_name = $2 ", "SELECT pg_sleep($1)", appName)

		var query, status string
		var startInsights, endInsights time.Time
		var fullScan bool
		var implicitTxn bool
		var cpuSQLNanos int64
		var errorCode string
		err = row.Scan(&query, &status, &startInsights, &endInsights, &fullScan, &implicitTxn, &cpuSQLNanos, &errorCode)

		if err != nil {
			return err
		}

		if status != "Completed" {
			return fmt.Errorf("expected 'Completed', but was %s", status)
		}

		if errorCode != "" {
			return fmt.Errorf("expected error code to be '' but was %s", errorCode)
		}

		delayFromTable := endInsights.Sub(startInsights).Seconds()
		if delayFromTable < queryDelayInSeconds {
			return fmt.Errorf("expected at least %f, but was %f", delayFromTable, queryDelayInSeconds)
		}

		// Add an extra margin of 10ms to the total size of CPU Time.
		maxCPUMs := delayFromTable*1e3 + 10
		if cpuSQLNanos < 0 || (cpuSQLNanos > (int64(maxCPUMs) * 1e6)) {
			return fmt.Errorf("expected cpuSQLNanos to be between zero and %f ms, but was %d", maxCPUMs, cpuSQLNanos)
		}

		return nil
	}, 1*time.Second)

	// TODO (xzhang) Turn this into a datadriven test
	// https://github.com/cockroachdb/cockroach/issues/95010
	// Verify the txn table content is valid.
	testutils.SucceedsWithin(t, func() error {
		row = conn.QueryRowContext(ctx, "SELECT "+
			"query, "+
			"start_time, "+
			"end_time, "+
			"implicit_txn, "+
			"cpu_sql_nanos, "+
			"COALESCE(last_error_code, '') last_error_code "+
			"FROM crdb_internal.cluster_txn_execution_insights WHERE "+
			"query = $1 and app_name = $2 ", "SELECT pg_sleep($1)", appName)

		var query string
		var startInsights, endInsights time.Time
		var implicitTxn bool
		var cpuSQLNanos int64
		var lastErrorCode string
		err = row.Scan(&query, &startInsights, &endInsights, &implicitTxn, &cpuSQLNanos, &lastErrorCode)

		if err != nil {
			return err
		}

		if lastErrorCode != "" {
			return fmt.Errorf("expected last error code to be '' but was %s", lastErrorCode)
		}

		if !implicitTxn {
			return fmt.Errorf("expected implictTxn to be true")
		}

		delayFromTable := endInsights.Sub(startInsights).Seconds()
		if delayFromTable < queryDelayInSeconds {
			return fmt.Errorf("expected at least %f, but was %f", delayFromTable, queryDelayInSeconds)
		}

		// Add an extra margin of 10ms to the total size of CPU Time.
		maxCPUMs := delayFromTable*1e3 + 10
		if cpuSQLNanos < 0 || (cpuSQLNanos > (int64(maxCPUMs) * 1e6)) {
			return fmt.Errorf("expected cpuSQLNanos to be between zero and %f ms, but was %d", maxCPUMs, cpuSQLNanos)
		}

		return nil
	}, 1*time.Second)
}

func TestFailedInsights(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const appName = "TestFailedInsights"

	// Start the cluster. (One node is sufficient; the outliers system is currently in-memory only.)
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	args := base.TestClusterArgs{ServerArgs: base.TestServerArgs{Settings: settings}}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)

	// Enable detection by setting a latencyThreshold > 0.
	latencyThreshold := 100 * time.Millisecond
	insights.LatencyThreshold.Override(ctx, &settings.SV, latencyThreshold)

	_, err := conn.ExecContext(ctx, "SET SESSION application_name=$1", appName)
	require.NoError(t, err)

	testCases := []struct {
		stmt        string
		fingerprint string
		status      string
		problem     string
		errorCode   string
	}{
		// Test case 1: a query that will result in FailedExecution.
		{
			stmt:        "CREATE TABLE crdb_internal.example (abc INT8)",
			fingerprint: "CREATE TABLE crdb_internal.example (abc INT8)",
			status:      "Failed",
			problem:     "FailedExecution",
			errorCode:   "42501",
		},
		// Test case 2: a slow query that will result in FailedExecution.
		{
			stmt:        "SELECT (pg_sleep(0.1), 2/0)",
			fingerprint: "SELECT (pg_sleep(_), _ / _)",
			status:      "Failed",
			problem:     "FailedExecution",
			errorCode:   "22012",
		},
		// Test case 3: a slow query that will result in CompletedExecution.
		{
			stmt:        "SELECT (pg_sleep(0.1), 2/1, 0)",
			fingerprint: "SELECT (pg_sleep(_), _ / _, _)",
			status:      "Completed",
			problem:     "SlowExecution",
			errorCode:   "",
		},
	}

	for _, tc := range testCases {
		_, _ = conn.ExecContext(ctx, tc.stmt)

		testutils.SucceedsWithin(t, func() error {
			var row *gosql.Row
			var query, status, problem, errorCode string

			// Query the node execution insights table.
			row = conn.QueryRowContext(ctx, "SELECT "+
				"query, "+
				"status, "+
				"problem, "+
				"COALESCE(error_code, '') error_code "+
				"FROM crdb_internal.node_execution_insights "+
				"WHERE query = $1 AND app_name = $2 ", tc.fingerprint, appName)

			err = row.Scan(&query, &status, &problem, &errorCode)

			if err != nil {
				return err
			}

			if status != tc.status {
				return fmt.Errorf("expected status to be '%s', but was '%s'", tc.status, status)
			}

			if problem != tc.problem {
				return fmt.Errorf("expected problem to be '%s', but was '%s'", tc.problem, problem)
			}

			if errorCode != tc.errorCode {
				return fmt.Errorf("expected error code to be '%s', but was '%s'", tc.errorCode, errorCode)
			}

			return nil
		}, 1*time.Second)

	}

	txnTestCases := []struct {
		stmts       string
		fingerprint string
		problems    string
		errorCode   string
		endTxn      bool
		txnStatus   string
	}{
		{
			// Single-statement txn that will fail.
			stmts:       "BEGIN; CREATE TABLE crdb_internal.example2 (abc INT8);",
			fingerprint: "CREATE TABLE crdb_internal.example2 (abc INT8)",
			problems:    "{FailedExecution}",
			errorCode:   "42501",
			endTxn:      true,
			txnStatus:   "Failed",
		},
		{
			// Multi-statement txn that will fail.
			stmts:       "BEGIN; SHOW DATABASES; SELECT (2/0);",
			fingerprint: "SHOW DATABASES ; SELECT (_ / _)",
			problems:    "{FailedExecution}",
			errorCode:   "22012",
			endTxn:      true,
			txnStatus:   "Failed",
		},
		{
			// Multi-statement txn with a slow stmt and then a failed execution.
			stmts:       "BEGIN; SELECT (pg_sleep(0.1)); CREATE TABLE exists(); CREATE TABLE exists();",
			fingerprint: "SELECT (pg_sleep(_)) ; CREATE TABLE \"exists\" () ; CREATE TABLE \"exists\" ()",
			problems:    "{SlowExecution,FailedExecution}",
			errorCode:   "42P07",
			endTxn:      true,
			txnStatus:   "Failed",
		},
		{
			// Multi-statement txn with a slow stmt but no failures.
			stmts:       "BEGIN; SELECT (pg_sleep(0.1)); SELECT 0; COMMIT;",
			fingerprint: "SELECT (pg_sleep(_)) ; SELECT _",
			problems:    "{SlowExecution}",
			errorCode:   "",
			endTxn:      false,
			txnStatus:   "Completed",
		},
	}

	for _, tc := range txnTestCases {
		_, _ = conn.ExecContext(ctx, tc.stmts)
		if tc.endTxn {
			_, _ = conn.ExecContext(ctx, "END;")
		}

		testutils.SucceedsWithin(t, func() error {
			var row *gosql.Row
			var query, problems, status, errorCode string

			// Query the node txn execution insights table.
			row = conn.QueryRowContext(ctx, "SELECT "+
				"query, "+
				"problems, "+
				"status, "+
				"COALESCE(last_error_code, '') last_error_code "+
				"FROM crdb_internal.node_txn_execution_insights "+
				"WHERE query = $1 AND app_name = $2 ", tc.fingerprint, appName)

			err = row.Scan(&query, &problems, &status, &errorCode)

			if err != nil {
				return err
			}

			if problems != tc.problems {
				// During tests some transactions can stay open for longer, adding an extra `SlowExecution` to the problems
				// list. This checks for that possibility.
				withSlow := strings.Replace(tc.problems, "{", "{SlowExecution,", -1)
				if problems != withSlow {
					return fmt.Errorf("expected problems to be '%s', but was '%s'. stmts: %s", tc.problems, problems, tc.stmts)
				}
			}

			if status != tc.txnStatus {
				return fmt.Errorf("expected status to be '%s', but was '%s'. stmts: %s", tc.txnStatus, status, tc.stmts)
			}

			if errorCode != tc.errorCode {
				return fmt.Errorf("expected error code to be '%s', but was '%s'. stmts: %s", tc.errorCode, errorCode, tc.stmts)
			}

			return nil
		}, 1*time.Second)
	}

}

func TestInsightsPriorityIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const appName = "TestInsightsPriorityIntegration"

	// Start the cluster. (One node is sufficient; the outliers system is currently in-memory only.)
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	args := base.TestClusterArgs{ServerArgs: base.TestServerArgs{Settings: settings}}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)

	// Enable detection by setting a latencyThreshold > 0.
	latencyThreshold := 50 * time.Millisecond
	insights.LatencyThreshold.Override(ctx, &settings.SV, latencyThreshold)

	_, err := conn.ExecContext(ctx, "SET SESSION application_name=$1", appName)
	require.NoError(t, err)

	_, err = conn.Exec("CREATE TABLE t (id string, s string);")
	require.NoError(t, err)

	// Execute a "long-running" statement, running longer than our latencyThreshold (100ms).
	_, err = conn.ExecContext(ctx, "SELECT pg_sleep(.11)")
	require.NoError(t, err)

	testutils.SucceedsWithin(t, func() error {
		row := conn.QueryRowContext(ctx, "SELECT "+
			"implicit_txn "+
			"FROM crdb_internal.node_execution_insights where "+
			"app_name = $1 and query = $2 ", appName, "SELECT pg_sleep(_)")

		var implicitTxn bool
		err = row.Scan(&implicitTxn)
		if err != nil {
			return err
		}

		if implicitTxn != true {
			return fmt.Errorf("expected implicit_txn '%v', but was %v", true, implicitTxn)
		}

		return nil
	}, 2*time.Second)

	var priorities = []struct {
		setPriorityQuery      string
		query                 string
		queryNoValues         string
		expectedPriorityValue string
	}{
		{
			setPriorityQuery:      "SET TRANSACTION PRIORITY LOW",
			query:                 "INSERT INTO t(id, s) VALUES ('test', 'originalValue')",
			queryNoValues:         "INSERT INTO t(id, s) VALUES ('_', '_')",
			expectedPriorityValue: "low",
		},
		{
			setPriorityQuery:      "SET TRANSACTION PRIORITY NORMAL",
			query:                 "UPDATE t set s = 'updatedValue' where id = 'test'",
			queryNoValues:         "UPDATE t SET s = '_' WHERE id = '_'",
			expectedPriorityValue: "normal",
		},
		{
			setPriorityQuery:      "SELECT 1", // use a dummy query to validate default scenario
			query:                 "UPDATE t set s = 'updatedValue'",
			queryNoValues:         "UPDATE t SET s = '_'",
			expectedPriorityValue: "normal",
		},
		{
			setPriorityQuery:      "SET TRANSACTION PRIORITY HIGH",
			query:                 "DELETE FROM t WHERE t.s = 'originalValue'",
			queryNoValues:         "DELETE FROM t WHERE t.s = '_'",
			expectedPriorityValue: "high",
		},
	}

	for _, p := range priorities {
		testutils.SucceedsWithin(t, func() error {
			tx, errTxn := conn.BeginTx(ctx, &gosql.TxOptions{})
			require.NoError(t, errTxn)

			_, errTxn = tx.ExecContext(ctx, p.setPriorityQuery)
			require.NoError(t, errTxn)

			_, errTxn = tx.ExecContext(ctx, p.query)
			require.NoError(t, errTxn)

			_, errTxn = tx.ExecContext(ctx, "select pg_sleep(.1);")
			require.NoError(t, errTxn)
			errTxn = tx.Commit()
			require.NoError(t, errTxn)
			return nil
		}, 2*time.Second)

		testutils.SucceedsWithin(t, func() error {
			row := conn.QueryRowContext(ctx, "SELECT "+
				"query, "+
				"priority, "+
				"implicit_txn "+
				"FROM crdb_internal.node_execution_insights where "+
				"app_name = $1 and query = $2  ", appName, p.queryNoValues)

			var query, priority string
			var implicitTxn bool
			err = row.Scan(&query, &priority, &implicitTxn)

			if err != nil {
				return err
			}

			if query != p.queryNoValues {
				return fmt.Errorf("expected query '%s', but was %s", p.queryNoValues, query)
			}

			if priority != p.expectedPriorityValue {
				return fmt.Errorf("expected priority '%s', but was %s", p.expectedPriorityValue, priority)
			}

			if implicitTxn != false {
				return fmt.Errorf("expected implicit_txn '%v', but was %v", false, implicitTxn)
			}

			return nil
		}, 2*time.Second)
	}
}

func TestInsightsIntegrationForContention(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start the cluster. (One node is sufficient; the outliers system is currently in-memory only.)
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	args := base.TestClusterArgs{ServerArgs: base.TestServerArgs{Settings: settings}}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)

	conn := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	// This connection will ensure the setting is changed for secondary tenant.

	conn.Exec(t, "SET tracing = true;")
	serverutils.SetClusterSetting(t, tc, "sql.txn_stats.sample_rate", "1")
	// Reduce the resolution interval to speed up the test.
	serverutils.SetClusterSetting(t, tc, "sql.contention.event_store.resolution_interval", "100ms")

	// Set the insights detection threshold lower.
	serverutils.SetClusterSetting(t, tc, "sql.insights.latency_threshold", "1ms")

	conn.Exec(t, "CREATE TABLE t (id string PRIMARY KEY, s string);")

	// Create a new connection, and then start a transaction, update a row, sleep for a time,
	// and then complete the transaction. In a separate go routine attempt to update the same
	//row being updated concurrently, this will be blocked until the original transaction completes.

	// Chan to wait for the txn to complete to avoid checking for insights before the txn is committed.
	txnDoneChan := make(chan struct{})

	tx := conn.Begin(t)

	_, errTxn := tx.ExecContext(ctx, "INSERT INTO t (id, s) VALUES ('test', 'originalValue');")
	require.NoError(t, errTxn)

	waitingTxStartedChan := make(chan struct{})
	approxStmtRuntime := timeutil.NewStopWatch()
	go func() {
		waitingTxStartedChan <- struct{}{}
		approxStmtRuntime.Start()
		// This will be blocked until the started txn above finishes.
		conn.Exec(t, "UPDATE t SET s = 'mainThread' where id = 'test';")
		approxStmtRuntime.Stop()
		txnDoneChan <- struct{}{}
	}()

	<-waitingTxStartedChan

	_, errTxn = tx.ExecContext(ctx, "select pg_sleep(.7);")
	require.NoError(t, errTxn)
	require.NoError(t, tx.Commit())

	<-txnDoneChan

	// Verify the approx run time was around 50ms. The pg_sleep should have blocked the stmt for at
	// least 700ms, but since the stopwatch doesn't measure the runtime exactly we'll use a much
	// smaller value that is >= the required insights threshold.
	require.GreaterOrEqualf(t,
		approxStmtRuntime.Elapsed().Milliseconds(), int64(100), "expected stmt to run for at least 100ms")

	// Verify the table content is valid.
	testutils.SucceedsSoon(t, func() error {
		rows, err := conn.DB.QueryContext(ctx, `SELECT
		query,
		insight.contention::FLOAT,
		sum(txn_contention.contention_duration)::FLOAT AS durationMs,
		txn_contention.schema_name,
		txn_contention.database_name,
		txn_contention.table_name,
		txn_contention.index_name,
		encode(txn_contention.waiting_txn_fingerprint_id, 'hex') AS waiting_txn_fingerprint_id
		FROM crdb_internal.cluster_execution_insights insight
		left join crdb_internal.transaction_contention_events txn_contention on  insight.stmt_id = txn_contention.waiting_stmt_id
																		 where query like 'UPDATE t SET s =%'
		group by query, insight.contention, txn_contention.schema_name, txn_contention.database_name, txn_contention.table_name, txn_contention.index_name, txn_contention.waiting_txn_fingerprint_id;`)
		if err != nil {
			return err
		}

		rowCount := 0
		for rows.Next() {
			rowCount++
			if err != nil {
				return err
			}

			var totalContentionFromQueryMs, contentionFromEventMs float64
			var queryText, schemaName, dbName, tableName, indexName, waitingTxnFingerprintID string
			err = rows.Scan(&queryText, &totalContentionFromQueryMs, &contentionFromEventMs, &schemaName, &dbName, &tableName, &indexName, &waitingTxnFingerprintID)
			if err != nil {
				return err
			}

			if totalContentionFromQueryMs <= 0 {
				return fmt.Errorf("contention time is %f must be greater than 0", totalContentionFromQueryMs)
			}

			if totalContentionFromQueryMs > 60*1000 {
				return fmt.Errorf("contention time must be less than 1 minute:  %f", totalContentionFromQueryMs)
			}

			diff := totalContentionFromQueryMs - contentionFromEventMs
			if math.Abs(diff) > .1 {
				return fmt.Errorf("contention time from column: %f should be the same as event value %f", totalContentionFromQueryMs, contentionFromEventMs)
			}

			if schemaName != "public" {
				return fmt.Errorf("schema names do not match 'public', %s", schemaName)
			}

			if dbName != "defaultdb" {
				return fmt.Errorf("db names do not match 'defaultdb', %s", dbName)
			}

			if tableName != "t" {
				return fmt.Errorf("table names do not match 't', %s", tableName)
			}

			if indexName != "t_pkey" {
				return fmt.Errorf("index names do not match 't_pkey', %s", indexName)
			}

			if waitingTxnFingerprintID == "0000000000000000" || waitingTxnFingerprintID == "" {
				return fmt.Errorf("waitingTxnFingerprintID is default value: %s", waitingTxnFingerprintID)
			}
		}

		if rowCount < 1 {
			var queryStatsMsg string
			var stats, txnEventContentionTime string
			err = conn.DB.QueryRowContext(ctx, `
			SELECT 
				ss.statistics,
				COALESCE(txn_contention.contention_duration::string, 'Not found')
			FROM crdb_internal.statement_statistics ss
			LEFT JOIN  crdb_internal.transaction_contention_events txn_contention on ss.fingerprint_id  = txn_contention.waiting_stmt_fingerprint_id
			WHERE metadata->>'query' like 'UPDATE t SET s =%'`).Scan(&stats, &txnEventContentionTime)
			if err != nil {
				queryStatsMsg = fmt.Sprintf("attempted to get contention statistics for 'UPDATE' query: %s", err.Error())
			} else {
				queryStatsMsg = fmt.Sprintf("contention mean for the 'UPDATE' query: transaction_contention_events.contention_duration: %s, approxStmtRuntime: %s, stats %s", txnEventContentionTime, approxStmtRuntime.Elapsed(), stats)
			}
			return fmt.Errorf("cluster_execution_insights did not return any rows - %s", queryStatsMsg)
		}
		return nil
	})
}

// Testing that the index recommendation is included
// in the insights table
func TestInsightsIndexRecommendationIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStressRace(t, "expensive tests")

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	args := base.TestClusterArgs{ServerArgs: base.TestServerArgs{Settings: settings}}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)

	// Enable detection by setting a latencyThreshold > 0.
	latencyThreshold := 30 * time.Millisecond
	insights.LatencyThreshold.Override(ctx, &settings.SV, latencyThreshold)

	sqlConn := tc.ServerConn(0)

	_, err := sqlConn.ExecContext(ctx, "CREATE TABLE t1 (k INT, i INT, f FLOAT, s STRING)")
	require.NoError(t, err)
	_, err = sqlConn.ExecContext(ctx, "CREATE TABLE t2 (k INT, i INT, s STRING)")
	require.NoError(t, err)

	query := "SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k WHERE t1.i > 3 AND t2.i > 3"

	// Execute enough times to have index recommendations generated.
	// This will generate two recommendations.
	for i := 0; i < 10; i++ {
		tx, err := sqlConn.BeginTx(ctx, &gosql.TxOptions{})
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, "select pg_sleep(.05);")
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, query)
		require.NoError(t, err)
		err = tx.Commit()
		require.NoError(t, err)
	}

	// Verify the table content is valid.
	testutils.SucceedsWithin(t, func() error {
		rows, err := sqlConn.QueryContext(ctx, "SELECT "+
			"query, "+
			"array_to_string(index_recommendations, ';') as cmb_index_recommendations "+
			"FROM crdb_internal.node_execution_insights "+
			"where array_length(index_recommendations, 1) > 0 and "+
			"query like 'SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k%' ")

		if err != nil {
			return err
		}

		var rowCount int
		for rows.Next() {
			var query string
			var idxRecommendation string
			err := rows.Scan(&query, &idxRecommendation)
			if err != nil {
				return err
			}

			if query != "SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k WHERE (t1.i > _) AND (t2.i > _)" {
				return fmt.Errorf("'SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k WHERE (t1.i > _) AND (t2.i > _)' should be %s", query)
			}

			if idxRecommendation == "" {
				return fmt.Errorf("index recommendation should not be empty '%s'", idxRecommendation)
			}

			if !strings.Contains(idxRecommendation, "CREATE INDEX") {
				return fmt.Errorf("index recommendation should contain 'CREATE INDEX' actual:'%s'", idxRecommendation)
			}

			rowCount++
		}

		if rowCount < 1 {
			return fmt.Errorf("no rows found with index recommendation")
		}

		return nil
	}, 1*time.Second)
}
