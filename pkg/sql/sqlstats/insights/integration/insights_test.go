// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package integration

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	const appNameToIgnore = "TestInsightsIntegrationIgnore"

	// Start the server. (One node is sufficient; the outliers system is
	// currently in-memory only.)
	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	sv := &srv.ApplicationLayer().ClusterSettings().SV

	// Enable detection by setting a latencyThreshold > 0.
	latencyThreshold := 250 * time.Millisecond
	insights.LatencyThreshold.Override(ctx, sv, latencyThreshold)

	_, err := conn.ExecContext(ctx, "SET SESSION application_name=$1", appNameToIgnore)
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
	_, err = conn.ExecContext(ctx, "SET SESSION application_name=$1", appName)
	require.NoError(t, err)

	// Execute a "long-running" statement, running longer than our latencyThreshold.
	// Use a specific app name just for this query, and ignore the other statements, since
	// the select used to check the values can end up or not as part of the Insights view.
	_, err = conn.ExecContext(ctx, "SELECT pg_sleep($1)", queryDelayInSeconds)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, "SET SESSION application_name=$1", appNameToIgnore)
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
			"query = $1 and app_name = $2 ", "SELECT pg_sleep(_)", appName)

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
			"query = $1 and app_name = $2 ", "SELECT pg_sleep(_)", appName)

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
	re := regexp.MustCompile(",?SlowExecution,?")

	// Start the server. (One node is sufficient; the outliers system is
	// currently in-memory only.)
	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	sv := &s.ClusterSettings().SV
	rootConn := sqlutils.MakeSQLRunner(conn)

	// Enable detection by setting a latencyThreshold > 0.
	latencyThreshold := 100 * time.Millisecond
	insights.LatencyThreshold.Override(ctx, sv, latencyThreshold)

	rootConn.Exec(t, fmt.Sprintf("CREATE USER %s WITH VIEWACTIVITYREDACTED", "testuser"))
	rootConn.Exec(t, "SET SESSION application_name=$1", appName)

	testutils.RunTrueAndFalse(t, "with_redaction", func(t *testing.T, testRedacted bool) {
		rootConn.Exec(t, `select crdb_internal.reset_sql_stats()`)
		conn := s.SQLConn(t)
		if testRedacted {
			conn = s.SQLConn(t, serverutils.User("testuser"))
		}
		conn.SetMaxOpenConns(1)
		_, err := conn.Exec("SET autocommit_before_ddl = false")
		require.NoError(t, err)
		_, err = conn.Exec("SET SESSION application_name=$1", appName)
		require.NoError(t, err)

		testCases := []struct {
			stmt           string
			fingerprint    string
			status         string
			problem        string
			errorCode      string
			errorMsg       string
			errMsgRedacted string
		}{
			// Test case 1: a query that will result in FailedExecution.
			{
				stmt:           "CREATE TABLE crdb_internal.example (abc INT8)",
				fingerprint:    "CREATE TABLE crdb_internal.example (abc INT8)",
				status:         "Failed",
				problem:        "FailedExecution",
				errorCode:      "42501",
				errorMsg:       `schema cannot be modified: ‹"crdb_internal"›`,
				errMsgRedacted: `schema cannot be modified: ‹×›`,
			},
			// Test case 2: a slow query that will result in FailedExecution.
			{
				stmt:           "SELECT (pg_sleep(0.1), 2/0)",
				fingerprint:    "SELECT (pg_sleep(_), _ / _)",
				status:         "Failed",
				problem:        "FailedExecution",
				errorCode:      "22012",
				errorMsg:       "division by zero",
				errMsgRedacted: `division by zero`,
			},
			// Test case 3: a slow query that will result in CompletedExecution.
			{
				stmt:        "SELECT (pg_sleep(0.1), 2/1, 0)",
				fingerprint: "SELECT (pg_sleep(_), _ / _, _)",
				status:      "Completed",
				problem:     "SlowExecution",
			},
		}

		for _, tc := range testCases {
			// The below execution may error.
			_, _ = conn.ExecContext(ctx, tc.stmt)

			var query, status, problem, errorCode, errorMsg string
			testutils.SucceedsWithin(t, func() error {

				// Query the node execution insights table.
				row := conn.QueryRowContext(ctx, `
SELECT query, 
       status, 
	   problem, 
	   COALESCE(error_code, '') error_code, 
	   COALESCE(last_error_redactable, '') last_error 
FROM crdb_internal.node_execution_insights 
WHERE query = $1 AND app_name = $2 `,
					tc.fingerprint, appName)

				return row.Scan(&query, &status, &problem, &errorCode, &errorMsg)
			}, 1*time.Second)

			require.Equal(t, tc.status, status)
			require.Equal(t, tc.problem, problem)
			require.Equal(t, tc.errorCode, errorCode)
			if testRedacted && tc.errorMsg != "" {
				require.Equal(t, tc.errMsgRedacted, errorMsg)
			} else {
				require.Contains(t, errorMsg, tc.errorMsg)
			}
		}

		txnTestCases := []struct {
			stmts            string
			fingerprint      string
			problems         string
			errorCode        string
			errorMsg         string
			errorMsgRedacted string
			endTxn           bool
			txnStatus        string
		}{
			{
				// Single-statement txn that will fail.
				stmts:            "BEGIN; CREATE TABLE crdb_internal.example2 (abc INT8);",
				fingerprint:      "CREATE TABLE crdb_internal.example2 (abc INT8)",
				problems:         "{FailedExecution}",
				errorCode:        "42501",
				errorMsg:         `schema cannot be modified: ‹"crdb_internal"›`,
				errorMsgRedacted: `schema cannot be modified: ‹×›`,
				endTxn:           true,
				txnStatus:        "Failed",
			},
			{
				// Multi-statement txn that will fail.
				stmts:            "BEGIN; SHOW DATABASES; SELECT (2/0);",
				fingerprint:      "SHOW DATABASES ; SELECT (_ / _)",
				problems:         "{FailedExecution}",
				errorCode:        "22012",
				errorMsg:         `division by zero`,
				errorMsgRedacted: `division by zero`,
				endTxn:           true,
				txnStatus:        "Failed",
			},
			{
				// Multi-statement txn with a slow stmt and then a failed execution.
				stmts:            "BEGIN; SELECT (pg_sleep(0.1)); CREATE TABLE exists(); CREATE TABLE exists();",
				fingerprint:      "SELECT (pg_sleep(_)) ; CREATE TABLE \"exists\" () ; CREATE TABLE \"exists\" ()",
				problems:         "{FailedExecution,SlowExecution}",
				errorCode:        "42P07",
				errorMsg:         `relation ‹"defaultdb.public.\"exists\""› already exists`,
				errorMsgRedacted: `relation ‹×› already exists`,
				endTxn:           true,
				txnStatus:        "Failed",
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

			var query, problems, status, errorCode, errorMsg string
			testutils.SucceedsWithin(t, func() error {

				// Query the node txn execution insights table.
				row := conn.QueryRowContext(ctx, `
SELECT query,
       problems,
       status,
       COALESCE(last_error_code, '') last_error_code,
       COALESCE(last_error_redactable, '') last_error
FROM crdb_internal.node_txn_execution_insights 
WHERE query = $1 AND app_name = $2`, tc.fingerprint, appName)

				return row.Scan(&query, &problems, &status, &errorCode, &errorMsg)
			}, 1*time.Second)

			require.Equal(t, tc.txnStatus, status)
			require.Equal(t, tc.errorCode, errorCode)
			if testRedacted && tc.errorMsg != "" {
				require.Equal(t, tc.errorMsgRedacted, errorMsg)
			} else {
				require.Contains(t, errorMsg, tc.errorMsg)
			}

			replacedSlowProblems := problems
			if problems != tc.problems {
				// During tests some transactions can stay open for longer, adding an extra
				// `SlowExecution` to the problems list. This checks for that possibility.
				replacedSlowProblems = re.ReplaceAllString(replacedSlowProblems, "")
			}
			// Print the original problems if we did any replacements, for debugging.
			require.Equal(t, tc.problems, replacedSlowProblems, "received: %s, used to compare: %s", problems, replacedSlowProblems)

		}
	})
}

// TestTransactionInsightsFailOnCommit specifically tests for the scenario where a transaction
// fails on COMMIT. COMMIT is executed specially and does not get stats recorded, skipping
// the insights recording step. We should ensure txns failing on COMMIT are also captured.
func TestTransactionInsightsFailOnCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const appName = "TestTransactionInsightsFailOnCommit"
	re := regexp.MustCompile(",?SlowExecution,?")

	type txnInConflict struct {
		txnID            uuid.UUID
		txnFingerprintID appstatspb.TransactionFingerprintID
		err              error
	}

	ctx := context.Background()
	conflictingTxns := make([]txnInConflict, 0, 4)

	// Start the server. (One node is sufficient; the outliers system is
	// currently in-memory only.)
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				BeforeTxnStatsRecorded: func(session *sessiondata.SessionData,
					txnID uuid.UUID, txnFingerprintID appstatspb.TransactionFingerprintID, err error) {
					if session.ApplicationName != appName {
						return
					}

					conflictingTxns = append(conflictingTxns,
						txnInConflict{txnID: txnID, txnFingerprintID: txnFingerprintID, err: err})

				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	connDefault := sqlutils.MakeSQLRunner(db)
	// Connections specifically to run our conflicting txns.
	conn1 := sqlutils.MakeSQLRunner(srv.ApplicationLayer().SQLConn(t))
	conn2 := sqlutils.MakeSQLRunner(srv.ApplicationLayer().SQLConn(t))

	connDefault.Exec(t, "SET CLUSTER SETTING sql.contention.event_store.resolution_interval = '100ms'")

	// Set up myUsers table with 2 users.
	connDefault.Exec(t, "CREATE TABLE myUsers (name STRING, city STRING)")
	connDefault.Exec(t, "INSERT INTO myUsers VALUES ('WENDY', 'NYC'), ('NOVI', 'TORONTO')")

	conn1.Exec(t, "SET SESSION application_name=$1", appName)
	conn2.Exec(t, "SET SESSION application_name=$1", appName)

	// The first 2 recorded txns are setting the session name.
	conflictingTxns = conflictingTxns[2:]

	testutils.RunTrueAndFalse(t, "enable recording SERIALIZATION_CONFLICT events", func(t *testing.T, enabled bool) {
		contention.EnableSerializationConflictEvents.Override(ctx, &srv.ApplicationLayer().ClusterSettings().SV, enabled)
		// We will simulate a 40001 transaction retry error due to conflicting locks.
		// Transaction 1 will fail on COMMIT.
		tx1 := conn1.Begin(t)
		_, err := tx1.Exec("SELECT * FROM myUsers WHERE city = 'TORONTO'")
		require.NoError(t, err)

		tx2 := conn2.Begin(t)
		_, err = tx2.Exec("UPDATE myUsers SET name = 'NOVI' WHERE city = 'TORONTO'")
		require.NoError(t, err)

		_, err = tx1.Exec("UPDATE myUsers SET name = 'WENDY' WHERE city = 'NYC'")
		require.NoError(t, err)
		require.Error(t, tx1.Commit())

		// Commit the blocking txn. There should be no error here.
		require.NoError(t, tx2.Commit())

		require.Equal(t, 2, len(conflictingTxns))

		t.Run("40001 error exists in txn insights", func(t *testing.T) {
			{
				var query, problems, status, errorCode, errorMsg string
				testutils.SucceedsSoon(t, func() error {
					// Query the node txn execution insights table.
					row := connDefault.DB.QueryRowContext(ctx, `
SELECT query,
       problems,
       status,
       COALESCE(last_error_code, '') last_error_code,
       COALESCE(last_error_redactable, '') last_error
FROM crdb_internal.node_txn_execution_insights 
WHERE app_name = $1
AND query = 'SELECT * FROM myusers WHERE city = _ ; UPDATE myusers SET name = _ WHERE city = _'`, appName)

					return row.Scan(&query, &problems, &status, &errorCode, &errorMsg)
				})

				require.Equalf(t,
					"SELECT * FROM myusers WHERE city = _ ; UPDATE myusers SET name = _ WHERE city = _", query,
					"unexpected txn insight found - query: %s, problems: %s, status: %s, errCode: %s, errMsg: %s",
					query, problems, status, errorCode, errorMsg)
				expectedProblem := "{FailedExecution}"
				replacedSlowProblems := problems
				if problems != expectedProblem {
					// During tests some transactions can stay open for longer, adding an extra
					// `SlowExecution` to the problems list. This checks for that possibility.
					replacedSlowProblems = re.ReplaceAllString(replacedSlowProblems, "")
				}
				// Print the original problems if we did any replacements, for debugging.
				require.Equal(t, expectedProblem, replacedSlowProblems, "received: %s, used to compare: %s", problems, replacedSlowProblems)
				require.Equal(t, "Failed", status)
				require.Equal(t, "40001", errorCode)
				require.Contains(t, errorMsg, "TransactionRetryWithProtoRefreshError")
			}
		})

		var blockingID uuid.UUID
		var blockingFingerprint, dbName, schema, table, index, contentionType string

		var txRetryErr *kvpb.TransactionRetryWithProtoRefreshError
		require.Error(t, conflictingTxns[0].err)
		require.ErrorAs(t, conflictingTxns[0].err, &txRetryErr)
		conflictingTxnID := txRetryErr.ConflictingTxn.ID

		if enabled {
			// We need this SucceedsSoon to wait for the txn fingerprint to resolve.
			testutils.SucceedsSoon(t, func() error {
				row := connDefault.DB.QueryRowContext(ctx, `
SELECT blocking_txn_id,
		   encode(blocking_txn_fingerprint_id, 'hex') AS blocking_txn_fingerprint_id,
       database_name,
       schema_name,
       table_name,
       index_name,
       contention_type
FROM crdb_internal.transaction_contention_events 
WHERE blocking_txn_id = $1 AND blocking_txn_fingerprint_id != $2`, conflictingTxnID, "0000000000000000")
				return row.Scan(&blockingID, &blockingFingerprint, &dbName, &schema, &table, &index, &contentionType)

			})

			require.Equal(t, blockingID, conflictingTxnID)
			require.Equal(t, "defaultdb", dbName)
			require.Equal(t, "public", schema)
			require.Equal(t, "myusers", table)
			require.Equal(t, "myusers_pkey", index)
			require.Equal(t, "SERIALIZATION_CONFLICT", contentionType)

			return
		}

		// Recording serialization conflicts is not enabled.
		// Check that we don't generate the SERIALIZATION_CONFLICT event.
		for i := 0; i < 100; i++ {
			rows, err := connDefault.DB.QueryContext(ctx, `
SELECT contention_type FROM crdb_internal.transaction_contention_events 
WHERE contention_type = 'SERIALIZATION_CONFLICT'`)

			require.NoError(t, err)
			require.False(t, rows.Next())
		}

		conflictingTxns = make([]txnInConflict, 0, 2)
	})
}

func TestInsightsPriorityIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const appName = "TestInsightsPriorityIntegration"

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	sv := &srv.ApplicationLayer().ClusterSettings().SV

	// Enable detection by setting a latencyThreshold > 0.
	latencyThreshold := 50 * time.Millisecond
	insights.LatencyThreshold.Override(ctx, sv, latencyThreshold)

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
			queryNoValues:         "INSERT INTO t(id, s) VALUES (_, __more__)",
			expectedPriorityValue: "low",
		},
		{
			setPriorityQuery:      "SET TRANSACTION PRIORITY NORMAL",
			query:                 "UPDATE t set s = 'updatedValue' where id = 'test'",
			queryNoValues:         "UPDATE t SET s = _ WHERE id = _",
			expectedPriorityValue: "normal",
		},
		{
			setPriorityQuery:      "SELECT 1", // use a dummy query to validate default scenario
			query:                 "UPDATE t set s = 'updatedValue'",
			queryNoValues:         "UPDATE t SET s = _",
			expectedPriorityValue: "normal",
		},
		{
			setPriorityQuery:      "SET TRANSACTION PRIORITY HIGH",
			query:                 "DELETE FROM t WHERE t.s = 'originalValue'",
			queryNoValues:         "DELETE FROM t WHERE t.s = _",
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

	conn := sqlutils.MakeSQLRunner(tc.ApplicationLayer(0).SQLConn(t))
	conn.Exec(t, "SET tracing = true;")
	serverutils.SetClusterSetting(t, tc, "sql.txn_stats.sample_rate", "1")
	// Set the insights detection threshold lower.
	serverutils.SetClusterSetting(t, tc, "sql.insights.latency_threshold", "100ms")
	// Set to a long interval as we'll manually trigger the event resolution later.
	serverutils.SetClusterSetting(t, tc, "sql.contention.event_store.resolution_interval", "30m")

	conn.Exec(t, "CREATE TABLE t (id string PRIMARY KEY, s string);")

	// Create a new connection, and then start a transaction, update a row, sleep for a time,
	// and then complete the transaction. In a separate go routine attempt to update the same
	//row being updated concurrently, this will be blocked until the original transaction completes.

	// Chan to wait for the txn to complete to avoid checking for insights before the txn is committed.
	txnDoneChan := make(chan struct{})

	observerConn := sqlutils.MakeSQLRunner(tc.ApplicationLayer(0).SQLConn(t))
	txConn := sqlutils.MakeSQLRunner(tc.ApplicationLayer(0).SQLConn(t))
	tx := txConn.Begin(t)

	_, errTxn := tx.ExecContext(ctx, "INSERT INTO t (id, s) VALUES ('test', 'originalValue');")
	require.NoError(t, errTxn)

	waitingTxStartedChan := make(chan struct{})
	approxStmtRuntime := timeutil.NewStopWatch()
	go func() {
		conn.Exec(t, "SET application_name = 'waiting_txn'")
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

	var waitingTxnID uuid.UUID
	observerConn.QueryRow(t,
		`SELECT id
         FROM crdb_internal.node_transactions
         WHERE application_name = 'waiting_txn'`).Scan(&waitingTxnID)

	require.NoError(t, tx.Commit())

	<-txnDoneChan

	// Verify the approx run time was around 50ms. The pg_sleep should have blocked the stmt for at
	// least 700ms, but since the stopwatch doesn't measure the runtime exactly we'll use a much
	// smaller value that is >= the required insights threshold.
	require.GreaterOrEqualf(t,
		approxStmtRuntime.Elapsed().Milliseconds(), int64(100), "expected stmt to run for at least 100ms")

	// We should ensure that the waiting txn id exists in the txn id cache. Failing to
	// lookup this id will result in the resolver potentially missing the event.
	txnIDCache := tc.ApplicationLayer(0).SQLServer().(*sql.Server).GetTxnIDCache()
	txnIDCache.DrainWriteBuffer()
	testutils.SucceedsSoon(t, func() error {
		waitingTxnFingerprintID, ok := txnIDCache.Lookup(waitingTxnID)
		if !ok || waitingTxnFingerprintID == appstatspb.InvalidTransactionFingerprintID {
			return fmt.Errorf("waiting txn fingerprint not found in cache")
		}
		return nil
	})

	// Verify the table content is valid.
	testutils.SucceedsSoon(t, func() error {
		err := tc.ApplicationLayer(0).ExecutorConfig().(sql.ExecutorConfig).ContentionRegistry.FlushEventsForTest(ctx)
		require.NoError(t, err)

		rows, err := conn.DB.QueryContext(ctx, `SELECT
		query,
		COALESCE(insight.contention, 0::INTERVAL)::FLOAT,
		COALESCE(sum(txn_contention.contention_duration), 0::INTERVAL)::FLOAT AS durationMs,
		COALESCE(txn_contention.schema_name, ''::STRING)::STRING AS schema_name,
		COALESCE(txn_contention.database_name, ''::STRING)::STRING AS database_name,
		COALESCE(txn_contention.table_name, ''::STRING)::STRING AS table_name,
		COALESCE(txn_contention.index_name, ''::STRING)::STRING AS index_name,
		COALESCE(encode(txn_contention.waiting_txn_fingerprint_id, 'hex'), '')::STRING AS waiting_txn_fingerprint_id
		FROM crdb_internal.cluster_execution_insights insight
		left join crdb_internal.transaction_contention_events txn_contention on  insight.stmt_id = txn_contention.waiting_stmt_id
																		 where query like 'UPDATE t SET s =%'
		group by query, insight.contention, txn_contention.schema_name, txn_contention.database_name, txn_contention.table_name, txn_contention.index_name, txn_contention.waiting_txn_fingerprint_id;`)
		if err != nil {
			return err
		}
		// There may be multiple contention events for the query. We'll verify that
		// at least 1 row matches the one we're looking for.
		foundRow := false
		var lastErr error
		rowsCount := 0
		for rows.Next() {
			if err != nil {
				return err
			}
			rowsCount++
			var totalContentionFromQueryMs, contentionFromEventMs float64
			var queryText, schemaName, dbName, tableName, indexName, waitingTxnFingerprintID string
			err = rows.Scan(&queryText, &totalContentionFromQueryMs, &contentionFromEventMs, &schemaName, &dbName, &tableName, &indexName, &waitingTxnFingerprintID)

			prettyPrintRow := fmt.Sprintf(`query: %s, totalContentionFromQueryMs: %f, contentionFromEventMs: %f,
				"schemaName: %s, dbName: %s, tableName: %s, indexName: %s, waitingTxnFingerprintID: %s`,
				queryText, totalContentionFromQueryMs, contentionFromEventMs,
				schemaName, dbName, tableName, indexName, waitingTxnFingerprintID)

			if err != nil {
				return err
			}

			if totalContentionFromQueryMs <= 0 {
				return fmt.Errorf("total contention time must be greater than 0\n%v", prettyPrintRow)
			}

			if totalContentionFromQueryMs > 60*1000 {
				lastErr = fmt.Errorf("contention time must be less than 1 minute:\n%s", prettyPrintRow)
				continue
			}

			if schemaName != "public" {
				lastErr = fmt.Errorf("schema names do not match 'public'\n%s", prettyPrintRow)
				continue
			}

			if dbName != "defaultdb" {
				lastErr = fmt.Errorf("db names do not match 'defaultdb'\n%s", prettyPrintRow)
				continue
			}

			if tableName != "t" {
				lastErr = fmt.Errorf("table names do not match 't'\n%s", prettyPrintRow)
				continue
			}

			if indexName != "t_pkey" {
				lastErr = fmt.Errorf("index names do not match 't_pkey'\n%s", prettyPrintRow)
				continue
			}

			foundRow = true
			break
		}

		if !foundRow && lastErr != nil {
			t.Logf("rowsCount = %d", rowsCount)
			t.Logf("ContentionRegistry: \n%s", tc.ApplicationLayer(0).ExecutorConfig().(sql.ExecutorConfig).ContentionRegistry.String())
			return lastErr
		}

		if !foundRow {
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

	skip.UnderRace(t, "expensive tests")

	ctx := context.Background()
	srv, sqlConn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()

	// Enable detection by setting a latencyThreshold > 0.
	latencyThreshold := 30 * time.Millisecond
	insights.LatencyThreshold.Override(ctx, &ts.ClusterSettings().SV, latencyThreshold)

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

// TestInsightsClearsPerSessionMemory ensures that memory allocated
// for a session is freed when that session is closed.
func TestInsightsClearsPerSessionMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	sessionClosedCh := make(chan struct{})
	clearedSessionID := clusterunique.ID{}
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Insights: &insights.TestingKnobs{
				OnSessionClear: func(sessionID clusterunique.ID) {
					defer close(sessionClosedCh)
					clearedSessionID = sessionID
				},
			},
		},
	})
	defer ts.Stopper().Stop(ctx)
	s := ts.ApplicationLayer()
	conn1 := sqlutils.MakeSQLRunner(s.SQLConn(t))
	conn2 := sqlutils.MakeSQLRunner(s.SQLConn(t))

	var sessionID1 string
	conn1.QueryRow(t, "SHOW session_id").Scan(&sessionID1)

	// Start a transaction and cancel the session - ensure that the memory is freed.
	conn1.Exec(t, "BEGIN")
	for i := 0; i < 5; i++ {
		conn1.Exec(t, "SELECT 1")
	}

	conn2.Exec(t, "CANCEL SESSION $1", sessionID1)

	<-sessionClosedCh
	require.Equal(t, clearedSessionID.String(), sessionID1)
}
