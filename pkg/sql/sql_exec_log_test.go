// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/stretchr/testify/require"
)

func TestSqlExecLog(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	ctx := context.Background()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	appLogsSpy := logtestutils.NewStructuredLogSpy(
		t,
		[]logpb.Channel{logpb.Channel_SQL_EXEC},
		[]string{"query_execute"},
		func(entry logpb.Entry) (eventpb.QueryExecute, error) {
			var qe eventpb.QueryExecute
			if err := json.Unmarshal([]byte(entry.Message[entry.StructuredStart:entry.StructuredEnd]), &qe); err != nil {
				return qe, err
			}
			return qe, nil
		},
		func(entry logpb.Entry, qe eventpb.QueryExecute) bool {
			// Filter out internal queries.
			return qe.ExecMode != executorTypeInternal.logLabel()
		},
	)

	cleanup := log.InterceptWith(ctx, appLogsSpy)
	defer cleanup()

	// This connection will be used to set cluster settings.
	setupConn := sqlutils.MakeSQLRunner(s.ApplicationLayer().SQLConn(t))
	setupConn.Exec(t, "SET application_name = 'setupConnApp'")
	// These logs should not appear since we haven't turned logging on.
	setupConn.Exec(t, "SELECT 'hello'")
	setupConn.Exec(t, "SELECT 1, 2")
	setupConn.Exec(t, "SELECT 3")

	// Turn sql execs log on.
	setupConn.Exec(t, "SET CLUSTER SETTING sql.trace.log_statement_execute = true")

	t.Run("verify statement age", func(t *testing.T) {
		// In #114571, it was found that the query age was not properly recorded for BEGIN statements.
		// This lead to the query age being a noticeably large value when it was the first statement
		// in a session, as the session query received time was not yet set. We can easily verify
		// if this is fixed by executing BEGIN as the first stmt in the connection below, and
		// observing the recorded query age.
		conn := sqlutils.MakeSQLRunner(s.ApplicationLayer().SQLConn(t))
		queries := []string{"BEGIN", "SELECT 1", "COMMIT"}
		for _, q := range queries {
			conn.Exec(t, q)
		}

		log.FlushAllSync()

		capturedLogs := appLogsSpy.GetLogs(logpb.Channel_SQL_EXEC)
		expectedIdx := 0

		require.GreaterOrEqual(t, len(capturedLogs), len(queries))
		for _, qe := range capturedLogs {
			if qe.Tag == "SET CLUSTER SETTING" {
				// Skip setting cluster settings statement.
				continue
			}

			if qe.ApplicationName == "setupConnApp" {
				// Other than set cluster setting, we shouldn't have seen any
				// logs from setupConnApp.
				t.Fatalf("unexpected log from setupConnApp: %v", qe)
			}

			// Verify expected log.
			expectedStmt := queries[expectedIdx]
			require.Containsf(t, qe.Statement.StripMarkers(), expectedStmt, "entry: %v", qe.Statement)
			// Query age should be less than 3 seconds.
			require.Lessf(t, qe.Age, float32(3000), "entry: %v", qe)
			expectedIdx++
		}

		// We should have error'd earlier if we didn't find an expected query, but we'll verify
		// that again here we found them all.
		require.Equal(t, expectedIdx, len(queries),
			"Didn't find all expected queries in logs, found %d / %d", expectedIdx, len(queries))
	})

	t.Run("verify failed errors", func(t *testing.T) {
		appLogsSpy.AddFilters(func(entry logpb.Entry, formattedEntry eventpb.QueryExecute) bool {
			return formattedEntry.ErrorText != ""
		})
		appLogsSpy.Reset()
		connE := s.ApplicationLayer().SQLConn(t)
		conn := sqlutils.MakeSQLRunner(connE)

		// Failure due to division by zero.
		_, err := connE.Exec("SELECT 1/0")
		require.Error(t, err)

		// Query timeout.
		conn.Exec(t, "SET statement_timeout = '0.5s'")
		_, err = connE.Exec("SELECT pg_sleep(1)")
		require.Error(t, err)
		conn.Exec(t, "SET statement_timeout = '10s'")

		// Txn timeout.
		conn.Exec(t, "SET transaction_timeout = '0.5s'")
		conn.Exec(t, "BEGIN")
		_, err = connE.Exec("SELECT pg_sleep(1)")
		require.Error(t, err)
		conn.Exec(t, "COMMIT")
		conn.Exec(t, "SET transaction_timeout = '10s'")

		// Cancel query.
		func() {
			_, err = connE.Exec("SELECT pg_sleep(100)")
			require.Error(t, err)
		}()

		cancelConn := s.ApplicationLayer().SQLConn(t)
		testutils.SucceedsSoon(t, func() error {
			if _, err := cancelConn.Exec("CANCEL QUERY (SELECT query_id FROM [SHOW QUERIES] WHERE application_name = 'setupConnApp')"); err != nil {
				return err
			}
			return nil
		})

		log.FlushAllSync()

		logs := appLogsSpy.GetLogs(logpb.Channel_SQL_EXEC)
		require.Len(t, logs, 4)
		require.Equal(t, logs[0].SQLSTATE, pgcode.DivisionByZero.String())

		// Make sure the query text references cancellation due to timeout.
		require.Equal(t, logs[1].SQLSTATE, pgcode.QueryCanceled.String())
		require.Contains(t, string(logs[1].ErrorText), "query execution canceled due to statement timeout")

		// Transaction timeout.
		require.Equal(t, logs[2].SQLSTATE, pgcode.QueryCanceled.String())
		require.Contains(t, string(logs[2].ErrorText), "query execution canceled due to transaction timeout")

		// Regular query cancellation.
		require.Equal(t, logs[2].SQLSTATE, pgcode.QueryCanceled.String())
		require.Contains(t, string(logs[2].ErrorText), "query execution canceled")
	})
}
