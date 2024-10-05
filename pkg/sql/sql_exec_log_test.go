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

}
