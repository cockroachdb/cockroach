// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatstestutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/diagutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestTelemetrySQLStatsIndependence verifies that the collection of SQL statement
// statistics is not affected by diagnostic reporting operations.
func TestTelemetrySQLStatsIndependence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var params base.TestServerArgs
	params.Knobs.SQLStatsKnobs = sqlstats.CreateTestingKnobs()

	r := diagutils.NewServer()
	defer r.Close()

	url := r.URL()
	params.Knobs.Server = &server.TestingKnobs{
		DiagnosticsTestingKnobs: diagnostics.TestingKnobs{
			OverrideReportingURL: &url,
		},
	}

	srv, sqlDB, _ := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (x INT PRIMARY KEY);
`); err != nil {
		t.Fatal(err)
	}

	sqlServer := s.SQLServer().(*sql.Server)

	// Flush stats at the beginning of the test.
	require.NoError(t, sqlServer.GetLocalSQLStatsProvider().Reset(ctx))
	require.NoError(t, sqlServer.GetReportedSQLStatsProvider().Reset(ctx))

	// Run some queries mixed with diagnostics, and ensure that the statistics
	// are unaffected by the calls to report diagnostics.
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ($1)`, 1); err != nil {
		t.Fatal(err)
	}
	s.DiagnosticsReporter().(*diagnostics.Reporter).ReportDiagnostics(ctx)
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ($1)`, 2); err != nil {
		t.Fatal(err)
	}

	conn := sqlutils.MakeSQLRunner(sqlDB)
	sqlstatstestutil.WaitForStatementEntriesAtLeast(t, conn, 1,
		sqlstatstestutil.StatementFilter{
			ExecCount: 2,
			Query:     "INSERT INTO t.test VALUES (_)",
		})

	s.DiagnosticsReporter().(*diagnostics.Reporter).ReportDiagnostics(ctx)

	// Ensure that our SQL statement data was not affected by the telemetry report.
	stats, err := sqlServer.GetScrubbedStmtStats(ctx)
	require.NoError(t, err)

	foundStat := false
	for _, stat := range stats {
		if stat.Key.Query == "INSERT INTO _ VALUES (_)" {
			foundStat = true
			if stat.Stats.Count != 2 {
				t.Fatal("expected to find 2 invocations, found", stat.Stats.Count)
			}
		}
	}
	if !foundStat {
		t.Fatal("expected to find stats for insert query, but didn't")
	}
}

func TestEnsureSQLStatsAreFlushedForTelemetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	sqlConn := sqlutils.MakeSQLRunner(sqlDB)

	tcs := []struct {
		stmt        string
		fingerprint string
	}{
		{
			stmt:        "SELECT 1",
			fingerprint: "SELECT _",
		},
		{
			stmt:        "SELECT 1, 1",
			fingerprint: "SELECT _, _",
		},
		{
			stmt:        "SELECT 1, 1, 1",
			fingerprint: "SELECT _, _, _",
		},
	}

	// Run some queries against the database.
	for _, tc := range tcs {
		sqlConn.Exec(t, tc.stmt)
	}

	sqlstatstestutil.WaitForStatementEntriesAtLeast(t, sqlConn, len(tcs))

	statusServer := s.StatusServer().(serverpb.StatusServer)
	sqlServer := s.SQLServer().(*sql.Server)
	sqlServer.GetSQLStatsProvider().MaybeFlush(ctx, srv.AppStopper())
	testutils.SucceedsSoon(t, func() error {
		// Get the diagnostic info.
		res, err := statusServer.Diagnostics(ctx, &serverpb.DiagnosticsRequest{NodeId: "local"})
		if err != nil {
			t.Fatal(err)
		}

		foundFingerprintCnt := 0
		for _, stat := range res.SqlStats {
			// These stats are scrubbed, so look for our scrubbed statement.
			for _, tc := range tcs {
				if tc.fingerprint == stat.Key.Query {
					foundFingerprintCnt++
					break
				}
			}
		}

		require.Equal(t, len(tcs), foundFingerprintCnt, "expected to find query stats, but didn't")

		// We should also not find the stat in the SQL stats pool, since the SQL
		// stats are getting flushed.
		stats, err := sqlServer.GetScrubbedStmtStats(ctx)
		require.NoError(t, err)

		for _, stat := range stats {
			for _, tc := range tcs {
				require.NotEqual(t, tc.fingerprint, stat.Key.Query, "expected to not found %s in stats, bud did", stat.Key.Query)
			}
		}
		return nil
	})
}
