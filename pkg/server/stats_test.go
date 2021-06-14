// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	gosql "database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/diagutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestTelemetrySQLStatsIndependence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()

	r := diagutils.NewServer()
	defer r.Close()

	url := r.URL()
	params.Knobs.Server = &TestingKnobs{
		DiagnosticsTestingKnobs: diagnostics.TestingKnobs{
			OverrideReportingURL: &url,
		},
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (x INT PRIMARY KEY);
`); err != nil {
		t.Fatal(err)
	}

	sqlServer := s.(*TestServer).Server.sqlServer.pgServer.SQLServer

	// Flush stats at the beginning of the test.
	sqlServer.ResetSQLStats(ctx)
	sqlServer.ResetReportedStats(ctx)

	// Run some queries mixed with diagnostics, and ensure that the statistics
	// are unnaffected by the calls to report diagnostics.
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ($1)`, 1); err != nil {
		t.Fatal(err)
	}
	s.DiagnosticsReporter().(*diagnostics.Reporter).ReportDiagnostics(ctx)
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ($1)`, 2); err != nil {
		t.Fatal(err)
	}
	s.DiagnosticsReporter().(*diagnostics.Reporter).ReportDiagnostics(ctx)

	// Ensure that our SQL statement data was not affected by the telemetry report.
	stats, err := sqlServer.GetScrubbedStmtStats(ctx)
	require.NoError(t, err)

	foundStat := false
	for _, stat := range stats {
		if stat.Key.Query == "INSERT INTO _ VALUES ($1)" {
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
	params, _ := tests.CreateTestServerParams()
	params.Settings = cluster.MakeClusterSettings()
	// Set the SQL stat refresh rate very low so that SQL stats are continuously
	// flushed into the telemetry reporting stats pool.
	sqlstats.SQLStatReset.Override(ctx, &params.Settings.SV, 10*time.Millisecond)
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Run some queries against the database.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (x INT PRIMARY KEY);
INSERT INTO t.test VALUES (1);
INSERT INTO t.test VALUES (2);
`); err != nil {
		t.Fatal(err)
	}

	statusServer := s.(*TestServer).status
	sqlServer := s.(*TestServer).Server.sqlServer.pgServer.SQLServer
	testutils.SucceedsSoon(t, func() error {
		// Get the diagnostic info.
		res, err := statusServer.Diagnostics(ctx, &serverpb.DiagnosticsRequest{NodeId: "local"})
		if err != nil {
			t.Fatal(err)
		}

		found := false
		for _, stat := range res.SqlStats {
			// These stats are scrubbed, so look for our scrubbed statement.
			if strings.HasPrefix(stat.Key.Query, "INSERT INTO _ VALUES (_)") {
				found = true
			}
		}

		if !found {
			return errors.New("expected to find query stats, but didn't")
		}

		// We should also not find the stat in the SQL stats pool, since the SQL
		// stats are getting flushed.
		stats, err := sqlServer.GetScrubbedStmtStats(ctx)
		require.NoError(t, err)

		for _, stat := range stats {
			// These stats are scrubbed, so look for our scrubbed statement.
			if strings.HasPrefix(stat.Key.Query, "INSERT INTO _ VALUES (_)") {
				t.Error("expected to not find stat, but did")
			}
		}
		return nil
	})
}

func TestSQLStatCollection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	sqlServer := s.(*TestServer).Server.sqlServer.pgServer.SQLServer

	// Flush stats at the beginning of the test.
	sqlServer.ResetSQLStats(ctx)
	sqlServer.ResetReportedStats(ctx)

	// Execute some queries against the sqlDB to build up some stats.
	if _, err := sqlDB.Exec(`
	CREATE DATABASE t;
	CREATE TABLE t.test (x INT PRIMARY KEY);
	INSERT INTO t.test VALUES (1);
	INSERT INTO t.test VALUES (2);
	INSERT INTO t.test VALUES (3);
`); err != nil {
		t.Fatal(err)
	}

	// Collect stats from the SQL server and ensure our queries are present.
	stats, err := sqlServer.GetScrubbedStmtStats(ctx)
	require.NoError(t, err)

	foundStat := false
	var sqlStatData roachpb.StatementStatistics

	for _, stat := range stats {
		if stat.Key.Query == "INSERT INTO _ VALUES (_)" {
			foundStat = true
			sqlStatData = stat.Stats
		}
	}
	if !foundStat {
		t.Fatal("expected to find stats for insert query, but didn't")
	}

	const epsilon = 0.00001

	// Reset the SQL statistics, which will dump stats into the
	// reported statistics pool.
	sqlServer.ResetSQLStats(ctx)

	// Query the reported statistics.
	stats, err = sqlServer.GetScrubbedReportingStats(ctx)
	require.NoError(t, err)

	foundStat = false
	for _, stat := range stats {
		if stat.Key.Query == "INSERT INTO _ VALUES (_)" {
			foundStat = true
			if !stat.Stats.AlmostEqual(&sqlStatData, epsilon) {
				t.Fatal("expected stats", sqlStatData.String(), "found", stat.Stats.String())
			}
		}
	}
	if !foundStat {
		t.Fatal("expected to find stats for insert query in reported pool, but didn't")
	}

	// Make another query to the db.
	if _, err := sqlDB.Exec(`
	INSERT INTO t.test VALUES (4);
	INSERT INTO t.test VALUES (5);
	INSERT INTO t.test VALUES (6);
		CREATE USER us WITH PASSWORD 'pass';
`); err != nil {
		t.Fatal(err)
	}

	// Find and record the stats for our second query.
	stats, err = sqlServer.GetScrubbedStmtStats(ctx)
	require.NoError(t, err)

	foundStat = false
	for _, stat := range stats {
		if stat.Key.Query == "INSERT INTO _ VALUES (_)" {
			foundStat = true
			// Add into the current stat data the collected data.
			sqlStatData.Add(&stat.Stats)
		}
	}
	if !foundStat {
		t.Fatal("expected to find stats for insert query, but didn't")
	}

	// Flush the SQL stats again.
	sqlServer.ResetSQLStats(ctx)

	// Find our statement stat from the reported stats pool.
	stats, err = sqlServer.GetScrubbedReportingStats(ctx)
	require.NoError(t, err)

	foundStat = false
	for _, stat := range stats {
		if stat.Key.Query == "INSERT INTO _ VALUES (_)" {
			foundStat = true
			// The new value for the stat should be the aggregate of the previous stat
			// value, and the old stat value. Additionally, zero out the timestamps for
			// the logical plans, as they won't be the same.
			now := timeutil.Now()
			stat.Stats.SensitiveInfo.MostRecentPlanTimestamp, sqlStatData.SensitiveInfo.MostRecentPlanTimestamp = now, now
			if !stat.Stats.AlmostEqual(&sqlStatData, epsilon) {
				t.Fatal("expected stats", sqlStatData, "found", stat.Stats)
			}
		}
	}

	if !foundStat {
		t.Fatal("expected to find stats for insert query in reported pool, but didn't")
	}
}

func populateStats(t *testing.T, sqlDB *gosql.DB) {
	if _, err := sqlDB.Exec(`
	CREATE DATABASE t;
	CREATE TABLE t.test (x INT PRIMARY KEY);
	INSERT INTO t.test VALUES (1);
	INSERT INTO t.test VALUES (2);
	INSERT INTO t.test VALUES (3);
`); err != nil {
		t.Fatal(err)
	}
}

func TestClusterResetSQLStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	testCluster := serverutils.StartNewTestCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Insecure: true,
		},
	})
	defer testCluster.Stopper().Stop(ctx)

	gatewayServer := testCluster.Server(1 /* idx */).(*TestServer)
	status := gatewayServer.status

	sqlDB := serverutils.OpenDBConn(
		t, gatewayServer.ServingSQLAddr(), "" /* useDatabase */, true, /* insecure */
		gatewayServer.Stopper())

	populateStats(t, sqlDB)

	statsPreReset, err := status.Statements(ctx, &serverpb.StatementsRequest{})
	require.NoError(t, err)

	if statsCount := len(statsPreReset.Statements); statsCount == 0 {
		t.Fatal("expected to find stats for at least one statement, but found:", statsCount)
	}

	_, err = status.ResetSQLStats(ctx, &serverpb.ResetSQLStatsRequest{})
	require.NoError(t, err)

	statsPostReset, err := status.Statements(ctx, &serverpb.StatementsRequest{})
	require.NoError(t, err)

	if !statsPostReset.LastReset.After(statsPreReset.LastReset) {
		t.Fatal("expected to find stats last reset value changed, but didn't")
	}

	for _, txn := range statsPostReset.Transactions {
		for _, previousTxn := range statsPreReset.Transactions {
			if reflect.DeepEqual(txn, previousTxn) {
				t.Fatal("expected to have reset SQL stats, but still found transaction", txn)
			}
		}
	}

	for _, stmt := range statsPostReset.Statements {
		for _, previousStmt := range statsPreReset.Statements {
			if reflect.DeepEqual(stmt, previousStmt) {
				t.Fatal("expected to have reset SQL stats, but still found statement", stmt)
			}
		}
	}
}
