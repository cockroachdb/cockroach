// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/diagutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

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
	sqlServer.GetSQLStatsController().ResetLocalSQLStats(ctx)
	sqlServer.GetReportedSQLStatsController().ResetLocalSQLStats(ctx)

	// Run some queries mixed with diagnostics, and ensure that the statistics
	// are unaffected by the calls to report diagnostics.
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

func TestSQLStatCollection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	sqlServer := srv.ApplicationLayer().SQLServer().(*sql.Server)

	// Flush stats at the beginning of the test.
	sqlServer.GetSQLStatsController().ResetLocalSQLStats(ctx)
	sqlServer.GetReportedSQLStatsController().ResetLocalSQLStats(ctx)

	// Execute some queries against the sqlDB to build up some stats.
	// As we are scrubbing the stats, we want to make sure the app name
	// is properly hashed.
	hashedAppName := "hashed app name"
	sqlRunner.Exec(t, `SET application_name = $1;`, hashedAppName)
	sqlRunner.Exec(t, `CREATE DATABASE t`)
	sqlRunner.Exec(t, `CREATE TABLE t.test (x INT PRIMARY KEY);`)
	sqlRunner.Exec(t, `INSERT INTO t.test VALUES (1);`)
	sqlRunner.Exec(t, `INSERT INTO t.test VALUES (2);`)
	sqlRunner.Exec(t, `INSERT INTO t.test VALUES (3);`)

	// Collect stats from the SQL server and ensure our queries are present.
	stats, err := sqlServer.GetScrubbedStmtStats(ctx)
	require.NoError(t, err)

	foundStat := false
	var sqlStatData appstatspb.StatementStatistics

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
	sqlServer.GetSQLStatsController().ResetLocalSQLStats(ctx)

	// Query the reported statistics.
	stats, err = sqlServer.GetScrubbedReportingStats(ctx, 1000, true)
	require.NoError(t, err)

	foundStat = false
	for _, stat := range stats {
		if stat.Key.Query == "INSERT INTO _ VALUES (_)" {
			require.NotEqual(t, hashedAppName, stat.Key.App,
				"expected app name to be hashed")
			require.NotEqual(t, sql.FailedHashedValue, stat.Key.App)
			foundStat = true
			if !stat.Stats.AlmostEqual(&sqlStatData, epsilon) {
				t.Fatal("expected stats", sqlStatData.String(), "found", stat.Stats.String())
			}
		} else if strings.HasPrefix(stat.Key.App,
			catconstants.DelegatedAppNamePrefix) {
			require.NotEqual(t, catconstants.DelegatedAppNamePrefix+hashedAppName,
				stat.Key.App, "expected app name to be hashed")
			require.NotEqual(t, sql.FailedHashedValue, stat.Key.App)
		}
	}
	if !foundStat {
		t.Fatal("expected to find stats for insert query in reported pool, but didn't")
	}

	// Make another query to the db.
	sqlRunner.Exec(t, `INSERT INTO t.test VALUES (4);`)
	sqlRunner.Exec(t, `INSERT INTO t.test VALUES (5);`)
	sqlRunner.Exec(t, `INSERT INTO t.test VALUES (6);`)
	sqlRunner.Exec(t, `CREATE USER us WITH PASSWORD 'pass';`)

	// Find and record the stats for our second query.
	stats, err = sqlServer.GetScrubbedStmtStats(ctx)
	require.NoError(t, err)

	foundStat = false
	for _, stat := range stats {
		if stat.Key.Query == "INSERT INTO _ VALUES (_)" {
			require.NotEqual(t, hashedAppName, stat.Key.App,
				"expected app name to be hashed")
			require.NotEqual(t, sql.FailedHashedValue, stat.Key.App)
			foundStat = true
			// Add into the current stat data the collected data.
			sqlStatData.Add(&stat.Stats)
		} else if strings.HasPrefix(stat.Key.App,
			catconstants.DelegatedAppNamePrefix) {
			require.NotEqual(t, catconstants.DelegatedAppNamePrefix+hashedAppName,
				stat.Key.App, "expected app name to be hashed")
			require.NotEqual(t, sql.FailedHashedValue, stat.Key.App)
		}
	}
	if !foundStat {
		t.Fatal("expected to find stats for insert query, but didn't")
	}

	// Flush the SQL stats again.
	sqlServer.GetSQLStatsController().ResetLocalSQLStats(ctx)

	// Find our statement stat from the reported stats pool.
	stats, err = sqlServer.GetScrubbedReportingStats(ctx, 1000, true)
	require.NoError(t, err)

	foundStat = false
	for _, stat := range stats {
		if stat.Key.Query == "INSERT INTO _ VALUES (_)" {
			require.NotEqual(t, hashedAppName, stat.Key.App,
				"expected app name to be hashed")
			require.NotEqual(t, sql.FailedHashedValue, stat.Key.App)
			foundStat = true
			// The new value for the stat should be the aggregate of the previous stat
			// value, and the old stat value. Additionally, zero out the timestamps for
			// the logical plans, as they won't be the same.
			now := timeutil.Now()
			stat.Stats.SensitiveInfo.MostRecentPlanTimestamp, sqlStatData.SensitiveInfo.MostRecentPlanTimestamp = now, now
			if !stat.Stats.AlmostEqual(&sqlStatData, epsilon) {
				t.Fatal("expected stats", sqlStatData, "found", stat.Stats)
			}
		} else if strings.HasPrefix(stat.Key.App,
			catconstants.DelegatedAppNamePrefix) {
			require.NotEqual(t, catconstants.DelegatedAppNamePrefix+hashedAppName,
				stat.Key.App, "expected app name to be hashed")
			require.NotEqual(t, sql.FailedHashedValue, stat.Key.App)
		}
	}

	if !foundStat {
		t.Fatal("expected to find stats for insert query in reported pool, but didn't")
	}
}

func populateStats(t *testing.T, sqlDB *gosql.DB) {
	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	sqlRunner.Exec(t, `CREATE DATABASE t;`)
	sqlRunner.Exec(t, `CREATE TABLE t.test (x INT PRIMARY KEY);`)
	sqlRunner.Exec(t, `INSERT INTO t.test VALUES (1);`)
	sqlRunner.Exec(t, `INSERT INTO t.test VALUES (2);`)
	sqlRunner.Exec(t, `INSERT INTO t.test VALUES (3);`)
}

func TestClusterResetSQLStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	for _, flushed := range []bool{false, true} {
		t.Run(fmt.Sprintf("flushed=%t", flushed), func(t *testing.T) {
			testCluster := serverutils.StartCluster(t, 3 /* numNodes */, base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					Insecure: true,
					Knobs: base.TestingKnobs{
						SQLStatsKnobs: sqlstats.CreateTestingKnobs(),
					},
				},
			})
			defer testCluster.Stopper().Stop(ctx)

			gateway := testCluster.Server(1 /* idx */).ApplicationLayer()
			status := gateway.StatusServer().(serverpb.SQLStatusServer)

			sqlDB := gateway.SQLConn(t)

			populateStats(t, sqlDB)
			if flushed {
				gateway.SQLServer().(*sql.Server).
					GetSQLStatsProvider().MaybeFlush(ctx, gateway.AppStopper())
			}

			statsPreReset, err := status.Statements(ctx, &serverpb.StatementsRequest{
				Combined: flushed,
			})
			require.NoError(t, err)

			if statsCount := len(statsPreReset.Statements); statsCount == 0 {
				t.Fatal("expected to find stats for at least one statement, but found:", statsCount)
			}

			_, err = status.ResetSQLStats(ctx, &serverpb.ResetSQLStatsRequest{
				ResetPersistedStats: true,
			})
			require.NoError(t, err)

			statsPostReset, err := status.Statements(ctx, &serverpb.StatementsRequest{
				Combined: flushed,
			})
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
		})
	}
}

func TestScrubbedReportingStatsLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	sqlServer := srv.ApplicationLayer().SQLServer().(*sql.Server)
	// Flush stats at the beginning of the test.
	sqlServer.GetSQLStatsController().ResetLocalSQLStats(ctx)
	sqlServer.GetReportedSQLStatsController().ResetLocalSQLStats(ctx)

	hashedAppName := "hashed app name"
	sqlRunner.Exec(t, `SET application_name = $1;`, hashedAppName)
	sqlRunner.Exec(t, `CREATE DATABASE t`)
	sqlRunner.Exec(t, `CREATE TABLE t.test (x INT PRIMARY KEY);`)
	sqlRunner.Exec(t, `INSERT INTO t.test VALUES (1);`)
	sqlRunner.Exec(t, `UPDATE t.test SET x=5 WHERE x=1`)
	sqlRunner.Exec(t, `SELECT * FROM t.test`)
	sqlRunner.Exec(t, `DELETE FROM t.test WHERE x=5`)

	// verify that with low limit, number of stats is within that limit
	sqlServer.GetSQLStatsController().ResetLocalSQLStats(ctx)
	stats, err := sqlServer.GetScrubbedReportingStats(ctx, 5, true)
	require.NoError(t, err)
	require.LessOrEqual(t, len(stats), 5)

	// verify that with high limit, the number of	queries is as much as the above
	sqlServer.GetSQLStatsController().ResetLocalSQLStats(ctx)
	stats, err = sqlServer.GetScrubbedReportingStats(ctx, 1000, true)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(stats), 7)
}
