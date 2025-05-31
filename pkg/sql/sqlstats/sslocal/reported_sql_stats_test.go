// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sslocal_test

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatstestutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestSQLStatCollection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	obsConn := sqlutils.MakeSQLRunner(srv.SQLConn(t))
	sqlServer := srv.ApplicationLayer().SQLServer().(*sql.Server)

	// Flush stats at the beginning of the test.
	require.NoError(t, sqlServer.GetLocalSQLStatsProvider().Reset(ctx))
	require.NoError(t, sqlServer.GetReportedSQLStatsProvider().Reset(ctx))

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

	sqlstatstestutil.WaitForStatementEntriesAtLeast(t, obsConn, 3,
		sqlstatstestutil.StatementFilter{
			App:       hashedAppName,
			ExecCount: 5,
		})

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
	require.NoError(t, sqlServer.GetLocalSQLStatsProvider().Reset(ctx))

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

	sqlstatstestutil.WaitForStatementEntriesAtLeast(t, obsConn, 2,
		sqlstatstestutil.StatementFilter{
			App:       hashedAppName,
			ExecCount: 4,
		})

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
	require.NoError(t, sqlServer.GetLocalSQLStatsProvider().Reset(ctx))

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

func TestScrubbedReportingStatsLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	sqlServer := srv.ApplicationLayer().SQLServer().(*sql.Server)
	// Flush stats at the beginning of the test.
	require.NoError(t, sqlServer.GetLocalSQLStatsProvider().Reset(ctx))
	require.NoError(t, sqlServer.GetReportedSQLStatsProvider().Reset(ctx))

	hashedAppName := "hashed app name"
	sqlRunner.Exec(t, `SET application_name = $1;`, hashedAppName)
	sqlRunner.Exec(t, `CREATE DATABASE t`)
	sqlRunner.Exec(t, `CREATE TABLE t.test (x INT PRIMARY KEY);`)
	sqlRunner.Exec(t, `INSERT INTO t.test VALUES (1);`)
	sqlRunner.Exec(t, `UPDATE t.test SET x=5 WHERE x=1`)
	sqlRunner.Exec(t, `SELECT * FROM t.test`)
	sqlRunner.Exec(t, `DELETE FROM t.test WHERE x=5`)

	sqlstatstestutil.WaitForStatementEntriesAtLeast(t, sqlRunner, 6,
		sqlstatstestutil.StatementFilter{App: hashedAppName})

	// verify that with low limit, number of stats is within that limit
	require.NoError(t, sqlServer.GetLocalSQLStatsProvider().Reset(ctx))
	stats, err := sqlServer.GetScrubbedReportingStats(ctx, 5, true)
	require.NoError(t, err)
	require.LessOrEqual(t, len(stats), 5)

	// verify that with high limit, the number of	queries is as much as the above
	require.NoError(t, sqlServer.GetLocalSQLStatsProvider().Reset(ctx))
	stats, err = sqlServer.GetScrubbedReportingStats(ctx, 1000, true)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(stats), 7)
}
