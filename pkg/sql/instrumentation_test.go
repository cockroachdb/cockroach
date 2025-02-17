// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	gosql "database/sql"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSampledStatsCollection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tt := s.ApplicationLayer()
	sv, sqlStats := &tt.ClusterSettings().SV, tt.SQLServer().(*Server).localSqlStats

	sqlutils.CreateTable(
		t, db, "test", "x INT", 10, sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)

	getStmtStats :=
		func(
			t *testing.T,
			stmt string,
			implicitTxn bool,
			database string,
		) *appstatspb.CollectedStatementStatistics {
			t.Helper()
			key := appstatspb.StatementStatisticsKey{
				Query:       stmt,
				ImplicitTxn: implicitTxn,
				Database:    database,
			}
			var stats *appstatspb.CollectedStatementStatistics
			require.NoError(t, sqlStats.
				IterateStatementStats(
					ctx,
					sqlstats.IteratorOptions{},
					func(ctx context.Context, statistics *appstatspb.CollectedStatementStatistics) error {
						if statistics.Key.Query == key.Query &&
							statistics.Key.ImplicitTxn == key.ImplicitTxn &&
							statistics.Key.Database == key.Database {
							stats = statistics
						}

						return nil
					},
				))
			require.NotNil(t, stats)
			require.NotZero(t, stats.Key.PlanHash)
			return stats
		}

	getTxnStats := func(
		t *testing.T,
		key appstatspb.TransactionFingerprintID,
	) *appstatspb.CollectedTransactionStatistics {
		t.Helper()
		var stats *appstatspb.CollectedTransactionStatistics

		require.NoError(t, sqlStats.
			IterateTransactionStats(
				ctx,
				sqlstats.IteratorOptions{},
				func(ctx context.Context, statistics *appstatspb.CollectedTransactionStatistics) error {
					if statistics.TransactionFingerprintID == key {
						stats = statistics
					}

					return nil
				},
			))

		require.NotNil(t, stats)
		return stats
	}

	toggleSampling := func(enable bool) {
		var v float64
		if enable {
			v = 1
		}
		collectTxnStatsSampleRate.Override(ctx, sv, v)
	}

	type queryer interface {
		Query(string, ...interface{}) (*gosql.Rows, error)
	}
	queryDB := func(t *testing.T, db queryer, query string) {
		t.Helper()
		r, err := db.Query(query)
		require.NoError(t, err)
		require.NoError(t, r.Close())
	}

	const selectOrderBy = "SELECT * FROM test.test ORDER BY x"
	t.Run("ImplicitTxn", func(t *testing.T) {
		toggleSampling(false)
		queryDB(t, db, selectOrderBy)
		toggleSampling(true)
		queryDB(t, db, selectOrderBy)

		stats := getStmtStats(t, selectOrderBy, true /* implicitTxn */, "defaultdb")

		require.Equal(t, int64(2), stats.Stats.Count, "expected to have collected two sets of general stats")
		require.Equal(t, int64(1), stats.Stats.ExecStats.Count, "expected to have collected exactly one set of execution stats")
		require.Greater(t, stats.Stats.RowsRead.Mean, float64(0), "expected statement to have read at least one row")
		require.Greater(t, stats.Stats.ExecStats.MaxMemUsage.Mean, float64(0), "expected statement to have used RAM")
	})

	t.Run("ExplicitTxn", func(t *testing.T) {
		const aggregation = "SELECT x, count(x) FROM test.test GROUP BY x"
		doTxn := func(t *testing.T) {
			tx, err := db.Begin()
			require.NoError(t, err)

			queryDB(t, tx, aggregation)
			queryDB(t, tx, selectOrderBy)

			require.NoError(t, tx.Commit())
		}

		toggleSampling(false)
		doTxn(t)
		toggleSampling(true)
		doTxn(t)

		aggStats := getStmtStats(t, aggregation, false /* implicitTxn */, "defaultdb")
		selectStats := getStmtStats(t, selectOrderBy, false /* implicitTxn */, "defaultdb")

		require.Equal(t, int64(2), aggStats.Stats.Count, "expected to have collected two sets of general stats")
		require.Equal(t, int64(1), aggStats.Stats.ExecStats.Count, "expected to have collected exactly one set of execution stats")
		require.Greater(t, aggStats.Stats.RowsRead.Mean, float64(0), "expected statement to have read at least one row")
		require.Greater(t, aggStats.Stats.ExecStats.MaxMemUsage.Mean, float64(0), "expected statement to have used RAM")

		require.Equal(t, int64(2), selectStats.Stats.Count, "expected to have collected two sets of general stats")
		require.Equal(t, int64(1), selectStats.Stats.ExecStats.Count, "expected to have collected exactly one set of execution stats")
		require.Greater(t, selectStats.Stats.RowsRead.Mean, float64(0), "expected statement to have read at least one row")
		require.Greater(t, selectStats.Stats.ExecStats.MaxMemUsage.Mean, float64(0), "expected statement to have used RAM")

		key := util.MakeFNV64()
		key.Add(uint64(aggStats.ID))
		key.Add(uint64(selectStats.ID))
		txStats := getTxnStats(t, appstatspb.TransactionFingerprintID(key.Sum()))

		require.Equal(t, int64(2), txStats.Stats.Count, "expected to have collected two sets of general stats")
		require.Equal(t, int64(1), txStats.Stats.ExecStats.Count, "expected to have collected exactly one set of execution stats")
		require.Equal(
			t,
			aggStats.Stats.RowsRead.Mean+selectStats.Stats.RowsRead.Mean,
			txStats.Stats.RowsRead.Mean,
			"expected txn to report having read the sum of rows read in both its statements",
		)
		require.Greater(t, txStats.Stats.ExecStats.MaxMemUsage.Mean, float64(0), "expected MaxMemUsage to be set on the txn")
	})

	t.Run("deallocate", func(t *testing.T) {
		toggleSampling(false)
		queryDB(t, db, "PREPARE abc AS SELECT 1")
		queryDB(t, db, "PREPARE xyz AS SELECT 2 ORDER BY 1")
		queryDB(t, db, "DEALLOCATE xyz")
		queryDB(t, db, "DEALLOCATE abc")

		// Make sure DEALLOCATE statements are grouped together rather than having
		// one key per prepared statement name.
		stats := getStmtStats(t, "DEALLOCATE _", true /* implicitTxn */, "defaultdb")

		require.Equal(t, int64(2), stats.Stats.Count, "expected to have collected two sets of general stats")
		require.Equal(t, int64(0), stats.Stats.ExecStats.Count, "expected to have collected zero execution stats")
		require.Equal(t, stats.Stats.RowsRead.Mean, float64(0), "expected statement to have read zero rows")

		// TODO(sql-observability): The PREPARE statements do not appear in the
		// statement stats because tree.Prepare has a special case in the
		// (*connExecutor).execStmtInOpenState function that short-circuits before
		// stats are collected. Should we make DEALLOCATE similar to that, or
		// should we change PREPARE so that stats are collected?
	})
}

// TestSampledStatsCollectionOnNewFingerprint tests that we sample
// a fingerprint if it's the first time the current sql stats
// container has seen it, unless the container is full.
func TestSampledStatsCollectionOnNewFingerprint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testApp := `sampling-test`
	ctx := context.Background()
	var collectedTxnStats []sqlstats.RecordedTxnStats
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLExecutor: &ExecutorTestingKnobs{
				DisableProbabilisticSampling: true,
				OnRecordTxnFinish: func(isInternal bool, _ *sessionphase.Times, stmt string, txnStats sqlstats.RecordedTxnStats) {
					// We won't run into a race here because we'll only observe
					// txns from a single connection.
					if txnStats.SessionData.ApplicationName == testApp {
						if strings.Contains(stmt, `SET application_name`) {
							return
						}
						collectedTxnStats = append(collectedTxnStats, txnStats)
					}
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	ts := s.ApplicationLayer()
	st := &ts.ClusterSettings().SV
	conn := sqlutils.MakeSQLRunner(ts.SQLConn(t))
	conn.Exec(t, "SET application_name = $1", testApp)

	t.Run("do-sampling-when-container-not-full", func(t *testing.T) {
		// All of these statements should be sampled because they are new
		// fingerprints.
		queries := []string{
			"SELECT 1",
			"SELECT 1, 2, 3",
			"CREATE TABLE IF NOT EXISTS foo (x INT)",
			"SELECT * FROM foo",
			// Since the sampling key does not include the txn fingerprint, no
			// statements in this txn should be sampled.
			"BEGIN; SELECT 1; COMMIT;",
		}

		for _, q := range queries {
			conn.Exec(t, q)
		}

		require.Equal(t, len(queries), len(collectedTxnStats))

		// We should have collected stats for each of the queries except the last.
		for i := range collectedTxnStats[:len(queries)-1] {
			require.True(t, collectedTxnStats[i].CollectedExecStats)
		}
		require.False(t, collectedTxnStats[len(queries)-1].CollectedExecStats)
	})

	collectedTxnStats = nil

	t.Run("skip-sampling-when-container-full", func(t *testing.T) {
		// We'll set the in-memory container cap to 1 statement. The container
		// will be full after the first statement is recorded, and thus each
		// subsequent statement will be new to the container.
		persistedsqlstats.MinimumInterval.Override(ctx, st, 10*time.Minute)
		sqlstats.MaxMemSQLStatsStmtFingerprints.Override(ctx, st, 1)
		// Use a new conn to ensure we hit the cap (the limit is node-wide).
		conn2 := sqlutils.MakeSQLRunner(ts.SQLConn(t))
		conn2.Exec(t, "SELECT 1")

		queries := []string{
			"SELECT 'aaaaaa'",
			"CREATE TABLE IF NOT EXISTS bar (x INT)",
			"SELECT * FROM bar",
			"BEGIN; SELECT 1; SELECT 1, 2; COMMIT;",
		}

		// Back to our observed connection.
		for _, q := range queries {
			conn.Exec(t, q)
		}
		require.Equal(t, len(queries), len(collectedTxnStats))

		// Verify we did not collect execution stats for any of the queries.
		for i := range collectedTxnStats {
			require.False(t, collectedTxnStats[i].CollectedExecStats)
		}
	})

}
