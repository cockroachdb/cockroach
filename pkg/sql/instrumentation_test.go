// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
	st := cluster.MakeTestingClusterSettings()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{Settings: st})
	defer s.Stopper().Stop(ctx)

	sqlutils.CreateTable(
		t, db, "test", "x INT", 10, sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)

	getStmtStats :=
		func(
			t *testing.T,
			server serverutils.TestServerInterface,
			stmt string,
			implicitTxn bool,
			database string,
		) *roachpb.CollectedStatementStatistics {
			t.Helper()
			key := roachpb.StatementStatisticsKey{
				Query:       stmt,
				ImplicitTxn: implicitTxn,
				Database:    database,
				Failed:      false,
			}
			stats, err := server.SQLServer().(*Server).sqlStats.GetStatementStats(&key)
			require.NoError(t, err)
			return stats
		}

	toggleSampling := func(enable bool) {
		var v float64
		if enable {
			v = 1
		}
		collectTxnStatsSampleRate.Override(ctx, &st.SV, v)
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

		stats := getStmtStats(t, s, selectOrderBy, true /* implicitTxn */, "defaultdb")

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

		aggStats := getStmtStats(t, s, aggregation, false /* implicitTxn */, "defaultdb")
		selectStats := getStmtStats(t, s, selectOrderBy, false /* implicitTxn */, "defaultdb")

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
		txStats, err := s.SQLServer().(*Server).sqlStats.GetTransactionStats("", roachpb.TransactionFingerprintID(key.Sum()))
		require.NoError(t, err)

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
}
