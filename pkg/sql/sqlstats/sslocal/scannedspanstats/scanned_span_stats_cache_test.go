// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scannedspanstats_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSQLStatsScannedSpanStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	st := cluster.MakeTestingClusterSettings()
	sqlstats.CollectScannedSpanStats.Override(ctx, &st.SV, true)
	params.Settings = st
	testServer, sqlConn, _ := serverutils.StartServer(t, params)
	defer func() {
		require.NoError(t, sqlConn.Close())
		testServer.Stopper().Stop(ctx)
	}()
	testConn := sqlutils.MakeSQLRunner(sqlConn)
	appName := "scanned-span-stats"
	sqlutils.CreateTable(
		t,
		sqlConn,
		"t1",
		"k INT, i INT, f FLOAT, s STRING",
		100, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)
	sqlutils.CreateTable(
		t,
		sqlConn,
		"t2",
		"j INT, h INT, n FLOAT, z STRING",
		100, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)
	testConn.Exec(t, "SET application_name = $1", appName)
	testConn.Exec(t, "USE test")
	minExecutions := 5

	// All test cases are DML statements
	testCases := []struct {
		name      string
		statement string
	}{
		{
			name:      "select all columns from table 1",
			statement: "SELECT * FROM t1",
		},
		{
			name:      "select all columns from table 2",
			statement: "SELECT * FROM t2",
		},
		{
			name:      "select two columns from table 1",
			statement: "SELECT k, i FROM t1",
		},
		{
			name:      "select three columns from table 2",
			statement: "SELECT j, h, n FROM t2",
		},
		{
			name:      "cross join columns from table 1, table 2",
			statement: "SELECT k, z FROM t1, t2",
		},
	}
	var liveBytes float64
	var pctLive float64
	var totalBytes float64
	for i := 0; i < (minExecutions + 2); i++ {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				testConn.Exec(t, tc.statement)

				if i >= minExecutions-1 {

					rows := testConn.QueryRow(t, "SELECT statistics -> 'statistics' -> 'scannedSpanStats' ->> 'liveBytes',"+
						"statistics -> 'statistics' -> 'scannedSpanStats' ->> 'pctLive',"+
						"statistics -> 'statistics' -> 'scannedSpanStats' ->> 'totalBytes'"+
						"FROM CRDB_INTERNAL.STATEMENT_STATISTICS WHERE app_name = $1"+
						"AND metadata ->> 'query'=$2", appName, tc.statement)
					rows.Scan(&liveBytes, &pctLive, &totalBytes)
					if i < minExecutions {
						require.Equal(t, float64(0), liveBytes)
						require.Equal(t, float64(0), pctLive)
						require.Equal(t, float64(0), totalBytes)
					} else {
						require.Positive(t, liveBytes)
						require.Positive(t, pctLive)
						require.Positive(t, totalBytes)
						require.GreaterOrEqual(t, totalBytes, liveBytes)
						require.LessOrEqual(t, pctLive, float64(1))
					}
				}
			})
		}
	}
}
