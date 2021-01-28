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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
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

	const query = "SELECT * FROM test.test ORDER BY x"

	collectStmtStatsSampleRate.Override(&st.SV, 0)

	r, err := db.Query(query)
	require.NoError(t, err)
	require.NoError(t, r.Close())

	collectStmtStatsSampleRate.Override(&st.SV, 1)

	r, err = db.Query(query)
	require.NoError(t, err)
	require.NoError(t, r.Close())

	sqlServer := s.SQLServer().(*Server)
	appStats := sqlServer.sqlStats.getStatsForApplication("")
	require.NotNil(t, appStats, "could not find app stats for default app")
	stmtStats, _ := appStats.getStatsForStmt(query, true /* implicitTxn */, nil /* err */, false /* createIfNonexistent */)
	require.NotNil(t, stmtStats, "could not find stmt stats for %s", query)

	stmtStats.mu.Lock()
	defer stmtStats.mu.Unlock()
	require.Equal(t, int64(2), stmtStats.mu.data.Count, "expected to have collected two sets of general stats")
	require.Equal(t, int64(1), stmtStats.mu.data.ExecStatCollectionCount, "expected to have collected exactly one set of execution stats")
	require.Greater(t, stmtStats.mu.data.BytesRead.Mean, float64(0), "expected statement to have read at least one byte")
}
