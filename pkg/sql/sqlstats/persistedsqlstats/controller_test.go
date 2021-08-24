// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// Package sqlstats is a subsystem that is responsible for tracking the
// statistics of statements and transactions.

package persistedsqlstats_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestPersistedSQLStatsReset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	server, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	sqlDB := sqlutils.MakeSQLRunner(conn)
	defer server.Stopper().Stop(ctx)

	testCases := map[string]struct{}{
		"SELECT 1":      {},
		"SELECT 1,1":    {},
		"SELECT 1,1, 1": {},
	}

	for query := range testCases {
		sqlDB.Exec(t, query)
	}

	sqlStats := server.SQLServer().(*sql.Server).GetSQLStatsProvider()
	sqlStats.(*persistedsqlstats.PersistedSQLStats).Flush(ctx)

	var count int
	sqlDB.QueryRow(t, "SELECT count(*) FROM system.statement_statistics").Scan(&count)
	require.LessOrEqual(t, len(testCases), count)

	sqlDB.QueryRow(t, "SELECT count(*) FROM system.transaction_statistics").Scan(&count)
	require.LessOrEqual(t, len(testCases), count)

	sqlStatsController := server.SQLServer().(*sql.Server).GetSQLStatsController()
	require.NoError(t, sqlStatsController.ResetClusterSQLStats(ctx))

	sqlDB.QueryRow(t, "SELECT count(*) FROM system.statement_statistics").Scan(&count)
	require.Equal(t, 0 /* expected */, count)

	sqlDB.QueryRow(t, "SELECT count(*) FROM system.transaction_statistics").Scan(&count)
	require.Equal(t, 0 /* expected */, count)
}
