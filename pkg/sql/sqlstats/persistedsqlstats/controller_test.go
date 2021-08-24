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
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
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
	params, _ := tests.CreateTestServerParams()

	cluster := serverutils.StartNewTestCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: params,
	})

	server := cluster.Server(0 /* idx */)

	// Open two connections so that we can run statements without messing up
	// the SQL stats.
	testConn := cluster.ServerConn(0 /* idx */)
	observerConn := cluster.ServerConn(1 /* idx */)

	sqlDB := sqlutils.MakeSQLRunner(testConn)
	observer := sqlutils.MakeSQLRunner(observerConn)
	defer cluster.Stopper().Stop(ctx)

	testCases := map[string]struct{}{
		"SELECT 1":     {},
		"SELECT 1,1":   {},
		"SELECT 1,1,1": {},
	}

	appName := "controller_test"
	sqlDB.Exec(t, "SET application_name = $1", appName)

	for query := range testCases {
		sqlDB.Exec(t, query)
	}

	sqlStats := server.SQLServer().(*sql.Server).GetSQLStatsProvider()
	sqlStats.(*persistedsqlstats.PersistedSQLStats).Flush(ctx)

	var count int

	observer.QueryRow(t,
		"SELECT count(*) FROM crdb_internal.transaction_statistics WHERE app_name = $1", appName).
		Scan(&count)
	require.Equal(t, len(testCases), count)

	observer.QueryRow(t,
		"SELECT count(*) FROM crdb_internal.statement_statistics WHERE app_name = $1", appName).
		Scan(&count)
	require.Equal(t, len(testCases), count)

	// Run few additional queries so that
	for query := range testCases {
		sqlDB.Exec(t, query)
	}

	// Sanity check that we still have the same count since we are still within
	// the same aggregation interval.
	observer.QueryRow(t,
		"SELECT count(*) FROM crdb_internal.transaction_statistics WHERE app_name = $1", appName).
		Scan(&count)
	require.Equal(t, len(testCases), count)

	observer.QueryRow(t,
		"SELECT count(*) FROM crdb_internal.statement_statistics WHERE app_name = $1", appName).
		Scan(&count)
	require.Equal(t, len(testCases), count)

	// Resets cluster wide SQL stats.
	sqlStatsController := server.SQLServer().(*sql.Server).GetSQLStatsController()
	require.NoError(t, sqlStatsController.ResetClusterSQLStats(ctx))

	observer.QueryRow(t,
		"SELECT count(*) FROM crdb_internal.statement_statistics WHERE app_name = $1", appName).
		Scan(&count)
	require.Equal(t, 0 /* expected */, count)

	observer.QueryRow(t,
		"SELECT count(*) FROM system.transaction_statistics WHERE app_name = $1", appName).
		Scan(&count)
	require.Equal(t, 0 /* expected */, count)
}
