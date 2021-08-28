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

	testCasesForDisk := map[string]string{
		"SELECT _":       "SELECT 1",
		"SELECT _, _":    "SELECT 1, 1",
		"SELECT _, _, _": "SELECT 1, 1, 1",
	}

	testCasesForMem := map[string]string{
		"SELECT _, _":          "SELECT 1, 1",
		"SELECT _, _, _":       "SELECT 1, 1, 1",
		"SELECT _, _, _, _":    "SELECT 1, 1, 1, 1",
		"SELECT _, _, _, _, _": "SELECT 1, 1, 1, 1, 1",
	}

	appName := "controller_test"
	sqlDB.Exec(t, "SET application_name = $1", appName)

	expectedStmtFingerprintToFingerprintID := make(map[string]string)
	for fingerprint, query := range testCasesForDisk {
		// We will populate the fingerprint ID later.
		expectedStmtFingerprintToFingerprintID[fingerprint] = ""
		sqlDB.Exec(t, query)
	}

	sqlStats := server.SQLServer().(*sql.Server).GetSQLStatsProvider()
	sqlStats.(*persistedsqlstats.PersistedSQLStats).Flush(ctx)

	checkInsertedStmtStatsAndUpdateFingerprintIDs(t, appName, observer, expectedStmtFingerprintToFingerprintID)
	checkInsertedTxnStats(t, appName, observer, expectedStmtFingerprintToFingerprintID)

	// Run few additional queries, so we would also have some SQL stats in-memory.
	for fingerprint, query := range testCasesForMem {
		sqlDB.Exec(t, query)
		if _, ok := expectedStmtFingerprintToFingerprintID[fingerprint]; !ok {
			expectedStmtFingerprintToFingerprintID[fingerprint] = ""
		}
	}

	// Sanity check that we still have the same count since we are still within
	// the same aggregation interval.
	checkInsertedStmtStatsAndUpdateFingerprintIDs(t, appName, observer, expectedStmtFingerprintToFingerprintID)
	checkInsertedTxnStats(t, appName, observer, expectedStmtFingerprintToFingerprintID)

	// Resets cluster wide SQL stats.
	sqlStatsController := server.SQLServer().(*sql.Server).GetSQLStatsController()
	require.NoError(t, sqlStatsController.ResetClusterSQLStats(ctx))

	var count int
	observer.QueryRow(t,
		"SELECT count(*) FROM crdb_internal.statement_statistics WHERE app_name = $1", appName).
		Scan(&count)
	require.Equal(t, 0 /* expected */, count)

	observer.QueryRow(t,
		"SELECT count(*) FROM crdb_internal.transaction_statistics WHERE app_name = $1", appName).
		Scan(&count)
	require.Equal(t, 0 /* expected */, count)
}

func checkInsertedStmtStatsAndUpdateFingerprintIDs(
	t *testing.T,
	appName string,
	observer *sqlutils.SQLRunner,
	expectedStmtFingerprintToFingerprintID map[string]string,
) {
	result := observer.QueryStr(t,
		`
SELECT encode(fingerprint_id, 'hex'), metadata ->> 'query'
FROM crdb_internal.statement_statistics
WHERE app_name = $1`, appName)

	for expectedFingerprint := range expectedStmtFingerprintToFingerprintID {
		var found bool
		for _, row := range result {
			if expectedFingerprint == row[1] {
				found = true

				// Populate fingerprintID.
				expectedStmtFingerprintToFingerprintID[expectedFingerprint] = row[0]
			}
		}
		require.True(t, found, "expect %s to be found, but it was not", expectedFingerprint)
	}
}

func checkInsertedTxnStats(
	t *testing.T,
	appName string,
	observer *sqlutils.SQLRunner,
	expectedStmtFingerprintToFingerprintID map[string]string,
) {
	result := observer.QueryStr(t,
		`
SELECT metadata -> 'stmtFingerprintIDs' ->> 0
FROM crdb_internal.transaction_statistics
WHERE app_name = $1
 AND metadata -> 'stmtFingerprintIDs' ->> 0 IN (
   SELECT encode(fingerprint_id, 'hex')
   FROM crdb_internal.statement_statistics
   WHERE app_name = $2
 )
`, appName, appName)
	for query, fingerprintID := range expectedStmtFingerprintToFingerprintID {
		var found bool
		for _, row := range result {
			if row[0] == fingerprintID {
				found = true
			}
		}
		require.True(t, found,
			`expected %s to be found in txn stats, but it was not.`, query)
	}
}
