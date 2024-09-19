// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// Test the server.max_open_transactions_per_gateway cluster setting. Only
// non-admins are subject to the limit.
func TestMaxOpenTxns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	rootSQLRunner := sqlutils.MakeSQLRunner(db)
	rootSQLRunner.Exec(t, "SET CLUSTER SETTING server.max_open_transactions_per_gateway = 4")
	rootSQLRunner.Exec(t, "CREATE USER testuser")

	testUserDB := s.SQLConn(t, serverutils.User("testuser"))

	tx1 := rootSQLRunner.Begin(t)
	_, err := testUserDB.Exec("SELECT 1")
	require.NoError(t, err)

	tx2 := rootSQLRunner.Begin(t)
	_, err = testUserDB.Exec("SELECT 1")
	require.NoError(t, err)

	tx3 := rootSQLRunner.Begin(t)
	_, err = testUserDB.Exec("SELECT 1")
	require.NoError(t, err)

	// After four transactions have been opened, testuser cannot run a query.
	tx4 := rootSQLRunner.Begin(t)
	_, err = testUserDB.Exec("SELECT 1")
	require.ErrorContains(t, err, "cannot execute operation due to server.max_open_transactions_per_gateway cluster setting")

	// testuser also cannot run anything in an explicit transaction. Starting the
	// transaction is allowed though.
	testuserTx, err := testUserDB.Begin()
	require.NoError(t, err)
	_, err = testuserTx.Exec("SELECT 1")
	require.ErrorContains(t, err, "cannot execute operation due to server.max_open_transactions_per_gateway cluster setting")
	require.NoError(t, testuserTx.Rollback())

	// Increase the limit, allowing testuser to run queries in a transaction.
	rootSQLRunner.Exec(t, "SET CLUSTER SETTING server.max_open_transactions_per_gateway = 5")
	testuserTx, err = testUserDB.Begin()
	require.NoError(t, err)
	_, err = testuserTx.Exec("SELECT 1")
	require.NoError(t, err)

	// Lower the limit again, and verify that the setting applies to transactions
	// that were already open at the time the setting changed.
	rootSQLRunner.Exec(t, "SET CLUSTER SETTING server.max_open_transactions_per_gateway = 4")
	_, err = testuserTx.Exec("SELECT 2")
	require.ErrorContains(t, err, "cannot execute operation due to server.max_open_transactions_per_gateway cluster setting")
	require.NoError(t, testuserTx.Rollback())

	// Making testuser admin should allow it to run queries.
	rootSQLRunner.Exec(t, "GRANT admin TO testuser")
	_, err = testUserDB.Exec("SELECT 1")
	require.NoError(t, err)

	testuserTx, err = testUserDB.Begin()
	require.NoError(t, err)
	_, err = testuserTx.Exec("SELECT 1")
	require.NoError(t, err)
	require.NoError(t, testuserTx.Rollback())

	require.NoError(t, tx1.Commit())
	require.NoError(t, tx2.Commit())
	require.NoError(t, tx3.Commit())
	require.NoError(t, tx4.Commit())
}
