// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package isession_test

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var sd = sql.SessionArgs{
	// TODO(jeffswenson): what user does the internal executor use?
	User:        username.RootUserName(),
	IsSuperuser: true,
	SessionDefaults: sql.SessionDefaults{
		// TODO(jeffswenson): should we use a database in the session or should
		// we require every name to be fully qualified?
		"database":                               "defaultdb",
		"kv_transaction_buffered_writes_enabled": "false",
		"plan_cache_mode":                        "force_generic_plan",
		"vectorize":                              "off",
		"distsql":                                "off",
	},
}

// TODO(jeffswenson): testing plan
// 1. Txn retries serializable errors.
// 2. Verify that each statement is inside its own savepoint.
//
// Idea: a test that has a bunch of functions that cause errors on the session, then verify the session can still
// start transactions, query, and execute prepared statements.
// 1. Cancel a query blocked on a sql lock.
// 2. Parse failures in prepared statements.
// 3. SQL constraint errors.
// 4. Insert failures.

func TestInternalSessionPrepare(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	db := s.InternalDB().(descs.DB)
	session, err := db.Session(ctx, "test-session")
	require.NoError(t, err)
	defer session.Close(ctx)

	stmt, err := parser.ParseOne("SELECT 1")
	require.NoError(t, err)

	prepared, err := session.Prepare(ctx, "test-stmt", stmt, nil)
	require.NoError(t, err)
	require.NotNil(t, prepared)

	for i := 0; i < 10; i++ {
		rows, err := session.Execute(ctx, prepared, nil)
		require.NoError(t, err)
		require.Equal(t, 1, rows)
	}
}

func TestInternalSessionInsertAndVerify(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a test table using a regular SQL client
	db := s.SQLConn(t)
	_, err := db.Exec("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	require.NoError(t, err)

	// Create an internal session
	idb := s.InternalDB().(descs.DB)
	session, err := idb.Session(ctx, "test-session")
	require.NoError(t, err)
	defer session.Close(ctx)

	// Prepare an insert statement
	stmt, err := parser.ParseOne("INSERT INTO defaultdb.test VALUES ($1, $2)")
	require.NoError(t, err)

	prepared, err := session.Prepare(ctx, "insert-stmt", stmt, []*types.T{types.Int, types.Int})
	require.NoError(t, err)
	require.NotNil(t, prepared)

	// Execute the insert multiple times
	for i := 1; i <= 3; i++ {
		rows, err := session.Execute(ctx, prepared, tree.Datums{
			tree.NewDInt(tree.DInt(i)),
			tree.NewDInt(tree.DInt(i * 10)),
		})
		require.NoError(t, err)
		require.Equal(t, 1, rows)
	}

	// Verify the data using a regular SQL client
	results := sqlutils.MakeSQLRunner(db).QueryStr(t, "SELECT id, val FROM test ORDER BY id")
	require.Equal(t, [][]string{
		{"1", "10"},
		{"2", "20"},
		{"3", "30"},
	}, results)
}

func TestTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a test table using a regular SQL client
	db := s.SQLConn(t)
	_, err := db.Exec("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	require.NoError(t, err)

	idb := s.InternalDB().(descs.DB)
	session, err := idb.Session(ctx, "test-session")
	require.NoError(t, err)
	defer session.Close(ctx)

	// Prepare an insert statement
	stmt, err := parser.ParseOne("INSERT INTO defaultdb.test VALUES ($1, $2)")
	require.NoError(t, err)

	prepared, err := session.Prepare(ctx, "test-stmt", stmt, nil)
	require.NoError(t, err)
	require.NotNil(t, prepared)

	err = session.Txn(ctx, func(ctx context.Context) error {
		_, err := session.Execute(ctx, prepared, tree.Datums{
			tree.NewDInt(tree.DInt(1)),
			tree.NewDInt(tree.DInt(2 * 10)),
		})
		if err != nil {
			return err
		}
		return errors.New("abort the transaction")
	})
	require.ErrorContains(t, err, "abort the transaction")

	err = session.Txn(ctx, func(ctx context.Context) error {
		_, err := session.Execute(ctx, prepared, tree.Datums{
			tree.NewDInt(tree.DInt(2)),
			tree.NewDInt(tree.DInt(2 * 10)),
		})
		if err != nil {
			return err
		}
		_, err = session.Execute(ctx, prepared, tree.Datums{
			tree.NewDInt(tree.DInt(3)),
			tree.NewDInt(tree.DInt(3 * 10)),
		})
		if err != nil {
			return err
		}
		return nil
	})

	// Verify the data using a regular SQL client
	results := sqlutils.MakeSQLRunner(db).QueryStr(t, "SELECT id, val FROM test ORDER BY id")
	require.Equal(t, [][]string{
		{"2", "20"},
		{"3", "30"},
	}, results)
}

func TestInternalSessionQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a test table using a regular SQL client
	db := s.SQLConn(t)
	_, err := db.Exec("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	require.NoError(t, err)

	// Insert test data
	_, err = db.Exec("INSERT INTO defaultdb.test VALUES (1, 10), (2, 20), (3, 30)")
	require.NoError(t, err)

	// Create an internal session
	idb := s.InternalDB().(descs.DB)
	session, err := idb.Session(ctx, "test-session")
	require.NoError(t, err)
	defer session.Close(ctx)

	// Prepare a select statement
	stmt, err := parser.ParseOne("SELECT id, val FROM defaultdb.test")
	require.NoError(t, err)

	prepared, err := session.Prepare(ctx, "select-stmt", stmt, nil)
	require.NoError(t, err)
	require.NotNil(t, prepared)

	// Query and verify basic functionality
	rows, err := session.Query(ctx, prepared, nil)
	require.NoError(t, err)
	require.Equal(t, []tree.Datums{
		{tree.NewDInt(tree.DInt(1)), tree.NewDInt(tree.DInt(10))},
		{tree.NewDInt(tree.DInt(2)), tree.NewDInt(tree.DInt(20))},
		{tree.NewDInt(tree.DInt(3)), tree.NewDInt(tree.DInt(30))},
	}, rows)
}

func TestInternalSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// This is a collection of tests that should all leave the internal session
	// in a valid state. The order is randomized to ensure there is no accidental
	// dependency on the order of operations. If the test fails, it is likely that
	// one of the earlier operations left the session in an invalid state.

	type tc struct {
		name string
		do   func(*testing.T, isql.Session, serverutils.ApplicationLayerInterface)
	}
	tests := []tc{
		{"tcTxnReadsItsOwnWrites", tcTxnReadsItsOwnWrites},
		{"tcCancelQueryBlockedOnSQLLock", tcCancelQueryBlockedOnSQLLock},
		{"tcRetrySerializableError", tcRetrySerializableError},
		{"tcInsertConflict", tcInsertConflict},
		{"tcTableNotFount", tcTableNotFound},
	}

	rand.Shuffle(len(tests), func(i, j int) {
		tests[i], tests[j] = tests[j], tests[i]
	})

	session, err := s.InternalDB().(descs.DB).Session(ctx, "test-session")
	require.NoError(t, err)
	defer session.Close(ctx)

	stmt, err := parser.ParseOne("SELECT 1")
	require.NoError(t, err)

	hc, err := session.Prepare(ctx, "health-check", stmt, nil)
	require.NoError(t, err)

	// TODO: after each test run `SELECT 1` to ensure the session is still mostly valid.
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.do(t, session, s.ApplicationLayer())

			rows, err := session.Query(ctx, hc, nil)
			require.NoError(t, err)
			require.Equal(t, []tree.Datums{
				{tree.NewDInt(tree.DInt(1))},
			}, rows)
		})
	}
}

func tcTxnReadsItsOwnWrites(
	t *testing.T, session isql.Session, server serverutils.ApplicationLayerInterface,
) {
	ctx := context.Background()

	// Create a table for the test
	db := server.SQLConn(t)
	_, err := db.Exec("CREATE TABLE defaultdb.read_your_writes (id INT PRIMARY KEY, val INT)")
	require.NoError(t, err)

	// Prepare an insert statement
	stmt, err := parser.ParseOne("INSERT INTO defaultdb.read_your_writes VALUES ($1, $2)")
	require.NoError(t, err)

	prepared, err := session.Prepare(ctx, "txn-reads-own-writes-insert", stmt, []*types.T{types.Int, types.Int})
	require.NoError(t, err)
	require.NotNil(t, prepared)

	// Prepare a select statement
	selectStmt, err := parser.ParseOne("SELECT val FROM defaultdb.read_your_writes WHERE id = $1")
	require.NoError(t, err)

	selectPrepared, err := session.Prepare(ctx, "txn-reads-own-writes-select", selectStmt, []*types.T{types.Int})
	require.NoError(t, err)
	require.NotNil(t, selectPrepared)

	// In a txn, insert a row and then read it back.
	err = session.Txn(ctx, func(ctx context.Context) error {
		// Insert a row
		_, err := session.Execute(ctx, prepared, tree.Datums{
			tree.NewDInt(tree.DInt(1)),
			tree.NewDInt(tree.DInt(42)),
		})
		if err != nil {
			return err
		}

		// Read it back - should see our own write
		rows, err := session.Query(ctx, selectPrepared, tree.Datums{tree.NewDInt(tree.DInt(1))})
		if err != nil {
			return err
		}
		require.Equal(t, []tree.Datums{
			{tree.NewDInt(tree.DInt(42))},
		}, rows)

		// Abort the txn by returning a non-retryable error
		return errors.New("abort the transaction")
	})
	require.ErrorContains(t, err, "abort the transaction")

	// Ensure the row was not committed by checking from outside the transaction
	rows, err := session.Query(ctx, selectPrepared, tree.Datums{tree.NewDInt(tree.DInt(1))})
	require.NoError(t, err)
	require.Empty(t, rows) // Should be empty since transaction was aborted
}

func tcCancelQueryBlockedOnSQLLock(
	t *testing.T, session isql.Session, server serverutils.ApplicationLayerInterface,
) {
	ctx := context.Background()

	// Create a table for the test
	db := server.SQLConn(t)
	_, err := db.Exec("CREATE TABLE defaultdb.lock_test (id INT PRIMARY KEY, val INT)")
	require.NoError(t, err)

	// Insert initial data
	_, err = db.Exec("INSERT INTO defaultdb.lock_test VALUES (1, 10)")
	require.NoError(t, err)

	// Create a conflicting transaction.
	txn, err := db.Begin()
	require.NoError(t, err)

	// Acquire a lock by updating the row
	_, err = txn.Exec("UPDATE defaultdb.lock_test SET val = 20 WHERE id = 1")
	require.NoError(t, err)

	// Prepare a conflicting update statement
	stmt, err := parser.ParseOne("UPDATE defaultdb.lock_test SET val = 30 WHERE id = 1")
	require.NoError(t, err)

	prepared, err := session.Prepare(ctx, "lock-test-update", stmt, nil)
	require.NoError(t, err)

	// Create a context that will timeout after 100ms
	queryCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	_, err = session.Execute(queryCtx, prepared, nil)
	require.ErrorContains(t, err, "query execution canceled")

	require.NoError(t, txn.Rollback())

	// Verify the original data is still there (no update occurred)
	selectStmt, err := parser.ParseOne("SELECT val FROM defaultdb.lock_test WHERE id = 1")
	require.NoError(t, err)

	selectPrepared, err := session.Prepare(ctx, "lock-test-select", selectStmt, nil)
	require.NoError(t, err)

	rows, err := session.Query(ctx, selectPrepared, nil)
	require.NoError(t, err)
	require.Equal(t, []tree.Datums{
		{tree.NewDInt(tree.DInt(10))}, // Original value, not updated
	}, rows)
}

func tcRetrySerializableError(
	t *testing.T, session isql.Session, server serverutils.ApplicationLayerInterface,
) {
	ctx := context.Background()

	// Create a table for the test with two accounts
	db := server.SQLConn(t)
	_, err := db.Exec("CREATE TABLE defaultdb.accounts (id INT PRIMARY KEY, balance INT)")
	require.NoError(t, err)

	// Insert two accounts with initial balances
	_, err = db.Exec("INSERT INTO defaultdb.accounts VALUES (1, 100), (2, 100)")
	require.NoError(t, err)

	// Prepare statements for the session
	readStmt, err := parser.ParseOne("SELECT balance FROM defaultdb.accounts WHERE id = $1")
	require.NoError(t, err)

	readPrepared, err := session.Prepare(ctx, "retry-read", readStmt, []*types.T{types.Int})
	require.NoError(t, err)

	updateStmt, err := parser.ParseOne("UPDATE defaultdb.accounts SET balance = $1 WHERE id = $2")
	require.NoError(t, err)

	updatePrepared, err := session.Prepare(ctx, "retry-update", updateStmt, []*types.T{types.Int, types.Int})
	require.NoError(t, err)

	// Channel to coordinate the conflicting transaction
	readyToConflict := make(chan struct{})
	conflictComplete := make(chan struct{})

	// Start a conflicting transaction using a regular SQL connection
	go func() {
		txn, err := db.Begin()
		require.NoError(t, err)
		defer func() {
			_ = txn.Rollback()
		}()

		// Read account 1's balance
		var balance1 int
		err = txn.QueryRow("SELECT balance FROM defaultdb.accounts WHERE id = 1").Scan(&balance1)
		require.NoError(t, err)
		require.Equal(t, 100, balance1)

		// Signal that we're ready to create the conflict
		close(readyToConflict)

		// Wait a bit to ensure the session transaction has also read the data
		time.Sleep(50 * time.Millisecond)

		// Update account 1's balance (this will conflict with the session transaction)
		_, err = txn.Exec("UPDATE defaultdb.accounts SET balance = balance - 10 WHERE id = 1")
		require.NoError(t, err)

		// Commit the transaction
		require.NoError(t, txn.Commit())

		// Signal that the conflict is complete
		close(conflictComplete)
	}()

	// Wait for the conflicting transaction to be ready
	<-readyToConflict

	// Execute a transaction in the session that will encounter a serializable error
	err = session.Txn(ctx, func(ctx context.Context) error {
		// Read account 1's balance
		rows, err := session.Query(ctx, readPrepared, tree.Datums{tree.NewDInt(tree.DInt(1))})
		if err != nil {
			return err
		}
		require.Equal(t, 1, len(rows))
		balance := int(tree.MustBeDInt(rows[0][0]))

		// Wait for the conflicting transaction to complete its update
		<-conflictComplete

		// Try to update account 1's balance based on what we read
		// This should cause a serializable error because the balance has changed
		_, err = session.Execute(ctx, updatePrepared, tree.Datums{
			tree.NewDInt(tree.DInt(balance - 5)), // Update based on old balance
			tree.NewDInt(tree.DInt(1)),
		})
		if err != nil {
			return err
		}

		return nil
	})

	// The transaction should succeed after retrying
	require.NoError(t, err)

	// Verify the final state
	// Account 1 should have balance 85 (100 - 10 - 5)
	// Account 2 should still have balance 100
	rows, err := session.Query(ctx, readPrepared, tree.Datums{tree.NewDInt(tree.DInt(1))})
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
	require.Equal(t, 85, int(tree.MustBeDInt(rows[0][0])))

	rows, err = session.Query(ctx, readPrepared, tree.Datums{tree.NewDInt(tree.DInt(2))})
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
	require.Equal(t, 100, int(tree.MustBeDInt(rows[0][0])))
}

func tcInsertConflict(
	t *testing.T, session isql.Session, server serverutils.ApplicationLayerInterface,
) {
	ctx := context.Background()

	// Create a table for the test with a unique constraint
	db := server.SQLConn(t)
	_, err := db.Exec("CREATE TABLE defaultdb.insert_conflict (id INT PRIMARY KEY, val INT)")
	require.NoError(t, err)

	// Prepare an insert statement
	stmt, err := parser.ParseOne("INSERT INTO defaultdb.insert_conflict VALUES ($1, $2)")
	require.NoError(t, err)

	prepared, err := session.Prepare(ctx, "insert-conflict-stmt", stmt, []*types.T{types.Int, types.Int})
	require.NoError(t, err)
	require.NotNil(t, prepared)

	_, err = session.Execute(ctx, prepared, tree.Datums{
		tree.NewDInt(tree.DInt(1)),
		tree.NewDInt(tree.DInt(10)),
	})
	require.NoError(t, err)

	// Second insert with same ID - should fail with primary key conflict
	_, err = session.Execute(ctx, prepared, tree.Datums{
		tree.NewDInt(tree.DInt(1)), // Same ID as first insert
		tree.NewDInt(tree.DInt(20)),
	})
	require.ErrorContains(t, err, "duplicate key value")

	_, err = session.Execute(ctx, prepared, tree.Datums{
		tree.NewDInt(tree.DInt(3)),
		tree.NewDInt(tree.DInt(30)),
	})
	require.NoError(t, err)

	selectStmt, err := parser.ParseOne("SELECT id, val FROM defaultdb.insert_conflict ORDER BY id")
	require.NoError(t, err)

	selectPrepared, err := session.Prepare(ctx, "insert-conflict-query", selectStmt, nil)
	require.NoError(t, err)
	require.NotNil(t, selectPrepared)

	rows, err := session.Query(ctx, selectPrepared, nil)
	require.NoError(t, err)
	require.Equal(t, []tree.Datums{
		{tree.NewDInt(tree.DInt(1)), tree.NewDInt(tree.DInt(10))},
		{tree.NewDInt(tree.DInt(3)), tree.NewDInt(tree.DInt(30))},
	}, rows)
}

func tcTableNotFound(
	t *testing.T, session isql.Session, server serverutils.ApplicationLayerInterface,
) {
	ctx := context.Background()

	// Try to query a table that does not exist.
	stmt, err := parser.ParseOne("SELECT * FROM defaultdb.non_existent_table")
	require.NoError(t, err)

	// The prepare should fail because the table doesn't exist
	_, err = session.Prepare(ctx, "table-not-found-select", stmt, nil)
	require.ErrorContains(t, err, "relation \"defaultdb.non_existent_table\" does not exist")
}
