// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestAllowUserCreateDuringTransaction ensures that creating new users does not
// need to wait for open transactions to complete.
func TestAllowUserCreateDuringTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	openUser := func(username, dbName string) (_ *gosql.DB, cleanup func()) {
		db := s.ApplicationLayer().
			SQLConn(t, serverutils.UserPassword(username, username), serverutils.DBName(dbName), serverutils.ClientCerts(false))
		return db, func() {
			require.NoError(t, db.Close())
		}
	}

	// Create two users. Use one of these users to hold open a transaction which
	// uses a lease on the system.users table. Ensure the other user (granted
	// admin) can create a new user without waiting for the first tx to commit.
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, "CREATE USER foo PASSWORD 'foo'")
	tdb.Exec(t, "CREATE USER bar PASSWORD 'bar'")
	tdb.Exec(t, "GRANT admin TO bar")
	tdb.Exec(t, "CREATE DATABASE db2")
	tdb.Exec(t, "CREATE TABLE db2.public.t (i int primary key)")
	tdb.Exec(t, "GRANT SELECT ON TABLE db2.public.t TO foo;")

	fooDB, cleanup := openUser("foo", "db2")
	defer cleanup()

	barDB, cleanup := openUser("bar", "db2")
	defer cleanup()

	// In this first test, we want to make sure that an open transaction doesn't
	// cause a user creation statement to block.
	t.Run("create user isn't blocked on open transaction", func(t *testing.T) {
		fooTx, err := fooDB.BeginTx(ctx, nil)
		require.NoError(t, err)
		var count int
		require.NoError(t,
			fooTx.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&count))
		require.Equal(t, 0, count)

		barTx, err := barDB.BeginTx(ctx, nil)
		require.NoError(t, err)

		errCh := make(chan error, 1)
		go func() {
			_, err := barTx.Exec("CREATE USER biz;")
			errCh <- err
		}()
		select {
		case <-time.After(5 * time.Second):
			t.Fatal("expected statement to complete but timed out")
		case err := <-errCh:
			if err != nil {
				t.Fatalf("expected statement to succeed, got err: %v", err)
			}
		}

		// The CREATE USER statement has a quirk (#150874) that changes the
		// status of the transaction.
		// https://github.com/lib/pq/blob/b7ffbd3b47da4290a4af2ccd253c74c2c22bfabf/conn.go#L83
		require.ErrorContains(t, barTx.Commit(), "unexpected transaction status idle")

		// Ensure the other user table transaction commits without issue
		require.NoError(t, fooTx.Commit())
	})

	// TODO(#150858): add testing for blocking when there is more than a version
	// bump on a table in the transaction
	t.Run("create user is blocked in transaction with blocking operations", func(t *testing.T) {
		t.Skip("not implemented")
	})
}
