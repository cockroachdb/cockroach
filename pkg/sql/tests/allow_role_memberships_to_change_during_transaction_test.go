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

// TestAllowRoleMembershipsToChangeDuringTransaction ensures user operations
// are not blocked by transactions with the corresponding session variable set.
func TestAllowRoleMembershipsToChangeDuringTransaction(t *testing.T) {
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

	// Create four users: foo, bar, biz, and baz.
	// Use one of these users to hold open a transaction which uses a lease
	// on the role_memberships table. Ensure that initially granting does
	// wait on that transaction. Then set the session variable and ensure
	// that it does not.
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, "CREATE USER foo PASSWORD 'foo'")
	tdb.Exec(t, "CREATE USER bar")
	tdb.Exec(t, "CREATE USER biz")
	tdb.Exec(t, "CREATE USER baz")
	tdb.Exec(t, "GRANT admin TO foo")
	tdb.Exec(t, "CREATE DATABASE db2")
	tdb.Exec(t, "CREATE TABLE db2.public.t (i int primary key)")

	fooDB, cleanupFoo := openUser("foo", "db2")
	defer cleanupFoo()

	// Ensure that the outer transaction blocks the user admin operations until
	// it is committed and its lease on the initial versions of the tables are
	// released.
	t.Run("normal path waits", func(t *testing.T) {
		outerTx, err := fooDB.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = outerTx.Rollback() }()

		var count int
		require.NoError(t,
			outerTx.QueryRow("SELECT count(*) FROM t").Scan(&count))
		require.Equal(t, 0, count)

		// the first user operation shouldn't block
		_, err = sqlDB.Exec("GRANT biz TO bar;")
		require.NoError(t, err)

		// the outer tx still has a lease on the initial table version so this should block
		errCh := make(chan error, 1)
		go func() {
			_, err = sqlDB.Exec("GRANT baz TO bar;")
			errCh <- err
		}()
		select {
		case <-time.After(10 * time.Millisecond):
			// Wait a tad and make sure nothing happens. This test will be
			// flaky if we mess up the leasing.
		case err := <-errCh:
			t.Fatalf("expected transaction to block, got err: %v", err)
		}
		require.NoError(t, outerTx.Commit())
		require.NoError(t, <-errCh)
	})

	// When the session variable is set, the outer transaction should not block
	// the user admin operations as it releases the leases after every
	// statement.
	t.Run("session variable prevents blocking", func(t *testing.T) {
		fooConn, err := fooDB.Conn(ctx)
		require.NoError(t, err)
		defer func() { _ = fooConn.Close() }()

		outerTx, err := fooConn.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = outerTx.Rollback() }()

		_, err = outerTx.ExecContext(ctx, "SET allow_role_memberships_to_change_during_transaction = true;")
		require.NoError(t, err)

		var count int
		require.NoError(t,
			outerTx.QueryRow("SELECT count(*) FROM t").Scan(&count))
		require.Equal(t, 0, count)

		// the first user operation shouldn't block
		_, err = sqlDB.Exec("GRANT biz TO bar;")
		require.NoError(t, err)

		// nor should the second
		_, err = sqlDB.Exec("GRANT baz TO bar;")
		require.NoError(t, err)

		require.NoError(t, outerTx.Commit())
	})
}
