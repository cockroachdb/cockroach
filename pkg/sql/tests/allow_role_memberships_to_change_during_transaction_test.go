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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestAllowRoleMembershipsToChangeDuringTransaction ensures that when the
// corresponding session variable is set for all sessions using cached
// role memberships, performing new grants do not need to wait for open
// transactions to complete.
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

	// Create three users: foo, bar, and baz.
	// Use one of these users to hold open a transaction which uses a lease
	// on the role_memberships table. Ensure that initially granting does
	// wait on that transaction. Then set the session variable and ensure
	// that it does not.
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, "CREATE USER foo PASSWORD 'foo'")
	tdb.Exec(t, "CREATE USER bar")
	tdb.Exec(t, "CREATE USER baz")
	tdb.Exec(t, "GRANT admin TO foo")
	tdb.Exec(t, "CREATE DATABASE db2")
	tdb.Exec(t, "CREATE TABLE db2.public.t (i int primary key)")

	fooDB, cleanupFoo := openUser("foo", "db2")
	defer cleanupFoo()

	// In this first test, we want to make sure that the query cannot proceed
	// until the outer transaction commits. We launch the grant while the
	// transaction is open, wait a tad, make sure it hasn't made progress,
	// then we commit the relevant transaction and ensure that it does finish.
	t.Run("normal path waits", func(t *testing.T) {
		fooTx, err := fooDB.BeginTx(ctx, nil)
		require.NoError(t, err)
		var count int
		require.NoError(t,
			fooTx.QueryRow("SELECT count(*) FROM t").Scan(&count))
		require.Equal(t, 0, count)
		errCh := make(chan error, 1)
		go func() {
			_, err := sqlDB.Exec("GRANT bar TO baz;")
			errCh <- err
		}()
		select {
		case <-time.After(10 * time.Millisecond):
			// Wait a tad and make sure nothing happens. This test will be
			// flaky if we mess up the leasing.
		case err := <-errCh:
			t.Fatalf("expected transaction to block, got err: %v", err)
		}
		require.NoError(t, fooTx.Commit())
		require.NoError(t, <-errCh)
	})

	// In this test we ensure that we can perform role grant and revoke
	// operations while the transaction which uses the relevant roles
	// remains open. We ensure that the transaction still succeeds and
	// that the operations occur in a timely manner.
	t.Run("session variable prevents waiting during GRANT and REVOKE", func(t *testing.T) {
		fooConn, err := fooDB.Conn(ctx)
		require.NoError(t, err)
		defer func() { _ = fooConn.Close() }()
		_, err = fooConn.ExecContext(ctx, "SET allow_role_memberships_to_change_during_transaction = true;")
		require.NoError(t, err)
		fooTx, err := fooConn.BeginTx(ctx, nil)
		require.NoError(t, err)
		var count int
		require.NoError(t,
			fooTx.QueryRow("SELECT count(*) FROM t").Scan(&count))
		require.Equal(t, 0, count)
		conn, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()
		// Set a timeout on the SQL operations to ensure that they both
		// happen in a timely manner.
		grantRevokeTimeout, cancel := context.WithTimeout(
			ctx, testutils.DefaultSucceedsSoonDuration,
		)
		defer cancel()

		_, err = conn.ExecContext(grantRevokeTimeout, "GRANT foo TO baz;")
		require.NoError(t, err)
		_, err = conn.ExecContext(grantRevokeTimeout, "REVOKE bar FROM baz;")
		require.NoError(t, err)

		// Ensure the transaction we held open commits without issue.
		require.NoError(t, fooTx.Commit())
	})

	t.Run("session variable prevents waiting during CREATE and DROP role", func(t *testing.T) {
		fooConn, err := fooDB.Conn(ctx)
		require.NoError(t, err)
		defer func() { _ = fooConn.Close() }()
		_, err = fooConn.ExecContext(ctx, "SET allow_role_memberships_to_change_during_transaction = true;")
		require.NoError(t, err)
		fooTx, err := fooConn.BeginTx(ctx, nil)
		require.NoError(t, err)
		// We need to use show roles because that access the system.users table.
		_, err = fooTx.Exec("SHOW ROLES")
		require.NoError(t, err)

		conn, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()
		// Set a timeout on the SQL operations to ensure that they both
		// happen in a timely manner.
		grantRevokeTimeout, cancel := context.WithTimeout(
			ctx, testutils.DefaultSucceedsSoonDuration,
		)
		defer cancel()

		_, err = conn.ExecContext(grantRevokeTimeout, "CREATE ROLE new_role;")
		require.NoError(t, err)
		_, err = conn.ExecContext(grantRevokeTimeout, "DROP ROLE new_role;")
		require.NoError(t, err)

		// Ensure the transaction we held open commits without issue.
		require.NoError(t, fooTx.Commit())
	})

}
