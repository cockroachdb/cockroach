// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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

	// Create two users.
	// Use one of these users to hold open a transaction which uses a lease on
	// the role_memberships table. Ensure the other user (granted admin) can
	// create a new user without waiting for the first tx to commit.
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, "CREATE USER foo")
	tdb.Exec(t, "CREATE USER bar PASSWORD 'boss'")
	tdb.Exec(t, "GRANT admin TO bar")
	tdb.Exec(t, "CREATE DATABASE db2")
	tdb.Exec(t, "CREATE TABLE db2.public.t (i int primary key)")

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
			fooTx.QueryRow("SELECT count(*) FROM t").Scan(&count))
		require.Equal(t, 0, count)

		grantRevokeTimeout, cancel := context.WithTimeout(
			ctx, testutils.DefaultSucceedsSoonDuration,
		)
		defer cancel()

		barTx, err := barDB.BeginTx(ctx, nil)
		require.NoError(t, err)

		_, err = barTx.ExecContext(grantRevokeTimeout, "CREATE USER biz;")
		require.NoError(t, err)

		// Ensure the transaction user create transaction commits without issue
		require.NoError(t, barTx.Commit())

		// Ensure the other user table transaction commits without issue
		require.NoError(t, fooTx.Commit())
	})

}
