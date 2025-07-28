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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAllowUserCreateDuringTransaction ensures that creating new users does not
// need to wait for open transactions to complete.
func TestCreateUser(t *testing.T) {
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

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, "CREATE USER foo PASSWORD 'foo'")
	tdb.Exec(t, "GRANT admin TO foo")

	fooDB, cleanup := openUser("foo", "db2")
	defer cleanup()

	t.Run("demo commit succeeds on noop ", func(t *testing.T) {
		tx, err := fooDB.BeginTx(ctx, nil)
		require.NoError(t, err)

		// https://github.com/lib/pq/blob/b7ffbd3b47da4290a4af2ccd253c74c2c22bfabf/conn.go#L83
		require.NoError(t, tx.Commit(), "should be idle in transaction?")
	})

	t.Run("demo commit fails with the create user", func(t *testing.T) {
		tx, err := fooDB.BeginTx(ctx, nil)
		require.NoError(t, err)

		_, err = tx.Exec("CREATE USER biz;")
		require.NoError(t, err, "previous wasn't committed")

		// FIXME: get error "unexpected transaction status idle"
		// https://github.com/lib/pq/blob/b7ffbd3b47da4290a4af2ccd253c74c2c22bfabf/conn.go#L83
		assert.NoError(t, tx.Commit(), "should be status idle in transaction?")
	})

	t.Run("create user has updated rows and the tx state is correct", func(t *testing.T) {
		fooTx, err := fooDB.BeginTx(ctx, nil)
		require.NoError(t, err)

		res, err := fooTx.Exec("CREATE USER baz;")
		require.NoError(t, err)
		affected, err := res.RowsAffected()
		require.NoError(t, err)

		assert.Equal(t, int64(1), affected, "should affect how many rows (user table)?")

		// Ensure the user create transaction commits without issue
		// FIXME: get error "unexpected transaction status idle"
		// https://github.com/lib/pq/blob/b7ffbd3b47da4290a4af2ccd253c74c2c22bfabf/conn.go#L83
		assert.NoError(t, fooTx.Commit(), "should be idle in transaction?")

		var count int
		require.NoError(t,
			fooTx.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&count))
		require.Equal(t, 0, count)

		require.NoError(t, fooTx.Commit())
	})

	// consider also other operations
	// var count int
	// require.NoError(t,
	// 	fooTx.QueryRowContext(ctx, "SELECT count(*) FROM system.users").Scan(&count))
	// require.GreaterOrEqual(t, count, 3)
}
