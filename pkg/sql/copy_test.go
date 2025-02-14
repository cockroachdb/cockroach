// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/jackc/pgx/v5"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// TestCopyLogging verifies copy works with logging turned on.
func TestCopyLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	for _, strings := range [][]string{
		{`SET CLUSTER SETTING sql.log.all_statements.enabled = true`},
		{`SET CLUSTER SETTING sql.telemetry.query_sampling.enabled = true`},
		{`SET CLUSTER SETTING sql.log.admin_audit.enabled = true`},
	} {
		t.Run(strings[0], func(t *testing.T) {
			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(context.Background())

			_, err := db.Exec(`CREATE TABLE t (i INT PRIMARY KEY);`)
			require.NoError(t, err)
			_, err = db.Exec(`CREATE USER testuser`)
			require.NoError(t, err)

			for _, str := range strings {
				_, err = db.Exec(str)
				require.NoError(t, err)
			}

			pgURL, cleanupGoDB, err := pgurlutils.PGUrlE(
				s.AdvSQLAddr(),
				"StartServer", /* prefix */
				url.User(username.RootUser),
			)
			require.NoError(t, err)
			s.Stopper().AddCloser(stop.CloserFn(func() { cleanupGoDB() }))
			config, err := pgx.ParseConfig(pgURL.String())
			require.NoError(t, err)

			const val = 2

			// We have to start a new connection every time to exercise all possible paths.
			t.Run("success during COPY FROM", func(t *testing.T) {
				db := s.SQLConn(t)
				txn, err := db.Begin()
				require.NoError(t, err)
				{
					stmt, err := txn.Prepare(pq.CopyIn("t", "i"))
					require.NoError(t, err)
					_, err = stmt.Exec(val)
					require.NoError(t, err)
					require.NoError(t, stmt.Close())
				}
				require.NoError(t, txn.Commit())

				var i int
				require.NoError(t, db.QueryRow("SELECT i FROM t").Scan(&i))
				require.Equal(t, val, i)
			})

			t.Run("success during COPY TO", func(t *testing.T) {
				conn, err := pgx.ConnectConfig(ctx, config)
				require.NoError(t, err)
				defer func() {
					require.NoError(t, conn.Close(ctx))
				}()

				var buf bytes.Buffer
				_, err = conn.PgConn().CopyTo(ctx, &buf, "COPY t(i) TO STDOUT")
				require.NoError(t, err)

				require.Equal(t, fmt.Sprintf("%d\n", val), buf.String())
			})

			t.Run("error in statement", func(t *testing.T) {
				db := s.SQLConn(t)
				txn, err := db.Begin()
				require.NoError(t, err)
				{
					_, err := txn.Prepare(pq.CopyIn("xxx", "yyy"))
					require.Error(t, err)
					require.ErrorContains(t, err, `relation "xxx" does not exist`)
				}
				require.NoError(t, txn.Rollback())
			})

			t.Run("error during COPY FROM", func(t *testing.T) {
				db := s.SQLConn(t)
				txn, err := db.Begin()
				require.NoError(t, err)
				{
					stmt, err := txn.Prepare(pq.CopyIn("t", "i"))
					require.NoError(t, err)
					_, err = stmt.Exec("bob")
					require.NoError(t, err)
					err = stmt.Close()
					require.Error(t, err)
					require.ErrorContains(t, err, `could not parse "bob" as type int`)
				}
				require.NoError(t, txn.Rollback())
			})

			t.Run("error in statement during COPY FROM", func(t *testing.T) {
				db := s.SQLConn(t)
				txn, err := db.Begin()
				require.NoError(t, err)
				{
					_, err := txn.Prepare(pq.CopyIn("xxx", "yyy"))
					require.Error(t, err)
					require.ErrorContains(t, err, `relation "xxx" does not exist`)
				}
				require.NoError(t, txn.Rollback())
			})

			t.Run("error during insert phase of COPY FROM", func(t *testing.T) {
				db := s.SQLConn(t)
				txn, err := db.Begin()
				require.NoError(t, err)
				{
					stmt, err := txn.Prepare(pq.CopyIn("t", "i"))
					require.NoError(t, err)
					_, err = stmt.Exec("2")
					require.NoError(t, err)
					err = stmt.Close()
					require.Error(t, err)
					require.ErrorContains(t, err, `duplicate key value violates unique constraint "t_pkey"`)
				}
				require.NoError(t, txn.Rollback())
			})

			t.Run("error in statement during COPY TO", func(t *testing.T) {
				conn, err := pgx.ConnectConfig(ctx, config)
				require.NoError(t, err)
				defer func() {
					require.NoError(t, conn.Close(ctx))
				}()

				var buf bytes.Buffer
				_, err = conn.PgConn().CopyTo(ctx, &buf, "COPY xxx(i) TO STDOUT")
				require.Error(t, err)
				require.ErrorContains(t, err, `relation "xxx" does not exist`)
			})

			t.Run("error during copy during COPY FROM", func(t *testing.T) {
				db := s.SQLConn(t)
				txn, err := db.Begin()
				require.NoError(t, err)
				{
					stmt, err := txn.Prepare(pq.CopyIn("t", "i"))
					require.NoError(t, err)
					_, err = stmt.Exec("bob")
					require.NoError(t, err)
					err = stmt.Close()
					require.Error(t, err)
					require.ErrorContains(t, err, `could not parse "bob" as type int`)
				}
				require.NoError(t, txn.Rollback())
			})

			t.Run("no privilege on table", func(t *testing.T) {
				pgURL, cleanup := pgurlutils.PGUrl(t, s.AdvSQLAddr(), "copy_test", url.User(username.TestUser))
				defer cleanup()
				conn, err := pgx.Connect(ctx, pgURL.String())
				require.NoError(t, err)
				err = conn.PgConn().Exec(ctx, `COPY t FROM STDIN`).Close()
				require.ErrorContains(t, err, "user testuser does not have INSERT privilege on relation t")
			})
		})
	}
}
