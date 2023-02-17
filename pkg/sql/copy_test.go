// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/jackc/pgx/v4"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// TestCopyLogging verifies copy works with logging turned on.
func TestCopyLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	for _, strings := range [][]string{
		{`SET CLUSTER SETTING sql.trace.log_statement_execute = true`},
		{`SET CLUSTER SETTING sql.telemetry.query_sampling.enabled = true`},
		{`SET CLUSTER SETTING sql.log.unstructured_entries.enabled = true`, `SET CLUSTER SETTING sql.trace.log_statement_execute = true`},
		{`SET CLUSTER SETTING sql.log.admin_audit.enabled = true`},
	} {
		t.Run(strings[0], func(t *testing.T) {
			params, _ := tests.CreateTestServerParams()
			s, db, _ := serverutils.StartServer(t, params)
			defer s.Stopper().Stop(context.Background())

			_, err := db.Exec(`
		CREATE TABLE t (
			i INT PRIMARY KEY
		);
	`)
			require.NoError(t, err)

			for _, str := range strings {
				_, err = db.Exec(str)
				require.NoError(t, err)
			}

			pgURL, cleanupGoDB, err := sqlutils.PGUrlE(
				s.ServingSQLAddr(),
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
				db := serverutils.OpenDBConn(
					t, s.ServingSQLAddr(), params.UseDatabase, params.Insecure, s.Stopper())
				require.NoError(t, err)
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
				db := serverutils.OpenDBConn(
					t, s.ServingSQLAddr(), params.UseDatabase, params.Insecure, s.Stopper())
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
				db := serverutils.OpenDBConn(
					t, s.ServingSQLAddr(), params.UseDatabase, params.Insecure, s.Stopper())
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
				db := serverutils.OpenDBConn(
					t, s.ServingSQLAddr(), params.UseDatabase, params.Insecure, s.Stopper())
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
				db := serverutils.OpenDBConn(
					t, s.ServingSQLAddr(), params.UseDatabase, params.Insecure, s.Stopper())
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
				db := serverutils.OpenDBConn(
					t, s.ServingSQLAddr(), params.UseDatabase, params.Insecure, s.Stopper())
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
		})
	}
}
