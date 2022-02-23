// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/datadriven"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

// TestSessionMigration tests migrating a session as a data driven test.
// It supports the following directives:
// * reset: resets the connection.
// * exec: executes a SQL command
// * query: executes a SQL command and returns the output
// * dump_vars: dumps variables into a variable called with the given input.
// * compare_vars: compares two dumped variables.
// * wire_prepare: prepare a statement using the pgwire protocol.
func TestSessionMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	datadriven.Walk(t, testutils.TestDataPath(t, "session_migration"), func(t *testing.T, path string) {
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
		defer tc.Stopper().Stop(ctx)

		openConnFunc := func() *pgx.Conn {
			pgURL, cleanupGoDB, err := sqlutils.PGUrlE(
				tc.Server(0).ServingSQLAddr(),
				"StartServer", /* prefix */
				url.User(security.RootUser),
			)
			require.NoError(t, err)
			pgURL.Path = "defaultdb"

			config, err := pgx.ParseConfig(pgURL.String())
			require.NoError(t, err)
			config.PreferSimpleProtocol = true
			conn, err := pgx.ConnectConfig(ctx, config)
			require.NoError(t, err)

			tc.Server(0).Stopper().AddCloser(
				stop.CloserFn(func() {
					cleanupGoDB()
				}))

			return conn
		}
		dbConn := openConnFunc()
		defer func() {
			_ = dbConn.Close(ctx)
		}()
		_, err := dbConn.Exec(ctx, "CREATE USER testuser")
		require.NoError(t, err)

		openUserConnFunc := func(user string) *pgx.Conn {
			pgURL, cleanupGoDB, err := sqlutils.PGUrlE(
				tc.Server(0).ServingSQLAddr(),
				"StartServer", /* prefix */
				url.User(user),
			)
			require.NoError(t, err)
			pgURL.Path = "defaultdb"

			config, err := pgx.ParseConfig(pgURL.String())
			require.NoError(t, err)
			config.PreferSimpleProtocol = true
			conn, err := pgx.ConnectConfig(ctx, config)
			require.NoError(t, err)

			tc.Server(0).Stopper().AddCloser(
				stop.CloserFn(func() {
					cleanupGoDB()
				}))

			return conn
		}

		vars := make(map[string]string)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			getQuery := func() string {
				q := d.Input
				for k, v := range vars {
					q = strings.ReplaceAll(q, k, v)
				}
				return q
			}
			switch d.Cmd {
			case "reset":
				require.NoError(t, dbConn.Close(ctx))
				dbConn = openConnFunc()
				return ""
			case "user":
				require.NoError(t, dbConn.Close(ctx))
				dbConn = openUserConnFunc(d.Input)
				return ""
			case "exec":
				_, err := dbConn.Exec(ctx, getQuery())
				if err != nil {
					return err.Error()
				}
				return ""
			case "dump_vars":
				require.NotEmpty(t, d.Input, "expected table name")
				_, err := dbConn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM [SHOW ALL]", d.Input))
				require.NoError(t, err)
				return ""
			case "compare_vars":
				tables := strings.Split(d.Input, "\n")
				require.Len(t, tables, 2, "require 2 tables to compare against")

				q := `SELECT dump.variable, dump.value, dump2.variable, dump2.value
FROM dump
FULL OUTER JOIN dump2
ON ( dump.variable = dump2.variable )
WHERE dump.variable IS NULL OR dump2.variable IS NULL OR dump.variable != dump2.variable`
				for _, repl := range []struct {
					from string
					to   string
				}{
					{"dump2", tables[1]},
					{"dump", tables[0]},
				} {
					q = strings.ReplaceAll(q, repl.from, repl.to)
				}
				rows, err := dbConn.Query(ctx, q)
				require.NoError(t, err)
				ret, err := sqlutils.PGXRowsToDataDrivenOutput(rows)
				require.NoError(t, err)
				return ret
			case "query":
				q := d.Input
				for k, v := range vars {
					q = strings.ReplaceAll(q, k, v)
				}
				rows, err := dbConn.Query(ctx, getQuery())
				if err != nil {
					return err.Error()
				}
				ret, err := sqlutils.PGXRowsToDataDrivenOutput(rows)
				if err != nil {
					return err.Error()
				}
				return ret
			case "let":
				row := dbConn.QueryRow(ctx, getQuery())
				var v string
				require.NoError(t, row.Scan(&v))
				require.Len(t, d.CmdArgs, 1, "only one argument permitted for let")
				for _, arg := range d.CmdArgs {
					vars[arg.Key] = v
				}
				return ""
			case "wire_prepare":
				q := getQuery()
				require.Len(t, d.CmdArgs, 1, "only one argument permitted for wire_prepare")
				stmtName := d.CmdArgs[0].Key
				_, err := dbConn.Prepare(ctx, stmtName, q)
				require.NoError(t, err)
				return ""
			case "wire_exec":
				require.GreaterOrEqual(t, len(d.CmdArgs), 1, "at least one argument required for wire_exec")
				stmtName := d.CmdArgs[0].Key
				args := make([]interface{}, len(d.CmdArgs[1:]))
				for i, a := range d.CmdArgs[1:] {
					args[i] = a.Key
				}
				_, err := dbConn.Exec(ctx, stmtName, args...)
				if err != nil {
					return err.Error()
				}
				return ""
			case "wire_query":
				require.GreaterOrEqual(t, len(d.CmdArgs), 1, "at least one argument required for wire_query")
				stmtName := d.CmdArgs[0].Key
				args := make([]interface{}, len(d.CmdArgs[1:]))
				for i, a := range d.CmdArgs[1:] {
					args[i] = a.Key
				}
				rows, err := dbConn.Query(ctx, stmtName, args...)
				if err != nil {
					return err.Error()
				}
				ret, err := sqlutils.PGXRowsToDataDrivenOutput(rows)
				if err != nil {
					return err.Error()
				}
				return ret
			}
			t.Fatalf("unknown command: %s", d.Cmd)
			return "unexpected"
		})
	})
}
