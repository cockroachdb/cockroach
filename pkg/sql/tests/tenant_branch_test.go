// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestTenantBranch runs end-to-end SQL scenarios against branched virtual
// clusters. Each datadriven file is a self-contained narrative showing how
// CREATE VIRTUAL CLUSTER ... BRANCH FROM behaves: the parent's data is
// visible from the branch via copy-on-write fallthrough, the branch's
// writes are isolated, and DELETEs on the branch shadow parent rows
// without touching them.
//
// Directives:
//
//   exec-sql tenant=<name>
//     Runs one or more statements against the named tenant. "system" routes
//     to the host; any other name connects to a shared-process tenant.
//
//   query-sql tenant=<name>
//     Runs a query and prints rows in datadriven format.
func TestTenantBranch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, datapathutils.TestDataPath(t, "tenant_branch"), func(t *testing.T, path string) {
		ctx := context.Background()
		srv := serverutils.StartServerOnly(t, base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		})
		defer srv.Stopper().Stop(ctx)

		systemDB := srv.SystemLayer().SQLConn(t)
		conns := map[string]*gosql.DB{"system": systemDB}
		defer func() {
			for name, db := range conns {
				if name != "system" {
					_ = db.Close()
				}
			}
		}()

		// connFor returns a *gosql.DB connected to the named tenant. For
		// non-system tenants it waits for the shared service to accept
		// connections; the caller is responsible for having issued
		// ALTER VIRTUAL CLUSTER ... START SERVICE SHARED first.
		connFor := func(name string) *gosql.DB {
			if db, ok := conns[name]; ok {
				return db
			}
			var db *gosql.DB
			testutils.SucceedsSoon(t, func() error {
				conn, err := srv.SystemLayer().SQLConnE(
					serverutils.DBName("cluster:" + name + "/defaultdb"))
				if err != nil {
					return err
				}
				if err := conn.Ping(); err != nil {
					_ = conn.Close()
					return err
				}
				db = conn
				return nil
			})
			conns[name] = db
			return db
		}

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			tenant := "system"
			for _, arg := range d.CmdArgs {
				if arg.Key == "tenant" {
					require.Lenf(t, arg.Vals, 1, "tenant= takes one value")
					tenant = arg.Vals[0]
				}
			}
			db := connFor(tenant)

			switch d.Cmd {
			case "exec-sql":
				// Run each statement in its own implicit txn. Some DDL
				// (e.g. ALTER VIRTUAL CLUSTER ... START SERVICE) is
				// rejected inside a multi-statement transaction.
				stmts, err := parser.Parse(d.Input)
				require.NoError(t, err)
				for _, s := range stmts {
					if _, err := db.Exec(s.SQL); err != nil {
						return err.Error()
					}
				}
				return ""
			case "query-sql":
				rows, err := db.Query(d.Input)
				if err != nil {
					return err.Error()
				}
				out, err := sqlutils.RowsToDataDrivenOutput(rows)
				require.NoError(t, err)
				return out
			default:
				return fmt.Sprintf("unknown command %s", d.Cmd)
			}
		})
	})
}
