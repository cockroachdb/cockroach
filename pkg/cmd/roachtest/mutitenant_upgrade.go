// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	gosql "database/sql"
	"net/url"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/stretchr/testify/require"
)

func registerMultiTenantUpgrade(r *testRegistry) {
	r.Add(testSpec{
		Name:              "multitenant-upgrade",
		MinVersion:        "v21.1.0",
		Cluster:           makeClusterSpec(1),
		Owner:             OwnerKV,
		NonReleaseBlocker: false,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runMultiTenantUpgrade(ctx, t, c, r.buildVersion)
		},
	})
}

func runMultiTenantUpgrade(ctx context.Context, t *test, c *cluster, v version.Version) {
	predecessor, err := PredecessorVersion(v)
	require.NoError(t, err)

	currentBinary := uploadVersion(ctx, t, c, c.All(), "")
	predecessorBinary := uploadVersion(ctx, t, c, c.All(), predecessor)

	c.Start(ctx, t, c.All(), startArgs("--binary="+predecessorBinary))

	{
		_, err := c.Conn(ctx, 1).Exec(`SELECT crdb_internal.create_tenant(123)`)
		require.NoError(t, err)
	}

	kvAddrs := c.ExternalAddr(ctx, c.All())

	errCh := startTenantServer(c, ctx, predecessorBinary, kvAddrs,
		"--log-dir=logs/mt")
	u, err := url.Parse(c.ExternalPGUrl(ctx, c.Node(1))[0])
	require.NoError(t, err)
	u.Host = c.ExternalIP(ctx, c.Node(1))[0] + ":36257"
	url := u.String()
	c.l.Printf("sql server should be running at %s", url)

	time.Sleep(time.Second)

	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}

	t.Status("checking that a client can connect to the tenant server")

	verifySQL(t, url,
		mkStmt(`CREATE TABLE foo (id INT PRIMARY KEY, v STRING)`),
		mkStmt(`INSERT INTO foo VALUES($1, $2)`, 1, "bar"),
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}))

	t.Status("preserving downgrade option on kv server")
	{
		_, err := c.Conn(ctx, 1).Exec(`SET CLUSTER SETTING cluster.preserve_downgrade_option = '20.2'`)
		require.NoError(t, err)
	}

	t.Status("upgrading kv server")
	c.Stop(ctx, c.All())
	c.Start(ctx, t, c.All(), startArgs("--binary="+currentBinary))
	time.Sleep(time.Second)

	t.Status("checking the sql server still works")
	verifySQL(t, url,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}))

	t.Status("stopping the tenant server ahead of upgrading")

	// Must use pkill because the context cancellation doesn't wait for the
	// process to exit
	c.Run(ctx, c.All(), "pkill -o -f '^"+predecessorBinary+" mt start'")
	t.logger().Printf("mt cluster exited: %v", <-errCh)

	t.Status("starting the upgraded tenant server")

	errCh = startTenantServer(c, ctx, currentBinary, kvAddrs,
		// Ensure that log files get created.
		"--log='file-defaults: {dir: logs/mt}'")

	time.Sleep(time.Second)
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}

	t.Status("migrating the tenant server")

	verifySQL(t, url,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}),
		mkStmt("SHOW CLUSTER SETTING version").
			withResults([][]string{{"20.2"}}),
		mkStmt("SET CLUSTER SETTING version = crdb_internal.node_executable_version()"),
		mkStmt("SHOW CLUSTER SETTING version").
			withResults([][]string{{"20.2-50"}}),
	)

	t.Status("finalizing the kv server")
	{
		_, err := c.Conn(ctx, 1).Exec(`SET CLUSTER SETTING cluster.preserve_downgrade_option = DEFAULT`)
		require.NoError(t, err)
	}
	sqlutils.MakeSQLRunner(c.Conn(ctx, 1)).CheckQueryResultsRetry(t,
		`SHOW CLUSTER SETTING version`, [][]string{{"20.2-50"}})

}

func startTenantServer(
	c *cluster, tenantCtx context.Context, binary string, kvAddrs []string, logFlags string,
) chan error {

	args := []string{
		// TODO(tbg): make this test secure.
		// "--certs-dir", "certs",
		"--insecure",
		"--tenant-id", "123",
		"--http-addr", ifLocal("127.0.0.1", "0.0.0.0") + ":8081",
		"--kv-addrs", strings.Join(kvAddrs, ","),
		// Don't bind to external interfaces when running locally.
		"--sql-addr", ifLocal("127.0.0.1", "0.0.0.0") + ":36257",
	}
	if logFlags != "" {
		args = append(args, logFlags)
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.RunE(tenantCtx, c.Node(1),
			append([]string{binary, "mt", "start-sql"}, args...)...,
		)
		close(errCh)
	}()
	return errCh
}

type sqlVerificationStmt struct {
	stmt            string
	args            []interface{}
	optionalResults [][]string
}

func (s sqlVerificationStmt) withResults(res [][]string) sqlVerificationStmt {
	s.optionalResults = res
	return s
}

func mkStmt(stmt string, args ...interface{}) sqlVerificationStmt {
	return sqlVerificationStmt{stmt: stmt, args: args}
}

func verifySQL(t *test, url string, stmts ...sqlVerificationStmt) {
	db, err := gosql.Open("postgres", url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	tdb := sqlutils.MakeSQLRunner(db)

	for _, stmt := range stmts {
		if stmt.optionalResults == nil {
			tdb.Exec(t, stmt.stmt, stmt.args...)
		} else {
			res := tdb.QueryStr(t, stmt.stmt, stmt.args...)
			require.Equal(t, stmt.optionalResults, res)
		}
	}
}
