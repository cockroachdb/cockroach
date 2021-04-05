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
	"fmt"
	"net/url"
	"strconv"
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
		Cluster:           makeClusterSpec(2),
		Owner:             OwnerKV,
		NonReleaseBlocker: false,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runMultiTenantUpgrade(ctx, t, c, r.buildVersion)
		},
	})
}

// tenantNode corresponds to a running tenant.
type tenantNode struct {
	tenantID          int
	httpPort, sqlPort int
	kvAddrs           []string
	pgURL             string

	binary string
	errCh  chan error
	node   int
}

func createTenantNode(
	ctx context.Context,
	t *test,
	c *cluster,
	binary string,
	kvAddrs []string,
	tenantID int,
	node int,
	httpPort, sqlPort int,
) *tenantNode {
	tn := &tenantNode{
		tenantID: tenantID,
		httpPort: httpPort,
		kvAddrs:  kvAddrs,
		binary:   binary,
		node:     node,
		sqlPort:  sqlPort,
	}
	tn.start(ctx, t, c, binary)
	return tn
}

func (tn *tenantNode) stop(ctx context.Context, t *test, c *cluster) {
	if tn.errCh == nil {
		return
	}
	// Must use pkill because the context cancellation doesn't wait for the
	// process to exit.
	c.Run(ctx, c.Node(tn.node),
		fmt.Sprintf("pkill -o -f '^%s mt start.*tenant-id=%d'", tn.binary, tn.tenantID))
	t.logger().Printf("mt cluster exited: %v", <-tn.errCh)
	tn.errCh = nil
}

func (tn *tenantNode) logDir() string {
	return fmt.Sprintf("logs/mt-%d", tn.tenantID)
}

func (tn *tenantNode) start(ctx context.Context, t *test, c *cluster, binary string) {
	tn.binary = binary
	tn.errCh = startTenantServer(
		ctx, c, c.Node(tn.node), binary, tn.kvAddrs, tn.tenantID,
		tn.httpPort, tn.sqlPort,
		"--log-dir="+tn.logDir(),
	)
	u, err := url.Parse(c.ExternalPGUrl(ctx, c.Node(tn.node))[0])
	require.NoError(t, err)
	u.Host = c.ExternalIP(ctx, c.Node(tn.node))[0] + ":" + strconv.Itoa(tn.sqlPort)
	tn.pgURL = u.String()
	c.l.Printf("sql server for tenant %d should be running at %s", tn.tenantID, tn.pgURL)

	const warmupPeriod = 3 * time.Second
	select {
	case err := <-tn.errCh:
		t.Fatal(err)
	case <-time.After(warmupPeriod):
	}
}

// runMultiTenantUpgrade exercises upgrading tenants and their host cluster.
//
// Sketch of the test:
//
//  * Start host cluster in the predecessor version.
//  * Create a tenant and verify it works and has the predecessor version.
//  * Upgrade the host cluster binary without finalizing the version.
//  * Upgrade the pre-upgrade tenant binary and verify it works.
//  * Run the version upgrade for the pre-upgrade tenant
//     * This is supported but not desirable. Exercise it just to show
//       that it doesn't explode.
//  * Create a mid-upgrade tenant, verify it works and has the predecessor
//    cluster version set.
//  * Finalize the host cluster version.
//  * Run the version upgrade for the mid-upgrade tenant.
//  * Create a post-upgrade tenant, verify it works and has the new version set.
func runMultiTenantUpgrade(ctx context.Context, t *test, c *cluster, v version.Version) {
	predecessor, err := PredecessorVersion(v)
	require.NoError(t, err)

	currentBinary := uploadVersion(ctx, t, c, c.All(), "")
	predecessorBinary := uploadVersion(ctx, t, c, c.All(), predecessor)

	kvNodes := c.Node(1)

	c.Start(ctx, t, kvNodes, startArgs("--binary="+predecessorBinary))

	kvAddrs := c.ExternalAddr(ctx, kvNodes)

	const preUpgradeHTTPPort, preUpgradeSQLPort = 8081, 36357
	const preUpgradeTenantID = 123
	runner := sqlutils.MakeSQLRunner(c.Conn(ctx, 1))
	runner.Exec(t, `SELECT crdb_internal.create_tenant($1)`, preUpgradeTenantID)

	var initialVersion string
	runner.QueryRow(t, "SHOW CLUSTER SETTING version").Scan(&initialVersion)

	const tenantNode = 2
	preUpgradeTenant := createTenantNode(ctx, t, c, predecessorBinary, kvAddrs,
		preUpgradeTenantID, tenantNode, preUpgradeHTTPPort, preUpgradeSQLPort)
	defer preUpgradeTenant.stop(ctx, t, c)

	t.Status("checking that a client can connect to the pre-upgrade tenant server")
	verifySQL(t, preUpgradeTenant.pgURL,
		mkStmt(`CREATE TABLE foo (id INT PRIMARY KEY, v STRING)`),
		mkStmt(`INSERT INTO foo VALUES($1, $2)`, 1, "bar"),
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}))

	t.Status("preserving downgrade option on kv server")
	runner.Exec(t, `SET CLUSTER SETTING cluster.preserve_downgrade_option = '20.2'`)

	t.Status("upgrading kv server")
	c.Stop(ctx, kvNodes)
	c.Start(ctx, t, kvNodes, startArgs("--binary="+currentBinary))
	time.Sleep(time.Second)

	t.Status("checking the pre-upgrade sql server still works after the KV binary upgrade")

	verifySQL(t, preUpgradeTenant.pgURL,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}))

	t.Status("stopping the pre-upgrade tenant server ahead of upgrading")
	preUpgradeTenant.stop(ctx, t, c)

	t.Status("starting the pre-upgrade tenant server with the current binary")

	preUpgradeTenant.start(ctx, t, c, currentBinary)

	// Note that this is exercising a path we likely want to eliminate in the
	// future where the tenant is upgraded before the KV nodes.
	t.Status("migrating the pre-upgrade tenant to the current version")

	verifySQL(t, preUpgradeTenant.pgURL,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}),
		mkStmt("SHOW CLUSTER SETTING version").
			withResults([][]string{{initialVersion}}),
		mkStmt("SET CLUSTER SETTING version = crdb_internal.node_executable_version()"),
		mkStmt("SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]").
			withResults([][]string{{"true"}}),
	)

	t.Status("creating a new mid-upgrade tenant")

	const midUpgradeHTTPPort, midUpgradeSQLPort = 8082, 36358
	const midUpgradeTenantID = 124
	runner.Exec(t, `SELECT crdb_internal.create_tenant($1)`, midUpgradeTenantID)

	t.Status("starting mid-upgrade tenant server")
	midUpgradeTenant := createTenantNode(ctx, t, c, currentBinary, kvAddrs,
		midUpgradeTenantID, tenantNode, midUpgradeHTTPPort, midUpgradeSQLPort)
	defer midUpgradeTenant.stop(ctx, t, c)

	t.Status("verifying that the mid-upgrade tenant server works and is at the earlier version")

	verifySQL(t, midUpgradeTenant.pgURL,
		mkStmt(`CREATE TABLE foo (id INT PRIMARY KEY, v STRING)`),
		mkStmt(`INSERT INTO foo VALUES($1, $2)`, 1, "bar"),
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}),
		// Note that the version here should be the version that is active on the
		mkStmt("SHOW CLUSTER SETTING version").
			withResults([][]string{{initialVersion}}),
	)

	t.Status("finalizing the kv server")
	runner.Exec(t, `SET CLUSTER SETTING cluster.preserve_downgrade_option = DEFAULT`)
	runner.CheckQueryResultsRetry(t,
		"SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]",
		[][]string{{"true"}})

	// Upgrade the tenant created in the mixed version state to the final version.
	t.Status("migrating the mid-upgrade tenant to the current version")
	verifySQL(t, midUpgradeTenant.pgURL,
		mkStmt("SET CLUSTER SETTING version = crdb_internal.node_executable_version()"),
		mkStmt("SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]").
			withResults([][]string{{"true"}}))

	t.Status("creating a new, post-upgrade tenant")

	const postUpgradeHTTPPort, postUpgradeSQLPort = 8083, 36359
	const postUpgradeTenantID = 125
	runner.Exec(t, `SELECT crdb_internal.create_tenant($1)`, postUpgradeTenantID)

	t.Status("verifying the post-upgrade tenant works and has the proper version")
	postUpgradeTenant := createTenantNode(ctx, t, c, currentBinary, kvAddrs,
		postUpgradeTenantID, tenantNode, postUpgradeHTTPPort, postUpgradeSQLPort)
	defer postUpgradeTenant.stop(ctx, t, c)
	verifySQL(t, postUpgradeTenant.pgURL,
		mkStmt("SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]").
			withResults([][]string{{"true"}}))
}

func startTenantServer(
	tenantCtx context.Context,
	c *cluster,
	node nodeListOption,
	binary string,
	kvAddrs []string,
	tenantID int,
	httpPort int,
	sqlPort int,
	logFlags string,
) chan error {

	args := []string{
		// TODO(tbg): make this test secure.
		// "--certs-dir", "certs",
		"--insecure",
		"--tenant-id=" + strconv.Itoa(tenantID),
		"--http-addr", ifLocal("127.0.0.1", "0.0.0.0") + ":" + strconv.Itoa(httpPort),
		"--kv-addrs", strings.Join(kvAddrs, ","),
		// Don't bind to external interfaces when running locally.
		"--sql-addr", ifLocal("127.0.0.1", "0.0.0.0") + ":" + strconv.Itoa(sqlPort),
	}
	if logFlags != "" {
		args = append(args, logFlags)
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.RunE(tenantCtx, node,
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
