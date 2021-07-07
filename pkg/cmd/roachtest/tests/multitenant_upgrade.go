// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/stretchr/testify/require"
)

func registerMultiTenantUpgrade(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:              "multitenant-upgrade",
		Cluster:           r.MakeClusterSpec(2),
		Owner:             registry.OwnerKV,
		NonReleaseBlocker: false,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runMultiTenantUpgrade(ctx, t, c, *t.BuildVersion())
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
	t test.Test,
	c cluster.Cluster,
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

func (tn *tenantNode) stop(ctx context.Context, t test.Test, c cluster.Cluster) {
	if tn.errCh == nil {
		return
	}
	// Must use pkill because the context cancellation doesn't wait for the
	// process to exit.
	c.Run(ctx, c.Node(tn.node),
		fmt.Sprintf("pkill -o -f '^%s mt start.*tenant-id=%d'", tn.binary, tn.tenantID))
	t.L().Printf("mt cluster exited: %v", <-tn.errCh)
	tn.errCh = nil
}

func (tn *tenantNode) logDir() string {
	return fmt.Sprintf("logs/mt-%d", tn.tenantID)
}

func (tn *tenantNode) start(ctx context.Context, t test.Test, c cluster.Cluster, binary string) {
	tn.binary = binary
	tn.errCh = startTenantServer(
		ctx, c, c.Node(tn.node), binary, tn.kvAddrs, tn.tenantID,
		tn.httpPort, tn.sqlPort,
		"--log-dir="+tn.logDir(),
	)
	externalUrls, err := c.ExternalPGUrl(ctx, c.Node(tn.node))
	require.NoError(t, err)
	u, err := url.Parse(externalUrls[0])
	require.NoError(t, err)
	internalUrls, err := c.ExternalIP(ctx, c.Node(tn.node))
	require.NoError(t, err)
	u.Host = internalUrls[0] + ":" + strconv.Itoa(tn.sqlPort)
	tn.pgURL = u.String()

	// The tenant is usually responsive ~right away, but it
	// has on occasions taken more than 3s for it to connect
	// to the KV layer, and it won't open the SQL port until
	// it has.
	if err := retry.ForDuration(45*time.Second, func() error {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tn.errCh:
			t.Fatal(err)
		default:
		}

		db, err := gosql.Open("postgres", tn.pgURL)
		if err != nil {
			return err
		}
		defer db.Close()
		_, err = db.ExecContext(ctx, `SELECT 1`)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	t.L().Printf("sql server for tenant %d running at %s", tn.tenantID, tn.pgURL)
}

// runMultiTenantUpgrade exercises upgrading tenants and their host cluster.
//
// Sketch of the test:
//
//  * Host{Binary: Prev, Cluster: Prev}: Start host cluster.
//  * Tenant11{Binary: Prev, Cluster: Prev}: Create tenant 11 and verify it works.
//  * Host{Binary: Cur, Cluster: Prev}: Upgrade host cluster (don't finalize).
//  * Tenant11{Binary: Prev, Cluster: Prev}: Verify tenant 11 still works.
//  * Tenant12{Binary: Prev, Cluster: Prev}: Create tenant 12 and verify it works.
//  * Tenant13{Binary: Cur, Cluster: Prev}: Create tenant 13 and verify it works.
//  * Tenant11{Binary: Cur, Cluster: Prev}: Upgrade tenant 11 binary and verify it works.
//  * Tenant11{Binary: Cur, Cluster: Cur}: Run the version upgrade for the tenant 11.
//     * This is supported but not necessarily desirable. Exercise it just to
//       show that it doesn't explode. This will verify new guard-rails when
//       and if they are added.
//  * Host{Binary: Cur, Cluster: Cur}: Finalize the upgrade on the host.
//  * Tenant12{Binary: Cur, Cluster: Prev}: Upgrade the tenant 12 binary.
//  * Tenant12{Binary: Cur, Cluster: Cur}: Run the version upgrade for tenant 12.
//  * Tenant12{Binary: Cur, Cluster: Cur}: Restart tenant 12 and make sure it still works.
//  * Tenant13{Binary: Cur, Cluster: Cur}: Run the version upgrade for tenant 13.
//  * Tenant12{Binary: Cur, Cluster: Cur}: Restart tenant 13 and make sure it still works.
//  * Tenant14{Binary: Cur, Cluster: Cur}: Create tenant 14 and verify it works.
//  * Tenant12{Binary: Cur, Cluster: Cur}: Restart tenant 14 and make sure it still works.
func runMultiTenantUpgrade(ctx context.Context, t test.Test, c cluster.Cluster, v version.Version) {
	predecessor, err := PredecessorVersion(v)
	require.NoError(t, err)

	currentBinary := uploadVersion(ctx, t, c, c.All(), "")
	predecessorBinary := uploadVersion(ctx, t, c, c.All(), predecessor)

	kvNodes := c.Node(1)

	c.Start(ctx, kvNodes, option.StartArgs("--binary="+predecessorBinary))

	kvAddrs, err := c.ExternalAddr(ctx, kvNodes)
	require.NoError(t, err)

	const tenant11HTTPPort, tenant11SQLPort = 8081, 36357
	const tenant11ID = 11
	runner := sqlutils.MakeSQLRunner(c.Conn(ctx, 1))
	runner.Exec(t, `SELECT crdb_internal.create_tenant($1)`, tenant11ID)

	var initialVersion string
	runner.QueryRow(t, "SHOW CLUSTER SETTING version").Scan(&initialVersion)

	const tenantNode = 2
	tenant11 := createTenantNode(ctx, t, c, predecessorBinary, kvAddrs,
		tenant11ID, tenantNode, tenant11HTTPPort, tenant11SQLPort)
	defer tenant11.stop(ctx, t, c)

	t.Status("checking that a client can connect to the tenant 11 server")
	verifySQL(t, tenant11.pgURL,
		mkStmt(`CREATE TABLE foo (id INT PRIMARY KEY, v STRING)`),
		mkStmt(`INSERT INTO foo VALUES($1, $2)`, 1, "bar"),
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}))

	var needsWorkaround bool
	if v, err := version.Parse("v" + predecessor); err != nil {
		t.Fatal(err)
	} else if !v.AtLeast(version.MustParse("v21.1.2")) {
		// Tenant upgrades are only fully functional when starting out at version
		// v21.1.2, which includes #64488. That PR is responsible for giving tenants
		// the initial cluster version, which is thus absent here.
		//
		// Without this workaround, the tenant would report a cluster version of
		// 20.2 which is going to trip up the test by making the read/write of the
		// cluster setting fail (after a time out), and also leading to an
		// ill-advised 20.2 cluster version reported for the tenant (once we run the
		// current binary). We won't actually upgrade tenants in production in
		// scenarios in which this branch is active (the host cluster will be at the
		// latest patch release before attempting an upgrade, which is going to be
		// at least 21.1.2).
		//
		// This can be deleted once PredecessorVersion(21.2) is >= 21.1.2.
		needsWorkaround = true
	}

	if !needsWorkaround {
		verifySQL(t, tenant11.pgURL,
			mkStmt("SHOW CLUSTER SETTING version").
				withResults([][]string{{initialVersion}}),
		)
	}

	t.Status("preserving downgrade option on host server")
	{
		s := runner.QueryStr(t, `SHOW CLUSTER SETTING version`)
		runner.Exec(
			t,
			`SET CLUSTER SETTING cluster.preserve_downgrade_option = $1`, s[0][0],
		)
	}

	t.Status("upgrading host server")
	c.Stop(ctx, kvNodes)
	c.Start(ctx, kvNodes, option.StartArgs("--binary="+currentBinary))
	time.Sleep(time.Second)

	t.Status("checking the pre-upgrade sql server still works after the KV binary upgrade")

	verifySQL(t, tenant11.pgURL,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}))

	t.Status("creating a new tenant 12")

	const tenant12HTTPPort, tenant12SQLPort = 8082, 36358
	const tenant12ID = 12
	runner.Exec(t, `SELECT crdb_internal.create_tenant($1)`, tenant12ID)

	t.Status("starting tenant 12 server with older binary")
	tenant12 := createTenantNode(ctx, t, c, predecessorBinary, kvAddrs,
		tenant12ID, tenantNode, tenant12HTTPPort, tenant12SQLPort)
	defer tenant12.stop(ctx, t, c)

	t.Status("verifying that the tenant 12 server works and is at the earlier version")

	verifySQL(t, tenant12.pgURL,
		mkStmt(`CREATE TABLE foo (id INT PRIMARY KEY, v STRING)`),
		mkStmt(`INSERT INTO foo VALUES($1, $2)`, 1, "bar"),
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}),
		mkStmt("SHOW CLUSTER SETTING version").
			withResults([][]string{{initialVersion}}),
	)

	t.Status("creating a new tenant 13")

	const tenant13HTTPPort, tenant13SQLPort = 8083, 36359
	const tenant13ID = 13
	runner.Exec(t, `SELECT crdb_internal.create_tenant($1)`, tenant13ID)

	t.Status("starting tenant 13 server with new binary")
	tenant13 := createTenantNode(ctx, t, c, currentBinary, kvAddrs,
		tenant13ID, tenantNode, tenant13HTTPPort, tenant13SQLPort)
	defer tenant13.stop(ctx, t, c)

	t.Status("verifying that the tenant 13 server works and is at the earlier version")

	verifySQL(t, tenant13.pgURL,
		mkStmt(`CREATE TABLE foo (id INT PRIMARY KEY, v STRING)`),
		mkStmt(`INSERT INTO foo VALUES($1, $2)`, 1, "bar"),
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}),
		mkStmt("SHOW CLUSTER SETTING version").
			withResults([][]string{{initialVersion}}),
	)

	t.Status("stopping the tenant 11 server ahead of upgrading")
	tenant11.stop(ctx, t, c)

	t.Status("starting the tenant 11 server with the current binary")
	tenant11.start(ctx, t, c, currentBinary)

	if needsWorkaround {
		// NB: we can't do this earlier, as the tenant would not be able to write
		// its cluster version under the binary that needed the workaround.
		verifySQL(t, tenant11.pgURL, mkStmt(`SET CLUSTER SETTING version = $1`, initialVersion))
	}

	t.Status("verify tenant 11 server works with the new binary")
	{
		verifySQL(t, tenant11.pgURL,
			mkStmt(`SELECT * FROM foo LIMIT 1`).
				withResults([][]string{{"1", "bar"}}),
			mkStmt("SHOW CLUSTER SETTING version").
				withResults([][]string{{initialVersion}}))
	}

	// Note that this is exercising a path we likely want to eliminate in the
	// future where the tenant is upgraded before the KV nodes.
	t.Status("migrating the tenant 11 to the current version before kv is finalized")

	verifySQL(t, tenant11.pgURL,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}),
		mkStmt("SHOW CLUSTER SETTING version").
			withResults([][]string{{initialVersion}}),
		mkStmt("SET CLUSTER SETTING version = crdb_internal.node_executable_version()"),
		mkStmt("SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]").
			withResults([][]string{{"true"}}),
	)

	t.Status("finalizing the kv server")
	runner.Exec(t, `SET CLUSTER SETTING cluster.preserve_downgrade_option = DEFAULT`)
	runner.CheckQueryResultsRetry(t,
		"SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]",
		[][]string{{"true"}})

	t.Status("stopping the tenant 12 server ahead of upgrading")
	tenant12.stop(ctx, t, c)

	t.Status("starting the tenant 12 server with the current binary")
	tenant12.start(ctx, t, c, currentBinary)

	t.Status("verify tenant 12 server works with the new binary")
	verifySQL(t, tenant12.pgURL,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}),
		mkStmt("SHOW CLUSTER SETTING version").
			withResults([][]string{{initialVersion}}))

	// Upgrade the tenant created in the mixed version state to the final version.
	t.Status("migrating tenant 12 to the current version")
	verifySQL(t, tenant12.pgURL,
		mkStmt("SET CLUSTER SETTING version = crdb_internal.node_executable_version()"),
		mkStmt("SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]").
			withResults([][]string{{"true"}}))

	t.Status("restarting the tenant 12 server to check it works after a restart")
	tenant12.stop(ctx, t, c)
	tenant12.start(ctx, t, c, currentBinary)

	t.Status("verify tenant 12 server works with the new binary after restart")
	verifySQL(t, tenant12.pgURL,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}),
		mkStmt("SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]").
			withResults([][]string{{"true"}}))

	// Upgrade the tenant created in the mixed version state to the final version.
	t.Status("migrating tenant 13 to the current version")
	verifySQL(t, tenant13.pgURL,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}),
		mkStmt("SET CLUSTER SETTING version = crdb_internal.node_executable_version()"),
		mkStmt("SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]").
			withResults([][]string{{"true"}}),
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}))

	t.Status("restarting the tenant 13 server to check it works after a restart")
	tenant13.stop(ctx, t, c)
	tenant13.start(ctx, t, c, currentBinary)

	t.Status("verify tenant 13 server works with the new binary after restart")
	verifySQL(t, tenant13.pgURL,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}),
		mkStmt("SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]").
			withResults([][]string{{"true"}}))

	t.Status("creating tenant 14 at the new version")

	const tenant14HTTPPort, tenant14SQLPort = 8084, 36360
	const tenant14ID = 14
	runner.Exec(t, `SELECT crdb_internal.create_tenant($1)`, tenant14ID)

	t.Status("verifying the tenant 14 works and has the proper version")
	tenant14 := createTenantNode(ctx, t, c, currentBinary, kvAddrs,
		tenant14ID, tenantNode, tenant14HTTPPort, tenant14SQLPort)
	defer tenant14.stop(ctx, t, c)
	verifySQL(t, tenant14.pgURL,
		mkStmt(`CREATE TABLE foo (id INT PRIMARY KEY, v STRING)`),
		mkStmt(`INSERT INTO foo VALUES($1, $2)`, 1, "bar"),
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}),
		mkStmt("SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]").
			withResults([][]string{{"true"}}))

	t.Status("restarting the tenant 14 server to check it works after a restart")
	tenant13.stop(ctx, t, c)
	tenant13.start(ctx, t, c, currentBinary)

	t.Status("verifying the post-upgrade tenant works and has the proper version")
	verifySQL(t, tenant14.pgURL,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}),
		mkStmt("SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]").
			withResults([][]string{{"true"}}))
}

func startTenantServer(
	tenantCtx context.Context,
	c cluster.Cluster,
	node option.NodeListOption,
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
		"--http-addr", ifLocal(c, "127.0.0.1", "0.0.0.0") + ":" + strconv.Itoa(httpPort),
		"--kv-addrs", strings.Join(kvAddrs, ","),
		// Don't bind to external interfaces when running locally.
		"--sql-addr", ifLocal(c, "127.0.0.1", "0.0.0.0") + ":" + strconv.Itoa(sqlPort),
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

func verifySQL(t test.Test, url string, stmts ...sqlVerificationStmt) {
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
