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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/stretchr/testify/require"
)

func registerMultiTenantUpgrade(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:              "multitenant-upgrade",
		Cluster:           r.MakeClusterSpec(2),
		Owner:             registry.OwnerMultiTenant,
		NonReleaseBlocker: false,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runMultiTenantUpgrade(ctx, t, c, *t.BuildVersion())
		},
	})
}

// runMultiTenantUpgrade exercises upgrading tenants and their storage cluster.
//
// Sketch of the test:
//
//   - Storage{Binary: Prev, Cluster: Prev}: Start storage cluster.
//   - Storage{Binary: Prev, Cluster: Prev}: Run create_tenant(11).
//   - Tenant11a{Binary: Prev, Cluster: Prev}: Start tenant pod 11a and 11b, and verify that they work.
//   - Storage{Binary: Prev, Cluster: Prev}: Set preserve downgrade option.
//   - Storage{Binary: Prev, Cluster: Prev}: Run create_tenant(12).
//   - Storage{Binary: Cur, Cluster: Prev}: Upgrade storage cluster (don't finalize).
//   - Tenant11a{Binary: Prev, Cluster: Prev}: Verify tenant pod 11a still works.
//   - Tenant12{Binary: Prev, Cluster: Prev}: Start tenant 12 and verify it works.
//   - Storage{Binary: Cur, Cluster: Prev}: Run create_tenant(13).
//   - Tenant13{Binary: Cur, Cluster: Prev}: Start tenant 13 and verify it works.
//   - Tenant11a{Binary: Cur, Cluster: Prev}: Upgrade tenant pod 11a's binary and verify it works.
//   - Tenant11a{Binary: Cur, Cluster: Prev}: Run the version upgrade for the tenant pod 11a and expect a failure.
//   - Storage{Binary: Cur, Cluster: Cur}: Finalize the upgrade on the host.
//   - Tenant11a{Binary: Cur, Cluster: Prev}: Run the version upgrade for the tenant pod 11a and expect a failure
//     due to tenant pod 11b being on the old binary.
//   - Tenant11b{Binary: Cur, Cluster: Prev}: Upgrade tenant pod 11b's binary.
//   - Tenant11a{Binary: Cur, Cluster: Cur}: Run the version upgrade for the tenant 11 through pod 11a and verify that it works.
//   - Tenant11b{Binary: Cur, Cluster: Cur}: Validate that the 11b pod sees the upgrade version.
//   - Tenant12{Binary: Cur, Cluster: Prev}: Upgrade the tenant 12 binary.
//   - Tenant12{Binary: Cur, Cluster: Cur}: Run the version upgrade for tenant 12.
//   - Tenant12{Binary: Cur, Cluster: Cur}: Restart tenant 12 and make sure it still works.
//   - Tenant13{Binary: Cur, Cluster: Cur}: Run the version upgrade for tenant 13.
//   - Tenant12{Binary: Cur, Cluster: Cur}: Restart tenant 13 and make sure it still works.
//   - Tenant14{Binary: Cur, Cluster: Cur}: Create tenant 14 and verify it works.
//   - Tenant12{Binary: Cur, Cluster: Cur}: Restart tenant 14 and make sure it still works.
func runMultiTenantUpgrade(ctx context.Context, t test.Test, c cluster.Cluster, v version.Version) {
	predecessor, err := version.PredecessorVersion(v)
	require.NoError(t, err)

	currentBinary := uploadVersion(ctx, t, c, c.All(), clusterupgrade.MainVersion)
	predecessorBinary := uploadVersion(ctx, t, c, c.All(), predecessor)

	kvNodes := c.Node(1)

	settings := install.MakeClusterSettings(install.BinaryOption(predecessorBinary), install.SecureOption(true))
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, kvNodes)
	tenantStartOpt := createTenantOtherTenantIDs([]int{11, 12, 13, 14})

	const tenant11aHTTPPort, tenant11aSQLPort = 8011, 20011
	const tenant11bHTTPPort, tenant11bSQLPort = 8016, 20016
	const tenant11ID = 11
	runner := sqlutils.MakeSQLRunner(c.Conn(ctx, t.L(), 1))
	// We'll sometimes have to wait out the backoff of the storage cluster
	// auto-update loop (at the time of writing 30s), plus some migrations may be
	// genuinely long-running.
	runner.SucceedsSoonDuration = 5 * time.Minute
	runner.Exec(t, `SELECT crdb_internal.create_tenant($1::INT)`, tenant11ID)

	var initialVersion string
	runner.QueryRow(t, "SHOW CLUSTER SETTING version").Scan(&initialVersion)

	// Create two instances of tenant 11 so that we can test with two pods
	// running during migration.
	const tenantNode = 2
	tenant11a := createTenantNode(ctx, t, c, kvNodes, tenant11ID, tenantNode, tenant11aHTTPPort, tenant11aSQLPort, tenantStartOpt)
	tenant11a.start(ctx, t, c, predecessorBinary)
	defer tenant11a.stop(ctx, t, c)

	// Since the certs are created with the createTenantNode call above, we
	// call the "no certs" version of create tenant here.
	tenant11b := createTenantNodeNoCerts(ctx, t, c, kvNodes, tenant11ID, tenantNode, tenant11bHTTPPort, tenant11bSQLPort, tenantStartOpt)
	tenant11b.start(ctx, t, c, predecessorBinary)
	defer tenant11b.stop(ctx, t, c)

	t.Status("checking that a client can connect to the first tenant 11 server")
	verifySQL(t, tenant11a.pgURL,
		mkStmt(`CREATE TABLE foo (id INT PRIMARY KEY, v STRING)`),
		mkStmt(`INSERT INTO foo VALUES($1, $2)`, 1, "bar"),
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}))

	verifySQL(t, tenant11a.pgURL,
		mkStmt("SHOW CLUSTER SETTING version").
			withResults([][]string{{initialVersion}}),
	)

	t.Status("checking that a client can connect to the second tenant 11 server")
	verifySQL(t, tenant11b.pgURL,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}))

	verifySQL(t, tenant11b.pgURL,
		mkStmt("SHOW CLUSTER SETTING version").
			withResults([][]string{{initialVersion}}),
	)

	t.Status("preserving downgrade option on system tenant")
	{
		s := runner.QueryStr(t, `SHOW CLUSTER SETTING version`)
		runner.Exec(
			t,
			`SET CLUSTER SETTING cluster.preserve_downgrade_option = $1`, s[0][0],
		)
	}

	t.Status("creating a new tenant 12")
	const tenant12HTTPPort, tenant12SQLPort = 8012, 20012
	const tenant12ID = 12
	runner.Exec(t, `SELECT crdb_internal.create_tenant($1::INT)`, tenant12ID)

	t.Status("upgrading system tenant binary")
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), kvNodes)
	settings.Binary = currentBinary
	// TODO (msbutler): investigate why the scheduled backup command fails due to a `Is the Server
	// running?` error.
	c.Start(ctx, t.L(), option.DefaultStartOptsNoBackups(), settings, kvNodes)
	time.Sleep(time.Second)

	t.Status("checking the pre-upgrade sql server still works after the system tenant binary upgrade")

	verifySQL(t, tenant11a.pgURL,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}))

	t.Status("starting tenant 12 server with older binary")
	tenant12 := createTenantNode(ctx, t, c, kvNodes, tenant12ID, tenantNode, tenant12HTTPPort, tenant12SQLPort, tenantStartOpt)
	tenant12.start(ctx, t, c, predecessorBinary)
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

	const tenant13HTTPPort, tenant13SQLPort = 8013, 20013
	const tenant13ID = 13
	runner.Exec(t, `SELECT crdb_internal.create_tenant($1::INT)`, tenant13ID)

	t.Status("starting tenant 13 server with new binary")
	tenant13 := createTenantNode(ctx, t, c, kvNodes, tenant13ID, tenantNode, tenant13HTTPPort, tenant13SQLPort, tenantStartOpt)
	tenant13.start(ctx, t, c, currentBinary)
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

	t.Status("stopping the first tenant 11 server ahead of upgrading")
	tenant11a.stop(ctx, t, c)

	t.Status("starting the first tenant 11 server with the current binary")
	tenant11a.start(ctx, t, c, currentBinary)

	t.Status("intentionally leaving the second tenant 11 server on the old binary")

	// Note that here we'd like to validate that the tenant 11 servers can still
	// query the storage cluster. The problem however, is that due to #88927,
	// they can't because they're at different binary versions. Once #88927 is
	// fixed, we should add checks in here that we're able to query from tenant
	// 11 servers.

	t.Status("attempting to upgrade tenant 11 before storage cluster is finalized and expecting a failure")
	expectErr(t, tenant11a.pgURL,
		fmt.Sprintf("pq: preventing tenant upgrade from running as the storage cluster has not yet been upgraded: storage cluster version = %s, tenant cluster version = %s", initialVersion, initialVersion),
		"SET CLUSTER SETTING version = crdb_internal.node_executable_version()")

	t.Status("finalizing the system tenant upgrade")
	runner.Exec(t, `SET CLUSTER SETTING cluster.preserve_downgrade_option = DEFAULT`)
	runner.CheckQueryResultsRetry(t,
		"SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]",
		[][]string{{"true"}})

	t.Status("migrating first tenant 11 server to the current version after system tenant is finalized which should fail because second server is still on old binary - expecting a failure here too")
	expectErr(t, tenant11a.pgURL,
		`pq: error validating the version of one or more SQL server instances: validate cluster version failed: some tenant pods running on binary less than 23.1`,
		"SET CLUSTER SETTING version = crdb_internal.node_executable_version()")

	// Note that here we'd like to validate that the first tenant 11 server can
	// query the storage cluster. The problem however, is that due to #88927,
	// they can't because they're at different DistSQL versions. We plan to never change
	// the DistSQL version again so once we have 23.1 images to test against we should
	// add a check in here that we're able to query from tenant 11 first server.
	t.Status("stop the second tenant 11 server and restart it on the new binary")
	tenant11b.stop(ctx, t, c)
	tenant11b.start(ctx, t, c, currentBinary)

	t.Status("verify that the first tenant 11 server can now query the storage cluster")
	{
		verifySQL(t, tenant11a.pgURL,
			mkStmt(`SELECT * FROM foo LIMIT 1`).
				withResults([][]string{{"1", "bar"}}),
			mkStmt("SHOW CLUSTER SETTING version").
				withResults([][]string{{initialVersion}}))
	}

	t.Status("verify the second tenant 11 server works with the new binary")
	{
		verifySQL(t, tenant11b.pgURL,
			mkStmt(`SELECT * FROM foo LIMIT 1`).
				withResults([][]string{{"1", "bar"}}),
			mkStmt("SHOW CLUSTER SETTING version").
				withResults([][]string{{initialVersion}}))
	}

	t.Status("migrating first tenant 11 server to the current version which should now succeed")
	verifySQL(t, tenant11a.pgURL,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}),
		mkStmt("SHOW CLUSTER SETTING version").
			withResults([][]string{{initialVersion}}),
		mkStmt("SET CLUSTER SETTING version = crdb_internal.node_executable_version()"),
		mkStmt("SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]").
			withResults([][]string{{"true"}}),
	)

	t.Status("validate that the second tenant 11 server has moved to the current version")
	verifySQL(t, tenant11b.pgURL,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}),
		mkStmt("SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]").
			withResults([][]string{{"true"}}),
	)

	t.Status("stopping the tenant 12 server ahead of upgrading")
	tenant12.stop(ctx, t, c)

	t.Status("starting the tenant 12 server with the current binary")
	tenant12.start(ctx, t, c, currentBinary)

	t.Status("verify that the tenant 12 server works with the new binary")
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

	t.Status("verify that the tenant 12 server works with the new binary after restart")
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

	const tenant14HTTPPort, tenant14SQLPort = 8014, 20014
	const tenant14ID = 14
	runner.Exec(t, `SELECT crdb_internal.create_tenant($1::INT)`, tenant14ID)

	t.Status("verifying that the tenant 14 server works and has the proper version")
	tenant14 := createTenantNode(ctx, t, c, kvNodes, tenant14ID, tenantNode, tenant14HTTPPort, tenant14SQLPort, tenantStartOpt)
	tenant14.start(ctx, t, c, currentBinary)
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

func openDBAndMakeSQLRunner(t test.Test, url string) (*sqlutils.SQLRunner, func()) {
	db, err := gosql.Open("postgres", url)
	if err != nil {
		t.Fatal(err)
	}
	f := func() { _ = db.Close() }
	return sqlutils.MakeSQLRunner(db), f
}

func verifySQL(t test.Test, url string, stmts ...sqlVerificationStmt) {
	tdb, closer := openDBAndMakeSQLRunner(t, url)
	defer closer()

	for _, stmt := range stmts {
		if stmt.optionalResults == nil {
			tdb.Exec(t, stmt.stmt, stmt.args...)
		} else {
			res := tdb.QueryStr(t, stmt.stmt, stmt.args...)
			require.Equal(t, stmt.optionalResults, res)
		}
	}
}

func expectErr(t test.Test, url string, error string, query string) {
	runner, closer := openDBAndMakeSQLRunner(t, url)
	defer closer()
	runner.ExpectErr(t, error, query)
}
