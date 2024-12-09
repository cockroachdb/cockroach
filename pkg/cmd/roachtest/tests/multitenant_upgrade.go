// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/stretchr/testify/require"
)

func registerMultiTenantUpgrade(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:              "multitenant-upgrade",
		Timeout:           1 * time.Hour,
		Cluster:           r.MakeClusterSpec(2),
		CompatibleClouds:  registry.AllExceptAWS,
		Suites:            registry.Suites(registry.Nightly),
		Owner:             registry.OwnerServer,
		NonReleaseBlocker: false,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runMultiTenantUpgrade(ctx, t, c, t.BuildVersion())
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
func runMultiTenantUpgrade(
	ctx context.Context, t test.Test, c cluster.Cluster, v *version.Version,
) {
	// Update this map with every new release.
	versionToMinSupportedVersion := map[string]string{
		"23.2": "23.1",
	}
	curBinaryMajorAndMinorVersion := getMajorAndMinorVersionOnly(v)
	currentBinaryMinSupportedVersion, ok := versionToMinSupportedVersion[curBinaryMajorAndMinorVersion]
	require.True(t, ok, "current binary '%s' not found in 'versionToMinSupportedVersion' map", curBinaryMajorAndMinorVersion)

	predecessorV, err := release.LatestPredecessor(v)
	require.NoError(t, err)
	predecessor := clusterupgrade.MustParseVersion(predecessorV)

	currentBinary := uploadCockroach(ctx, t, c, c.All(), clusterupgrade.CurrentVersion())
	predecessorBinary := uploadCockroach(ctx, t, c, c.All(), predecessor)

	kvNodes := c.Node(1)

	settings := install.MakeClusterSettings(install.BinaryOption(predecessorBinary))
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, kvNodes)

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
	tenant11a := createTenantNode(ctx, t, c, kvNodes, tenant11ID, tenantNode, tenant11aHTTPPort, tenant11aSQLPort)
	tenant11a.start(ctx, t, c, predecessorBinary)
	defer tenant11a.stop(ctx, t, c)

	// Since the certs are created with the createTenantNode call above, we
	// call the "no certs" version of create tenant here.
	tenant11b := createTenantNodeNoCerts(ctx, t, c, kvNodes, tenant11ID, tenantNode, tenant11bHTTPPort, tenant11bSQLPort)
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
	c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), settings, kvNodes)
	time.Sleep(time.Second)

	t.Status("checking the pre-upgrade sql server still works after the system tenant binary upgrade")

	verifySQL(t, tenant11a.pgURL,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}))

	t.Status("starting tenant 12 server with older binary")
	tenant12 := createTenantNode(ctx, t, c, kvNodes, tenant12ID, tenantNode, tenant12HTTPPort, tenant12SQLPort)
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
	tenant13 := createTenantNode(ctx, t, c, kvNodes, tenant13ID, tenantNode, tenant13HTTPPort, tenant13SQLPort)
	tenant13.start(ctx, t, c, currentBinary)
	defer tenant13.stop(ctx, t, c)

	t.Status("verifying that the tenant 13 server works and is at the earlier version")

	verifySQL(t, tenant13.pgURL,
		mkStmt(`CREATE TABLE foo (id INT PRIMARY KEY, v STRING)`),
		mkStmt(`INSERT INTO foo VALUES($1, $2)`, 1, "bar"),
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}),
		mkStmt("SHOW CLUSTER SETTING version").
			withResults([][]string{{currentBinaryMinSupportedVersion}}),
	)

	t.Status("stopping the first tenant 11 server ahead of upgrading")
	tenant11a.stop(ctx, t, c)

	t.Status("starting the first tenant 11 server with the current binary")
	tenant11a.start(ctx, t, c, currentBinary)

	t.Status("intentionally leaving the second tenant 11 server on the old binary")

	t.Status("verify first tenant 11 server works with the new binary")
	{
		verifySQL(t, tenant11a.pgURL,
			mkStmt(`SELECT * FROM foo LIMIT 1`).
				withResults([][]string{{"1", "bar"}}),
			mkStmt("SHOW CLUSTER SETTING version").
				withResults([][]string{{initialVersion}}))
	}

	t.Status("verify second tenant 11 server still works with the old binary")
	{
		verifySQL(t, tenant11b.pgURL,
			mkStmt(`SELECT * FROM foo LIMIT 1`).
				withResults([][]string{{"1", "bar"}}),
			mkStmt("SHOW CLUSTER SETTING version").
				withResults([][]string{{initialVersion}}))
	}

	t.Status("attempting to upgrade tenant 11 before storage cluster is finalized and expecting a failure")
	expectErr(t, tenant11a.pgURL,
		fmt.Sprintf("pq: preventing tenant upgrade from running as the storage cluster has not yet been upgraded: storage cluster version = %s, tenant cluster version = %s", initialVersion, initialVersion),
		"SET CLUSTER SETTING version = crdb_internal.node_executable_version()")

	t.Status("finalizing the system tenant upgrade")
	runner.Exec(t, `SET CLUSTER SETTING cluster.preserve_downgrade_option = DEFAULT`)
	runner.CheckQueryResultsRetry(t,
		"SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]",
		[][]string{{"true"}})

	tenant11aRunner, tenant11aRunnerCloser := openDBAndMakeSQLRunner(t, tenant11a.pgURL)
	defer tenant11aRunnerCloser()

	finalVersion := tenant11aRunner.QueryStr(t, "SELECT * FROM crdb_internal.node_executable_version();")[0][0]
	// Remove patch release from predecessorVersion.
	predecessorVersion := fmt.Sprintf("%d.%d", predecessor.Major(), predecessor.Minor())

	t.Status("migrating first tenant 11 server to the current version after system tenant is finalized which should fail because second server is still on old binary - expecting a failure here too")
	expectErr(t, tenant11a.pgURL,
		fmt.Sprintf(`pq: error validating the version of one or more SQL server instances: rpc error: code = Unknown desc = sql server 2 is running a binary version %s which is less than the attempted upgrade version %s
HINT: check the binary versions of all running SQL server instances to ensure that they are compatible with the attempted upgrade version`, predecessorVersion, finalVersion),
		"SET CLUSTER SETTING version = crdb_internal.node_executable_version()")

	t.Status("verify that the first tenant 11 server can now query the storage cluster")
	{
		verifySQL(t, tenant11a.pgURL,
			mkStmt(`SELECT * FROM foo LIMIT 1`).
				withResults([][]string{{"1", "bar"}}),
			mkStmt("SHOW CLUSTER SETTING version").
				withResults([][]string{{initialVersion}}))
	}

	t.Status("stop the second tenant 11 server and restart it on the new binary")
	tenant11b.stop(ctx, t, c)
	tenant11b.start(ctx, t, c, currentBinary)

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
	tenant14 := createTenantNode(ctx, t, c, kvNodes, tenant14ID, tenantNode, tenant14HTTPPort, tenant14SQLPort)
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
	tenant14.stop(ctx, t, c)
	tenant14.start(ctx, t, c, currentBinary)

	t.Status("verifying the post-upgrade tenant works and has the proper version")
	verifySQL(t, tenant14.pgURL,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}),
		mkStmt("SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]").
			withResults([][]string{{"true"}}))

	t.Status("restarting the tenant 14 server to check it works with preserve downgrade option as an override")
	runner.Exec(
		t,
		`ALTER TENANT [14] SET CLUSTER SETTING cluster.preserve_downgrade_option = crdb_internal.node_executable_version()`)
	tenant14.stop(ctx, t, c)
	tenant14.start(ctx, t, c, currentBinary)

	t.Status("restarting the tenant 13 server to check it works with preserve downgrade option in the tenant")
	verifySQL(t, tenant13.pgURL,
		mkStmt("SET CLUSTER SETTING cluster.preserve_downgrade_option = crdb_internal.node_executable_version()"))
	tenant13.stop(ctx, t, c)
	tenant13.start(ctx, t, c, currentBinary)
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

func getMajorAndMinorVersionOnly(v *version.Version) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d.%d", v.Major(), v.Minor())
	return b.String()
}
