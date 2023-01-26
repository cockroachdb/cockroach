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
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
			if c.IsLocal() && runtime.GOARCH == "arm64" {
				t.Skip("Skip under ARM64. See https://github.com/cockroachdb/cockroach/issues/89268")
			}
			runMultiTenantUpgrade(ctx, t, c, *t.BuildVersion())
		},
	})
}

// runMultiTenantUpgrade exercises upgrading tenants and their storage cluster.
//
// Sketch of the test:
//
//   - Storage{Binary: Prev, Cluster: Prev}: Start storage cluster.
//   - Storage{Binary: Prev, Cluster: Prev}: Run create_tenant(11) and create_tenant(12).
//   - Tenant11{Binary: Prev, Cluster: Prev}: Start tenant 11 and verify it works.
//   - Storage{Binary: Cur, Cluster: Prev}: Upgrade storage cluster (don't finalize).
//   - Tenant11{Binary: Prev, Cluster: Prev}: Verify tenant 11 still works.
//   - Tenant12{Binary: Prev, Cluster: Prev}: Start tenant 12 and verify it works.
//   - Storage{Binary: Cur, Cluster: Prev}: Run create_tenant(13).
//   - Tenant13{Binary: Cur, Cluster: Prev}: Create tenant 13 and verify it works.
//   - Tenant11{Binary: Cur, Cluster: Prev}: Upgrade tenant 11 binary and verify it works.
//   - Tenant11{Binary: Cur, Cluster: Cur}: Run the version upgrade for the tenant 11.
//   - This is supported but not necessarily desirable. Exercise it just to
//     show that it doesn't explode. This will verify new guard-rails when
//     and if they are added.
//   - Storage{Binary: Cur, Cluster: Cur}: Finalize the upgrade on the Storage.
//   - Tenant12{Binary: Cur, Cluster: Prev}: Upgrade the tenant 12 binary.
//   - Tenant12{Binary: Cur, Cluster: Cur}: Run the version upgrade for tenant 12.
//   - Tenant12{Binary: Cur, Cluster: Cur}: Restart tenant 12 and make sure it still works.
//   - Tenant13{Binary: Cur, Cluster: Cur}: Run the version upgrade for tenant 13.
//   - Tenant12{Binary: Cur, Cluster: Cur}: Restart tenant 13 and make sure it still works.
//   - Tenant14{Binary: Cur, Cluster: Cur}: Create tenant 14 and verify it works.
//   - Tenant12{Binary: Cur, Cluster: Cur}: Restart tenant 14 and make sure it still works.
func runMultiTenantUpgrade(ctx context.Context, t test.Test, c cluster.Cluster, v version.Version) {
	predecessor, err := clusterupgrade.PredecessorVersion(v)
	require.NoError(t, err)

	currentBinary := uploadVersion(ctx, t, c, c.All(), clusterupgrade.MainVersion)
	predecessorBinary := uploadVersion(ctx, t, c, c.All(), predecessor)

	kvNodes := c.Node(1)

	settings := install.MakeClusterSettings(install.BinaryOption(predecessorBinary), install.SecureOption(true))
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, kvNodes)
	tenantStartOpt := createTenantOtherTenantIDs([]int{11, 12, 13, 14})

	const tenant11HTTPPort, tenant11SQLPort = 8011, 20011
	const tenant11ID = 11
	runner := sqlutils.MakeSQLRunner(c.Conn(ctx, t.L(), 1))
	// We'll sometimes have to wait out the backoff of the storage cluster
	// auto-update loop (at the time of writing 30s), plus some migrations may be
	// genuinely long-running.
	runner.SucceedsSoonDuration = 5 * time.Minute
	runner.Exec(t, `SELECT crdb_internal.create_tenant($1::INT)`, tenant11ID)

	var initialVersion string
	runner.QueryRow(t, "SHOW CLUSTER SETTING version").Scan(&initialVersion)

	const tenantNode = 2
	tenant11 := createTenantNode(ctx, t, c, kvNodes, tenant11ID, tenantNode, tenant11HTTPPort, tenant11SQLPort, tenantStartOpt)
	tenant11.start(ctx, t, c, predecessorBinary)
	defer tenant11.stop(ctx, t, c)

	t.Status("checking that a client can connect to the tenant 11 server")
	verifySQL(t, tenant11.pgURL,
		mkStmt(`CREATE TABLE foo (id INT PRIMARY KEY, v STRING)`),
		mkStmt(`INSERT INTO foo VALUES($1, $2)`, 1, "bar"),
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}))

	verifySQL(t, tenant11.pgURL,
		mkStmt("SHOW CLUSTER SETTING version").
			withResults([][]string{{initialVersion}}).
			withCustomResultsEqualFn(requireEqualVersionsIgnoreDevOffset),
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
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, kvNodes)
	time.Sleep(time.Second)

	t.Status("checking the pre-upgrade sql server still works after the system tenant binary upgrade")

	verifySQL(t, tenant11.pgURL,
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
			withResults([][]string{{initialVersion}}).
			withCustomResultsEqualFn(requireEqualVersionsIgnoreDevOffset),
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
			withResults([][]string{{initialVersion}}).
			withCustomResultsEqualFn(requireEqualVersionsIgnoreDevOffset),
	)

	t.Status("stopping the tenant 11 server ahead of upgrading")
	tenant11.stop(ctx, t, c)

	t.Status("starting the tenant 11 server with the current binary")
	tenant11.start(ctx, t, c, currentBinary)

	t.Status("verify tenant 11 server works with the new binary")
	{
		verifySQL(t, tenant11.pgURL,
			mkStmt(`SELECT * FROM foo LIMIT 1`).
				withResults([][]string{{"1", "bar"}}),
			mkStmt("SHOW CLUSTER SETTING version").
				withResults([][]string{{initialVersion}}).
				withCustomResultsEqualFn(requireEqualVersionsIgnoreDevOffset),
		)
	}

	t.Status("attempting to upgrade tenant 11 before storage cluster is finalized and expecting a failure")
	expectErr(t, tenant11.pgURL,
		fmt.Sprintf("pq: preventing tenant upgrade from running as the storage cluster has not yet been upgraded: storage cluster version = %s, tenant cluster version = %s", initialVersion, initialVersion),
		"SET CLUSTER SETTING version = crdb_internal.node_executable_version()")

	t.Status("finalizing the system tenant upgrade")
	runner.Exec(t, `SET CLUSTER SETTING cluster.preserve_downgrade_option = DEFAULT`)
	runner.CheckQueryResultsRetry(t,
		"SELECT version = crdb_internal.node_executable_version() FROM [SHOW CLUSTER SETTING version]",
		[][]string{{"true"}})

	t.Status("migrating tenant 11 to the current version after system tenant is finalized")

	verifySQL(t, tenant11.pgURL,
		mkStmt(`SELECT * FROM foo LIMIT 1`).
			withResults([][]string{{"1", "bar"}}),
		mkStmt("SHOW CLUSTER SETTING version").
			withResults([][]string{{initialVersion}}).
			withCustomResultsEqualFn(requireEqualVersionsIgnoreDevOffset),
		mkStmt("SET CLUSTER SETTING version = crdb_internal.node_executable_version()"),
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
			withResults([][]string{{initialVersion}}).
			withCustomResultsEqualFn(requireEqualVersionsIgnoreDevOffset),
	)

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

// EqualFn is implemented by both `require.Equal` and `requireEqualVersionsIgnoreDevOffset`.
type EqualFn = func(t require.TestingT, expected interface{}, actual interface{}, msgAndArgs ...interface{})

func requireEqualVersionsIgnoreDevOffset(
	t require.TestingT, expected interface{}, actual interface{}, msgAndArgs ...interface{},
) {
	normalizeVersion := func(v roachpb.Version) roachpb.Version {
		if (v.Major - clusterversion.DevOffset) >= 0 {
			v.Major -= clusterversion.DevOffset
		}
		return v
	}
	normalizedExpectedVersion := normalizeVersion(roachpb.MustParseVersion(expected.([][]string)[0][0]))
	normalizedActualVersion := normalizeVersion(roachpb.MustParseVersion(actual.([][]string)[0][0]))
	require.Equal(t, normalizedExpectedVersion, normalizedActualVersion, msgAndArgs...)
}

type sqlVerificationStmt struct {
	stmt                   string
	args                   []interface{}
	optionalResults        [][]string
	optionalResultsEqualFn EqualFn
}

func (s sqlVerificationStmt) withResults(res [][]string) sqlVerificationStmt {
	s.optionalResults = res
	return s
}

func (s sqlVerificationStmt) withCustomResultsEqualFn(fn EqualFn) sqlVerificationStmt {
	s.optionalResultsEqualFn = fn
	return s
}

func mkStmt(stmt string, args ...interface{}) sqlVerificationStmt {
	return sqlVerificationStmt{stmt: stmt, args: args, optionalResultsEqualFn: require.Equal}
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
			stmt.optionalResultsEqualFn(t, stmt.optionalResults, res)
		}
	}
}

func expectErr(t test.Test, url string, error string, query string) {
	runner, closer := openDBAndMakeSQLRunner(t, url)
	defer closer()
	runner.ExpectErr(t, error, query)
}
