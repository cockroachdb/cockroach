// Copyright 2018 The Cockroach Authors.
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
	"math/rand"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

var (
	v201 = roachpb.Version{Major: 20, Minor: 1}
	v202 = roachpb.Version{Major: 20, Minor: 2}
)

// Feature tests that are invoked between each step of the version upgrade test.
// Tests can use u.clusterVersion to determine which version is active at the
// moment.
//
// A gotcha is that these feature tests are also invoked when the cluster is
// in the middle of upgrading -- i.e. a state where the cluster version has
// already been bumped, but not all nodes are aware). This should be considered
// a feature of this test, and feature tests that flake because of it need to
// be fixed.
var versionUpgradeTestFeatures = versionFeatureStep{
	// NB: the next four tests are ancient and supported since v2.0. However,
	// in 19.2 -> 20.1 we had a migration that disallowed most DDL in the
	// mixed version state, and so for convenience we gate them on v20.1.
	stmtFeatureTest("Object Access", v201, `
-- We should be able to successfully select from objects created in ancient
-- versions of CRDB using their FQNs. Prevents bugs such as #43141, where
-- databases created before a migration were inaccessible after the
-- migration.
--
-- NB: the data has been baked into the fixtures. Originally created via:
--   create database persistent_db
--   create table persistent_db.persistent_table(a int)"))
-- on CRDB v1.0
select * from persistent_db.persistent_table;
show tables from persistent_db;
`),
	stmtFeatureTest("JSONB", v201, `
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE test.t (j JSONB);
DROP TABLE test.t;
	`),
	stmtFeatureTest("Sequences", v201, `
CREATE DATABASE IF NOT EXISTS test;
CREATE SEQUENCE test.test_sequence;
DROP SEQUENCE test.test_sequence;
	`),
	stmtFeatureTest("Computed Columns", v201, `
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE test.t (x INT AS (3) STORED);
DROP TABLE test.t;
	`),
}

func runVersionUpgrade(ctx context.Context, t *test, c *cluster, buildVersion version.Version) {
	predecessorVersion, err := PredecessorVersion(buildVersion)
	if err != nil {
		t.Fatal(err)
	}
	// This test uses fixtures and we do not have encrypted fixtures right now.
	c.encryptDefault = false

	// Set the bool within to true to create a new fixture for this test. This
	// is necessary after every release. For example, the day `master` becomes
	// the 20.2 release, this test will fail because it is missing a fixture for
	// 20.1; run the test (on 20.1) with the bool flipped to create the fixture.
	// Check it in (instructions will be logged below) and off we go.
	if false {
		// The version to create/update the fixture for. Must be released (i.e.
		// can download it from the homepage); if that is not the case use the
		// empty string which uses the local cockroach binary.
		newV := "21.1.0-beta.5"
		predV, err := PredecessorVersion(*version.MustParse("v" + newV))
		if err != nil {
			t.Fatal(err)
		}
		makeVersionFixtureAndFatal(ctx, t, c, predV, newV)
	}

	testFeaturesStep := versionUpgradeTestFeatures.step(c.All())
	schemaChangeStep := runSchemaChangeWorkloadStep(c.All().randNode()[0], 10 /* maxOps */, 2 /* concurrency */)
	// TODO(irfansharif): All schema change instances were commented out while
	// of #58489 is being addressed.
	_ = schemaChangeStep
	backupStep := func(ctx context.Context, t *test, u *versionUpgradeTest) {
		// This check was introduced for the system.tenants table and the associated
		// changes to full-cluster backup to include tenants. It mostly wants to
		// check that 20.1 (which does not have system.tenants) and 20.2 (which
		// does have the table) can both run full cluster backups.
		//
		// This step can be removed once 20.2 is released.
		if u.binaryVersion(ctx, t, 1).Major != 20 {
			return
		}
		dest := fmt.Sprintf("nodelocal://0/%d", timeutil.Now().UnixNano())
		_, err := u.conn(ctx, t, 1).ExecContext(ctx, `BACKUP TO $1`, dest)
		require.NoError(t, err)
	}

	// The steps below start a cluster at predecessorVersion (from a fixture),
	// then start an upgrade that is rolled back, and finally start and finalize
	// the upgrade. Between each step, we run the feature tests defined in
	// versionUpgradeTestFeatures.
	u := newVersionUpgradeTest(c,
		// Start the cluster from a fixture. That fixture's cluster version may
		// be at the predecessor version (though in practice it's fully up to
		// date, if it was created via the checkpointer above), so add a
		// waitForUpgradeStep to make sure we're upgraded all the way before
		// moving on.
		//
		// See the comment on createCheckpoints for details on fixtures.
		uploadAndStartFromCheckpointFixture(c.All(), predecessorVersion),
		uploadAndInitSchemaChangeWorkload(),
		waitForUpgradeStep(c.All()),
		testFeaturesStep,

		// NB: at this point, cluster and binary version equal predecessorVersion,
		// and auto-upgrades are on.

		// We use an empty string for the version below, which means to use the
		// main ./cockroach binary (i.e. the one being tested in this run).
		// We upgrade into this version more capriciously to ensure better
		// coverage by first rolling the cluster into the new version with
		// auto-upgrade disabled, then rolling back, and then rolling forward
		// and finalizing on the auto-upgrade path.
		preventAutoUpgradeStep(1),
		// Roll nodes forward.
		binaryUpgradeStep(c.All(), ""),
		testFeaturesStep,
		// Run a quick schemachange workload in between each upgrade.
		// The maxOps is 10 to keep the test runtime under 1-2 minutes.
		// schemaChangeStep,
		backupStep,
		// Roll back again. Note that bad things would happen if the cluster had
		// ignored our request to not auto-upgrade. The `autoupgrade` roachtest
		// exercises this in more detail, so here we just rely on things working
		// as they ought to.
		binaryUpgradeStep(c.All(), predecessorVersion),
		testFeaturesStep,
		// schemaChangeStep,
		backupStep,
		// Roll nodes forward, this time allowing them to upgrade, and waiting
		// for it to happen.
		binaryUpgradeStep(c.All(), ""),
		allowAutoUpgradeStep(1),
		testFeaturesStep,
		// schemaChangeStep,
		backupStep,
		waitForUpgradeStep(c.All()),
		testFeaturesStep,
		// schemaChangeStep,
		backupStep,
		// Turn tracing on globally to give it a fighting chance at exposing any
		// crash-inducing incompatibilities or horrendous memory leaks. (It won't
		// catch most memory leaks since this test doesn't run for too long or does
		// too much work). Then, run the previous tests again.
		enableTracingGloballyStep,
		testFeaturesStep,
		// schemaChangeStep,
		backupStep,
	)

	u.run(ctx, t)
}

func (u *versionUpgradeTest) run(ctx context.Context, t *test) {
	defer func() {
		for _, db := range u.conns {
			_ = db.Close()
		}
	}()

	for _, step := range u.steps {
		if step != nil {
			step(ctx, t, u)
		}
	}
}

type versionUpgradeTest struct {
	goOS  string
	c     *cluster
	steps []versionStep

	// Cache conns because opening one takes hundreds of ms, and we do it quite
	// a lot.
	conns []*gosql.DB
}

func newVersionUpgradeTest(c *cluster, steps ...versionStep) *versionUpgradeTest {
	return &versionUpgradeTest{
		goOS:  ifLocal(runtime.GOOS, "linux"),
		c:     c,
		steps: steps,
	}
}

func checkpointName(binaryVersion string) string { return "checkpoint-v" + binaryVersion }

// Return a cached conn to the given node. Don't call .Close(), the test harness
// will do it.
func (u *versionUpgradeTest) conn(ctx context.Context, t *test, i int) *gosql.DB {
	if u.conns == nil {
		for _, i := range u.c.All() {
			u.conns = append(u.conns, u.c.Conn(ctx, i))
		}
	}
	db := u.conns[i-1]
	// Run a trivial query to shake out errors that can occur when the server has
	// restarted in the meantime.
	_ = db.PingContext(ctx)
	return db
}

func uploadVersion(
	ctx context.Context, t *test, c *cluster, nodes nodeListOption, newVersion string,
) (binaryName string) {
	binaryName = "./cockroach"
	if newVersion == "" {
		c.Put(ctx, cockroach, binaryName, nodes)
	} else {
		newVersion = "v" + newVersion
		dir := newVersion
		binaryName = filepath.Join(dir, "cockroach")
		// Check if the cockroach binary already exists.
		if err := c.RunE(ctx, nodes, "test", "-e", binaryName); err != nil {
			if err := c.RunE(ctx, nodes, "mkdir", "-p", dir); err != nil {
				t.Fatal(err)
			}
			if err := c.Stage(ctx, c.l, "release", newVersion, dir, nodes); err != nil {
				t.Fatal(err)
			}
		}
	}
	return binaryName
}

func (u *versionUpgradeTest) uploadVersion(
	ctx context.Context, t *test, nodes nodeListOption, newVersion string,
) option {
	return startArgs("--binary=" + uploadVersion(ctx, t, u.c, nodes, newVersion))
}

// binaryVersion returns the binary running on the (one-indexed) node.
// NB: version means major.minor[-internal]; the patch level isn't returned. For example, a binary
// of version 19.2.4 will return 19.2.
func (u *versionUpgradeTest) binaryVersion(ctx context.Context, t *test, i int) roachpb.Version {
	db := u.conn(ctx, t, i)

	var sv string
	if err := db.QueryRow(`SELECT crdb_internal.node_executable_version();`).Scan(&sv); err != nil {
		t.Fatal(err)
	}

	if len(sv) == 0 {
		t.Fatal("empty version")
	}

	cv, err := roachpb.ParseVersion(sv)
	if err != nil {
		t.Fatal(err)
	}
	return cv
}

// binaryVersion returns the cluster version active on the (one-indexed) node. Note that the
// returned value might become stale due to the cluster auto-upgrading in the background plus
// gossip asynchronicity.
// NB: cluster versions are always major.minor[-internal]; there isn't a patch level.
func (u *versionUpgradeTest) clusterVersion(ctx context.Context, t *test, i int) roachpb.Version {
	db := u.conn(ctx, t, i)

	var sv string
	if err := db.QueryRowContext(ctx, `SHOW CLUSTER SETTING version`).Scan(&sv); err != nil {
		t.Fatal(err)
	}

	cv, err := roachpb.ParseVersion(sv)
	if err != nil {
		t.Fatal(err)
	}
	return cv
}

// versionStep is an isolated version migration on a running cluster.
type versionStep func(ctx context.Context, t *test, u *versionUpgradeTest)

func uploadAndStartFromCheckpointFixture(nodes nodeListOption, v string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		u.c.Run(ctx, nodes, "mkdir", "-p", "{store-dir}")
		vv := version.MustParse("v" + v)
		// The fixtures use cluster version (major.minor) but the input might be
		// a patch release.
		name := checkpointName(
			roachpb.Version{Major: int32(vv.Major()), Minor: int32(vv.Minor())}.String(),
		)
		for _, i := range nodes {
			u.c.Put(ctx,
				"pkg/cmd/roachtest/fixtures/"+strconv.Itoa(i)+"/"+name+".tgz",
				"{store-dir}/fixture.tgz", u.c.Node(i),
			)
		}
		// Extract fixture. Fail if there's already an LSM in the store dir.
		u.c.Run(ctx, nodes, "cd {store-dir} && [ ! -f {store-dir}/CURRENT ] && tar -xf fixture.tgz")

		// Put and start the binary.
		args := u.uploadVersion(ctx, t, nodes, v)
		// NB: can't start sequentially since cluster already bootstrapped.
		u.c.Start(ctx, t, nodes, args, startArgsDontEncrypt, roachprodArgOption{"--sequential=false"})
	}
}

// binaryUpgradeStep rolling-restarts the given nodes into the new binary
// version. Note that this does *not* wait for the cluster version to upgrade.
// Use a waitForUpgradeStep() for that.
func binaryUpgradeStep(nodes nodeListOption, newVersion string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		c := u.c

		// NB: We could technically stage the binary on all nodes before
		// restarting each one, but on Unix it's invalid to write to an
		// executable file while it is currently running. So we do the
		// simple thing and upload it serially instead.

		// Restart nodes in a random order; otherwise node 1 would be running all
		// the migrations and it probably also has all the leases.
		rand.Shuffle(len(nodes), func(i, j int) {
			nodes[i], nodes[j] = nodes[j], nodes[i]
		})
		for _, node := range nodes {
			t.l.Printf("restarting node %d", node)
			c.Stop(ctx, c.Node(node))
			args := u.uploadVersion(ctx, t, c.Node(node), newVersion)
			c.Start(ctx, t, c.Node(node), args, startArgsDontEncrypt)
			t.l.Printf("node %d now running binary version %s", node, u.binaryVersion(ctx, t, node))

			// TODO(nvanbenschoten): add upgrade qualification step. What should we
			// test? We could run logictests. We could add custom logic here. Maybe
			// this should all be pushed to nightly migration tests instead.
		}
	}
}

func enableTracingGloballyStep(ctx context.Context, t *test, u *versionUpgradeTest) {
	db := u.conn(ctx, t, 1)
	// NB: this enables net/trace, and as a side effect creates verbose trace spans everywhere.
	_, err := db.ExecContext(ctx, `SET CLUSTER SETTING trace.debug.enable = $1`, true)
	if err != nil {
		t.Fatal(err)
	}
}

func preventAutoUpgradeStep(node int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx, `SET CLUSTER SETTING cluster.preserve_downgrade_option = $1`, u.binaryVersion(ctx, t, node).String())
		if err != nil {
			t.Fatal(err)
		}
	}
}

func allowAutoUpgradeStep(node int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx, `RESET CLUSTER SETTING cluster.preserve_downgrade_option`)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// waitForUpgradeStep waits for the cluster version to reach the first node's
// binary version (which is assumed to be every node's binary version). We rely
// on the cluster's internal self-upgrading mechanism.
//
// NB: this is intentionally kept separate from binaryUpgradeStep because we run
// feature tests between the steps, and we want to expose them (at least
// heuristically) to the real-world situation in which some nodes have already
// learned of a cluster version bump (from Gossip) where others haven't. This
// situation tends to exhibit unexpected behavior.
func waitForUpgradeStep(nodes nodeListOption) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		newVersion := u.binaryVersion(ctx, t, nodes[0]).String()
		t.l.Printf("%s: waiting for cluster to auto-upgrade\n", newVersion)

		for _, i := range nodes {
			err := retry.ForDuration(30*time.Second, func() error {
				currentVersion := u.clusterVersion(ctx, t, i).String()
				if currentVersion != newVersion {
					return fmt.Errorf("%d: expected version %s, got %s", i, newVersion, currentVersion)
				}
				t.l.Printf("%s: acked by n%d", currentVersion, i)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		t.l.Printf("%s: nodes %v are upgraded\n", newVersion, nodes)

		// TODO(nvanbenschoten): add upgrade qualification step.
	}
}

func setClusterSettingVersionStep(ctx context.Context, t *test, u *versionUpgradeTest) {
	db := u.conn(ctx, t, 1)
	u.c.l.Printf("bumping cluster version")
	// TODO(tbg): once this is using a job, poll and periodically print the job status
	// instead of blocking.
	if _, err := db.ExecContext(
		ctx, `SET CLUSTER SETTING version = crdb_internal.node_executable_version()`,
	); err != nil {
		t.Fatal(err)
	}
	u.c.l.Printf("cluster version bumped")
}

type versionFeatureTest struct {
	name string
	fn   func(context.Context, *test, *versionUpgradeTest, nodeListOption) (skipped bool)
}

type versionFeatureStep []versionFeatureTest

func (vs versionFeatureStep) step(nodes nodeListOption) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		for _, feature := range vs {
			t.l.Printf("checking %s", feature.name)
			tBegin := timeutil.Now()
			skipped := feature.fn(ctx, t, u, nodes)
			dur := fmt.Sprintf("%.2fs", timeutil.Since(tBegin).Seconds())
			if skipped {
				t.l.Printf("^-- skip (%s)", dur)
			} else {
				t.l.Printf("^-- ok (%s)", dur)
			}
		}
	}
}

func stmtFeatureTest(
	name string, minVersion roachpb.Version, stmt string, args ...interface{},
) versionFeatureTest {
	return versionFeatureTest{
		name: name,
		fn: func(ctx context.Context, t *test, u *versionUpgradeTest, nodes nodeListOption) (skipped bool) {
			i := nodes.randNode()[0]
			if u.clusterVersion(ctx, t, i).Less(minVersion) {
				return true // skipped
			}
			db := u.conn(ctx, t, i)
			if _, err := db.ExecContext(ctx, stmt, args...); err != nil {
				if testutils.IsError(err, "no inbound stream connection") && u.clusterVersion(ctx, t, i).Less(v202) {
					// This error has been fixed in 20.2+ but may still occur on earlier
					// versions.
					return true // skipped
				}
				t.Fatal(err)
			}
			return false
		},
	}
}

// makeVersionFixtureAndFatal creates fixtures to "age out" old versions of CockroachDB.
// We want to test data that was created at v1.0, but we don't actually want to
// run a long chain of binaries starting all the way at v1.0. Instead, we
// periodically bake a set of store directories that originally started out on
// v1.0 and maintain it as a fixture for this test.
//
// The checkpoints will be created in the log directories downloaded as part of
// the artifacts. The test will fail on purpose when it's done with instructions
// on where to move the files.
func makeVersionFixtureAndFatal(
	ctx context.Context, t *test, c *cluster, predecessorVersion string, makeFixtureVersion string,
) {
	c.l.Printf("making fixture for %s (starting at %s)", makeFixtureVersion, predecessorVersion)
	c.encryptDefault = false
	newVersionUpgradeTest(c,
		// Start the cluster from a fixture. That fixture's cluster version may
		// be at the predecessor version (though in practice it's fully up to
		// date, if it was created via the checkpointer above), so add a
		// waitForUpgradeStep to make sure we're upgraded all the way before
		// moving on.
		//
		// See the comment on createCheckpoints for details on fixtures.
		uploadAndStartFromCheckpointFixture(c.All(), predecessorVersion),
		waitForUpgradeStep(c.All()),

		// NB: at this point, cluster and binary version equal predecessorVersion,
		// and auto-upgrades are on.

		binaryUpgradeStep(c.All(), makeFixtureVersion),
		waitForUpgradeStep(c.All()),

		func(ctx context.Context, t *test, u *versionUpgradeTest) {
			// If we're taking checkpoints, momentarily stop the cluster (we
			// need to do that to get the checkpoints to reflect a
			// consistent cluster state). The binary at this point will be
			// the new one, but the cluster version was not explicitly
			// bumped, though auto-update may have taken place already.
			// For example, if newVersion is 2.1, the cluster version in
			// the store directories may be 2.0 on some stores and 2.1 on
			// the others (though if any are on 2.1, then that's what's
			// stored in system.settings).
			// This means that when we restart from that version, we're
			// going to want to use the binary mentioned in the checkpoint,
			// or at least one compatible with the *predecessor* of the
			// checkpoint version. For example, for checkpoint-2.1, the
			// cluster version might be 2.0, so we can only use the 2.0 or
			// 2.1 binary, but not the 19.1 binary (as 19.1 and 2.0 are not
			// compatible).
			name := checkpointName(u.binaryVersion(ctx, t, 1).String())
			u.c.Stop(ctx, c.All())

			c.Run(ctx, c.All(), cockroach, "debug", "pebble", "db", "checkpoint",
				"{store-dir}", "{store-dir}/"+name)
			// The `cluster-bootstrapped` marker can already be found within
			// store-dir, but the rocksdb checkpoint step above does not pick it
			// up as it isn't recognized by RocksDB. We copy the marker
			// manually, it's necessary for roachprod created clusters. See
			// #54761.
			c.Run(ctx, c.Node(1), "cp", "{store-dir}/cluster-bootstrapped", "{store-dir}/"+name)
			c.Run(ctx, c.All(), "tar", "-C", "{store-dir}/"+name, "-czf", "{log-dir}/"+name+".tgz", ".")
			t.Fatalf(`successfully created checkpoints; failing test on purpose.

Invoke the following to move the archives to the right place and commit the
result:

for i in 1 2 3 4; do
  mkdir -p pkg/cmd/roachtest/fixtures/${i} && \
  mv artifacts/acceptance/version-upgrade/run_1/logs/${i}.unredacted/checkpoint-*.tgz \
     pkg/cmd/roachtest/fixtures/${i}/
done
`)
		}).run(ctx, t)
}

// importTPCCStep runs a TPCC import import on the first crdbNode (monitoring them all for
// crashes during the import). If oldV is nil, this runs the import using the specified
// version (for example "19.2.1", as provided by PredecessorVersion()) using the location
// used by c.Stage(). An empty oldV uses the main cockroach binary.
func importTPCCStep(oldV string, headroomWarehouses int, crdbNodes nodeListOption) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		// We need to use the predecessor binary to load into the
		// predecessor cluster to avoid random breakage. For example, you
		// can't use 21.1 to import into 20.2 due to some flag changes.
		//
		// TODO(tbg): also import a large dataset (for example 2TB bank)
		// that will provide cold data that may need to be migrated.
		var cmd string
		if oldV == "" {
			cmd = tpccImportCmd(headroomWarehouses)
		} else {
			cmd = tpccImportCmdWithCockroachBinary(filepath.Join("v"+oldV, "cockroach"), headroomWarehouses, "--checks=false")
		}
		// Use a monitor so that we fail cleanly if the cluster crashes
		// during import.
		m := newMonitor(ctx, u.c, crdbNodes)
		m.Go(func(ctx context.Context) error {
			return u.c.RunE(ctx, u.c.Node(crdbNodes[0]), cmd)
		})
		m.Wait()
	}
}

func importLargeBankStep(oldV string, rows int, crdbNodes nodeListOption) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		// Use the predecessor binary to load into the predecessor
		// cluster to avoid random breakage due to flag changes, etc.
		binary := "./cockroach"
		if oldV != "" {
			binary = filepath.Join("v"+oldV, "cockroach")
		}

		// Use a monitor so that we fail cleanly if the cluster crashes
		// during import.
		m := newMonitor(ctx, u.c, crdbNodes)
		m.Go(func(ctx context.Context) error {
			return u.c.RunE(ctx, u.c.Node(crdbNodes[0]), binary, "workload", "fixtures", "import", "bank",
				"--payload-bytes=10240", "--rows="+fmt.Sprint(rows), "--seed=4", "--db=bigbank")
		})
		m.Wait()
	}
}
