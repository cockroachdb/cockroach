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
	"runtime"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/binfetcher"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	_ "github.com/lib/pq"
)

var v201 = roachpb.Version{Major: 20, Minor: 1}

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

func runVersionUpgrade(ctx context.Context, t *test, c *cluster, predecessorVersion string) {
	// This test uses fixtures and we do not have encrypted fixtures right now.
	c.encryptDefault = false

	// Set the bool within to true to create a new fixture for this test. This
	// is necessary after every release. For example, when day `master` becomes
	// the 20.2 release, this test will fail because it is missing a fixture for
	// 20.1; run the test (on 20.1) with the bool flipped to create the fixture.
	// Check it in (instructions are on the 'checkpointer' struct) and off we
	// go.
	cp := checkpointer{on: false, nodes: c.All()}

	testFeaturesStep := versionUpgradeTestFeatures.step(c.All())

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
		waitForUpgradeStep(c.All()),
		testFeaturesStep,

		// Work around a bug in <= 20.1 that exists at the time of writing. The
		// bug would sometimes cause the cluster version (predecessorVersion)
		// to not be persisted on the engines. In that case, moving to the next
		// version would fail. For details, see:
		//
		// https://github.com/cockroachdb/cockroach/pull/47358
		//
		// TODO(tbg): remove this when we stop running this test against 20.1
		// or any earlier release. Or, make sure the <=20.1 fixtures all have
		// the bumped cluster version on all stores.
		binaryUpgradeStep(c.All(), predecessorVersion),

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
		// Roll back again. Note that bad things would happen if the cluster had
		// ignored our request to not auto-upgrade. The `autoupgrade` roachtest
		// exercises this in more detail, so here we just rely on things working
		// as they ought to.
		binaryUpgradeStep(c.All(), predecessorVersion),
		testFeaturesStep,
		// Roll nodes forward, this time allowing them to upgrade, and waiting
		// for it to happen.
		binaryUpgradeStep(c.All(), ""),
		allowAutoUpgradeStep(1),
		testFeaturesStep,
		waitForUpgradeStep(c.All()),
		testFeaturesStep,

		// This is usually a noop, but if cp.on was set above, this is where we
		// create fixtures. This is last so that we're creating the fixtures for
		// the binary we're testing (i.e. if this run is on release-20.1, we'll
		// create a 20.1 fixture).
		cp.createCheckpointsAndFail,
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
		step(ctx, t, u)

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
	return u.conns[i-1]
}

func (u *versionUpgradeTest) uploadVersion(
	ctx context.Context, t *test, nodes nodeListOption, newVersion string,
) option {
	var binary string
	if newVersion == "" {
		binary = cockroach
	} else {
		var err error
		binary, err = binfetcher.Download(ctx, binfetcher.Options{
			Binary:  "cockroach",
			Version: "v" + newVersion,
			GOOS:    u.goOS,
			GOARCH:  "amd64",
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	target := "./cockroach"
	if newVersion != "" {
		target += "-" + newVersion
	}
	u.c.Put(ctx, binary, target, nodes)
	return startArgs("--binary=" + target)
}

// binaryVersion returns the binary running on the (one-indexed) node.
// NB: version means major.minor[-unstable]; the patch level isn't returned. For example, a binary
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
// NB: cluster versions are always major.minor[-unstable]; there isn't a patch level.
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

func uploadAndStartFromCheckpointFixture(nodes nodeListOption, version string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		u.c.Run(ctx, nodes, "mkdir", "-p", "{store-dir}")
		name := checkpointName(version)
		for _, i := range nodes {
			u.c.Put(ctx,
				"pkg/cmd/roachtest/fixtures/"+strconv.Itoa(i)+"/"+name+".tgz",
				"{store-dir}/fixture.tgz", u.c.Node(i),
			)
		}
		// Extract fixture. Fail if there's already an LSM in the store dir.
		u.c.Run(ctx, nodes, "cd {store-dir} && [ ! -f {store-dir}/CURRENT ] && tar -xf fixture.tgz")

		// Put and start the binary.
		args := u.uploadVersion(ctx, t, nodes, version)
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
		args := u.uploadVersion(ctx, t, nodes, newVersion)

		// Restart nodes in a random order; otherwise node 1 would be running all
		// the migrations and it probably also has all the leases.
		rand.Shuffle(len(nodes), func(i, j int) {
			nodes[i], nodes[j] = nodes[j], nodes[i]
		})
		for _, node := range nodes {
			t.l.Printf("restarting node %d", node)
			c.Stop(ctx, c.Node(node))
			c.Start(ctx, t, c.Node(node), args, startArgsDontEncrypt)
			t.l.Printf("node %d now running binary version %s", node, u.binaryVersion(ctx, t, node))

			// TODO(nvanbenschoten): add upgrade qualification step. What should we
			// test? We could run logictests. We could add custom logic here. Maybe
			// this should all be pushed to nightly migration tests instead.
		}
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

// checkpointer creates fixtures to "age out" old versions of CockroachDB. We
// want to test data that was created at v1.0, but we don't actually want to run
// a long chain of binaries starting all the way at v1.0. Instead, we
// periodically bake a set of store directories that originally started out on
// v1.0 and maintain it as a fixture for this test.
//
// The checkpoints will be created in the log directories downloaded as part of
// the artifacts. The test will fail on purpose when it's done. After, manually
// invoke the following to move the archives to the right place and commit the
// result:
//
// for i in 1 2 3 4; do
//   mkdir -p pkg/cmd/roachtest/fixtures/${i} && \
//   mv artifacts/acceptance/version-upgrade/run_1/${i}.logs/checkpoint-*.tgz \
//     pkg/cmd/roachtest/fixtures/${i}/
// done
type checkpointer struct {
	on    bool
	nodes nodeListOption
}

func (cp *checkpointer) createCheckpointsAndFail(
	ctx context.Context, t *test, u *versionUpgradeTest,
) {
	if !cp.on {
		return
	}
	c := u.c
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
	c.Stop(ctx, cp.nodes)
	c.Run(ctx, cp.nodes, "./cockroach", "debug", "rocksdb", "--db={store-dir}",
		"checkpoint", "--checkpoint_dir={store-dir}/"+name)
	c.Run(ctx, cp.nodes, "tar", "-C", "{store-dir}/"+name, "-czf", "{log-dir}/"+name+".tgz", ".")
	t.Fatalf("successfully created checkpoints; failing test on purpose")
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
				t.Fatal(err)
			}
			return false
		},
	}
}
