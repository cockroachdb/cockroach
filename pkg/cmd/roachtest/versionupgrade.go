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

type versionFeatureTest struct {
	name string
	fn   func(context.Context, *test, *versionUpgradeTest) (skipped bool)
}

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
var versionUpgradeTestFeatures = []versionFeatureTest{
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

const (
	headVersion = "HEAD"
)

func runVersionUpgrade(ctx context.Context, t *test, c *cluster) {
	// This is ugly, but we can't pass `--encrypt=false` to old versions of
	// Cockroach.
	//
	// TODO(tbg): revisit as old versions are aged out of this test.
	c.encryptDefault = false

	const baseVersion = "19.1.5"
	u := newVersionUpgradeTest(c, versionUpgradeTestFeatures,
		// Load baseVersion fixture. That fixture's cluster version may be
		// at the predecessor version, so add a waitForUpgradeStep to make
		// sure we're actually running at baseVersion before moving on.
		//
		// See the comment on createCheckpoints for details on fixtures.
		uploadAndStartFromCheckpointFixture(baseVersion),
		waitForUpgradeStep(),

		// NB: before the next step, cluster and binary version equals baseVersion,
		// and auto-upgrades are on.

		binaryUpgradeStep("19.2.1"),
		waitForUpgradeStep(),

		// Each new release has to be added here. When adding a new release,
		// you'll probably need to use a release candidate binary. You may also
		// want to bake in the last step above, i.e. run this test with
		// createCheckpoints=true, update the fixtures (see comments there), and
		// then change baseVersion to one release later.

		// HEAD gives us the main binary for this roachtest run. We upgrade into
		// this version more capriciously to ensure better coverage by first
		// rolling the cluster into the new version with auto-upgrade disabled,
		// then rolling back, and then rolling forward and finalizing
		// (automatically).
		preventAutoUpgradeStep(),
		// Roll nodes forward.
		binaryUpgradeStep("HEAD"),
		// Roll back again. Note that bad things would happen if the cluster had
		// ignored our request to not auto-upgrade. The `autoupgrade` roachtest
		// exercises this in more detail, so here we just rely on things working
		// as they ought to.
		binaryUpgradeStep("19.2.1"),
		// Roll nodes forward, this time allowing them to upgrade.
		binaryUpgradeStep("HEAD"),
		allowAutoUpgradeStep(),
		waitForUpgradeStep(),
	)

	u.run(ctx, t)
}

// createCheckpoints is used to "age out" old versions of CockroachDB. We want
// to test data that was created at v1.0, but we don't actually want to run a
// long chain of binaries starting all the way at v1.0. Instead, we periodically
// bake a set of store directories that originally started out on v1.0 and
// maintain it as a fixture for this test.
//
// The checkpoints will be created in the log directories. The test will fail
// on purpose when it's done. After, manually invoke the following to move the
// archives to the right place and commit the result:
//
// for i in 1 2 3 4; do
//   mkdir -p pkg/cmd/roachtest/fixtures/${i} && \
//   mv artifacts/acceptance/version-upgrade/run_1/${i}.logs/checkpoint-*.tgz \
//     pkg/cmd/roachtest/fixtures/${i}/
// done
const createCheckpoints = false

func (u *versionUpgradeTest) run(ctx context.Context, t *test) {
	if createCheckpoints {
		// We rely on cockroach-HEAD to create checkpoint, so upload it early.
		_ = u.uploadVersion(ctx, t, headVersion)
	}

	defer func() {
		for _, db := range u.conns {
			_ = db.Close()
		}
	}()

	for _, step := range u.steps {
		step(ctx, t, u)
		for _, feature := range u.features {
			t.l.Printf("checking %s", feature.name)
			tBegin := timeutil.Now()
			skipped := feature.fn(ctx, t, u)
			dur := fmt.Sprintf("%.2fs", timeutil.Since(tBegin).Seconds())
			if skipped {
				t.l.Printf("^-- skip (%s)", dur)
			} else {
				t.l.Printf("^-- ok (%s)", dur)
			}
		}
	}

	if createCheckpoints {
		t.Fatal("failing on purpose to have store snapshots collected in artifacts")
	}
}

type versionUpgradeTest struct {
	goOS     string
	c        *cluster
	steps    []versionStep
	features []versionFeatureTest

	// Cache conns because opening one takes hundreds of ms, and we do it quite
	// a lot.
	conns []*gosql.DB
}

func newVersionUpgradeTest(
	c *cluster, features []versionFeatureTest, steps ...versionStep,
) *versionUpgradeTest {
	return &versionUpgradeTest{
		goOS:     ifLocal(runtime.GOOS, "linux"),
		c:        c, // all nodes are CRDB nodes
		steps:    steps,
		features: features,
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

func (u *versionUpgradeTest) uploadVersion(ctx context.Context, t *test, newVersion string) option {
	var binary string
	if newVersion == headVersion {
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

	target := "./cockroach-" + newVersion
	u.c.Put(ctx, binary, target, u.c.All())
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

func uploadAndStartFromCheckpointFixture(version string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		nodes := u.c.All()
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

		// Start the binary.
		uploadAndstartStep(version)(ctx, t, u)
	}
}

func uploadAndstartStep(v string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		args := u.uploadVersion(ctx, t, v)
		// NB: can't start sequentially since cluster already bootstrapped.
		u.c.Start(ctx, t, u.c.All(), args, startArgsDontEncrypt, roachprodArgOption{"--sequential=false"})
	}
}

// binaryUpgradeStep rolling-restarts the given nodes into the new binary
// version. Note that this does *not* wait for the cluster version to upgrade.
// Use a waitForUpgradeStep() for that.
func binaryUpgradeStep(newVersion string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		c := u.c
		nodes := c.All()
		t.l.Printf("%s: binary\n", newVersion)
		args := u.uploadVersion(ctx, t, newVersion)

		// Restart nodes in a random order; otherwise node 1 would be running all
		// the migrations and it probably also has all the leases.
		rand.Shuffle(len(nodes), func(i, j int) {
			nodes[i], nodes[j] = nodes[j], nodes[i]
		})
		for _, node := range nodes {
			t.l.Printf("restarting node %d into %s", node, newVersion)
			c.Stop(ctx, c.Node(node))
			c.Start(ctx, t, c.Node(node), args, startArgsDontEncrypt)
			t.l.Printf("node %d now running binary version %s", node, u.binaryVersion(ctx, t, node))

			// TODO(nvanbenschoten): add upgrade qualification step. What should we
			// test? We could run logictests. We could add custom logic here. Maybe
			// this should all be pushed to nightly migration tests instead.
		}

		if createCheckpoints && newVersion != headVersion {
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
			name := checkpointName(newVersion)
			c.Stop(ctx, nodes)
			c.Run(ctx, c.All(), "./cockroach-HEAD", "debug", "rocksdb", "--db={store-dir}",
				"checkpoint", "--checkpoint_dir={store-dir}/"+name)
			c.Run(ctx, c.All(), "tar", "-C", "{store-dir}/"+name, "-czf", "{log-dir}/"+name+".tgz", ".")
			c.Start(ctx, t, nodes, args, startArgsDontEncrypt, roachprodArgOption{"--sequential=false"})
		}
	}
}

func preventAutoUpgradeStep() versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, 1)
		_, err := db.ExecContext(ctx, `SET CLUSTER SETTING cluster.preserve_downgrade_option = $1`, u.binaryVersion(ctx, t, 1).String())
		if err != nil {
			t.Fatal(err)
		}
	}
}

func allowAutoUpgradeStep() versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, 1)
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
func waitForUpgradeStep() versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		c := u.c
		newVersion := u.binaryVersion(ctx, t, 1).String()
		t.l.Printf("%s: waiting for cluster to auto-upgrade\n", newVersion)

		for i := 1; i < c.spec.NodeCount; i++ {
			err := retry.ForDuration(30*time.Second, func() error {
				db := u.conn(ctx, t, i)

				var currentVersion string
				if err := db.QueryRow("SHOW CLUSTER SETTING version").Scan(&currentVersion); err != nil {
					t.Fatalf("%d: %s", i, err)
				}
				if currentVersion != newVersion {
					return fmt.Errorf("%d: expected version %s, got %s", i, newVersion, currentVersion)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		t.l.Printf("%s: cluster is upgraded\n", newVersion)

		// TODO(nvanbenschoten): add upgrade qualification step.
	}
}

func stmtFeatureTest(
	name string, minVersion roachpb.Version, stmt string, args ...interface{},
) versionFeatureTest {
	return versionFeatureTest{
		name: name,
		fn: func(ctx context.Context, t *test, u *versionUpgradeTest) (skipped bool) {
			i := u.c.All().randNode()[0]
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
