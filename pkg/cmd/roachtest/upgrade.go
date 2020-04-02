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
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/binfetcher"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

func registerUpgrade(r *testRegistry) {
	runUpgrade := func(ctx context.Context, t *test, c *cluster, oldVersion string) {
		nodes := c.spec.NodeCount
		goos := ifLocal(runtime.GOOS, "linux")

		b, err := binfetcher.Download(ctx, binfetcher.Options{
			Binary:  "cockroach",
			Version: "v" + oldVersion,
			GOOS:    goos,
			GOARCH:  "amd64",
		})
		if err != nil {
			t.Fatal(err)
		}

		c.Put(ctx, b, "./cockroach", c.Range(1, nodes))

		// NB: remove startArgsDontEncrypt across this file once we're not running
		// roachtest against v2.1 any more (which would start a v2.0 cluster here).
		c.Start(ctx, t, c.Range(1, nodes), startArgsDontEncrypt)

		const stageDuration = 30 * time.Second
		const timeUntilStoreDead = 90 * time.Second
		const buff = 10 * time.Second

		sleep := func(ts time.Duration) error {
			t.WorkerStatus("sleeping")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(ts):
				return nil
			}
		}

		db := c.Conn(ctx, 1)
		defer db.Close()
		// Without this line, the test reliably fails (at least on OSX), presumably
		// because a connection to a node that gets restarted somehow sticks around
		// in the pool and throws an error at the next client using it (instead of
		// transparently reconnecting).
		db.SetMaxIdleConns(0)

		if _, err := db.ExecContext(ctx,
			"SET CLUSTER SETTING server.time_until_store_dead = $1", timeUntilStoreDead.String(),
		); err != nil {
			t.Fatal(err)
		}

		if err := sleep(stageDuration); err != nil {
			t.Fatal(err)
		}

		decommissionAndStop := func(node int) error {
			t.WorkerStatus("decomission")
			port := fmt.Sprintf("{pgport:%d}", node)
			// Note that the following command line needs to run against both v2.1
			// and the current branch. Do not change it in a manner that is
			// incompatible with 2.1.
			if err := c.RunE(ctx, c.Node(node), "./cockroach quit --decommission --insecure --port="+port); err != nil {
				return err
			}
			t.WorkerStatus("stop")
			c.Stop(ctx, c.Node(node))
			return nil
		}

		clusterVersion := func() (string, error) {
			var version string
			if err := db.QueryRowContext(ctx, `SHOW CLUSTER SETTING version`).Scan(&version); err != nil {
				return "", errors.Wrap(err, "determining cluster version")
			}
			return version, nil
		}

		oldVersion, err = clusterVersion()
		if err != nil {
			t.Fatal(err)
		}

		checkUpgraded := func() (bool, error) {
			upgradedVersion, err := clusterVersion()
			if err != nil {
				return false, err
			}
			return upgradedVersion != oldVersion, nil
		}

		checkDowngradeOption := func(version string) error {
			if _, err := db.ExecContext(ctx,
				"SET CLUSTER SETTING cluster.preserve_downgrade_option = $1;", version,
			); err == nil {
				return fmt.Errorf("cluster.preserve_downgrade_option shouldn't be set to any other values besides current cluster version; was able to set it to %s", version)
			} else if !testutils.IsError(err, "cannot set cluster.preserve_downgrade_option") {
				return err
			}
			return nil
		}

		// Now perform a rolling restart into the new binary, except the last node.
		for i := 1; i < nodes; i++ {
			t.WorkerStatus("upgrading ", i)
			if err := c.StopCockroachGracefullyOnNode(ctx, i); err != nil {
				t.Fatal(err)
			}
			c.Put(ctx, cockroach, "./cockroach", c.Node(i))
			c.Start(ctx, t, c.Node(i), startArgsDontEncrypt)
			if err := sleep(stageDuration); err != nil {
				t.Fatal(err)
			}

			// Check cluster version is not upgraded until all nodes are running the new version.
			if upgraded, err := checkUpgraded(); err != nil {
				t.Fatal(err)
			} else if upgraded {
				t.Fatal("cluster setting version shouldn't be upgraded before all nodes are running the new version")
			}
		}

		// Now stop a previously started node and upgrade the last node.
		// Check cluster version is not upgraded.
		if err := c.StopCockroachGracefullyOnNode(ctx, nodes-1); err != nil {
			t.Fatal(err)
		}
		if err := c.StopCockroachGracefullyOnNode(ctx, nodes); err != nil {
			t.Fatal(err)
		}
		c.Put(ctx, cockroach, "./cockroach", c.Node(nodes))
		c.Start(ctx, t, c.Node(nodes), startArgsDontEncrypt)
		if err := sleep(stageDuration); err != nil {
			t.Fatal(err)
		}

		if upgraded, err := checkUpgraded(); err != nil {
			t.Fatal(err)
		} else if upgraded {
			t.Fatal("cluster setting version shouldn't be upgraded before all non-decommissioned nodes are alive")
		}

		// Now decommission and stop n3.
		// The decommissioned nodes should not prevent auto upgrade.
		if err := decommissionAndStop(nodes - 2); err != nil {
			t.Fatal(err)
		}
		if err := sleep(timeUntilStoreDead + buff); err != nil {
			t.Fatal(err)
		}

		// Check cannot set cluster setting cluster.preserve_downgrade_option to any
		// value besides the old cluster version.
		if err := checkDowngradeOption("1.9"); err != nil {
			t.Fatal(err)
		}
		if err := checkDowngradeOption("99.9"); err != nil {
			t.Fatal(err)
		}

		// Set cluster setting cluster.preserve_downgrade_option to be current
		// cluster version to prevent upgrade.
		if _, err := db.ExecContext(ctx,
			"SET CLUSTER SETTING cluster.preserve_downgrade_option = $1;", oldVersion,
		); err != nil {
			t.Fatal(err)
		}
		if err := sleep(stageDuration); err != nil {
			t.Fatal(err)
		}

		// Restart the previously stopped node.
		c.Start(ctx, t, c.Node(nodes-1), startArgsDontEncrypt)
		if err := sleep(stageDuration); err != nil {
			t.Fatal(err)
		}

		t.WorkerStatus("check cluster version has not been upgraded")
		if upgraded, err := checkUpgraded(); err != nil {
			t.Fatal(err)
		} else if upgraded {
			t.Fatal("cluster setting version shouldn't be upgraded because cluster.preserve_downgrade_option is set properly")
		}

		// Check cannot set cluster setting version until cluster.preserve_downgrade_option
		// is cleared.
		if _, err := db.ExecContext(ctx,
			"SET CLUSTER SETTING version = crdb_internal.node_executable_version();",
		); err == nil {
			t.Fatal("should not be able to set cluster setting version before resetting cluster.preserve_downgrade_option")
		} else if !testutils.IsError(err, "cluster.preserve_downgrade_option is set to") {
			t.Fatal(err)
		}

		// Reset cluster.preserve_downgrade_option to enable upgrade.
		if _, err := db.ExecContext(ctx,
			"RESET CLUSTER SETTING cluster.preserve_downgrade_option;",
		); err != nil {
			t.Fatal(err)
		}
		if err := sleep(stageDuration); err != nil {
			t.Fatal(err)
		}

		// Check if the cluster version has been upgraded.
		t.WorkerStatus("check cluster version has been upgraded")
		if upgraded, err := checkUpgraded(); err != nil {
			t.Fatal(err)
		} else if !upgraded {
			t.Fatalf("cluster setting version is not upgraded, still %s", oldVersion)
		}

		// Finally, check if the cluster.preserve_downgrade_option has been reset.
		t.WorkerStatus("check cluster setting cluster.preserve_downgrade_option has been set to an empty string")
		var downgradeVersion string
		if err := db.QueryRowContext(ctx,
			"SHOW CLUSTER SETTING cluster.preserve_downgrade_option",
		).Scan(&downgradeVersion); err != nil {
			t.Fatal(err)
		}
		if downgradeVersion != "" {
			t.Fatalf("cluster setting cluster.preserve_downgrade_option is %s, should be an empty string", downgradeVersion)
		}

		// Start n3 again to satisfy the dead node detector.
		c.Start(ctx, t, c.Node(nodes-2))
	}

	r.Add(testSpec{
		Name:       fmt.Sprintf("upgrade"),
		Owner:      OwnerKV,
		MinVersion: "v2.1.0",
		Cluster:    makeClusterSpec(5),
		Run: func(ctx context.Context, t *test, c *cluster) {
			pred, err := PredecessorVersion(r.buildVersion)
			if err != nil {
				t.Fatal(err)
			}
			runUpgrade(ctx, t, c, pred)
		},
	})
}

type versionFeatureTest struct {
	name string
	fn   func(context.Context, *test, *versionUpgradeTest) (skipped bool)
}

var v20 = roachpb.Version{Major: 2}

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
	stmtFeatureTest("Object Access", v20, `
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
	stmtFeatureTest("JSONB", v20, `
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE test.t (j JSONB);
DROP TABLE test.t;
	`),
	stmtFeatureTest("Sequences", v20, `
CREATE DATABASE IF NOT EXISTS test;
CREATE SEQUENCE test.test_sequence;
DROP SEQUENCE test.test_sequence;
	`),
	stmtFeatureTest("Computed Columns", v20, `
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

	const baseVersion = "v19.1.5"
	u := newVersionUpgradeTest(c, versionUpgradeTestFeatures,
		// Load baseVersion fixture. That fixture's cluster version may be
		// at the predecessor version, so add a waitForUpgradeStep to make
		// sure we're actually running at baseVersion before moving on.
		//
		// See the comment on createCheckpoints for details on fixtures.
		uploadAndStartFromCheckpointFixture(baseVersion),
		waitForUpgradeStep(),

		// NB: before the first step, cluster and binary version equals baseVersion.
		binaryUpgradeStep("v19.2.1"),
		waitForUpgradeStep(),

		// Each new release has to be added here. When adding a new release, you'll
		// probably need to use a release candidate binary.

		// HEAD gives us the main binary for this roachtest run.
		binaryUpgradeStep("HEAD"),
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

	c := u.c

	db := c.Conn(ctx, 1)
	defer db.Close()

	for _, step := range u.steps {
		step.run(ctx, t, u)
		for _, feature := range u.features {
			t.l.Printf("checking %s", feature.name)
			if skipped := feature.fn(ctx, t, u); skipped {
				t.l.Printf("^-- skipped")
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

func checkpointName(binaryVersion string) string { return "checkpoint-" + binaryVersion }

func (u *versionUpgradeTest) uploadVersion(ctx context.Context, t *test, newVersion string) option {
	var binary string
	if newVersion == headVersion {
		binary = cockroach
	} else {
		var err error
		binary, err = binfetcher.Download(ctx, binfetcher.Options{
			Binary:  "cockroach",
			Version: newVersion,
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

func (u *versionUpgradeTest) checkNode(
	ctx context.Context, t *test, nodeIdx int, newVersion string,
) {
	err := retry.ForDuration(30*time.Second, func() error {
		db := u.c.Conn(ctx, nodeIdx)
		defer db.Close()

		// 'Version' for 1.1, 'Tag' in 1.0.x.
		var version string
		if err := db.QueryRow(
			`SELECT value FROM crdb_internal.node_build_info where field IN ('Version' , 'Tag')`,
		).Scan(&version); err != nil {
			return err
		}
		if version != newVersion && newVersion != headVersion {
			t.Fatalf("created node at v%s, but it is %s", newVersion, version)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (u *versionUpgradeTest) version(ctx context.Context, t *test, i int) roachpb.Version {
	db := u.c.Conn(ctx, i)
	defer db.Close()

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
type versionStep struct {
	clusterVersion string // if empty, use crdb_internal.node_executable_version
	run            func(ctx context.Context, t *test, u *versionUpgradeTest)
}

func uploadAndStartFromCheckpointFixture(version string) versionStep {
	return versionStep{
		clusterVersion: "",
		run: func(ctx context.Context, t *test, u *versionUpgradeTest) {
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
			uploadAndstartStep(version).run(ctx, t, u)
		},
	}
}

func uploadAndstartStep(v string) versionStep {
	return versionStep{
		run: func(ctx context.Context, t *test, u *versionUpgradeTest) {
			args := u.uploadVersion(ctx, t, v)
			// NB: can't start sequentially since cluster already bootstrapped.
			u.c.Start(ctx, t, u.c.All(), args, startArgsDontEncrypt, roachprodArgOption{"--sequential=false"})
		},
	}
}

// binaryUpgradeStep rolling-restarts the cluster into the new binary version.
// Note that this does *not* wait for the cluster version to upgrade. Use a
// waitForUpgradeStep() for that.
func binaryUpgradeStep(newVersion string) versionStep {
	return versionStep{
		run: func(ctx context.Context, t *test, u *versionUpgradeTest) {
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
				t.l.Printf("%s: upgrading node %d\n", newVersion, node)
				c.Stop(ctx, c.Node(node))
				c.Start(ctx, t, c.Node(node), args, startArgsDontEncrypt)

				u.checkNode(ctx, t, node, newVersion)

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
		},
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
	return versionStep{
		run: func(ctx context.Context, t *test, u *versionUpgradeTest) {
			c := u.c
			var newVersion string // the cluster version to bump to

			db1 := c.Conn(ctx, 1)
			defer db1.Close()
			if err := db1.QueryRow(`SELECT crdb_internal.node_executable_version()`).Scan(&newVersion); err != nil {
				t.Fatal(err)
			}

			t.l.Printf("%s: waiting for cluster to auto-upgrade\n", newVersion)

			for i := 1; i < c.spec.NodeCount; i++ {
				err := retry.ForDuration(30*time.Second, func() error {
					db := c.Conn(ctx, i)
					defer db.Close()

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
		},
	}
}

func stmtFeatureTest(
	name string, minVersion roachpb.Version, stmt string, args ...interface{},
) versionFeatureTest {
	return versionFeatureTest{
		name: name,
		fn: func(ctx context.Context, t *test, u *versionUpgradeTest) (skipped bool) {
			i := u.c.All().randNode()[0]
			if u.version(ctx, t, i).Less(minVersion) {
				return true // skipped
			}
			db := u.c.Conn(ctx, i)
			defer db.Close()
			if _, err := db.ExecContext(ctx, stmt, args...); err != nil {
				t.Fatal(err)
			}
			return false
		},
	}
}
