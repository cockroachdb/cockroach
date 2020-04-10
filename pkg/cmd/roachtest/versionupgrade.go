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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/binfetcher"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
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

func runVersionUpgrade(ctx context.Context, t *test, c *cluster, predecessorVersion string) {
	// This is ugly, but we can't pass `--encrypt=false` to old versions of
	// Cockroach.
	//
	// TODO(tbg): revisit as old versions are aged out of this test.
	c.encryptDefault = false

	testFeaturesStep := func(ctx context.Context, t *test, u *versionUpgradeTest) {
		loadNode := u.c.All().randNode()[0]
		u.c.Run(ctx, u.c.Node(loadNode), "./workload init schemachange")
		runCmd := []string{
			"./workload run",
			"schemachange --concurrency 2 --max-ops 10 --verbose=1",
			fmt.Sprintf("{pgurl:1-%d}", u.c.spec.NodeCount),
		}
		t.l.Printf("running workload %s", strings.Join(runCmd, " "))

		err := u.c.RunE(ctx, u.c.Node(loadNode), runCmd...)
		if err != nil {
			t.l.Printf("running workload %s failed with %v", strings.Join(runCmd, " "), err)
			c.t.Fatal(err)
		}
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
		waitForUpgradeStep(c.All()),

		// NB: at this point, cluster and binary version equal predecessorVersion,
		// and auto-upgrades are on.

		// We use an empty string for the version below, which means to use the
		// main ./cockroach binary (i.e. the one being tested in this run).
		// We upgrade into this version more capriciously to ensure better
		// coverage by first rolling the cluster into the new version with
		// auto-upgrade disabled, then rolling back, and then rolling forward
		// and finalizing on the auto-upgrade path.
		preventAutoUpgradeStep(1),
		testFeaturesStep,
		// Roll nodes forward.
		binaryUpgradeStep(c.Node(3), ""),
		preventDeadlock(3),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(2), ""),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(1), ""),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(4), ""),
		testFeaturesStep,
		// Roll back again. Note that bad things would happen if the cluster had
		// ignored our request to not auto-upgrade. The `autoupgrade` roachtest
		// exercises this in more detail, so here we just rely on things working
		// as they ought to.
		binaryUpgradeStep(c.Node(2), predecessorVersion),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(4), predecessorVersion),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(3), predecessorVersion),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(1), predecessorVersion),
		testFeaturesStep,
		// Roll nodes forward, this time allowing them to upgrade, and waiting
		// for it to happen.
		binaryUpgradeStep(c.Node(4), ""),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(3), ""),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(1), ""),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(2), ""),
		testFeaturesStep,
		allowAutoUpgradeStep(1),
		waitForUpgradeStep(c.All()),
		testFeaturesStep,
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
		// Put and start the binary.
		args := u.uploadVersion(ctx, t, nodes, version)
		// Stage workload on all nodes as the load node to run workload is chosen
		// randomly.
		u.c.Put(ctx, workload, "./workload", u.c.All())
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

func preventDeadlock(node int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx, `SET CLUSTER SETTING sql.lease_manager.remove_lease_once_deferenced = true;`)
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
