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
	"runtime"
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

const (
	baseVersion = "19.2.5"
	headVersion = "HEAD"
)

var v192 = roachpb.Version{Major: 19, Minor: 2}

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
	runWorkLoadFeature(
		"schemachange workload",
		v192,
		"schemachange --concurrency 2 --max-ops 150 --verbose=true",
	),
}

func runVersionUpgrade(ctx context.Context, t *test, c *cluster) {
	// This is ugly, but we can't pass `--encrypt=false` to old versions of
	// Cockroach.
	//
	// TODO(tbg): revisit as old versions are aged out of this test.
	c.encryptDefault = false

	u := newVersionUpgradeTest(c, versionUpgradeTestFeatures,
		// NB: after the next step, cluster and binary version equals baseVersion,
		// and auto-upgrades are on.
		uploadAndstartStep(baseVersion),

		// HEAD gives us the main binary for this roachtest run. We upgrade into
		// this version more capriciously to ensure better coverage by first
		// rolling the cluster into the new version with auto-upgrade disabled,
		// then rolling back, and then rolling forward and finalizing
		// (automatically).
		preventAutoUpgradeStep(),
		// Roll nodes forward.
		// Run the features between each node upgrade!
		binaryUpgradeStep(3, headVersion),
		binaryUpgradeStep(2, headVersion),
		binaryUpgradeStep(1, headVersion),
		binaryUpgradeStep(4, headVersion),
		// Roll back again. Note that bad things would happen if the cluster had
		// ignored our request to not auto-upgrade. The `autoupgrade` roachtest
		// exercises this in more detail, so here we just rely on things working
		// as they ought to.
		binaryUpgradeStep(4, baseVersion),
		binaryUpgradeStep(1, baseVersion),
		binaryUpgradeStep(2, baseVersion),
		binaryUpgradeStep(3, baseVersion),
		// Roll nodes forward, this time allowing them to upgrade.
		binaryUpgradeStep(2, headVersion),
		binaryUpgradeStep(3, headVersion),
		binaryUpgradeStep(1, headVersion),
		binaryUpgradeStep(4, headVersion),
		allowAutoUpgradeStep(),
		waitForUpgradeStep(),
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

func uploadAndstartStep(v string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		args := u.uploadVersion(ctx, t, v)
		// Stage workload on all nodes as the load node to run workload is chosen
		// randomly.
		u.c.Put(ctx, workload, "./workload", u.c.All())
		// NB: can't start sequentially since cluster already bootstrapped.
		u.c.Start(ctx, t, u.c.All(), args, startArgsDontEncrypt, roachprodArgOption{"--sequential=false"})
	}
}

// binaryUpgradeStep rolling-restarts the given nodes into the new binary
// version. Note that this does *not* wait for the cluster version to upgrade.
// Use a waitForUpgradeStep() for that.
func binaryUpgradeStep(node int, newVersion string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		c := u.c
		args := u.uploadVersion(ctx, t, newVersion)

		t.l.Printf("restarting node %d into %s", node, newVersion)
		c.Stop(ctx, c.Node(node))
		c.Start(ctx, t, c.Node(node), args, startArgsDontEncrypt)
		t.l.Printf("node %d now running binary version %s", node, u.binaryVersion(ctx, t, node))
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

		for i := 1; i <= c.spec.NodeCount; i++ {
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

func runWorkLoadFeature(name string, minVersion roachpb.Version, wkld string) versionFeatureTest {
	return versionFeatureTest{
		name: name,
		fn: func(ctx context.Context, t *test, u *versionUpgradeTest) (skipped bool) {
			loadNode := u.c.All().randNode()[0]
			if u.clusterVersion(ctx, t, loadNode).Less(minVersion) {
				return true // skipped
			}
			u.c.Run(ctx, u.c.Node(loadNode), "./workload init schemachange")
			runCmd := []string{
				"./workload run",
				wkld,
				fmt.Sprintf("{pgurl:1-%d}", u.c.spec.NodeCount),
			}
			u.c.Run(ctx, u.c.Node(loadNode), runCmd...)
			return false
		},
	}
}
