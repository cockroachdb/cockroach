// Copyright 2020 The Cockroach Authors.
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
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
)

// runDecommissionMixedVersions runs through randomized
// decommission/recommission processes in mixed-version clusters.
func runDecommissionMixedVersions(
	ctx context.Context, t *test, c *cluster, buildVersion version.Version,
) {
	predecessorVersion, err := PredecessorVersion(buildVersion)
	if err != nil {
		t.Fatal(err)
	}

	h := newDecommTestHelper(t, c)

	// The v20.2 CLI can only be run against servers running v20.2. For this
	// reason, we grab a handle on a specific server slated for an upgrade.
	pinnedUpgrade := h.getRandNode()
	t.l.Printf("pinned n%d for upgrade", pinnedUpgrade)

	// An empty string means that the cockroach binary specified by flag
	// `cockroach` will be used.
	const mainVersion = ""
	allNodes := c.All()
	u := newVersionUpgradeTest(c,
		// We upload both binaries to each node, to be able to vary the binary
		// used when issuing `cockroach node` subcommands.
		uploadVersion(allNodes, predecessorVersion),
		uploadVersion(allNodes, mainVersion),

		startVersion(allNodes, predecessorVersion),
		waitForUpgradeStep(allNodes),
		preventAutoUpgradeStep(h.nodeIDs[0]),

		// We upgrade a subset of the cluster to v20.2.
		binaryUpgradeStep(c.Node(pinnedUpgrade), mainVersion),
		binaryUpgradeStep(c.Node(h.getRandNodeOtherThan(pinnedUpgrade)), mainVersion),
		checkAllMembership(pinnedUpgrade, "active"),

		// 1. Partially decommission a random node from another random node. We
		// use the v20.1 CLI to do so.
		partialDecommissionStep(h.getRandNode(), h.getRandNode(), predecessorVersion),
		checkOneDecommissioning(h.getRandNode()),
		checkOneMembership(pinnedUpgrade, "decommissioning"),

		// 2. Recommission all nodes, including the partially decommissioned
		// one, from a random node. Use the v20.1 CLI to do so.
		recommissionAllStep(h.getRandNode(), predecessorVersion),
		checkNoDecommissioning(h.getRandNode()),
		checkAllMembership(pinnedUpgrade, "active"),
		//
		// 3. Attempt to fully decommission a from a random node, again using
		// the v20.1 CLI.
		fullyDecommissionStep(h.getRandNode(), h.getRandNode(), predecessorVersion),
		checkOneDecommissioning(h.getRandNode()),
		checkOneMembership(pinnedUpgrade, "decommissioning"),

		// Roll back, which should to be fine because the cluster upgrade was
		// not finalized.
		binaryUpgradeStep(allNodes, predecessorVersion),
		checkOneDecommissioning(h.getRandNode()),

		// Repeat similar recommission/decommission cycles as above. We can no
		// longer assert against the `membership` column as none of the servers
		// are running v20.2.
		recommissionAllStep(h.getRandNode(), predecessorVersion),
		checkNoDecommissioning(h.getRandNode()),

		partialDecommissionStep(h.getRandNode(), h.getRandNode(), predecessorVersion),
		checkOneDecommissioning(h.getRandNode()),

		// Roll all nodes forward, and finalize upgrade.
		binaryUpgradeStep(allNodes, mainVersion),
		allowAutoUpgradeStep(1),
		waitForUpgradeStep(allNodes),

		checkOneMembership(h.getRandNode(), "decommissioning"),

		// Use the v20.2 CLI here on forth. Lets start with recommissioning all
		// the nodes in the cluster.
		recommissionAllStep(h.getRandNode(), mainVersion),
		checkNoDecommissioning(h.getRandNode()),
		checkAllMembership(h.getRandNode(), "active"),

		// We partially decommission a random node.
		partialDecommissionStep(h.getRandNode(), h.getRandNode(), mainVersion),
		checkOneDecommissioning(h.getRandNode()),
		checkOneMembership(h.getRandNode(), "decommissioning"),

		// We check that recommissioning is still functional.
		recommissionAllStep(h.getRandNode(), mainVersion),
		checkNoDecommissioning(h.getRandNode()),
		checkAllMembership(h.getRandNode(), "active"),

		// We fully decommission a random node. We need to use the v20.2 CLI to
		// do so.
		fullyDecommissionStep(h.getRandNode(), h.getRandNode(), mainVersion),
		checkOneDecommissioning(h.getRandNode()),
		checkOneMembership(h.getRandNode(), "decommissioned"),
	)

	u.run(ctx, t)
}

// cockroachBinaryPath is a shorthand to retrieve the path for a cockroach
// binary of a given version.
func cockroachBinaryPath(version string) string {
	path := "./cockroach"
	if version != "" {
		path += "-" + version
	}
	return path
}

// partialDecommissionStep runs `cockroach node decommission --wait=none` from a
// given node, targeting another. It uses the specified binary version to run
// the command.
func partialDecommissionStep(target, from int, binaryVersion string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		c := u.c
		c.Run(ctx, c.Node(from), cockroachBinaryPath(binaryVersion), "node", "decommission",
			"--wait=none", "--insecure", strconv.Itoa(target))
	}
}

// recommissionAllStep runs `cockroach node recommission` from a given node,
// targeting all nodes in the cluster. It uses the specified binary version to
// run the command.
func recommissionAllStep(from int, binaryVersion string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		c := u.c
		c.Run(ctx, c.Node(from), cockroachBinaryPath(binaryVersion), "node", "recommission",
			"--insecure", c.All().nodeIDsString())
	}
}

// fullyDecommissionStep is like partialDecommissionStep, except it uses
// `--wait=all`.
func fullyDecommissionStep(target, from int, binaryVersion string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		c := u.c
		c.Run(ctx, c.Node(from), cockroachBinaryPath(binaryVersion), "node", "decommission",
			"--wait=all", "--insecure", strconv.Itoa(target))
	}
}

// checkOneDecommissioning checks against the `decommissioning` column in
// crdb_internal.gossip_liveness, asserting that only one node is marked as
// decommissioning. This check can be run against both v20.1 and v20.2 servers.
func checkOneDecommissioning(from int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		// We use a retry block here (and elsewhere) because we're consulting
		// crdb_internal.gossip_liveness, and need to make allowances for gossip
		// propagation delays.
		if err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
			db := u.conn(ctx, t, from)
			var count int
			if err := db.QueryRow(
				`select count(*) from crdb_internal.gossip_liveness where decommissioning = true;`).Scan(&count); err != nil {
				t.Fatal(err)
			}

			if count != 1 {
				return errors.Newf("expected to find 1 node with decommissioning=true, found %d", count)
			}

			var nodeID int
			if err := db.QueryRow(
				`select node_id from crdb_internal.gossip_liveness where decommissioning = true;`).Scan(&nodeID); err != nil {
				t.Fatal(err)
			}
			t.l.Printf("n%d decommissioning=true", nodeID)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// checkNoDecommissioning checks against the `decommissioning` column in
// crdb_internal.gossip_liveness, asserting that only no nodes are marked as
// decommissioning. This check can be run against both v20.1 and v20.2 servers.
func checkNoDecommissioning(from int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		if err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
			db := u.conn(ctx, t, from)
			var count int
			if err := db.QueryRow(
				`select count(*) from crdb_internal.gossip_liveness where decommissioning = true;`).Scan(&count); err != nil {
				t.Fatal(err)
			}

			if count != 0 {
				return errors.Newf("expected to find 0 nodes with decommissioning=false, found %d", count)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// checkOneMembership checks against the `membership` column in
// crdb_internal.gossip_liveness, asserting that only one node is marked with
// the specified membership status. This check can be only be run against
// servers running v20.2 and beyond.
func checkOneMembership(from int, membership string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		if err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
			db := u.conn(ctx, t, from)
			var count int
			if err := db.QueryRow(
				`select count(*) from crdb_internal.gossip_liveness where membership = $1;`, membership).Scan(&count); err != nil {
				t.Fatal(err)
			}

			if count != 1 {
				return errors.Newf("expected to find 1 node with membership=%s, found %d", membership, count)
			}

			var nodeID int
			if err := db.QueryRow(
				`select node_id from crdb_internal.gossip_liveness where decommissioning = true;`).Scan(&nodeID); err != nil {
				t.Fatal(err)
			}
			t.l.Printf("n%d membership=%s", nodeID, membership)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// checkAllMembership checks against the `membership` column in
// crdb_internal.gossip_liveness, asserting that all nodes are marked with
// the specified membership status. This check can be only be run against
// servers running v20.2 and beyond.
func checkAllMembership(from int, membership string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		if err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
			db := u.conn(ctx, t, from)
			var count int
			if err := db.QueryRow(
				`select count(*) from crdb_internal.gossip_liveness where membership != $1;`, membership).Scan(&count); err != nil {
				t.Fatal(err)
			}

			if count != 0 {
				return errors.Newf("expected to find 0 nodes with membership!=%s, found %d", membership, count)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// uploadVersion uploads the specified cockroach binary version on the specified
// nodes.
func uploadVersion(nodes nodeListOption, version string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		// Put the binary.
		u.uploadVersion(ctx, t, nodes, version)
	}
}

// startVersion starts the specified cockroach binary version on the specified
// nodes.
func startVersion(nodes nodeListOption, version string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		args := startArgs("--binary=" + cockroachBinaryPath(version))
		u.c.Start(ctx, t, nodes, args, startArgsDontEncrypt)
	}
}
