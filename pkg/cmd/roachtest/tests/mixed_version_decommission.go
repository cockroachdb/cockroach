// Copyright 2020 The Cockroach Authors.
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
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
)

// runDecommissionMixedVersions runs through randomized
// decommission/recommission processes in mixed-version clusters.
func runDecommissionMixedVersions(
	ctx context.Context, t test.Test, c cluster.Cluster, buildVersion version.Version,
) {
	predecessorVersion, err := PredecessorVersion(buildVersion)
	if err != nil {
		t.Fatal(err)
	}

	h := newDecommTestHelper(t, c)

	// The v20.2 CLI can only be run against servers running v20.2. For this
	// reason, we grab a handle on a specific server slated for an upgrade.
	pinnedUpgrade := h.getRandNode()
	t.L().Printf("pinned n%d for upgrade", pinnedUpgrade)

	// An empty string means that the cockroach binary specified by flag
	// `cockroach` will be used.
	const mainVersion = ""
	allNodes := c.All()
	u := newVersionUpgradeTest(c,
		// We upload both binaries to each node, to be able to vary the binary
		// used when issuing `cockroach node` subcommands.
		uploadVersionStep(allNodes, predecessorVersion),
		uploadVersionStep(allNodes, mainVersion),

		startVersion(allNodes, predecessorVersion),
		waitForUpgradeStep(allNodes),
		preventAutoUpgradeStep(h.nodeIDs[0]),

		// We upgrade a pinnedUpgrade and one other random node of the cluster to v20.2.
		binaryUpgradeStep(c.Node(pinnedUpgrade), mainVersion),
		binaryUpgradeStep(c.Node(h.getRandNodeOtherThan(pinnedUpgrade)), mainVersion),
		checkAllMembership(pinnedUpgrade, "active"),

		// Partially decommission a random node from another random node. We
		// use the predecessor CLI to do so.
		partialDecommissionStep(h.getRandNode(), h.getRandNode(), predecessorVersion),
		checkOneDecommissioning(h.getRandNode()),
		checkOneMembership(pinnedUpgrade, "decommissioning"),

		// Recommission all nodes, including the partially decommissioned
		// one, from a random node. Use the predecessor CLI to do so.
		recommissionAllStep(h.getRandNode(), predecessorVersion),
		checkNoDecommissioning(h.getRandNode()),
		checkAllMembership(pinnedUpgrade, "active"),

		// Roll back, which should to be fine because the cluster upgrade was
		// not finalized.
		binaryUpgradeStep(allNodes, predecessorVersion),

		// Roll all nodes forward, and finalize upgrade.
		binaryUpgradeStep(allNodes, mainVersion),
		allowAutoUpgradeStep(1),
		waitForUpgradeStep(allNodes),

		// Fully decommission a random node. Note that we can no longer use the
		// predecessor cli, as the cluster has upgraded and won't allow connections
		// from the predecessor version binary.
		//
		// Note also that this has to remain the last step unless we want this test to
		// handle the fact that the decommissioned node will no longer be able
		// to communicate with the cluster (i.e. most commands against it will fail).
		// This is also why we're making sure to avoid decommissioning pinnedUpgrade
		// itself, as we use it to check the membership after.
		fullyDecommissionStep(h.getRandNodeOtherThan(pinnedUpgrade), h.getRandNode(), ""),
		checkOneMembership(pinnedUpgrade, "decommissioned"),
	)

	u.run(ctx, t)
}

// cockroachBinaryPath is a shorthand to retrieve the path for a cockroach
// binary of a given version.
func cockroachBinaryPath(version string) string {
	if version == "" {
		return "./cockroach"
	}
	return fmt.Sprintf("./v%s/cockroach", version)
}

// partialDecommissionStep runs `cockroach node decommission --wait=none` from a
// given node, targeting another. It uses the specified binary version to run
// the command.
func partialDecommissionStep(target, from int, binaryVersion string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		c := u.c
		c.Run(ctx, c.Node(from), cockroachBinaryPath(binaryVersion), "node", "decommission",
			"--wait=none", "--insecure", strconv.Itoa(target))
	}
}

// recommissionAllStep runs `cockroach node recommission` from a given node,
// targeting all nodes in the cluster. It uses the specified binary version to
// run the command.
func recommissionAllStep(from int, binaryVersion string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		c := u.c
		c.Run(ctx, c.Node(from), cockroachBinaryPath(binaryVersion), "node", "recommission",
			"--insecure", c.All().NodeIDsString())
	}
}

// fullyDecommissionStep is like partialDecommissionStep, except it uses
// `--wait=all`.
func fullyDecommissionStep(target, from int, binaryVersion string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		c := u.c
		c.Run(ctx, c.Node(from), cockroachBinaryPath(binaryVersion), "node", "decommission",
			"--wait=all", "--insecure", strconv.Itoa(target))
	}
}

// checkOneDecommissioning checks against the `decommissioning` column in
// crdb_internal.gossip_liveness, asserting that only one node is marked as
// decommissioning. This check can be run against both v20.1 and v20.2 servers.
func checkOneDecommissioning(from int) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
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
			t.L().Printf("n%d decommissioning=true", nodeID)
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
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
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
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
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
			t.L().Printf("n%d membership=%s", nodeID, membership)
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
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
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

// uploadVersionStep uploads the specified cockroach binary version on the specified
// nodes.
func uploadVersionStep(nodes option.NodeListOption, version string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		// Put the binary.
		uploadVersion(ctx, t, u.c, nodes, version)
	}
}

// startVersion starts the specified cockroach binary version on the specified
// nodes.
func startVersion(nodes option.NodeListOption, version string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		args := option.StartArgs("--binary=" + cockroachBinaryPath(version))
		u.c.Start(ctx, nodes, args, option.StartArgsDontEncrypt)
	}
}
