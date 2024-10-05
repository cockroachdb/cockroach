// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
)

// runDecommissionMixedVersions runs through randomized
// decommission/recommission processes in mixed-version clusters.
func runDecommissionMixedVersions(
	ctx context.Context, t test.Test, c cluster.Cluster, buildVersion *version.Version,
) {
	predecessorVersionStr, err := release.LatestPredecessor(buildVersion)
	if err != nil {
		t.Fatal(err)
	}
	predecessorVersion := clusterupgrade.MustParseVersion(predecessorVersionStr)

	h := newDecommTestHelper(t, c)

	pinnedUpgrade := h.getRandNode()
	t.L().Printf("pinned n%d for upgrade", pinnedUpgrade)

	// NB: The suspect duration must be at least 10s, as versions 23.2 and
	// beyond will reset to the default of 30s if it fails validation, even if
	// set by a previous version.
	const suspectDuration = 10 * time.Second

	allNodes := c.All()
	u := newVersionUpgradeTest(c,
		// We upload both binaries to each node, to be able to vary the binary
		// used when issuing `cockroach node` subcommands.
		uploadCockroachStep(allNodes, predecessorVersion),
		uploadCockroachStep(allNodes, clusterupgrade.CurrentVersion()),

		startVersion(allNodes, predecessorVersion),
		waitForUpgradeStep(allNodes),
		preventAutoUpgradeStep(h.nodeIDs[0]),
		suspectLivenessSettingsStep(h.nodeIDs[0], suspectDuration),

		preloadDataStep(pinnedUpgrade),

		// We upgrade a pinned node and one other random node of the cluster to the current version.
		binaryUpgradeStep(c.Node(pinnedUpgrade), clusterupgrade.CurrentVersion()),
		binaryUpgradeStep(c.Node(h.getRandNodeOtherThan(pinnedUpgrade)), clusterupgrade.CurrentVersion()),
		checkAllMembership(pinnedUpgrade, "active"),

		// After upgrading, which restarts the nodes, ensure that nodes are not
		// considered suspect unnecessarily.
		sleepStep(2*suspectDuration),

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
		binaryUpgradeStep(allNodes, clusterupgrade.CurrentVersion()),
		allowAutoUpgradeStep(1),
		waitForUpgradeStep(allNodes),

		// Again ensure that nodes are not considered suspect unnecessarily.
		sleepStep(2*suspectDuration),

		// Fully decommission a random node. Note that we can no longer use the
		// predecessor cli, as the cluster has upgraded and won't allow connections
		// from the predecessor version binary.
		//
		// Note also that this has to remain the last step unless we want this test to
		// handle the fact that the decommissioned node will no longer be able
		// to communicate with the cluster (i.e. most commands against it will fail).
		// This is also why we're making sure to avoid decommissioning the pinned node
		// itself, as we use it to check the membership after.
		fullyDecommissionStep(
			h.getRandNodeOtherThan(pinnedUpgrade), h.getRandNode(), clusterupgrade.CurrentVersion(),
		),
		checkOneMembership(pinnedUpgrade, "decommissioned"),
	)

	u.run(ctx, t)
}

// suspectLivenessSettingsStep sets the duration a node is considered "suspect"
// after it becomes unavailable.
func suspectLivenessSettingsStep(target int, suspectDuration time.Duration) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, target)
		_, err := db.ExecContext(ctx, `SET CLUSTER SETTING server.time_after_store_suspect = $1`, suspectDuration.String())
		if err != nil {
			t.Fatal(err)
		}
	}
}

// preloadDataStep load data into cluster to ensure we have a large enough
// number of replicas to move on decommissioning.
func preloadDataStep(target int) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		// Load data into cluster to ensure we have a large enough number of replicas
		// to move on decommissioning.
		c := u.c
		c.Run(ctx, c.Node(target),
			`./cockroach workload fixtures import tpcc --warehouses=100 {pgurl:1}`)
		db := c.Conn(ctx, t.L(), target)
		defer db.Close()
		if err := WaitFor3XReplication(ctx, t, db); err != nil {
			t.Fatal(err)
		}
	}
}

// partialDecommissionStep runs `cockroach node decommission --wait=none` from a
// given node, targeting another. It uses the specified binary version to run
// the command.
func partialDecommissionStep(target, from int, binaryVersion *clusterupgrade.Version) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		c := u.c
		c.Run(ctx, c.Node(from), clusterupgrade.CockroachPathForVersion(t, binaryVersion), "node", "decommission",
			"--wait=none", strconv.Itoa(target), "--port", fmt.Sprintf("{pgport:%d}", from), fmt.Sprintf("--certs-dir=%s", install.CockroachNodeCertsDir))
	}
}

// recommissionAllStep runs `cockroach node recommission` from a given node,
// targeting all nodes in the cluster. It uses the specified binary version to
// run the command.
func recommissionAllStep(from int, binaryVersion *clusterupgrade.Version) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		c := u.c
		c.Run(ctx, c.Node(from), clusterupgrade.CockroachPathForVersion(t, binaryVersion), "node", "recommission",
			c.All().NodeIDsString(), "--port", fmt.Sprintf("{pgport:%d}", from), fmt.Sprintf("--certs-dir=%s", install.CockroachNodeCertsDir))
	}
}

// fullyDecommissionStep is like partialDecommissionStep, except it uses
// `--wait=all`.
func fullyDecommissionStep(target, from int, binaryVersion *clusterupgrade.Version) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		c := u.c
		c.Run(ctx, c.Node(from), clusterupgrade.CockroachPathForVersion(t, binaryVersion), "node", "decommission",
			"--wait=all", strconv.Itoa(target), "--port={pgport:1}", fmt.Sprintf("--certs-dir=%s", install.CockroachNodeCertsDir))

		// If we are decommissioning a target node from the same node, the drain
		// step will be skipped. In this case, we should not consider the step done
		// until the health check for the node returns non-200 OK.
		// TODO(sarkesian): This could be removed after 23.2, as in these versions
		// the "decommissioned" state is considered in the health check.
		if target == from {
			t.L().Printf("waiting for n%d to fail health check after decommission")
			var healthCheckURL string
			if addrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Node(target)); err != nil {
				t.Fatalf("failed to get admin ui addresses: %v", err)
			} else {
				healthCheckURL = fmt.Sprintf(`http://%s/health?ready=1`, addrs[0])
			}

			if err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
				resp, err := httputil.Get(ctx, healthCheckURL)
				if err != nil {
					return errors.Wrapf(err, "failed to get n%d /health?ready=1 HTTP endpoint", target)
				}
				if resp.StatusCode == 200 {
					return errors.Errorf("n%d /health?ready=1 status=%d %s "+
						"(expected 503 service unavailable after decommission)",
						target, resp.StatusCode, resp.Status)
				}

				t.L().Printf("n%d /health?ready=1 status=%d %s", target, resp.StatusCode, resp.Status)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// checkOneDecommissioning checks against the `decommissioning` column in
// crdb_internal.gossip_liveness, asserting that only one node is marked as
// decommissioning.
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
// decommissioning.
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
// the specified membership status.
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
// the specified membership status.
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

// uploadCockroachStep uploads the specified cockroach binary version on the specified
// nodes.
func uploadCockroachStep(nodes option.NodeListOption, version *clusterupgrade.Version) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		uploadCockroach(ctx, t, u.c, nodes, version)
	}
}

// startVersion starts the specified cockroach binary version on the specified
// nodes.
func startVersion(nodes option.NodeListOption, version *clusterupgrade.Version) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		settings := install.MakeClusterSettings(install.BinaryOption(
			clusterupgrade.CockroachPathForVersion(t, version),
		))
		startOpts := option.DefaultStartOpts()
		u.c.Start(ctx, t.L(), startOpts, settings, nodes)
	}
}
