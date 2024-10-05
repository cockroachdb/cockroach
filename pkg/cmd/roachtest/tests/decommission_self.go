// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

// runDecommissionSelf decommissions n2 through n2. This is an acceptance test.
//
// See https://github.com/cockroachdb/cockroach/issues/56718
func runDecommissionSelf(ctx context.Context, t test.Test, c cluster.Cluster) {
	allNodes := c.All()
	u := newVersionUpgradeTest(c,
		uploadCockroachStep(allNodes, clusterupgrade.CurrentVersion()),
		startVersion(allNodes, clusterupgrade.CurrentVersion()),
		fullyDecommissionStep(2, 2, clusterupgrade.CurrentVersion()),
		func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			// Stop n2 and exclude it from post-test consistency checks,
			// as this node can't contact cluster any more and operations
			// on it will hang.
			u.c.Wipe(ctx, c.Node(2))
		},
		checkOneMembership(1, "decommissioned"),
	)

	u.run(ctx, t)
}
