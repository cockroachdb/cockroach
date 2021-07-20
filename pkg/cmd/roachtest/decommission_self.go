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

import "context"

// runDecommissionSelf decommissions n2 through n2. This is an acceptance test.
//
// See https://github.com/cockroachdb/cockroach/issues/56718
func runDecommissionSelf(ctx context.Context, t *test, c *cluster) {
	// An empty string means that the cockroach binary specified by flag
	// `cockroach` will be used.
	const mainVersion = ""

	allNodes := c.All()
	u := newVersionUpgradeTest(c,
		uploadVersionStep(allNodes, mainVersion),
		startVersion(allNodes, mainVersion),
		fullyDecommissionStep(2, 2, mainVersion),
		func(ctx context.Context, t *test, u *versionUpgradeTest) {
			// Stop n2 and exclude it from post-test consistency checks,
			// as this node can't contact cluster any more and operations
			// on it will hang.
			u.c.Wipe(ctx, c.Node(2))
		},
		checkOneMembership(1, "decommissioned"),
	)

	u.run(ctx, t)
}
