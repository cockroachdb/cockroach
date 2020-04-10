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
	"fmt"
)

func registerSchemaChangeMixedVersions(r *testRegistry) {
	r.Add(testSpec{
		Name:    "schemachange/mixed-versions",
		Owner:   OwnerSQLSchema,
		Cluster: makeClusterSpec(4),
		Run: func(ctx context.Context, t *test, c *cluster) {
			predV, err := PredecessorVersion(r.buildVersion)
			if err != nil {
				t.Fatal(err)
			}
			runSchemaChangeMixedVersions(ctx, t, c, 100, predV)
		},
	})
}

func runSchemaChangeMixedVersions(
	ctx context.Context, t *test, c *cluster, maxOps int, predecessorVersion string,
) {
	var numFeatureRuns int
	testFeaturesStep := func(ctx context.Context, t *test, u *versionUpgradeTest) {
		numFeatureRuns++
		t.l.Printf("Feature test run: %d", numFeatureRuns)
		loadNode := u.c.All().randNode()[0]
		// Note that this will run `workload init` every time instead of just once
		// per node which is unnecessary. Since this call is idempotent we prefer
		// to make it repeatedly to complicating the test setup.
		u.c.Run(ctx, u.c.Node(loadNode), "./workload init schemachange")
		runCmd := []string{
			"./workload run",
			fmt.Sprintf("schemachange --concurrency 2 --max-ops %d --verbose=1", maxOps),
			fmt.Sprintf("{pgurl:1-%d}", u.c.spec.NodeCount),
		}

		err := u.c.RunE(ctx, u.c.Node(loadNode), runCmd...)
		if err != nil {
			c.t.Fatal(err)
		}
	}
	const mainVersion = ""
	u := newVersionUpgradeTest(c,
		uploadAndStartFromCheckpointFixture(c.All(), predecessorVersion),
		waitForUpgradeStep(c.All()),

		// NB: at this point, cluster and binary version equal predecessorVersion,
		// and auto-upgrades are on.

		preventAutoUpgradeStep(1),
		testFeaturesStep,

		// Roll the nodes into the new version one by one, while repeatedly running
		// schema changes. We use an empty string for the version below, which means
		// use the main ./cockroach binary (i.e. the one being tested in this run).
		binaryUpgradeStep(c.Node(3), mainVersion),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(2), mainVersion),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(1), mainVersion),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(4), mainVersion),
		testFeaturesStep,

		// Roll back again, which ought to be fine because the cluster upgrade was
		// not finalized.
		binaryUpgradeStep(c.Node(2), predecessorVersion),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(4), predecessorVersion),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(3), predecessorVersion),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(1), predecessorVersion),
		testFeaturesStep,

		// Roll nodes forward and finalize upgrade.
		binaryUpgradeStep(c.Node(4), mainVersion),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(3), mainVersion),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(1), mainVersion),
		testFeaturesStep,
		binaryUpgradeStep(c.Node(2), mainVersion),
		testFeaturesStep,

		allowAutoUpgradeStep(1),
		waitForUpgradeStep(c.All()),
		testFeaturesStep,
	)

	u.run(ctx, t)
}
