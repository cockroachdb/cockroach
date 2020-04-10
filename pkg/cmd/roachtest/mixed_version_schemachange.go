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
	"strings"
)

const predecessorVersion = "19.2.5"

func runMixedVersionSchemaChange(ctx context.Context, t *test, c *cluster) {
	var numFeatureRuns int
	testFeaturesStep := func(ctx context.Context, t *test, u *versionUpgradeTest) {
		numFeatureRuns++
		t.l.Printf("Feature test run: %d", numFeatureRuns)
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
			t.l.Printf("--- FAILi:\n%s\n%v", strings.Join(runCmd, " "), err)
			c.t.Fatal(err)
		}
	}
	u := newVersionUpgradeTest(c,
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

		// Roll nodes forward.
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
