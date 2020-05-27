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
		Name:  "schemachange/mixed-versions",
		Owner: OwnerSQLSchema,
		// This tests the work done for 20.1 that made schema changes jobs and in
		// addition prevented making any new schema changes on a mixed cluster in
		// order to prevent bugs during upgrades.
		MinVersion: "v20.1.0",
		Cluster:    makeClusterSpec(4),
		Run: func(ctx context.Context, t *test, c *cluster) {
			predV, err := PredecessorVersion(r.buildVersion)
			if err != nil {
				t.Fatal(err)
			}
			maxOps := 100
			concurrency := 5
			if local {
				maxOps = 10
				concurrency = 2
			}
			runSchemaChangeMixedVersions(ctx, t, c, maxOps, concurrency, predV)
		},
	})
}

func uploadAndInitSchemaChangeWorkload() versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		// Stage workload on all nodes as the load node to run workload is chosen
		// randomly.
		u.c.Put(ctx, workload, "./workload", u.c.All())
		u.c.Run(ctx, u.c.All(), "./workload init schemachange")
	}
}

func runSchemaChangeWorkloadStep(loadNode, maxOps, concurrency int) versionStep {
	var numFeatureRuns int
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		numFeatureRuns++
		t.l.Printf("Workload step run: %d", numFeatureRuns)
		runCmd := []string{
			"./workload run",
			fmt.Sprintf("schemachange --concurrency %d --max-ops %d --verbose=1", maxOps, concurrency),
			fmt.Sprintf("{pgurl:1-%d}", u.c.spec.NodeCount),
		}
		u.c.Run(ctx, u.c.Node(loadNode), runCmd...)
	}
}

func runSchemaChangeMixedVersions(
	ctx context.Context, t *test, c *cluster, maxOps, concurrency int, predecessorVersion string,
) {
	// An empty string will lead to the cockroach binary specified by flag
	// `cockroach` to be used.
	const mainVersion = ""
	schemaChangeStep := runSchemaChangeWorkloadStep(c.All().randNode()[0], maxOps, concurrency)

	u := newVersionUpgradeTest(c,
		uploadAndStartFromCheckpointFixture(c.All(), predecessorVersion),
		uploadAndInitSchemaChangeWorkload(),
		waitForUpgradeStep(c.All()),

		// NB: at this point, cluster and binary version equal predecessorVersion,
		// and auto-upgrades are on.

		preventAutoUpgradeStep(1),
		schemaChangeStep,

		// Roll the nodes into the new version one by one, while repeatedly running
		// schema changes. We use an empty string for the version below, which means
		// use the main ./cockroach binary (i.e. the one being tested in this run).
		binaryUpgradeStep(c.Node(3), mainVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(2), mainVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(1), mainVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(4), mainVersion),
		schemaChangeStep,

		// Roll back again, which ought to be fine because the cluster upgrade was
		// not finalized.
		binaryUpgradeStep(c.Node(2), predecessorVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(4), predecessorVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(3), predecessorVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(1), predecessorVersion),
		schemaChangeStep,

		// Roll nodes forward and finalize upgrade.
		binaryUpgradeStep(c.Node(4), mainVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(3), mainVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(1), mainVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(2), mainVersion),
		schemaChangeStep,

		allowAutoUpgradeStep(1),
		waitForUpgradeStep(c.All()),
		schemaChangeStep,
	)

	u.run(ctx, t)
}
