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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/version"
)

func registerSchemaChangeMixedVersions(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:  "schemachange/mixed-versions",
		Owner: registry.OwnerSQLSchema,
		// This tests the work done for 20.1 that made schema changes jobs and in
		// addition prevented making any new schema changes on a mixed cluster in
		// order to prevent bugs during upgrades.
		Cluster: r.MakeClusterSpec(4),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			maxOps := 100
			concurrency := 5
			if c.IsLocal() {
				maxOps = 10
				concurrency = 2
			}
			runSchemaChangeMixedVersions(ctx, t, c, maxOps, concurrency, *t.BuildVersion())
		},
	})
}

func uploadAndInitSchemaChangeWorkload() versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		// Stage workload on all nodes as the load node to run workload is chosen
		// randomly.
		u.c.Put(ctx, t.DeprecatedWorkload(), "./workload", u.c.All())
		u.c.Run(ctx, u.c.All(), "./workload init schemachange")
	}
}

func runSchemaChangeWorkloadStep(loadNode, maxOps, concurrency int) versionStep {
	var numFeatureRuns int
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		numFeatureRuns++
		t.L().Printf("Workload step run: %d", numFeatureRuns)
		runCmd := []string{
			"./workload run schemachange --verbose=1",
			// The workload is still in development and occasionally discovers schema
			// change errors so for now we don't fail on them but only on panics, server
			// crashes, deadlocks, etc.
			// TODO(spaskob): remove when https://github.com/cockroachdb/cockroach/issues/47430
			// is closed.
			"--tolerate-errors=true",
			fmt.Sprintf("--max-ops %d", maxOps),
			fmt.Sprintf("--concurrency %d", concurrency),
			fmt.Sprintf("{pgurl:1-%d}", u.c.Spec().NodeCount),
		}
		u.c.Run(ctx, u.c.Node(loadNode), runCmd...)
	}
}

func runSchemaChangeMixedVersions(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	maxOps int,
	concurrency int,
	buildVersion version.Version,
) {
	predecessorVersion, err := PredecessorVersion(buildVersion)
	if err != nil {
		t.Fatal(err)
	}

	// An empty string will lead to the cockroach binary specified by flag
	// `cockroach` to be used.
	const mainVersion = ""
	schemaChangeStep := runSchemaChangeWorkloadStep(c.All().RandNode()[0], maxOps, concurrency)
	if buildVersion.Major() < 20 {
		// Schema change workload is meant to run only on versions 19.2 or higher.
		// If the main version is below 20.1 then then predecessor version will be
		// below 19.2.
		schemaChangeStep = nil
	}

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
