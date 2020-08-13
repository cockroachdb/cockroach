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
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/util/version"
)

// runMixedVersionSplits runs a workload against a mixed-version cluster that
// would end up generating a bunch of splits.
func runMixedVersionSplits(ctx context.Context, t *test, c *cluster, buildVersion version.Version) {
	predecessorVersion, err := PredecessorVersion(buildVersion)
	if err != nil {
		t.Fatal(err)
	}

	var nodeIDs []int
	for i := 1; i <= c.spec.NodeCount; i++ {
		nodeIDs = append(nodeIDs, i)
	}
	getRandNode := func() int {
		return nodeIDs[rand.Intn(len(nodeIDs))]
	}

	pinnedUpgrade := getRandNode()
	t.l.Printf("pinned n%d for upgrade", pinnedUpgrade)

	pinnedWorkload := getRandNode()
	t.l.Printf("pinned n%d for workload", pinnedWorkload)

	// An empty string means that the cockroach binary specified by flag
	// `cockroach` will be used.
	const mainVersion = ""
	allNodes := c.All()
	u := newVersionUpgradeTest(c,
		// Upload all the binaries needed in this test.
		uploadVersionStep(allNodes, predecessorVersion),
		uploadVersionStep(allNodes, mainVersion),
		uploadWorkloadStep(pinnedWorkload),

		// Start the cluster at the predecessor version.
		startVersionStep(allNodes, predecessorVersion),
		waitForUpgradeStep(allNodes),

		// Initialize schema for the workload, and limit range size to induce
		// splits more readily.
		initWorkloadStep(pinnedWorkload, getRandNode()),
		limitRangeSizeStep(getRandNode(), 1<<20 /* 1 MiB */),

		// Upgrade one of the nodes and run the workload.
		binaryUpgradeStep(c.Node(pinnedUpgrade), mainVersion),
		runSplitsWorkloadStep(pinnedWorkload, getRandNode()),

		binaryUpgradeStep(allNodes, mainVersion),
		waitForUpgradeStep(allNodes),
	)

	u.run(ctx, t)
}

func uploadWorkloadStep(from int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		u.c.Put(ctx, workload, "./workload", u.c.Node(from))
	}
}

func limitRangeSizeStep(from int, size int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, from)
		_, err := db.ExecContext(ctx, `ALTER RANGE default CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = $1`, size)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func initWorkloadStep(from, to int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		//  Add a secondary index to avoid UPSERTs performing blind writes.
		u.c.Run(ctx, u.c.Node(from), "./workload init kv --secondary-index", fmt.Sprintf("{pgurl:%d}", to))
	}
}

func runSplitsWorkloadStep(from, to int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		// We tune these numbers so that there's enough just enough data being
		// generated to cause splits.
		const cycleLength = 1024
		const concurrency = 64
		const avgConcPerKey = 1
		const batchSize = avgConcPerKey * (cycleLength / concurrency)

		runCmd := []string{
			"./workload run kv",
			fmt.Sprintf("--concurrency %d", concurrency),
			fmt.Sprintf("--cycle-length %d", cycleLength),
			fmt.Sprintf("--batch %d", batchSize),
			"--duration 5m",
			"--duration 0m",
			"--read-percent 0",
			"--tolerate-errors",
			fmt.Sprintf("{pgurl:%d}", to),
		}
		u.c.Run(ctx, u.c.Node(from), runCmd...)
	}
}

func registerSplitsMixedVersions(r *testRegistry) {
	r.Add(testSpec{
		Name:       "splits/mixed-versions",
		Owner:      OwnerKV,
		MinVersion: "v20.2.0",
		Cluster:    makeClusterSpec(3),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runMixedVersionSplits(ctx, t, c, r.buildVersion)
		},
	})
}
