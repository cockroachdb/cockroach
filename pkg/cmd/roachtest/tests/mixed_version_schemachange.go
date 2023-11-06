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
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

func registerSchemaChangeMixedVersions(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:  "schemachange/mixed-versions",
		Owner: registry.OwnerSQLFoundations,
		// This tests the work done for 20.1 that made schema changes jobs and in
		// addition prevented making any new schema changes on a mixed cluster in
		// order to prevent bugs during upgrades.
		Cluster:          r.MakeClusterSpec(4),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		NativeLibs:       registry.LibGEOS,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			maxOps := 100
			concurrency := 5
			if c.IsLocal() {
				maxOps = 10
				concurrency = 2
			}
			runSchemaChangeMixedVersions(ctx, t, c, maxOps, concurrency)
		},
	})
}

// runSchemaChangeMixedVersions runs through randomized schema change processes in a mixed-version state.
func runSchemaChangeMixedVersions(
	ctx context.Context, t test.Test, c cluster.Cluster, maxOps int, concurrency int,
) {
	numFeatureRuns := 0
	mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.All(), mixedversion.AlwaysUseLatestPredecessors)

	if err := c.PutE(ctx, t.L(), t.DeprecatedWorkload(), "./workload", c.All()); err != nil {
		t.Fatal(err)
	}

	// Run the schemachange workload on a random node, along with validating the schema changes for the cluster on a random node.
	schemaChangeAndValidationStep := func(
		ctx context.Context, l *logger.Logger, rand *rand.Rand, h *mixedversion.Helper,
	) error {
		numFeatureRuns += 1
		l.Printf("Workload step run: %d", numFeatureRuns)
		runCmd := roachtestutil.NewCommand("./workload run schemachange").
			Flag("verbose", 1).
			Flag("max-ops", maxOps).
			Flag("concurrency", concurrency).
			Arg("{pgurl:1-%d}", c.Spec().NodeCount).
			String()
		workloadNodes := c.All()
		if err := c.RunE(ctx, option.NodeListOption{h.RandomNode(rand, workloadNodes)}, runCmd); err != nil {
			return err
		}

		certs := "certs"
		if c.IsLocal() {
			// TODO(before merge): is there a c.localCertsDir equivalent to use here?
			fmt.Println("ignore lint err for now")
		}
		// Now we validate that nothing is broken after the random schema changes have been run.
		runCmd = roachtestutil.NewCommand("%s debug doctor examine cluster", test.DefaultCockroachPath).
			Flag("certs-dir", certs).
			String()
		return c.RunE(ctx,
			option.NodeListOption{h.RandomNode(rand, workloadNodes)},
			runCmd)
	}

	// Stage all nodes on our test cluster with the schemachange workload as the load node to run the workload is chosen randomly.
	mvt.OnStartup("set up schemachange workload", func(ctx context.Context, l *logger.Logger, r *rand.Rand, helper *mixedversion.Helper) error {
		//if err := c.PutE(ctx, t.L(), t.Cockroach(), "./cockroach-doctor", c.All()); err != nil {
		//	return err
		//}
		return c.RunE(ctx, c.All(), fmt.Sprintf("./workload init schemachange {pgurl%s}", c.All()))
	})

	mvt.InMixedVersion("run schemachange workload and validation in mixed version", schemaChangeAndValidationStep)

	mvt.AfterUpgradeFinalized("run schemachange workload and validation after upgrade has finalized", schemaChangeAndValidationStep)

	mvt.Run()
}
