// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

func registerSchemaChangeMixedVersions(r registry.Registry) {
	r.Add(registry.TestSpec{
		// schemachange/mixed-versions tests random schema changes (via the schemachange workload)
		// in a mixed version state, validating that the cluster is still healthy (via debug doctor examine).
		Name:                       "schemachange/mixed-versions",
		Owner:                      registry.OwnerSQLFoundations,
		Cluster:                    r.MakeClusterSpec(4, spec.WorkloadNode()),
		CompatibleClouds:           registry.AllExceptAWS,
		Suites:                     registry.Suites(registry.MixedVersion, registry.Nightly),
		Randomized:                 true,
		NativeLibs:                 registry.LibGEOS,
		RequiresDeprecatedWorkload: true, // uses schemachange
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			maxOps := 1000
			concurrency := 5
			if c.IsLocal() {
				maxOps = 10
				concurrency = 2
			}
			runSchemaChangeMixedVersions(ctx, t, c, maxOps, concurrency)
		},
	})
}

func runSchemaChangeMixedVersions(
	ctx context.Context, t test.Test, c cluster.Cluster, maxOps int, concurrency int,
) {
	numFeatureRuns := 0
	mvt := mixedversion.NewTest(
		ctx, t, t.L(), c, c.All(),
		// Disable version skipping and limit the test to only one upgrade as the workload is only
		// compatible with the branch it was built from and the major version before that.
		mixedversion.NumUpgrades(1),
		mixedversion.DisableSkipVersionUpgrades,
		// Always use latest predecessors, since mixed-version bug fixes only
		// appear in the latest patch of the predecessor version.
		// See: https://github.com/cockroachdb/cockroach/issues/121411.
		mixedversion.AlwaysUseLatestPredecessors,
	)

	// Run the schemachange workload on a random node, along with validating the schema changes for the cluster on a random node.
	schemaChangeAndValidationStep := func(
		ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
	) error {
		numFeatureRuns += 1
		l.Printf("Workload step run: %d", numFeatureRuns)
		workloadSeed := r.Int63()
		runCmd := roachtestutil.NewCommand("COCKROACH_RANDOM_SEED=%d ./workload run schemachange", workloadSeed).
			Flag("verbose", 1).
			Flag("max-ops", maxOps).
			Flag("concurrency", concurrency).
			Arg("{pgurl%s}", c.All()).
			String()
		if err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), runCmd); err != nil {
			return err
		}

		randomNode := c.All().SeededRandNode(r)[0]
		doctorURL := fmt.Sprintf("{pgurl:%d}", randomNode)
		// Now we validate that nothing is broken after the random schema changes have been run.
		runCmd = roachtestutil.NewCommand("%s debug doctor examine cluster", test.DefaultCockroachPath).
			Flag("url", doctorURL).
			String()
		return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), runCmd)
	}

	// Stage our workload node with the schemachange workload.
	mvt.OnStartup("set up schemachange workload", func(ctx context.Context, l *logger.Logger, r *rand.Rand, helper *mixedversion.Helper) error {
		return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf("./workload init schemachange {pgurl%s}", c.WorkloadNode()))
	})

	mvt.InMixedVersion("run schemachange workload and validation in mixed version", schemaChangeAndValidationStep)

	mvt.AfterUpgradeFinalized("run schemachange workload and validation after upgrade has finalized", schemaChangeAndValidationStep)

	mvt.Run()
}
