// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

func registerSchemaChangeMixedVersions(r registry.Registry) {
	r.Add(registry.TestSpec{
		// schemachange/mixed-versions tests random schema changes (via the schemachange workload)
		// in a mixed version state, validating that the cluster is still healthy (via debug doctor examine).
		Name:    "schemachange/mixed-versions/will",
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(4, spec.WorkloadNode()),
		// Disabled on IBM because s390x is only built on master and mixed-version
		// is impossible to test as of 05/2025.
		CompatibleClouds:           registry.AllClouds.NoAWS().NoIBM(),
		Suites:                     registry.Suites(registry.MixedVersion, registry.Nightly),
		Monitor:                    true,
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
	mvt := mixedversion.NewTest(
		ctx, t, t.L(), c, c.All(),
		// Disable version skipping and limit the test to only one upgrade as the workload is only
		// compatible with the branch it was built from and the major version before that.
		//mixedversion.NumUpgrades(1),
		//mixedversion.DisableSkipVersionUpgrades,
		// Always use latest predecessors, since mixed-version bug fixes only
		// appear in the latest patch of the predecessor version.
		// See: https://github.com/cockroachdb/cockroach/issues/121411.
		mixedversion.AlwaysUseLatestPredecessors,
		mixedversion.WithWorkloadNodesWithDedicatedWorkloadBinary(c.WorkloadNode()),
	)
	tester := newSchemaChangeMixedVersionTester(t, c, maxOps, concurrency)

	mvt.OnStartup(
		"set up schemachange workload",
		tester.initWorkload,
	)

	mvt.InMixedVersion("run schemachange workload and validation in mixed version",
		tester.schemaChangeAndValidationStep)

	mvt.AfterUpgradeFinalized("run schemachange workload and validation after upgrade has finalized",
		tester.schemaChangeAndValidationStep)

	mvt.Run()
}

// schemaChangeMixedVersionTester bundles state and configuration to pass to
// test hooks
type schemaChangeMixedVersionTester struct {
	t              test.Test
	c              cluster.Cluster
	maxOps         int
	concurrency    int
	numFeatureRuns int
}

func newSchemaChangeMixedVersionTester(
	t test.Test, c cluster.Cluster, maxOps int, concurrency int,
) schemaChangeMixedVersionTester {
	return schemaChangeMixedVersionTester{
		t:           t,
		c:           c,
		maxOps:      maxOps,
		concurrency: concurrency,
	}
}

func (t schemaChangeMixedVersionTester) initWorkload(
	ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
) error {
	// Just a util to get the binary path, this shouldn't upload a binary because we front loaded that work
	binaryPath, _, err := clusterupgrade.UploadWorkload(
		ctx, t.t, l, t.c, t.c.WorkloadNode(), h.System.FromVersion)
	if err != nil {
		return err
	}
	initCmd := roachtestutil.NewCommand("%s init schemachange", binaryPath).
		Arg("{pgurl%s}", t.c.WorkloadNode()).String()
	return t.c.RunE(ctx, option.WithNodes(t.c.WorkloadNode()), initCmd)
}

func (t schemaChangeMixedVersionTester) schemaChangeAndValidationStep(
	ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
) error {
	t.numFeatureRuns += 1
	l.Printf("Workload step run: %d", t.numFeatureRuns)
	workloadSeed := r.Int63()
	// Just a util to get the binary path, this shouldn't upload a binary because we front loaded that work
	binaryPath, _, err := clusterupgrade.UploadWorkload(
		ctx, t.t, l, t.c, t.c.WorkloadNode(), h.System.FromVersion)
	if err != nil {
		return err
	}
	runCmd := roachtestutil.NewCommand("%s run schemachange", binaryPath).
		Flag("verbose", 1).
		Flag("max-ops", t.maxOps).
		Flag("concurrency", t.concurrency).
		Arg("{pgurl%s}", t.c.All()).
		EnvVar("COCKROACH_RANDOM_SEED", strconv.FormatInt(workloadSeed, 10)).
		String()

	if err := t.c.RunE(ctx, option.WithNodes(t.c.WorkloadNode()), runCmd); err != nil {
		return err
	}

	randomNode := t.c.All().SeededRandNode(r)[0]
	doctorURL := fmt.Sprintf("{pgurl:%d}", randomNode)
	// Now we validate that nothing is broken after the random schema changes have been run.
	runCmd = roachtestutil.NewCommand("%s debug doctor examine cluster", test.DefaultCockroachPath).
		Flag("url", doctorURL).
		String()
	return t.c.RunE(ctx, option.WithNodes(t.c.WorkloadNode()), runCmd)

}
