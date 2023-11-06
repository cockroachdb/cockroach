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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/version"
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
			runSchemaChangeMixedVersions(ctx, t, c, maxOps, concurrency, t.BuildVersion())
		},
	})
}

type schemaChangeMixedVersionTester struct {
	c              cluster.Cluster
	t              test.Test
	maxOps         int
	concurrency    int
	numFeatureRuns int
}

func newSchemaChangeMixedVersionTester(
	c cluster.Cluster, t test.Test, maxOps int, concurrency int, numFeatureRuns int,
) *schemaChangeMixedVersionTester {

	return &schemaChangeMixedVersionTester{
		c:              c,
		t:              t,
		maxOps:         maxOps,
		concurrency:    concurrency,
		numFeatureRuns: numFeatureRuns,
	}
}

// uploadAndInitSchemaChangeWorkload stages all nodes on our test cluster with the schemachange workload
// as the load node to run the workload is chosen randomly.
func (scmvt *schemaChangeMixedVersionTester) uploadAndInitSchemaChangeWorkload(
	ctx context.Context, l *logger.Logger, rand *rand.Rand, h *mixedversion.Helper,
) error {
	if err := scmvt.c.PutE(ctx, l, scmvt.t.DeprecatedWorkload(), "./workload", scmvt.c.All()); err != nil {
		return err
	}
	if err := scmvt.c.PutE(ctx, l, scmvt.t.Cockroach(), "./cockroach-doctor", scmvt.c.All()); err != nil {
		return err
	}
	return scmvt.c.RunE(ctx, scmvt.c.All(), fmt.Sprintf("./workload init schemachange {pgurl%s}", scmvt.c.All()))
}

// runSchemaChangeWorkloadStepAndValidate runs the schemachange workload on a random node, along with validating
// the schema changes for the cluster on a random node.
func (scmvt *schemaChangeMixedVersionTester) runSchemaChangeWorkloadStepAndValidate(
	ctx context.Context, l *logger.Logger, rand *rand.Rand, h *mixedversion.Helper,
) error {
	scmvt.numFeatureRuns += 1
	l.Printf("Workload step run: %d", scmvt.numFeatureRuns)
	runCmd := []string{
		"./workload run schemachange --verbose=1",
		fmt.Sprintf("--max-ops %d", scmvt.maxOps),
		fmt.Sprintf("--concurrency %d", scmvt.concurrency),
		fmt.Sprintf("{pgurl:1-%d}", scmvt.c.Spec().NodeCount),
	}
	workloadNodes := scmvt.c.All()
	if err := scmvt.c.RunE(ctx, option.NodeListOption{h.RandomNode(rand, workloadNodes)}, runCmd...); err != nil {
		return err
	}

	certs := "certs"
	//nolint:empty branch (SA9003)
	if scmvt.c.IsLocal() {
		// TODO(before merge): is there a c.localCertsDir equivalent to use here?
	}
	// Now we validate that nothing is broken after the random schema changes have been run.
	runCmd = []string{
		"./cockroach-doctor",
		fmt.Sprintf("debug doctor examine cluster --certs-dir=%s", certs),
	}
	return scmvt.c.RunE(ctx,
		option.NodeListOption{h.RandomNode(rand, workloadNodes)},
		runCmd...)
}

// runSchemaChangeMixedVersions runs through randomized schema change processes in a mixed-version state.
func runSchemaChangeMixedVersions(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	maxOps int,
	concurrency int,
	buildVersion *version.Version,
) {
	scmvt := newSchemaChangeMixedVersionTester(c, t, maxOps, concurrency, 0 /* numFeatureRuns */)
	mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.All(), mixedversion.AlwaysUseLatestPredecessors, mixedversion.AlwaysUseFixtures)

	schemaChangeAndValidationStep := scmvt.runSchemaChangeWorkloadStepAndValidate
	if buildVersion.Major() < 20 {
		// Schema change workload is meant to run only on versions 19.2 or higher.
		// If the main version is below 20.1 then the predecessor version will be
		// below 19.2.
		schemaChangeAndValidationStep = nil
	}

	// TODO(before merge): is there a way to set rollbackIntermediateUpgradesProbability to always be true/is that necessary?
	mvt.OnStartup("set up schemachange workload", scmvt.uploadAndInitSchemaChangeWorkload)

	mvt.InMixedVersion("run schemachange workload and validation in mixed version", schemaChangeAndValidationStep)

	mvt.AfterUpgradeFinalized("run schemachange workload and validation after upgrade has finalized", schemaChangeAndValidationStep)

	mvt.Run()
}
