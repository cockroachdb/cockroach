// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"golang.org/x/exp/maps"
)

func registerMultiRegionMixedVersion(r registry.Registry) {
	regionToZones := map[string][]string{
		"us-east1":        {"us-east1-b"},
		"us-west1":        {"us-west1-b"},
		"europe-west2":    {"europe-west2-b"},
		"europe-central2": {"europe-central2-b"},
	}

	regions := maps.Keys(regionToZones)
	var zones []string
	for _, zs := range maps.Values(regionToZones) {
		zones = append(zones, zs...)
	}

	const (
		nodesPerRegion = 20
		// These values are somewhat arbitrary: currently, they are
		// sufficient to keep the cluster relatively busy (CPU utilization
		// varying from 10-60%). In the future, these values might be
		// revisited as we add new workloads.
		backgroundWarehousesPerRegion   = 150
		mixedVersionWarehousesPerRegion = 100
	)

	r.Add(registry.TestSpec{
		Name: "multi-region/mixed-version",
		// This test can run very long as it performs multiple upgrades
		// (which can also involve temporary downgrades). Note that actual
		// cluster finalization is capped at 1 hour.
		Timeout: 36 * time.Hour,
		Owner:   registry.OwnerTestEng,
		Cluster: r.MakeClusterSpec(
			len(regions)*nodesPerRegion+1, // add one workload node
			spec.WorkloadNode(),
			spec.Geo(),
			spec.GCEZones(strings.Join(zones, ",")),
		),
		EncryptionSupport: registry.EncryptionMetamorphic,
		CompatibleClouds:  registry.OnlyGCE,
		Suites:            registry.Suites(registry.MixedVersion, registry.Weekly),
		Randomized:        true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			partitionConfig := fmt.Sprintf(
				"--regions=%s --partitions=%d",
				strings.Join(regions, ","), len(regions),
			)

			roachNodes := c.Range(1, c.Spec().NodeCount-1)
			mvt := mixedversion.NewTest(
				ctx, t, t.L(), c, roachNodes,
				// Our current fixtures only work for 4-node clusters.
				mixedversion.NeverUseFixtures,
				// Allow migrations to run for a longer period of time due to
				// added latency and cluster size.
				mixedversion.UpgradeTimeout(1*time.Hour),
				// There are known issues upgrading from older patch releases
				// in MR clusters (e.g., #113908), so use the latest patch
				// releases to avoid flakes.
				mixedversion.AlwaysUseLatestPredecessors,
			)

			// Note that we don't specify a `Duration` for this workload,
			// and that's intentional: this will cause the workload to run
			// throughout the entire test, until the test's context is
			// canceled.
			backgroundTPCCOpts := tpccOptions{
				DB:                     "tpcc_background",
				Warehouses:             len(regions) * backgroundWarehousesPerRegion,
				ExtraSetupArgs:         partitionConfig,
				ExtraRunArgs:           "--tolerate-errors " + partitionConfig,
				ExpectedDeaths:         10000, // we don't want the internal monitor to fail
				Start:                  func(_ context.Context, t test.Test, c cluster.Cluster) {},
				SetupType:              usingInit,
				SkipPostRunCheck:       true,
				DisablePrometheus:      true,
				DisableIsolationLevels: true,
			}

			mixedVersionTPCCOpts := tpccOptions{
				DB:                     "tpcc_mixed_version",
				Warehouses:             len(regions) * mixedVersionWarehousesPerRegion,
				ExtraSetupArgs:         partitionConfig,
				ExtraRunArgs:           partitionConfig,
				Start:                  func(_ context.Context, t test.Test, c cluster.Cluster) {},
				SetupType:              usingInit,
				Duration:               10 * time.Minute,
				ExpensiveChecks:        true,
				DisablePrometheus:      true,
				DisableIsolationLevels: true,
			}

			backgroundTPCCSetupDone := make(chan struct{})

			mvt.OnStartup(
				"setup tpcc",
				func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
					if err := enableTenantSplitScatter(l, rng, h); err != nil {
						return err
					}
					if err := enableTenantMultiRegion(l, rng, h); err != nil {
						return err
					}

					setupTPCC(ctx, t, l, c, backgroundTPCCOpts)
					// Update the `SetupType` so that the corresponding
					// `runTPCC` calls don't attempt to import data again.
					backgroundTPCCOpts.SetupType = usingExistingData
					close(backgroundTPCCSetupDone)

					setupTPCC(ctx, t, l, c, mixedVersionTPCCOpts)
					mixedVersionTPCCOpts.SetupType = usingExistingData

					return nil
				},
			)

			mvt.BackgroundFunc(
				"run TPCC background workload",
				func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
					l.Printf("waiting for setup to finish")
					<-backgroundTPCCSetupDone

					runTPCC(ctx, t, l, c, backgroundTPCCOpts)
					return nil
				},
			)

			mvt.InMixedVersion(
				"run tpcc",
				func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
					runTPCC(ctx, t, l, c, mixedVersionTPCCOpts)
					return nil
				},
			)

			mvt.AfterUpgradeFinalized(
				"run tpcc",
				func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
					runTPCC(ctx, t, l, c, mixedVersionTPCCOpts)
					return nil
				},
			)

			mvt.Run()
		},
	})
}
