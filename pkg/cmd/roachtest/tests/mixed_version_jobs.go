// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

func registerJobsMixedVersions(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:  "jobs/mixed-versions",
		Owner: registry.OwnerDisasterRecovery,
		Cluster: r.MakeClusterSpec(
			3, /* nodeCount */
			spec.CPU(4),
			spec.Geo(),
			spec.GCEZones("us-east1-b,us-west1-b,europe-west2-b"),
		),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Randomized:       true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runJobsMixedVersions(ctx, t, c)
		},
	})
}

func runJobsMixedVersions(ctx context.Context, t test.Test, c cluster.Cluster) {
	mvt := mixedversion.NewTest(
		ctx, t, t.L(), c, c.All(),
		mixedversion.NumUpgrades(2),
		mixedversion.UpgradeTimeout(time.Minute*30),
		mixedversion.MinimumSupportedVersion("v24.3.0"),
		mixedversion.AlwaysUseLatestPredecessors,
		mixedversion.NeverUseFixtures,
	)
	mvt.OnStartup("create a few jobs", func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		// The history retention job is a good candidate for this test because it
		// can fail easily because no one bumps the pts.
		return h.Exec(r, "select count(crdb_internal.protect_mvcc_history(0.1, '20s', 'test')) from generate_series(1, 100);")
	})
	mvt.BackgroundFunc("keep making jobs", func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				if err := h.Exec(r, "select crdb_internal.protect_mvcc_history(0.1, '20s', 'test');"); err != nil {
					l.Printf("error running operation: %v", err)
				}
			}
		}
	})
	mvt.Run()
}
