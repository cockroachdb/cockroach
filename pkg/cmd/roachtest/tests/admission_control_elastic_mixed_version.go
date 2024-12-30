// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/stretchr/testify/require"
)

// This test exercises Replication Admission Control v1 and v2 with respect to
// regular and elastic workloads in a mixed-version setup.
//
// It runs 2 workloads: kv consisting of "regular" priority writes and kv
// consisting of "background" (elastic) priority writes. The goal is to show
// that even with a demanding "background" workload that is able to push the
// used bandwidth much higher than the provisioned one, AC paces the traffic at
// the set bandwidth limit, and favours regular writes. This behaviour does not
// regress after the cluster is upgraded to v24.3.
func registerElasticWorkloadMixedVersion(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "admission-control/elastic-workload/mixed-version",
		Owner:            registry.OwnerKV,
		Timeout:          1 * time.Hour,
		Benchmark:        true,
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Cluster: r.MakeClusterSpec(4, spec.CPU(8),
			spec.WorkloadNode(), spec.ReuseNone()),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			require.Equal(t, 4, c.Spec().NodeCount)

			settings := install.MakeClusterSettings()
			mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.CRDBNodes(),
				mixedversion.NeverUseFixtures,
				mixedversion.ClusterSettingOption(
					install.ClusterSettingsOption(settings.ClusterSettings),
				),
				mixedversion.EnabledDeploymentModes(mixedversion.SystemOnlyDeployment),
				mixedversion.AlwaysUseLatestPredecessors,
				// Don't go back too far. We are mostly interested in upgrading to v24.3
				// where RACv2 was introduced.
				mixedversion.MaxUpgrades(2),
				mixedversion.MinimumSupportedVersion("v24.1.0"),
			)

			// Limit the disk throughput to 128 MiB/s, to more easily stress the
			// elastic traffic waiting.
			const diskBand = 128 << 20 // 128 MiB
			setDiskBandwidth := func() {
				t.Status(fmt.Sprintf("limiting disk bandwidth to %d bytes/s", diskBand))
				staller := roachtestutil.MakeCgroupDiskStaller(t, c,
					false /* readsToo */, false /* logsToo */)
				staller.Setup(ctx)
				staller.Slow(ctx, c.CRDBNodes(), diskBand)
			}

			// Init KV workload with a bunch of pre-split ranges and pre-inserted
			// rows. The block sizes are picked the same as for the "foreground"
			// workload below.
			initKV := func(ctx context.Context, version *clusterupgrade.Version) error {
				binary := uploadCockroach(ctx, t, c, c.WorkloadNode(), version)
				return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf(
					"%s workload init kv --drop --splits=1000 --insert-count=3000 "+
						"--min-block-bytes=512 --max-block-bytes=1024 {pgurl%s}",
					binary, c.Node(1)))
			}
			labels := map[string]string{
				"concurrency":  "500",
				"read-percent": "5",
			}
			// The workloads are tuned to keep the cluster busy at 30-40% CPU, and IO
			// overload metric approaching 20-30% which causes elastic traffic being
			// de-prioritized and wait.
			runForeground := func(ctx context.Context, duration time.Duration) error {
				cmd := roachtestutil.NewCommand("./cockroach workload run kv "+
					"%s --concurrency=500 "+
					"--max-rate=5000 --read-percent=5 "+
					"--min-block-bytes=512 --max-block-bytes=1024 "+
					"--txn-qos='regular' "+
					"--duration=%v {pgurl%s}", roachtestutil.GetWorkloadHistogramArgs(t, c, labels), duration, c.CRDBNodes())
				return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd.String())
			}

			labels["read-percent"] = "0"
			runBackground := func(ctx context.Context, duration time.Duration) error {
				cmd := roachtestutil.NewCommand("./cockroach workload run kv "+
					"%s --concurrency=500 "+
					"--max-rate=10000 --read-percent=0 "+
					"--min-block-bytes=2048 --max-block-bytes=4096 "+
					"--txn-qos='background' "+
					"--duration=%v {pgurl%s}", roachtestutil.GetWorkloadHistogramArgs(t, c, labels), duration, c.CRDBNodes())
				return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd.String())
			}
			runWorkloads := func(ctx2 context.Context) error {
				const duration = 5 * time.Minute
				m := c.NewMonitor(ctx, c.CRDBNodes())
				m.Go(func(ctx context.Context) error { return runForeground(ctx, duration) })
				m.Go(func(ctx context.Context) error { return runBackground(ctx, duration) })
				return m.WaitE()
			}

			mvt.OnStartup("initializing kv dataset",
				func(ctx context.Context, _ *logger.Logger, _ *rand.Rand, h *mixedversion.Helper) error {
					return initKV(ctx, h.System.FromVersion)
				})
			mvt.InMixedVersion("running kv workloads in mixed version",
				func(ctx context.Context, _ *logger.Logger, _ *rand.Rand, _ *mixedversion.Helper) error {
					setDiskBandwidth()
					return runWorkloads(ctx)
				})
			mvt.AfterUpgradeFinalized("running kv workloads after upgrade",
				func(ctx context.Context, _ *logger.Logger, _ *rand.Rand, _ *mixedversion.Helper) error {
					return runWorkloads(ctx)
				})

			mvt.Run()
			// TODO(pav-kv): also validate that the write throughput was kept under
			// control, and the foreground traffic was not starved.
			roachtestutil.ValidateTokensReturned(ctx, t, c, c.CRDBNodes(), time.Minute)
		},
	})
}
