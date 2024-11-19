// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

func registerLedger(r registry.Registry) {
	const nodes = 6
	// NB: us-central1-a has been causing issues, see:
	// https://github.com/cockroachdb/cockroach/issues/66184
	const azs = "us-central1-f,us-central1-b,us-central1-c"
	r.Add(registry.TestSpec{
		Name:                       fmt.Sprintf("ledger/nodes=%d/multi-az", nodes),
		Owner:                      registry.OwnerKV,
		Benchmark:                  true,
		Cluster:                    r.MakeClusterSpec(nodes+1, spec.CPU(16), spec.WorkloadNode(), spec.WorkloadNodeCPU(16), spec.Geo(), spec.GCEZones(azs)),
		CompatibleClouds:           registry.OnlyGCE,
		Suites:                     registry.Suites(registry.Nightly),
		RequiresDeprecatedWorkload: true, // uses ledger
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			gatewayNodes := c.Range(1, nodes/3)

			// Don't start a scheduled backup on this perf sensitive roachtest that reports to roachperf.
			c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), install.MakeClusterSettings(), c.CRDBNodes())

			t.Status("running workload")
			m := c.NewMonitor(ctx, c.CRDBNodes())
			m.Go(func(ctx context.Context) error {
				concurrency := roachtestutil.IfLocal(c, "", " --concurrency="+fmt.Sprint(nodes*32))
				duration := " --duration=" + roachtestutil.IfLocal(c, "10s", "10m")

				labels := map[string]string{
					"concurrency": fmt.Sprint(nodes * 32),
					"duration":    roachtestutil.IfLocal(c, "10000", "600000"),
				}

				// See https://github.com/cockroachdb/cockroach/issues/94062 for the --data-loader.
				cmd := fmt.Sprintf("./workload run ledger --init --data-loader=INSERT %s %s %s {pgurl%s}",
					roachtestutil.GetWorkloadHistogramArgs(t, c, labels), concurrency, duration, gatewayNodes)
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)
				return nil
			})
			m.Wait()
		},
	})
}
