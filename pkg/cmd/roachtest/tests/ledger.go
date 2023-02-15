// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
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
		Name:    fmt.Sprintf("ledger/nodes=%d/multi-az", nodes),
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(nodes+1, spec.CPU(16), spec.Geo(), spec.Zones(azs)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			roachNodes := c.Range(1, nodes)
			gatewayNodes := c.Range(1, nodes/3)
			loadNode := c.Node(nodes + 1)

			c.Put(ctx, t.Cockroach(), "./cockroach", roachNodes)
			c.Put(ctx, t.DeprecatedWorkload(), "./workload", loadNode)

			// Don't start a scheduled backup on this perf sensitive roachtest that reports to roachperf.
			c.Start(ctx, t.L(), option.DefaultStartOptsNoBackups(), install.MakeClusterSettings(), roachNodes)

			t.Status("running workload")
			m := c.NewMonitor(ctx, roachNodes)
			m.Go(func(ctx context.Context) error {
				concurrency := ifLocal(c, "", " --concurrency="+fmt.Sprint(nodes*32))
				duration := " --duration=" + ifLocal(c, "10s", "10m")

				// See https://github.com/cockroachdb/cockroach/issues/94062 for the --data-loader.
				cmd := fmt.Sprintf("./workload run ledger --init --data-loader=INSERT --histograms="+t.PerfArtifactsDir()+"/stats.json"+
					concurrency+duration+" {pgurl%s}", gatewayNodes)
				c.Run(ctx, loadNode, cmd)
				return nil
			})
			m.Wait()
		},
	})
}
