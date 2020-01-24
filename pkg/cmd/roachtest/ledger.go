// Copyright 2018 The Cockroach Authors.
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
)

func registerLedger(r *testRegistry) {
	const nodes = 6
	const azs = "us-central1-a,us-central1-b,us-central1-c"
	r.Add(testSpec{
		Name:    fmt.Sprintf("ledger/nodes=%d/multi-az", nodes),
		Owner:   OwnerKV,
		Cluster: makeClusterSpec(nodes+1, cpu(16), geo(), zones(azs)),
		Run: func(ctx context.Context, t *test, c *cluster) {
			roachNodes := c.Range(1, nodes)
			gatewayNodes := c.Range(1, nodes/3)
			loadNode := c.Node(nodes + 1)

			c.Put(ctx, cockroach, "./cockroach", roachNodes)
			c.Put(ctx, workload, "./workload", loadNode)
			c.Start(ctx, t, roachNodes)

			t.Status("running workload")
			m := newMonitor(ctx, c, roachNodes)
			m.Go(func(ctx context.Context) error {
				concurrency := ifLocal("", " --concurrency="+fmt.Sprint(nodes*32))
				duration := " --duration=" + ifLocal("10s", "10m")

				cmd := fmt.Sprintf("./workload run ledger --init --histograms="+perfArtifactsDir+"/stats.json"+
					concurrency+duration+" {pgurl%s}", gatewayNodes)
				c.Run(ctx, loadNode, cmd)
				return nil
			})
			m.Wait()
		},
	})
}
