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

func registerYCSB(r *testRegistry) {
	workloads := []string{"A", "B", "C", "D", "E", "F"}
	cpusConfigs := []int{8, 32}

	// concurrencyConfigs contains near-optimal concurrency levels for each
	// (workload, cpu count) combination. All of these figures were tuned on GCP
	// n1-standard instance types. We should consider implementing a search for
	// the optimal concurrency level in the roachtest itself (see kvbench).
	concurrencyConfigs := map[string] /* workload */ map[int] /* cpus */ int{
		"A": {8: 96, 32: 144},
		"B": {8: 144, 32: 192},
		"C": {8: 144, 32: 192},
		"D": {8: 96, 32: 144},
		"E": {8: 96, 32: 144},
		"F": {8: 96, 32: 144},
	}

	runYCSB := func(ctx context.Context, t *test, c *cluster, wl string, cpus int) {
		nodes := c.spec.NodeCount - 1

		conc, ok := concurrencyConfigs[wl][cpus]
		if !ok {
			t.Fatalf("missing concurrency for (workload, cpus) = (%s, %d)", wl, cpus)
		}

		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Node(nodes+1))
		c.Start(ctx, t, c.Range(1, nodes))
		waitForFullReplication(t, c.Conn(ctx, 1))

		t.Status("running workload")
		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			sfu := fmt.Sprintf(" --select-for-update=%t", t.IsBuildVersion("v19.2.0"))
			ramp := " --ramp=" + ifLocal("0s", "1m")
			duration := " --duration=" + ifLocal("10s", "10m")
			cmd := fmt.Sprintf(
				"./workload run ycsb --init --insert-count=1000000 --workload=%s --concurrency=%d"+
					" --splits=%d --histograms="+perfArtifactsDir+"/stats.json"+sfu+ramp+duration+
					" {pgurl:1-%d}",
				wl, conc, nodes, nodes)
			c.Run(ctx, c.Node(nodes+1), cmd)
			return nil
		})
		m.Wait()
	}

	for _, wl := range workloads {
		for _, cpus := range cpusConfigs {
			var name string
			if cpus == 8 { // support legacy test name which didn't include cpu
				name = fmt.Sprintf("ycsb/%s/nodes=3", wl)
			} else {
				name = fmt.Sprintf("ycsb/%s/nodes=3/cpu=%d", wl, cpus)
			}
			wl, cpus := wl, cpus
			r.Add(testSpec{
				Name:    name,
				Owner:   OwnerKV,
				Cluster: makeClusterSpec(4, cpu(cpus)),
				Run: func(ctx context.Context, t *test, c *cluster) {
					runYCSB(ctx, t, c, wl, cpus)
				},
			})
		}
	}
}
