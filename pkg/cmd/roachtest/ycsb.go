// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"fmt"
)

func registerYCSB(r *registry) {
	runYCSB := func(ctx context.Context, t *test, c *cluster, wl string, cpus int) {
		nodes := c.nodes - 1

		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Node(nodes+1))
		c.Start(ctx, t, c.Range(1, nodes))

		t.Status("running workload")
		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			ramp := " --ramp=" + ifLocal("0s", "1m")
			duration := " --duration=" + ifLocal("10s", "10m")
			cmd := fmt.Sprintf(
				"./workload run ycsb --init --record-count=1000000 --splits=100"+
					" --workload=%s --concurrency=64 --histograms=logs/stats.json"+
					ramp+duration+" {pgurl:1-%d}",
				wl, nodes)
			c.Run(ctx, c.Node(nodes+1), cmd)
			return nil
		})
		m.Wait()
	}

	for _, wl := range []string{"A", "B", "C", "D", "E", "F"} {
		for _, cpus := range []int{8, 32} {
			var name string
			if cpus == 8 { // support legacy test name which didn't include cpu
				name = fmt.Sprintf("ycsb/%s/nodes=3", wl)
			} else {
				name = fmt.Sprintf("ycsb/%s/nodes=3/cpu=%d", wl, cpus)
			}
			wl, cpus := wl, cpus
			r.Add(testSpec{
				Name:    name,
				Cluster: makeClusterSpec(4, cpu(cpus)),
				Run: func(ctx context.Context, t *test, c *cluster) {
					runYCSB(ctx, t, c, wl, cpus)
				},
			})
		}
	}
}
