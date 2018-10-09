// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	"fmt"
)

func registerYCSB(r *registry) {
	runYCSB := func(ctx context.Context, t *test, c *cluster, wl string) {
		if !c.isLocal() {
			c.RemountNoBarrier(ctx)
		}

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
				"./workload run ycsb --init --initial-rows=1000000 --splits=100"+
					" --workload=%s --concurrency=64 --histograms=logs/stats.json"+
					ramp+duration+" {pgurl:1-%d}",
				wl, nodes)
			c.Run(ctx, c.Node(nodes+1), cmd)
			return nil
		})
		m.Wait()
	}

	for _, wl := range []string{"A", "B", "C", "D", "E", "F"} {
		if wl == "D" || wl == "E" {
			// These workloads are currently unsupported by workload.
			// See TODOs in workload/ycsb/ycsb.go.
			continue
		}

		wl := wl
		r.Add(testSpec{
			Name:  fmt.Sprintf("ycsb/%s/nodes=3", wl),
			Nodes: nodes(4, cpu(8)),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runYCSB(ctx, t, c, wl)
			},
		})
	}
}
