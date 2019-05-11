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

func registerNIndexes(r *registry, secondaryIndexes int) {
	const nodes = 6
	const geoZones = "us-west1-b,us-east1-b,us-central1-a"
	r.Add(testSpec{
		Name:    fmt.Sprintf("indexes/%d/nodes=%d/multi-region", secondaryIndexes, nodes),
		Cluster: makeClusterSpec(nodes+1, cpu(16), geo(), zones(geoZones)),
		Run: func(ctx context.Context, t *test, c *cluster) {
			lastNodeInFirstAZ := nodes / 3
			var roachNodes, gatewayNodes, loadNode nodeListOption
			for i := 0; i < c.nodes; i++ {
				n := c.Node(i + 1)
				if i == lastNodeInFirstAZ {
					loadNode = n
				} else {
					roachNodes = roachNodes.merge(n)
					if i < lastNodeInFirstAZ {
						gatewayNodes = gatewayNodes.merge(n)
					}
				}
			}

			c.Put(ctx, cockroach, "./cockroach", roachNodes)
			c.Put(ctx, workload, "./workload", loadNode)
			c.Start(ctx, t, roachNodes)

			t.Status("running workload")
			m := newMonitor(ctx, c, roachNodes)
			m.Go(func(ctx context.Context) error {
				payload := " --payload=256"
				indexes := " --secondary-indexes=" + fmt.Sprint(secondaryIndexes)
				concurrency := ifLocal("", " --concurrency="+fmt.Sprint(nodes*32))
				duration := " --duration=" + ifLocal("10s", "30m")

				cmd := fmt.Sprintf("./workload run indexes --init --histograms=logs/stats.json"+
					payload+indexes+concurrency+duration+" {pgurl%s}", gatewayNodes)
				c.Run(ctx, loadNode, cmd)
				return nil
			})
			m.Wait()
		},
	})
}

func registerIndexes(r *registry) {
	registerNIndexes(r, 2)
}

func registerIndexesBench(r *registry) {
	for i := 0; i <= 10; i++ {
		registerNIndexes(r, i)
	}
}
