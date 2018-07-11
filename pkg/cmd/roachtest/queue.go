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

func registerQueue(r *registry) {
	// One node runs the workload generator, all other nodes host CockroachDB.
	const numNodes = 2
	r.Add(testSpec{
		Name:  fmt.Sprintf("queue/nodes=%d", numNodes-1),
		Nodes: nodes(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runQueue(ctx, t, c)
		},
	})
}

func runQueue(ctx context.Context, t *test, c *cluster) {
	dbNodeCount := c.nodes - 1
	workloadNode := c.nodes

	// Distribute programs to the correct nodes and start CockroachDB.
	c.Put(ctx, cockroach, "./cockroach", c.Range(1, dbNodeCount))
	c.Put(ctx, workload, "./workload", c.Node(workloadNode))
	c.Start(ctx, c.Range(1, dbNodeCount))

	t.Status("running workload")
	m := newMonitor(ctx, c, c.Range(1, dbNodeCount))
	m.Go(func(ctx context.Context) error {
		concurrency := ifLocal("", " --concurrency="+fmt.Sprint(dbNodeCount*64))
		duration := " --duration=10m"
		cmd := fmt.Sprintf(
			"./workload run queue --init --histograms=logs/stats.json"+
				concurrency+
				duration+
				" {pgurl:1-%d}",
			dbNodeCount,
		)
		c.Run(ctx, c.Node(workloadNode), cmd)
		return nil
	})
	m.Wait()
}
