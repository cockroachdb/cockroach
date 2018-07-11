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
	"time"
)

func registerQueueWorkload(r *registry) {
	const numNodes = 3

	r.Add(testSpec{
		Name:    fmt.Sprintf("queueWorkload/nodes=%d", numNodes),
		Nodes:   nodes(numNodes),
		Timeout: 5 * time.Hour,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runQueueWorkload(ctx, t, c)
		},
	})
}

func runQueueWorkload(ctx context.Context, t *test, c *cluster) {
	nodes := c.nodes - 1
	c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
	c.Put(ctx, workload, "./workload", c.Node(nodes+1))
	c.Start(ctx, c.Range(1, nodes))

	t.Status("running workload")
	m := newMonitor(ctx, c, c.Range(1, nodes))
	m.Go(func(ctx context.Context) error {
		concurrency := ifLocal("", " --concurrency="+fmt.Sprint(nodes*64))
		duration := " --duration=" + ifLocal("10m", "10m")
		cmd := fmt.Sprintf(
			"./workload run queue --init --histograms=logs/stats.json"+
				concurrency+duration+
				" {pgurl:1-%d}",
			nodes)
		c.Run(ctx, c.Node(nodes+1), cmd)
		return nil
	})
	m.Wait()
}
