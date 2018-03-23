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

func init() {
	runTPCC := func(ctx context.Context, t *test, c *cluster, warehouses int, extra string) {
		nodes := c.nodes - 1

		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Node(nodes+1))
		c.Start(ctx, c.Range(1, nodes))

		t.Status("running workload")
		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			duration := " --duration=" + ifLocal("10s", "10m")
			cmd := fmt.Sprintf(
				"./workload run tpcc --init --warehouses=%d"+
					extra+duration+" {pgurl:1-%d}",
				warehouses, nodes)
			c.Run(ctx, c.Node(nodes+1), cmd)
			return nil
		})
		m.Wait()
	}

	tests.Add(testSpec{
		Name:  "tpcc/w=1/nodes=3",
		Nodes: nodes(4),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCC(ctx, t, c, 1, " --wait=false")
		},
	})
	tests.Add(testSpec{
		Name:  "tpmc/w=1/nodes=3",
		Nodes: nodes(4),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCC(ctx, t, c, 1, "")
		},
	})
}
