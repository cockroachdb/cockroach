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
	runTPCC := func(t *test, warehouses, nodes int, extra string) {
		ctx := context.Background()
		c := newCluster(ctx, t, nodes+1)
		defer c.Destroy(ctx)

		c.Put(ctx, cockroachPath, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workloadPath, "./workload", c.Node(nodes+1))
		c.Start(ctx, c.Range(1, nodes))

		t.Status("running workload")
		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			duration := " --duration=" + ifLocal("10s", "10m")
			cmd := fmt.Sprintf(
				"./workload run tpcc --init --warehouses=%d"+
					extra+duration+" {pgurl:1-%d}",
				warehouses, nodes)
			c.Run(ctx, nodes+1, cmd)
			return nil
		})
		m.Wait()
	}

	tests.Add("tpcc/w=1/nodes=3", func(t *test) {
		concurrency := ifLocal("", " --concurrency=384")
		runTPCC(t, 1, 3, " --wait=false"+concurrency)
	})
	tests.Add("tpmc/w=1/nodes=3", func(t *test) {
		runTPCC(t, 1, 3, " --concurrency=10")
	})
}
