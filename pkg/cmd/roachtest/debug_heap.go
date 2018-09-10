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

func registerDebugHeap(r *registry) {
	runDebug := func(ctx context.Context, t *test, c *cluster) {
		nodes := c.nodes
		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Start(ctx, c.Range(1, nodes))
		db := c.Conn(ctx, nodes)

		// Run debug zip command against a node, produce a zip file, extract it, and
		// check information from heap profiler is present.
		debugExtractExist := func(node int, file string) error {
			port := fmt.Sprintf("{pgport:%d}", node)
			if err := c.RunE(ctx, c.Node(node), "./cockroach sql --insecure --host=:"+port+
				" -e 'set cluster setting server.heap_profile.system_memory_threshold_fraction=0;'"); err != nil {
				return err
			}

			// Needed for the new cluster setting to take hold and
			// trigger a heap profile generation.
			time.Sleep(30 * time.Second)

			if err := c.RunE(ctx, c.Node(node), "./cockroach debug zip "+file+" --insecure --host=:"+port); err != nil {
				return err
			}

			if err := c.RunE(ctx, c.Node(node), "unzip -v || sudo apt-get install unzip"); err != nil {
				return err
			}

			if err := c.RunE(ctx, c.Node(node), "unzip "+file); err != nil {
				return err
			}

			if err := c.RunE(ctx, c.Node(node), "find debug/ -print | grep 'memprof.fraction_system_memory'"); err != nil {
				return err
			}

			if err := c.RunE(ctx, c.Node(node), "./cockroach sql --insecure --host=:"+port+
				" -e 'reset cluster setting server.heap_profile.system_memory_threshold_fraction;'"); err != nil {
				return err
			}

			if err := c.RunE(ctx, c.Node(node), "rm -rf debug"); err != nil {
				return err
			}

			return c.RunE(ctx, c.Node(node), "rm "+file)
		}

		waitForFullReplication(t, db)

		// Run debug zip command against the last node, extract the output.zip,
		// and check gossip information is there.
		if err := debugExtractExist(nodes, "output.zip"); err != nil {
			t.Fatal(err)
		}
	}

	for _, n := range []int{3} {
		r.Add(testSpec{
			Name:   fmt.Sprintf("heapprof/nodes=%d", n),
			Nodes:  nodes(n),
			Stable: true, // DO NOT COPY to new tests
			Run: func(ctx context.Context, t *test, c *cluster) {
				runDebug(ctx, t, c)
			},
		})
	}
}
