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

func registerDebug(r *registry) {
	runDebug := func(ctx context.Context, t *test, c *cluster) {
		nodes := c.nodes
		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Start(ctx, c.Range(1, nodes))
		db := c.Conn(ctx, nodes)

		// Run debug zip command against a node, produce a zip file, extract it, and
		// check information from crdb_internal.gossip_nodes, and gossip_liveness is
		// present.
		debugExtractExist := func(node int, file string) error {
			port := fmt.Sprintf("{pgport:%d}", node)
			if err := c.RunE(ctx, c.Node(node), "./cockroach debug zip "+file+" --insecure --port "+port); err != nil {
				return err
			}

			if err := c.RunE(ctx, c.Node(node), "sudo apt-get install unzip"); err != nil {
				return err
			}

			if err := c.RunE(ctx, c.Node(node), "unzip "+file); err != nil {
				return err
			}

			if err := c.RunE(ctx, c.Node(node), "grep 'epoch' ./debug/gossip/liveness"); err != nil {
				return err
			}

			if err := c.RunE(ctx, c.Node(node), "grep 'server_version' ./debug/gossip/nodes"); err != nil {
				return err
			}

			if err := c.RunE(ctx, c.Node(node), "rm -rf debug"); err != nil {
				return err
			}

			return c.RunE(ctx, c.Node(node), "rm "+file)
		}

		// Wait until each nodes has at least 3 replications.
		for ok := false; !ok; time.Sleep(time.Second) {
			if err := db.QueryRow(
				"SELECT min(array_length(replicas, 1)) >= 3 FROM crdb_internal.ranges",
			).Scan(&ok); err != nil {
				t.Fatal(err)
			}
		}

		// Kill first nodes-1 nodes.
		for i := 1; i < nodes; i++ {
			c.Stop(ctx, c.Node(i))
		}

		// Run debug zip command against the last node, extract the output.zip,
		// and check gossip information is there.
		if err := debugExtractExist(nodes, "output.zip"); err != nil {
			t.Fatal(err)
		}
	}

	for _, n := range []int{3} {
		r.Add(testSpec{
			Name:   fmt.Sprintf("debug/nodes=%d", n),
			Nodes:  nodes(n),
			Stable: true, // DO NOT COPY to new tests
			Run: func(ctx context.Context, t *test, c *cluster) {
				runDebug(ctx, t, c)
			},
		})
	}
}
