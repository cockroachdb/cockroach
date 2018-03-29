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
	runKV := func(ctx context.Context, t *test, c *cluster, percent int) {
		nodes := c.nodes - 1
		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Node(nodes+1))
		c.Start(ctx, c.Range(1, nodes))

		t.Status("running workload")
		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			concurrency := ifLocal("", " --concurrency="+fmt.Sprint(nodes*64))
			duration := " --duration=" + ifLocal("10s", "10m")
			cmd := fmt.Sprintf(
				"./workload run kv --init --read-percent=%d --splits=1000"+
					concurrency+duration+
					" {pgurl:1-%d}",
				percent, nodes)
			c.Run(ctx, c.Node(nodes+1), cmd)
			return nil
		})
		m.Wait()
	}

	for _, p := range []int{0, 95} {
		p := p
		for _, n := range []int{1, 3} {
			tests.Add(testSpec{
				Name:  fmt.Sprintf("kv%d/nodes=%d", p, n),
				Nodes: nodes(n + 1),
				Run: func(ctx context.Context, t *test, c *cluster) {
					runKV(ctx, t, c, p)
				},
			})
		}
	}
}

func init() {
	tests.Add(testSpec{
		Name:  "kv/splits/nodes=3",
		Nodes: nodes(4),
		Run: func(ctx context.Context, t *test, c *cluster) {
			nodes := c.nodes - 1
			c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
			c.Put(ctx, workload, "./workload", c.Node(nodes+1))
			c.Start(ctx, c.Range(1, nodes))

			t.Status("running workload")
			m := newMonitor(ctx, c, c.Range(1, nodes))
			m.Go(func(ctx context.Context) error {
				concurrency := ifLocal("", " --concurrency="+fmt.Sprint(nodes*64))
				splits := " --splits=" + ifLocal("2000", "500000")
				cmd := fmt.Sprintf(
					"./workload run kv --init --max-ops=1"+
						concurrency+splits+
						" {pgurl:1-%d}",
					nodes)
				c.Run(ctx, c.Node(nodes+1), cmd)
				return nil
			})
			m.Wait()
		},
	})
}

func init() {
	runScalability := func(ctx context.Context, t *test, c *cluster, percent int) {
		nodes := c.nodes - 1

		if !c.isLocal() {
			c.Run(ctx, c.All(),
				"sudo", "umount", "/mnt/data1", ";",
				"sudo", "mount", "-o", "discard,defaults,nobarrier",
				"/dev/disk/by-id/google-local-ssd-0", "/mnt/data1")
		}

		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Node(nodes+1))

		const maxPerNodeConcurrency = 64
		for i := nodes; i <= nodes*maxPerNodeConcurrency; i += nodes {
			c.Wipe(ctx, c.Range(1, nodes))
			c.Start(ctx, c.Range(1, nodes))

			t.Status("running workload")
			m := newMonitor(ctx, c, c.Range(1, nodes))
			m.Go(func(ctx context.Context) error {
				cmd := fmt.Sprintf("./workload run kv --init --read-percent=%d "+
					"--splits=1000 --duration=1m "+fmt.Sprintf("--concurrency=%d", i)+
					" {pgurl:1-%d}",
					percent, nodes)

				l, err := c.l.childLogger(fmt.Sprint(i))
				if err != nil {
					t.Fatal(err)
				}
				defer l.close()

				return c.RunL(ctx, l, c.Node(nodes+1), cmd)
			})
			m.Wait()
		}
	}

	// TODO(peter): work in progress adaption of `roachprod test kv{0,95}`.
	if false {
		for _, p := range []int{0, 95} {
			p := p
			tests.Add(testSpec{
				Name:  fmt.Sprintf("kv%d/scale/nodes=6", p),
				Nodes: nodes(7, cpu(8)),
				Run: func(ctx context.Context, t *test, c *cluster) {
					runScalability(ctx, t, c, p)
				},
			})
		}
	}
}
