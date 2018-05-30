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

func registerKV(r *registry) {
	runKV := func(ctx context.Context, t *test, c *cluster, percent int, encryption option) {
		if !c.isLocal() {
			c.RemountNoBarrier(ctx)
		}

		nodes := c.nodes - 1
		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Node(nodes+1))
		c.Start(ctx, c.Range(1, nodes), encryption)

		t.Status("running workload")
		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			concurrency := ifLocal("", " --concurrency="+fmt.Sprint(nodes*64))
			duration := " --duration=" + ifLocal("10s", "10m")
			cmd := fmt.Sprintf(
				"./workload run kv --init --read-percent=%d --splits=1000 --histograms=logs/stats.json"+
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
			// Run kv with encryption turned off because of recently found
			// checksum mismatch when running workload against encrypted
			// cluster.
			// TODO(marc): add back true to turn on encryption for kv when
			// checksum issue is resolved.
			for _, e := range []bool{false} {
				minVersion := "2.0.0"
				if e {
					minVersion = "2.1.0"
				}
				r.Add(testSpec{
					Name:       fmt.Sprintf("kv%d/encrypt=%t/nodes=%d", p, e, n),
					MinVersion: minVersion,
					Nodes:      nodes(n+1, cpu(8)),
					Run: func(ctx context.Context, t *test, c *cluster) {
						runKV(ctx, t, c, p, startArgs(fmt.Sprintf("--encrypt=%t", e)))
					},
				})
			}
		}
	}
}

func registerKVSplits(r *registry) {
	r.Add(testSpec{
		Name:   "kv/splits/nodes=3",
		Nodes:  nodes(4),
		Stable: true, // DO NOT COPY to new tests
		Run: func(ctx context.Context, t *test, c *cluster) {
			nodes := c.nodes - 1
			c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
			c.Put(ctx, workload, "./workload", c.Node(nodes+1))
			c.Start(ctx, c.Range(1, nodes), startArgs("--env=COCKROACH_MEMPROF_INTERVAL=1m"))

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

func registerKVScalability(r *registry) {
	runScalability := func(ctx context.Context, t *test, c *cluster, percent int) {
		nodes := c.nodes - 1

		if !c.isLocal() {
			c.RemountNoBarrier(ctx)
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
			r.Add(testSpec{
				Name:  fmt.Sprintf("kv%d/scale/nodes=6", p),
				Nodes: nodes(7, cpu(8)),
				Run: func(ctx context.Context, t *test, c *cluster) {
					runScalability(ctx, t, c, p)
				},
			})
		}
	}
}
