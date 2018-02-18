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
	"sync"
)

func init() {
	runKV := func(t *test, percent, nodes int) {
		ctx := context.Background()
		c := newCluster(ctx, t, nodes+1, "--local-ssd")
		defer c.Destroy(ctx)

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
			c.Run(ctx, nodes+1, cmd)
			return nil
		})
		m.Wait()
	}

	for _, p := range []int{0, 95} {
		p := p
		for _, n := range []int{1, 3} {
			n := n
			tests.Add(fmt.Sprintf("kv%d/nodes=%d", p, n),
				func(t *test) {
					runKV(t, p, n)
				})
		}
	}
}

func init() {
	runSplits := func(t *test, nodes int) {
		ctx := context.Background()
		c := newCluster(ctx, t, nodes+1, "--local-ssd")
		defer c.Destroy(ctx)

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
			c.Run(ctx, nodes+1, cmd)
			return nil
		})
		m.Wait()
	}

	tests.Add("splits/nodes=3", func(t *test) {
		runSplits(t, 3)
	})
}

func init() {
	runScalability := func(t *test, percent, nodes int) {
		ctx := context.Background()
		c := newCluster(ctx, t, nodes+1, "--local-ssd", "--machine-type", "n1-highcpu-8")
		defer c.Destroy(ctx)

		if !c.isLocal() {
			var wg sync.WaitGroup
			wg.Add(nodes)
			for i := 1; i <= 6; i++ {
				go func(i int) {
					defer wg.Done()
					c.Run(ctx, i,
						"sudo", "umount", "/mnt/data1", ";",
						"sudo", "mount", "-o", "discard,defaults,nobarrier",
						"/dev/disk/by-id/google-local-ssd-0", "/mnt/data1")
				}(i)
			}
			wg.Wait()
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

				c.RunL(ctx, l, nodes+1, cmd)
				return nil
			})
			m.Wait()
		}
	}

	// TODO(peter): work in progress adaption of `roachprod test kv{0,95}`.
	if false {
		for _, p := range []int{0, 95} {
			p := p
			tests.Add(fmt.Sprintf("kv%d/scale/nodes=6", p),
				func(t *test) {
					runScalability(t, p, 6)
				})
		}
	}
}
