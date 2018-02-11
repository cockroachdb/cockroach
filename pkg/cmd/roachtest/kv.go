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
		concurrency := " --concurrency=384"
		duration := " --duration=10m"
		if local {
			concurrency = ""
			duration = " --duration=10s"
		}

		ctx := context.Background()
		c := newCluster(ctx, t, "-n", nodes+1)
		defer c.Destroy(ctx)

		c.Put(ctx, cockroach, "<cockroach>")
		c.Put(ctx, workload, "<workload>")
		c.Start(ctx, 1, nodes)

		m := newMonitor(ctx, c)
		m.Go(func(ctx context.Context) error {
			cmd := fmt.Sprintf(
				"<workload> run kv --init --read-percent=%d --splits=1000"+
					concurrency+duration+
					" {pgurl:1-%d}",
				percent, nodes)
			c.Run(ctx, nodes+1, cmd)
			return nil
		})
		m.Wait(1, nodes)
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
		concurrency := " --concurrency=384"
		splits := " --splits=500000"
		if local {
			concurrency = ""
			splits = " --splits=2000"
		}

		ctx := context.Background()
		c := newCluster(ctx, t, "-n", nodes+1)
		defer c.Destroy(ctx)

		c.Put(ctx, cockroach, "<cockroach>")
		c.Put(ctx, workload, "<workload>")
		c.Start(ctx, 1, nodes)

		m := newMonitor(ctx, c)
		m.Go(func(ctx context.Context) error {
			cmd := fmt.Sprintf(
				"<workload> run kv --init --max-ops=1"+
					concurrency+splits+
					" {pgurl:1-%d}",
				nodes)
			c.Run(ctx, nodes+1, cmd)
			return nil
		})
		m.Wait(1, nodes)
	}

	tests.Add("splits/nodes=3", func(t *test) {
		runSplits(t, 3)
	})
}

func init() {
	runScalability := func(t *test, percent, nodes int) {
		ctx := context.Background()
		c := newCluster(ctx, t, "-n", nodes+1, "--local-ssd", "--machine-type", "n1-highcpu-8")
		defer c.Destroy(ctx)

		if !local {
			var wg sync.WaitGroup
			wg.Add(6)
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

		c.Put(ctx, cockroach, "<cockroach>")
		c.Put(ctx, workload, "<workload>")

		for i := nodes; i <= nodes*64; i += nodes {
			c.Wipe(ctx, 1, nodes)
			c.Start(ctx, 1, nodes)

			m := newMonitor(ctx, c)
			m.Go(func(ctx context.Context) error {
				cmd := fmt.Sprintf("<workload> run kv --init --read-percent=%d "+
					"--splits=1000 --duration=1m "+fmt.Sprintf("--concurrency=%d", i)+
					" {pgurl:1-%d}",
					percent, nodes)
				c.Run(ctx, nodes+1, cmd)
				return nil
			})
			m.Wait(1, nodes)
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
