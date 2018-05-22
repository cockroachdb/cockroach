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
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerEncryptionBenchmark(r *registry) {
	runBenchmark := func(ctx context.Context, t *test, c *cluster, opt option) {
		nodes := c.nodes - 1
		logMsg := "encryption off: "

		if opt != nil {
			// Encryption is turned on.
			for i := 1; i <= nodes; i++ {
				if err := c.RunE(ctx, c.Node(i), "test -d keys"); err == nil {
					if err := c.RunE(ctx, c.Node(i), "rm -rf keys"); err != nil {
						t.Fatal(err)
					}
				}

				// Create store key directory.
				if err := c.RunE(ctx, c.Node(i), "mkdir keys"); err != nil {
					t.Fatal(err)
				}

				// Create AES 128 bit store key.
				if err := c.RunE(ctx, c.Node(i), "openssl rand -out keys/aes-128.key 48"); err != nil {
					t.Fatal(err)
				}
			}
			logMsg = "encryption on:"
		}

		c.Start(ctx, c.Range(1, nodes), opt)

		const minNumOps = 1000
		const maxNumOps = 2000
		const incr = 50

		var now time.Time
		var elapsed time.Duration
		for n := minNumOps; n <= maxNumOps; n += incr {
			numOps := " --max-ops=" + strconv.Itoa(n)
			cmd := "./workload run kv --tolerate-errors" + numOps + " {pgurl:1-%d}"
			cmd = fmt.Sprintf(cmd, nodes)
			dropCmd := "./workload init kv --drop {pgurl:1-%d}"
			dropCmd = fmt.Sprintf(dropCmd, nodes)
			c.Run(ctx, c.Node(nodes+1), dropCmd)

			now = timeutil.Now()
			if err := c.RunE(ctx, c.Node(nodes+1), cmd); err != nil {
				t.Fatal(err)
			}
			elapsed = timeutil.Since(now)

			// TODO(mberhault): make a better visualization of the benchmark result.
			c.l.printf("%s numOps: %d, elapsed: %s\n", logMsg, n, elapsed)
		}

		stopAndWipe := func(node int) error {
			port := fmt.Sprintf("{pgport:%d}", node)
			if err := c.RunE(ctx, c.Node(node), "./cockroach quit --insecure --port "+port); err != nil {
				return err
			}
			// NB: we still call Stop to make sure the process is dead when we try
			// to restart it (or we'll catch an error from the RocksDB dir being
			// locked). This won't happen unless run with --local due to timing.
			// However, it serves as a reminder that `./cockroach quit` doesn't yet
			// work well enough -- ideally all listeners and engines are closed by
			// the time it returns to the client.
			//
			// TODO(tschottdorf): should return an error. I doubt that we want to
			// call these *testing.T-style methods on goroutines.
			c.Stop(ctx, c.Node(node))
			if opt != nil {
				if err := c.RunE(ctx, c.Node(node), "rm -rf keys"); err != nil {
					return err
				}
			}
			return c.RunE(ctx, c.Node(node), "rm -rf /mnt/data1/cockroach")
		}

		for i := 1; i <= nodes; i++ {
			if err := stopAndWipe(i); err != nil {
				t.Fatal(err)
			}
		}
	}

	runEncryption := func(ctx context.Context, t *test, c *cluster) {
		nodes := c.nodes - 1
		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Node(nodes+1))

		// Args to start node with encryption at rest enabled.
		args := startArgs("--args=--enterprise-encryption=path=/mnt/data1/cockroach,key=keys/aes-128.key,old-key=plain")
		// Run benchmark with encryption turned on.
		runBenchmark(ctx, t, c, args)
		// Run benchmark with encryption turned off.
		runBenchmark(ctx, t, c, nil)
	}

	for _, n := range []int{3} {
		r.Add(testSpec{
			Name:  fmt.Sprintf("encryption/benchmark/nodes=%d", n),
			Nodes: nodes(n + 1),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runEncryption(ctx, t, c)
			},
		})
	}
}
