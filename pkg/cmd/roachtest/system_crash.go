// Copyright 2019 The Cockroach Authors.
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
	"bufio"
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
)

func registerSystemCrashTest(r *registry) {
	// Two variants of this test:
	//
	// - `syncErrors == false`: The unreliable cockroach processes (i.e., the ones running
	//   with fault injection `Env`s) will crash themselves during a randomly chosen sync
	//   operation. The crash occurs before that sync operation writes out data.
	//
	// - `syncErrors == true`: The unreliable cockroach processes will return an error
	//   from a randomly chosen sync operation, and also drop the writes corresponding
	//   to that sync. If cockroach does not crash itself, the fault injection `Env`
	//   will crash for us shortly afterwards.
	for _, syncErrors := range []bool{false, true} {
		syncErrors := syncErrors
		r.Add(testSpec{
			Name:    fmt.Sprintf("system-crash/sync-errors=%t", syncErrors),
			Cluster: makeClusterSpec(6 /* nodeCount */),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runSystemCrashTest(ctx, t, c, syncErrors)
			},
		})
	}
}

func runSystemCrashTest(ctx context.Context, t *test, c *cluster, syncErrors bool) {
	// Currently setup is as follows:
	// - Node 1 runs a reliable (i.e., no fault injection) Cockroach process
	// - Node 2..n-1 run unreliable Cockroach processes
	// - Node n runs a `workload kv` process. It targets the reliable Cockroach on node 1.
	getCockroachNodes := func(c *cluster) nodeListOption {
		return c.Range(1, c.nodes-1)
	}

	getReliableCockroachNode := func(c *cluster) nodeListOption {
		cockroachNodes := getCockroachNodes(c)
		return cockroachNodes[0:1]
	}

	getUnreliableCockroachNodes := func(c *cluster) nodeListOption {
		cockroachNodes := getCockroachNodes(c)
		return cockroachNodes[1:]
	}

	getWorkloadNode := func(c *cluster) nodeListOption {
		return c.Node(c.nodes)
	}

	t.Status("installing binaries")
	c.Put(ctx, cockroach, "./cockroach", getCockroachNodes(c))
	c.Put(ctx, workload, "./workload", getWorkloadNode(c))

	startCockroachNodes := func(
		ctx context.Context, t *test, c *cluster, nodes nodeListOption, reliable, syncErrors bool,
	) {
		envVars := "COCKROACH_CONSISTENCY_CHECK_INTERVAL=10s"
		args := "--store=path=./data"
		if !reliable {
			if syncErrors {
				args += ",rocksdb=env=sync-failure-injection-wrapping-default-env"
			} else {
				args += ",rocksdb=env=crash-failure-injection-wrapping-default-env"
			}
		}
		c.Start(ctx, t, nodes, startArgs(
			"--env", envVars,
			"--args", args,
		))
	}

	t.Status("starting cockroach")
	startCockroachNodes(ctx, t, c, getReliableCockroachNode(c), true /* reliable */, syncErrors)
	startCockroachNodes(ctx, t, c, getUnreliableCockroachNodes(c), false /* reliable */, syncErrors)

	// There are two goroutines run under the monitor `m`:
	//
	// (1) A goroutine that runs `workload` synchronously.
	// (2) A goroutine that loops running `roachprod monitor`, looking for dead nodes and
	//     restarting them.
	//
	// When nothing goes wrong, (1) finishes first. It then invokes `cancel()` to tell (2)
	// to exit its loop. Then once both goroutines have completed, `m.Wait()` returns.
	ctx, cancel := context.WithCancel(ctx)
	m := newMonitor(ctx, c, getCockroachNodes(c))

	t.Status("running kv workload")
	// Launch goroutine running kv workload
	m.Go(func(ctx context.Context) error {
		c.Run(
			ctx, getWorkloadNode(c), "./workload", "run", "kv", "--init", "--splits=1000",
			"--histograms=logs/stats.json", "--concurrency=12", "--duration=1h",
			"--read-percent=0", "--batch=32", "--min-block-bytes=128", "--max-block-bytes=128",
			"--tolerate-errors", fmt.Sprintf("{pgurl%s}", getReliableCockroachNode(c)),
		)
		cancel()
		return nil
	})

	// Launch goroutine for monitoring and restarting dead cockroach nodes
	m.Go(func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(1 * time.Second):
			}

			var restartNodeIds []int
			// Running `roachprod monitor` under a timeout is a hack copied from
			// `FailOnDeadNodes`. It is necessary because sometimes that command
			// gets stuck in an infinite loop when run with the `--oneshot` option.
			_ = contextutil.RunWithTimeout(
				ctx, "restart dead nodes", 1*time.Second,
				func(ctx context.Context) error {
					output, err := execCmdWithBuffer(
						ctx, t.l, roachprod, "monitor", c.name, "--oneshot", "--ignore-empty-nodes",
					)
					// If there's an error, it means either that the monitor command failed
					// completely, or that it found a dead node worth complaining about.
					if err != nil {
						if ctx.Err() != nil {
							// Don't fail if we timed out. Could be the known infinite loop bug.
							return nil
						}

						// Figure out which nodes are dead and restart them. Output looks like below
						// when there are dead nodes.
						//
						// 3: dead
						// 1: 6890
						// 4: 7075
						// 2: 6951
						// Error:  3: dead
						scanner := bufio.NewScanner(strings.NewReader(string(output)))
						for scanner.Scan() {
							line := scanner.Text()
							if strings.HasPrefix(line, "Error:") {
								// We already passed over all the nodes' statuses once. The
								// remaining lines are redundant (see example output above).
								break
							}
							fields := strings.Split(line, ": ")
							if len(fields) != 2 {
								t.Fatalf("unexpected `roachprod monitor` output line: %s", line)
							}
							nodeId, err := strconv.Atoi(fields[0])
							if err != nil {
								t.Fatalf("unexpected `roachprod monitor` output line: %s", line)
							}
							nodeStatus := fields[1]
							if nodeStatus == "dead" {
								restartNodeIds = append(restartNodeIds, nodeId)
							}
						}
					}
					return nil
				},
			)
			for i := range restartNodeIds {
				startCockroachNodes(
					ctx, t, c, c.Node(restartNodeIds[i]), false, /* reliable */
					syncErrors,
				)
			}
		}
	})

	// Permit any number of node crashes/restarts since this test causes them intentionally.
	m.ExpectDeaths(math.MaxInt32)

	m.Wait()
}
