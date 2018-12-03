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

func registerKV(r *registry) {
	runKV := func(ctx context.Context, t *test, c *cluster, percent int, encryption option) {
		if !c.isLocal() {
			c.RemountNoBarrier(ctx)
		}

		nodes := c.nodes - 1
		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Node(nodes+1))
		c.Start(ctx, t, c.Range(1, nodes), encryption)

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
			for _, e := range []bool{false, true} {
				e := e
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

func registerKVQuiescenceDead(r *registry) {
	r.Add(testSpec{
		Name:       "kv/quiescence/nodes=3",
		Nodes:      nodes(4),
		MinVersion: "2.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			if !c.isLocal() {
				c.RemountNoBarrier(ctx)
			}

			nodes := c.nodes - 1
			c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
			c.Put(ctx, workload, "./workload", c.Node(nodes+1))
			c.Start(ctx, t, c.Range(1, nodes))

			run := func(cmd string, lastDown bool) {
				n := nodes
				if lastDown {
					n--
				}
				m := newMonitor(ctx, c, c.Range(1, n))
				m.Go(func(ctx context.Context) error {
					t.WorkerStatus(cmd)
					defer t.WorkerStatus()
					return c.RunE(ctx, c.Node(nodes+1), cmd)
				})
				m.Wait()
			}

			db := c.Conn(ctx, 1)
			defer db.Close()

			waitForFullReplication(t, db)

			qps := func(f func()) float64 {

				numInserts := func() float64 {
					var v float64
					if err := db.QueryRowContext(
						ctx, `SELECT value FROM crdb_internal.node_metrics WHERE name = 'sql.insert.count'`,
					).Scan(&v); err != nil {
						t.Fatal(err)
					}
					return v
				}

				tBegin := timeutil.Now()
				before := numInserts()
				f()
				after := numInserts()
				return (after - before) / timeutil.Since(tBegin).Seconds()
			}

			const kv = "./workload run kv --duration=10m --read-percent=0"

			// Initialize the database with ~10k ranges so that the absence of
			// quiescence hits hard once a node goes down.
			run("./workload run kv --init --max-ops=1 --splits 10000 --concurrency 100 {pgurl:1}", false)
			run(kv+" --seed 0 {pgurl:1}", true) // warm-up
			// Measure qps with all nodes up (i.e. with quiescence).
			qpsAllUp := qps(func() {
				run(kv+" --seed 1 {pgurl:1}", true)
			})
			// Gracefully shut down third node (doesn't matter whether it's graceful or not).
			c.Run(ctx, c.Node(nodes), "./cockroach quit --insecure --host=:{pgport:3}")
			c.Stop(ctx, c.Node(nodes))
			// Measure qps with node down (i.e. without quiescence).
			qpsOneDown := qps(func() {
				// Use a different seed to make sure it's not just stepping into the
				// other earlier kv invocation's footsteps.
				run(kv+" --seed 2 {pgurl:1}", true)
			})

			if minFrac, actFrac := 0.8, qpsOneDown/qpsAllUp; actFrac < minFrac {
				t.Fatalf(
					"QPS dropped from %.2f to %.2f (factor of %.2f, min allowed %.2f)",
					qpsAllUp, qpsOneDown, actFrac, minFrac,
				)
			}
			t.l.Printf("QPS went from %.2f to %2.f with one node down\n", qpsAllUp, qpsOneDown)
		},
	})
}

func registerKVSplits(r *registry) {
	for _, item := range []struct {
		quiesce bool
		splits  int
		timeout time.Duration
	}{
		{true, 500000, 2 * time.Hour},
		{false, 100000, 2 * time.Hour},
	} {
		item := item // for use in closure below
		r.Add(testSpec{
			Name:    fmt.Sprintf("kv/splits/nodes=3/quiesce=%t", item.quiesce),
			Timeout: item.timeout,
			Nodes:   nodes(4),
			Run: func(ctx context.Context, t *test, c *cluster) {
				nodes := c.nodes - 1
				c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
				c.Put(ctx, workload, "./workload", c.Node(nodes+1))
				c.Start(ctx, t, c.Range(1, nodes),
					startArgs(
						"--env=COCKROACH_MEMPROF_INTERVAL=1m",
						"--env=COCKROACH_DISABLE_QUIESCENCE="+strconv.FormatBool(!item.quiesce),
						"--args=--cache=256MiB",
					))

				t.Status("running workload")
				m := newMonitor(ctx, c, c.Range(1, nodes))
				m.Go(func(ctx context.Context) error {
					concurrency := ifLocal("", " --concurrency="+fmt.Sprint(nodes*64))
					splits := " --splits=" + ifLocal("2000", fmt.Sprint(item.splits))
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
			c.Start(ctx, t, c.Range(1, nodes))

			t.Status("running workload")
			m := newMonitor(ctx, c, c.Range(1, nodes))
			m.Go(func(ctx context.Context) error {
				cmd := fmt.Sprintf("./workload run kv --init --read-percent=%d "+
					"--splits=1000 --duration=1m "+fmt.Sprintf("--concurrency=%d", i)+
					" {pgurl:1-%d}",
					percent, nodes)

				l, err := t.l.ChildLogger(fmt.Sprint(i))
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
