// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	_ "github.com/lib/pq"
	"golang.org/x/exp/rand"
)

func registerEngineSwitch(r *testRegistry) {
	runEngineSwitch := func(ctx context.Context, t *test, c *cluster, additionalArgs ...string) {
		roachNodes := c.Range(1, c.spec.NodeCount-1)
		loadNode := c.Node(c.spec.NodeCount)
		c.Put(ctx, workload, "./workload", loadNode)
		c.Put(ctx, cockroach, "./cockroach", roachNodes)
		pebbleArgs := startArgs(append(additionalArgs, "--args=--storage-engine=pebble")...)
		rocksdbArgs := startArgs(append(additionalArgs, "--args=--storage-engine=rocksdb")...)
		c.Start(ctx, t, roachNodes, rocksdbArgs)
		stageDuration := 1 * time.Minute
		if local {
			t.l.Printf("local mode: speeding up test\n")
			stageDuration = 10 * time.Second
		}
		numIters := 5 * len(roachNodes)

		loadDuration := " --duration=" + (time.Duration(numIters) * stageDuration).String()

		workloads := []string{
			// Currently tpcc is the only one with CheckConsistency. We can add more later.
			"./workload run tpcc --tolerate-errors --wait=false --drop --init --warehouses=1 " + loadDuration + " {pgurl:1-%d}",
		}
		checkWorkloads := []string{
			"./workload check tpcc --warehouses=1 --expensive-checks=true {pgurl:1}",
		}
		m := newMonitor(ctx, c, roachNodes)
		for _, cmd := range workloads {
			cmd := cmd // loop-local copy
			m.Go(func(ctx context.Context) error {
				cmd = fmt.Sprintf(cmd, len(roachNodes))
				return c.RunE(ctx, loadNode, cmd)
			})
		}

		usingPebble := make([]bool, len(roachNodes))
		rng := rand.New(rand.NewSource(uint64(timeutil.Now().UnixNano())))
		m.Go(func(ctx context.Context) error {
			l, err := t.l.ChildLogger("engine-switcher")
			if err != nil {
				return err
			}
			// NB: the number of calls to `sleep` needs to be reflected in `loadDuration`.
			sleepAndCheck := func() error {
				t.WorkerStatus("sleeping")
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(stageDuration):
				}
				// Make sure everyone is still running.
				for i := 1; i <= len(roachNodes); i++ {
					t.WorkerStatus("checking ", i)
					db := c.Conn(ctx, i)
					defer db.Close()
					rows, err := db.Query(`SHOW DATABASES`)
					if err != nil {
						return err
					}
					if err := rows.Close(); err != nil {
						return err
					}
					if err := c.CheckReplicaDivergenceOnDB(ctx, db); err != nil {
						return errors.Wrapf(err, "node %d", i)
					}
				}
				return nil
			}

			for i := 0; i < numIters; i++ {
				// First let the load generators run in the cluster.
				if err := sleepAndCheck(); err != nil {
					return err
				}

				stop := func(node int) error {
					m.ExpectDeath()
					if rng.Intn(2) == 0 {
						l.Printf("stopping node gracefully %d\n", node)
						return c.StopCockroachGracefullyOnNode(ctx, node)
					}
					l.Printf("stopping node %d\n", node)
					c.Stop(ctx, c.Node(node))
					return nil
				}

				i := rng.Intn(len(roachNodes))
				var args option
				usingPebble[i] = !usingPebble[i]
				if usingPebble[i] {
					args = pebbleArgs
				} else {
					args = rocksdbArgs
				}
				t.WorkerStatus("switching ", i+1)
				l.Printf("switching %d\n", i+1)
				if err := stop(i + 1); err != nil {
					return err
				}
				c.Start(ctx, t, c.Node(i+1), args)
			}
			return sleepAndCheck()
		})
		m.Wait()

		for _, cmd := range checkWorkloads {
			c.Run(ctx, loadNode, cmd)
		}
	}

	n := 3
	r.Add(testSpec{
		Name:       fmt.Sprintf("engine/switch/nodes=%d", n),
		Owner:      OwnerStorage,
		MinVersion: "v20.1.0",
		Cluster:    makeClusterSpec(n + 1),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runEngineSwitch(ctx, t, c)
		},
	})
	r.Add(testSpec{
		Name:       fmt.Sprintf("engine/switch/encrypted/nodes=%d", n),
		Owner:      OwnerStorage,
		MinVersion: "v20.1.0",
		Cluster:    makeClusterSpec(n + 1),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runEngineSwitch(ctx, t, c, "--encrypt=true")
		},
	})
}
