// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerDiskFull(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "disk-full",
		Owner:   registry.OwnerStorage,
		Cluster: r.MakeClusterSpec(5),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.IsLocal() {
				t.Skip("you probably don't want to fill your local disk")
			}

			nodes := c.Spec().NodeCount - 1
			c.Put(ctx, t.Cockroach(), "./cockroach", c.Range(1, nodes))
			c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(nodes+1))
			c.Start(ctx, c.Range(1, nodes))

			t.Status("running workload")
			m := c.NewMonitor(ctx, c.Range(1, nodes))
			m.Go(func(ctx context.Context) error {
				cmd := fmt.Sprintf(
					"./workload run kv --tolerate-errors --init --read-percent=0"+
						" --concurrency=10 --duration=2m {pgurl:2-%d}",
					nodes)
				c.Run(ctx, c.Node(nodes+1), cmd)
				return nil
			})
			m.Go(func(ctx context.Context) error {
				time.Sleep(30 * time.Second)
				const n = 1
				m.ExpectDeath()
				t.L().Printf("filling disk on %d\n", n)
				// The 100% ballast size will cause the disk to fill up and the ballast
				// command to exit with an error. The "|| true" is used to ignore that
				// error.
				c.Run(ctx, c.Node(n), "./cockroach debug ballast {store-dir}/ballast --size=100% || true")

				// Restart cockroach in a loop for 30s.
				for start := timeutil.Now(); timeutil.Since(start) < 30*time.Second; {
					if t.Failed() {
						return nil
					}
					t.L().Printf("starting %d when disk is full\n", n)
					// Pebble treats "no space left on device" as a background error. Kill
					// cockroach if it is still running. Note that this is to kill the
					// node that was started prior to this for loop, before the ballast
					// was created. Now that the disk is full, any subsequent node starts
					// must fail with an error for this test to succeed.
					_ = c.StopE(ctx, c.Node(n))
					// We expect cockroach to die during startup, though it might get far
					// enough along that the monitor detects the death.
					m.ExpectDeath()
					if err := c.StartE(ctx, c.Node(n)); err == nil {
						t.Fatalf("node successfully started unexpectedly")
					} else if strings.Contains(cluster.GetStderr(err), "a panic has occurred") {
						t.Fatal(err)
					}
				}

				// Clear the disk full condition and restart cockroach again.
				t.L().Printf("clearing full disk on %d\n", n)
				c.Run(ctx, c.Node(n), "rm -f {store-dir}/ballast")
				// Clear any death expectations that did not occur.
				m.ResetDeaths()
				return c.StartE(ctx, c.Node(n))
			})
			m.Wait()
		},
	})
}
