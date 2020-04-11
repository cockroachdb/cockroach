// Copyright 2018 The Cockroach Authors.
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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerDiskFull(r *testRegistry) {
	r.Add(testSpec{
		Name:       "disk-full",
		Owner:      OwnerKV,
		MinVersion: `v2.1.0`,
		Skip:       "https://github.com/cockroachdb/cockroach/issues/35328#issuecomment-478540195",
		Cluster:    makeClusterSpec(5),
		Run: func(ctx context.Context, t *test, c *cluster) {
			if c.isLocal() {
				t.spec.Skip = "you probably don't want to fill your local disk"
				return
			}

			nodes := c.spec.NodeCount - 1
			c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
			c.Put(ctx, workload, "./workload", c.Node(nodes+1))
			c.Start(ctx, t, c.Range(1, nodes))

			t.Status("running workload")
			m := newMonitor(ctx, c, c.Range(1, nodes))
			m.Go(func(ctx context.Context) error {
				cmd := fmt.Sprintf(
					"./workload run kv --tolerate-errors --init --read-percent=0"+
						" --concurrency=10 --duration=2m {pgurl:1-%d}",
					nodes)
				c.Run(ctx, c.Node(nodes+1), cmd)
				return nil
			})
			m.Go(func(ctx context.Context) error {
				time.Sleep(30 * time.Second)
				const n = 1
				m.ExpectDeath()
				t.l.Printf("filling disk on %d\n", n)
				// The 100% ballast size will cause the disk to fill up and the ballast
				// command to exit with an error. The "|| true" is used to ignore that
				// error.
				c.Run(ctx, c.Node(n), "./cockroach debug ballast {store-dir}/ballast --size=100% || true")

				// Restart cockroach in a loop for 30s.
				for start := timeutil.Now(); timeutil.Since(start) < 30*time.Second; {
					if t.Failed() {
						return nil
					}
					t.l.Printf("starting %d when disk is full\n", n)
					// We expect cockroach to die during startup, though it might get far
					// enough along that the monitor detects the death.
					m.ExpectDeath()
					if err := c.StartE(ctx, c.Node(n)); err == nil {
						t.Fatalf("node successfully started unexpectedly")
					} else if strings.Contains(GetStderr(err), "a panic has occurred") {
						t.Fatal(err)
					}
				}

				// Clear the disk full condition and restart cockroach again.
				t.l.Printf("clearing full disk on %d\n", n)
				c.Run(ctx, c.Node(n), "rm -f {store-dir}/ballast")
				// Clear any death expectations that did not occur.
				m.ResetDeaths()
				return c.StartE(ctx, c.Node(n))
			})
			m.Wait()
		},
	})
}
