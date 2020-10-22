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
)

func registerRoachmart(r *testRegistry) {
	runRoachmart := func(ctx context.Context, t *test, c *cluster, partition bool) {
		c.Put(ctx, cockroach, "./cockroach")
		c.Put(ctx, workload, "./workload")
		c.Start(ctx, t)

		// TODO(benesch): avoid hardcoding this list.
		nodes := []struct {
			i    int
			zone string
		}{
			{1, "us-central1-b"},
			{4, "us-west1-b"},
			{7, "europe-west2-b"},
		}

		roachmartRun := func(ctx context.Context, i int, args ...string) {
			args = append(args,
				"--local-zone="+nodes[i].zone,
				"--local-percent=90",
				"--users=10",
				"--orders=100",
				fmt.Sprintf("--partition=%v", partition))

			if err := c.RunE(ctx, c.Node(nodes[i].i), args...); err != nil {
				t.Fatal(err)
			}
		}
		t.Status("initializing workload")
		roachmartRun(ctx, 0, "./workload", "init", "roachmart")

		duration := " --duration=" + ifLocal("10s", "10m")

		t.Status("running workload")
		m := newMonitor(ctx, c)
		for i := range nodes {
			i := i
			m.Go(func(ctx context.Context) error {
				roachmartRun(ctx, i, "./workload", "run", "roachmart", duration)
				return nil
			})
		}

		m.Wait()
	}

	for _, v := range []bool{true, false} {
		v := v
		r.Add(testSpec{
			Name:    fmt.Sprintf("roachmart/partition=%v", v),
			Owner:   OwnerPartitioning,
			Cluster: makeClusterSpec(9, geo(), zones("us-central1-b,us-west1-b,europe-west2-b")),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runRoachmart(ctx, t, c, v)
			},
		})
	}
}
