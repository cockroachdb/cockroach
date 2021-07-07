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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

func registerRoachmart(r registry.Registry) {
	runRoachmart := func(ctx context.Context, t test.Test, c cluster.Cluster, partition bool) {
		c.Put(ctx, t.Cockroach(), "./cockroach")
		c.Put(ctx, t.DeprecatedWorkload(), "./workload")
		c.Start(ctx)

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

		duration := " --duration=" + ifLocal(c, "10s", "10m")

		t.Status("running workload")
		m := c.NewMonitor(ctx)
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
		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("roachmart/partition=%v", v),
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(9, spec.Geo(), spec.Zones("us-central1-b,us-west1-b,europe-west2-b")),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runRoachmart(ctx, t, c, v)
			},
		})
	}
}
