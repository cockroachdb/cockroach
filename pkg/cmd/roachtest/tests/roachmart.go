// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

func registerRoachmart(r registry.Registry) {
	runRoachmart := func(ctx context.Context, t test.Test, c cluster.Cluster, partition bool) {
		// This test expects the workload binary on all nodes.
		c.Put(ctx, t.DeprecatedWorkload(), "./workload")
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())

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

			if err := c.RunE(ctx, option.WithNodes(c.Node(nodes[i].i)), args...); err != nil {
				t.Fatal(err)
			}
		}
		t.Status("initializing workload")

		// See https://github.com/cockroachdb/cockroach/issues/94062 for the --data-loader.
		roachmartRun(ctx, 0, "./workload", "init", "roachmart", "--data-loader=INSERT", "{pgurl:1}")

		duration := " --duration=" + roachtestutil.IfLocal(c, "10s", "10m")

		t.Status("running workload")
		m := c.NewMonitor(ctx)
		for i := range nodes {
			i := i
			m.Go(func(ctx context.Context) error {
				roachmartRun(ctx, i, "./workload", "run", "roachmart", duration, fmt.Sprintf("{pgurl%s}", c.Node(i+1)))
				return nil
			})
		}

		m.Wait()
	}

	for _, v := range []bool{true, false} {
		v := v
		r.Add(registry.TestSpec{
			Name:                       fmt.Sprintf("roachmart/partition=%v", v),
			Owner:                      registry.OwnerKV,
			Cluster:                    r.MakeClusterSpec(9, spec.Geo(), spec.GCEZones("us-central1-b,us-west1-b,europe-west2-b")),
			CompatibleClouds:           registry.OnlyGCE,
			Suites:                     registry.Suites(registry.Nightly),
			Leases:                     registry.MetamorphicLeases,
			RequiresDeprecatedWorkload: true, // uses roachmart
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runRoachmart(ctx, t, c, v)
			},
		})
	}
}
