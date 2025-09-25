// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

func registerSimple(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "simple/herko-simple",
		Owner:            registry.OwnerTestEng,
		Cluster:          r.MakeClusterSpec(2, spec.CPU(4), spec.WorkloadNodeCount(1), spec.WorkloadNodeCPU(4)),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.DefaultLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			t.L().Printf("CLUSTER NAME = %s", c.Name())

			details, err := c.RunWithDetails(ctx, t.L(), option.WithNodes(c.All()), "lsblk -dn -o NAME,SIZE")
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range details {
				t.L().Printf("lsblk node %s: %s", d.Node, d.Stdout)
			}

			details, err = c.RunWithDetails(ctx, t.L(), option.WithNodes(c.All()), "df -h")
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range details {
				t.L().Printf("df node %s: %s", d.Node, d.Stdout)
			}

			// Sleep for 10 minutes to allow for manual testing.
			time.Sleep(10 * time.Minute)
		},
	})
}
