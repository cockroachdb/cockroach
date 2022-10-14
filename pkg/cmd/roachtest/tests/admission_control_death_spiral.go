// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

func registerDeathSpiral(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:  "admission-control/death-spiral",
		Owner: registry.OwnerAdmissionControl,
		// TODO(abaptist): This test will require a lot of admission control work
		// to pass. Just putting it here to make easy to run at any time.
		Skip:    "#89142",
		Cluster: r.MakeClusterSpec(7, spec.CPU(8)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			roachNodes := c.Range(1, c.Spec().NodeCount-1)
			workloadNode := c.Spec().NodeCount

			c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), roachNodes)

			t.Status("initializing (~1h)")
			c.Run(ctx, c.Node(workloadNode), "./cockroach workload fixtures import tpcc --checks=false --warehouses=10000 {pgurl:1}")

			// Lots of bad things happen during this run.
			t.Status("running workload - this will cause all nodes to crash with OOM (fails in ~2-3 hours)")
			c.Run(ctx, c.Node(workloadNode), "./cockroach workload run tpcc --ramp=6h --tolerate-errors --warehouses=10000 '{pgurl:1-6}'")
		},
	})
}
