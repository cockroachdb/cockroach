// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

func runRestartRollingAndRolling(
	ctx context.Context, t test.Test, c cluster.Cluster, downDuration time.Duration,
) {
	t.Status("starting the cluster")
	startOpts := option.DefaultStartOpts()
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.CRDBNodes())

	t.Status("initializing kv workload")
	// Init KV workload with a bunch of pre-split ranges and pre-inserted rows.
	// The block sizes are picked the same as for the workload below.
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf(
		"./cockroach workload init kv --drop --splits=1000 --insert-count=3000 "+
			"--min-block-bytes=128 --max-block-bytes=256 {pgurl%s}",
		c.Node(1)))

	m := c.NewMonitor(ctx, c.CRDBNodes())
	m.Go(func(ctx context.Context) error {
		t.Status("running kv workload")
		const duration = 20 * time.Minute
		// The workloads are tuned to keep the cluster busy at 30-40% CPU, and IO
		// overload metric approaching 20-30% which causes elastic traffic being
		// de-prioritized and wait.
		cmd := roachtestutil.NewCommand("./cockroach workload run kv "+
			"--histograms=perf/stats.json --concurrency=500 "+
			"--max-rate=5000 --read-percent=5 "+
			"--min-block-bytes=128 --max-block-bytes=256 "+
			"--txn-qos='regular' --tolerate-errors"+
			"--duration=%v {pgurl%s}", duration, c.CRDBNodes())
		return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd.String())
	})

	restart := func() {
		for _, n := range c.CRDBNodes() {
			m.ExpectDeath()
			t.Status(fmt.Sprintf("Stopping node %d.", n))
			c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(n))
			m.ResetDeaths()
			t.Status(fmt.Sprintf("Node %d stopped. Restarting.", n))
			c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(n))
			t.Status(fmt.Sprintf("Node %d restarted. Waiting.", n))
			time.Sleep(2 * time.Minute)
		}
	}

	for i := 0; i < 3; i++ {
		t.Status(fmt.Sprintf("Rolling restart #%d", i))
		restart()
	}

	m.Wait()
}

func registerRestartRollingAndRolling(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "restart/rolling-and-rolling",
		Owner:            registry.OwnerKV,
		Cluster:          r.MakeClusterSpec(4, spec.CPU(8), spec.WorkloadNode(), spec.ReuseNone()),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.ManualOnly,
		Leases:           registry.EpochLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runRestartRollingAndRolling(ctx, t, c, 2*time.Minute)
		},
	})
}
