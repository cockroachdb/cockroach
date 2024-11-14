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

func runRestartRollingAndRolling(ctx context.Context, t test.Test, c cluster.Cluster) {
	t.Status("starting the cluster")
	startOpts := option.DefaultStartOpts()
	cs := install.MakeClusterSettings()
	cs.ClusterSettings["server.consistency_check.interval"] = "1m"
	cs.ClusterSettings["server.consistency_check.max_rate"] = "1GB"
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.CRDBNodes())

	// Limit the disk throughput to 128 MiB/s, to more easily stress IO overload.
	const diskBand = 128 << 20 // 128 MiB
	t.Status(fmt.Sprintf("limiting disk bandwidth to %d bytes/s", diskBand))
	staller := roachtestutil.MakeCgroupDiskStaller(t, c,
		false /* readsToo */, false /* logsToo */)
	staller.Setup(ctx)
	staller.Slow(ctx, c.CRDBNodes(), diskBand)

	t.Status("initializing kv workload")
	// Init KV workload with a bunch of pre-split ranges and pre-inserted rows.
	// The block sizes are picked the same as for the workload below.
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf(
		"./cockroach workload init kv --drop --splits=3000 --insert-count=3000 "+
			"--min-block-bytes=128 --max-block-bytes=256 {pgurl%s}",
		c.Node(1)))

	m := c.NewMonitor(ctx, c.CRDBNodes())
	m.Go(func(ctx context.Context) error {
		t.Status("running kv workload")
		const duration = 20 * time.Minute
		// TODO: Tune the workload are to keep the cluster busy 60% CPU, and IO
		// overload metric approaching 10-20%.
		cmd := roachtestutil.NewCommand("./cockroach workload run kv "+
			"--histograms=perf/stats.json --concurrency=500 "+
			"--max-rate=5000 --read-percent=5 "+
			"--min-block-bytes=128 --max-block-bytes=256 "+
			"--txn-qos='regular' --tolerate-errors "+
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
			time.Sleep(15 * time.Second)
			c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(n))
			staller.Slow(ctx, c.Node(n), diskBand)
			t.Status(fmt.Sprintf("Node %d restarted. Waiting.", n))
			time.Sleep(2 * time.Minute)

			db := c.Conn(ctx, t.L(), 2)
			if err := roachtestutil.CheckReplicaDivergenceOnDB(ctx, t.L(), db); err != nil {
				t.Fatal(err)
			}
			db.Close()
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
		Cluster:          r.MakeClusterSpec(4, spec.CPU(4), spec.WorkloadNode(), spec.ReuseNone()),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.ManualOnly,
		Leases:           registry.EpochLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runRestartRollingAndRolling(ctx, t, c)
		},
	})
}
