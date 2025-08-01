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
	"github.com/stretchr/testify/require"
)

func registerLimitCapacity(r registry.Registry) {
	spec := func(subtest string, cfg limitCapacityOpts) registry.TestSpec {
		return registry.TestSpec{
			Name:             "limit_capacity/" + subtest,
			Owner:            registry.OwnerKV,
			Timeout:          1 * time.Hour,
			CompatibleClouds: registry.OnlyGCE,
			Suites:           registry.ManualOnly,
			Cluster:          r.MakeClusterSpec(5, spec.CPU(8), spec.WorkloadNode()),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runLimitCapacity(ctx, t, c, cfg)
			},
		}
	}

	r.Add(spec("write_b=25mb", newAllocatorOverloadOpts().limitWriteCap(26214400)))
	r.Add(spec("compact=0", newAllocatorOverloadOpts().limitCompaction(0)))
}

type limitCapacityOpts struct {
	writeCapBytes      int
	compactConcurrency int
}

func newAllocatorOverloadOpts() limitCapacityOpts {
	return limitCapacityOpts{
		writeCapBytes:      -1,
		compactConcurrency: -1,
	}
}

func (a limitCapacityOpts) limitCompaction(concurrency int) limitCapacityOpts {
	a.compactConcurrency = concurrency
	return a
}

func (a limitCapacityOpts) limitWriteCap(bytes int) limitCapacityOpts {
	a.writeCapBytes = bytes
	return a
}

// runLimitCapacity sets up a kv50 fixed rate workload running against the
// first n-1 nodes, the last node is used for the workload. The n-1'th node has
// any defined capacity limitations applied to it. The test initially runs for
// 10 minutes to establish a baseline level of workload throughput, the
// capacity constraint is applied and after another 2 minutes the throughput is
// measured relative to the baseline QPS.
func runLimitCapacity(ctx context.Context, t test.Test, c cluster.Cluster, cfg limitCapacityOpts) {
	require.False(t, c.IsLocal())

	limitedNodeID := c.Spec().NodeCount - 1
	limitedNode := c.Node(limitedNodeID)

	initialDuration := 10 * time.Minute
	limitDuration := 2 * time.Minute

	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.CRDBNodes())
	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), conn))
	var cancels []func()

	c.Run(ctx, option.WithNodes(c.WorkloadNode()), "./cockroach workload init kv --splits=1000 {pgurl:1}")

	m := c.NewMonitor(ctx, c.CRDBNodes())
	cancels = append(cancels, m.GoWithCancel(func(ctx context.Context) error {
		t.L().Printf("starting load generator\n")
		// NB: kv50 with 4kb block size at 5k rate will incur approx. 500mb/s write
		// bandwidth after 10 minutes across the cluster. Spread across 4 CRDB
		// nodes, expect approx. 125 mb/s write bandwidth each and 30-50% CPU
		// utilization.
		err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf(
			"./cockroach workload run kv --read-percent=50 --tolerate-errors --concurrency=400 "+
				"--min-block-bytes=4096 --max-block-bytes=4096 --max-rate=5000 "+
				"--duration=30m {pgurl:1-%d}", c.Spec().NodeCount-2))
		return err
	}))

	t.Status(fmt.Sprintf("waiting %s for baseline workload throughput", initialDuration))
	wait(c.NewMonitor(ctx, c.CRDBNodes()), initialDuration)
	qpsInitial := roachtestutil.MeasureQPS(ctx, t, c, 10*time.Second, c.Node(1))
	t.Status(fmt.Sprintf("initial (single node) qps: %.0f", qpsInitial))

	if cfg.writeCapBytes >= 0 {
		c.Run(ctx, option.WithNodes(limitedNode), "sudo", "systemctl", "set-property",
			roachtestutil.SystemInterfaceSystemdUnitName(),
			fmt.Sprintf("'IOWriteBandwidthMax=/mnt/data1 %d'", cfg.writeCapBytes))
	}

	if cfg.compactConcurrency >= 0 {
		cancels = append(cancels, m.GoWithCancel(func(ctx context.Context) error {
			t.Status(fmt.Sprintf("setting compaction concurrency on n%d to %d", limitedNodeID, cfg.compactConcurrency))
			limitedConn := c.Conn(ctx, t.L(), limitedNodeID)
			defer limitedConn.Close()

			// Execution will be blocked on setting the compaction concurrency. The
			// execution is cancelled below after measuring the final QPS below.
			_, err := limitedConn.ExecContext(ctx,
				fmt.Sprintf(`SELECT crdb_internal.set_compaction_concurrency(%d,%d,%d)`,
					limitedNodeID, limitedNodeID, cfg.compactConcurrency,
				))
			return err
		}))
	}

	wait(c.NewMonitor(ctx, c.CRDBNodes()), limitDuration)
	qpsFinal := roachtestutil.MeasureQPS(ctx, t, c, 10*time.Second, c.Node(1))
	qpsRelative := qpsFinal / qpsInitial
	t.Status(fmt.Sprintf("initial qps=%f final qps=%f (%f%%)", qpsInitial, qpsFinal, 100*qpsRelative))
	for _, cancel := range cancels {
		cancel()
	}
	// Expect that the relative QPS is at least 90% of the starting QPS.
	require.GreaterOrEqual(t, qpsRelative, 0.9)
}
