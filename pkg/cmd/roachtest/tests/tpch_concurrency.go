// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

func registerTPCHConcurrency(r registry.Registry) {
	const numNodes = 4

	setupCluster := func(ctx context.Context, t test.Test, c cluster.Cluster, admissionControl bool) {
		c.Put(ctx, t.Cockroach(), "./cockroach", c.Range(1, numNodes-1))
		c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(numNodes))
		c.Start(ctx, c.Range(1, numNodes-1))

		// In order to keep more things constant throughout the different test
		// runs, we disable range merges and range movements.
		conn := c.Conn(ctx, 1)
		if _, err := conn.Exec("SET CLUSTER SETTING kv.allocator.min_lease_transfer_interval = '24h';"); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec("SET CLUSTER SETTING kv.range_merge.queue_enabled = false;"); err != nil {
			t.Fatal(err)
		}

		if err := loadTPCHDataset(ctx, t, c, 1 /* sf */, c.NewMonitor(ctx, c.Range(1, numNodes-1)), c.Range(1, numNodes-1)); err != nil {
			t.Fatal(err)
		}
		if admissionControl {
			EnableAdmissionControl(ctx, t, c)
		}
	}

	restartCluster := func(ctx context.Context, c cluster.Cluster) {
		c.Stop(ctx, c.Range(1, numNodes-1))
		c.Start(ctx, c.Range(1, numNodes-1))
	}

	// checkConcurrency returns an error if at least one node of the cluster
	// crashes when the TPCH workload is run with the specified concurrency
	// against the cluster.
	checkConcurrency := func(ctx context.Context, t test.Test, c cluster.Cluster, concurrency int) error {
		restartCluster(ctx, c)
		// Make sure to kill any workloads running from the previous
		// iteration.
		_ = c.RunE(ctx, c.Node(numNodes), "killall workload")

		// Scatter the ranges so that a poor initial placement (after loading
		// the data set) doesn't impact the results much.
		conn := c.Conn(ctx, 1)
		if _, err := conn.Exec("USE tpch;"); err != nil {
			t.Fatal(err)
		}
		scatterTables(t, conn, tpchTables)
		WaitFor3XReplication(t, conn)

		m := c.NewMonitor(ctx, c.Range(1, numNodes-1))
		m.Go(func(ctx context.Context) error {
			t.Status(fmt.Sprintf("running with concurrency = %d", concurrency))
			cmd := fmt.Sprintf(
				"./workload run tpch {pgurl:1-%d} --tolerate-errors "+
					"--duration=1h --concurrency=%d", numNodes-1, concurrency,
			)
			return c.RunE(ctx, c.Node(numNodes), cmd)
		})
		return m.WaitE()
	}

	runTPCHConcurrency := func(ctx context.Context, t test.Test, c cluster.Cluster, admissionControl bool) {
		setupCluster(ctx, t, c, admissionControl)
		minConcurrency, maxConcurrency := 32, 256
		// Run the binary search to find the largest concurrency that doesn't
		// crash a node in the cluster. The current range is represented by
		// [minConcurrency, maxConcurrency).
		for minConcurrency < maxConcurrency-1 {
			concurrency := (minConcurrency + maxConcurrency) / 2
			if err := checkConcurrency(ctx, t, c, concurrency); err != nil {
				maxConcurrency = concurrency
			} else {
				minConcurrency = concurrency
			}
		}
		// Restart the cluster so that if any nodes crashed in the last
		// iteration, it doesn't fail the test.
		restartCluster(ctx, c)
		t.Status(fmt.Sprintf("max supported concurrency is %d", minConcurrency))
		// Write the concurrency number into the stats.json file to be used by
		// the roachperf.
		c.Run(ctx, c.Node(numNodes), "mkdir", t.PerfArtifactsDir())
		cmd := fmt.Sprintf(
			`echo '{ "max_concurrency": %d }' > %s/stats.json`,
			minConcurrency, t.PerfArtifactsDir(),
		)
		c.Run(ctx, c.Node(numNodes), cmd)
	}

	for _, admissionControl := range []bool{false, true} {
		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("tpch_concurrency/admission_control=%t", admissionControl),
			Owner:   registry.OwnerSQLQueries,
			Cluster: r.MakeClusterSpec(numNodes),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runTPCHConcurrency(ctx, t, c, admissionControl)
			},
		})
	}
}
