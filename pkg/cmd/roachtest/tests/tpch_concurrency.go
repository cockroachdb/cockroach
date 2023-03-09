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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/workload/tpch"
	"github.com/stretchr/testify/require"
)

func registerTPCHConcurrency(r registry.Registry) {
	const numNodes = 4

	setupCluster := func(
		ctx context.Context,
		t test.Test,
		c cluster.Cluster,
		disableStreamer bool,
	) {
		c.Put(ctx, t.Cockroach(), "./cockroach", c.Range(1, numNodes-1))
		c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(numNodes))
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(1, numNodes-1))

		conn := c.Conn(ctx, t.L(), 1)
		if disableStreamer {
			if _, err := conn.Exec("SET CLUSTER SETTING sql.distsql.use_streamer.enabled = false;"); err != nil {
				t.Fatal(err)
			}
		}

		if err := loadTPCHDataset(
			ctx, t, c, conn, 1 /* sf */, c.NewMonitor(ctx, c.Range(1, numNodes-1)),
			c.Range(1, numNodes-1), true, /* disableMergeQueue */
		); err != nil {
			t.Fatal(err)
		}
	}

	restartCluster := func(ctx context.Context, c cluster.Cluster, t test.Test) {
		c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Range(1, numNodes-1))
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(1, numNodes-1))
	}

	// checkConcurrency returns an error if at least one node of the cluster
	// crashes when the TPCH queries are run with the specified concurrency
	// against the cluster.
	checkConcurrency := func(ctx context.Context, t test.Test, c cluster.Cluster, concurrency int) error {
		// Make sure to kill any workloads running from the previous
		// iteration.
		_ = c.RunE(ctx, c.Node(numNodes), "killall workload")

		restartCluster(ctx, c, t)

		// Scatter the ranges so that a poor initial placement (after loading
		// the data set) doesn't impact the results much.
		conn := c.Conn(ctx, t.L(), 1)
		if _, err := conn.Exec("USE tpch;"); err != nil {
			t.Fatal(err)
		}
		scatterTables(t, conn, tpchTables)
		err := WaitFor3XReplication(ctx, t, conn)
		require.NoError(t, err)

		// Populate the range cache on each node.
		for nodeIdx := 1; nodeIdx < numNodes; nodeIdx++ {
			node := c.Conn(ctx, t.L(), nodeIdx)
			if _, err := node.Exec("USE tpch;"); err != nil {
				t.Fatal(err)
			}
			for _, table := range tpchTables {
				if _, err := node.Exec(fmt.Sprintf("SELECT count(*) FROM %s", table)); err != nil {
					t.Fatal(err)
				}
			}
		}

		m := c.NewMonitor(ctx, c.Range(1, numNodes-1))
		m.Go(func(ctx context.Context) error {
			t.Status(fmt.Sprintf("running with concurrency = %d", concurrency))
			// Run each query once on each connection.
			for queryNum := 1; queryNum <= tpch.NumQueries; queryNum++ {
				if queryNum == 15 {
					// Skip Q15 because it involves a schema change which - when
					// run with high concurrency - takes non-trivial amount of
					// time.
					t.Status("skipping Q", queryNum)
					continue
				}
				t.Status("running Q", queryNum)
				// The way --max-ops flag works is as follows: the global ops
				// counter is incremented **after** each worker completes a
				// single operation, so it is possible for all connections start
				// up, issue queries, and then the "faster" connections (those
				// for which the queries return sooner) will issue the next
				// query because the global ops counter hasn't reached the
				// --max-ops limit. Only once the limit is reached, no new
				// queries are issued, yet the workload still waits for the
				// already issued queries to complete.
				//
				// Consider the following example: we use --concurrency=3,
				// --max-ops=3, and imagine that
				//   - conn1 completes a query in 1s
				//   - conn2 completes a query in 2s
				//   - conn3 completes a query in 3s.
				// The workload will behave as follows:
				// 1. all three connections issue queries, so we have 3 queries
				//    in flight, 0 completed.
				// 2. after 1s, conn1 completes a query, increases the counter
				//    to 1 which is lower than 3, so it issues another query. We
				//    have 3 queries in flight, 1 completed.
				// 3. after 2s, conn1 and conn2 complete their queries, both
				//    increase a counter, which will eventually become 3. The
				//    connection that increased the counter first will issue
				//    another query, let's assume that conn1 was first. We have
				//    2 queries in flight, 3 completed. conn2 is closed.
				// 4. after 3s, conn1 and conn3 complete their queries and both
				//    exit. In the end a total of 5 ops were completed.
				//
				// In order to make it so that each connection executes the
				// query at least once and usually exactly once, we make the
				// --max-ops flag pretty small. We still want to give enough
				// time to the workload to spin up all connections, so we make
				// it proportional to the total concurrency.
				maxOps := concurrency / 10
				// Use very short duration for --display-every parameter so that
				// all query runs are logged.
				cmd := fmt.Sprintf(
					"./workload run tpch {pgurl:1-%d} --display-every=1ns --tolerate-errors "+
						"--count-errors --queries=%d --concurrency=%d --max-ops=%d",
					numNodes-1, queryNum, concurrency, maxOps,
				)
				if err := c.RunE(ctx, c.Node(numNodes), cmd); err != nil {
					return err
				}
			}
			return nil
		})
		return m.WaitE()
	}

	runTPCHConcurrency := func(
		ctx context.Context,
		t test.Test,
		c cluster.Cluster,
		disableStreamer bool,
	) {
		setupCluster(ctx, t, c, disableStreamer)
		// We use [970, 1030) range to perform the binary search over because
		// - the interval length is smaller than 64, so we will need 6
		// iterations. Each iteration can take 2-3 hours, and we have 18 hour
		// timeout, so 6 iterations is the maximum without marking the test as
		// "weekly".
		// - the interval is centered around 1000. We often can push this a bit
		// higher, but then the iterations also get longer. 1000 concurrently
		// running analytical queries on the 3 node cluster that doesn't crash
		// is much more than we expect our users to run.
		const minConcurrency, maxConcurrency = 970, 1030
		min, max := minConcurrency, maxConcurrency
		// Run the binary search to find the largest concurrency that doesn't
		// crash a node in the cluster. The current range is represented by
		// [min, max).
		for min < max-1 {
			concurrency := (min + max) / 2
			if err := checkConcurrency(ctx, t, c, concurrency); err != nil {
				max = concurrency
			} else {
				min = concurrency
			}
		}
		// Restart the cluster so that if any nodes crashed in the last
		// iteration, it doesn't fail the test.
		restartCluster(ctx, c, t)
		t.Status(fmt.Sprintf("max supported concurrency is %d", min))
		// Write the concurrency number into the stats.json file to be used by
		// the roachperf.
		c.Run(ctx, c.Node(numNodes), "mkdir", t.PerfArtifactsDir())
		cmd := fmt.Sprintf(
			`echo '{ "max_concurrency": %d }' > %s/stats.json`,
			min, t.PerfArtifactsDir(),
		)
		c.Run(ctx, c.Node(numNodes), cmd)
		if min == minConcurrency {
			// In this case, we had a node crash in each iteration of the binary
			// search. This is unexpected, so fail the test.
			t.Fatal("couldn't sustain minimum concurrency")
		}
	}

	// Each iteration of the binary search can take on the order of 2-3 hours
	// (with concurrency around 1000), so use the longest timeout allowed by the
	// roachtest infra (without marking the test as "weekly").
	const timeout = 18 * time.Hour

	r.Add(registry.TestSpec{
		Name:    "tpch_concurrency",
		Owner:   registry.OwnerSQLQueries,
		Timeout: timeout,
		Cluster: r.MakeClusterSpec(numNodes),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCHConcurrency(ctx, t, c, false /* disableStreamer */)
		},
	})

	r.Add(registry.TestSpec{
		Name:    "tpch_concurrency/no_streamer",
		Owner:   registry.OwnerSQLQueries,
		Timeout: timeout,
		Cluster: r.MakeClusterSpec(numNodes),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCHConcurrency(ctx, t, c, true /* disableStreamer */)
		},
	})
}
