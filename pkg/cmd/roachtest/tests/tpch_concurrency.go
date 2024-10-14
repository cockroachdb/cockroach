// Copyright 2021 The Cockroach Authors.
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
		c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), install.MakeClusterSettings(), c.CRDBNodes())

		conn := c.Conn(ctx, t.L(), 1)
		if disableStreamer {
			if _, err := conn.Exec("SET CLUSTER SETTING sql.distsql.use_streamer.enabled = false;"); err != nil {
				t.Fatal(err)
			}
		}

		if err := loadTPCHDataset(
			ctx, t, c, conn, 1 /* sf */, c.NewMonitor(ctx, c.CRDBNodes()),
			c.CRDBNodes(), true, /* disableMergeQueue */
		); err != nil {
			t.Fatal(err)
		}
	}

	restartCluster := func(ctx context.Context, c cluster.Cluster, t test.Test) {
		c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.CRDBNodes())
		c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), install.MakeClusterSettings(), c.CRDBNodes())
	}

	// checkConcurrency returns an error if at least one node of the cluster
	// crashes when the TPCH queries are run with the specified concurrency
	// against the cluster.
	checkConcurrency := func(ctx context.Context, t test.Test, c cluster.Cluster, concurrency int) error {
		// Make sure to kill any workloads running from the previous
		// iteration.
		_ = c.RunE(ctx, option.WithNodes(c.WorkloadNode()), "killall workload")

		restartCluster(ctx, c, t)

		// Scatter the ranges so that a poor initial placement (after loading
		// the data set) doesn't impact the results much.
		conn := c.Conn(ctx, t.L(), 1)
		if _, err := conn.Exec("USE tpch;"); err != nil {
			t.Fatal(err)
		}
		scatterTables(t, conn, tpchTables)
		err := roachtestutil.WaitFor3XReplication(ctx, t.L(), conn)
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

		m := c.NewMonitor(ctx, c.CRDBNodes())
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
					"./cockroach workload run tpch {pgurl%s} --display-every=1ns --tolerate-errors "+
						"--count-errors --queries=%d --concurrency=%d --max-ops=%d",
					c.CRDBNodes(), queryNum, concurrency, maxOps,
				)
				if err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd); err != nil {
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
		// Run at concurrency 500. We often can push this higher, but then the
		// iterations also get longer. 500 concurrently running analytical
		// queries on the 3 node cluster that doesn't crash is much more than we
		// expect our users to run.
		const concurrency = 500
		// Each iteration can take on the order of 3 hours, so we choose the
		// iteration count such that it'd be definitely completed with 18 hour
		// timeout.
		const numIterations = 4
		var numFails int
		for i := 0; i < numIterations; i++ {
			if err := checkConcurrency(ctx, t, c, concurrency); err != nil {
				numFails++
			}
		}
		// Restart the cluster so that if any nodes crashed in the last
		// iteration, it doesn't fail the test.
		restartCluster(ctx, c, t)
		// When the streamer is enabled, we expect better stability, so the
		// failure condition depends on the disableStreamer boolean.
		if disableStreamer {
			if numFails == numIterations {
				// If we had a node crash in every iteration when the streamer
				// is disabled, then we fail the test.
				t.Fatalf("crashed %d times out of %d iterations (streamer disabled)", numFails, numIterations)
			}
		} else {
			if numFails > numIterations/2 {
				// If we had a node crash in more than half of all iterations
				// when the streamer is enabled, then we fail the test.
				t.Fatalf("crashed %d times out of %d iterations (streamer enabled)", numFails, numIterations)
			}
		}
	}

	// Use the longest timeout allowed by the roachtest infra (without marking
	// the test as "weekly").
	const timeout = 18 * time.Hour

	// We run this test without runtime assertions as it pushes the VMs way past
	// the overload point, so it cannot withstand any metamorphic perturbations.
	cockroachBinary := registry.StandardCockroach
	r.Add(registry.TestSpec{
		Name:    "tpch_concurrency",
		Owner:   registry.OwnerSQLQueries,
		Timeout: timeout,
		Cluster: r.MakeClusterSpec(numNodes, spec.WorkloadNode()),
		// Uses gs://cockroach-fixtures-us-east1. See:
		// https://github.com/cockroachdb/cockroach/issues/105968
		CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
		Suites:           registry.Suites(registry.Nightly),
		CockroachBinary:  cockroachBinary,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCHConcurrency(ctx, t, c, false /* disableStreamer */)
		},
	})

	r.Add(registry.TestSpec{
		Name:    "tpch_concurrency/no_streamer",
		Owner:   registry.OwnerSQLQueries,
		Timeout: timeout,
		Cluster: r.MakeClusterSpec(numNodes, spec.WorkloadNode()),
		// Uses gs://cockroach-fixtures-us-east1. See:
		// https://github.com/cockroachdb/cockroach/issues/105968
		CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
		Suites:           registry.Suites(registry.Nightly),
		CockroachBinary:  cockroachBinary,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCHConcurrency(ctx, t, c, true /* disableStreamer */)
		},
	})
}
