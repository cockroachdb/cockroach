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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func registerFailover(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "failover/non-system/crash",
		Owner:   registry.OwnerKV,
		Timeout: time.Hour,
		Cluster: r.MakeClusterSpec(7, spec.CPU(4)),
		Run:     runFailoverNonSystemCrash,
	})
}

// runFailoverNonSystemCrash benchmarks the maximum duration of range
// unavailability following a leaseholder crash with only non-system ranges. It
// tests the simplest possible failure:
//
//   - A process crash, where the host/OS remains available (in particular, the
//     TCP/IP stack is responsive and sends immediate RST packets to peers).
//
//   - No system ranges located on the crashed node.
//
//   - SQL clients do not connect to the crashed node.
//
//   - The workload consists of individual point reads and writes.
//
// Since the lease unavailability is probabilistic, depending e.g. on the time
// since the last heartbeat and other variables, we run 9 crashes and record the
// pMax latency to find the upper bound on unavailability. We expect this
// worse-case latency to be slightly larger than the lease interval (9s), to
// account for lease acquisition and retry latencies. We do not assert this, but
// instead export latency histograms for graphing.
//
// The cluster layout is as follows:
//
// n1-n3: System ranges and SQL gateways.
// n4-n6: Workload ranges.
// n7:    Workload runner.
//
// The test runs a kv50 workload with batch size 1, using 256 concurrent workers
// directed at n1-n3 with a rate of 2048 reqs/s. n4-n6 are killed and restarted
// in order, with 30 seconds between each operation, for 3 cycles totaling 9
// crashes.
func runFailoverNonSystemCrash(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Put(ctx, t.Cockroach(), "./cockroach")

	require.Equal(t, 7, c.Spec().NodeCount)

	// Create cluster.
	opts := option.DefaultStartOpts()
	settings := install.MakeClusterSettings()
	c.Start(ctx, t.L(), opts, settings, c.Range(1, 6))

	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	// Configure cluster. This test controls the ranges manually.
	t.Status("configuring cluster")
	_, err := conn.ExecContext(ctx, `SET CLUSTER SETTING kv.range_split.by_load_enabled = 'false'`)
	require.NoError(t, err)

	// Constrain all existing zone configs to n1-n3.
	rows, err := conn.QueryContext(ctx, `SELECT target FROM [SHOW ALL ZONE CONFIGURATIONS]`)
	require.NoError(t, err)
	for rows.Next() {
		var target string
		require.NoError(t, rows.Scan(&target))
		_, err = conn.ExecContext(ctx, fmt.Sprintf(
			`ALTER %s CONFIGURE ZONE USING num_replicas = 3, constraints = '[-node4, -node5, -node6]'`,
			target))
		require.NoError(t, err)
	}
	require.NoError(t, rows.Err())

	// Wait for upreplication.
	require.NoError(t, WaitFor3XReplication(ctx, t, conn))

	// Create the kv database, constrained to n4-n6. Despite the zone config, the
	// ranges will initially be distributed across all cluster nodes.
	t.Status("creating workload database")
	_, err = conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `ALTER DATABASE kv CONFIGURE ZONE USING `+
		`num_replicas = 3, constraints = '[-node1, -node2, -node3]'`)
	require.NoError(t, err)
	c.Run(ctx, c.Node(7), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	// The replicate queue takes forever to move the kv ranges from n1-n3 to
	// n4-n6, so we do it ourselves. Precreating the database/range and moving it
	// to the correct nodes first is not sufficient, since workload will spread
	// the ranges across all nodes regardless.
	relocateRanges := func(predicate string, from, to []int) {
		require.NotEmpty(t, predicate)
		var count int
		for _, source := range from {
			where := fmt.Sprintf("%s AND %d = ANY(replicas)", predicate, source)
			for {
				require.NoError(t, conn.QueryRowContext(ctx,
					`SELECT count(*) FROM crdb_internal.ranges WHERE `+where).Scan(&count))
				if count == 0 {
					break
				}
				t.Status(fmt.Sprintf("moving %d ranges off of n%d (%s)", count, source, predicate))
				for _, target := range to {
					_, err = conn.ExecContext(ctx, `ALTER RANGE RELOCATE FROM $1::int TO $2::int FOR `+
						`SELECT range_id FROM crdb_internal.ranges WHERE `+where,
						source, target)
					require.NoError(t, err)
				}
				time.Sleep(time.Second)
			}
		}
	}
	relocateRanges(`database_name = 'kv'`, []int{1, 2, 3}, []int{4, 5, 6})

	// Start workload on n7, using n1-n3 as gateways. Run it for 10 minutes, since
	// we take ~1 minute to kill and restart each node, and we do 3 cycles of
	// killing the 3 nodes in order.
	t.Status("running workload")
	m := c.NewMonitor(ctx, c.Range(1, 6))
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, c.Node(7), `./cockroach workload run kv --read-percent 50 `+
			`--duration 600s --concurrency 256 --max-rate 2048 --timeout 30s --tolerate-errors `+
			`--histograms=`+t.PerfArtifactsDir()+`/stats.json `+
			`{pgurl:1-3}`)
		return nil
	})

	// Start a worker to kill and restart n4-n6 in order, for 3 cycles.
	m.Go(func(ctx context.Context) error {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for i := 0; i < 3; i++ {
			for _, node := range []int{4, 5, 6} {
				select {
				case <-ticker.C:
				case <-ctx.Done():
					return ctx.Err()
				}

				// Ranges may occasionally escape their constraints. Move them
				// to where they should be.
				relocateRanges(`database_name = 'kv'`, []int{1, 2, 3}, []int{4, 5, 6})
				relocateRanges(`database_name != 'kv'`, []int{node}, []int{1, 2, 3})

				t.Status(fmt.Sprintf("killing n%d", node))
				m.ExpectDeath()
				c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(node)) // uses SIGKILL

				select {
				case <-ticker.C:
				case <-ctx.Done():
					return ctx.Err()
				}
				t.Status(fmt.Sprintf("restarting n%d", node))
				c.Start(ctx, t.L(), opts, settings, c.Node(node))
			}
		}
		return nil
	})
	m.Wait()
}
