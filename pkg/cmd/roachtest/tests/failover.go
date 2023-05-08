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
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func registerFailover(r registry.Registry) {
	for _, expirationLeases := range []bool{false, true} {
		expirationLeases := expirationLeases // pin loop variable
		var suffix string
		if expirationLeases {
			suffix = "/lease=expiration"
		}

		r.Add(registry.TestSpec{
			Name:    "failover/partial/lease-liveness" + suffix,
			Owner:   registry.OwnerKV,
			Timeout: 30 * time.Minute,
			Cluster: r.MakeClusterSpec(6, spec.CPU(4)),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runDisconnect(ctx, t, c, expirationLeases)
			},
		})

		for _, failureMode := range []failureMode{
			failureModeBlackhole,
			failureModeBlackholeRecv,
			failureModeBlackholeSend,
			failureModeCrash,
			failureModeDiskStall,
			failureModePause,
		} {
			failureMode := failureMode // pin loop variable
			makeSpec := func(nNodes, nCPU int) spec.ClusterSpec {
				s := r.MakeClusterSpec(nNodes, spec.CPU(nCPU))
				if failureMode == failureModeDiskStall {
					// Use PDs in an attempt to work around flakes encountered when using
					// SSDs. See #97968.
					s.PreferLocalSSD = false
				}
				return s
			}
			r.Add(registry.TestSpec{
				Name:    fmt.Sprintf("failover/non-system/%s%s", failureMode, suffix),
				Owner:   registry.OwnerKV,
				Timeout: 30 * time.Minute,
				Cluster: makeSpec(7 /* nodes */, 4 /* cpus */),
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runFailoverNonSystem(ctx, t, c, failureMode, expirationLeases)
				},
			})
			r.Add(registry.TestSpec{
				Name:    fmt.Sprintf("failover/liveness/%s%s", failureMode, suffix),
				Owner:   registry.OwnerKV,
				Timeout: 30 * time.Minute,
				Cluster: makeSpec(5 /* nodes */, 4 /* cpus */),
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runFailoverLiveness(ctx, t, c, failureMode, expirationLeases)
				},
			})
			r.Add(registry.TestSpec{
				Name:    fmt.Sprintf("failover/system-non-liveness/%s%s", failureMode, suffix),
				Owner:   registry.OwnerKV,
				Timeout: 30 * time.Minute,
				Cluster: makeSpec(7 /* nodes */, 4 /* cpus */),
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runFailoverSystemNonLiveness(ctx, t, c, failureMode, expirationLeases)
				},
			})
		}
	}
}

func randSleep(ctx context.Context, rng *rand.Rand, max time.Duration) {
	randTimer := time.After(randutil.RandDuration(rng, max))
	select {
	case <-randTimer:
	case <-ctx.Done():
	}
}

// 5 nodes fully connected. Break the connection between a pair of nodes 4 and 5
// while running a workload against nodes 1 through 3. Before each disconnect,
// move all the leases to nodes 4 and 5 in a different pattern.
func runDisconnect(ctx context.Context, t test.Test, c cluster.Cluster, expLeases bool) {
	require.Equal(t, 6, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()
	// Create cluster.
	opts := option.DefaultStartOpts()
	settings := install.MakeClusterSettings()

	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), opts, settings, c.Range(1, 5))

	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	_, err := conn.ExecContext(ctx, `SET CLUSTER SETTING kv.expiration_leases_only.enabled = $1`,
		expLeases)
	require.NoError(t, err)

	constrainAllConfig(t, ctx, conn, 3, []int{4, 5}, 0)
	constrainConfig(t, ctx, conn, `RANGE liveness`, 3, []int{3, 5}, 4)
	// Wait for upreplication.
	require.NoError(t, WaitFor3XReplication(ctx, t, conn))

	t.Status("creating workload database")
	_, err = conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	constrainConfig(t, ctx, conn, `DATABASE kv`, 3, []int{2, 3, 5}, 0)

	c.Run(ctx, c.Node(6), `./cockroach workload init kv --splits 100 {pgurl:1}`)

	// Start workload on n6 using nodes 1-3 (not part of partition). We could
	// additionally test the behavior of running SQL against nodes 4-5 however
	// that complicates the analysis as we want to focus on KV behavior.
	t.Status("running workload")
	m := c.NewMonitor(ctx, c.Range(1, 3))
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, c.Node(6), `./cockroach workload run kv --read-percent 50 `+
			`--duration 10m --concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors `+
			`--histograms=`+t.PerfArtifactsDir()+`/stats.json `+
			`{pgurl:1-3}`)
		return nil
	})
	// Make sure we don't leave an outage if this test fails midway.
	defer Cleanup(t, c, ctx)

	// Start and stop partial between nodes 4 and 5 every 30 seconds.
	m.Go(func(ctx context.Context) error {
		var raftCfg base.RaftConfig
		raftCfg.SetDefaults()

		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		// All the system ranges will be on all the nodes, so they will all move,
		// plus many of the non-system ranges.
		for i := 0; i < 9; i++ {
			t.Status("Moving ranges to nodes 4 and 5 before partition", i)
			relocateLeases(t, ctx, conn, `range_id = 2`, 4)
			relocateLeases(t, ctx, conn, `voting_replicas @> ARRAY[5] AND range_id != 2`, 5)

			// Randomly sleep up to the lease renewal interval, to vary the time
			// between the last lease renewal and the failure.
			randSleep(ctx, rng, raftCfg.RangeLeaseRenewalDuration())

			t.Status("disconnecting n4 and n5")
			Disconnect(t, c, ctx, []int{4, 5})

			select {
			case <-ticker.C:
			case <-ctx.Done():
				return ctx.Err()
			}

			qps := measureQPS(ctx, t, conn, 5*time.Second)
			t.Status("Node 1 QPS after waiting is: ", qps, " recovering nodes")

			Cleanup(t, c, ctx)
		}
		return nil
	})
	m.Wait()
}

// runFailoverNonSystem benchmarks the maximum duration of range unavailability
// following a leaseholder failure with only non-system ranges.
//
//   - No system ranges located on the failed node.
//
//   - SQL clients do not connect to the failed node.
//
//   - The workload consists of individual point reads and writes.
//
// Since the lease unavailability is probabilistic, depending e.g. on the time
// since the last heartbeat and other variables, we run 9 failures and record
// the pMax latency to find the upper bound on unavailability. We expect this
// worst-case latency to be slightly larger than the lease interval (9s), to
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
// directed at n1-n3 with a rate of 2048 reqs/s. n4-n6 fail and recover in
// order, with 1 minute between each operation, for 3 cycles totaling 9
// failures.
func runFailoverNonSystem(
	ctx context.Context, t test.Test, c cluster.Cluster, failureMode failureMode, expLeases bool,
) {
	require.Equal(t, 7, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster.
	opts := option.DefaultStartOpts()
	settings := install.MakeClusterSettings()

	failer := makeFailer(t, c, failureMode, opts, settings)
	failer.Setup(ctx)
	defer failer.Cleanup(ctx)

	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), opts, settings, c.Range(1, 6))

	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	// Configure cluster. This test controls the ranges manually.
	t.Status("configuring cluster")
	_, err := conn.ExecContext(ctx, `SET CLUSTER SETTING kv.range_split.by_load_enabled = 'false'`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `SET CLUSTER SETTING kv.expiration_leases_only.enabled = $1`,
		expLeases)
	require.NoError(t, err)

	// Constrain all existing zone configs to n1-n3.
	constrainAllConfig(t, ctx, conn, 3, []int{4, 5, 6}, 0)

	// Wait for upreplication.
	require.NoError(t, WaitFor3XReplication(ctx, t, conn))

	// Create the kv database, constrained to n4-n6. Despite the zone config, the
	// ranges will initially be distributed across all cluster nodes.
	t.Status("creating workload database")
	_, err = conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	constrainConfig(t, ctx, conn, `DATABASE kv`, 3, []int{1, 2, 3}, 0)
	c.Run(ctx, c.Node(7), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	// The replicate queue takes forever to move the kv ranges from n1-n3 to
	// n4-n6, so we do it ourselves. Precreating the database/range and moving it
	// to the correct nodes first is not sufficient, since workload will spread
	// the ranges across all nodes regardless.
	relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 2, 3}, []int{4, 5, 6})

	// Start workload on n7, using n1-n3 as gateways. Run it for 20
	// minutes, since we take ~2 minutes to fail and recover each node, and
	// we do 3 cycles of each of the 3 nodes in order.
	t.Status("running workload")
	m := c.NewMonitor(ctx, c.Range(1, 6))
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, c.Node(7), `./cockroach workload run kv --read-percent 50 `+
			`--duration 20m --concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors `+
			`--histograms=`+t.PerfArtifactsDir()+`/stats.json `+
			`{pgurl:1-3}`)
		return nil
	})

	// Start a worker to fail and recover n4-n6 in order.
	failer.Ready(ctx, m)
	m.Go(func(ctx context.Context) error {
		var raftCfg base.RaftConfig
		raftCfg.SetDefaults()

		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for i := 0; i < 3; i++ {
			for _, node := range []int{4, 5, 6} {
				select {
				case <-ticker.C:
				case <-ctx.Done():
					return ctx.Err()
				}

				randTimer := time.After(randutil.RandDuration(rng, raftCfg.RangeLeaseRenewalDuration()))

				// Ranges may occasionally escape their constraints. Move them
				// to where they should be.
				relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 2, 3}, []int{4, 5, 6})
				relocateRanges(t, ctx, conn, `database_name != 'kv'`, []int{node}, []int{1, 2, 3})

				// Randomly sleep up to the lease renewal interval, to vary the time
				// between the last lease renewal and the failure. We start the timer
				// before the range relocation above to run them concurrently.
				select {
				case <-randTimer:
				case <-ctx.Done():
				}

				t.Status(fmt.Sprintf("failing n%d (%s)", node, failureMode))
				failer.Fail(ctx, node)

				select {
				case <-ticker.C:
				case <-ctx.Done():
					return ctx.Err()
				}

				t.Status(fmt.Sprintf("recovering n%d (%s)", node, failureMode))
				failer.Recover(ctx, node)
			}
		}
		return nil
	})
	m.Wait()
}

// runFailoverLiveness benchmarks the maximum duration of *user* range
// unavailability following a liveness-only leaseholder failure. When the
// liveness range becomes unavailable, other nodes are unable to heartbeat and
// extend their leases, and their leases may thus expire as well making them
// unavailable.
//
//   - Only liveness range located on the failed node, as leaseholder.
//
//   - SQL clients do not connect to the failed node.
//
//   - The workload consists of individual point reads and writes.
//
// Since the range unavailability is probabilistic, depending e.g. on the time
// since the last heartbeat and other variables, we run 9 failures and record
// the number of expired leases on n1-n3 as well as the pMax latency to find the
// upper bound on unavailability. We do not assert anything, but instead export
// metrics for graphing.
//
// The cluster layout is as follows:
//
// n1-n3: All ranges, including liveness.
// n4:    Liveness range leaseholder.
// n5:    Workload runner.
//
// The test runs a kv50 workload with batch size 1, using 256 concurrent workers
// directed at n1-n3 with a rate of 2048 reqs/s. n4 fails and recovers, with 1
// minute between each operation, for 9 cycles.
//
// TODO(erikgrinaker): The metrics resolution of 10 seconds isn't really good
// enough to accurately measure the number of invalid leases, but it's what we
// have currently. Prometheus scraping more often isn't enough, because CRDB
// itself only samples every 10 seconds.
func runFailoverLiveness(
	ctx context.Context, t test.Test, c cluster.Cluster, failureMode failureMode, expLeases bool,
) {
	require.Equal(t, 5, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster. Don't schedule a backup as this roachtest reports to roachperf.
	opts := option.DefaultStartOptsNoBackups()
	settings := install.MakeClusterSettings()

	failer := makeFailer(t, c, failureMode, opts, settings)
	failer.Setup(ctx)
	defer failer.Cleanup(ctx)

	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), opts, settings, c.Range(1, 4))

	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	// Configure cluster. This test controls the ranges manually.
	t.Status("configuring cluster")
	_, err := conn.ExecContext(ctx, `SET CLUSTER SETTING kv.range_split.by_load_enabled = 'false'`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `SET CLUSTER SETTING kv.expiration_leases_only.enabled = $1`,
		expLeases)
	require.NoError(t, err)

	// Constrain all existing zone configs to n1-n3.
	constrainAllConfig(t, ctx, conn, 3, []int{4}, 0)

	// Constrain the liveness range to n1-n4, with leaseholder preference on n4.
	constrainConfig(t, ctx, conn, `RANGE liveness`, 4, nil, 4)
	require.NoError(t, err)

	// Wait for upreplication.
	require.NoError(t, WaitFor3XReplication(ctx, t, conn))

	// Create the kv database, constrained to n1-n3. Despite the zone config, the
	// ranges will initially be distributed across all cluster nodes.
	t.Status("creating workload database")
	_, err = conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	constrainConfig(t, ctx, conn, `DATABASE kv`, 3, []int{4}, 0)
	c.Run(ctx, c.Node(5), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	// The replicate queue takes forever to move the other ranges off of n4 so we
	// do it ourselves. Precreating the database/range and moving it to the
	// correct nodes first is not sufficient, since workload will spread the
	// ranges across all nodes regardless.
	relocateRanges(t, ctx, conn, `range_id != 2`, []int{4}, []int{1, 2, 3})

	// We also make sure the lease is located on n4.
	relocateLeases(t, ctx, conn, `range_id = 2`, 4)

	// Start workload on n7, using n1-n3 as gateways. Run it for 20 minutes, since
	// we take ~2 minutes to fail and recover the node, and we do 9 cycles.
	t.Status("running workload")
	m := c.NewMonitor(ctx, c.Range(1, 4))
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, c.Node(5), `./cockroach workload run kv --read-percent 50 `+
			`--duration 20m --concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors `+
			`--histograms=`+t.PerfArtifactsDir()+`/stats.json `+
			`{pgurl:1-3}`)
		return nil
	})

	// Start a worker to fail and recover n4.
	failer.Ready(ctx, m)
	m.Go(func(ctx context.Context) error {
		var raftCfg base.RaftConfig
		raftCfg.SetDefaults()

		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for i := 0; i < 9; i++ {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return ctx.Err()
			}

			randTimer := time.After(randutil.RandDuration(rng, raftCfg.RangeLeaseRenewalDuration()))

			// Ranges and leases may occasionally escape their constraints. Move them
			// to where they should be.
			relocateRanges(t, ctx, conn, `range_id != 2`, []int{4}, []int{1, 2, 3})
			relocateLeases(t, ctx, conn, `range_id = 2`, 4)

			// Randomly sleep up to the lease renewal interval, to vary the time
			// between the last lease renewal and the failure. We start the timer
			// before the range relocation above to run them concurrently.
			select {
			case <-randTimer:
			case <-ctx.Done():
			}

			t.Status(fmt.Sprintf("failing n%d (%s)", 4, failureMode))
			failer.Fail(ctx, 4)

			select {
			case <-ticker.C:
			case <-ctx.Done():
				return ctx.Err()
			}

			t.Status(fmt.Sprintf("recovering n%d (%s)", 4, failureMode))
			failer.Recover(ctx, 4)
			relocateLeases(t, ctx, conn, `range_id = 2`, 4)
		}
		return nil
	})
	m.Wait()
}

// runFailoverSystemNonLiveness benchmarks the maximum duration of range
// unavailability following a leaseholder failure with only system ranges,
// excluding the liveness range which is tested separately in
// runFailoverLiveness.
//
//   - No user or liveness ranges located on the failed node.
//
//   - SQL clients do not connect to the failed node.
//
//   - The workload consists of individual point reads and writes.
//
// Since the lease unavailability is probabilistic, depending e.g. on the time
// since the last heartbeat and other variables, we run 9 failures and record
// the pMax latency to find the upper bound on unavailability. Ideally, losing
// the lease on these ranges should have no impact on the user traffic.
//
// The cluster layout is as follows:
//
// n1-n3: Workload ranges, liveness range, and SQL gateways.
// n4-n6: System ranges excluding liveness.
// n7:    Workload runner.
//
// The test runs a kv50 workload with batch size 1, using 256 concurrent workers
// directed at n1-n3 with a rate of 2048 reqs/s. n4-n6 fail and recover in
// order, with 1 minute between each operation, for 3 cycles totaling 9
// failures.
func runFailoverSystemNonLiveness(
	ctx context.Context, t test.Test, c cluster.Cluster, failureMode failureMode, expLeases bool,
) {
	require.Equal(t, 7, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster.
	opts := option.DefaultStartOpts()
	settings := install.MakeClusterSettings()

	failer := makeFailer(t, c, failureMode, opts, settings)
	failer.Setup(ctx)
	defer failer.Cleanup(ctx)

	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), opts, settings, c.Range(1, 6))

	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	// Configure cluster. This test controls the ranges manually.
	t.Status("configuring cluster")
	_, err := conn.ExecContext(ctx, `SET CLUSTER SETTING kv.range_split.by_load_enabled = 'false'`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `SET CLUSTER SETTING kv.expiration_leases_only.enabled = $1`,
		expLeases)
	require.NoError(t, err)

	// Constrain all existing zone configs to n4-n6, except liveness which is
	// constrained to n1-n3.
	constrainAllConfig(t, ctx, conn, 3, []int{1, 2, 3}, 0)
	constrainConfig(t, ctx, conn, `RANGE liveness`, 3, []int{4, 5, 6}, 0)
	require.NoError(t, err)

	// Wait for upreplication.
	require.NoError(t, WaitFor3XReplication(ctx, t, conn))

	// Create the kv database, constrained to n1-n3. Despite the zone config, the
	// ranges will initially be distributed across all cluster nodes.
	t.Status("creating workload database")
	_, err = conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	constrainConfig(t, ctx, conn, `DATABASE kv`, 3, []int{4, 5, 6}, 0)
	c.Run(ctx, c.Node(7), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	// The replicate queue takes forever to move the kv ranges from n4-n6 to
	// n1-n3, so we do it ourselves. Precreating the database/range and moving it
	// to the correct nodes first is not sufficient, since workload will spread
	// the ranges across all nodes regardless.
	relocateRanges(t, ctx, conn, `database_name = 'kv' OR range_id = 2`,
		[]int{4, 5, 6}, []int{1, 2, 3})
	relocateRanges(t, ctx, conn, `database_name != 'kv' AND range_id != 2`,
		[]int{1, 2, 3}, []int{4, 5, 6})

	// Start workload on n7, using n1-n3 as gateways. Run it for 20 minutes, since
	// we take ~2 minutes to fail and recover each node, and we do 3 cycles of each
	// of the 3 nodes in order.
	t.Status("running workload")
	m := c.NewMonitor(ctx, c.Range(1, 6))
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, c.Node(7), `./cockroach workload run kv --read-percent 50 `+
			`--duration 20m --concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors `+
			`--histograms=`+t.PerfArtifactsDir()+`/stats.json `+
			`{pgurl:1-3}`)
		return nil
	})

	// Start a worker to fail and recover n4-n6 in order.
	failer.Ready(ctx, m)
	m.Go(func(ctx context.Context) error {
		var raftCfg base.RaftConfig
		raftCfg.SetDefaults()

		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for i := 0; i < 3; i++ {
			for _, node := range []int{4, 5, 6} {
				select {
				case <-ticker.C:
				case <-ctx.Done():
					return ctx.Err()
				}

				randTimer := time.After(randutil.RandDuration(rng, raftCfg.RangeLeaseRenewalDuration()))

				// Ranges may occasionally escape their constraints. Move them
				// to where they should be.
				relocateRanges(t, ctx, conn, `database_name != 'kv' AND range_id != 2`,
					[]int{1, 2, 3}, []int{4, 5, 6})
				relocateRanges(t, ctx, conn, `database_name = 'kv' OR range_id = 2`,
					[]int{4, 5, 6}, []int{1, 2, 3})

				// Randomly sleep up to the lease renewal interval, to vary the time
				// between the last lease renewal and the failure. We start the timer
				// before the range relocation above to run them concurrently.
				select {
				case <-randTimer:
				case <-ctx.Done():
				}

				t.Status(fmt.Sprintf("failing n%d (%s)", node, failureMode))
				failer.Fail(ctx, node)

				select {
				case <-ticker.C:
				case <-ctx.Done():
					return ctx.Err()
				}

				t.Status(fmt.Sprintf("recovering n%d (%s)", node, failureMode))
				failer.Recover(ctx, node)
			}
		}
		return nil
	})
	m.Wait()
}

// failureMode specifies a failure mode.
type failureMode string

const (
	failureModeBlackhole     failureMode = "blackhole"
	failureModeBlackholeRecv failureMode = "blackhole-recv"
	failureModeBlackholeSend failureMode = "blackhole-send"
	failureModeCrash         failureMode = "crash"
	failureModeDiskStall     failureMode = "disk-stall"
	failureModePause         failureMode = "pause"
)

// makeFailer creates a new failer for the given failureMode.
func makeFailer(
	t test.Test,
	c cluster.Cluster,
	failureMode failureMode,
	opts option.StartOpts,
	settings install.ClusterSettings,
) failer {
	switch failureMode {
	case failureModeBlackhole:
		return &blackholeFailer{
			t:      t,
			c:      c,
			input:  true,
			output: true,
		}
	case failureModeBlackholeRecv:
		return &blackholeFailer{
			t:     t,
			c:     c,
			input: true,
		}
	case failureModeBlackholeSend:
		return &blackholeFailer{
			t:      t,
			c:      c,
			output: true,
		}
	case failureModeCrash:
		return &crashFailer{
			t:             t,
			c:             c,
			startOpts:     opts,
			startSettings: settings,
		}
	case failureModeDiskStall:
		// TODO(baptist): This mode doesn't work on local clusters since
		// dmsetupDiskStaller does not support local clusters. Either support could
		// be added for it or there could be a flag to not fatal when run in local
		// mode. The net impact is that this failure can't be simulated on local
		// clusters today.
		return &diskStallFailer{
			t:             t,
			c:             c,
			startOpts:     opts,
			startSettings: settings,
			staller:       &dmsetupDiskStaller{t: t, c: c},
		}
	case failureModePause:
		return &pauseFailer{
			t: t,
			c: c,
		}
	default:
		t.Fatalf("unknown failure mode %s", failureMode)
		return nil
	}
}

// Disconnect takes a set of nodes and each nodes internal ips. It disconnects
// each node from all the others in the list.
func Disconnect(t test.Test, c cluster.Cluster, ctx context.Context, nodes []int) {
	if c.IsLocal() {
		t.Status("skipping iptables disconnect on local cluster")
		return
	}

	ips, err := c.InternalIP(ctx, t.L(), nodes)
	require.NoError(t, err)

	// disconnect each node from every other passed in node.
	for n := 0; n < len(nodes); n++ {
		for ip := 0; ip < len(ips); ip++ {
			if n != ip {
				c.Run(ctx, c.Node(nodes[n]), `sudo iptables -A INPUT -s `+ips[ip]+` -j DROP`)
				c.Run(ctx, c.Node(nodes[n]), `sudo iptables -A OUTPUT -d `+ips[ip]+` -j DROP`)
			}
		}
	}
}

// Cleanup takes a set of nodes and each nodes internal ips. It disconnects
// each node from all the others in the list.
func Cleanup(t test.Test, c cluster.Cluster, ctx context.Context) {
	if c.IsLocal() {
		t.Status("skipping iptables cleanup on local cluster")
		return
	}
	c.Run(ctx, c.All(), `sudo iptables -F`)
}

// failer fails and recovers a given node in some particular way.
type failer interface {
	// Setup prepares the failer. It is called before the cluster is started.
	Setup(ctx context.Context)

	// Ready is called when the cluster is ready, with a running workload.
	Ready(ctx context.Context, m cluster.Monitor)

	// Cleanup cleans up when the test exits. This is needed e.g. when the cluster
	// is reused by a different test.
	Cleanup(ctx context.Context)

	// Fail fails the given node.
	Fail(ctx context.Context, nodeID int)

	// Recover recovers the given node.
	Recover(ctx context.Context, nodeID int)
}

// blackholeFailer causes a network failure where TCP/IP packets to/from port
// 26257 are dropped, causing network hangs and timeouts.
//
// If only one if input or output are enabled, connections in that direction
// will fail (even already established connections), but connections in the
// other direction are still functional (including responses).
type blackholeFailer struct {
	t      test.Test
	c      cluster.Cluster
	input  bool
	output bool
}

func (f *blackholeFailer) Setup(_ context.Context)                    {}
func (f *blackholeFailer) Ready(_ context.Context, _ cluster.Monitor) {}

func (f *blackholeFailer) Cleanup(ctx context.Context) {
	if f.c.IsLocal() {
		f.t.Status("skipping iptables cleanup on local cluster")
		return
	}
	f.c.Run(ctx, f.c.All(), `sudo iptables -F`)
}

func (f *blackholeFailer) Fail(ctx context.Context, nodeID int) {
	if f.c.IsLocal() {
		f.t.Status("skipping fail on local cluster")
		return
	}
	// When dropping both input and output, we use multiport to block traffic both
	// to port 26257 and from port 26257 on either side of the connection, to
	// avoid any spurious packets from making it through.
	//
	// We don't do this when only blocking in one direction, because e.g. in the
	// input case we don't want inbound connections to work (INPUT to 26257), but
	// we do want responses for outbound connections to work (INPUT from 26257).
	if f.input && f.output {
		f.c.Run(ctx, f.c.Node(nodeID),
			`sudo iptables -A INPUT -m multiport -p tcp --ports 26257 -j DROP`)
		f.c.Run(ctx, f.c.Node(nodeID),
			`sudo iptables -A OUTPUT -m multiport -p tcp --ports 26257 -j DROP`)
	} else if f.input {
		f.c.Run(ctx, f.c.Node(nodeID), `sudo iptables -A INPUT -p tcp --dport 26257 -j DROP`)
	} else if f.output {
		f.c.Run(ctx, f.c.Node(nodeID), `sudo iptables -A OUTPUT -p tcp --dport 26257 -j DROP`)
	}
}

func (f *blackholeFailer) Recover(ctx context.Context, nodeID int) {
	if f.c.IsLocal() {
		f.t.Status("skipping iptables recover on local cluster")
		return
	}
	f.c.Run(ctx, f.c.Node(nodeID), `sudo iptables -F`)
}

// crashFailer is a process crash where the TCP/IP stack remains responsive
// and sends immediate RST packets to peers.
type crashFailer struct {
	t             test.Test
	c             cluster.Cluster
	m             cluster.Monitor
	startOpts     option.StartOpts
	startSettings install.ClusterSettings
}

func (f *crashFailer) Setup(_ context.Context)                    {}
func (f *crashFailer) Ready(_ context.Context, m cluster.Monitor) { f.m = m }
func (f *crashFailer) Cleanup(_ context.Context)                  {}

func (f *crashFailer) Fail(ctx context.Context, nodeID int) {
	f.m.ExpectDeath()
	f.c.Stop(ctx, f.t.L(), option.DefaultStopOpts(), f.c.Node(nodeID)) // uses SIGKILL
}

func (f *crashFailer) Recover(ctx context.Context, nodeID int) {
	f.c.Start(ctx, f.t.L(), f.startOpts, f.startSettings, f.c.Node(nodeID))
}

// diskStallFailer stalls the disk indefinitely. This should cause the node to
// eventually self-terminate, but we'd want leases to move off before then.
type diskStallFailer struct {
	t             test.Test
	c             cluster.Cluster
	m             cluster.Monitor
	startOpts     option.StartOpts
	startSettings install.ClusterSettings
	staller       diskStaller
}

func (f *diskStallFailer) Setup(ctx context.Context) {
	f.staller.Setup(ctx)
}

func (f *diskStallFailer) Ready(_ context.Context, m cluster.Monitor) {
	f.m = m
}

func (f *diskStallFailer) Cleanup(ctx context.Context) {
	f.staller.Unstall(ctx, f.c.All())
	// We have to stop the cluster before cleaning up the staller.
	f.m.ExpectDeaths(int32(f.c.Spec().NodeCount))
	f.c.Stop(ctx, f.t.L(), option.DefaultStopOpts(), f.c.All())
	f.staller.Cleanup(ctx)
}

func (f *diskStallFailer) Fail(ctx context.Context, nodeID int) {
	// Pebble's disk stall detector should crash the node.
	f.m.ExpectDeath()
	f.staller.Stall(ctx, f.c.Node(nodeID))
}

func (f *diskStallFailer) Recover(ctx context.Context, nodeID int) {
	f.staller.Unstall(ctx, f.c.Node(nodeID))
	// Pebble's disk stall detector should have terminated the node, but in case
	// it didn't, we explicitly stop it first.
	f.c.Stop(ctx, f.t.L(), option.DefaultStopOpts(), f.c.Node(nodeID))
	f.c.Start(ctx, f.t.L(), f.startOpts, f.startSettings, f.c.Node(nodeID))
}

// pauseFailer pauses the process, but keeps the OS (and thus network
// connections) alive.
type pauseFailer struct {
	t test.Test
	c cluster.Cluster
}

func (f *pauseFailer) Setup(ctx context.Context)   {}
func (f *pauseFailer) Cleanup(ctx context.Context) {}

func (f *pauseFailer) Ready(ctx context.Context, m cluster.Monitor) {
	// The process pause can trip the disk stall detector, so we disable it.
	conn := f.c.Conn(ctx, f.t.L(), 1)
	_, err := conn.ExecContext(ctx, `SET CLUSTER SETTING storage.max_sync_duration.fatal.enabled = false`)
	require.NoError(f.t, err)
}

func (f *pauseFailer) Fail(ctx context.Context, nodeID int) {
	f.c.Signal(ctx, f.t.L(), 19, f.c.Node(nodeID)) // SIGSTOP
}

func (f *pauseFailer) Recover(ctx context.Context, nodeID int) {
	f.c.Signal(ctx, f.t.L(), 18, f.c.Node(nodeID)) // SIGCONT
}

// relocateRanges relocates all ranges matching the given predicate from a set
// of nodes to a different set of nodes. Moves are attempted sequentially from
// each source onto each target, and errors are retried indefinitely.
func relocateRanges(
	t test.Test, ctx context.Context, conn *gosql.DB, predicate string, from, to []int,
) {
	require.NotEmpty(t, predicate)
	var count int
	for _, source := range from {
		where := fmt.Sprintf("(%s) AND %d = ANY(replicas)", predicate, source)
		for {
			require.NoError(t, conn.QueryRowContext(ctx,
				`SELECT count(distinct range_id) FROM [SHOW CLUSTER RANGES WITH TABLES] WHERE `+where).
				Scan(&count))
			if count == 0 {
				break
			}
			t.Status(fmt.Sprintf("moving %d ranges off of n%d (%s)", count, source, predicate))
			for _, target := range to {
				_, err := conn.ExecContext(ctx, `ALTER RANGE RELOCATE FROM $1::int TO $2::int FOR `+
					`SELECT DISTINCT range_id FROM [SHOW CLUSTER RANGES WITH TABLES] WHERE `+where,
					source, target)
				if err != nil {
					t.Status(fmt.Sprintf("failed to move ranges: %s", err))
				}
			}
			time.Sleep(time.Second)
		}
	}
}

// relocateLeases relocates all leases matching the given predicate to the
// given node. Errors and failures are retried indefinitely.
func relocateLeases(t test.Test, ctx context.Context, conn *gosql.DB, predicate string, to int) {
	require.NotEmpty(t, predicate)
	var count int
	where := fmt.Sprintf("%s AND lease_holder != %d", predicate, to)
	for {
		require.NoError(t, conn.QueryRowContext(ctx,
			`SELECT count(distinct range_id) FROM [SHOW CLUSTER RANGES WITH TABLES, DETAILS] WHERE `+
				where).
			Scan(&count))
		if count == 0 {
			break
		}
		t.Status(fmt.Sprintf("moving %d leases to n%d (%s)", count, to, predicate))
		_, err := conn.ExecContext(ctx, `ALTER RANGE RELOCATE LEASE TO $1::int FOR `+
			`SELECT DISTINCT range_id FROM [SHOW CLUSTER RANGES WITH TABLES, DETAILS] WHERE `+where, to)
		if err != nil {
			t.Status(fmt.Sprintf("failed to move leases: %s", err))
		}
		time.Sleep(time.Second)
	}
}

// constrainConfig will alter the zone config for the target to specify the
// number of nodes the target can be on, the replicas it is prevented from being
// on and an optional leaseholder.
func constrainConfig(
	t test.Test,
	ctx context.Context,
	conn *gosql.DB,
	target string,
	numNodes int,
	constrainedReplicas []int,
	lease int,
) {
	replica := make([]string, len(constrainedReplicas))
	for i, n := range constrainedReplicas {
		replica[i] = fmt.Sprintf("-node%d", n)
	}
	replicaStr := fmt.Sprintf(`'[%s]'`, strings.Join(replica, ","))

	leaseStr := ""
	if lease > 0 {
		leaseStr = fmt.Sprintf(`[+node%d]`, lease)
	}

	str :=
		fmt.Sprintf(
			`ALTER %s CONFIGURE ZONE USING num_replicas = %d, constraints = %s, lease_preferences = '[%s]'`,
			target, numNodes, replicaStr, leaseStr)
	_, err := conn.ExecContext(ctx, str)
	t.Status(str)
	require.NoError(t, err)
}

// constrainAllConfig will alter the zone config for all zone configurations to
// specify the number of nodes the target can be on, the replicas it is
// prevented from being on and an optional leaseholder.
func constrainAllConfig(
	t test.Test, ctx context.Context, conn *gosql.DB, numNodes int, replicas []int, lease int,
) {
	rows, err := conn.QueryContext(ctx, `SELECT target FROM [SHOW ALL ZONE CONFIGURATIONS]`)
	require.NoError(t, err)

	for rows.Next() {
		var target string
		require.NoError(t, rows.Scan(&target))
		constrainConfig(t, ctx, conn, target, numNodes, replicas, lease)
	}
	require.NoError(t, rows.Err())
}
