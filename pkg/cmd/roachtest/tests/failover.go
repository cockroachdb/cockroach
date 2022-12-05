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
	for _, failureMode := range []failureMode{
		&failureModeBlackhole{},
		&failureModeBlackholeRecv{},
		&failureModeBlackholeSend{},
		&failureModeCrash{},
	} {
		failureMode := failureMode // pin loop variable
		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("failover/non-system/%s", failureMode),
			Owner:   registry.OwnerKV,
			Timeout: 20 * time.Minute,
			Cluster: r.MakeClusterSpec(7, spec.CPU(4)),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runFailoverNonSystem(ctx, t, c, failureMode)
			},
		})
	}

	for _, failureMode := range []failureMode{
		&failureModeBlackhole{},
		&failureModeBlackholeRecv{},
		&failureModeBlackholeSend{},
		&failureModeCrash{},
	} {
		failureMode := failureMode // pin loop variable
		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("failover/system-non-liveness/%s", failureMode),
			Owner:   registry.OwnerKV,
			Timeout: 20 * time.Minute,
			Cluster: r.MakeClusterSpec(7, spec.CPU(4)),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runFailoverSystemNonLiveness(ctx, t, c, failureMode)
			},
		})
	}
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
// order, with 30 seconds between each operation, for 3 cycles totaling 9
// failures.
func runFailoverNonSystem(
	ctx context.Context, t test.Test, c cluster.Cluster, failureMode failureMode,
) {
	require.Equal(t, 7, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster.
	opts := option.DefaultStartOpts()
	settings := install.MakeClusterSettings()
	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), opts, settings, c.Range(1, 6))

	if f, ok := failureMode.(*failureModeCrash); ok {
		f.startOpts = opts
		f.startSettings = settings
	}

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
	relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 2, 3}, []int{4, 5, 6})

	// Start workload on n7, using n1-n3 as gateways. Run it for 10 minutes, since
	// we take ~1 minute to fail and recover each node, and we do 3 cycles of each
	// of the 3 nodes in order.
	t.Status("running workload")
	m := c.NewMonitor(ctx, c.Range(1, 6))
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, c.Node(7), `./cockroach workload run kv --read-percent 50 `+
			`--duration 600s --concurrency 256 --max-rate 2048 --timeout 30s --tolerate-errors `+
			`--histograms=`+t.PerfArtifactsDir()+`/stats.json `+
			`{pgurl:1-3}`)
		return nil
	})

	// Start a worker to fail and recover n4-n6 in order.
	defer failureMode.Cleanup(ctx, t, c)

	m.Go(func(ctx context.Context) error {
		var raftCfg base.RaftConfig
		raftCfg.SetDefaults()

		ticker := time.NewTicker(30 * time.Second)
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
				if failureMode.ExpectDeath() {
					m.ExpectDeath()
				}
				failureMode.Fail(ctx, t, c, node)

				select {
				case <-ticker.C:
				case <-ctx.Done():
					return ctx.Err()
				}

				t.Status(fmt.Sprintf("recovering n%d (%s)", node, failureMode))
				failureMode.Recover(ctx, t, c, node)
			}
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
// order, with 30 seconds between each operation, for 3 cycles totaling 9
// failures.
func runFailoverSystemNonLiveness(
	ctx context.Context, t test.Test, c cluster.Cluster, failureMode failureMode,
) {
	require.Equal(t, 7, c.Spec().NodeCount)
	require.False(t, c.IsLocal(), "test can't use local cluster") // messes with iptables

	rng, _ := randutil.NewTestRand()

	// Create cluster.
	opts := option.DefaultStartOpts()
	settings := install.MakeClusterSettings()
	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), opts, settings, c.Range(1, 6))

	if f, ok := failureMode.(*failureModeCrash); ok {
		f.startOpts = opts
		f.startSettings = settings
	}

	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	// Configure cluster. This test controls the ranges manually.
	t.Status("configuring cluster")
	_, err := conn.ExecContext(ctx, `SET CLUSTER SETTING kv.range_split.by_load_enabled = 'false'`)
	require.NoError(t, err)

	// Constrain all existing zone configs to n4-n6, except liveness which is
	// constrained to n1-n3.
	rows, err := conn.QueryContext(ctx, `SELECT target FROM [SHOW ALL ZONE CONFIGURATIONS]`)
	require.NoError(t, err)
	for rows.Next() {
		var target string
		require.NoError(t, rows.Scan(&target))
		_, err = conn.ExecContext(ctx, fmt.Sprintf(
			`ALTER %s CONFIGURE ZONE USING num_replicas = 3, constraints = '[-node1, -node2, -node3]'`,
			target))
		require.NoError(t, err)
	}
	require.NoError(t, rows.Err())

	_, err = conn.ExecContext(ctx, `ALTER RANGE liveness CONFIGURE ZONE USING `+
		`num_replicas = 3, constraints = '[-node4, -node5, -node6]'`)
	require.NoError(t, err)

	// Wait for upreplication.
	require.NoError(t, WaitFor3XReplication(ctx, t, conn))

	// Create the kv database, constrained to n1-n3. Despite the zone config, the
	// ranges will initially be distributed across all cluster nodes.
	t.Status("creating workload database")
	_, err = conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `ALTER DATABASE kv CONFIGURE ZONE USING `+
		`num_replicas = 3, constraints = '[-node4, -node5, -node6]'`)
	require.NoError(t, err)
	c.Run(ctx, c.Node(7), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	// The replicate queue takes forever to move the kv ranges from n4-n6 to
	// n1-n3, so we do it ourselves. Precreating the database/range and moving it
	// to the correct nodes first is not sufficient, since workload will spread
	// the ranges across all nodes regardless.
	relocateRanges(t, ctx, conn, `database_name = 'kv' OR range_id = 2`,
		[]int{4, 5, 6}, []int{1, 2, 3})
	relocateRanges(t, ctx, conn, `database_name != 'kv' AND range_id != 2`,
		[]int{1, 2, 3}, []int{4, 5, 6})

	// Start workload on n7, using n1-n3 as gateways. Run it for 10 minutes, since
	// we take ~1 minute to fail and recover each node, and we do 3 cycles of each
	// of the 3 nodes in order.
	t.Status("running workload")
	m := c.NewMonitor(ctx, c.Range(1, 6))
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, c.Node(7), `./cockroach workload run kv --read-percent 50 `+
			`--duration 600s --concurrency 256 --max-rate 2048 --timeout 30s --tolerate-errors `+
			`--histograms=`+t.PerfArtifactsDir()+`/stats.json `+
			`{pgurl:1-3}`)
		return nil
	})

	// Start a worker to fail and recover n4-n6 in order.
	defer failureMode.Cleanup(ctx, t, c)

	m.Go(func(ctx context.Context) error {
		var raftCfg base.RaftConfig
		raftCfg.SetDefaults()

		ticker := time.NewTicker(30 * time.Second)
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
				if failureMode.ExpectDeath() {
					m.ExpectDeath()
				}
				failureMode.Fail(ctx, t, c, node)

				select {
				case <-ticker.C:
				case <-ctx.Done():
					return ctx.Err()
				}

				t.Status(fmt.Sprintf("recovering n%d (%s)", node, failureMode))
				failureMode.Recover(ctx, t, c, node)
			}
		}
		return nil
	})
	m.Wait()
}

// failureMode fails and recovers a given node in some particular way.
type failureMode interface {
	fmt.Stringer

	// Fail fails the given node.
	Fail(ctx context.Context, t test.Test, c cluster.Cluster, nodeID int)

	// Recover recovers the given node.
	Recover(ctx context.Context, t test.Test, c cluster.Cluster, nodeID int)

	// Cleanup cleans up when the test exits. This is needed e.g. when the cluster
	// is reused by a different test.
	Cleanup(ctx context.Context, t test.Test, c cluster.Cluster)

	// ExpectDeath returns true if the node is expected to die on failure.
	ExpectDeath() bool
}

// failureModeCrash is a process crash where the TCP/IP stack remains responsive
// and sends immediate RST packets to peers.
type failureModeCrash struct {
	startOpts     option.StartOpts
	startSettings install.ClusterSettings
}

func (f *failureModeCrash) String() string    { return "crash" }
func (f *failureModeCrash) ExpectDeath() bool { return true }

func (f *failureModeCrash) Fail(ctx context.Context, t test.Test, c cluster.Cluster, nodeID int) {
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(nodeID)) // uses SIGKILL
}

func (f *failureModeCrash) Recover(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeID int,
) {
	c.Start(ctx, t.L(), f.startOpts, f.startSettings, c.Node(nodeID))
}

func (f *failureModeCrash) Cleanup(ctx context.Context, t test.Test, c cluster.Cluster) {
}

// failureModeBlackhole is a network outage where all inbound and outbound
// TCP/IP packets to/from port 26257 are dropped, causing network hangs and
// timeouts.
type failureModeBlackhole struct{}

func (f *failureModeBlackhole) String() string    { return "blackhole" }
func (f *failureModeBlackhole) ExpectDeath() bool { return false }

func (f *failureModeBlackhole) Fail(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeID int,
) {
	c.Run(ctx, c.Node(nodeID), `sudo iptables -A INPUT -m multiport -p tcp --ports 26257 -j DROP`)
	c.Run(ctx, c.Node(nodeID), `sudo iptables -A OUTPUT -m multiport -p tcp --ports 26257 -j DROP`)
}

func (f *failureModeBlackhole) Recover(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeID int,
) {
	c.Run(ctx, c.Node(nodeID), `sudo iptables -F`)
}

func (f *failureModeBlackhole) Cleanup(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Run(ctx, c.All(), `sudo iptables -F`)
}

// failureModeBlackholeRecv is an asymmetric network outage where all inbound
// TCP/IP packets to port 26257 are dropped, causing network hangs and timeouts.
// The node can still send traffic on outbound connections.
type failureModeBlackholeRecv struct{}

func (f *failureModeBlackholeRecv) String() string    { return "blackhole-recv" }
func (f *failureModeBlackholeRecv) ExpectDeath() bool { return false }

func (f *failureModeBlackholeRecv) Fail(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeID int,
) {
	c.Run(ctx, c.Node(nodeID), `sudo iptables -A INPUT -p tcp --dport 26257 -j DROP`)
}

func (f *failureModeBlackholeRecv) Recover(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeID int,
) {
	c.Run(ctx, c.Node(nodeID), `sudo iptables -F`)
}

func (f *failureModeBlackholeRecv) Cleanup(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Run(ctx, c.All(), `sudo iptables -F`)
}

// failureModeBlackholeSend is an asymmetric network outage where all outbound
// TCP/IP packets to port 26257 are dropped, causing network hangs and
// timeouts. The node can still receive traffic on inbound connections.
type failureModeBlackholeSend struct{}

func (f *failureModeBlackholeSend) String() string    { return "blackhole-send" }
func (f *failureModeBlackholeSend) ExpectDeath() bool { return false }

func (f *failureModeBlackholeSend) Fail(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeID int,
) {
	c.Run(ctx, c.Node(nodeID), `sudo iptables -A OUTPUT -p tcp --dport 26257 -j DROP`)
}

func (f *failureModeBlackholeSend) Recover(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeID int,
) {
	c.Run(ctx, c.Node(nodeID), `sudo iptables -F`)
}

func (f *failureModeBlackholeSend) Cleanup(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Run(ctx, c.All(), `sudo iptables -F`)
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
				`SELECT count(*) FROM crdb_internal.ranges WHERE `+where).Scan(&count))
			if count == 0 {
				break
			}
			t.Status(fmt.Sprintf("moving %d ranges off of n%d (%s)", count, source, predicate))
			for _, target := range to {
				_, err := conn.ExecContext(ctx, `ALTER RANGE RELOCATE FROM $1::int TO $2::int FOR `+
					`SELECT range_id FROM crdb_internal.ranges WHERE `+where,
					source, target)
				require.NoError(t, err)
			}
			time.Sleep(time.Second)
		}
	}
}
