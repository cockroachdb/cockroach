// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

var rangeLeaseRenewalDuration = func() time.Duration {
	var raftCfg base.RaftConfig
	raftCfg.SetDefaults()
	return raftCfg.RangeLeaseRenewalDuration()
}()

// registerFailover registers a set of failover benchmarks. These tests
// benchmark the maximum unavailability experienced by clients during various
// node failures, and exports them for roachperf graphing. They do not make any
// assertions on recovery time, similarly to other performance benchmarks.
//
// The tests run a kv workload against a cluster while subjecting individual
// nodes to various failures. The workload uses dedicated SQL gateway nodes that
// don't fail, relying on these for error handling and retries. The pMax latency
// seen by any client is exported and graphed. Since recovery times are
// probabilistic, each test performs multiple failures (typically 9) in order to
// find the worst-case recovery time following a failure.
//
// Failures last for 60 seconds before the node is recovered. Thus, the maximum
// measured unavailability is 60 seconds, which in practice means permanent
// unavailability (for some or all clients).
//
// No attempt is made to find the distribution of recovery times (e.g. the
// minimum and median), since this requires more sophisticated data recording
// and analysis. Simply taking the median across all requests is not sufficient,
// since requests are also executed against a healthy cluster between failures,
// and against healthy replicas during failures, thus the vast majority of
// requests are successful with nominal latencies. See also:
// https://github.com/cockroachdb/cockroach/issues/103654
func registerFailover(r registry.Registry) {
	for _, leases := range registry.LeaseTypes {
		var leasesStr string
		switch leases {
		case registry.EpochLeases:
			// TODO(nvanbenschoten): when leader leases become the default, we should
			// change this to "/lease=epoch" and change leader leases to "".
			leasesStr = ""
		case registry.ExpirationLeases:
			leasesStr = "/lease=expiration"
		case registry.LeaderLeases:
			leasesStr = "/lease=leader"
		default:
			panic(errors.AssertionFailedf("unknown lease type: %v", leases))
		}

		for _, readOnly := range []bool{false, true} {
			var readOnlyStr string
			if readOnly {
				readOnlyStr = "/read-only"
			} else {
				readOnlyStr = "/read-write"
			}

			r.Add(registry.TestSpec{
				Name:                "failover/chaos" + readOnlyStr + leasesStr,
				Owner:               registry.OwnerKV,
				Benchmark:           true,
				Timeout:             90 * time.Minute,
				Cluster:             r.MakeClusterSpec(10, spec.CPU(2), spec.WorkloadNode(), spec.WorkloadNodeCPU(2), spec.DisableLocalSSD(), spec.ReuseNone()), // uses disk stalls
				CompatibleClouds:    registry.OnlyGCE,                                                                                                           // dmsetup only configured for gce
				Suites:              registry.Suites(registry.Nightly),
				Leases:              leases,
				SkipPostValidations: registry.PostValidationNoDeadNodes, // cleanup kills nodes
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runFailoverChaos(ctx, t, c, readOnly)
				},
			})
		}

		r.Add(registry.TestSpec{
			Name:             "failover/partial/lease-gateway" + leasesStr,
			Owner:            registry.OwnerKV,
			Benchmark:        true,
			Timeout:          45 * time.Minute,
			Cluster:          r.MakeClusterSpec(8, spec.CPU(2), spec.WorkloadNode(), spec.WorkloadNodeCPU(2)),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           leases,
			Run:              runFailoverPartialLeaseGateway,
		})

		r.Add(registry.TestSpec{
			Name:             "failover/partial/lease-leader" + leasesStr,
			Owner:            registry.OwnerKV,
			Benchmark:        true,
			Timeout:          45 * time.Minute,
			Cluster:          r.MakeClusterSpec(7, spec.CPU(2), spec.WorkloadNode(), spec.WorkloadNodeCPU(2)),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           leases,
			Run:              runFailoverPartialLeaseLeader,
		})

		r.Add(registry.TestSpec{
			Name:             "failover/partial/lease-liveness" + leasesStr,
			Owner:            registry.OwnerKV,
			Benchmark:        true,
			Timeout:          45 * time.Minute,
			Cluster:          r.MakeClusterSpec(8, spec.CPU(2), spec.WorkloadNode(), spec.WorkloadNodeCPU(2)),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           leases,
			Run:              runFailoverPartialLeaseLiveness,
		})

		for _, failureMode := range allFailureModes {
			clusterOpts := make([]spec.Option, 0)
			clusterOpts = append(clusterOpts, spec.CPU(2), spec.WorkloadNode(), spec.WorkloadNodeCPU(2))
			clouds := registry.AllExceptAWS

			var postValidation registry.PostValidation
			if failureMode == failureModeDiskStall {
				// Use PDs in an attempt to work around flakes encountered when using
				// SSDs. See #97968.
				clusterOpts = append(clusterOpts, spec.DisableLocalSSD())
				// Don't reuse the cluster for tests that call dmsetup to avoid
				// spurious flakes from previous runs. See #107865
				clusterOpts = append(clusterOpts, spec.ReuseNone())
				postValidation = registry.PostValidationNoDeadNodes
				// dmsetup is currently only configured for gce.
				clouds = registry.OnlyGCE
			}
			r.Add(registry.TestSpec{
				Name:                fmt.Sprintf("failover/non-system/%s%s", failureMode, leasesStr),
				Owner:               registry.OwnerKV,
				Benchmark:           true,
				Timeout:             45 * time.Minute,
				SkipPostValidations: postValidation,
				Cluster:             r.MakeClusterSpec(7, clusterOpts...),
				CompatibleClouds:    clouds,
				Suites:              registry.Suites(registry.Nightly),
				Leases:              leases,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runFailoverNonSystem(ctx, t, c, failureMode)
				},
			})
			r.Add(registry.TestSpec{
				Name:                fmt.Sprintf("failover/liveness/%s%s", failureMode, leasesStr),
				Owner:               registry.OwnerKV,
				CompatibleClouds:    registry.AllExceptAWS,
				Suites:              registry.Suites(registry.Weekly),
				Benchmark:           true,
				Timeout:             45 * time.Minute,
				SkipPostValidations: postValidation,
				Cluster:             r.MakeClusterSpec(5, clusterOpts...),
				Leases:              leases,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runFailoverLiveness(ctx, t, c, failureMode)
				},
			})
			r.Add(registry.TestSpec{
				Name:                fmt.Sprintf("failover/system-non-liveness/%s%s", failureMode, leasesStr),
				Owner:               registry.OwnerKV,
				CompatibleClouds:    registry.AllExceptAWS,
				Suites:              registry.Suites(registry.Weekly),
				Benchmark:           true,
				Timeout:             45 * time.Minute,
				SkipPostValidations: postValidation,
				Cluster:             r.MakeClusterSpec(7, clusterOpts...),
				Leases:              leases,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runFailoverSystemNonLiveness(ctx, t, c, failureMode)
				},
			})
		}
	}
}

// runFailoverChaos sets up a 9-node cluster with RF=5 and randomly scattered
// ranges and replicas, and then runs a random failure on one or two random
// nodes for 1 minute with 1 minute recovery, for 20 cycles total. Nodes n1-n2
// are used as SQL gateways, and are not failed to avoid disconnecting the
// client workload.
//
// It runs with either a read-write or read-only KV workload, measuring the pMax
// unavailability for graphing. The read-only workload is useful to test e.g.
// recovering nodes stealing Raft leadership away, since this requires the
// replica to still be up-to-date on the log.
func runFailoverChaos(ctx context.Context, t test.Test, c cluster.Cluster, readOnly bool) {
	require.Equal(t, 10, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster, and set up failers for all failure modes.
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_ENABLE_UNSAFE_TEST_BUILTINS=true")
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=100ms") // speed up replication

	// DistSender circuit breakers are useful for these chaos tests. Turn them on.
	// TODO(arul): this can be removed if/when we turn on DistSender circuit
	// breakers for all ranges by default.
	settings.ClusterSettings["kv.dist_sender.circuit_breakers.mode"] = "all ranges"

	m := c.NewMonitor(ctx, c.CRDBNodes())

	failers := []Failer{}
	for _, failureMode := range allFailureModes {
		failer := makeFailerWithoutLocalNoop(t, c, m, failureMode, settings, rng)
		if c.IsLocal() && !failer.CanUseLocal() {
			t.L().Printf("skipping failure mode %q on local cluster", failureMode)
			continue
		}
		if !failer.CanUseChaos() {
			t.L().Printf("skipping failure mode %q in chaos test", failureMode)
			continue
		}
		failer.Setup(ctx)
		//nolint:deferloop TODO(#137605)
		defer failer.Cleanup(ctx)
		failers = append(failers, failer)
	}

	c.Start(ctx, t.L(), failoverStartOpts(), settings, c.CRDBNodes())

	conn := c.Conn(ctx, t.L(), 1)

	// Place 5 replicas of all ranges on n3-n9, keeping n1-n2 as SQL gateways.
	configureAllZones(t, ctx, conn, zoneConfig{replicas: 5, onlyNodes: []int{3, 4, 5, 6, 7, 8, 9}})

	// Wait for upreplication.
	require.NoError(
		t, roachtestutil.WaitForReplication(ctx, t.L(), conn, 5 /* replicationFactor */, roachtestutil.AtLeastReplicationFactor),
	)

	// Create the kv database. If this is a read-only workload, populate it with
	// 100.000 keys.
	var insertCount int
	if readOnly {
		insertCount = 100000
	}
	t.L().Printf("creating workload database")
	_, err := conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf(
		`./cockroach workload init kv --splits 1000 --insert-count %d {pgurl:1}`, insertCount))

	// Scatter the ranges, then relocate them off of the SQL gateways n1-n2.
	t.L().Printf("scattering table")
	_, err = conn.ExecContext(ctx, `ALTER TABLE kv.kv SCATTER`)
	require.NoError(t, err)
	relocateRanges(t, ctx, conn, `true`, []int{1, 2}, []int{3, 4, 5, 6, 7, 8, 9})

	// Wait for upreplication of the new ranges.
	require.NoError(
		t, roachtestutil.WaitForReplication(ctx, t.L(), conn, 5 /* replicationFactor */, roachtestutil.AtLeastReplicationFactor),
	)

	// Run workload on n10 via n1-n2 gateways until test ends (context cancels).
	t.L().Printf("running workload")
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		readPercent := 50
		if readOnly {
			readPercent = 100
		}

		err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf(
			`./cockroach workload run kv --read-percent %d --write-seq R%d `+
				`--concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors %s {pgurl:1-2}`, readPercent, insertCount,
			roachtestutil.GetWorkloadHistogramArgs(t, c, getKVLabels(256, insertCount, readPercent))))
		if ctx.Err() != nil {
			return nil // test requested workload shutdown
		}
		return err
	})

	// Start a worker to randomly fail random nodes for 1 minute, with 20 cycles.
	m.Go(func(ctx context.Context) error {
		defer cancelWorkload()

		for i := 0; i < 20; i++ {
			t.L().Printf("chaos iteration %d", i)
			sleepFor(ctx, t, time.Minute)

			// Ranges may occasionally escape their constraints. Move them to where
			// they should be.
			relocateRanges(t, ctx, conn, `true`, []int{1, 2}, []int{3, 4, 5, 6, 7, 8, 9})

			// Randomly sleep up to the lease renewal interval, to vary the time
			// between the last lease renewal and the failure.
			sleepFor(ctx, t, randutil.RandDuration(rng, rangeLeaseRenewalDuration))

			// Pick 1 or 2 random nodes and failure modes. Make sure we call Ready()
			// on both before failing, since we may need to fetch information from the
			// cluster which won't work if there's an active failure.
			nodeFailers := map[int]Failer{}
			for numNodes := 1 + rng.Intn(2); len(nodeFailers) < numNodes; {
				var node int
				for node == 0 || nodeFailers[node] != nil {
					node = 3 + rng.Intn(7) // n1-n2 are SQL gateways, n10 is workload runner
				}
				var failer Failer
				for failer == nil {
					failer = failers[rng.Intn(len(failers))]
					for _, other := range nodeFailers {
						if !other.CanRunWith(failer.Mode()) || !failer.CanRunWith(other.Mode()) {
							failer = nil // failers aren't compatible, pick a different one
							break
						}
					}
				}
				if d, ok := failer.(*deadlockFailer); ok { // randomize deadlockFailer
					d.numReplicas = 1 + rng.Intn(5)
					d.onlyLeaseholders = rng.Float64() < 0.5
				}
				failer.Ready(ctx, node)
				nodeFailers[node] = failer
			}

			for node, failer := range nodeFailers {
				// If the failer supports partial failures (e.g. partial partitions), do
				// one with 50% probability against a random node (including SQL
				// gateways).
				if partialFailer, ok := failer.(PartialFailer); ok && rng.Float64() < 0.5 {
					var partialPeer int
					for partialPeer == 0 || partialPeer == node {
						partialPeer = 1 + rng.Intn(9)
					}
					t.L().Printf("failing n%d to n%d (%s)", node, partialPeer, failer)
					partialFailer.FailPartial(ctx, node, []int{partialPeer})
				} else {
					t.L().Printf("failing n%d (%s)", node, failer)
					failer.Fail(ctx, node)
				}
			}

			sleepFor(ctx, t, time.Minute)

			// Recover the failers on different goroutines. Otherwise, they
			// might interact as certain failures can prevent other failures
			// from recovering.
			var wg sync.WaitGroup
			for node, failer := range nodeFailers {
				wg.Add(1)
				node := node
				failer := failer
				m.Go(func(ctx context.Context) error {
					defer wg.Done()
					t.L().Printf("recovering n%d (%s)", node, failer)
					failer.Recover(ctx, node)

					return nil
				})
			}
			wg.Wait()
		}

		sleepFor(ctx, t, time.Minute) // let cluster recover
		return nil
	})
	m.Wait()
}

// runFailoverPartialLeaseGateway tests a partial network partition between a
// SQL gateway and a user range leaseholder. These must be routed via other
// nodes to be able to serve the request.
//
// Cluster topology:
//
// n1-n3: system ranges and user ranges (2/5 replicas)
// n4-n5: user range leaseholders (2/5 replicas)
// n6-n7: SQL gateways and 1 user replica (1/5 replicas)
//
// 5 user range replicas will be placed on n2-n6, with leases on n4. A partial
// partition will be introduced between n4,n5 and n6,n7, both fully and
// individually. This corresponds to the case where we have three data centers
// with a broken network link between one pair. For example:
//
//	                        n1-n3 (2 replicas, liveness)
//	                          A
//	                        /   \
//	                       /     \
//	               n4-n5  B --x-- C  n6-n7  <---  n8 (workload)
//	(2 replicas, leases)             (1 replica, SQL gateways)
//
// Once follower routing is implemented, this tests the following scenarios:
//
// - Routes via followers in both A, B, and C when possible.
// - Skips follower replica on local node that can't reach leaseholder (n6).
// - Skips follower replica in C that can't reach leaseholder (n7 via n6).
// - Skips follower replica in B that's unreachable (n5).
//
// We run a kv50 workload on SQL gateways and collect pMax latency for graphing.
func runFailoverPartialLeaseGateway(ctx context.Context, t test.Test, c cluster.Cluster) {
	require.Equal(t, 8, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster.
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=100ms") // speed up replication

	m := c.NewMonitor(ctx, c.CRDBNodes())

	failer := makeFailer(t, c, m, failureModeBlackhole, settings, rng).(PartialFailer)
	failer.Setup(ctx)
	defer failer.Cleanup(ctx)

	c.Start(ctx, t.L(), failoverStartOpts(), settings, c.CRDBNodes())

	conn := c.Conn(ctx, t.L(), 1)

	// Place all ranges on n1-n3 to start with.
	configureAllZones(t, ctx, conn, zoneConfig{replicas: 3, onlyNodes: []int{1, 2, 3}})

	// Wait for upreplication.
	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), conn))

	// Create the kv database with 5 replicas on n2-n6, and leases on n4.
	t.L().Printf("creating workload database")
	_, err := conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	configureZone(t, ctx, conn, `DATABASE kv`, zoneConfig{
		replicas: 5, onlyNodes: []int{2, 3, 4, 5, 6}, leasePreference: "[+node4]"})

	c.Run(ctx, option.WithNodes(c.Node(6)), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	// Wait for the KV table to upreplicate.
	waitForUpreplication(t, ctx, conn, `database_name = 'kv'`, 5)

	// The replicate queue takes forever to move the ranges, so we do it
	// ourselves. Precreating the database/range and moving it to the correct
	// nodes first is not sufficient, since workload will spread the ranges across
	// all nodes regardless.
	relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 7}, []int{2, 3, 4, 5, 6})
	relocateRanges(t, ctx, conn, `database_name != 'kv'`, []int{4, 5, 6, 7}, []int{1, 2, 3})
	relocateLeases(t, ctx, conn, `database_name = 'kv'`, 4)

	// Run workload on n8 via n6-n7 gateways until test ends (context cancels).
	t.L().Printf("running workload")
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), `./cockroach workload run kv --read-percent 50 `+
			`--concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors `+
			roachtestutil.GetWorkloadHistogramArgs(t, c, getKVLabels(256, 0, 50))+` {pgurl:6-7}`)
		if ctx.Err() != nil {
			return nil // test requested workload shutdown
		}
		return err
	})

	// Start a worker to fail and recover partial partitions between n4,n5
	// (leases) and n6,n7 (gateways), both fully and individually, for 3 cycles.
	// Leases are only placed on n4.
	m.Go(func(ctx context.Context) error {
		defer cancelWorkload()

		for i := 0; i < 3; i++ {
			testcases := []struct {
				nodes []int
				peers []int
			}{
				// Fully partition leases from gateways, must route via n1-n3. In
				// addition to n4 leaseholder being unreachable, follower on n5 is
				// unreachable, and follower replica on n6 can't reach leaseholder.
				{[]int{6, 7}, []int{4, 5}},
				// Partition n6 (gateway with local follower) from n4 (leaseholder).
				// Local follower replica can't reach leaseholder.
				{[]int{6}, []int{4}},
				// Partition n7 (gateway) from n4 (leaseholder).
				{[]int{7}, []int{4}},
			}
			for _, tc := range testcases {
				sleepFor(ctx, t, time.Minute)

				// Ranges and leases may occasionally escape their constraints. Move
				// them to where they should be.
				relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 7}, []int{2, 3, 4, 5, 6})
				relocateRanges(t, ctx, conn, `database_name != 'kv'`, []int{4, 5, 6, 7}, []int{1, 2, 3})
				relocateLeases(t, ctx, conn, `database_name = 'kv'`, 4)

				// Randomly sleep up to the lease renewal interval, to vary the time
				// between the last lease renewal and the failure.
				sleepFor(ctx, t, randutil.RandDuration(rng, rangeLeaseRenewalDuration))

				for _, node := range tc.nodes {
					failer.Ready(ctx, node)
				}

				for _, node := range tc.nodes {
					t.L().Printf("failing n%d to n%v (%s lease/gateway)", node, tc.peers, failer)
					failer.FailPartial(ctx, node, tc.peers)
				}

				sleepFor(ctx, t, time.Minute)

				for _, node := range tc.nodes {
					t.L().Printf("recovering n%d to n%v (%s lease/gateway)", node, tc.peers, failer)
					failer.Recover(ctx, node)
				}
			}
		}

		sleepFor(ctx, t, time.Minute) // let cluster recover
		return nil
	})
	m.Wait()
}

// runFailoverPartialLeaseLeader tests a partial network partition between
// leaseholders and Raft leaders. This will prevent the leaseholder from making
// Raft proposals, but it can still hold onto leases as long as it can heartbeat
// liveness.
//
// Cluster topology:
//
// n1-n3: system and liveness ranges, SQL gateway
// n4-n6: user ranges
//
// We then create partial partitions where one of n4-n6 is unable to reach the
// other two nodes but is still able to reach the liveness range. This will
// cause split leader/leaseholder scenarios if raft leadership fails over from a
// partitioned node but the lease does not. We expect this to be a problem for
// epoch-based leases, but not for other types of leases.
//
// We run a kv50 workload on SQL gateways and collect pMax latency for graphing.
func runFailoverPartialLeaseLeader(ctx context.Context, t test.Test, c cluster.Cluster) {
	require.Equal(t, 7, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster. We only start n1-n3, to precisely place system ranges,
	// since we'll have to disable the replicate queue shortly.
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=100ms") // speed up replication

	// DistSender circuit breakers are useful in this test to avoid artificially
	// inflated latencies due to the way the test measures failover time. Without
	// circuit breakers, a request stuck on the partitioned leaseholder will get
	// blocked indefinitely, despite the range recovering on the other side of the
	// partition. As a result, the test won't differentiate between temporary and
	// permanent range unavailability. We have other tests which demonstrate the
	// benefit of DistSender circuit breakers (especially when applications do not
	// use statement timeouts), so we don't need to test them here.
	// TODO(arul): this can be removed if/when we turn on DistSender circuit
	// breakers for all ranges by default.
	settings.ClusterSettings["kv.dist_sender.circuit_breakers.mode"] = "all ranges"

	m := c.NewMonitor(ctx, c.CRDBNodes())

	failer := makeFailer(t, c, m, failureModeBlackhole, settings, rng).(PartialFailer)
	failer.Setup(ctx)
	defer failer.Cleanup(ctx)

	c.Start(ctx, t.L(), failoverStartOpts(), settings, c.Range(1, 3))

	conn := c.Conn(ctx, t.L(), 1)

	// Place all ranges on n1-n3 to start with, and wait for upreplication.
	configureAllZones(t, ctx, conn, zoneConfig{replicas: 3, onlyNodes: []int{1, 2, 3}})

	// NB: We want to ensure the system ranges are all down-replicated from their
	// initial RF of 5, so pass in roachtestutil.ExactlyReplicationFactor below.
	require.NoError(t, roachtestutil.WaitForReplication(ctx, t.L(), conn, 3, roachtestutil.ExactlyReplicationFactor))

	// Now that system ranges are properly placed on n1-n3, start n4-n6.
	c.Start(ctx, t.L(), failoverStartOpts(), settings, c.Range(4, 6))

	// Create the kv database on n4-n6.
	t.L().Printf("creating workload database")
	_, err := conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	configureZone(t, ctx, conn, `DATABASE kv`, zoneConfig{replicas: 3, onlyNodes: []int{4, 5, 6}})

	c.Run(ctx, option.WithNodes(c.Node(6)), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	// Move ranges to the appropriate nodes. Precreating the database/range and
	// moving it to the correct nodes first is not sufficient, since workload will
	// spread the ranges across all nodes regardless.
	relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 2, 3}, []int{4, 5, 6})
	relocateRanges(t, ctx, conn, `database_name != 'kv'`, []int{4, 5, 6}, []int{1, 2, 3})

	// Run workload on n7 via n1-n3 gateways until test ends (context cancels).
	t.L().Printf("running workload")
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), `./cockroach workload run kv --read-percent 50 `+
			`--concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors `+
			roachtestutil.GetWorkloadHistogramArgs(t, c, getKVLabels(256, 0, 50))+` {pgurl:1-3}`)
		if ctx.Err() != nil {
			return nil // test requested workload shutdown
		}
		return err
	})

	// Start a worker to fail and recover partial partitions between each of n4-n6
	// and the other two nodes for 3 cycles (9 failures total).
	m.Go(func(ctx context.Context) error {
		defer cancelWorkload()

		nodes := []int{4, 5, 6}
		for i := 0; i < 3; i++ {
			for _, node := range nodes {
				var peers []int
				for _, peer := range nodes {
					if peer != node {
						peers = append(peers, peer)
					}
				}

				sleepFor(ctx, t, time.Minute)

				// Ranges may occasionally escape their constraints. Move them to where
				// they should be.
				relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 2, 3}, []int{4, 5, 6})
				relocateRanges(t, ctx, conn, `database_name != 'kv'`, []int{4, 5, 6}, []int{1, 2, 3})

				// Randomly sleep up to the lease renewal interval, to vary the time
				// between the last lease renewal and the failure.
				sleepFor(ctx, t, randutil.RandDuration(rng, rangeLeaseRenewalDuration))

				failer.Ready(ctx, node)

				for _, peer := range peers {
					t.L().Printf("failing n%d to n%d (%s lease/leader)", node, peer, failer)
					failer.FailPartial(ctx, node, []int{peer})
				}

				sleepFor(ctx, t, time.Minute)

				for _, peer := range peers {
					t.L().Printf("recovering n%d to n%d (%s lease/leader)", node, peer, failer)
					failer.Recover(ctx, node)
				}
			}
		}

		sleepFor(ctx, t, time.Minute) // let cluster recover
		return nil
	})
	m.Wait()
}

// runFailoverPartialLeaseLiveness tests a partial network partition between a
// leaseholder and node liveness. With epoch leases we would normally expect
// this to recover shortly, since the node can't heartbeat its liveness record
// and thus its leases will expire. However, it will maintain Raft leadership,
// and we prevent non-leaders from acquiring leases, which can prevent the lease
// from moving unless we explicitly handle this. See also:
// https://github.com/cockroachdb/cockroach/pull/87244.
//
// Cluster topology:
//
// n1-n3: system ranges and SQL gateways
// n4:    liveness leaseholder
// n5-7:  user ranges
//
// A partial blackhole network partition is triggered between n4 and each of
// n5-n7 sequentially, 3 times per node for a total of 9 times. A kv50 workload
// is running against SQL gateways on n1-n3, and we collect the pMax latency for
// graphing.
func runFailoverPartialLeaseLiveness(ctx context.Context, t test.Test, c cluster.Cluster) {
	require.Equal(t, 8, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster.
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=100ms") // speed up replication

	m := c.NewMonitor(ctx, c.CRDBNodes())

	failer := makeFailer(t, c, m, failureModeBlackhole, settings, rng).(PartialFailer)
	failer.Setup(ctx)
	defer failer.Cleanup(ctx)

	c.Start(ctx, t.L(), failoverStartOpts(), settings, c.CRDBNodes())

	conn := c.Conn(ctx, t.L(), 1)

	// Place all ranges on n1-n3, and an extra liveness leaseholder replica on n4.
	configureAllZones(t, ctx, conn, zoneConfig{replicas: 3, onlyNodes: []int{1, 2, 3}})
	configureZone(t, ctx, conn, `RANGE liveness`, zoneConfig{
		replicas: 4, onlyNodes: []int{1, 2, 3, 4}, leasePreference: "[+node4]"})

	// Wait for upreplication.
	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), conn))

	// Create the kv database on n5-n7.
	t.L().Printf("creating workload database")
	_, err := conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	configureZone(t, ctx, conn, `DATABASE kv`, zoneConfig{replicas: 3, onlyNodes: []int{5, 6, 7}})

	c.Run(ctx, option.WithNodes(c.Node(6)), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	// The replicate queue takes forever to move the ranges, so we do it
	// ourselves. Precreating the database/range and moving it to the correct
	// nodes first is not sufficient, since workload will spread the ranges across
	// all nodes regardless.
	relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 2, 3, 4}, []int{5, 6, 7})
	relocateRanges(t, ctx, conn, `database_name != 'kv'`, []int{5, 6, 7}, []int{1, 2, 3, 4})
	relocateRanges(t, ctx, conn, `range_id != 2`, []int{4}, []int{1, 2, 3})

	// Run workload on n8 using n1-n3 as gateways (not partitioned) until test
	// ends (context cancels).
	t.L().Printf("running workload")
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), `./cockroach workload run kv --read-percent 50 `+
			`--concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors `+
			roachtestutil.GetWorkloadHistogramArgs(t, c, getKVLabels(256, 0, 50))+` {pgurl:1-3}`)
		if ctx.Err() != nil {
			return nil // test requested workload shutdown
		}
		return err
	})

	// Start a worker to fail and recover partial partitions between n4 (liveness)
	// and workload leaseholders n5-n7 for 1 minute each, 3 times per node for 9
	// times total.
	m.Go(func(ctx context.Context) error {
		defer cancelWorkload()

		for i := 0; i < 3; i++ {
			for _, node := range []int{5, 6, 7} {
				sleepFor(ctx, t, time.Minute)

				// Ranges and leases may occasionally escape their constraints. Move
				// them to where they should be.
				relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 2, 3, 4}, []int{5, 6, 7})
				relocateRanges(t, ctx, conn, `database_name != 'kv'`, []int{node}, []int{1, 2, 3})
				relocateRanges(t, ctx, conn, `range_id = 2`, []int{5, 6, 7}, []int{1, 2, 3, 4})
				relocateLeases(t, ctx, conn, `range_id = 2`, 4)

				// Randomly sleep up to the lease renewal interval, to vary the time
				// between the last lease renewal and the failure.
				sleepFor(ctx, t, randutil.RandDuration(rng, rangeLeaseRenewalDuration))

				failer.Ready(ctx, node)

				peer := 4
				t.L().Printf("failing n%d to n%d (%s lease/liveness)", node, peer, failer)
				failer.FailPartial(ctx, node, []int{peer})

				sleepFor(ctx, t, time.Minute)

				t.L().Printf("recovering n%d to n%d (%s lease/liveness)", node, peer, failer)
				failer.Recover(ctx, node)
			}
		}

		sleepFor(ctx, t, time.Minute) // let cluster recover
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
// The cluster layout is as follows:
//
// n1-n3: System ranges and SQL gateways.
// n4-n6: Workload ranges.
// n7:    Workload runner.
//
// The test runs a kv50 workload via gateways on n1-n3, measuring the pMax
// latency for graphing.
func runFailoverNonSystem(
	ctx context.Context, t test.Test, c cluster.Cluster, failureMode failureMode,
) {
	require.Equal(t, 7, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster.
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_ENABLE_UNSAFE_TEST_BUILTINS=true")
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=100ms") // speed up replication

	m := c.NewMonitor(ctx, c.CRDBNodes())

	failer := makeFailer(t, c, m, failureMode, settings, rng)
	failer.Setup(ctx)
	defer failer.Cleanup(ctx)

	c.Start(ctx, t.L(), failoverStartOpts(), settings, c.CRDBNodes())

	conn := c.Conn(ctx, t.L(), 1)

	// Constrain all existing zone configs to n1-n3.
	configureAllZones(t, ctx, conn, zoneConfig{replicas: 3, onlyNodes: []int{1, 2, 3}})

	// Wait for upreplication.
	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), conn))

	// Create the kv database, constrained to n4-n6. Despite the zone config, the
	// ranges will initially be distributed across all cluster nodes.
	t.L().Printf("creating workload database")
	_, err := conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	configureZone(t, ctx, conn, `DATABASE kv`, zoneConfig{replicas: 3, onlyNodes: []int{4, 5, 6}})
	c.Run(ctx, option.WithNodes(c.Node(7)), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	// The replicate queue takes forever to move the kv ranges from n1-n3 to
	// n4-n6, so we do it ourselves. Precreating the database/range and moving it
	// to the correct nodes first is not sufficient, since workload will spread
	// the ranges across all nodes regardless.
	relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 2, 3}, []int{4, 5, 6})

	// Run workload on n7 via n1-n3 gateways until test ends (context cancels).
	t.L().Printf("running workload")
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), `./cockroach workload run kv --read-percent 50 `+
			`--concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors `+
			roachtestutil.GetWorkloadHistogramArgs(t, c, getKVLabels(256, 0, 50))+` {pgurl:1-3}`)
		if ctx.Err() != nil {
			return nil // test requested workload shutdown
		}
		return err
	})

	// Start a worker to fail and recover n4-n6 in order.
	m.Go(func(ctx context.Context) error {
		defer cancelWorkload()

		for i := 0; i < 3; i++ {
			for _, node := range []int{4, 5, 6} {
				sleepFor(ctx, t, time.Minute)

				// Ranges may occasionally escape their constraints. Move them
				// to where they should be.
				relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 2, 3}, []int{4, 5, 6})
				relocateRanges(t, ctx, conn, `database_name != 'kv'`, []int{node}, []int{1, 2, 3})

				// Randomly sleep up to the lease renewal interval, to vary the time
				// between the last lease renewal and the failure.
				sleepFor(ctx, t, randutil.RandDuration(rng, rangeLeaseRenewalDuration))

				failer.Ready(ctx, node)

				t.L().Printf("failing n%d (%s)", node, failer)
				failer.Fail(ctx, node)

				sleepFor(ctx, t, time.Minute)

				t.L().Printf("recovering n%d (%s)", node, failer)
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
// The cluster layout is as follows:
//
// n1-n3: All ranges, including liveness.
// n4:    Liveness range leaseholder.
// n5:    Workload runner.
//
// The test runs a kv50 workload via gateways on n1-n3, measuring the pMax
// latency for graphing.
func runFailoverLiveness(
	ctx context.Context, t test.Test, c cluster.Cluster, failureMode failureMode,
) {
	require.Equal(t, 5, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster.
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_ENABLE_UNSAFE_TEST_BUILTINS=true")
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=100ms") // speed up replication

	m := c.NewMonitor(ctx, c.CRDBNodes())

	failer := makeFailer(t, c, m, failureMode, settings, rng)
	failer.Setup(ctx)
	defer failer.Cleanup(ctx)

	c.Start(ctx, t.L(), failoverStartOpts(), settings, c.CRDBNodes())

	conn := c.Conn(ctx, t.L(), 1)

	// Constrain all existing zone configs to n1-n3.
	configureAllZones(t, ctx, conn, zoneConfig{replicas: 3, onlyNodes: []int{1, 2, 3}})

	// Constrain the liveness range to n1-n4, with leaseholder preference on n4.
	configureZone(t, ctx, conn, `RANGE liveness`, zoneConfig{replicas: 4, leasePreference: "[+node4]"})

	// Wait for upreplication.
	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), conn))

	// Create the kv database, constrained to n1-n3. Despite the zone config, the
	// ranges will initially be distributed across all cluster nodes.
	t.L().Printf("creating workload database")
	_, err := conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	configureZone(t, ctx, conn, `DATABASE kv`, zoneConfig{replicas: 3, onlyNodes: []int{1, 2, 3}})
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	// The replicate queue takes forever to move the other ranges off of n4 so we
	// do it ourselves. Precreating the database/range and moving it to the
	// correct nodes first is not sufficient, since workload will spread the
	// ranges across all nodes regardless.
	relocateRanges(t, ctx, conn, `range_id != 2`, []int{4}, []int{1, 2, 3})

	// We also make sure the lease is located on n4.
	relocateLeases(t, ctx, conn, `range_id = 2`, 4)

	// Run workload on n5 via n1-n3 gateways until test ends (context cancels).
	t.L().Printf("running workload")
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), `./cockroach workload run kv --read-percent 50 `+
			`--concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors `+
			roachtestutil.GetWorkloadHistogramArgs(t, c, getKVLabels(256, 0, 50))+` {pgurl:1-3}`)
		if ctx.Err() != nil {
			return nil // test requested workload shutdown
		}
		return err
	})

	// Start a worker to fail and recover n4.
	m.Go(func(ctx context.Context) error {
		defer cancelWorkload()

		for i := 0; i < 9; i++ {
			sleepFor(ctx, t, time.Minute)

			// Ranges and leases may occasionally escape their constraints. Move them
			// to where they should be.
			relocateRanges(t, ctx, conn, `range_id != 2`, []int{4}, []int{1, 2, 3})
			relocateLeases(t, ctx, conn, `range_id = 2`, 4)

			// Randomly sleep up to the lease renewal interval, to vary the time
			// between the last lease renewal and the failure.
			sleepFor(ctx, t, randutil.RandDuration(rng, rangeLeaseRenewalDuration))

			failer.Ready(ctx, 4)

			t.L().Printf("failing n%d (%s)", 4, failer)
			failer.Fail(ctx, 4)

			sleepFor(ctx, t, time.Minute)

			t.L().Printf("recovering n%d (%s)", 4, failer)
			failer.Recover(ctx, 4)
			relocateLeases(t, ctx, conn, `range_id = 2`, 4)
		}

		sleepFor(ctx, t, time.Minute) // let cluster recover
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
// The cluster layout is as follows:
//
// n1-n3: Workload ranges, liveness range, and SQL gateways.
// n4-n6: System ranges excluding liveness.
// n7:    Workload runner.
//
// The test runs a kv50 workload via gateways on n1-n3, measuring the pMax
// latency for graphing.
func runFailoverSystemNonLiveness(
	ctx context.Context, t test.Test, c cluster.Cluster, failureMode failureMode,
) {
	require.Equal(t, 7, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster.
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_ENABLE_UNSAFE_TEST_BUILTINS=true")
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=100ms") // speed up replication

	m := c.NewMonitor(ctx, c.CRDBNodes())

	failer := makeFailer(t, c, m, failureMode, settings, rng)
	failer.Setup(ctx)
	defer failer.Cleanup(ctx)

	c.Start(ctx, t.L(), failoverStartOpts(), settings, c.CRDBNodes())

	conn := c.Conn(ctx, t.L(), 1)

	// Constrain all existing zone configs to n4-n6, except liveness which is
	// constrained to n1-n3.
	configureAllZones(t, ctx, conn, zoneConfig{replicas: 3, onlyNodes: []int{4, 5, 6}})
	configureZone(t, ctx, conn, `RANGE liveness`, zoneConfig{replicas: 3, onlyNodes: []int{1, 2, 3}})

	// Wait for upreplication.
	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), conn))

	// Create the kv database, constrained to n1-n3. Despite the zone config, the
	// ranges will initially be distributed across all cluster nodes.
	t.L().Printf("creating workload database")
	_, err := conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	configureZone(t, ctx, conn, `DATABASE kv`, zoneConfig{replicas: 3, onlyNodes: []int{1, 2, 3}})
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	// The replicate queue takes forever to move the kv ranges from n4-n6 to
	// n1-n3, so we do it ourselves. Precreating the database/range and moving it
	// to the correct nodes first is not sufficient, since workload will spread
	// the ranges across all nodes regardless.
	relocateRanges(t, ctx, conn, `database_name = 'kv' OR range_id = 2`,
		[]int{4, 5, 6}, []int{1, 2, 3})
	relocateRanges(t, ctx, conn, `database_name != 'kv' AND range_id != 2`,
		[]int{1, 2, 3}, []int{4, 5, 6})

	// Run workload on n7 via n1-n3 as gateways until test ends (context cancels).
	t.L().Printf("running workload")
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), `./cockroach workload run kv --read-percent 50 `+
			`--concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors `+
			roachtestutil.GetWorkloadHistogramArgs(t, c, getKVLabels(256, 0, 50))+` {pgurl:1-3}`)
		if ctx.Err() != nil {
			return nil // test requested workload shutdown
		}
		return err
	})

	// Start a worker to fail and recover n4-n6 in order.
	m.Go(func(ctx context.Context) error {
		defer cancelWorkload()

		for i := 0; i < 3; i++ {
			for _, node := range []int{4, 5, 6} {
				sleepFor(ctx, t, time.Minute)

				// Ranges may occasionally escape their constraints. Move them
				// to where they should be.
				relocateRanges(t, ctx, conn, `database_name != 'kv' AND range_id != 2`,
					[]int{1, 2, 3}, []int{4, 5, 6})
				relocateRanges(t, ctx, conn, `database_name = 'kv' OR range_id = 2`,
					[]int{4, 5, 6}, []int{1, 2, 3})

				// Randomly sleep up to the lease renewal interval, to vary the time
				// between the last lease renewal and the failure.
				sleepFor(ctx, t, randutil.RandDuration(rng, rangeLeaseRenewalDuration))

				failer.Ready(ctx, node)

				t.L().Printf("failing n%d (%s)", node, failer)
				failer.Fail(ctx, node)

				sleepFor(ctx, t, time.Minute)

				t.L().Printf("recovering n%d (%s)", node, failer)
				failer.Recover(ctx, node)
			}
		}

		sleepFor(ctx, t, time.Minute) // let cluster recover
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
	failureModeDeadlock      failureMode = "deadlock"
	failureModeDiskStall     failureMode = "disk-stall"
	failureModePause         failureMode = "pause"
	failureModeNoop          failureMode = "noop"
)

var allFailureModes = []failureMode{
	failureModeBlackhole,
	failureModeBlackholeRecv,
	failureModeBlackholeSend,
	failureModeCrash,
	failureModeDeadlock,
	failureModeDiskStall,
	failureModePause,
	// failureModeNoop intentionally omitted
}

// makeFailer creates a new failer for the given failureMode. It may return a
// noopFailer on local clusters.
func makeFailer(
	t test.Test,
	c cluster.Cluster,
	m cluster.Monitor,
	failureMode failureMode,
	settings install.ClusterSettings,
	rng *rand.Rand,
) Failer {
	f := makeFailerWithoutLocalNoop(t, c, m, failureMode, settings, rng)
	if c.IsLocal() && !f.CanUseLocal() {
		t.L().Printf(
			`failure mode %q not supported on local clusters, using "noop" failure mode instead`,
			failureMode)
		f = &noopFailer{}
	}
	return f
}

func makeFailerWithoutLocalNoop(
	t test.Test,
	c cluster.Cluster,
	m cluster.Monitor,
	failureMode failureMode,
	settings install.ClusterSettings,
	rng *rand.Rand,
) Failer {
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
			m:             m,
			startSettings: settings,
		}
	case failureModeDeadlock:
		return &deadlockFailer{
			t:                t,
			c:                c,
			m:                m,
			rng:              rng,
			startSettings:    settings,
			onlyLeaseholders: true,
			numReplicas:      5,
		}
	case failureModeDiskStall:
		return &diskStallFailer{
			t:             t,
			c:             c,
			m:             m,
			startSettings: settings,
			staller:       roachtestutil.MakeDmsetupDiskStaller(t, c),
		}
	case failureModePause:
		return &pauseFailer{
			t: t,
			c: c,
		}
	case failureModeNoop:
		return &noopFailer{}
	default:
		t.Fatalf("unknown failure mode %s", failureMode)
		return nil
	}
}

// Failer fails and recovers a given node in some particular way.
type Failer interface {
	fmt.Stringer

	// Mode returns the failure mode of the failer.
	Mode() failureMode

	// CanUseLocal returns true if the failer can be run with a local cluster.
	CanUseLocal() bool

	// CanUseChaos returns true if the failer can be used in the chaos tests. A
	// failer may not want to run in chaos tests if we do not expect cockroach to
	// recover from it and do not want to lose signal for the chaos tests. Even if
	// a failer is not included in the chaos tests, it will still be run alone in
	// its own tests.
	CanUseChaos() bool

	// CanRunWith returns true if the failer can run concurrently with another
	// given failure mode on a different cluster node. It is not required to
	// commute, i.e. A may not be able to run with B even though B can run with A.
	CanRunWith(other failureMode) bool

	// Setup prepares the failer. It is called before the cluster is started.
	Setup(ctx context.Context)

	// Cleanup cleans up when the test exits. This is needed e.g. when the cluster
	// is reused by a different test.
	Cleanup(ctx context.Context)

	// Ready is called before failing each node, when the cluster and workload is
	// running and after recovering the previous node failure if any.
	Ready(ctx context.Context, nodeID int)

	// Fail fails the given node.
	Fail(ctx context.Context, nodeID int)

	// Recover recovers the given node.
	Recover(ctx context.Context, nodeID int)
}

// PartialFailer supports partial failures between specific node pairs.
type PartialFailer interface {
	Failer

	// FailPartial fails the node for the given peers.
	FailPartial(ctx context.Context, nodeID int, peerIDs []int)
}

// noopFailer doesn't do anything.
type noopFailer struct{}

func (f *noopFailer) Mode() failureMode                       { return failureModeNoop }
func (f *noopFailer) String() string                          { return string(f.Mode()) }
func (f *noopFailer) CanUseLocal() bool                       { return true }
func (f *noopFailer) CanUseChaos() bool                       { return true }
func (f *noopFailer) CanRunWith(failureMode) bool             { return true }
func (f *noopFailer) Setup(context.Context)                   {}
func (f *noopFailer) Ready(context.Context, int)              {}
func (f *noopFailer) Cleanup(context.Context)                 {}
func (f *noopFailer) Fail(context.Context, int)               {}
func (f *noopFailer) FailPartial(context.Context, int, []int) {}
func (f *noopFailer) Recover(context.Context, int)            {}

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

func (f *blackholeFailer) Mode() failureMode {
	if f.input && !f.output {
		return failureModeBlackholeRecv
	} else if f.output && !f.input {
		return failureModeBlackholeSend
	}
	return failureModeBlackhole
}

func (f *blackholeFailer) String() string              { return string(f.Mode()) }
func (f *blackholeFailer) CanUseLocal() bool           { return false } // needs iptables
func (f *blackholeFailer) CanUseChaos() bool           { return true }
func (f *blackholeFailer) CanRunWith(failureMode) bool { return true }
func (f *blackholeFailer) Setup(context.Context)       {}
func (f *blackholeFailer) Ready(context.Context, int)  {}

func (f *blackholeFailer) Cleanup(ctx context.Context) {
	f.c.Run(ctx, option.WithNodes(f.c.All()), `sudo iptables -F`)
}

func (f *blackholeFailer) Fail(ctx context.Context, nodeID int) {
	pgport := fmt.Sprintf("{pgport:%d}", nodeID)

	// When dropping both input and output, make sure we drop packets in both
	// directions for both the inbound and outbound TCP connections, such that we
	// get a proper black hole. Only dropping one direction for both of INPUT and
	// OUTPUT will still let e.g. TCP retransmits through, which may affect the
	// TCP stack behavior and is not representative of real network outages.
	//
	// For the asymmetric partitions, only drop packets in one direction since
	// this is representative of accidental firewall rules we've seen cause such
	// outages in the wild.
	if f.input && f.output {
		// Inbound TCP connections, both received and sent packets.
		f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(`sudo iptables -A INPUT -p tcp --dport %s -j DROP`, pgport))
		f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(`sudo iptables -A OUTPUT -p tcp --sport %s -j DROP`, pgport))
		// Outbound TCP connections, both sent and received packets.
		f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(`sudo iptables -A OUTPUT -p tcp --dport %s -j DROP`, pgport))
		f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(`sudo iptables -A INPUT -p tcp --sport %s -j DROP`, pgport))
	} else if f.input {
		f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(`sudo iptables -A INPUT -p tcp --dport %s -j DROP`, pgport))
	} else if f.output {
		f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(`sudo iptables -A OUTPUT -p tcp --dport %s -j DROP`, pgport))
	}
}

// FailPartial creates a partial blackhole failure between the given node and
// peers.
func (f *blackholeFailer) FailPartial(ctx context.Context, nodeID int, peerIDs []int) {
	for _, peerID := range peerIDs {
		pgport := fmt.Sprintf("{pgport:%d}", nodeID)
		peerIP := fmt.Sprintf("{ip:%d}", peerID)

		// When dropping both input and output, make sure we drop packets in both
		// directions for both the inbound and outbound TCP connections, such that
		// we get a proper black hole. Only dropping one direction for both of INPUT
		// and OUTPUT will still let e.g. TCP retransmits through, which may affect
		// TCP stack behavior and is not representative of real network outages.
		//
		// For the asymmetric partitions, only drop packets in one direction since
		// this is representative of accidental firewall rules we've seen cause such
		// outages in the wild.
		if f.input && f.output {
			// Inbound TCP connections, both received and sent packets.
			f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(
				`sudo iptables -A INPUT -p tcp -s %s --dport %s -j DROP`, peerIP, pgport))
			f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(
				`sudo iptables -A OUTPUT -p tcp -d %s --sport %s -j DROP`, peerIP, pgport))
			// Outbound TCP connections, both sent and received packets.
			f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(
				`sudo iptables -A OUTPUT -p tcp -d %s --dport %s -j DROP`, peerIP, pgport))
			f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(
				`sudo iptables -A INPUT -p tcp -s %s --sport %s -j DROP`, peerIP, pgport))
		} else if f.input {
			f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(
				`sudo iptables -A INPUT -p tcp -s %s --dport %s -j DROP`, peerIP, pgport))
		} else if f.output {
			f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(
				`sudo iptables -A OUTPUT -p tcp -d %s --dport %s -j DROP`, peerIP, pgport))
		}
	}
}

func (f *blackholeFailer) Recover(ctx context.Context, nodeID int) {
	f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), `sudo iptables -F`)
}

// crashFailer is a process crash where the TCP/IP stack remains responsive
// and sends immediate RST packets to peers.
type crashFailer struct {
	t             test.Test
	c             cluster.Cluster
	m             cluster.Monitor
	startSettings install.ClusterSettings
}

func (f *crashFailer) Mode() failureMode           { return failureModeCrash }
func (f *crashFailer) String() string              { return string(f.Mode()) }
func (f *crashFailer) CanUseLocal() bool           { return true }
func (f *crashFailer) CanUseChaos() bool           { return true }
func (f *crashFailer) CanRunWith(failureMode) bool { return true }
func (f *crashFailer) Setup(context.Context)       {}
func (f *crashFailer) Ready(context.Context, int)  {}
func (f *crashFailer) Cleanup(context.Context)     {}

func (f *crashFailer) Fail(ctx context.Context, nodeID int) {
	f.m.ExpectDeath()
	f.c.Stop(ctx, f.t.L(), option.DefaultStopOpts(), f.c.Node(nodeID)) // uses SIGKILL
}

func (f *crashFailer) Recover(ctx context.Context, nodeID int) {
	f.c.Start(ctx, f.t.L(), failoverRestartOpts(), f.startSettings, f.c.Node(nodeID))
}

// deadlockFailer deadlocks replicas. In addition to deadlocks, this failure
// mode is representative of all failure modes that leave a replica unresponsive
// while the node is otherwise still functional.
type deadlockFailer struct {
	t                test.Test
	c                cluster.Cluster
	m                cluster.Monitor
	rng              *rand.Rand
	startSettings    install.ClusterSettings
	onlyLeaseholders bool
	numReplicas      int

	locks  map[int][]roachpb.RangeID // track locks by node
	ranges map[int][]roachpb.RangeID // ranges present on nodes
	leases map[int][]roachpb.RangeID // range leases present on nodes
}

func (f *deadlockFailer) Mode() failureMode             { return failureModeDeadlock }
func (f *deadlockFailer) String() string                { return string(f.Mode()) }
func (f *deadlockFailer) CanUseLocal() bool             { return true }
func (f *deadlockFailer) CanRunWith(m failureMode) bool { return true }
func (f *deadlockFailer) Setup(context.Context)         {}
func (f *deadlockFailer) Cleanup(context.Context)       {}

func (f *deadlockFailer) CanUseChaos() bool {
	// We disable the deadlock failer in chaos tests, because none of the three
	// forms of leases (expiration, epoch, and leader) can survive it. Epoch and
	// leader leases cannot survive it by design without the introduction of a
	// replica watchdog. Expiration leases can survive it if combined with
	// DistSender circuit breakers, but those are not yet enabled by default, even
	// though we do enable them in the chaos tests.
	return false
}

func (f *deadlockFailer) Ready(ctx context.Context, nodeID int) {
	// In chaos tests, other nodes will be failing concurrently. We therefore
	// can't run SHOW CLUSTER RANGES WITH DETAILS in Fail(), since it needs to
	// read from all ranges. Instead, we fetch a snapshot of replicas and leases
	// now, and if any replicas should move we'll skip them later.
	//
	// We also have to ensure we have an active connection to the node in the
	// pool, since we may be unable to create one during concurrent failures.
	conn := f.c.Conn(ctx, f.t.L(), nodeID)
	rows, err := conn.QueryContext(ctx,
		`SELECT range_id, replicas, lease_holder FROM [SHOW CLUSTER RANGES WITH DETAILS]`)
	require.NoError(f.t, err)

	f.ranges = map[int][]roachpb.RangeID{}
	f.leases = map[int][]roachpb.RangeID{}
	for rows.Next() {
		var rangeID roachpb.RangeID
		var replicas []int64
		var leaseHolder int
		require.NoError(f.t, rows.Scan(&rangeID, (*pq.Int64Array)(&replicas), &leaseHolder))
		f.leases[leaseHolder] = append(f.leases[leaseHolder], rangeID)
		for _, nodeID := range replicas {
			f.ranges[int(nodeID)] = append(f.ranges[int(nodeID)], rangeID)
		}
	}
	require.NoError(f.t, rows.Err())
}

func (f *deadlockFailer) Fail(ctx context.Context, nodeID int) {
	require.NotZero(f.t, f.numReplicas)
	if f.locks == nil {
		f.locks = map[int][]roachpb.RangeID{}
	}

	var ranges []roachpb.RangeID
	if f.onlyLeaseholders {
		ranges = append(ranges, f.leases[nodeID]...)
	} else {
		ranges = append(ranges, f.ranges[nodeID]...)
	}
	f.rng.Shuffle(len(ranges), func(i, j int) {
		ranges[i], ranges[j] = ranges[j], ranges[i]
	})

	conn := f.c.Conn(ctx, f.t.L(), nodeID)

	for i := 0; i < len(ranges) && len(f.locks[nodeID]) < f.numReplicas; i++ {
		rangeID := ranges[i]
		var locked bool
		// Retry the lock acquisition for a bit. Transient errors are possible here
		// if there is another failure in the system that hasn't cleared up yet; to
		// run a SQL query we may need to run internal queries related to user auth.
		//
		// See: https://github.com/cockroachdb/cockroach/issues/129918
		testutils.SucceedsSoon(f.t, func() error {
			ctx, cancel := context.WithTimeout(ctx, 20*time.Second) // can take a while to lock
			defer cancel()
			return conn.QueryRowContext(ctx,
				`SELECT crdb_internal.unsafe_lock_replica($1::int, true)`, rangeID).Scan(&locked)
		})
		// NB: `locked` is false if the replica moved off the node in the interim.
		if locked {
			f.locks[nodeID] = append(f.locks[nodeID], rangeID)
			f.t.L().Printf("locked r%d on n%d", rangeID, nodeID)
		}
	}
	// Some nodes may have fewer ranges than the requested numReplicas locks, and
	// replicas may also have moved in the meanwhile. Just assert that we were able
	// to lock at least 1 replica.
	require.NotEmpty(f.t, f.locks[nodeID], "didn't lock any replicas")
}

func (f *deadlockFailer) Recover(ctx context.Context, nodeID int) {
	if f.locks == nil || len(f.locks[nodeID]) == 0 {
		return
	}

	err := func() error {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		conn, err := f.c.ConnE(ctx, f.t.L(), nodeID)
		if err != nil {
			return err
		}
		for _, rangeID := range f.locks[nodeID] {
			var unlocked bool
			err := conn.QueryRowContext(ctx,
				`SELECT crdb_internal.unsafe_lock_replica($1, false)`, rangeID).Scan(&unlocked)
			if err != nil {
				return err
			} else if !unlocked {
				return errors.Errorf("r%d was not unlocked", rangeID)
			} else {
				f.t.L().Printf("unlocked r%d on n%d", rangeID, nodeID)
			}
		}
		return nil
	}()
	// We may have locked replicas that prevent us from connecting to the node
	// again, so we fall back to restarting the node.
	if err != nil {
		f.t.L().Printf("failed to unlock replicas on n%d, restarting node: %s", nodeID, err)
		f.m.ExpectDeath()
		f.c.Stop(ctx, f.t.L(), option.DefaultStopOpts(), f.c.Node(nodeID))
		f.c.Start(ctx, f.t.L(), failoverRestartOpts(), f.startSettings, f.c.Node(nodeID))
	}
	delete(f.locks, nodeID)
}

// diskStallFailer stalls the disk indefinitely. This should cause the node to
// eventually self-terminate, but we'd want leases to move off before then.
type diskStallFailer struct {
	t             test.Test
	c             cluster.Cluster
	m             cluster.Monitor
	startSettings install.ClusterSettings
	staller       diskStaller
}

func (f *diskStallFailer) Mode() failureMode           { return failureModeDiskStall }
func (f *diskStallFailer) String() string              { return string(f.Mode()) }
func (f *diskStallFailer) CanUseLocal() bool           { return false } // needs dmsetup
func (f *diskStallFailer) CanUseChaos() bool           { return true }
func (f *diskStallFailer) CanRunWith(failureMode) bool { return true }

func (f *diskStallFailer) Setup(ctx context.Context) {
	f.staller.Setup(ctx)
}

func (f *diskStallFailer) Cleanup(ctx context.Context) {
	f.staller.Unstall(ctx, f.c.All())
	// We have to stop the cluster before cleaning up the staller.
	f.m.ExpectDeaths(int32(f.c.Spec().NodeCount))
	f.c.Stop(ctx, f.t.L(), option.DefaultStopOpts(), f.c.All())
	f.staller.Cleanup(ctx)
}

func (f *diskStallFailer) Ready(ctx context.Context, nodeID int) {
	// Other failure modes may have disabled the disk stall detector (see
	// pauseFailer), so we explicitly enable it.
	conn := f.c.Conn(ctx, f.t.L(), nodeID)
	_, err := conn.ExecContext(ctx,
		`SET CLUSTER SETTING storage.max_sync_duration.fatal.enabled = true`)
	require.NoError(f.t, err)
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
	f.c.Start(ctx, f.t.L(), failoverRestartOpts(), f.startSettings, f.c.Node(nodeID))
}

// pauseFailer pauses the process, but keeps the OS (and thus network
// connections) alive.
type pauseFailer struct {
	t test.Test
	c cluster.Cluster
}

func (f *pauseFailer) Mode() failureMode       { return failureModePause }
func (f *pauseFailer) String() string          { return string(f.Mode()) }
func (f *pauseFailer) CanUseLocal() bool       { return true }
func (f *pauseFailer) CanUseChaos() bool       { return true }
func (f *pauseFailer) Setup(context.Context)   {}
func (f *pauseFailer) Cleanup(context.Context) {}

func (f *pauseFailer) CanRunWith(other failureMode) bool {
	// Since we disable the disk stall detector, we can't run concurrently with
	// a disk stall on a different node.
	return other != failureModeDiskStall
}

func (f *pauseFailer) Ready(ctx context.Context, nodeID int) {
	// The process pause can trip the disk stall detector, so we disable it. We
	// could let it fire, but we'd like to see if the node can recover from the
	// pause and keep working. It will be re-enabled by diskStallFailer.Ready().
	conn := f.c.Conn(ctx, f.t.L(), nodeID)
	_, err := conn.ExecContext(ctx,
		`SET CLUSTER SETTING storage.max_sync_duration.fatal.enabled = false`)
	require.NoError(f.t, err)
}

func (f *pauseFailer) Fail(ctx context.Context, nodeID int) {
	f.c.Signal(ctx, f.t.L(), 19, f.c.Node(nodeID)) // SIGSTOP
}

func (f *pauseFailer) Recover(ctx context.Context, nodeID int) {
	f.c.Signal(ctx, f.t.L(), 18, f.c.Node(nodeID)) // SIGCONT
	// NB: We don't re-enable the disk stall detector here, but instead rely on
	// diskStallFailer.Ready to ensure it's enabled, since the cluster likely
	// hasn't recovered yet and we may fail to set the cluster setting.
}

// waitForUpreplication waits for upreplication of ranges that satisfy the
// given predicate (using SHOW RANGES).
//
// TODO(erikgrinaker): move this into roachtestutil.WaitForReplication() when it can use SHOW
// RANGES, i.e. when it's no longer needed in mixed-version tests with older
// versions that don't have SHOW RANGES.
func waitForUpreplication(
	t test.Test, ctx context.Context, conn *gosql.DB, predicate string, replicationFactor int,
) {
	var count int
	where := fmt.Sprintf("WHERE array_length(replicas, 1) < %d", replicationFactor)
	if predicate != "" {
		where += fmt.Sprintf(" AND (%s)", predicate)
	}
	for {
		require.NoError(t, conn.QueryRowContext(ctx,
			`SELECT count(distinct range_id) FROM [SHOW CLUSTER RANGES WITH TABLES, DETAILS] `+where).
			Scan(&count))
		if count == 0 {
			break
		}
		t.L().Printf("waiting for %d ranges to upreplicate (%s)", count, predicate)
		time.Sleep(time.Second)
	}
}

// relocateRanges relocates all ranges matching the given predicate from a set
// of nodes to a different set of nodes. Moves are attempted sequentially from
// each source onto each target, and errors are retried indefinitely.
//
// TODO(erikgrinaker): It would be really neat if the replicate queue could
// deal with this for us. For that to happen, we need three things:
//
//  1. The replicate queue must do this ~immediately. It should take ~10 seconds
//     for 1000 ranges, not 10 minutes.
//
//  2. We need to know when the replicate queue is done placing both replicas and
//     leases in accordance with the zone configurations, so that we can wait for
//     it. SpanConfigConformance should provide this, but currently doesn't have
//     a public API, and doesn't handle lease preferences.
//
//  3. The replicate queue must guarantee that replicas and leases won't escape
//     their constraints after the initial setup. We see them do so currently.
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
			t.L().Printf("moving %d ranges off of n%d (%s)", count, source, predicate)
			for _, target := range to {
				_, err := conn.ExecContext(ctx, `ALTER RANGE RELOCATE FROM $1::int TO $2::int FOR `+
					`SELECT DISTINCT range_id FROM [SHOW CLUSTER RANGES WITH TABLES] WHERE `+where,
					source, target)
				if err != nil {
					t.L().Printf("failed to move ranges: %s", err)
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
		t.L().Printf("moving %d leases to n%d (%s)", count, to, predicate)
		_, err := conn.ExecContext(ctx, `ALTER RANGE RELOCATE LEASE TO $1::int FOR `+
			`SELECT DISTINCT range_id FROM [SHOW CLUSTER RANGES WITH TABLES, DETAILS] WHERE `+where, to)
		if err != nil {
			t.L().Printf("failed to move leases: %s", err)
		}
		time.Sleep(time.Second)
	}
}

type zoneConfig struct {
	replicas        int
	onlyNodes       []int
	leasePreference string
}

// configureZone sets the zone config for the given target.
func configureZone(
	t test.Test, ctx context.Context, conn *gosql.DB, target string, cfg zoneConfig,
) {
	require.NotZero(t, cfg.replicas, "num_replicas must be > 0")

	// If onlyNodes is given, invert the constraint and specify which nodes are
	// prohibited. Otherwise, the allocator may leave replicas outside of the
	// specified nodes.
	var constraintsString string
	if len(cfg.onlyNodes) > 0 {
		nodeCount := t.Spec().(*registry.TestSpec).Cluster.NodeCount - 1 // subtract worker node
		included := map[int]bool{}
		for _, nodeID := range cfg.onlyNodes {
			included[nodeID] = true
		}
		excluded := []int{}
		for nodeID := 1; nodeID <= nodeCount; nodeID++ {
			if !included[nodeID] {
				excluded = append(excluded, nodeID)
			}
		}
		for _, nodeID := range excluded {
			if len(constraintsString) > 0 {
				constraintsString += ","
			}
			constraintsString += fmt.Sprintf("-node%d", nodeID)
		}
	}

	_, err := conn.ExecContext(ctx, fmt.Sprintf(
		`ALTER %s CONFIGURE ZONE USING num_replicas = %d, constraints = '[%s]', lease_preferences = '[%s]'`,
		target, cfg.replicas, constraintsString, cfg.leasePreference))
	require.NoError(t, err)
}

// configureAllZones will set zone configuration for all targets in the
// clusters.
func configureAllZones(t test.Test, ctx context.Context, conn *gosql.DB, cfg zoneConfig) {
	rows, err := conn.QueryContext(ctx, `SELECT target FROM [SHOW ALL ZONE CONFIGURATIONS]`)
	require.NoError(t, err)
	for rows.Next() {
		var target string
		require.NoError(t, rows.Scan(&target))
		configureZone(t, ctx, conn, target, cfg)
	}
	require.NoError(t, rows.Err())
}

// nodeMetric fetches the given metric value from the given node.
func nodeMetric(
	ctx context.Context, t test.Test, c cluster.Cluster, node int, metric string,
) float64 {
	var value float64
	conn := c.Conn(ctx, t.L(), node)
	defer conn.Close()
	err := conn.QueryRowContext(
		ctx, `SELECT value FROM crdb_internal.node_metrics WHERE name = $1`, metric).Scan(&value)
	require.NoError(t, err)
	return value
}

// sleepFor sleeps for the given duration. The test fails on context cancellation.
func sleepFor(ctx context.Context, t test.Test, duration time.Duration) {
	select {
	case <-time.After(duration):
	case <-ctx.Done():
		t.Fatalf("sleep failed: %s", ctx.Err())
	}
}

func failoverStartOpts() option.StartOpts {
	startOpts := option.DefaultStartOpts()
	startOpts.RoachprodOpts.ScheduleBackups = false
	return startOpts
}

func failoverRestartOpts() option.StartOpts {
	startOpts := option.DefaultStartOpts()
	startOpts.RoachprodOpts.ScheduleBackups = false
	startOpts.RoachprodOpts.SkipInit = true
	return startOpts
}

func getKVLabels(concurrency int, insertCount int, readPercent int) map[string]string {
	return map[string]string{
		"concurrency":  fmt.Sprintf("%d", concurrency),
		"insert_count": fmt.Sprintf("%d", insertCount),
		"read_percent": fmt.Sprintf("%d", readPercent),
	}
}
