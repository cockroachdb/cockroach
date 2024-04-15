// Copyright 2018 The Cockroach Authors.
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
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

const (
	// storeToRangeFactor is the number of ranges to create per store in the
	// cluster.
	storeToRangeFactor = 5
	// meanCPUTolerance is the tolerance applied when checking normalized (0-100)
	// CPU percent utilization of stores against the mean. In multi-store tests,
	// the same CPU utilization will be reported for stores on the same node. The
	// acceptable range for CPU w.r.t the mean is:
	//
	//  mean_tolerance = mean * meanCPUTolerance
	//  [mean - mean_tolerance, mean + mean_tolerance].
	//
	// The store rebalancer watches the replica CPU load and balances within
	// +-10% of the mean (by default). To reduce noise, add a buffer (+10%)
	// ontop.
	// TODO(kvoli): Reduce the buffer once we attribute other CPU usage to a
	// store via a node, such a SQL execution, stats collection and compactions.
	// See #109768.
	meanCPUTolerance = 0.20
	// statSamplePeriod is the period at which timeseries stats are sampled.
	statSamplePeriod = 10 * time.Second
	// stableDuration is the duration which the cluster's load must remain
	// balanced for to pass.
	stableDuration = time.Minute
)

func registerRebalanceLoad(r registry.Registry) {
	// This test creates a single table for kv to use and splits the table to
	// have 5 ranges for every node in the cluster. Because even brand new
	// clusters start with 40+ ranges in them, the number of new ranges in kv's
	// table is small enough that it typically won't trigger significant
	// rebalancing of leases in the cluster based on lease count alone. We let kv
	// generate a lot of load against the ranges such that we'd expect load-based
	// rebalancing to distribute the load evenly across the nodes in the cluster.
	rebalanceLoadRun := func(
		ctx context.Context,
		t test.Test,
		c cluster.Cluster,
		rebalanceMode string,
		maxDuration time.Duration,
		concurrency int,
		mixedVersion bool,
	) {
		// This test asserts on the distribution of CPU utilization between nodes
		// in the cluster, having backups also running could lead to unrelated
		// flakes - disable backup schedule.
		startOpts := option.NewStartOpts(option.NoBackupSchedule)
		roachNodes := c.Range(1, c.Spec().NodeCount-1)
		appNode := c.Node(c.Spec().NodeCount)
		numNodes := len(roachNodes)
		numStores := numNodes
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
			"--vmodule=store_rebalancer=5,allocator=5,allocator_scorer=5,replicate_queue=5")
		if c.Spec().SSDs > 1 && !c.Spec().RAID0 {
			numStores *= c.Spec().SSDs
			startOpts.RoachprodOpts.StoreCount = c.Spec().SSDs
		}

		settings := install.MakeClusterSettings()
		settings.ClusterSettings["kv.allocator.load_based_rebalancing"] = rebalanceMode
		settings.ClusterSettings["kv.range_split.by_load_enabled"] = "false"

		if mixedVersion {
			mvt := mixedversion.NewTest(ctx, t, t.L(), c, roachNodes, mixedversion.NeverUseFixtures,
				mixedversion.ClusterSettingOption(
					install.ClusterSettingsOption(settings.ClusterSettings),
					// In 23.1, the `user_id` field was added to `system.web_sessions`.
					// If the cluster is migrating to 23.1, auth-session login will not
					// be aware of this new field and authentication will fail.
					// TODO(DarrylWong): When 22.2 is no longer supported, we won't run
					// into the above issue anymore and can enable secure clusters.
					install.SecureOption(false),
				),
			)
			mvt.InMixedVersion("rebalance load run",
				func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
					return rebalanceByLoad(
						ctx, t, c, rebalanceMode, maxDuration, concurrency, appNode, numStores, numNodes)
				})
			mvt.Run()
		} else {
			// Enable collecting CPU profiles when the CPU utilization exceeds 90%.
			// This helps debug failures which occur as a result of mismatches
			// between allocation (QPS/replica CPU) and hardware signals e.g. see
			// #111900. The setting names changed between v22.2 and v23.1, we can't
			// easily setup CPU profiling in mixed version tests.
			//
			// TODO(kvoli): Remove this setup once CPU profiling is enabled by default
			// on perf roachtests #97699.
			settings.ClusterSettings["server.cpu_profile.duration"] = "2s"
			settings.ClusterSettings["server.cpu_profile.interval"] = "2"
			settings.ClusterSettings["server.cpu_profile.cpu_usage_combined_threshold"] = "90"
			c.Start(ctx, t.L(), startOpts, settings, roachNodes)
			require.NoError(t, rebalanceByLoad(
				ctx, t, c, rebalanceMode, maxDuration,
				concurrency, appNode, numStores, numNodes,
			))
		}

	}
	concurrency := 128
	r.Add(
		registry.TestSpec{
			Name:             `rebalance/by-load/leases`,
			Owner:            registry.OwnerKV,
			Cluster:          r.MakeClusterSpec(4), // the last node is just used to generate load
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           registry.MetamorphicLeases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.IsLocal() {
					concurrency = 32
					fmt.Printf("lowering concurrency to %d in local testing\n", concurrency)
				}
				rebalanceLoadRun(ctx, t, c, "leases", 10*time.Minute, concurrency, false /* mixedVersion */)
			},
		},
	)
	r.Add(
		registry.TestSpec{
			Name:             `rebalance/by-load/leases/mixed-version`,
			Owner:            registry.OwnerKV,
			Cluster:          r.MakeClusterSpec(4), // the last node is just used to generate load
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.IsLocal() {
					concurrency = 32
					fmt.Printf("lowering concurrency to %d in local testing\n", concurrency)
				}
				rebalanceLoadRun(ctx, t, c, "leases", 10*time.Minute, concurrency, true /* mixedVersion */)
			},
		},
	)
	r.Add(
		registry.TestSpec{
			Name:             `rebalance/by-load/replicas`,
			Owner:            registry.OwnerKV,
			Cluster:          r.MakeClusterSpec(7), // the last node is just used to generate load
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           registry.MetamorphicLeases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.IsLocal() {
					concurrency = 32
					fmt.Printf("lowering concurrency to %d in local testing\n", concurrency)
				}
				rebalanceLoadRun(
					ctx, t, c, "leases and replicas", 10*time.Minute, concurrency, false, /* mixedVersion */
				)
			},
		},
	)
	r.Add(
		registry.TestSpec{
			Name:             `rebalance/by-load/replicas/mixed-version`,
			Owner:            registry.OwnerKV,
			Cluster:          r.MakeClusterSpec(7), // the last node is just used to generate load
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.IsLocal() {
					concurrency = 32
					fmt.Printf("lowering concurrency to %d in local testing\n", concurrency)
				}
				rebalanceLoadRun(
					ctx, t, c, "leases and replicas", 10*time.Minute, concurrency, true, /* mixedVersion */
				)
			},
		},
	)

	r.Add(
		registry.TestSpec{
			Name:  `rebalance/by-load/replicas/ssds=2`,
			Owner: registry.OwnerKV,
			Cluster: r.MakeClusterSpec(7,
				// When using ssd > 1, only local SSDs on AMD64 arch are compatible
				// currently. See #121951.
				spec.SSD(2),
				spec.Arch(vm.ArchAMD64),
				spec.PreferLocalSSD(),
			), // the last node is just used to generate load
			CompatibleClouds: registry.OnlyGCE,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           registry.MetamorphicLeases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.IsLocal() {
					t.Fatal("cannot run multi-store in local mode")
				}
				rebalanceLoadRun(
					ctx, t, c, "leases and replicas", 10*time.Minute, concurrency, false, /* mixedVersion */
				)
			},
		},
	)
}

func rebalanceByLoad(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	rebalanceMode string,
	maxDuration time.Duration,
	concurrency int,
	appNode option.NodeListOption,
	numStores, numNodes int,
) error {
	// We want each store to end up with approximately storeToRangeFactor
	// (factor) leases such that the CPU load is evenly spread, e.g.
	//   (n * factor) -1 splits = factor * n ranges = factor leases per store
	// Note that we only assert on the CPU of each store w.r.t the mean, not
	// the lease count.
	splits := (numStores * storeToRangeFactor) - 1
	c.Run(ctx, option.WithNodes(appNode), fmt.Sprintf("./cockroach workload init kv --drop --splits=%d {pgurl:1}", splits))

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	require.NoError(t, WaitFor3XReplication(ctx, t, t.L(), db))

	var m *errgroup.Group
	m, ctx = errgroup.WithContext(ctx)

	// Enable us to exit out of workload early when we achieve the desired CPU
	// balance. This drastically shortens the duration of the test in the
	// common case.
	ctx, cancel := context.WithCancel(ctx)

	m.Go(func() error {
		t.L().Printf("starting load generator\n")
		err := c.RunE(ctx, option.WithNodes(appNode), fmt.Sprintf(
			"./cockroach workload run kv --read-percent=95 --tolerate-errors --concurrency=%d "+
				"--duration=%v {pgurl:1-%d}",
			concurrency, maxDuration, numNodes))
		if errors.Is(ctx.Err(), context.Canceled) {
			// We got canceled either because CPU balance was achieved or the
			// other worker hit an error. In either case, it's not this worker's
			// fault.
			return nil
		}
		return err
	})

	m.Go(func() error {
		t.Status("checking for CPU balance")

		storeCPUFn, err := makeStoreCPUFn(ctx, c, t, numNodes, numStores)
		if err != nil {
			return err
		}

		var reason string
		var balancedStartTime time.Time
		var prevIsBalanced bool
		for tBegin := timeutil.Now(); timeutil.Since(tBegin) <= maxDuration; {
			// Wait out the sample period initially to allow the timeseries to
			// populate meaningful information for the test to query.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(statSamplePeriod):
			}

			now := timeutil.Now()
			clusterStoresCPU, err := storeCPUFn(ctx)
			if err != nil {
				t.L().Printf("unable to get the cluster stores CPU %s\n", err.Error())
				continue
			}
			var curIsBalanced bool
			curIsBalanced, reason = isLoadEvenlyDistributed(clusterStoresCPU, meanCPUTolerance)
			t.L().Printf("cpu %s", reason)
			if !prevIsBalanced && curIsBalanced {
				balancedStartTime = now
			}
			prevIsBalanced = curIsBalanced
			if prevIsBalanced && now.Sub(balancedStartTime) > stableDuration {
				t.Status("successfully achieved CPU balance; waiting for kv to finish running")
				cancel()
				return nil
			}
		}
		return errors.Errorf("CPU not evenly balanced after timeout: %s", reason)
	})
	return m.Wait()
}

// makeStoreCPUFn returns a function which can be called to gather the CPU of
// the cluster stores. When there are multiple stores per node, stores on the
// same node will report identical CPU.
func makeStoreCPUFn(
	octx context.Context, c cluster.Cluster, t test.Test, numNodes, numStores int,
) (func(ctx context.Context) ([]float64, error), error) {
	adminURLs, err := c.ExternalAdminUIAddr(octx, t.L(), c.Node(1))
	if err != nil {
		return nil, err
	}
	url := adminURLs[0]
	startTime := timeutil.Now()
	tsQueries := make([]tsQuery, numNodes)
	for i := range tsQueries {
		tsQueries[i] = tsQuery{
			name:      "cr.node.sys.cpu.combined.percent-normalized",
			queryType: total,
			sources:   []string{fmt.Sprintf("%d", i+1)},
		}
	}

	return func(ctx context.Context) ([]float64, error) {
		now := timeutil.Now()
		resp, err := getMetricsWithSamplePeriod(
			ctx, c, t, url, startTime, now, statSamplePeriod, tsQueries)
		if err != nil {
			return nil, err
		}

		// Assume that stores on the same node will have sequential store IDs e.g.
		// when the stores per node is 2:
		//   node 1 = store 1, store 2 ... node N = store 2N-1, store 2N
		storesPerNode := numStores / numNodes
		storeCPUs := make([]float64, numStores)
		for node, result := range resp.Results {
			if len(result.Datapoints) == 0 {
				// If any node has no datapoints, there isn't much point looking at
				// others because the comparison is useless.
				return nil, errors.Newf("node %d has no CPU datapoints", node)
			}
			// Take the latest CPU data point only.
			cpu := result.Datapoints[len(result.Datapoints)-1].Value
			nodeIdx := node * storesPerNode
			for storeOffset := 0; storeOffset < storesPerNode; storeOffset++ {
				// The values will be a normalized float in [0,1.0], scale to a
				// percentage [0,100].
				storeCPUs[nodeIdx+storeOffset] = cpu * 100
			}
		}
		return storeCPUs, nil
	}, nil
}

// isLoadEvenlyDistributed checks whether the load for the stores given are
// within tolerance of the mean. If the store loads are, true is returned as
// well as reason, otherwise false. The function expects the loads to be
// indexed to store IDs, see makeStoreCPUFn for example format.
func isLoadEvenlyDistributed(loads []float64, tolerance float64) (ok bool, reason string) {
	mean := arithmeticMean(loads)
	// If the mean is zero, there's nothing meaningful to assert on. Return early
	// that the load isn't evenly distributed.
	if mean == 0 {
		return false, "no load: mean=0"
	}

	meanTolerance := mean * tolerance
	lb := mean - meanTolerance
	ub := mean + meanTolerance

	// Partiton the loads into above, below and within the tolerance bounds of
	// the load mean.
	above, below, within := []int{}, []int{}, []int{}
	for i, load := range loads {
		storeID := i + 1
		if load > ub {
			above = append(above, storeID)
		} else if load < lb {
			below = append(below, storeID)
		} else {
			within = append(within, storeID)
		}
	}

	boundsStr := fmt.Sprintf("mean=%.1f tolerance=%.1f%% (±%.1f) bounds=[%.1f, %.1f]",
		mean, 100*tolerance, meanTolerance, lb, ub)
	if len(below) > 0 || len(above) > 0 {
		ok = false
		reason = fmt.Sprintf(
			"outside bounds %s\n\tbelow  = %s\n\twithin = %s\n\tabove  = %s\n",
			boundsStr,
			formatLoads(below, loads, mean),
			formatLoads(within, loads, mean),
			formatLoads(above, loads, mean),
		)
	} else {
		ok = true
		reason = fmt.Sprintf("within bounds %s\n\tstores=%s\n",
			boundsStr, formatLoads(within, loads, mean))
	}
	return
}

func formatLoads(storeIDs []int, loads []float64, mean float64) string {
	fmtLoads := make([]string, len(storeIDs))
	for i, storeID := range storeIDs {
		load := loads[storeID-1]
		fmtLoads[i] = fmt.Sprintf("s%d: %d (%+3.1f%%)",
			storeID, int(load), (load-mean)/mean*100,
		)
	}
	return fmt.Sprintf("[%s]", strings.Join(fmtLoads, ", "))
}
