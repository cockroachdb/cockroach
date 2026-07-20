// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const (
	// storeToRangeFactor is the number of ranges to create per store in the
	// cluster.
	storeToRangeFactor = 10
	// meanCPUTolerance is the fractional slack allowed around the mean for the
	// overfull check. It has two components:
	//
	//   0.10 — the allocator's own convergence threshold, from
	//          kv.allocator.store_cpu_rebalance_threshold in
	//          pkg/kv/kvserver/allocator/base.go. A store is considered
	//          overfull once its CPU exceeds mean * (1 + 0.10).
	//   0.05 — a test-side noise buffer for TSDB sampling and the ~30-min
	//          decaying window used by replica load stats. The value the
	//          test reads lags the true load; this padding absorbs that.
	meanCPUTolerance = 0.10 + 0.05
	// minCPUThresholdDifferenceMs mirrors the allocator constant
	// MinCPUThresholdDifference in pkg/kv/kvserver/allocator/base.go (= 50ms/s,
	// converted from ns/s). It is the absolute floor for the overfull
	// classification: a store is considered overfull once its CPU exceeds
	// mean + max(mean * k, MinCPUThresholdDifference), where k is the
	// store_cpu_rebalance_threshold setting.
	minCPUThresholdDifferenceMs = 50.0
	// minCPUDifferenceForTransfersMs mirrors the allocator constant
	// MinCPUDifferenceForTransfers in pkg/kv/kvserver/allocator/base.go
	// (= 2 * MinCPUThresholdDifference = 100ms/s). It is the anti-thrashing
	// floor for lease transfers: bestStoreToMinimizeLoadDelta in
	// allocator_scorer.go declines any transfer where
	//
	//   (source_cpu - lease_cpu) - coldest_cpu < 100ms/s
	//
	// so the hottest store can legitimately sit up to that far above the
	// coldest at equilibrium.
	minCPUDifferenceForTransfersMs = 100.0
	// statSamplePeriod is the period at which timeseries stats are sampled.
	statSamplePeriod = 10 * time.Second
	// stableDuration is the duration which the cluster's load must remain
	// balanced for to pass.
	stableDuration = time.Minute
	// leaseOnlyRebalanceDuration is the duration for which the cluster's load
	// must balance within in order to pass the lease transfer only rebalancing
	// variation.
	leaseOnlyRebalanceDuration = 10 * time.Minute
	// leaseAndReplicaRebalanceDuration is the duration for which the cluster's
	// load must balance within in order to pass the replica and lease
	// rebalancing variation.
	leaseAndReplicaRebalanceDuration = 15 * time.Minute
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
		// This test asserts on the distribution of replica-attributed CPU between
		// stores in the cluster. Having backups also running could lead to unrelated
		// flakes - disable backup schedule.
		startOpts := option.NewStartOpts(option.NoBackupSchedule)
		roachNodes := c.Range(1, c.Spec().NodeCount-1)
		appNode := c.Node(c.Spec().NodeCount)
		numNodes := len(roachNodes)
		numStores := numNodes
		if c.Spec().DiskCount > 1 && !c.Spec().RAID0 {
			numStores *= c.Spec().DiskCount
			startOpts.RoachprodOpts.StoreCount = c.Spec().DiskCount
		}

		settings := install.MakeClusterSettings()
		settings.ClusterSettings["kv.allocator.load_based_rebalancing"] = rebalanceMode
		settings.ClusterSettings["kv.range_split.by_load_enabled"] = "false"

		// Take a 10s profile every minute.
		settings.ClusterSettings["server.cpu_profile.duration"] = "10s"
		settings.ClusterSettings["server.cpu_profile.interval"] = "1m"
		settings.ClusterSettings["server.cpu_profile.cpu_usage_combined_threshold"] = "1" // basically always true
		settings.ClusterSettings["server.cpu_profile.total_dump_size_limit"] = "256 MiB"

		if mixedVersion {
			mvt := mixedversion.NewTest(ctx, t, t.L(), c, roachNodes, mixedversion.NeverUseFixtures,
				mixedversion.ClusterSettingOption(
					install.ClusterSettingsOption(settings.ClusterSettings),
				),
				// Only use the latest version of each release to work around #127029.
				mixedversion.AlwaysUseLatestPredecessors,
				// There is a known edge case in the mixed version framework when there
				// is only one upgrade that can run user hooks (25.1 -> 25.2) and the
				// fromVersion (25.1) is skippable. The framework always enforces at
				// least one skip upgrade if enabled (24.3 -> 25.2), which would lead to
				// zero upgrades running user hooks. Instead, disable skip upgrades.
				// TODO(#151408): Remove when the framework handles this case.
				mixedversion.DisableSkipVersionUpgrades,
				// There have been many performance improvements in versions 25.1.0+.
				// The assertion uses replica-attributed CPU, which is less sensitive
				// to version differences than host CPU, but keep the floor since it
				// also works around other mixed-version issues (e.g. #150603).
				mixedversion.MinimumSupportedVersion("v25.1.0"),
			)
			mvt.OnStartup("maybe enable split/scatter on tenant",
				func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
					return enableTenantSplitScatter(l, r, h)
				})
			mvt.InMixedVersion("rebalance load run",
				func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
					return rebalanceByLoad(
						ctx, t, l, c, rebalanceMode, maxDuration, concurrency, appNode, numStores, numNodes)
				})
			mvt.Run()
		} else {
			c.Start(ctx, t.L(), startOpts, settings, roachNodes)
			require.NoError(t, rebalanceByLoad(
				ctx, t, t.L(), c, rebalanceMode, maxDuration,
				concurrency, appNode, numStores, numNodes,
			))
		}

	}
	// Concurrency is set high enough to produce meaningful
	// replica-attributed CPU on all stores so the rebalancer has a
	// signal to act on.
	concurrency := 512
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
				rebalanceLoadRun(ctx, t, c, "leases", leaseOnlyRebalanceDuration, concurrency, false /* mixedVersion */)
			},
		},
	)
	r.Add(
		registry.TestSpec{
			Name:             `rebalance/by-load/leases/mixed-version`,
			Owner:            registry.OwnerKV,
			Cluster:          r.MakeClusterSpec(4), // the last node is just used to generate load
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
			Randomized:       true,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.IsLocal() {
					concurrency = 32
					fmt.Printf("lowering concurrency to %d in local testing\n", concurrency)
				}
				rebalanceLoadRun(ctx, t, c, "leases", leaseOnlyRebalanceDuration, concurrency, true /* mixedVersion */)
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
					ctx, t, c, "leases and replicas", leaseAndReplicaRebalanceDuration, concurrency, false, /* mixedVersion */
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
			Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
			Randomized:       true,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.IsLocal() {
					concurrency = 32
					t.L().Printf("lowering concurrency to %d in local testing", concurrency)
				}
				rebalanceLoadRun(
					ctx, t, c, "leases and replicas", leaseAndReplicaRebalanceDuration, concurrency, true, /* mixedVersion */
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
				spec.Disks(2),
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
					ctx, t, c, "leases and replicas", leaseAndReplicaRebalanceDuration, concurrency, false, /* mixedVersion */
				)
			},
		},
	)
}

func rebalanceByLoad(
	ctx context.Context,
	t test.Test,
	l *logger.Logger,
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
	// Note that we only assert on the replica-attributed CPU of each store w.r.t
	// the mean, not the lease count.
	splits := (numStores * storeToRangeFactor) - 1
	c.Run(ctx, option.WithNodes(appNode), fmt.Sprintf("./cockroach workload init kv --drop --splits=%d {pgurl:1}", splits))

	db := c.Conn(ctx, l, 1)
	defer db.Close()

	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, l, db))

	// Enable us to exit out of workload early when we achieve the desired CPU
	// balance. This drastically shortens the duration of the test in the
	// common case.
	ctx, cancel := context.WithCancel(ctx)
	m := t.NewErrorGroup(task.WithContext(ctx))

	m.Go(func(ctx context.Context, l *logger.Logger) error {
		l.Printf("starting load generator")
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
	}, task.Name("load-generator"))

	m.Go(func(ctx context.Context, l *logger.Logger) error {
		l.Printf("checking for CPU balance")

		storeCPUFn, err := makeStoreCPUFn(ctx, t, l, c, numStores)
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
				l.Printf("unable to get the cluster stores CPU: %v", err)
				continue
			}
			var curIsBalanced bool
			curIsBalanced, reason = isLoadEvenlyDistributed(clusterStoresCPU, meanCPUTolerance, storeToRangeFactor)
			l.Printf("cpu %s", reason)
			if !prevIsBalanced && curIsBalanced {
				balancedStartTime = now
			}
			prevIsBalanced = curIsBalanced
			if prevIsBalanced && now.Sub(balancedStartTime) > stableDuration {
				l.Printf("successfully achieved CPU balance; waiting for kv to finish running")
				cancel()
				return nil
			}
		}
		return errors.Errorf("CPU not evenly balanced after timeout: %s", reason)
	}, task.Name("cpu-balance"))
	return m.WaitE()
}

// makeStoreCPUFn returns a function which can be called to gather the
// replica-attributed CPU of the cluster stores in ms/s. Store IDs are assumed
// to be sequential starting at 1.
func makeStoreCPUFn(
	ctx context.Context, t test.Test, l *logger.Logger, c cluster.Cluster, numStores int,
) (func(ctx context.Context) ([]float64, error), error) {
	adminURLs, err := c.ExternalAdminUIAddr(ctx, l, c.Node(1), option.VirtualClusterName(install.SystemInterfaceName))
	if err != nil {
		return nil, err
	}
	url := adminURLs[0]
	startTime := timeutil.Now()
	tsQueries := make([]tsQuery, numStores)
	for i := range tsQueries {
		tsQueries[i] = tsQuery{
			name:      "cr.store.rebalancing.cpunanospersecond",
			queryType: total,
			sources:   []string{fmt.Sprintf("%d", i+1)},
			tenantID:  roachpb.SystemTenantID,
		}
	}

	return func(ctx context.Context) ([]float64, error) {
		now := timeutil.Now()
		resp, err := getMetricsWithSamplePeriod(
			ctx, c, t, url, install.SystemInterfaceName, startTime, now, statSamplePeriod, tsQueries)
		if err != nil {
			return nil, err
		}

		storeCPUs := make([]float64, numStores)
		for storeIdx, result := range resp.Results {
			if len(result.Datapoints) == 0 {
				// If any store has no datapoints, there isn't much point looking at
				// others because the comparison is useless.
				return nil, errors.Newf("store %d has no CPU datapoints", storeIdx+1)
			}
			// Take the latest CPU data point only.
			cpuNanosPerSecond := result.Datapoints[len(result.Datapoints)-1].Value
			if cpuNanosPerSecond < 0 {
				return nil, errors.Newf(
					"store %d has negative replica-attributed CPU ts datapoint: %v [resp=%+v]",
					storeIdx+1, cpuNanosPerSecond, resp)
			}
			// Convert ns/s to ms/s for human-readable logs (e.g. 2400 ms/s ≈ 2.4 cores).
			storeCPUs[storeIdx] = cpuNanosPerSecond / 1e6
		}
		return storeCPUs, nil
	}, nil
}

// isLoadEvenlyDistributed checks whether the observed per-store CPU is
// consistent with an SMA lease-rebalance equilibrium. The rebalancer is at
// legitimate equilibrium in either of two regimes:
//
//	(a) hot-check:   no store exceeds the SMA overfull threshold, so there
//	                 is nothing to shed.
//	(b) gap-check:   some store is overfull but the hot-cold gap is within
//	                 the friction bound, so the rebalancer has shed
//	                 everything it usefully can.
//
// The check passes if either holds; only when both fail is the load
// meaningfully unbalanced.
//
// The hot-check mirrors OverfullLoadThresholds in
// pkg/kv/kvserver/allocator/allocatorimpl/threshold.go:
//
//	overfullBound = mean + max(mean * tolerance, MinCPUThresholdDifference)
//
// where tolerance folds in the allocator's own k = 0.10 plus a small
// stats-lag buffer, and MinCPUThresholdDifference = 50 ms/s.
//
// The gap-check mirrors the friction check in bestStoreToMinimizeLoadDelta
// (allocator_scorer.go): (source - lease) - coldest >= MinCPUDifferenceForTransfers.
// Rearranged: hot - cold <= MinCPUDifferenceForTransfers + lease. Under kv
// workload's uniform per-key load, lease ≈ mean/leasesPerStore. The 2x
// multiplier absorbs stats-lag noise and the fact that leases on a hot store
// often carry more than the cluster-average load.
//
// Stores below (mean - overfullSlack) are reported for visibility but never
// fail the check on their own — the SMA rebalancer only sheds from overloaded
// sources, so a cold-side outlier can indicate a rebalancer problem only when
// paired with a hot store above the overfull threshold, which is exactly the
// case the gap-check catches. See prior fix in b7376523652.
func isLoadEvenlyDistributed(
	loads []float64, tolerance float64, leasesPerStore int,
) (ok bool, reason string) {
	mean := arithmeticMean(loads)
	// If the mean is zero, there's nothing meaningful to assert on. Return early
	// that the load isn't evenly distributed.
	if mean == 0 {
		return false, "no load: mean=0"
	}

	overfullSlack := max(mean*tolerance, minCPUThresholdDifferenceMs)
	overfullBound := mean + overfullSlack
	lb := mean - overfullSlack

	gapBound := minCPUDifferenceForTransfersMs + 2*mean/float64(leasesPerStore)

	// Partition the loads into above, below and within the overfull bounds of
	// the load mean, while tracking the hottest and coldest observed loads for
	// the gap-check.
	above, below, within := []int{}, []int{}, []int{}
	hot, cold := loads[0], loads[0]
	for i, load := range loads {
		storeID := i + 1
		if load > hot {
			hot = load
		}
		if load < cold {
			cold = load
		}
		if load > overfullBound {
			above = append(above, storeID)
		} else if load < lb {
			below = append(below, storeID)
		} else {
			within = append(within, storeID)
		}
	}

	hotOk := hot <= overfullBound
	gap := hot - cold
	gapOk := gap <= gapBound
	ok = hotOk || gapOk

	boundsStr := fmt.Sprintf(
		"mean=%.0fms/s overfull=%.0f (tolerance=%.1f%% floor=%.0fms/s) gap=%.0f (bound=%.0f)",
		mean, overfullBound, 100*tolerance, minCPUThresholdDifferenceMs, gap, gapBound,
	)
	switch {
	case !ok:
		reason = fmt.Sprintf(
			"unbalanced %s\n\tabove overfull = %s\n\tbelow (info)   = %s\n\twithin         = %s\n",
			boundsStr,
			formatLoads(above, loads, mean),
			formatLoads(below, loads, mean),
			formatLoads(within, loads, mean),
		)
	case len(above) > 0 || len(below) > 0:
		reason = fmt.Sprintf(
			"balanced (gap ok) %s\n\tabove overfull = %s\n\tbelow (info)   = %s\n\twithin         = %s\n",
			boundsStr,
			formatLoads(above, loads, mean),
			formatLoads(below, loads, mean),
			formatLoads(within, loads, mean),
		)
	default:
		reason = fmt.Sprintf("balanced %s\n\tstores=%s\n",
			boundsStr, formatLoads(within, loads, mean))
	}
	return
}

func formatLoads(storeIDs []int, loads []float64, mean float64) string {
	fmtLoads := make([]string, len(storeIDs))
	for i, storeID := range storeIDs {
		load := loads[storeID-1]
		fmtLoads[i] = fmt.Sprintf("s%d: %.0fms/s (%+3.1f%%)",
			storeID, load, (load-mean)/mean*100,
		)
	}
	return fmt.Sprintf("[%s]", strings.Join(fmtLoads, ", "))
}
