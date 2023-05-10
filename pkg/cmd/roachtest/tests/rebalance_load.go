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
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
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
	meanCPUTolerance = 0.1
	// statSamplePeriod is the period at which timeseries stats are sampled.
	statSamplePeriod = 10 * time.Second
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
		startOpts := option.DefaultStartOptsNoBackups()
		roachNodes := c.Range(1, c.Spec().NodeCount-1)
		appNode := c.Node(c.Spec().NodeCount)
		numNodes := len(roachNodes)
		numStores := numNodes
		if c.Spec().SSDs > 1 && !c.Spec().RAID0 {
			numStores *= c.Spec().SSDs
			startOpts.RoachprodOpts.StoreCount = c.Spec().SSDs
		}
		// We want each store to end up with approximately storeToRangeFactor
		// (factor) leases such that the CPU load is evenly spread, e.g.
		//   (n * factor) -1 splits = factor * n ranges = factor leases per store
		// Note that we only assert on the CPU of each store w.r.t the mean, not
		// the lease count.
		splits := (numStores * storeToRangeFactor) - 1
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
			"--vmodule=store_rebalancer=5,allocator=5,allocator_scorer=5,replicate_queue=5")
		settings := install.MakeClusterSettings()
		if mixedVersion {
			predecessorVersion, err := version.PredecessorVersion(*t.BuildVersion())
			require.NoError(t, err)
			settings.Binary = uploadVersion(ctx, t, c, c.All(), predecessorVersion)
			// Upgrade some (or all) of the first N-1 CRDB nodes. We ignore the last
			// CRDB node (to leave at least one node on the older version), and the
			// app node.
			lastNodeToUpgrade := rand.Intn(c.Spec().NodeCount-2) + 1
			t.L().Printf("upgrading %d nodes to the current cockroach binary", lastNodeToUpgrade)
			nodesToUpgrade := c.Range(1, lastNodeToUpgrade)
			c.Start(ctx, t.L(), startOpts, settings, roachNodes)
			upgradeNodes(ctx, t, c, nodesToUpgrade, startOpts, clusterupgrade.MainVersion)
		} else {
			c.Put(ctx, t.Cockroach(), "./cockroach", roachNodes)
			c.Start(ctx, t.L(), startOpts, settings, roachNodes)
		}

		c.Put(ctx, t.DeprecatedWorkload(), "./workload", appNode)
		c.Run(ctx, appNode, fmt.Sprintf("./workload init kv --drop --splits=%d {pgurl:1}", splits))

		var m *errgroup.Group // see comment in version.go
		m, ctx = errgroup.WithContext(ctx)

		// Enable us to exit out of workload early when we achieve the desired CPU
		// balance. This drastically shortens the duration of the test in the
		// common case.
		ctx, cancel := context.WithCancel(ctx)

		m.Go(func() error {
			t.L().Printf("starting load generator\n")

			err := c.RunE(ctx, appNode, fmt.Sprintf(
				"./workload run kv --read-percent=95 --tolerate-errors --concurrency=%d "+
					"--duration=%v {pgurl:1-%d}",
				concurrency, maxDuration, len(roachNodes)))
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

			db := c.Conn(ctx, t.L(), 1)
			defer db.Close()

			t.Status("disable load based splitting")
			if err := disableLoadBasedSplitting(ctx, db); err != nil {
				return err
			}

			if _, err := db.ExecContext(
				ctx, `SET CLUSTER SETTING kv.allocator.load_based_rebalancing=$1::string`, rebalanceMode,
			); err != nil {
				return err
			}

			storeCPUFn, err := makeStoreCPUFn(ctx, c, t, numNodes, numStores)
			if err != nil {
				return err
			}

			var reason string
			var done bool
			for tBegin := timeutil.Now(); timeutil.Since(tBegin) <= maxDuration; {
				// Wait out the sample period initially to allow the timeseries to
				// populate meaningful information for the test to query.
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(statSamplePeriod):
				}

				clusterStoresCPU, err := storeCPUFn(ctx)
				if err != nil {
					t.L().Printf("unable to get the cluster stores CPU %s\n", err.Error())
				}

				done, reason = isLoadEvenlyDistributed(clusterStoresCPU, meanCPUTolerance)
				t.L().Printf("cpu %s", reason)
				if done {
					t.Status("successfully achieved CPU balance; waiting for kv to finish running")
					cancel()
					return nil
				}
			}

			return errors.Errorf("CPU not evenly balanced after timeout: %s", reason)
		})
		if err := m.Wait(); err != nil {
			t.Fatal(err)
		}
	}

	concurrency := 128

	r.Add(
		registry.TestSpec{
			Name:    `rebalance/by-load/leases`,
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(4), // the last node is just used to generate load
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.IsLocal() && runtime.GOARCH == "arm64" {
					t.Skip("Skip under ARM64. See https://github.com/cockroachdb/cockroach/issues/89268")
				}
				if c.IsLocal() {
					concurrency = 32
					fmt.Printf("lowering concurrency to %d in local testing\n", concurrency)
				}
				rebalanceLoadRun(ctx, t, c, "leases", 3*time.Minute, concurrency, false /* mixedVersion */)
			},
		},
	)
	r.Add(
		registry.TestSpec{
			Name:    `rebalance/by-load/leases/mixed-version`,
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(4), // the last node is just used to generate load
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.IsLocal() {
					concurrency = 32
					fmt.Printf("lowering concurrency to %d in local testing\n", concurrency)
				}
				rebalanceLoadRun(ctx, t, c, "leases", 3*time.Minute, concurrency, true /* mixedVersion */)
			},
		},
	)
	r.Add(
		registry.TestSpec{
			Name:    `rebalance/by-load/replicas`,
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(7), // the last node is just used to generate load
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.IsLocal() {
					concurrency = 32
					fmt.Printf("lowering concurrency to %d in local testing\n", concurrency)
				}
				rebalanceLoadRun(
					ctx, t, c, "leases and replicas", 5*time.Minute, concurrency, false, /* mixedVersion */
				)
			},
		},
	)
	r.Add(
		registry.TestSpec{
			Name:    `rebalance/by-load/replicas/mixed-version`,
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(7), // the last node is just used to generate load
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.IsLocal() {
					concurrency = 32
					fmt.Printf("lowering concurrency to %d in local testing\n", concurrency)
				}
				rebalanceLoadRun(
					ctx, t, c, "leases and replicas", 5*time.Minute, concurrency, true, /* mixedVersion */
				)
			},
		},
	)
	cSpec := r.MakeClusterSpec(7, spec.SSD(2)) // the last node is just used to generate load
	var skip string
	if cSpec.Cloud != spec.GCE {
		skip = fmt.Sprintf("multi-store tests are not supported on cloud %s", cSpec.Cloud)
	}
	r.Add(
		registry.TestSpec{
			Skip:    skip,
			Name:    `rebalance/by-load/replicas/ssds=2`,
			Owner:   registry.OwnerKV,
			Cluster: cSpec,
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
			url, startTime, now, statSamplePeriod, tsQueries)
		if err != nil {
			return nil, err
		}

		// Assume that stores on the same node will have sequential store IDs e.g.
		// when the stores per node is 2:
		//   node 1 = store 1, store 2 ... node N = store 2N-1, store 2N
		storesPerNode := numStores / numNodes
		storeCPUs := make([]float64, numStores)
		for node, result := range resp.Results {
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
