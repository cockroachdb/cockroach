// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package asim_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestRunAllocatorSimulator(t *testing.T) {
	ctx := context.Background()
	settings := config.DefaultSimulationSettings()
	settings.Duration = 1000 * time.Second

	rwg := make([]workload.Generator, 1)
	rwg[0] = workload.TestCreateWorkloadGenerator(settings.Start, 500, 10)
	m := metrics.NewTracker(metrics.NewClusterMetricsTracker(os.Stdout))
	s := state.LoadConfig(state.ComplexConfig)

	sim := asim.NewSimulator(rwg, s, settings, m)
	sim.RunSim(ctx)
}

// TestAllocatorSimulatorSpeed tests that the simulation runs at a rate of at
// least 1.67 simulated minutes per wall clock second (1:100) for a 32 node
// cluster, with 32000 replicas. The workload is generating 16000 keys per
// second with a uniform distribution.
func TestAllocatorSimulatorSpeed(t *testing.T) {
	ctx := context.Background()

	skipString := "Skipping test under (?stress|?race) as it asserts on speed of the run."
	skip.UnderStress(t, skipString)
	skip.UnderStressRace(t, skipString)
	skip.UnderRace(t, skipString)

	settings := config.DefaultSimulationSettings()
	// Run each simulation for 5 minutes.
	settings.Duration = 5 * time.Minute

	stores := 32
	replsPerRange := 3
	replicasPerStore := 1000
	// NB: We want 1000 replicas per store, so the number of ranges required
	// will be 1/3 of the total replicas.
	ranges := (replicasPerStore * stores) / replsPerRange
	// NB: In this test we are using a uniform workload and expect to see at
	// most 3 splits occur due to range size, therefore the keyspace need not
	// be larger than 3 keys per range.
	keyspace := 3 * ranges

	sample := func() int64 {
		rwg := make([]workload.Generator, 1)
		rwg[0] = workload.TestCreateWorkloadGenerator(settings.Start, stores*500, int64(keyspace))
		m := metrics.NewTracker() // no output
		replicaDistribution := make([]float64, stores)

		// NB: Here create half of the stores with equal replica counts, the
		// other half have no replicas. This will lead to a flurry of activity
		// rebalancing towards these stores, based on the replica count
		// imbalance.
		for i := 0; i < stores/2; i++ {
			replicaDistribution[i] = 1.0 / float64(stores/2)
		}
		for i := stores / 2; i < stores; i++ {
			replicaDistribution[i] = 0
		}

		s := state.NewTestStateReplDistribution(replicaDistribution, ranges, replsPerRange, keyspace)
		sim := asim.NewSimulator(rwg, s, settings, m)

		startTime := timeutil.Now()
		sim.RunSim(ctx)
		return timeutil.Since(startTime).Nanoseconds()
	}

	// We sample 5 runs and take the minimum. The minimum is the cleanest
	// estimate here of performance, as any additional time over the minimum is
	// noise in a run.
	minRunTime := int64(math.MaxInt64)
	// TODO(kvoli): Hit perf wall again when running at a lower tick rate on
	// selected operartions.
	requiredRunTime := 0 * time.Second.Nanoseconds()
	samples := 3
	for i := 0; i < samples; i++ {
		if sampledRun := sample(); sampledRun < minRunTime {
			minRunTime = sampledRun
		}
		// NB: When we satisfy the test required runtime, exit early to avoid
		// additional runs.
		if minRunTime < requiredRunTime {
			break
		}
	}

	fmt.Println(time.Duration(minRunTime).Seconds())
	// TODO(kvoli): Add back the assertion on speed after triaging speed
	// issues.
}

func TestAllocatorSimulatorDeterministic(t *testing.T) {
	keyspan := 10000
	runs := 3

	testCases := []struct {
		desc                           string
		stores                         int
		ranges                         int
		replFactor                     int
		workloadRate                   int
		workloadRWRatio                float64
		LBMode                         int
		DisableSplitQueue              bool
		DisableReplicateQueue          bool
		DisableReplicateQueueTransfers bool
		skewed                         bool
		LBSplitThresh                  int
		SizeSplitThresh                int
		enabled                        bool
	}{
		{
			desc:                  "size based splitting",
			stores:                1,
			ranges:                1,
			replFactor:            1,
			workloadRate:          2000,
			workloadRWRatio:       0.01,
			DisableReplicateQueue: true,
			LBSplitThresh:         100000,  /* disable */
			SizeSplitThresh:       1 << 20, /* 1mb */
		},
		{
			desc:                  "load based splitting",
			stores:                1,
			ranges:                1,
			replFactor:            1,
			workloadRate:          2000,
			DisableReplicateQueue: true,
			LBSplitThresh:         250,
			SizeSplitThresh:       1 << 31, /* disabled */
		},
		{
			desc:                           "range count rebalancing",
			stores:                         7,
			ranges:                         1400,
			DisableSplitQueue:              true,
			skewed:                         true,
			DisableReplicateQueueTransfers: true,
		},
		{
			desc:              "range count rebalancing + transfers",
			stores:            7,
			ranges:            1400,
			DisableSplitQueue: true,
			skewed:            true,
			enabled:           true,
		},
		{
			desc:                  "load based lease rebalancing",
			stores:                7,
			ranges:                70,
			DisableSplitQueue:     true,
			DisableReplicateQueue: true,
			workloadRate:          7000,
			LBMode:                1,
			skewed:                true,
		},
		{
			desc:                  "load based lease+replica rebalancing",
			stores:                7,
			ranges:                70,
			DisableSplitQueue:     true,
			DisableReplicateQueue: true,
			workloadRate:          7000,
			LBMode:                2,
			skewed:                true,
		},
		{
			// NB: This test is enabled to run in CI. It tests every component
			// together, whereas the other tests isolate components of the
			// simulator and may be enabled for debugging purposes should this
			// fail.
			desc:         "everything",
			stores:       13,
			ranges:       13,
			LBMode:       2,
			skewed:       true,
			workloadRate: 20000,
			enabled:      false,
		},
	}

	for _, tc := range testCases {
		ctx := context.Background()
		t.Run(tc.desc, func(t *testing.T) {
			if !tc.enabled {
				t.Log("disabled, skipping")
				return
			}
			var refRun state.StoreUsageInfo
			for run := 0; run < runs; run++ {
				settings := config.DefaultSimulationSettings()
				settings.Duration = 10 * time.Minute
				settings.LBRebalancingMode = int64(tc.LBMode)
				if tc.replFactor == 0 {
					tc.replFactor = 3
				}
				if tc.DisableReplicateQueue {
					settings.ReplicateQueueEnabled = false
				}
				if tc.DisableSplitQueue {
					settings.SplitQueueEnabled = false
				}
				if tc.DisableReplicateQueueTransfers {
					settings.ReplQueueTransfersEnabled = false
				}
				if tc.LBSplitThresh > 0 {
					settings.SplitQPSThreshold = float64(tc.LBSplitThresh)
				}
				if tc.SizeSplitThresh > 0 {
					settings.RangeSizeSplitThreshold = int64(tc.SizeSplitThresh)
				}
				if tc.workloadRWRatio == 0 {
					tc.workloadRWRatio = 0.95
				}

				var s state.State
				if tc.skewed {
					s = state.NewTestStateSkewedDistribution(tc.stores, tc.ranges, tc.replFactor, keyspan)
				} else {
					s = state.NewTestStateEvenDistribution(tc.stores, tc.ranges, tc.replFactor, keyspan)
				}
				rwg := []workload.Generator{
					workload.NewRandomGenerator(
						settings.Start,
						settings.Seed,
						workload.NewUniformKeyGen(int64(keyspan), rand.New(rand.NewSource(settings.Seed))),
						float64(tc.workloadRate),
						tc.workloadRWRatio,
						256, /* block max size */
						128, /* block min size */
					),
				}
				m := metrics.NewTracker()
				sim := asim.NewSimulator(rwg, s, settings, m)
				sim.RunSim(ctx)
				usage := s.ClusterUsageInfo()
				if run == 0 {
					refRun = usage.AggregateUsageInfo()
					continue
				}
				require.Equal(t, refRun, usage.AggregateUsageInfo())
			}
		})
	}
}
