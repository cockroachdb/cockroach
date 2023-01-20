// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metrics_test

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Example_plotMetrics may be used to plot the metrics for a run of the
// simulator. It is a placeholder and all examples are disabled by default,
// setting the enabled flag = true will enable specific cases.
func Example_plotMetrics() {
	keyspan := 10000

	testCases := []struct {
		desc                  string
		stores                int
		ranges                int
		replFactor            int
		workloadRate          int
		workloadRWRatio       float64
		LBMode                int
		DisableSplitQueue     bool
		DisableReplicateQueue bool
		skewed                bool
		LBSplitThresh         int
		SizeSplitThresh       int
		enabled               bool
		fixedQPSPerRange      float64
		workloadSkew          bool
	}{
		{
			desc:                  "workload",
			stores:                1,
			ranges:                1,
			replFactor:            1,
			workloadRate:          5000,
			DisableReplicateQueue: true,
			DisableSplitQueue:     true,
			enabled:               false,
		},
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
			enabled:               false,
		},
		{
			desc:                  "load based splitting",
			stores:                1,
			ranges:                1,
			replFactor:            1,
			workloadRate:          2500,
			DisableReplicateQueue: true,
			LBSplitThresh:         1000,
			SizeSplitThresh:       1 << 31, /* disabled */
			enabled:               false,
		},
		{
			desc:              "range count rebalancing",
			stores:            7,
			ranges:            2800,
			skewed:            true,
			DisableSplitQueue: true,
			enabled:           false,
		},
		{
			desc:                  "load based lease rebalancing",
			stores:                3,
			ranges:                33,
			DisableSplitQueue:     true,
			DisableReplicateQueue: true,
			fixedQPSPerRange:      300,
			LBMode:                1,
			skewed:                true,
			enabled:               false,
		},
		{
			desc:                  "load based lease+replica rebalancing",
			stores:                7,
			ranges:                70,
			DisableSplitQueue:     true,
			DisableReplicateQueue: false,
			LBMode:                2,
			skewed:                true,
			fixedQPSPerRange:      1000,
			enabled:               false,
		},
		{
			desc:              "replicate queue and load based lease+replica rebalancing",
			stores:            7,
			ranges:            50,
			DisableSplitQueue: true,
			workloadRate:      10000,
			LBMode:            2,
			skewed:            true,
			enabled:           false,
		},
		{
			desc:         "one range per store, all components",
			stores:       9,
			ranges:       9,
			LBMode:       2,
			skewed:       true,
			workloadRate: 6000,
			enabled:      false,
		},
		{
			desc:         "1000 ranges per store, all components, skewed workload",
			stores:       9,
			ranges:       9000,
			LBMode:       2,
			skewed:       false,
			workloadSkew: true,
			workloadRate: 27000,
			enabled:      true,
		},
		{
			desc:         "all components",
			ranges:       1,
			stores:       9,
			LBMode:       2,
			skewed:       true,
			workloadRate: 27000,
			enabled:      false,
		},
	}

	for _, tc := range testCases {
		ctx := context.Background()
		if !tc.enabled {
			log.Infof(ctx, "example disabled, skipping.")
			continue
		}
		settings := config.DefaultSimulationSettings()
		start := state.TestingStartTime()
		end := start.Add(30 * time.Minute)

		settings.LBRebalancingMode = int64(tc.LBMode)
		if tc.replFactor == 0 {
			tc.replFactor = 3
		}
		if tc.DisableReplicateQueue {
			settings.ReplicateQueueDisabled = true
		}
		if tc.DisableSplitQueue {
			settings.SplitQueueDisabled = true
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
		if tc.fixedQPSPerRange > 0 {
			for _, r := range s.Ranges() {
				state.TestingSetRangeQPS(s, r.RangeID(), tc.fixedQPSPerRange)
			}
		}
		// state.AddEmptyRanges(s, 45)

		var keyGen workload.KeyGenerator
		if tc.workloadSkew {
			keyGen = workload.NewZipfianKeyGen(int64(keyspan), 1.1, 1, rand.New(rand.NewSource(settings.Seed)))
		} else {
			keyGen = workload.NewUniformKeyGen(int64(keyspan), rand.New(rand.NewSource(settings.Seed)))
		}
		rwg := []workload.Generator{
			workload.NewRandomGenerator(
				start,
				settings.Seed,
				keyGen,
				float64(tc.workloadRate),
				tc.workloadRWRatio,
				256, /* block max size */
				128, /* block min size */
			),
		}
		collected := make(map[string][][]float64)
		m := metrics.NewTracker(
			metrics.NewRatedStoreMetricListener(metrics.NewTimeSeriesMetricListener(&collected, tc.stores)),
			//metrics.NewRatedStoreMetricListener(metrics.NewStoreMetricsTracker(os.Stdout)),
		)
		sim := asim.NewSimulator(start, end, 500*time.Millisecond, 10*time.Second, rwg, s, state.NewReplicaChanger(), settings, m)
		sim.RunSim(ctx)
		fmt.Println(metrics.PlotSeries(collected, "replicas", "leases", "qps", "lease_moves", "replica_moves", "replica_b_sent"))
	}
	// Output:
}
