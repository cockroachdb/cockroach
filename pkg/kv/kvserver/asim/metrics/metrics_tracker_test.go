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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func Example_noWriters() {
	ctx := context.Background()
	start := state.TestingStartTime()
	s := state.LoadConfig(state.ComplexConfig)
	m := metrics.NewTracker()

	m.Tick(ctx, start, s)
	// Output:
}

func Example_tickEmptyState() {
	ctx := context.Background()
	start := state.TestingStartTime()
	s := state.LoadConfig(state.ComplexConfig)
	m := metrics.NewTracker(metrics.NewClusterMetricsTracker(os.Stdout))

	m.Tick(ctx, start, s)
	// Output:
	//tick,c_ranges,c_write,c_write_b,c_read,c_read_b,s_ranges,s_write,s_write_b,s_read,s_read_b,c_lease_moves,c_replica_moves,c_replica_b_moves
	//2022-03-21 11:00:00 +0000 UTC,1,0,0,0,0,0,0,0,0,0,0,0
}

func TestTickEmptyState(t *testing.T) {
	ctx := context.Background()
	start := state.TestingStartTime()
	s := state.LoadConfig(state.ComplexConfig)

	var buf bytes.Buffer
	m := metrics.NewTracker(metrics.NewClusterMetricsTracker(&buf))

	m.Tick(ctx, start, s)

	expected :=
		"tick,c_ranges,c_write,c_write_b,c_read,c_read_b,s_ranges,s_write,s_write_b,s_read,s_read_b,c_lease_moves,c_replica_moves,c_replica_b_moves\n" +
			"2022-03-21 11:00:00 +0000 UTC,1,0,0,0,0,0,0,0,0,0,0,0\n"
	require.Equal(t, expected, buf.String())
}

func Example_multipleWriters() {
	ctx := context.Background()
	start := state.TestingStartTime()
	s := state.LoadConfig(state.ComplexConfig)
	m := metrics.NewTracker(metrics.NewClusterMetricsTracker(os.Stdout, os.Stdout))

	m.Tick(ctx, start, s)
	// Output:
	//tick,c_ranges,c_write,c_write_b,c_read,c_read_b,s_ranges,s_write,s_write_b,s_read,s_read_b,c_lease_moves,c_replica_moves,c_replica_b_moves
	//tick,c_ranges,c_write,c_write_b,c_read,c_read_b,s_ranges,s_write,s_write_b,s_read,s_read_b,c_lease_moves,c_replica_moves,c_replica_b_moves
	//2022-03-21 11:00:00 +0000 UTC,1,0,0,0,0,0,0,0,0,0,0,0
	//2022-03-21 11:00:00 +0000 UTC,1,0,0,0,0,0,0,0,0,0,0,0
}

func Example_leaseTransfer() {
	ctx := context.Background()
	start := state.TestingStartTime()
	s := state.LoadConfig(state.ComplexConfig)
	m := metrics.NewTracker(metrics.NewClusterMetricsTracker(os.Stdout))
	changer := state.NewReplicaChanger()
	changer.Push(state.TestingStartTime(), &state.LeaseTransferChange{
		RangeID:        1,
		TransferTarget: 2,
		Author:         1,
		Wait:           0,
	})
	changer.Tick(state.TestingStartTime(), s)
	m.Tick(ctx, start, s)
	// Output:
	//tick,c_ranges,c_write,c_write_b,c_read,c_read_b,s_ranges,s_write,s_write_b,s_read,s_read_b,c_lease_moves,c_replica_moves,c_replica_b_moves
	//2022-03-21 11:00:00 +0000 UTC,1,0,0,0,0,0,0,0,0,1,0,0
}

func Example_rebalance() {
	ctx := context.Background()
	start := state.TestingStartTime()
	s := state.LoadConfig(state.ComplexConfig)
	m := metrics.NewTracker(metrics.NewClusterMetricsTracker(os.Stdout))

	// Apply load, to get a replica size greater than 0.
	le := workload.LoadBatch{workload.LoadEvent{Writes: 1, WriteSize: 7, Reads: 2, ReadSize: 9, Key: 5}}
	s.ApplyLoad(le)

	// Do the rebalance.
	c := &state.ReplicaChange{RangeID: 1, Add: 5, Remove: 1}
	c.Apply(s)

	m.Tick(ctx, start, s)
	// Output:
	//tick,c_ranges,c_write,c_write_b,c_read,c_read_b,s_ranges,s_write,s_write_b,s_read,s_read_b,c_lease_moves,c_replica_moves,c_replica_b_moves
	//2022-03-21 11:00:00 +0000 UTC,1,3,21,2,9,1,7,2,9,0,1,7
}

func Example_workload() {
	ctx := context.Background()
	settings := config.DefaultSimulationSettings()
	settings.Duration = 200 * time.Second
	keyspace := int64(10000)
	stores := 6
	workloadRate := stores * 1000

	rwg := make([]workload.Generator, 1)
	rwg[0] = workload.TestCreateWorkloadGenerator(settings.Start, workloadRate, keyspace)
	m := metrics.NewTracker(metrics.NewClusterMetricsTracker(os.Stdout))

	s := state.NewTestStateSkewedDistribution(stores, stores*10, 3, int(keyspace))
	sim := asim.NewSimulator(rwg, s, settings, m)
	sim.RunSim(ctx)
	// Output:
	//tick,c_ranges,c_write,c_write_b,c_read,c_read_b,s_ranges,s_write,s_write_b,s_read,s_read_b,c_lease_moves,c_replica_moves,c_replica_b_moves
	//2022-03-21 11:00:00.5 +0000 UTC,60,450,86835,2850,545210,150,28945,2850,545210,0,0,0
	//2022-03-21 11:00:10.5 +0000 UTC,60,9450,1810380,59850,11480828,2962,568173,55989,10741258,0,14,8541
	//2022-03-21 11:00:20.5 +0000 UTC,60,18450,3543534,116850,22421282,5565,1069761,105432,20226405,0,28,152980
	//2022-03-21 11:00:30.5 +0000 UTC,60,27450,5275839,173850,33385860,7891,1517039,149074,28628263,0,43,461516
	//2022-03-21 11:00:40.5 +0000 UTC,60,36450,7010592,230850,44345990,9856,1894552,185760,35675718,0,58,903005
	//2022-03-21 11:00:50.5 +0000 UTC,60,45450,8739450,287850,55290219,11694,2248023,217952,41858137,0,77,1644356
	//2022-03-21 11:01:00.5 +0000 UTC,60,54450,10463025,344850,66219764,13137,2525614,248008,47627814,0,92,2374709
	//2022-03-21 11:01:10.5 +0000 UTC,60,63450,12187833,401850,77160381,14604,2809085,276402,53076921,1,98,2736533
	//2022-03-21 11:01:20.5 +0000 UTC,60,72450,13916178,458850,88091924,16125,3101632,305688,58690100,13,99,2793293
	//2022-03-21 11:01:30.5 +0000 UTC,60,81450,15638331,515850,99027898,17697,3403406,335102,64333934,13,104,3171107
	//2022-03-21 11:01:40.5 +0000 UTC,60,90450,17366265,572850,109972093,19229,3697744,364565,69992191,13,109,3624299
	//2022-03-21 11:01:50.5 +0000 UTC,60,99450,19100757,629850,120924056,20740,3988267,394046,75664882,13,109,3624299
	//2022-03-21 11:02:00.5 +0000 UTC,60,108450,20812980,686850,131881863,22251,4278356,423101,81246228,13,109,3624299
	//2022-03-21 11:02:10.5 +0000 UTC,60,117450,22542990,743850,142816575,23796,4574851,449563,86327536,17,109,3624299
	//2022-03-21 11:02:20.5 +0000 UTC,60,126450,24269025,800850,153774697,25329,4870008,475949,91401761,17,109,3624299
	//2022-03-21 11:02:30.5 +0000 UTC,60,135450,26012895,857850,164734884,26905,5174290,502427,96503379,17,109,3624299
	//2022-03-21 11:02:40.5 +0000 UTC,60,144450,27737094,914850,175680541,28475,5474980,529098,101621596,17,109,3624299
	//2022-03-21 11:02:50.5 +0000 UTC,60,153450,29463957,971850,186622982,30047,5774430,555656,106719504,17,109,3624299
	//2022-03-21 11:03:00.5 +0000 UTC,60,162450,31195332,1028850,197569315,31555,6065311,582170,111822545,17,109,3624299
	//2022-03-21 11:03:10.5 +0000 UTC,60,171450,32923164,1085850,208516725,33129,6366988,604959,116202619,21,109,3624299
}

func Example_storeMetricsWorkload() {
	ctx := context.Background()
	settings := config.DefaultSimulationSettings()
	settings.Duration = 90 * time.Second
	keyspace := int64(10000)
	stores := 6
	workloadRate := stores * 1000

	rwg := make([]workload.Generator, 1)
	rwg[0] = workload.TestCreateWorkloadGenerator(settings.Start, workloadRate, keyspace)
	m := metrics.NewTracker(metrics.NewStoreMetricsTracker(os.Stdout))
	s := state.NewTestStateSkewedDistribution(stores, stores*10, 3, int(keyspace))
	sim := asim.NewSimulator(rwg, s, settings, m)
	sim.RunSim(ctx)
	// Output:
	//tick,store,qps,write,write_b,read,read_b,replicas,leases,lease_moves,replica_moves,replica_b_rcvd,replica_b_sent,range_splits
	//2022-03-21 11:00:00.5 +0000 UTC,1,10,150,28945,2850,545210,60,60,0,0,0,0,0
	//2022-03-21 11:00:00.5 +0000 UTC,2,0,150,28945,0,0,60,0,0,0,0,0,0
	//2022-03-21 11:00:00.5 +0000 UTC,3,0,150,28945,0,0,60,0,0,0,0,0,0
	//2022-03-21 11:00:00.5 +0000 UTC,4,0,0,0,0,0,0,0,0,0,0,0,0
	//2022-03-21 11:00:00.5 +0000 UTC,5,0,0,0,0,0,0,0,0,0,0,0,0
	//2022-03-21 11:00:00.5 +0000 UTC,6,0,0,0,0,0,0,0,0,0,0,0,0
	//2022-03-21 11:00:10.5 +0000 UTC,1,189,2962,568173,55989,10741258,56,56,0,9,0,5364,0
	//2022-03-21 11:00:10.5 +0000 UTC,2,0,2920,559818,0,0,55,0,0,0,0,0,0
	//2022-03-21 11:00:10.5 +0000 UTC,3,0,2876,550909,0,0,55,0,0,0,0,0,0
	//2022-03-21 11:00:10.5 +0000 UTC,4,223,257,49088,962,185337,5,1,0,1,3636,849,0
	//2022-03-21 11:00:10.5 +0000 UTC,5,272,203,38906,1927,367327,4,2,0,4,1958,2328,0
	//2022-03-21 11:00:10.5 +0000 UTC,6,681,232,43486,972,186906,5,1,0,0,2947,0,0
	//2022-03-21 11:00:20.5 +0000 UTC,1,332,5565,1069761,105432,20226405,52,52,0,19,0,112493,0
	//2022-03-21 11:00:20.5 +0000 UTC,2,0,5414,1040065,0,0,50,0,0,0,0,0,0
	//2022-03-21 11:00:20.5 +0000 UTC,3,0,5400,1036915,0,0,50,0,0,0,0,0,0
	//2022-03-21 11:00:20.5 +0000 UTC,4,429,759,146015,3837,740363,10,3,0,3,57848,19325,0
	//2022-03-21 11:00:20.5 +0000 UTC,5,2416,668,127609,5709,1094517,9,4,0,4,52048,2328,0
	//2022-03-21 11:00:20.5 +0000 UTC,6,170,644,123169,1872,359997,9,1,0,2,43084,18834,0
	//2022-03-21 11:00:30.5 +0000 UTC,1,423,7891,1517039,149074,28628263,46,46,0,29,0,308338,0
	//2022-03-21 11:00:30.5 +0000 UTC,2,0,7748,1489478,0,0,46,0,0,0,0,0,0
	//2022-03-21 11:00:30.5 +0000 UTC,3,0,7687,1477516,0,0,45,0,0,0,0,0,0
	//2022-03-21 11:00:30.5 +0000 UTC,4,1027,1446,279124,8586,1651522,15,5,0,5,159036,69482,0
	//2022-03-21 11:00:30.5 +0000 UTC,5,1461,1360,260108,12279,2353535,15,7,0,7,178612,64862,0
	//2022-03-21 11:00:30.5 +0000 UTC,6,348,1318,252574,3911,752540,13,2,0,2,123868,18834,0
	//2022-03-21 11:00:40.5 +0000 UTC,1,462,9805,1887049,185760,35675718,39,39,0,39,0,600072,0
	//2022-03-21 11:00:40.5 +0000 UTC,2,0,9811,1886591,0,0,41,0,0,0,0,0,0
	//2022-03-21 11:00:40.5 +0000 UTC,3,0,9856,1894552,0,0,42,0,0,0,0,0,0
	//2022-03-21 11:00:40.5 +0000 UTC,4,1026,2360,455111,15286,2943849,19,7,0,7,268932,123278,0
	//2022-03-21 11:00:40.5 +0000 UTC,5,1880,2356,451642,20986,4033528,20,9,0,9,327470,127595,0
	//2022-03-21 11:00:40.5 +0000 UTC,6,681,2262,435647,8818,1692895,19,5,0,3,306603,52060,0
	//2022-03-21 11:00:50.5 +0000 UTC,1,488,11552,2225357,217952,41858137,34,34,0,49,0,993468,0
	//2022-03-21 11:00:50.5 +0000 UTC,2,0,11508,2214054,0,0,33,0,0,0,0,0,0
	//2022-03-21 11:00:50.5 +0000 UTC,3,0,11694,2248023,0,0,36,0,0,0,0,0,0
	//2022-03-21 11:00:50.5 +0000 UTC,4,2961,3515,676405,23734,4568647,25,9,0,11,502793,277175,0
	//2022-03-21 11:00:50.5 +0000 UTC,5,2045,3657,699902,31426,6032427,26,11,0,13,558150,283420,0
	//2022-03-21 11:00:50.5 +0000 UTC,6,703,3524,675709,14738,2831008,26,6,0,4,583413,90293,0
	//2022-03-21 11:01:00.5 +0000 UTC,1,534,13111,2523421,248008,47627814,32,32,0,59,0,1473439,0
	//2022-03-21 11:01:00.5 +0000 UTC,2,0,12897,2480537,0,0,28,0,0,0,0,0,0
	//2022-03-21 11:01:00.5 +0000 UTC,3,0,13137,2525614,0,0,28,0,0,0,0,0,0
	//2022-03-21 11:01:00.5 +0000 UTC,4,1106,5035,966783,32330,6218085,30,9,0,13,746965,376257,0
	//2022-03-21 11:01:00.5 +0000 UTC,5,1597,5202,997003,42902,8223963,31,12,0,15,802375,385897,0
	//2022-03-21 11:01:00.5 +0000 UTC,6,1087,5068,969667,21610,4149902,31,7,0,5,825369,139116,0
	//2022-03-21 11:01:10.5 +0000 UTC,1,567,14604,2809085,276402,53076921,30,30,0,63,0,1714322,0
	//2022-03-21 11:01:10.5 +0000 UTC,2,0,14235,2734426,0,0,27,1,0,0,0,0,0
	//2022-03-21 11:01:10.5 +0000 UTC,3,0,14395,2768811,0,0,25,0,0,0,0,0,0
	//2022-03-21 11:01:10.5 +0000 UTC,4,885,6674,1280503,40764,7836791,33,8,1,13,931853,376257,0
	//2022-03-21 11:01:10.5 +0000 UTC,5,1579,6851,1315504,55314,10605967,32,13,0,15,866322,385897,0
	//2022-03-21 11:01:10.5 +0000 UTC,6,940,6691,1279504,29370,5640702,33,8,0,7,938358,260057,0
	//2022-03-21 11:01:20.5 +0000 UTC,1,979,16125,3101632,305688,58690100,31,31,0,63,56760,1714322,0
	//2022-03-21 11:01:20.5 +0000 UTC,2,1042,15599,2996390,6688,1281823,27,7,0,0,0,0,0
	//2022-03-21 11:01:20.5 +0000 UTC,3,1264,15627,3006401,5884,1130797,25,6,0,0,0,0,0
	//2022-03-21 11:01:20.5 +0000 UTC,4,648,8405,1611170,46405,8918523,33,6,3,13,931853,376257,0
	//2022-03-21 11:01:20.5 +0000 UTC,5,634,8411,1615172,61078,11713212,32,6,7,15,866322,385897,0
	//2022-03-21 11:01:20.5 +0000 UTC,6,418,8283,1585413,33107,6357469,32,4,3,8,938358,316817,0
}

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
	}{
		{
			desc:                  "workload",
			stores:                1,
			ranges:                1,
			replFactor:            1,
			workloadRate:          5000,
			DisableReplicateQueue: true,
			DisableSplitQueue:     true,
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
		},
		{
			desc:              "range count rebalancing",
			stores:            7,
			ranges:            1400,
			DisableSplitQueue: true,
			skewed:            true,
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
		},
		{
			desc:              "load based lease+replica rebalancing",
			stores:            7,
			ranges:            50,
			DisableSplitQueue: true,
			workloadRate:      10000,
			LBMode:            2,
			skewed:            true,
		},
		{
			desc:         "one range per store, all components",
			stores:       9,
			ranges:       9,
			LBMode:       2,
			skewed:       true,
			workloadRate: 6000,
		},
	}

	for _, tc := range testCases {
		ctx := context.Background()
		if !tc.enabled {
			log.Infof(ctx, "example disabled, skipping.")
			return
		}
		settings := config.DefaultSimulationSettings()
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
		collected := make(map[string][][]float64)
		m := metrics.NewTracker(metrics.NewRatedStoreMetricListener(metrics.NewTimeSeriesMetricListener(&collected, tc.stores)))
		sim := asim.NewSimulator(rwg, s, settings, m)
		sim.RunSim(ctx)
		fmt.Println(metrics.PlotSeries(collected, "replicas", "leases", "qps", "lease_moves", "replica_moves"))
	}
}
