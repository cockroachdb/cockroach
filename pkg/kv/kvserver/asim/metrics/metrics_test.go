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
	c := &state.ReplicaChange{RangeID: 1, Add: 5, Remove: 1, Author: 1}
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
	//2022-03-21 11:00:10.5 +0000 UTC,60,9450,1820361,59850,11477501,3059,588693,56405,10820302,5,17,111388
	//2022-03-21 11:00:20.5 +0000 UTC,60,18450,3541809,116850,22421857,5596,1073717,100806,19342738,11,33,340401
	//2022-03-21 11:00:30.5 +0000 UTC,60,27450,5269644,173850,33387925,7932,1522814,135532,26032786,16,49,731823
	//2022-03-21 11:00:40.5 +0000 UTC,60,36450,7004808,230850,44347918,9977,1916098,160560,30847748,21,68,1374322
	//2022-03-21 11:00:50.5 +0000 UTC,60,45450,8739927,287850,55290060,11725,2253998,176485,33906430,26,82,2002718
	//2022-03-21 11:01:00.5 +0000 UTC,60,54450,10448463,344850,66224618,13296,2552128,187604,36040690,26,88,2315857
	//2022-03-21 11:01:10.5 +0000 UTC,60,63450,12179874,401850,77163034,14873,2856179,198134,38069801,27,88,2315857
	//2022-03-21 11:01:20.5 +0000 UTC,60,72450,13913004,458850,88092982,16422,3152640,208512,40061046,29,88,2315857
	//2022-03-21 11:01:30.5 +0000 UTC,60,81450,15640074,515850,99027317,17918,3439136,218972,42060618,29,88,2315857
	//2022-03-21 11:01:40.5 +0000 UTC,60,90450,17357547,572850,109974999,19502,3742953,229331,44048014,29,88,2315857
	//2022-03-21 11:01:50.5 +0000 UTC,60,99450,19092753,629850,120926724,21035,4039165,239694,46036197,29,88,2315857
	//2022-03-21 11:02:00.5 +0000 UTC,60,108450,20816334,686850,131880745,22592,4337072,250100,48035069,29,88,2315857
	//2022-03-21 11:02:10.5 +0000 UTC,60,117450,22550427,743850,142814096,24125,4631698,260563,50044343,29,88,2315857
	//2022-03-21 11:02:20.5 +0000 UTC,60,126450,24277041,800850,153772025,25633,4921052,270947,52034867,29,88,2315857
	//2022-03-21 11:02:30.5 +0000 UTC,60,135450,26015577,857850,164733990,27165,5217639,281388,54045764,29,88,2315857
	//2022-03-21 11:02:40.5 +0000 UTC,60,144450,27732711,914850,175682002,28741,5518647,291892,56062795,29,88,2315857
	//2022-03-21 11:02:50.5 +0000 UTC,60,153450,29463351,971850,186623184,30245,5808012,302234,58057251,29,88,2315857
	//2022-03-21 11:03:00.5 +0000 UTC,60,162450,31189377,1028850,197571300,31772,6102230,312575,60038719,29,88,2315857
	//2022-03-21 11:03:10.5 +0000 UTC,60,171450,32915184,1085850,208519385,33297,6395689,323053,62057333,29,88,2315857

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
	//2022-03-21 11:00:00.5 +0000 UTC,1,0,150,28945,2850,545210,60,60,0,0,0,0,0
	//2022-03-21 11:00:00.5 +0000 UTC,2,0,150,28945,0,0,60,0,0,0,0,0,0
	//2022-03-21 11:00:00.5 +0000 UTC,3,0,150,28945,0,0,60,0,0,0,0,0,0
	//2022-03-21 11:00:00.5 +0000 UTC,4,0,0,0,0,0,0,0,0,0,0,0,0
	//2022-03-21 11:00:00.5 +0000 UTC,5,0,0,0,0,0,0,0,0,0,0,0,0
	//2022-03-21 11:00:00.5 +0000 UTC,6,0,0,0,0,0,0,0,0,0,0,0,0
	//2022-03-21 11:00:10.5 +0000 UTC,1,5084,3044,586569,56405,10820302,54,51,5,9,0,50071,0
	//2022-03-21 11:00:10.5 +0000 UTC,2,0,3001,578088,48,8788,54,1,0,0,0,0,0
	//2022-03-21 11:00:10.5 +0000 UTC,3,105,3059,588693,632,120547,55,1,0,0,0,0,0
	//2022-03-21 11:00:10.5 +0000 UTC,4,0,136,26610,595,113567,6,2,0,2,38914,15752,0
	//2022-03-21 11:00:10.5 +0000 UTC,5,105,110,21161,1277,243903,5,2,0,4,31314,29351,0
	//2022-03-21 11:00:10.5 +0000 UTC,6,96,100,19240,893,170394,6,3,0,2,41160,16214,0
	//2022-03-21 11:00:20.5 +0000 UTC,1,4197,5583,1071455,100806,19342738,49,42,11,19,0,194831,0
	//2022-03-21 11:00:20.5 +0000 UTC,2,299,5563,1068188,2874,554943,50,5,0,0,0,0,0
	//2022-03-21 11:00:20.5 +0000 UTC,3,99,5596,1073717,1619,308563,48,2,0,0,0,0,0
	//2022-03-21 11:00:20.5 +0000 UTC,4,303,593,114144,3626,693354,11,4,0,5,112638,63565,0
	//2022-03-21 11:00:20.5 +0000 UTC,5,303,562,108029,3757,721460,11,3,0,4,112200,29351,0
	//2022-03-21 11:00:20.5 +0000 UTC,6,296,553,106276,4168,800799,11,4,0,5,115563,52654,0
	//2022-03-21 11:00:30.5 +0000 UTC,1,3191,7932,1522814,135532,26032786,44,32,16,29,0,436465,0
	//2022-03-21 11:00:30.5 +0000 UTC,2,495,7872,1510731,7566,1459288,44,5,0,0,0,0,0
	//2022-03-21 11:00:30.5 +0000 UTC,3,300,7913,1519188,4639,886543,43,4,0,0,0,0,0
	//2022-03-21 11:00:30.5 +0000 UTC,4,615,1290,248515,8963,1714647,16,7,0,7,240238,119472,0
	//2022-03-21 11:00:30.5 +0000 UTC,5,497,1229,235676,7903,1517420,17,5,0,6,261026,77098,0
	//2022-03-21 11:00:30.5 +0000 UTC,6,503,1214,232720,9247,1777241,16,7,0,7,230559,98788,0
	//2022-03-21 11:00:40.5 +0000 UTC,1,2194,9977,1916098,160560,30847748,37,21,21,39,0,777534,0
	//2022-03-21 11:00:40.5 +0000 UTC,2,491,9922,1905790,12747,2452253,38,7,0,0,0,0,0
	//2022-03-21 11:00:40.5 +0000 UTC,3,400,9969,1915837,8413,1611080,37,4,0,0,0,0,0
	//2022-03-21 11:00:40.5 +0000 UTC,4,824,2232,430625,16846,3231128,22,8,0,10,430245,232145,0
	//2022-03-21 11:00:40.5 +0000 UTC,5,700,2189,420804,14465,2778861,23,9,0,8,465934,134973,0
	//2022-03-21 11:00:40.5 +0000 UTC,6,889,2161,415654,17819,3426848,23,11,0,11,478143,229670,0
	//2022-03-21 11:00:50.5 +0000 UTC,1,1390,11725,2253445,176485,33906430,34,14,26,49,0,1213722,0
	//2022-03-21 11:00:50.5 +0000 UTC,2,784,11682,2247723,20143,3869128,32,9,0,0,0,0,0
	//2022-03-21 11:00:50.5 +0000 UTC,3,497,11720,2253998,12759,2442665,32,5,0,0,0,0,0
	//2022-03-21 11:00:50.5 +0000 UTC,4,925,3460,666249,26027,4995920,28,10,0,11,704703,273013,0
	//2022-03-21 11:00:50.5 +0000 UTC,5,1004,3436,658983,23634,4543180,27,10,0,10,643936,238712,0
	//2022-03-21 11:00:50.5 +0000 UTC,6,1105,3427,659529,28802,5532737,27,12,0,12,654079,277271,0
	//2022-03-21 11:01:00.5 +0000 UTC,1,1095,13296,2552128,187604,36040690,31,11,26,54,0,1477625,0
	//2022-03-21 11:01:00.5 +0000 UTC,2,891,13193,2534594,28602,5487903,30,9,0,0,0,0,0
	//2022-03-21 11:01:00.5 +0000 UTC,3,495,13272,2547217,17432,3340060,31,5,0,0,0,0,0
	//2022-03-21 11:01:00.5 +0000 UTC,4,1128,4980,954901,36578,7020283,30,11,0,11,803105,273013,0
	//2022-03-21 11:01:00.5 +0000 UTC,5,1088,4847,927462,33475,6435169,29,11,0,10,757297,238712,0
	//2022-03-21 11:01:00.5 +0000 UTC,6,1309,4862,932161,41159,7900513,29,13,0,13,755455,326507,0
	//2022-03-21 11:01:10.5 +0000 UTC,1,1096,14873,2856179,198134,38069801,31,11,26,54,0,1477625,0
	//2022-03-21 11:01:10.5 +0000 UTC,2,893,14744,2833098,37130,7120522,30,9,0,0,0,0,0
	//2022-03-21 11:01:10.5 +0000 UTC,3,493,14805,2840497,22056,4232361,31,6,0,0,0,0,0
	//2022-03-21 11:01:10.5 +0000 UTC,4,1126,6504,1247404,47309,9076235,30,10,1,11,803105,273013,0
	//2022-03-21 11:01:10.5 +0000 UTC,5,1090,6240,1195987,43788,8413415,29,11,0,10,757297,238712,0
	//2022-03-21 11:01:10.5 +0000 UTC,6,1301,6284,1206709,53433,10250700,29,13,0,13,755455,326507,0
	//2022-03-21 11:01:20.5 +0000 UTC,1,1096,16422,3152640,208512,40061046,31,11,26,54,0,1477625,0
	//2022-03-21 11:01:20.5 +0000 UTC,2,894,16249,3122072,45751,8768269,30,9,0,0,0,0,0
	//2022-03-21 11:01:20.5 +0000 UTC,3,600,16333,3134807,28510,5476566,31,8,0,0,0,0,0
	//2022-03-21 11:01:20.5 +0000 UTC,4,1022,8003,1536595,56860,10906505,30,10,1,11,803105,273013,0
	//2022-03-21 11:01:20.5 +0000 UTC,5,1090,7684,1475247,54161,10399502,29,11,0,10,757297,238712,0
	//2022-03-21 11:01:20.5 +0000 UTC,6,1094,7759,1491643,65056,12481094,29,11,2,13,755455,326507,0
}

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
			ranges:            2800,
			DisableSplitQueue: true,
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
		{
			desc:         "everything",
			stores:       7,
			ranges:       7,
			LBMode:       2,
			skewed:       true,
			workloadRate: 21000,
		},
	}

	for _, tc := range testCases {
		ctx := context.Background()
		if !tc.enabled {
			log.Infof(ctx, "example disabled, skipping.")
			continue
		}
		settings := config.DefaultSimulationSettings()
		settings.Duration = 30 * time.Minute
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
		state.AddEmptyRanges(s, 45)

		var keyGen workload.KeyGenerator
		if tc.workloadSkew {
			keyGen = workload.NewZipfianKeyGen(int64(keyspan), 1.1, 1, rand.New(rand.NewSource(settings.Seed)))
		} else {
			keyGen = workload.NewUniformKeyGen(int64(keyspan), rand.New(rand.NewSource(settings.Seed)))
		}
		rwg := []workload.Generator{
			workload.NewRandomGenerator(
				settings.Start,
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
		sim := asim.NewSimulator(rwg, s, settings, m)
		sim.RunSim(ctx)
		fmt.Println(metrics.PlotSeries(collected, "replicas", "leases", "qps", "lease_moves", "replica_moves", "replica_b_sent"))
	}
	// Output:
}
