// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package democluster

type regionPair struct {
	regionA string
	regionB string
}

var regionToRegionToLatency map[string]map[string]int

func insertPair(pair regionPair, latency int) {
	regionToLatency, ok := regionToRegionToLatency[pair.regionA]
	if !ok {
		regionToLatency = make(map[string]int)
		regionToRegionToLatency[pair.regionA] = regionToLatency
	}
	regionToLatency[pair.regionB] = latency
}

// Round-trip latencies collected from http://cloudping.co on 2019-09-11.
var regionRoundTripLatencies = map[regionPair]int{
	{regionA: "us-east1", regionB: "us-west1"}:     66,
	{regionA: "us-east1", regionB: "europe-west1"}: 64,
	{regionA: "us-west1", regionB: "europe-west1"}: 146,
}

var regionOneWayLatencies = make(map[regionPair]int)

func init() {
	// We record one-way latencies next, because the logic in our delayingConn
	// and delayingListener is in terms of one-way network delays.
	for pair, latency := range regionRoundTripLatencies {
		regionOneWayLatencies[pair] = latency / 2
	}
	regionToRegionToLatency = make(map[string]map[string]int)
	for pair, latency := range regionOneWayLatencies {
		insertPair(pair, latency)
		insertPair(regionPair{
			regionA: pair.regionB,
			regionB: pair.regionA,
		}, latency)
	}
}
