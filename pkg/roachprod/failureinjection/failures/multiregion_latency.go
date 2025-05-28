// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

type (
	// Region represents a region in which a node can be located.
	Region string
	// RegionToNodes is a convenience type that maps regions to their nodes.
	RegionToNodes map[Region][]install.Node
	// LatencyMap is a mapping of all the one way latencies between regions,
	// i.e. 0.5 * RTT.
	LatencyMap map[Region]map[Region]time.Duration
	// roundTripLatency represents the RTT from RegionA to RegionB.
	roundTripLatency struct {
		RegionA Region
		RegionB Region
		Latency time.Duration
	}
)

const (
	USEast     Region = "us-east"
	USWest     Region = "us-west"
	EuropeWest Region = "europe-west"
)

var AllRegions = []Region{
	USEast,
	USWest,
	EuropeWest,
}

// createLatencyMap creates a LatencyMap from a slice of roundTripLatency.
// N.B. Latencies are assumed to be symmetric. The latency from region A
// to region B is the same as the latency from region B to region A, i.e.
// 0.5 * RTT.
func createLatencyMap(roundTripLatency []roundTripLatency) LatencyMap {
	latencyMap := make(map[Region]map[Region]time.Duration)
	for _, rtt := range roundTripLatency {
		if _, ok := latencyMap[rtt.RegionA]; !ok {
			latencyMap[rtt.RegionA] = make(map[Region]time.Duration)
		}
		if _, ok := latencyMap[rtt.RegionB]; !ok {
			latencyMap[rtt.RegionB] = make(map[Region]time.Duration)
		}
		latencyMap[rtt.RegionA][rtt.RegionB] = rtt.Latency / 2
		latencyMap[rtt.RegionB][rtt.RegionA] = rtt.Latency / 2
	}
	return latencyMap
}

// defaultLatencyMap returns a LatencyMap with estimated latencies between
// VMs in different regions. Numbers are copied from the GCP console at
// of the time of this comment:
// https://console.cloud.google.com/net-intelligence/performance/global-dashboard
//
// N.B. Assumes the actual roachprod cluster was created in the same region/zone, i.e.
// the original latency is negligible (gce claims sub 1ms for intra zone).
var defaultLatencyMap = func() LatencyMap {
	regionLatencies := []roundTripLatency{
		{
			RegionA: USEast,
			RegionB: USWest,
			Latency: 64 * time.Millisecond,
		},
		{
			RegionA: USEast,
			RegionB: EuropeWest,
			Latency: 86 * time.Millisecond,
		},
		{
			RegionA: USWest,
			RegionB: EuropeWest,
			Latency: 132 * time.Millisecond,
		},
	}
	return createLatencyMap(regionLatencies)
}()

// MakeNetworkLatencyArgs returns the NetworkLatencyArgs to simulate the latency
// of a multiregion cluster with the provided mapping.
func MakeNetworkLatencyArgs(regionToNodeMap RegionToNodes) (NetworkLatencyArgs, error) {
	artificialLatencies := make([]ArtificialLatency, 0)
	LatencyMappingNotFoundErr := func(regionA, regionB Region) error {
		return fmt.Errorf("no latency mapping found from region %s to %s", regionA, regionB)
	}
	for regionA, srcNodes := range regionToNodeMap {
		for regionB, destNodes := range regionToNodeMap {
			if regionA == regionB {
				continue
			}
			if defaultLatencyMap[regionA] == nil {
				return NetworkLatencyArgs{}, LatencyMappingNotFoundErr(regionA, regionB)
			}
			delay, ok := defaultLatencyMap[regionA][regionB]
			if !ok {
				return NetworkLatencyArgs{}, LatencyMappingNotFoundErr(regionA, regionB)
			}

			artificialLatencies = append(artificialLatencies, ArtificialLatency{
				Source:      srcNodes,
				Destination: destNodes,
				Delay:       delay,
			})
		}
	}

	args := NetworkLatencyArgs{
		ArtificialLatencies: artificialLatencies,
	}

	return args, nil
}
