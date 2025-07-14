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
	// LatencyMap is a mapping of all the one way latencies between regions.
	LatencyMap map[Region]map[Region]time.Duration
	// oneWayLatency represents the latency from RegionA to RegionB.
	oneWayLatency struct {
		RegionA Region
		RegionB Region
		Latency time.Duration
	}
)

const (
	RegionUSEast     Region = "us-east"
	RegionUSWest     Region = "us-west"
	RegionEuropeWest Region = "europe-west"
)

// createLatencyMap creates a LatencyMap from a slice of oneWayLatency.
// N.B. Latencies are assumed to be symmetric. Only one direction should be
// as specified since the latency from A to B is also injected from B to A,
// i.e. the RTT is double that of the one way latency.
func createLatencyMap(oneWayLatencies []oneWayLatency) LatencyMap {
	latencyMap := make(map[Region]map[Region]time.Duration)
	for _, latencies := range oneWayLatencies {
		if _, ok := latencyMap[latencies.RegionA]; !ok {
			latencyMap[latencies.RegionA] = make(map[Region]time.Duration)
		}
		if _, ok := latencyMap[latencies.RegionB]; !ok {
			latencyMap[latencies.RegionB] = make(map[Region]time.Duration)
		}
		latencyMap[latencies.RegionA][latencies.RegionB] = latencies.Latency
		latencyMap[latencies.RegionB][latencies.RegionA] = latencies.Latency
	}
	return latencyMap
}

// defaultLatencyMap returns a LatencyMap with estimated latencies between
// VMs in different regions. Numbers are copied from GCE documentation at
// of the time of this comment.
//
// N.B. Assumes the actual roachprod cluster was created in the same region/zone, i.e.
// the original latency is negligible (gce claims sub 1ms for intra zone).
var defaultLatencyMap = func() LatencyMap {
	regionLatencies := []oneWayLatency{
		{
			RegionA: RegionUSEast,
			RegionB: RegionUSWest,
			Latency: 32 * time.Millisecond,
		},
		{
			RegionA: RegionUSEast,
			RegionB: RegionEuropeWest,
			Latency: 43 * time.Millisecond,
		},
		{
			RegionA: RegionUSWest,
			RegionB: RegionEuropeWest,
			Latency: 66 * time.Millisecond,
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
