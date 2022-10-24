// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package state

import "github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"

// SingleRegionConfig is a simple cluster config with a single region and 3
// zones, all have the same number of nodes.
var SingleRegionConfig = ClusterInfo{
	DiskCapacityGB: 1024,
	Regions: []Region{
		{
			Name: "US",
			Zones: []Zone{
				{Name: "US_1", NodeCount: 5},
				{Name: "US_2", NodeCount: 5},
				{Name: "US_3", NodeCount: 5},
			},
		},
	},
}

// MultiRegionConfig is a perfectly balanced cluster config with 3 regions.
var MultiRegionConfig = ClusterInfo{
	DiskCapacityGB: 2048,
	Regions: []Region{
		{
			Name: "US_East",
			Zones: []Zone{
				{Name: "US_East_1", NodeCount: 4},
				{Name: "US_East_2", NodeCount: 4},
				{Name: "US_East_3", NodeCount: 4},
			},
		},
		{
			Name: "US_West",
			Zones: []Zone{
				{Name: "US_West_1", NodeCount: 4},
				{Name: "US_West_2", NodeCount: 4},
				{Name: "US_West_3", NodeCount: 4},
			},
		},
		{
			Name: "EU",
			Zones: []Zone{
				{Name: "EU_1", NodeCount: 4},
				{Name: "EU_2", NodeCount: 4},
				{Name: "EU_3", NodeCount: 4},
			},
		},
	},
}

// ComplexConfig is an imbalanced multi-region cluster config.
var ComplexConfig = ClusterInfo{
	DiskCapacityGB: 2048,
	Regions: []Region{
		{
			Name: "US_East",
			Zones: []Zone{
				{Name: "US_East_1", NodeCount: 1},
				{Name: "US_East_2", NodeCount: 2},
				{Name: "US_East_3", NodeCount: 3},
				{Name: "US_East_3", NodeCount: 10},
			},
		},
		{
			Name: "US_West",
			Zones: []Zone{
				{Name: "US_West_1", NodeCount: 2},
			},
		},
		{
			Name: "EU",
			Zones: []Zone{
				{Name: "EU_1", NodeCount: 3},
				{Name: "EU_2", NodeCount: 3},
				{Name: "EU_3", NodeCount: 4},
			},
		},
	},
}

// Zone is a simulated availability zone.
type Zone struct {
	Name      string
	NodeCount int
}

// Region is a simulated region which contains one or more zones.
type Region struct {
	Name  string
	Zones []Zone
}

// ClusterInfo contains cluster information needed for allocation decisions.
// TODO(lidor): add cross region network latencies.
type ClusterInfo struct {
	DiskCapacityGB int
	Regions        []Region
}

// LoadConfig loads a predefined configuration which contains cluster
// information such as regions, zones, etc.
func LoadConfig(c ClusterInfo) State {
	s := newState(config.DefaultSimulationSettings())
	// A new state has a single range - add the replica load for that range.
	s.clusterinfo = c
	// TODO(lidor): load locality info to be used by the allocator. Do we need a
	// NodeDescriptor and higher level localities? or can we simulate those?
	for _, r := range c.Regions {
		for _, z := range r.Zones {
			for i := 0; i < z.NodeCount; i++ {
				node := s.AddNode()
				store, _ := s.AddStore(node.NodeID())

				// A new state has a range before stores were added, if this new store
				// is one of the first to be added, make sure we create replicas for
				// that first range on the new store.
				// TODO(lidor): clean this up, and potentially merge LoadConfig() with
				// NewTestState().
				r, ok := s.Range(1)
				if ok && len(r.Replicas()) < 3 {
					s.AddReplica(1, store.StoreID())
				}
			}
		}
	}
	return s
}
