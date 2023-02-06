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

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// TODO(kvoli): Add a loader/translator for the existing
// []*roachpb.StoreDescriptor configurations in kvserver/*_test.go and
// allocatorimpl/*_test.go.

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

// SingleRegionMultiStoreConfig is a simple cluster config with a single region
// and 3 zones, all zones have 1 node and 6 stores per node.
var SingleRegionMultiStoreConfig = ClusterInfo{
	DiskCapacityGB: 1024,
	Regions: []Region{
		{
			Name: "US",
			Zones: []Zone{
				{Name: "US_1", NodeCount: 1, StoresPerNode: 5},
				{Name: "US_2", NodeCount: 1, StoresPerNode: 5},
				{Name: "US_3", NodeCount: 1, StoresPerNode: 5},
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

// SingleRangeConfig is a single range config where there are 3 replicas on
// stores 1, 2 and 3. Store 1 is the leaseholder.
var SingleRangeConfig = []RangeInfo{
	{
		Descriptor: roachpb.RangeDescriptor{
			StartKey: MinKey.ToRKey(),
			EndKey:   (MinKey + 1000).ToRKey(),
			InternalReplicas: []roachpb.ReplicaDescriptor{
				{
					StoreID: 1,
				},
				{
					StoreID: 2,
				},
				{
					StoreID: 3,
				},
			},
		},
		Config:      &defaultSpanConfig,
		Leaseholder: 1,
	},
}

// MultiRangeConfig is a ranges config where there are three ranges and stores
// 1,2,3 have replicas for each range. There is 1 leaseholder on each of store
// 1,2,3.
var MultiRangeConfig = []RangeInfo{
	{
		Descriptor: roachpb.RangeDescriptor{
			StartKey: MinKey.ToRKey(),
			InternalReplicas: []roachpb.ReplicaDescriptor{
				{
					StoreID: 1,
				},
				{
					StoreID: 2,
				},
				{
					StoreID: 3,
				},
			},
		},
		Config:      &defaultSpanConfig,
		Leaseholder: 1,
	},
	{
		Descriptor: roachpb.RangeDescriptor{
			StartKey: (MinKey + 1000).ToRKey(),
			InternalReplicas: []roachpb.ReplicaDescriptor{
				{
					StoreID: 1,
				},
				{
					StoreID: 2,
				},
				{
					StoreID: 3,
				},
			},
		},
		Config:      &defaultSpanConfig,
		Leaseholder: 2,
	},
	{
		Descriptor: roachpb.RangeDescriptor{
			StartKey: (MinKey + 2000).ToRKey(),
			InternalReplicas: []roachpb.ReplicaDescriptor{
				{
					StoreID: 1,
				},
				{
					StoreID: 2,
				},
				{
					StoreID: 3,
				},
			},
		},
		Config:      &defaultSpanConfig,
		Leaseholder: 3,
	},
}

// RangeInfoWithReplicas returns a new RangeInfo using the supplied arguments.
func RangeInfoWithReplicas(
	startKey Key, replicas []StoreID, leaseholder StoreID, config *roachpb.SpanConfig,
) RangeInfo {
	desc := roachpb.RangeDescriptor{
		StartKey:         startKey.ToRKey(),
		InternalReplicas: make([]roachpb.ReplicaDescriptor, len(replicas)),
	}
	for i, storeID := range replicas {
		desc.InternalReplicas[i] = roachpb.ReplicaDescriptor{
			StoreID: roachpb.StoreID(storeID),
		}
	}
	return RangeInfo{Descriptor: desc, Leaseholder: leaseholder, Config: config}
}

// Zone is a simulated availability zone. When StoresPerNode is 0, a default
// value of 1 store per node is used instead.
type Zone struct {
	Name          string
	NodeCount     int
	StoresPerNode int
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

type RangeInfo struct {
	Descriptor  roachpb.RangeDescriptor
	Config      *roachpb.SpanConfig
	Leaseholder StoreID
}

type RangesInfo []RangeInfo

// LoadConfig loads a predefined configuration which contains cluster
// information, range info and initial replica/lease placement.
func LoadConfig(c ClusterInfo, r RangesInfo, settings *config.SimulationSettings) State {
	s := LoadClusterInfo(c, settings)
	LoadRangeInfo(s, r...)
	return s
}

// LoadClusterInfo loads a predefined configuration which contains cluster
// information such as regions, zones, etc.
func LoadClusterInfo(c ClusterInfo, settings *config.SimulationSettings) State {
	s := newState(settings)
	// A new state has a single range - add the replica load for that range.
	s.clusterinfo = c
	// TODO(lidor): load locality info to be used by the allocator. Do we need a
	// NodeDescriptor and higher level localities? or can we simulate those?
	for _, r := range c.Regions {
		for _, z := range r.Zones {
			for i := 0; i < z.NodeCount; i++ {
				node := s.AddNode()
				storesRequired := z.StoresPerNode
				if storesRequired < 1 {
					storesRequired = 1
				}
				for store := 0; store < storesRequired; store++ {
					if _, ok := s.AddStore(node.NodeID()); !ok {
						panic(fmt.Sprintf(
							"Unable to load config: cannot add store %d",
							node.NodeID(),
						))
					}
				}
			}
		}
	}
	return s
}

// LoadRangeInfo loads the ranges specified in RangesInfo into state. If any
// operation fails this function panics.
func LoadRangeInfo(s State, rangeInfos ...RangeInfo) {
	for _, r := range rangeInfos {
		var rng Range
		var ok bool

		// Use the default span config if not set in the configuration.
		if r.Config == nil {
			copiedDefaultConfig := defaultSpanConfig
			r.Config = &copiedDefaultConfig
		}

		// When the state is initialized there will always be at least one
		// range that spans the entire keyspace. All other ranges are split off
		// of this one. If the range info has a start key that is equal to
		// MinKey, then we assume that this refers to the start range - use
		// that existing range rather than splitting.
		startKey := ToKey(r.Descriptor.StartKey.AsRawKey())
		if startKey == MinKey {
			rng, ok = s.RangeFor(startKey), true
		} else {
			_, rng, ok = s.SplitRange(startKey)
		}
		if !ok {
			panic(fmt.Sprintf(
				"Unable to load config: failed create range %d",
				startKey,
			))
		}

		if !s.SetSpanConfig(rng.RangeID(), *r.Config) {
			panic(fmt.Sprintf(
				"Unable to load config: cannot set span config for range %s",
				rng,
			))
		}
	}

	// Create the replicas for each range and transfer the range lease to the
	// specified leaseholder. If this were done in the above loop, it would be
	// necessary to delete all existing replicas that were carried over from
	// the lhs split, before adding the new replicas to the rhs.
	for _, r := range rangeInfos {
		startKey := ToKey(r.Descriptor.StartKey.AsRawKey())
		rng := s.RangeFor(startKey)
		for _, desc := range r.Descriptor.InternalReplicas {
			if _, ok := s.AddReplica(rng.RangeID(), StoreID(desc.StoreID)); !ok {
				panic(fmt.Sprintf(
					"Unable to load config: add replica to store %d failed at "+
						"for range %s replicas %s",
					desc.StoreID, rng, rng.Replicas()))
			}
		}
		if store, _ := s.LeaseholderStore(rng.RangeID()); store.StoreID() != r.Leaseholder {
			if !s.TransferLease(rng.RangeID(), r.Leaseholder) {
				panic(fmt.Sprintf(
					"Unable to load config: transfer lease to %d failed at for range %s",
					r.Leaseholder, rng))
			}
		}
	}
}
