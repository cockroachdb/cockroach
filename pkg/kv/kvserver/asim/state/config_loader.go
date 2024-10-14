// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"google.golang.org/protobuf/proto"
)

var SingleRegionClusterOptions = [...]string{"single_region", "single_region_multi_store"}
var MultiRegionClusterOptions = [...]string{"multi_region", "complex"}
var AllClusterOptions = [...]string{"single_region", "single_region_multi_store", "multi_region", "complex"}

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
				NewZoneWithSingleStore("US_1", 5),
				NewZoneWithSingleStore("US_2", 5),
				NewZoneWithSingleStore("US_3", 5),
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
				NewZone("US_1", 1, 5),
				NewZone("US_2", 1, 5),
				NewZone("US_3", 1, 5),
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
				NewZoneWithSingleStore("US_East_1", 4),
				NewZoneWithSingleStore("US_East_2", 4),
				NewZoneWithSingleStore("US_East_3", 4),
			},
		},
		{
			Name: "US_West",
			Zones: []Zone{
				NewZoneWithSingleStore("US_West_1", 4),
				NewZoneWithSingleStore("US_West_2", 4),
				NewZoneWithSingleStore("US_West_3", 4),
			},
		},
		{
			Name: "EU",
			Zones: []Zone{
				NewZoneWithSingleStore("EU_1", 4),
				NewZoneWithSingleStore("EU_2", 4),
				NewZoneWithSingleStore("EU_3", 4),
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
				NewZoneWithSingleStore("US_East_1", 1),
				NewZoneWithSingleStore("US_East_2", 2),
				NewZoneWithSingleStore("US_East_3", 3),
				NewZoneWithSingleStore("US_East_3", 10),
			},
		},
		{
			Name: "US_West",
			Zones: []Zone{
				NewZoneWithSingleStore("US_West_1", 2),
			},
		},
		{
			Name: "EU",
			Zones: []Zone{
				NewZoneWithSingleStore("EU_1", 3),
				NewZoneWithSingleStore("EU_2", 3),
				NewZoneWithSingleStore("EU_3", 4),
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

// GetClusterInfo returns ClusterInfo for a given configName and panics if no
// match is found in existing configurations.
func GetClusterInfo(configName string) ClusterInfo {
	switch configName {
	case "single_region":
		return SingleRegionConfig
	case "single_region_multi_store":
		return SingleRegionMultiStoreConfig
	case "multi_region":
		return MultiRegionConfig
	case "complex":
		return ComplexConfig
	default:
		panic(fmt.Sprintf("no matching cluster info found for %s", configName))
	}
}

// RangeInfoWithReplicas returns a new RangeInfo using the supplied arguments.
func RangeInfoWithReplicas(
	startKey Key, voters, nonVoters []StoreID, leaseholder StoreID, config *roachpb.SpanConfig,
) RangeInfo {
	desc := roachpb.RangeDescriptor{
		StartKey:         startKey.ToRKey(),
		InternalReplicas: make([]roachpb.ReplicaDescriptor, len(voters)+len(nonVoters)),
	}
	for i, storeID := range voters {
		desc.InternalReplicas[i] = roachpb.ReplicaDescriptor{
			StoreID: roachpb.StoreID(storeID),
			Type:    roachpb.VOTER_FULL,
		}
	}
	for i, storeID := range nonVoters {
		desc.InternalReplicas[i+len(voters)] = roachpb.ReplicaDescriptor{
			StoreID: roachpb.StoreID(storeID),
			Type:    roachpb.NON_VOTER,
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

// NewZoneWithSingleStore is a constructor for a simulated availability zone,
// taking zone name, node count, and a default of one store per node.
func NewZoneWithSingleStore(name string, nodeCount int) Zone {
	return NewZone(name, nodeCount, 1)
}

// NewZone is a constructor for a simulated availability zone, taking zone name,
// node count, and custom stores per node.
func NewZone(name string, nodeCount int, storesPerNode int) Zone {
	if storesPerNode < 1 {
		panic(fmt.Sprintf("storesPerNode cannot be less than one but found %v", storesPerNode))
	}
	return Zone{
		Name:          name,
		NodeCount:     nodeCount,
		StoresPerNode: storesPerNode,
	}
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

func (c ClusterInfo) String() (s string) {
	buf := &strings.Builder{}
	for i, r := range c.Regions {
		buf.WriteString(fmt.Sprintf("\t\tregion:%s [", r.Name))
		if len(r.Zones) == 0 {
			panic(fmt.Sprintf("number of zones within region %s is zero", r.Name))
		}
		for j, z := range r.Zones {
			buf.WriteString(fmt.Sprintf("zone=%s(nodes=%d,stores=%d)", z.Name, z.NodeCount, z.StoresPerNode))
			if j != len(r.Zones)-1 {
				buf.WriteString(", ")
			}
		}
		buf.WriteString("]")
		if i != len(c.Regions)-1 {
			buf.WriteString("\n")
		}
	}
	return buf.String()
}

type RangeInfo struct {
	Descriptor  roachpb.RangeDescriptor
	Config      *roachpb.SpanConfig
	Size        int64
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
	for _, r := range c.Regions {
		regionTier := roachpb.Tier{
			Key:   "region",
			Value: r.Name,
		}
		for _, z := range r.Zones {
			zoneTier := roachpb.Tier{
				Key:   "zone",
				Value: z.Name,
			}
			locality := roachpb.Locality{
				Tiers: []roachpb.Tier{regionTier, zoneTier},
			}
			for i := 0; i < z.NodeCount; i++ {
				node := s.AddNode()
				s.SetNodeLocality(node.NodeID(), locality)
				storesRequired := z.StoresPerNode
				if storesRequired < 1 {
					panic(fmt.Sprintf("storesPerNode cannot be less than one but found %v", storesRequired))
				}
				for store := 0; store < storesRequired; store++ {
					if newStore, ok := s.AddStore(node.NodeID()); !ok {
						panic(fmt.Sprintf(
							"Unable to load config: cannot add store %d",
							node.NodeID(),
						))
					} else {
						s.SetStoreCapacity(newStore.StoreID(), int64(c.DiskCapacityGB)*1<<30)
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

		if !s.SetSpanConfigForRange(rng.RangeID(), r.Config) {
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
		s.SetRangeBytes(rng.RangeID(), r.Size)
		for _, desc := range r.Descriptor.InternalReplicas {
			if _, ok := s.AddReplica(rng.RangeID(), StoreID(desc.StoreID), desc.Type); !ok {
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

func GetRegionSurvivalConfig(
	regionOne string, regionTwo string, regionThree string,
) zonepb.ZoneConfig {
	zoneConfig := zonepb.DefaultZoneConfig()
	zoneConfig.NumReplicas = proto.Int32(5)
	zoneConfig.NumVoters = proto.Int32(5)
	zoneConfig.LeasePreferences = []zonepb.LeasePreference{
		{
			Constraints: []zonepb.Constraint{
				{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: regionOne}},
		},
	}
	zoneConfig.Constraints = []zonepb.ConstraintsConjunction{
		{
			NumReplicas: 1,
			Constraints: []zonepb.Constraint{
				{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: regionOne},
			},
		},
		{
			NumReplicas: 1,
			Constraints: []zonepb.Constraint{
				{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: regionTwo},
			},
		},
		{
			NumReplicas: 1,
			Constraints: []zonepb.Constraint{
				{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: regionThree},
			},
		},
	}
	zoneConfig.VoterConstraints = []zonepb.ConstraintsConjunction{
		{
			NumReplicas: 2,
			Constraints: []zonepb.Constraint{
				{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: regionOne},
			},
		},
	}
	return zoneConfig
}

func GetZoneSurvivalConfig(
	regionOne string, regionTwo string, regionThree string,
) zonepb.ZoneConfig {
	zoneConfig := zonepb.DefaultZoneConfig()
	zoneConfig.NumReplicas = proto.Int32(5)
	zoneConfig.NumVoters = proto.Int32(3)
	zoneConfig.LeasePreferences = []zonepb.LeasePreference{{
		Constraints: []zonepb.Constraint{
			{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: regionOne},
		},
	}}

	zoneConfig.Constraints = []zonepb.ConstraintsConjunction{
		{
			NumReplicas: 1,
			Constraints: []zonepb.Constraint{
				{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: regionOne},
			},
		},
		{
			NumReplicas: 1,
			Constraints: []zonepb.Constraint{
				{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: regionTwo},
			},
		},
		{
			NumReplicas: 1,
			Constraints: []zonepb.Constraint{
				{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: regionThree},
			},
		},
	}

	zoneConfig.VoterConstraints = []zonepb.ConstraintsConjunction{{
		Constraints: []zonepb.Constraint{
			{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: regionOne},
		},
	},
	}
	return zoneConfig
}
