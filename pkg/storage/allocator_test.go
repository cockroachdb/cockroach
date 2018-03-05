// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"context"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const firstRange = roachpb.RangeID(1)

var firstRangeInfo = testRangeInfo([]roachpb.ReplicaDescriptor{}, firstRange)

var simpleZoneConfig = config.ZoneConfig{
	NumReplicas: 1,
	Constraints: []config.Constraints{
		{
			Constraints: []config.Constraint{
				{Value: "a", Type: config.Constraint_REQUIRED},
				{Value: "ssd", Type: config.Constraint_REQUIRED},
			},
		},
	},
}

var multiDCConfig = config.ZoneConfig{
	NumReplicas: 2,
	Constraints: []config.Constraints{
		{Constraints: []config.Constraint{{Value: "ssd", Type: config.Constraint_REQUIRED}}},
	},
}

var singleStore = []*roachpb.StoreDescriptor{
	{
		StoreID: 1,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 1,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
}

var sameDCStores = []*roachpb.StoreDescriptor{
	{
		StoreID: 1,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 1,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
	{
		StoreID: 2,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 2,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
	{
		StoreID: 3,
		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 3,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
	{
		StoreID: 4,
		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 4,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
	{
		StoreID: 5,
		Attrs:   roachpb.Attributes{Attrs: []string{"mem"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 5,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
}

var multiDCStores = []*roachpb.StoreDescriptor{
	{
		StoreID: 1,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 1,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
	{
		StoreID: 2,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 2,
			Attrs:  roachpb.Attributes{Attrs: []string{"b"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:     200,
			Available:    100,
			LogicalBytes: 100,
		},
	},
}

var multiDiversityDCStores = []*roachpb.StoreDescriptor{
	{
		StoreID: 1,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 1,
			Attrs:  roachpb.Attributes{Attrs: []string{"odd"}},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "datacenter", Value: "a"},
				},
			},
		},
	},
	{
		StoreID: 2,
		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 2,
			Attrs:  roachpb.Attributes{Attrs: []string{"even"}},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "datacenter", Value: "a"},
				},
			},
		},
	},
	{
		StoreID: 3,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 3,
			Attrs:  roachpb.Attributes{Attrs: []string{"odd"}},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "datacenter", Value: "b"},
				},
			},
		},
	},
	{
		StoreID: 4,
		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 4,
			Attrs:  roachpb.Attributes{Attrs: []string{"even"}},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "datacenter", Value: "b"},
				},
			},
		},
	},
	{
		StoreID: 5,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 5,
			Attrs:  roachpb.Attributes{Attrs: []string{"odd"}},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "datacenter", Value: "c"},
				},
			},
		},
	},
	{
		StoreID: 6,
		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 6,
			Attrs:  roachpb.Attributes{Attrs: []string{"even"}},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "datacenter", Value: "c"},
				},
			},
		},
	},
	{
		StoreID: 7,
		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 7,
			Attrs:  roachpb.Attributes{Attrs: []string{"odd"}},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "datacenter", Value: "d"},
				},
			},
		},
	},
	{
		StoreID: 8,
		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 8,
			Attrs:  roachpb.Attributes{Attrs: []string{"even"}},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "datacenter", Value: "d"},
				},
			},
		},
	},
}

func testRangeInfo(replicas []roachpb.ReplicaDescriptor, rangeID roachpb.RangeID) RangeInfo {
	return RangeInfo{
		Desc: &roachpb.RangeDescriptor{
			Replicas: replicas,
			RangeID:  rangeID,
		},
	}
}

func replicas(storeIDs ...roachpb.StoreID) []roachpb.ReplicaDescriptor {
	res := make([]roachpb.ReplicaDescriptor, len(storeIDs))
	for i, storeID := range storeIDs {
		res[i].NodeID = roachpb.NodeID(storeID)
		res[i].StoreID = storeID
	}
	return res
}

// createTestAllocator creates a stopper, gossip, store pool and allocator for
// use in tests. Stopper must be stopped by the caller.
func createTestAllocator(
	deterministic bool,
) (*stop.Stopper, *gossip.Gossip, *StorePool, Allocator, *hlc.ManualClock) {
	stopper, g, manual, storePool, _ := createTestStorePool(
		TestTimeUntilStoreDeadOff, deterministic, NodeLivenessStatus_LIVE)
	a := MakeAllocator(storePool, func(string) (time.Duration, bool) {
		return 0, true
	})
	return stopper, g, storePool, a, manual
}

// mockStorePool sets up a collection of a alive and dead stores in the store
// pool for testing purposes. It also adds dead replicas to the stores and
// ranges in deadReplicas.
func mockStorePool(
	storePool *StorePool,
	aliveStoreIDs, deadStoreIDs, decommissioningStoreIDs, decommissionedStoreIDs []roachpb.StoreID,
	deadReplicas []roachpb.ReplicaIdent,
) {
	storePool.detailsMu.Lock()
	defer storePool.detailsMu.Unlock()

	liveNodeSet := map[roachpb.NodeID]NodeLivenessStatus{}
	storePool.detailsMu.storeDetails = map[roachpb.StoreID]*storeDetail{}
	for _, storeID := range aliveStoreIDs {
		liveNodeSet[roachpb.NodeID(storeID)] = NodeLivenessStatus_LIVE
		detail := storePool.getStoreDetailLocked(storeID)
		detail.desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}
	for _, storeID := range deadStoreIDs {
		liveNodeSet[roachpb.NodeID(storeID)] = NodeLivenessStatus_DEAD
		detail := storePool.getStoreDetailLocked(storeID)
		detail.desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}
	for _, storeID := range decommissioningStoreIDs {
		liveNodeSet[roachpb.NodeID(storeID)] = NodeLivenessStatus_DECOMMISSIONING
		detail := storePool.getStoreDetailLocked(storeID)
		detail.desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}
	for _, storeID := range decommissionedStoreIDs {
		liveNodeSet[roachpb.NodeID(storeID)] = NodeLivenessStatus_DECOMMISSIONED
		detail := storePool.getStoreDetailLocked(storeID)
		detail.desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}
	for storeID, detail := range storePool.detailsMu.storeDetails {
		for _, replica := range deadReplicas {
			if storeID != replica.Replica.StoreID {
				continue
			}
			detail.deadReplicas[replica.RangeID] = append(detail.deadReplicas[replica.RangeID], replica.Replica)
		}
	}

	// Set the node liveness function using the set we constructed.
	storePool.nodeLivenessFn =
		func(nodeID roachpb.NodeID, now time.Time, threshold time.Duration) NodeLivenessStatus {
			if status, ok := liveNodeSet[nodeID]; ok {
				return status
			}
			return NodeLivenessStatus_UNAVAILABLE
		}
}

func TestAllocatorSimpleRetrieval(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.Background())
	gossiputil.NewStoreGossiper(g).GossipStores(singleStore, t)
	result, _, err := a.AllocateTarget(
		context.Background(),
		simpleZoneConfig,
		[]roachpb.ReplicaDescriptor{},
		firstRangeInfo,
		false,
	)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	if result.Node.NodeID != 1 || result.StoreID != 1 {
		t.Errorf("expected NodeID 1 and StoreID 1: %+v", result)
	}
}

// TestAllocatorCorruptReplica ensures that the allocator never attempts to
// allocate a new replica on top of a dead (corrupt) one.
func TestAllocatorCorruptReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, g, sp, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.Background())
	gossiputil.NewStoreGossiper(g).GossipStores(sameDCStores, t)
	const store1ID = roachpb.StoreID(1)

	// Set store 1 to have a dead replica in the store pool.
	sp.detailsMu.Lock()
	sp.detailsMu.storeDetails[store1ID].deadReplicas[firstRange] =
		[]roachpb.ReplicaDescriptor{{
			NodeID:  roachpb.NodeID(1),
			StoreID: store1ID,
		}}
	sp.detailsMu.Unlock()

	result, _, err := a.AllocateTarget(
		context.Background(),
		simpleZoneConfig,
		[]roachpb.ReplicaDescriptor{},
		firstRangeInfo,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Node.NodeID != 2 || result.StoreID != 2 {
		t.Errorf("expected NodeID 2 and StoreID 2; got %+v", result)
	}
}

func TestAllocatorNoAvailableDisks(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, _, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.Background())
	result, _, err := a.AllocateTarget(
		context.Background(),
		simpleZoneConfig,
		[]roachpb.ReplicaDescriptor{},
		firstRangeInfo,
		false,
	)
	if result != nil {
		t.Errorf("expected nil result: %+v", result)
	}
	if err == nil {
		t.Errorf("allocation succeeded despite there being no available disks: %v", result)
	}
}

func TestAllocatorTwoDatacenters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.Background())
	gossiputil.NewStoreGossiper(g).GossipStores(multiDCStores, t)
	ctx := context.Background()
	result1, _, err := a.AllocateTarget(
		ctx,
		multiDCConfig,
		[]roachpb.ReplicaDescriptor{},
		firstRangeInfo,
		false,
	)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	result2, _, err := a.AllocateTarget(
		ctx,
		multiDCConfig,
		[]roachpb.ReplicaDescriptor{{
			NodeID:  result1.Node.NodeID,
			StoreID: result1.StoreID,
		}},
		firstRangeInfo,
		false,
	)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	ids := []int{int(result1.Node.NodeID), int(result2.Node.NodeID)}
	sort.Ints(ids)
	if expected := []int{1, 2}; !reflect.DeepEqual(ids, expected) {
		t.Errorf("Expected nodes %+v: %+v vs %+v", expected, result1.Node, result2.Node)
	}
	// Verify that no result is forthcoming if we already have a replica.
	result3, _, err := a.AllocateTarget(
		ctx,
		multiDCConfig,
		[]roachpb.ReplicaDescriptor{
			{
				NodeID:  result1.Node.NodeID,
				StoreID: result1.StoreID,
			},
			{
				NodeID:  result2.Node.NodeID,
				StoreID: result2.StoreID,
			},
		},
		firstRangeInfo,
		false,
	)
	if err == nil {
		t.Errorf("expected error on allocation without available stores: %+v", result3)
	}
}

func TestAllocatorExistingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.Background())
	gossiputil.NewStoreGossiper(g).GossipStores(sameDCStores, t)
	result, _, err := a.AllocateTarget(
		context.Background(),
		config.ZoneConfig{
			Constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Value: "a", Type: config.Constraint_REQUIRED},
						{Value: "hdd", Type: config.Constraint_REQUIRED},
					},
				},
			},
		},
		[]roachpb.ReplicaDescriptor{
			{
				NodeID:  2,
				StoreID: 2,
			},
		},
		firstRangeInfo,
		false,
	)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	if !(result.StoreID == 3 || result.StoreID == 4) {
		t.Errorf("expected result to have store ID 3 or 4: %+v", result)
	}
}

// TestAllocatorRebalance verifies that rebalance targets are chosen
// randomly from amongst stores under the maxFractionUsedThreshold.
func TestAllocatorRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stores := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 100, RangeCount: 1},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 50, RangeCount: 1},
		},
		{
			StoreID: 3,
			Node:    roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{
				Capacity:   100,
				Available:  100 - int64(100*maxFractionUsedThreshold),
				RangeCount: 5,
			},
		},
		{
			// This store must not be rebalanced to, because it's too full.
			StoreID: 4,
			Node:    roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{
				Capacity:   100,
				Available:  (100 - int64(100*maxFractionUsedThreshold)) / 2,
				RangeCount: 10,
			},
		},
		{
			// This store will not be rebalanced to, because it already has more
			// replicas than the mean range count.
			StoreID: 5,
			Node:    roachpb.NodeDescriptor{NodeID: 5},
			Capacity: roachpb.StoreCapacity{
				Capacity:   100,
				Available:  100,
				RangeCount: 10,
			},
		},
	}

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.Background())

	st := a.storePool.st
	EnableStatsBasedRebalancing.Override(&st.SV, false)

	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)
	ctx := context.Background()

	// Every rebalance target must be either store 1 or 2.
	for i := 0; i < 10; i++ {
		result, _ := a.RebalanceTarget(
			ctx,
			config.ZoneConfig{},
			nil,
			testRangeInfo([]roachpb.ReplicaDescriptor{{NodeID: 3, StoreID: 3}}, firstRange),
			storeFilterThrottled,
			false,
		)
		if result == nil {
			i-- // loop until we find 10 candidates
			continue
		}
		// We might not get a rebalance target if the random nodes selected as
		// candidates are not suitable targets.
		if result.StoreID != 1 && result.StoreID != 2 {
			t.Errorf("%d: expected store 1 or 2; got %d", i, result.StoreID)
		}
	}

	// Verify shouldRebalance results.
	for i, store := range stores {
		desc, ok := a.storePool.getStoreDescriptor(store.StoreID)
		if !ok {
			t.Fatalf("%d: unable to get store %d descriptor", i, store.StoreID)
		}
		sl, _, _ := a.storePool.getStoreList(firstRange, storeFilterThrottled)
		result := shouldRebalance(ctx, desc, sl, firstRangeInfo, a.scorerOptions(false))
		if expResult := (i >= 2); expResult != result {
			t.Errorf("%d: expected rebalance %t; got %t; desc %+v; sl: %+v", i, expResult, result, desc, sl)
		}
	}
}

// TestAllocatorRebalanceTarget could help us to verify whether we'll rebalance to a target that
// we'll immediately remove.
func TestAllocatorRebalanceTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.Background())
	// We make 5 stores in this test -- 3 in the same datacenter, and 1 each in
	// 2 other datacenters. All of our replicas are distributed within these 3
	// datacenters. Originally, the stores that are all alone in their datacenter
	// are fuller than the other stores. If we didn't simulate RemoveTarget in
	// RebalanceTarget, we would try to choose store 2 or 3 as the target store
	// to make a rebalance. However, we would immediately remove the replica on
	// store 1 or 2 to retain the locality diversity.
	stores := []*roachpb.StoreDescriptor{
		{
			StoreID: 1,
			Node: roachpb.NodeDescriptor{
				NodeID: 1,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "datacenter", Value: "a"},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				RangeCount: 50,
			},
		},
		{
			StoreID: 2,
			Node: roachpb.NodeDescriptor{
				NodeID: 2,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "datacenter", Value: "a"},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				RangeCount: 55,
			},
		},
		{
			StoreID: 3,
			Node: roachpb.NodeDescriptor{
				NodeID: 3,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "datacenter", Value: "a"},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				RangeCount: 55,
			},
		},
		{
			StoreID: 4,
			Node: roachpb.NodeDescriptor{
				NodeID: 4,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "datacenter", Value: "b"},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				RangeCount: 100,
			},
		},
		{
			StoreID: 5,
			Node: roachpb.NodeDescriptor{
				NodeID: 5,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "datacenter", Value: "c"},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{
				RangeCount: 100,
			},
		},
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	st := a.storePool.st
	EnableStatsBasedRebalancing.Override(&st.SV, false)
	replicas := []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: 1},
		{NodeID: 4, StoreID: 4},
		{NodeID: 5, StoreID: 5},
	}
	repl := &Replica{RangeID: firstRange}

	repl.mu.Lock()
	repl.mu.state.Stats = &enginepb.MVCCStats{}
	repl.mu.Unlock()

	rs := newReplicaStats(clock, nil)
	repl.writeStats = rs

	desc := &roachpb.RangeDescriptor{
		Replicas: replicas,
		RangeID:  firstRange,
	}

	rangeInfo := rangeInfoForRepl(repl, desc)

	status := &raft.Status{
		Progress: make(map[uint64]raft.Progress),
	}
	for _, replica := range replicas {
		status.Progress[uint64(replica.NodeID)] = raft.Progress{
			Match: 10,
		}
	}
	for i := 0; i < 10; i++ {
		result, details := a.RebalanceTarget(
			context.Background(),
			config.ZoneConfig{},
			status,
			rangeInfo,
			storeFilterThrottled,
			false,
		)
		if result != nil {
			t.Fatalf("expected no rebalance, but got target s%d; details: %s", result.StoreID, details)
		}
	}

	// Set up a second round of testing where the other two stores in the big
	// locality actually have fewer replicas, but enough that it still isn't
	// worth rebalancing to them.
	stores[1].Capacity.RangeCount = 46
	stores[2].Capacity.RangeCount = 46
	sg.GossipStores(stores, t)
	for i := 0; i < 10; i++ {
		result, details := a.RebalanceTarget(
			context.Background(),
			config.ZoneConfig{},
			status,
			rangeInfo,
			storeFilterThrottled,
			false,
		)
		if result != nil {
			t.Fatalf("expected no rebalance, but got target s%d; details: %s", result.StoreID, details)
		}
	}

	// Make sure rebalancing does happen if we drop just a little further down.
	stores[1].Capacity.RangeCount = 44
	sg.GossipStores(stores, t)
	for i := 0; i < 10; i++ {
		result, details := a.RebalanceTarget(
			context.Background(),
			config.ZoneConfig{},
			status,
			rangeInfo,
			storeFilterThrottled,
			false,
		)
		if result == nil || result.StoreID != stores[1].StoreID {
			t.Fatalf("%d: expected rebalance to s%d, but got %v; details: %s",
				i, stores[1].StoreID, result, details)
		}
	}
}

func TestAllocatorRebalanceDeadNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, _, sp, a, _ := createTestAllocator( /* deterministic */ false)
	ctx := context.Background()
	defer stopper.Stop(ctx)

	EnableStatsBasedRebalancing.Override(&sp.st.SV, false)

	mockStorePool(
		sp,
		[]roachpb.StoreID{1, 2, 3, 4, 5, 6},
		[]roachpb.StoreID{7, 8},
		nil,
		nil,
		nil,
	)

	ranges := func(rangeCount int32) roachpb.StoreCapacity {
		return roachpb.StoreCapacity{
			Capacity:     1000,
			Available:    1000,
			LogicalBytes: 0,
			RangeCount:   rangeCount,
		}
	}

	// Initialize 8 stores: where store 6 is the target for rebalancing.
	sp.detailsMu.Lock()
	sp.getStoreDetailLocked(1).desc.Capacity = ranges(100)
	sp.getStoreDetailLocked(2).desc.Capacity = ranges(100)
	sp.getStoreDetailLocked(3).desc.Capacity = ranges(100)
	sp.getStoreDetailLocked(4).desc.Capacity = ranges(100)
	sp.getStoreDetailLocked(5).desc.Capacity = ranges(100)
	sp.getStoreDetailLocked(6).desc.Capacity = ranges(0)
	sp.getStoreDetailLocked(7).desc.Capacity = ranges(100)
	sp.getStoreDetailLocked(8).desc.Capacity = ranges(100)
	sp.detailsMu.Unlock()

	// Each test case should describe a repair situation which has a lower
	// priority than the previous test case.
	testCases := []struct {
		existing []roachpb.ReplicaDescriptor
		expected roachpb.StoreID
	}{
		// 3/3 live -> 3/4 live: ok
		{replicas(1, 2, 3), 6},
		// 2/3 live -> 2/4 live: nope
		{replicas(1, 2, 7), 0},
		// 4/4 live -> 4/5 live: ok
		{replicas(1, 2, 3, 4), 6},
		// 3/4 live -> 3/5 live: ok
		{replicas(1, 2, 3, 7), 6},
		// 5/5 live -> 5/6 live: ok
		{replicas(1, 2, 3, 4, 5), 6},
		// 4/5 live -> 4/6 live: ok
		{replicas(1, 2, 3, 4, 7), 6},
		// 3/5 live -> 3/6 live: nope
		{replicas(1, 2, 3, 7, 8), 0},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			result, _ := a.RebalanceTarget(
				ctx,
				config.ZoneConfig{},
				nil,
				testRangeInfo(c.existing, firstRange),
				storeFilterThrottled,
				false)
			if c.expected > 0 {
				if result == nil {
					t.Fatalf("expected %d, but found nil", c.expected)
				} else if c.expected != result.StoreID {
					t.Fatalf("expected %d, but found %d", c.expected, result.StoreID)
				}
			} else if result != nil {
				t.Fatalf("expected nil, but found %d", result.StoreID)
			}
		})
	}
}

// TestAllocatorRebalanceThrashing tests that the rebalancer does not thrash
// when replica counts are balanced, within the appropriate thresholds, across
// stores.
func TestAllocatorRebalanceThrashing(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testStore struct {
		rangeCount          int32
		shouldRebalanceFrom bool
	}

	// Returns a slice of stores with the specified mean. The first replica will
	// have a range count that's above the target range count for the rebalancer,
	// so it should be rebalanced from.
	oneStoreAboveRebalanceTarget := func(mean int32, numStores int) func(*cluster.Settings) []testStore {
		return func(st *cluster.Settings) []testStore {
			stores := make([]testStore, numStores)
			for i := range stores {
				stores[i].rangeCount = mean
			}
			surplus := int32(math.Ceil(float64(mean)*rangeRebalanceThreshold.Get(&st.SV) + 1))
			stores[0].rangeCount += surplus
			stores[0].shouldRebalanceFrom = true
			for i := 1; i < len(stores); i++ {
				stores[i].rangeCount -= int32(math.Ceil(float64(surplus) / float64(len(stores)-1)))
			}
			return stores
		}
	}

	// Returns a slice of stores with the specified mean such that the first store
	// has few enough replicas to make it a rebalance target.
	oneUnderusedStore := func(mean int32, numStores int) func(*cluster.Settings) []testStore {
		return func(st *cluster.Settings) []testStore {
			stores := make([]testStore, numStores)
			for i := range stores {
				stores[i].rangeCount = mean
			}
			// Subtract enough ranges from the first store to make it a suitable
			// rebalance target. To maintain the specified mean, we then add that delta
			// back to the rest of the replicas.
			deficit := int32(math.Ceil(float64(mean)*rangeRebalanceThreshold.Get(&st.SV) + 1))
			stores[0].rangeCount -= deficit
			for i := 1; i < len(stores); i++ {
				stores[i].rangeCount += int32(math.Ceil(float64(deficit) / float64(len(stores)-1)))
				stores[i].shouldRebalanceFrom = true
			}
			return stores
		}
	}

	// Each test case defines the range counts for the test stores and whether we
	// should rebalance from the store.
	testCases := []struct {
		name    string
		cluster func(*cluster.Settings) []testStore
	}{
		// An evenly balanced cluster should not rebalance.
		{"balanced", func(*cluster.Settings) []testStore {
			return []testStore{{5, false}, {5, false}, {5, false}, {5, false}}
		}},
		// Adding an empty node to a 3-node cluster triggers rebalancing from
		// existing nodes.
		{"empty-node", func(*cluster.Settings) []testStore {
			return []testStore{{100, true}, {100, true}, {100, true}, {0, false}}
		}},
		// A cluster where all range counts are within rangeRebalanceThreshold should
		// not rebalance. This assumes rangeRebalanceThreshold > 2%.
		{"within-threshold", func(*cluster.Settings) []testStore {
			return []testStore{{98, false}, {99, false}, {101, false}, {102, false}}
		}},
		{"5-stores-mean-100-one-above", oneStoreAboveRebalanceTarget(100, 5)},
		{"5-stores-mean-1000-one-above", oneStoreAboveRebalanceTarget(1000, 5)},
		{"5-stores-mean-10000-one-above", oneStoreAboveRebalanceTarget(10000, 5)},

		{"5-stores-mean-1000-one-underused", oneUnderusedStore(1000, 5)},
		{"10-stores-mean-1000-one-underused", oneUnderusedStore(1000, 10)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Deterministic is required when stressing as test case 8 may rebalance
			// to different configurations.
			stopper, g, _, a, _ := createTestAllocator( /* deterministic */ true)
			defer stopper.Stop(context.Background())

			st := a.storePool.st
			EnableStatsBasedRebalancing.Override(&st.SV, false)

			cluster := tc.cluster(st)

			// It doesn't make sense to test sets of stores containing fewer than 4
			// stores, because 4 stores is the minimum number of stores needed to
			// trigger rebalancing with the default replication factor of 3. Also, the
			// above local functions need a minimum number of stores to properly create
			// the desired distribution of range counts.
			const minStores = 4
			if numStores := len(cluster); numStores < minStores {
				t.Fatalf("numStores %d < min %d", numStores, minStores)
			}

			// Create stores with the range counts from the test case and gossip them.
			var stores []*roachpb.StoreDescriptor
			for j, store := range cluster {
				stores = append(stores, &roachpb.StoreDescriptor{
					StoreID:  roachpb.StoreID(j + 1),
					Node:     roachpb.NodeDescriptor{NodeID: roachpb.NodeID(j + 1)},
					Capacity: roachpb.StoreCapacity{Capacity: 1, Available: 1, RangeCount: store.rangeCount},
				})
			}
			gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

			// Ensure gossiped store descriptor changes have propagated.
			testutils.SucceedsSoon(t, func() error {
				sl, _, _ := a.storePool.getStoreList(firstRange, storeFilterThrottled)
				for j, s := range sl.stores {
					if a, e := s.Capacity.RangeCount, cluster[j].rangeCount; a != e {
						return errors.Errorf("range count for %d = %d != expected %d", j, a, e)
					}
				}
				return nil
			})
			sl, _, _ := a.storePool.getStoreList(firstRange, storeFilterThrottled)

			// Verify shouldRebalance returns the expected value.
			for j, store := range stores {
				desc, ok := a.storePool.getStoreDescriptor(store.StoreID)
				if !ok {
					t.Fatalf("[store %d]: unable to get store %d descriptor", j, store.StoreID)
				}
				if a, e := shouldRebalance(context.Background(), desc, sl, firstRangeInfo, a.scorerOptions(false)), cluster[j].shouldRebalanceFrom; a != e {
					t.Errorf("[store %d]: shouldRebalance %t != expected %t", store.StoreID, a, e)
				}
			}
		})
	}
}

// TestAllocatorRebalanceByCount verifies that rebalance targets are
// chosen by range counts in the event that available capacities
// exceed the maxAvailCapacityThreshold.
func TestAllocatorRebalanceByCount(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Setup the stores so that only one is below the standard deviation threshold.
	stores := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 100, RangeCount: 10},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 99, RangeCount: 10},
		},
		{
			StoreID:  3,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 98, RangeCount: 10},
		},
		{
			StoreID:  4,
			Node:     roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 98, RangeCount: 2},
		},
	}

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.Background())

	st := a.storePool.st
	EnableStatsBasedRebalancing.Override(&st.SV, false)

	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)
	ctx := context.Background()

	// Every rebalance target must be store 4 (or nil for case of missing the only option).
	for i := 0; i < 10; i++ {
		result, _ := a.RebalanceTarget(
			ctx,
			config.ZoneConfig{},
			nil,
			testRangeInfo([]roachpb.ReplicaDescriptor{{StoreID: stores[0].StoreID}}, firstRange),
			storeFilterThrottled,
			false,
		)
		if result != nil && result.StoreID != 4 {
			t.Errorf("expected store 4; got %d", result.StoreID)
		}
	}

	// Verify shouldRebalance results.
	for i, store := range stores {
		desc, ok := a.storePool.getStoreDescriptor(store.StoreID)
		if !ok {
			t.Fatalf("%d: unable to get store %d descriptor", i, store.StoreID)
		}
		sl, _, _ := a.storePool.getStoreList(firstRange, storeFilterThrottled)
		result := shouldRebalance(ctx, desc, sl, firstRangeInfo, a.scorerOptions(false))
		if expResult := (i < 3); expResult != result {
			t.Errorf("%d: expected rebalance %t; got %t", i, expResult, result)
		}
	}
}

func TestAllocatorTransferLeaseTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ true)
	defer stopper.Stop(context.Background())

	// 3 stores where the lease count for each store is equal to 10x the store
	// ID.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 3; i++ {
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID:  roachpb.StoreID(i),
			Node:     roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(10 * i)},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	existing := []roachpb.ReplicaDescriptor{
		{StoreID: 1},
		{StoreID: 2},
		{StoreID: 3},
	}

	// TODO(peter): Add test cases for non-empty constraints.
	testCases := []struct {
		existing    []roachpb.ReplicaDescriptor
		leaseholder roachpb.StoreID
		check       bool
		expected    roachpb.StoreID
	}{
		// No existing lease holder, nothing to do.
		{existing: existing, leaseholder: 0, check: true, expected: 0},
		// Store 1 is not a lease transfer source.
		{existing: existing, leaseholder: 1, check: true, expected: 0},
		{existing: existing, leaseholder: 1, check: false, expected: 2},
		// Store 2 is not a lease transfer source.
		{existing: existing, leaseholder: 2, check: true, expected: 0},
		{existing: existing, leaseholder: 2, check: false, expected: 1},
		// Store 3 is a lease transfer source.
		{existing: existing, leaseholder: 3, check: true, expected: 1},
		{existing: existing, leaseholder: 3, check: false, expected: 1},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			target := a.TransferLeaseTarget(
				context.Background(),
				config.ZoneConfig{},
				c.existing,
				c.leaseholder,
				0,
				nil, /* replicaStats */
				c.check,
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
			)
			if c.expected != target.StoreID {
				t.Fatalf("expected %d, but found %d", c.expected, target.StoreID)
			}
		})
	}
}

// TestAllocatorTransferLeaseTargetDraining verifies that the allocator will
// not choose to transfer leases to a store that is draining.
func TestAllocatorTransferLeaseTargetDraining(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, storePool, nl := createTestStorePool(
		TestTimeUntilStoreDeadOff, true /* deterministic */, NodeLivenessStatus_LIVE)
	a := MakeAllocator(storePool, func(string) (time.Duration, bool) {
		return 0, true
	})
	defer stopper.Stop(context.Background())

	// 3 stores where the lease count for each store is equal to 100x the store
	// ID. We'll be draining the store with the fewest leases on it.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 3; i++ {
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID:  roachpb.StoreID(i),
			Node:     roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(100 * i)},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	// UNAVAILABLE is the node liveness status used for a node that's draining.
	nl.setNodeStatus(1, NodeLivenessStatus_UNAVAILABLE)

	existing := []roachpb.ReplicaDescriptor{
		{StoreID: 1},
		{StoreID: 2},
		{StoreID: 3},
	}

	testCases := []struct {
		existing    []roachpb.ReplicaDescriptor
		leaseholder roachpb.StoreID
		check       bool
		expected    roachpb.StoreID
	}{
		// No existing lease holder, nothing to do.
		{existing: existing, leaseholder: 0, check: true, expected: 0},
		// Store 1 is draining, so it will try to transfer its lease if
		// checkTransferLeaseSource is false. This behavior isn't relied upon,
		// though; leases are manually transferred when draining.
		{existing: existing, leaseholder: 1, check: true, expected: 0},
		{existing: existing, leaseholder: 1, check: false, expected: 2},
		// Store 2 is not a lease transfer source.
		{existing: existing, leaseholder: 2, check: true, expected: 0},
		{existing: existing, leaseholder: 2, check: false, expected: 3},
		// Store 3 is a lease transfer source, but won't transfer to
		// node 1 because it's draining.
		{existing: existing, leaseholder: 3, check: true, expected: 2},
		{existing: existing, leaseholder: 3, check: false, expected: 2},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			target := a.TransferLeaseTarget(
				context.Background(),
				config.ZoneConfig{},
				c.existing,
				c.leaseholder,
				0,
				nil, /* replicaStats */
				c.check,
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
			)
			if c.expected != target.StoreID {
				t.Fatalf("expected %d, but found %d", c.expected, target.StoreID)
			}
		})
	}
}

func TestAllocatorRebalanceDifferentLocalitySizes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	ctx := context.Background()
	defer stopper.Stop(ctx)

	// Set up 8 stores -- 2 in each of the first 2 localities, and 4 in the third.
	// Because of the desire for diversity, the nodes in the small localities end
	// up being fuller than the nodes in the large locality. In the past this has
	// caused an over-eagerness to rebalance to nodes in the large locality, and
	// not enough willingness to rebalance within the small localities. This test
	// verifies that we compare fairly amongst stores that will givve the store
	// an optimal diversity score, not considering the fullness of those that
	// will make for worse diversity.
	stores := []*roachpb.StoreDescriptor{
		{
			StoreID: 1,
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(1),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "locale", Value: "1"}},
				},
			},
			Capacity: testStoreCapacitySetup(50, 50),
		},
		{
			StoreID: 2,
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(2),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "locale", Value: "1"}},
				},
			},
			Capacity: testStoreCapacitySetup(40, 60),
		},
		{
			StoreID: 3,
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(3),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "locale", Value: "2"}},
				},
			},
			Capacity: testStoreCapacitySetup(50, 50),
		},
		{
			StoreID: 4,
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(4),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "locale", Value: "2"}},
				},
			},
			Capacity: testStoreCapacitySetup(40, 60),
		},
		{
			StoreID: 5,
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(5),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "locale", Value: "3"}},
				},
			},
			Capacity: testStoreCapacitySetup(90, 10),
		},
		{
			StoreID: 6,
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(6),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "locale", Value: "3"}},
				},
			},
			Capacity: testStoreCapacitySetup(80, 20),
		},
		{
			StoreID: 7,
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(7),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "locale", Value: "3"}},
				},
			},
			Capacity: testStoreCapacitySetup(80, 20),
		},
		{
			StoreID: 8,
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(8),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "locale", Value: "3"}},
				},
			},
			Capacity: testStoreCapacitySetup(80, 20),
		},
	}

	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	testCases := []struct {
		existing []roachpb.ReplicaDescriptor
		expected roachpb.StoreID // 0 if no rebalance is expected
	}{
		{replicas(1, 3, 5), 0},
		{replicas(2, 3, 5), 1},
		{replicas(1, 4, 5), 3},
		{replicas(1, 3, 6), 5},
		{replicas(1, 5, 6), 3},
		{replicas(2, 5, 6), 3},
		{replicas(3, 5, 6), 1},
		{replicas(4, 5, 6), 1},
	}

	for i, tc := range testCases {
		result, details := a.RebalanceTarget(
			ctx,
			config.ZoneConfig{},
			nil, /* raftStatus */
			testRangeInfo(tc.existing, firstRange),
			storeFilterThrottled,
			false,
		)
		var resultID roachpb.StoreID
		if result != nil {
			resultID = result.StoreID
		}
		if resultID != tc.expected {
			t.Errorf("%d: RebalanceTarget(%v) expected s%d; got %v: %s", i, tc.existing, tc.expected, result, details)
		}
	}

	// Add a couple less full nodes in a fourth locality, then run a few more tests:
	stores = append(stores, &roachpb.StoreDescriptor{
		StoreID: 9,
		Node: roachpb.NodeDescriptor{
			NodeID: roachpb.NodeID(9),
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "locale", Value: "4"}},
			},
		},
		Capacity: testStoreCapacitySetup(70, 30),
	})
	stores = append(stores, &roachpb.StoreDescriptor{
		StoreID: 10,
		Node: roachpb.NodeDescriptor{
			NodeID: roachpb.NodeID(10),
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "locale", Value: "4"}},
			},
		},
		Capacity: testStoreCapacitySetup(60, 40),
	})

	sg.GossipStores(stores, t)

	testCases2 := []struct {
		existing []roachpb.ReplicaDescriptor
		expected []roachpb.StoreID
	}{
		{replicas(1, 3, 5), []roachpb.StoreID{9}},
		{replicas(2, 3, 5), []roachpb.StoreID{9}},
		{replicas(1, 4, 5), []roachpb.StoreID{9}},
		{replicas(1, 3, 6), []roachpb.StoreID{9}},
		{replicas(1, 5, 6), []roachpb.StoreID{9}},
		{replicas(2, 5, 6), []roachpb.StoreID{9}},
		{replicas(3, 5, 6), []roachpb.StoreID{9}},
		{replicas(4, 5, 6), []roachpb.StoreID{9}},
		{replicas(5, 6, 7), []roachpb.StoreID{9}},
		{replicas(1, 5, 9), nil},
		{replicas(3, 5, 9), nil},
		{replicas(1, 3, 9), []roachpb.StoreID{5, 6, 7, 8}},
		{replicas(1, 3, 10), []roachpb.StoreID{5, 6, 7, 8}},
		// This last case is a bit more interesting - the difference in range count
		// between s10 an s9 is significant enough to motivate a rebalance if they
		// were the only two valid options, but they're both considered underful
		// relative to the other equally valid placement options (s3 and s4), so
		// the system doesn't consider it helpful to rebalance between them. It'd
		// prefer to move replicas onto both s9 and s10 from other stores.
		{replicas(1, 5, 10), nil},
	}

	for i, tc := range testCases2 {
		log.Infof(ctx, "case #%d", i)
		result, details := a.RebalanceTarget(
			ctx,
			config.ZoneConfig{},
			nil, /* raftStatus */
			testRangeInfo(tc.existing, firstRange),
			storeFilterThrottled,
			false,
		)
		var gotExpected bool
		if result == nil {
			gotExpected = (tc.expected == nil)
		} else {
			for _, expectedStoreID := range tc.expected {
				if result.StoreID == expectedStoreID {
					gotExpected = true
					break
				}
			}
		}
		if !gotExpected {
			t.Errorf("%d: RebalanceTarget(%v) expected store in %v; got %v: %s",
				i, tc.existing, tc.expected, result, details)
		}
	}
}

func TestAllocatorTransferLeaseTargetMultiStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ true)
	defer stopper.Stop(context.Background())

	// 3 nodes and 6 stores where the lease count for the first store on each
	// node is equal to 10x the node ID.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 6; i++ {
		node := 1 + (i-1)/2
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID:  roachpb.StoreID(i),
			Node:     roachpb.NodeDescriptor{NodeID: roachpb.NodeID(node)},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(10 * node * (i % 2))},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	existing := []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: 1},
		{NodeID: 2, StoreID: 3},
		{NodeID: 3, StoreID: 5},
	}

	testCases := []struct {
		leaseholder roachpb.StoreID
		check       bool
		expected    roachpb.StoreID
	}{
		{leaseholder: 1, check: false, expected: 3},
		{leaseholder: 1, check: true, expected: 0},
		{leaseholder: 3, check: false, expected: 1},
		{leaseholder: 3, check: true, expected: 0},
		{leaseholder: 5, check: false, expected: 1},
		{leaseholder: 5, check: true, expected: 1},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			target := a.TransferLeaseTarget(
				context.Background(),
				config.ZoneConfig{},
				existing,
				c.leaseholder,
				0,
				nil, /* replicaStats */
				c.check,
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
			)
			if c.expected != target.StoreID {
				t.Fatalf("expected %d, but found %d", c.expected, target.StoreID)
			}
		})
	}
}

func TestAllocatorShouldTransferLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ true)
	defer stopper.Stop(context.Background())

	// 4 stores where the lease count for each store is equal to 10x the store
	// ID.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 4; i++ {
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID:  roachpb.StoreID(i),
			Node:     roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(10 * i)},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	testCases := []struct {
		leaseholder roachpb.StoreID
		existing    []roachpb.ReplicaDescriptor
		expected    bool
	}{
		{leaseholder: 1, existing: nil, expected: false},
		{leaseholder: 2, existing: nil, expected: false},
		{leaseholder: 3, existing: nil, expected: false},
		{leaseholder: 4, existing: nil, expected: false},
		{leaseholder: 3, existing: replicas(1), expected: true},
		{leaseholder: 3, existing: replicas(1, 2), expected: true},
		{leaseholder: 3, existing: replicas(2), expected: false},
		{leaseholder: 3, existing: replicas(3), expected: false},
		{leaseholder: 3, existing: replicas(4), expected: false},
		{leaseholder: 4, existing: replicas(1), expected: true},
		{leaseholder: 4, existing: replicas(2), expected: true},
		{leaseholder: 4, existing: replicas(3), expected: true},
		{leaseholder: 4, existing: replicas(1, 2, 3), expected: true},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			result := a.ShouldTransferLease(
				context.Background(),
				config.ZoneConfig{},
				c.existing,
				c.leaseholder,
				0,
				nil, /* replicaStats */
			)
			if c.expected != result {
				t.Fatalf("expected %v, but found %v", c.expected, result)
			}
		})
	}
}

func TestAllocatorShouldTransferLeaseDraining(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, storePool, nl := createTestStorePool(
		TestTimeUntilStoreDeadOff, true /* deterministic */, NodeLivenessStatus_LIVE)
	a := MakeAllocator(storePool, func(string) (time.Duration, bool) {
		return 0, true
	})
	defer stopper.Stop(context.Background())

	// 4 stores where the lease count for each store is equal to 10x the store
	// ID.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 4; i++ {
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID:  roachpb.StoreID(i),
			Node:     roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(10 * i)},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	// UNAVAILABLE is the node liveness status used for a node that's draining.
	nl.setNodeStatus(1, NodeLivenessStatus_UNAVAILABLE)

	testCases := []struct {
		leaseholder roachpb.StoreID
		existing    []roachpb.ReplicaDescriptor
		expected    bool
	}{
		{leaseholder: 1, existing: nil, expected: false},
		{leaseholder: 2, existing: nil, expected: false},
		{leaseholder: 3, existing: nil, expected: false},
		{leaseholder: 4, existing: nil, expected: false},
		{leaseholder: 2, existing: replicas(1), expected: false},
		{leaseholder: 3, existing: replicas(1), expected: false},
		{leaseholder: 3, existing: replicas(1, 2), expected: false},
		{leaseholder: 3, existing: replicas(1, 2, 4), expected: false},
		{leaseholder: 4, existing: replicas(1), expected: false},
		{leaseholder: 4, existing: replicas(1, 2), expected: true},
		{leaseholder: 4, existing: replicas(1, 3), expected: true},
		{leaseholder: 4, existing: replicas(1, 2, 3), expected: true},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			result := a.ShouldTransferLease(
				context.Background(),
				config.ZoneConfig{},
				c.existing,
				c.leaseholder,
				0,
				nil, /* replicaStats */
			)
			if c.expected != result {
				t.Fatalf("expected %v, but found %v", c.expected, result)
			}
		})
	}
}

func TestAllocatorLeasePreferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ true)
	defer stopper.Stop(context.Background())

	// 4 stores with distinct localities, store attributes, and node attributes
	// where the lease count for each store is equal to 100x the store ID.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 4; i++ {
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID: roachpb.StoreID(i),
			Attrs:   roachpb.Attributes{Attrs: []string{fmt.Sprintf("s%d", i)}},
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(i),
				Attrs:  roachpb.Attributes{Attrs: []string{fmt.Sprintf("n%d", i)}},
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "dc", Value: strconv.Itoa(i)},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(100 * i)},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	preferDC1 := []config.LeasePreference{
		{Constraints: []config.Constraint{{Key: "dc", Value: "1", Type: config.Constraint_REQUIRED}}},
	}
	preferDC4Then3Then2 := []config.LeasePreference{
		{Constraints: []config.Constraint{{Key: "dc", Value: "4", Type: config.Constraint_REQUIRED}}},
		{Constraints: []config.Constraint{{Key: "dc", Value: "3", Type: config.Constraint_REQUIRED}}},
		{Constraints: []config.Constraint{{Key: "dc", Value: "2", Type: config.Constraint_REQUIRED}}},
	}
	preferN2ThenS3 := []config.LeasePreference{
		{Constraints: []config.Constraint{{Value: "n2", Type: config.Constraint_REQUIRED}}},
		{Constraints: []config.Constraint{{Value: "s3", Type: config.Constraint_REQUIRED}}},
	}
	preferNotS1ThenNotN2 := []config.LeasePreference{
		{Constraints: []config.Constraint{{Value: "s1", Type: config.Constraint_PROHIBITED}}},
		{Constraints: []config.Constraint{{Value: "n2", Type: config.Constraint_PROHIBITED}}},
	}
	preferNotS1AndNotN2 := []config.LeasePreference{
		{
			Constraints: []config.Constraint{
				{Value: "s1", Type: config.Constraint_PROHIBITED},
				{Value: "n2", Type: config.Constraint_PROHIBITED},
			},
		},
	}
	preferMatchesNothing := []config.LeasePreference{
		{Constraints: []config.Constraint{{Key: "dc", Value: "5", Type: config.Constraint_REQUIRED}}},
		{Constraints: []config.Constraint{{Value: "n6", Type: config.Constraint_REQUIRED}}},
	}

	testCases := []struct {
		leaseholder        roachpb.StoreID
		existing           []roachpb.ReplicaDescriptor
		preferences        []config.LeasePreference
		expectedCheckTrue  roachpb.StoreID /* checkTransferLeaseSource = true */
		expectedCheckFalse roachpb.StoreID /* checkTransferLeaseSource = false */
	}{
		{1, nil, preferDC1, 0, 0},
		{1, replicas(1, 2, 3, 4), preferDC1, 0, 2},
		{1, replicas(2, 3, 4), preferDC1, 0, 2},
		{2, replicas(1, 2, 3, 4), preferDC1, 1, 1},
		{2, replicas(2, 3, 4), preferDC1, 0, 3},
		{4, replicas(2, 3, 4), preferDC1, 2, 2},
		{1, nil, preferDC4Then3Then2, 0, 0},
		{1, replicas(1, 2, 3, 4), preferDC4Then3Then2, 4, 4},
		{1, replicas(1, 2, 3), preferDC4Then3Then2, 3, 3},
		{1, replicas(1, 2), preferDC4Then3Then2, 2, 2},
		{3, replicas(1, 2, 3, 4), preferDC4Then3Then2, 4, 4},
		{3, replicas(1, 2, 3), preferDC4Then3Then2, 0, 2},
		{3, replicas(1, 3), preferDC4Then3Then2, 0, 1},
		{4, replicas(1, 2, 3, 4), preferDC4Then3Then2, 0, 3},
		{4, replicas(1, 2, 4), preferDC4Then3Then2, 0, 2},
		{4, replicas(1, 4), preferDC4Then3Then2, 0, 1},
		{1, replicas(1, 2, 3, 4), preferN2ThenS3, 2, 2},
		{1, replicas(1, 3, 4), preferN2ThenS3, 3, 3},
		{1, replicas(1, 4), preferN2ThenS3, 0, 4},
		{2, replicas(1, 2, 3, 4), preferN2ThenS3, 0, 3},
		{2, replicas(1, 2, 4), preferN2ThenS3, 0, 1},
		{3, replicas(1, 2, 3, 4), preferN2ThenS3, 2, 2},
		{3, replicas(1, 3, 4), preferN2ThenS3, 0, 1},
		{4, replicas(1, 4), preferN2ThenS3, 1, 1},
		{1, replicas(1, 2, 3, 4), preferNotS1ThenNotN2, 2, 2},
		{1, replicas(1, 3, 4), preferNotS1ThenNotN2, 3, 3},
		{1, replicas(1, 2), preferNotS1ThenNotN2, 2, 2},
		{1, replicas(1), preferNotS1ThenNotN2, 0, 0},
		{2, replicas(1, 2, 3, 4), preferNotS1ThenNotN2, 0, 3},
		{2, replicas(2, 3, 4), preferNotS1ThenNotN2, 0, 3},
		{2, replicas(1, 2, 3), preferNotS1ThenNotN2, 0, 3},
		{2, replicas(1, 2, 4), preferNotS1ThenNotN2, 0, 4},
		{4, replicas(1, 2, 3, 4), preferNotS1ThenNotN2, 2, 2},
		{4, replicas(1, 4), preferNotS1ThenNotN2, 0, 1},
		{1, replicas(1, 2, 3, 4), preferNotS1AndNotN2, 3, 3},
		{1, replicas(1, 2), preferNotS1AndNotN2, 0, 2},
		{2, replicas(1, 2, 3, 4), preferNotS1AndNotN2, 3, 3},
		{2, replicas(2, 3, 4), preferNotS1AndNotN2, 3, 3},
		{2, replicas(1, 2, 3), preferNotS1AndNotN2, 3, 3},
		{2, replicas(1, 2, 4), preferNotS1AndNotN2, 4, 4},
		{3, replicas(1, 3), preferNotS1AndNotN2, 0, 1},
		{4, replicas(1, 4), preferNotS1AndNotN2, 0, 1},
		{1, replicas(1, 2, 3, 4), preferMatchesNothing, 0, 2},
		{2, replicas(1, 2, 3, 4), preferMatchesNothing, 0, 1},
		{3, replicas(1, 3, 4), preferMatchesNothing, 1, 1},
		{4, replicas(1, 3, 4), preferMatchesNothing, 1, 1},
		{4, replicas(2, 3, 4), preferMatchesNothing, 2, 2},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			zone := config.ZoneConfig{LeasePreferences: c.preferences}
			result := a.ShouldTransferLease(
				context.Background(),
				zone,
				c.existing,
				c.leaseholder,
				0,
				nil, /* replicaStats */
			)
			expectTransfer := c.expectedCheckTrue != 0
			if expectTransfer != result {
				t.Errorf("expected %v, but found %v", expectTransfer, result)
			}
			target := a.TransferLeaseTarget(
				context.Background(),
				zone,
				c.existing,
				c.leaseholder,
				0,
				nil,   /* replicaStats */
				true,  /* checkTransferLeaseSource */
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
			)
			if c.expectedCheckTrue != target.StoreID {
				t.Errorf("expected s%d for check=true, but found %v", c.expectedCheckTrue, target)
			}
			target = a.TransferLeaseTarget(
				context.Background(),
				zone,
				c.existing,
				c.leaseholder,
				0,
				nil,   /* replicaStats */
				false, /* checkTransferLeaseSource */
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
			)
			if c.expectedCheckFalse != target.StoreID {
				t.Errorf("expected s%d for check=false, but found %v", c.expectedCheckFalse, target)
			}
		})
	}
}

func TestAllocatorLeasePreferencesMultipleStoresPerLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ true)
	defer stopper.Stop(context.Background())

	// 6 stores, 2 in each of 3 distinct localities.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 6; i++ {
		var region, zone string
		if i <= 2 {
			region = "us-east1"
			zone = "us-east1-a"
		} else if i <= 4 {
			region = "us-east1"
			zone = "us-east1-b"
		} else {
			region = "us-west1"
			zone = "us-west1-a"
		}
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID: roachpb.StoreID(i),
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(i),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "region", Value: region},
						{Key: "zone", Value: zone},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(100 * i)},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	preferEast := []config.LeasePreference{
		{Constraints: []config.Constraint{{Key: "region", Value: "us-east1", Type: config.Constraint_REQUIRED}}},
	}
	preferNotEast := []config.LeasePreference{
		{Constraints: []config.Constraint{{Key: "region", Value: "us-east1", Type: config.Constraint_PROHIBITED}}},
	}

	testCases := []struct {
		leaseholder        roachpb.StoreID
		existing           []roachpb.ReplicaDescriptor
		preferences        []config.LeasePreference
		expectedCheckTrue  roachpb.StoreID /* checkTransferLeaseSource = true */
		expectedCheckFalse roachpb.StoreID /* checkTransferLeaseSource = false */
	}{
		{1, replicas(1, 3, 5), preferEast, 0, 3},
		{1, replicas(1, 2, 3), preferEast, 0, 2},
		{3, replicas(1, 3, 5), preferEast, 0, 1},
		{5, replicas(1, 4, 5), preferEast, 1, 1},
		{5, replicas(3, 4, 5), preferEast, 3, 3},
		{1, replicas(1, 5, 6), preferEast, 0, 5},
		{1, replicas(1, 3, 5), preferNotEast, 5, 5},
		{1, replicas(1, 5, 6), preferNotEast, 5, 5},
		{3, replicas(1, 3, 5), preferNotEast, 5, 5},
		{5, replicas(1, 5, 6), preferNotEast, 0, 6},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			zone := config.ZoneConfig{LeasePreferences: c.preferences}
			target := a.TransferLeaseTarget(
				context.Background(),
				zone,
				c.existing,
				c.leaseholder,
				0,
				nil,   /* replicaStats */
				true,  /* checkTransferLeaseSource */
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
			)
			if c.expectedCheckTrue != target.StoreID {
				t.Errorf("expected s%d for check=true, but found %v", c.expectedCheckTrue, target)
			}
			target = a.TransferLeaseTarget(
				context.Background(),
				zone,
				c.existing,
				c.leaseholder,
				0,
				nil,   /* replicaStats */
				false, /* checkTransferLeaseSource */
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
			)
			if c.expectedCheckFalse != target.StoreID {
				t.Errorf("expected s%d for check=false, but found %v", c.expectedCheckFalse, target)
			}
		})
	}
}

func TestAllocatorRemoveTargetLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.Background())
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(multiDiversityDCStores, t)

	// Given a set of existing replicas for a range, pick out the ones that should
	// be removed purely on the basis of locality diversity.
	testCases := []struct {
		existing []roachpb.StoreID
		expected []roachpb.StoreID
	}{
		{
			[]roachpb.StoreID{1, 2, 3, 5},
			[]roachpb.StoreID{1, 2},
		},
		{
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{1, 2},
		},
		{
			[]roachpb.StoreID{1, 3, 4, 5},
			[]roachpb.StoreID{3, 4},
		},
		{
			[]roachpb.StoreID{1, 3, 5, 6},
			[]roachpb.StoreID{5, 6},
		},
		{
			[]roachpb.StoreID{1, 3, 5},
			[]roachpb.StoreID{1, 3, 5},
		},
		{
			[]roachpb.StoreID{1, 3, 4, 6, 7, 8},
			[]roachpb.StoreID{3, 4, 7, 8},
		},
	}
	for _, c := range testCases {
		existingRepls := make([]roachpb.ReplicaDescriptor, len(c.existing))
		for i, storeID := range c.existing {
			existingRepls[i] = roachpb.ReplicaDescriptor{
				NodeID:  roachpb.NodeID(storeID),
				StoreID: storeID,
			}
		}
		targetRepl, details, err := a.RemoveTarget(
			context.Background(),
			config.ZoneConfig{},
			existingRepls,
			testRangeInfo(existingRepls, firstRange),
			false,
		)
		if err != nil {
			t.Fatal(err)
		}
		var found bool
		for _, storeID := range c.expected {
			if targetRepl.StoreID == storeID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected RemoveTarget(%v) in %v, but got %d; details: %s", c.existing, c.expected, targetRepl.StoreID, details)
		}
	}
}

func TestAllocatorAllocateTargetLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.Background())
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(multiDiversityDCStores, t)

	// Given a set of existing replicas for a range, rank which of the remaining
	// stores from multiDiversityDCStores would be the best addition to the range
	// purely on the basis of locality diversity.
	testCases := []struct {
		existing []roachpb.StoreID
		expected []roachpb.StoreID
	}{
		{
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{5, 6, 7, 8},
		},
		{
			[]roachpb.StoreID{1, 3, 4},
			[]roachpb.StoreID{5, 6, 7, 8},
		},
		{
			[]roachpb.StoreID{3, 4, 5},
			[]roachpb.StoreID{1, 2, 7, 8},
		},
		{
			[]roachpb.StoreID{1, 7, 8},
			[]roachpb.StoreID{3, 4, 5, 6},
		},
		{
			[]roachpb.StoreID{5, 7, 8},
			[]roachpb.StoreID{1, 2, 3, 4},
		},
		{
			[]roachpb.StoreID{1, 3, 5},
			[]roachpb.StoreID{7, 8},
		},
		{
			[]roachpb.StoreID{1, 3, 7},
			[]roachpb.StoreID{5, 6},
		},
		{
			[]roachpb.StoreID{1, 5, 7},
			[]roachpb.StoreID{3, 4},
		},
		{
			[]roachpb.StoreID{3, 5, 7},
			[]roachpb.StoreID{1, 2},
		},
	}

	for _, c := range testCases {
		existingRepls := make([]roachpb.ReplicaDescriptor, len(c.existing))
		for i, storeID := range c.existing {
			existingRepls[i] = roachpb.ReplicaDescriptor{
				NodeID:  roachpb.NodeID(storeID),
				StoreID: storeID,
			}
		}
		targetStore, details, err := a.AllocateTarget(
			context.Background(),
			config.ZoneConfig{},
			existingRepls,
			testRangeInfo(existingRepls, firstRange),
			false,
		)
		if err != nil {
			t.Fatal(err)
		}
		var found bool
		for _, storeID := range c.expected {
			if targetStore.StoreID == storeID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected AllocateTarget(%v) in %v, but got %d; details: %s", c.existing, c.expected, targetStore.StoreID, details)
		}
	}
}

func TestAllocatorRebalanceTargetLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.Background())

	stores := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     multiDiversityDCStores[0].Node,
			Capacity: roachpb.StoreCapacity{RangeCount: 10},
		},
		{
			StoreID:  2,
			Node:     multiDiversityDCStores[1].Node,
			Capacity: roachpb.StoreCapacity{RangeCount: 20},
		},
		{
			StoreID:  3,
			Node:     multiDiversityDCStores[2].Node,
			Capacity: roachpb.StoreCapacity{RangeCount: 10},
		},
		{
			StoreID:  4,
			Node:     multiDiversityDCStores[3].Node,
			Capacity: roachpb.StoreCapacity{RangeCount: 20},
		},
		{
			StoreID:  5,
			Node:     multiDiversityDCStores[4].Node,
			Capacity: roachpb.StoreCapacity{RangeCount: 10},
		},
		{
			StoreID:  6,
			Node:     multiDiversityDCStores[5].Node,
			Capacity: roachpb.StoreCapacity{RangeCount: 20},
		},
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	testCases := []struct {
		existing []roachpb.StoreID
		expected []roachpb.StoreID
	}{
		{
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{5},
		},
		{
			[]roachpb.StoreID{1, 3, 4},
			[]roachpb.StoreID{5},
		},
		{
			[]roachpb.StoreID{1, 3, 6},
			[]roachpb.StoreID{5},
		},
		{
			[]roachpb.StoreID{1, 2, 5},
			[]roachpb.StoreID{3},
		},
		{
			[]roachpb.StoreID{1, 2, 6},
			[]roachpb.StoreID{3},
		},
		{
			[]roachpb.StoreID{1, 4, 5},
			[]roachpb.StoreID{3},
		},
		{
			[]roachpb.StoreID{1, 4, 6},
			[]roachpb.StoreID{3, 5},
		},
		{
			[]roachpb.StoreID{3, 4, 5},
			[]roachpb.StoreID{1},
		},
		{
			[]roachpb.StoreID{3, 4, 6},
			[]roachpb.StoreID{1},
		},
		{
			[]roachpb.StoreID{4, 5, 6},
			[]roachpb.StoreID{1},
		},
		{
			[]roachpb.StoreID{2, 4, 6},
			[]roachpb.StoreID{1, 3, 5},
		},
	}

	for i, c := range testCases {
		existingRepls := make([]roachpb.ReplicaDescriptor, len(c.existing))
		for i, storeID := range c.existing {
			existingRepls[i] = roachpb.ReplicaDescriptor{
				NodeID:  roachpb.NodeID(storeID),
				StoreID: storeID,
			}
		}
		targetStore, details := a.RebalanceTarget(
			context.Background(),
			config.ZoneConfig{},
			nil,
			testRangeInfo(existingRepls, firstRange),
			storeFilterThrottled,
			false,
		)
		if targetStore == nil {
			t.Fatalf("%d: RebalanceTarget(%v) returned no target store; details: %s", i, c.existing, details)
		}
		var found bool
		for _, storeID := range c.expected {
			if targetStore.StoreID == storeID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%d: expected RebalanceTarget(%v) in %v, but got %d; details: %s",
				i, c.existing, c.expected, targetStore.StoreID, details)
		}
	}
}

var (
	threeSpecificLocalities = []config.Constraints{
		{
			Constraints: []config.Constraint{
				{Key: "datacenter", Value: "a", Type: config.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
		{
			Constraints: []config.Constraint{
				{Key: "datacenter", Value: "b", Type: config.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
		{
			Constraints: []config.Constraint{
				{Key: "datacenter", Value: "c", Type: config.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
	}

	twoAndOneLocalities = []config.Constraints{
		{
			Constraints: []config.Constraint{
				{Key: "datacenter", Value: "a", Type: config.Constraint_REQUIRED},
			},
			NumReplicas: 2,
		},
		{
			Constraints: []config.Constraint{
				{Key: "datacenter", Value: "b", Type: config.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
	}

	threeInOneLocality = []config.Constraints{
		{
			Constraints: []config.Constraint{
				{Key: "datacenter", Value: "a", Type: config.Constraint_REQUIRED},
			},
			NumReplicas: 3,
		},
	}

	twoAndOneNodeAttrs = []config.Constraints{
		{
			Constraints: []config.Constraint{
				{Value: "ssd", Type: config.Constraint_REQUIRED},
			},
			NumReplicas: 2,
		},
		{
			Constraints: []config.Constraint{
				{Value: "hdd", Type: config.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
	}

	twoAndOneStoreAttrs = []config.Constraints{
		{
			Constraints: []config.Constraint{
				{Value: "odd", Type: config.Constraint_REQUIRED},
			},
			NumReplicas: 2,
		},
		{
			Constraints: []config.Constraint{
				{Value: "even", Type: config.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
	}

	mixLocalityAndAttrs = []config.Constraints{
		{
			Constraints: []config.Constraint{
				{Key: "datacenter", Value: "a", Type: config.Constraint_REQUIRED},
				{Value: "ssd", Type: config.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
		{
			Constraints: []config.Constraint{
				{Key: "datacenter", Value: "b", Type: config.Constraint_REQUIRED},
				{Value: "odd", Type: config.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
		{
			Constraints: []config.Constraint{
				{Value: "even", Type: config.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
	}

	twoSpecificLocalities = []config.Constraints{
		{
			Constraints: []config.Constraint{
				{Key: "datacenter", Value: "a", Type: config.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
		{
			Constraints: []config.Constraint{
				{Key: "datacenter", Value: "b", Type: config.Constraint_REQUIRED},
			},
			NumReplicas: 1,
		},
	}
)

func TestAllocateCandidatesNumReplicasConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.Background())
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(multiDiversityDCStores, t)
	sl, _, _ := a.storePool.getStoreList(firstRange, storeFilterThrottled)

	// Given a set of existing replicas for a range, rank which of the remaining
	// stores from multiDiversityDCStores would be the best addition to the range
	// purely on the basis of constraint satisfaction and locality diversity.
	testCases := []struct {
		constraints []config.Constraints
		existing    []roachpb.StoreID
		expected    []roachpb.StoreID
	}{
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{},
			[]roachpb.StoreID{1, 2, 3, 4, 5, 6},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{7, 8},
			[]roachpb.StoreID{1, 2, 3, 4, 5, 6},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{3, 4, 5, 6},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{3, 5},
			[]roachpb.StoreID{1, 2},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{3, 4},
			[]roachpb.StoreID{1, 2, 5, 6},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{1, 3, 5},
			[]roachpb.StoreID{2, 4, 6},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{},
			[]roachpb.StoreID{1, 2, 3, 4},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{3, 4}, // 2 isn't included because its diversity is worse
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{1, 2},
			[]roachpb.StoreID{3, 4},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{4},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{3},
			[]roachpb.StoreID{1, 2},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{5},
			[]roachpb.StoreID{1, 2, 3, 4},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{},
			[]roachpb.StoreID{1, 2},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{3, 4, 5},
			[]roachpb.StoreID{1, 2},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{2},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{1, 2},
			[]roachpb.StoreID{},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{},
			[]roachpb.StoreID{1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{2},
			[]roachpb.StoreID{3, 5, 7},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{3, 4, 5, 6, 7, 8},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1, 2},
			[]roachpb.StoreID{3, 5, 7},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1, 3},
			[]roachpb.StoreID{6, 8},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1, 3, 6},
			[]roachpb.StoreID{7, 8},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{},
			[]roachpb.StoreID{1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{2},
			[]roachpb.StoreID{3, 5, 7},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{3, 4, 5, 6, 7, 8},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{1, 2},
			[]roachpb.StoreID{3, 5, 7},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{1, 3},
			[]roachpb.StoreID{6, 8},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{1, 3, 6},
			[]roachpb.StoreID{7, 8},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{},
			[]roachpb.StoreID{1, 2, 3, 4, 6, 8},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{3, 4, 6, 8},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{2},
			[]roachpb.StoreID{3},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{3},
			[]roachpb.StoreID{1, 2, 6, 8},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{2, 3},
			[]roachpb.StoreID{1},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{1, 2},
			[]roachpb.StoreID{3},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{1, 3},
			[]roachpb.StoreID{6, 8},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{6, 8},
		},
	}

	for testIdx, tc := range testCases {
		existingRepls := make([]roachpb.ReplicaDescriptor, len(tc.existing))
		for i, storeID := range tc.existing {
			existingRepls[i] = roachpb.ReplicaDescriptor{
				NodeID:  roachpb.NodeID(storeID),
				StoreID: storeID,
			}
		}
		rangeInfo := testRangeInfo(existingRepls, firstRange)
		zone := config.ZoneConfig{Constraints: tc.constraints}
		analyzed := analyzeConstraints(
			context.Background(), a.storePool.getStoreDescriptor, rangeInfo.Desc.Replicas, zone)
		candidates := allocateCandidates(
			sl,
			analyzed,
			existingRepls,
			rangeInfo,
			a.storePool.getLocalities(existingRepls),
			a.scorerOptions(false),
		)
		best := candidates.best()
		match := true
		if len(tc.expected) != len(best) {
			match = false
		} else {
			sort.Slice(best, func(i, j int) bool {
				return best[i].store.StoreID < best[j].store.StoreID
			})
			for i := range tc.expected {
				if tc.expected[i] != best[i].store.StoreID {
					match = false
					break
				}
			}
		}
		if !match {
			t.Errorf("%d: expected allocateCandidates(%v) = %v, but got %v",
				testIdx, tc.existing, tc.expected, candidates)
		}
	}
}

func TestRemoveCandidatesNumReplicasConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.Background())
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(multiDiversityDCStores, t)

	// Given a set of existing replicas for a range, rank which of the remaining
	// stores would be best to remove if we had to remove one purely on the basis
	// of constraint-matching and locality diversity.
	testCases := []struct {
		constraints []config.Constraints
		existing    []roachpb.StoreID
		expected    []roachpb.StoreID
	}{
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{1},
			[]roachpb.StoreID{1},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{1, 2},
			[]roachpb.StoreID{1, 2},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{1, 3},
			[]roachpb.StoreID{1, 3},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{1, 2},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{1, 3, 5},
			[]roachpb.StoreID{1, 3, 5},
		},
		{
			threeSpecificLocalities,
			[]roachpb.StoreID{1, 3, 7},
			[]roachpb.StoreID{7},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{1, 3},
			[]roachpb.StoreID{1, 3},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{1, 3, 5},
			[]roachpb.StoreID{5},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{1, 3, 4},
			[]roachpb.StoreID{3, 4},
		},
		{
			twoAndOneLocalities,
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{1, 2},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{1, 3},
			[]roachpb.StoreID{3},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{2, 3},
			[]roachpb.StoreID{3},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{3},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{3, 5, 7},
			[]roachpb.StoreID{3, 5, 7},
		},
		{
			threeInOneLocality,
			[]roachpb.StoreID{1, 2, 3, 5, 7},
			[]roachpb.StoreID{3, 5, 7},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1, 3},
			[]roachpb.StoreID{1, 3},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{1, 2},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1, 3, 6},
			[]roachpb.StoreID{1, 3, 6},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1, 4, 6},
			[]roachpb.StoreID{4, 6},
		},
		{
			twoAndOneNodeAttrs,
			[]roachpb.StoreID{1, 2, 6},
			[]roachpb.StoreID{2},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{1, 2},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{1, 3, 6},
			[]roachpb.StoreID{1, 3, 6},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{1, 4, 6},
			[]roachpb.StoreID{4, 6},
		},
		{
			twoAndOneStoreAttrs,
			[]roachpb.StoreID{1, 2, 6},
			[]roachpb.StoreID{2},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{1, 3, 6},
			[]roachpb.StoreID{1, 3, 6},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{1, 2, 3},
			[]roachpb.StoreID{1, 2},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{2, 3, 6},
			[]roachpb.StoreID{2, 6},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{2, 3, 4},
			[]roachpb.StoreID{4},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{5, 7},
			[]roachpb.StoreID{5, 7},
		},
		{
			// TODO(a-robinson): Should we prefer just 5 here for diversity reasons?
			// We'd have to rework our handling of invalid stores in a handful of
			// places, including in `candidateList.worst()`, to consider traits beyond
			// just invalidity.
			mixLocalityAndAttrs,
			[]roachpb.StoreID{5, 6, 7},
			[]roachpb.StoreID{5, 7},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{1, 5, 7},
			[]roachpb.StoreID{5, 7},
		},
		{
			mixLocalityAndAttrs,
			[]roachpb.StoreID{1, 6, 8},
			[]roachpb.StoreID{6, 8},
		},
	}

	for testIdx, tc := range testCases {
		sl, _, _ := a.storePool.getStoreListFromIDs(tc.existing, roachpb.RangeID(0), storeFilterNone)
		existingRepls := make([]roachpb.ReplicaDescriptor, len(tc.existing))
		for i, storeID := range tc.existing {
			existingRepls[i] = roachpb.ReplicaDescriptor{
				NodeID:  roachpb.NodeID(storeID),
				StoreID: storeID,
			}
		}
		rangeInfo := testRangeInfo(existingRepls, firstRange)
		zone := config.ZoneConfig{Constraints: tc.constraints}
		analyzed := analyzeConstraints(
			context.Background(), a.storePool.getStoreDescriptor, rangeInfo.Desc.Replicas, zone)
		candidates := removeCandidates(
			sl,
			analyzed,
			rangeInfo,
			a.storePool.getLocalities(existingRepls),
			a.scorerOptions(false),
		)
		if !expectedStoreIDsMatch(tc.expected, candidates.worst()) {
			t.Errorf("%d: expected removeCandidates(%v) = %v, but got %v",
				testIdx, tc.existing, tc.expected, candidates)
		}
	}
}

func expectedStoreIDsMatch(expected []roachpb.StoreID, results candidateList) bool {
	if len(expected) != len(results) {
		return false
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].store.StoreID < results[j].store.StoreID
	})
	for i := range expected {
		if expected[i] != results[i].store.StoreID {
			return false
		}
	}
	return true
}

func TestRebalanceCandidatesNumReplicasConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.Background())
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(multiDiversityDCStores, t)
	sl, _, _ := a.storePool.getStoreList(firstRange, storeFilterThrottled)

	// Given a set of existing replicas for a range, rank which of the remaining
	// stores would be best to remove if we had to remove one purely on the basis
	// of constraint-matching and locality diversity.
	type rebalanceStoreIDs struct {
		existing   []roachpb.StoreID
		candidates []roachpb.StoreID
	}
	testCases := []struct {
		constraints     []config.Constraints
		zoneNumReplicas int32
		existing        []roachpb.StoreID
		expected        []rebalanceStoreIDs
		validTargets    []roachpb.StoreID
	}{
		{
			constraints:  threeSpecificLocalities,
			existing:     []roachpb.StoreID{1},
			expected:     []rebalanceStoreIDs{}, // a store must be an improvement to justify rebalancing
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints:  threeSpecificLocalities,
			existing:     []roachpb.StoreID{1, 3},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints:  threeSpecificLocalities,
			existing:     []roachpb.StoreID{1, 3, 5},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints: threeSpecificLocalities,
			existing:    []roachpb.StoreID{1, 2},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
			},
			validTargets: []roachpb.StoreID{3, 4, 5, 6},
		},
		{
			constraints: threeSpecificLocalities,
			existing:    []roachpb.StoreID{1, 2, 3},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{5, 6},
				},
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{5, 6},
				},
			},
			validTargets: []roachpb.StoreID{5, 6},
		},
		{
			constraints: threeSpecificLocalities,
			existing:    []roachpb.StoreID{1, 3, 7},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{7},
					candidates: []roachpb.StoreID{5, 6},
				},
			},
			validTargets: []roachpb.StoreID{5, 6},
		},
		{
			constraints: threeSpecificLocalities,
			existing:    []roachpb.StoreID{1, 2, 7},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
				{
					existing:   []roachpb.StoreID{7},
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
			},
			validTargets: []roachpb.StoreID{3, 4, 5, 6},
		},
		{
			constraints: threeSpecificLocalities,
			existing:    []roachpb.StoreID{1, 7, 8},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{7},
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
				{
					existing:   []roachpb.StoreID{8},
					candidates: []roachpb.StoreID{3, 4, 5, 6},
				},
			},
			validTargets: []roachpb.StoreID{3, 4, 5, 6},
		},
		{
			constraints:  twoAndOneLocalities,
			existing:     []roachpb.StoreID{1, 2, 3},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints: twoAndOneLocalities,
			existing:    []roachpb.StoreID{2, 3, 4},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{1},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{1},
				},
			},
			validTargets: []roachpb.StoreID{1},
		},
		{
			constraints: twoAndOneLocalities,
			existing:    []roachpb.StoreID{1, 2, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{3, 4},
				},
			},
			validTargets: []roachpb.StoreID{3, 4},
		},
		{
			constraints: twoAndOneLocalities,
			existing:    []roachpb.StoreID{1, 3, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{2},
				},
			},
			validTargets: []roachpb.StoreID{2},
		},
		{
			constraints: twoAndOneLocalities,
			existing:    []roachpb.StoreID{1, 5, 6},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   []roachpb.StoreID{6},
					candidates: []roachpb.StoreID{3, 4},
				},
			},
			validTargets: []roachpb.StoreID{3, 4},
		},
		{
			constraints: twoAndOneLocalities,
			existing:    []roachpb.StoreID{3, 5, 6},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{1, 2},
				},
				{
					existing:   []roachpb.StoreID{6},
					candidates: []roachpb.StoreID{1, 2},
				},
			},
			validTargets: []roachpb.StoreID{1, 2},
		},
		{
			constraints: twoAndOneLocalities,
			existing:    []roachpb.StoreID{1, 3, 4},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{2},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{2},
				},
			},
			validTargets: []roachpb.StoreID{2},
		},
		{
			constraints:  threeInOneLocality,
			existing:     []roachpb.StoreID{1, 2, 3},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints: threeInOneLocality,
			existing:    []roachpb.StoreID{1, 3, 4},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{2},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{2},
				},
			},
			validTargets: []roachpb.StoreID{2},
		},
		{
			constraints: threeInOneLocality,
			existing:    []roachpb.StoreID{3, 4, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{1, 2},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{1, 2},
				},
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{1, 2},
				},
			},
			validTargets: []roachpb.StoreID{1, 2},
		},
		{
			constraints:  twoAndOneNodeAttrs,
			existing:     []roachpb.StoreID{1, 4, 5},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints:  twoAndOneNodeAttrs,
			existing:     []roachpb.StoreID{3, 6, 7},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints: twoAndOneNodeAttrs,
			existing:    []roachpb.StoreID{1, 2, 3},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{5, 7},
				},
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{6, 8},
				},
			},
			validTargets: []roachpb.StoreID{5, 6, 7, 8},
		},
		{
			constraints: twoAndOneNodeAttrs,
			existing:    []roachpb.StoreID{2, 3, 4},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{1, 5, 7},
				},
				{
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{5, 7},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{5, 7},
				},
			},
			validTargets: []roachpb.StoreID{5, 7},
		},
		{
			constraints: twoAndOneNodeAttrs,
			existing:    []roachpb.StoreID{2, 4, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{1, 7},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{3, 7},
				},
			},
			validTargets: []roachpb.StoreID{1, 3, 7},
		},
		{
			constraints: twoAndOneNodeAttrs,
			existing:    []roachpb.StoreID{1, 3, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{2, 8},
				},
				{
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{4, 8},
				},
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{6, 8},
				},
			},
			validTargets: []roachpb.StoreID{2, 4, 6, 8},
		},
		{
			constraints: twoAndOneNodeAttrs,
			existing:    []roachpb.StoreID{2, 4, 6},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{1, 7},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{3, 7},
				},
				{
					existing:   []roachpb.StoreID{6},
					candidates: []roachpb.StoreID{5, 7},
				},
			},
			validTargets: []roachpb.StoreID{1, 3, 5, 7},
		},
		{
			constraints:  twoAndOneStoreAttrs,
			existing:     []roachpb.StoreID{1, 4, 5},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints:  twoAndOneStoreAttrs,
			existing:     []roachpb.StoreID{3, 6, 7},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints: twoAndOneStoreAttrs,
			existing:    []roachpb.StoreID{1, 2, 3},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{5, 7},
				},
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{6, 8},
				},
			},
			validTargets: []roachpb.StoreID{5, 6, 7, 8},
		},
		{
			constraints: twoAndOneStoreAttrs,
			existing:    []roachpb.StoreID{2, 3, 4},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{1, 5, 7},
				},
				{
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{5, 7},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{5, 7},
				},
			},
			validTargets: []roachpb.StoreID{5, 7},
		},
		{
			constraints: twoAndOneStoreAttrs,
			existing:    []roachpb.StoreID{2, 4, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{1, 7},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{3, 7},
				},
			},
			validTargets: []roachpb.StoreID{1, 3, 7},
		},
		{
			constraints: twoAndOneStoreAttrs,
			existing:    []roachpb.StoreID{1, 3, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{2, 8},
				},
				{
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{4, 8},
				},
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{6, 8},
				},
			},
			validTargets: []roachpb.StoreID{2, 4, 6, 8},
		},
		{
			constraints: twoAndOneStoreAttrs,
			existing:    []roachpb.StoreID{2, 4, 6},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{1, 7},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{3, 7},
				},
				{
					existing:   []roachpb.StoreID{6},
					candidates: []roachpb.StoreID{5, 7},
				},
			},
			validTargets: []roachpb.StoreID{1, 3, 5, 7},
		},
		{
			constraints:  mixLocalityAndAttrs,
			existing:     []roachpb.StoreID{1, 3, 6},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints:  mixLocalityAndAttrs,
			existing:     []roachpb.StoreID{1, 3, 8},
			expected:     []rebalanceStoreIDs{},
			validTargets: []roachpb.StoreID{},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{1, 5, 8},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{3},
				},
			},
			validTargets: []roachpb.StoreID{3},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{1, 5, 6},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{3},
				},
				{
					existing:   []roachpb.StoreID{6},
					candidates: []roachpb.StoreID{3, 4, 8},
				},
			},
			validTargets: []roachpb.StoreID{3},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{1, 3, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{6, 8},
				},
			},
			validTargets: []roachpb.StoreID{6, 8},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{1, 2, 3},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{6, 8},
				},
			},
			validTargets: []roachpb.StoreID{6, 8},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{1, 3, 4},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{6, 8},
				},
			},
			validTargets: []roachpb.StoreID{6, 8},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{2, 3, 4},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{1},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{1},
				},
			},
			validTargets: []roachpb.StoreID{1},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{5, 6, 7},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{1, 3},
				},
				{
					existing:   []roachpb.StoreID{6},
					candidates: []roachpb.StoreID{1, 2, 3, 4},
				},
				{
					existing:   []roachpb.StoreID{7},
					candidates: []roachpb.StoreID{1, 3},
				},
			},
			validTargets: []roachpb.StoreID{1, 3},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{6, 7, 8},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{6},
					candidates: []roachpb.StoreID{1, 3},
				},
				{
					existing:   []roachpb.StoreID{7},
					candidates: []roachpb.StoreID{1, 3},
				},
				{
					existing:   []roachpb.StoreID{8},
					candidates: []roachpb.StoreID{1, 3},
				},
			},
			validTargets: []roachpb.StoreID{1, 3},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{1, 6, 8},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{6},
					candidates: []roachpb.StoreID{3},
				},
				{
					existing:   []roachpb.StoreID{8},
					candidates: []roachpb.StoreID{3},
				},
			},
			validTargets: []roachpb.StoreID{3},
		},
		{
			constraints: mixLocalityAndAttrs,
			existing:    []roachpb.StoreID{1, 5, 7},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{3, 4, 6},
				},
				{
					existing:   []roachpb.StoreID{7},
					candidates: []roachpb.StoreID{3, 4, 8},
				},
			},
			validTargets: []roachpb.StoreID{3, 4, 6, 8},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{1, 3, 5},
			expected:        []rebalanceStoreIDs{},
			validTargets:    []roachpb.StoreID{},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{1, 3, 7},
			expected:        []rebalanceStoreIDs{},
			validTargets:    []roachpb.StoreID{},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{2, 4, 8},
			expected:        []rebalanceStoreIDs{},
			validTargets:    []roachpb.StoreID{},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{1, 2, 3},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{5, 6, 7, 8},
				},
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{5, 6, 7, 8},
				},
			},
			validTargets: []roachpb.StoreID{5, 6, 7, 8},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{2, 3, 4},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{5, 6, 7, 8},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{5, 6, 7, 8},
				},
			},
			validTargets: []roachpb.StoreID{5, 6, 7, 8},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{1, 2, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{1},
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   []roachpb.StoreID{2},
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{3, 4},
				},
			},
			validTargets: []roachpb.StoreID{3, 4},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{3, 4, 5},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{3},
					candidates: []roachpb.StoreID{1, 2},
				},
				{
					existing:   []roachpb.StoreID{4},
					candidates: []roachpb.StoreID{1, 2},
				},
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{1, 2},
				},
			},
			validTargets: []roachpb.StoreID{1, 2},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{1, 5, 7},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   []roachpb.StoreID{7},
					candidates: []roachpb.StoreID{3, 4},
				},
			},
			validTargets: []roachpb.StoreID{3, 4},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{1, 5, 6},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{3, 4},
				},
				{
					existing:   []roachpb.StoreID{6},
					candidates: []roachpb.StoreID{3, 4},
				},
			},
			validTargets: []roachpb.StoreID{3, 4},
		},
		{
			constraints:     twoSpecificLocalities,
			zoneNumReplicas: 3,
			existing:        []roachpb.StoreID{5, 6, 7},
			expected: []rebalanceStoreIDs{
				{
					existing:   []roachpb.StoreID{5},
					candidates: []roachpb.StoreID{1, 2, 3, 4},
				},
				{
					existing:   []roachpb.StoreID{6},
					candidates: []roachpb.StoreID{1, 2, 3, 4},
				},
				{
					existing:   []roachpb.StoreID{7},
					candidates: []roachpb.StoreID{1, 2, 3, 4},
				},
			},
			validTargets: []roachpb.StoreID{1, 2, 3, 4},
		},
	}

	for testIdx, tc := range testCases {
		existingRepls := make([]roachpb.ReplicaDescriptor, len(tc.existing))
		for i, storeID := range tc.existing {
			existingRepls[i] = roachpb.ReplicaDescriptor{
				NodeID:  roachpb.NodeID(storeID),
				StoreID: storeID,
			}
		}
		rangeInfo := testRangeInfo(existingRepls, firstRange)
		zone := config.ZoneConfig{
			Constraints: tc.constraints,
			NumReplicas: tc.zoneNumReplicas,
		}
		analyzed := analyzeConstraints(
			context.Background(), a.storePool.getStoreDescriptor, rangeInfo.Desc.Replicas, zone)
		results := rebalanceCandidates(
			context.Background(),
			sl,
			analyzed,
			rangeInfo,
			a.storePool.getLocalities(existingRepls),
			a.storePool.getNodeLocalityString,
			a.scorerOptions(false),
		)
		match := true
		if len(tc.expected) != len(results) {
			match = false
		} else {
			sort.Slice(results, func(i, j int) bool {
				return results[i].existingCandidates[0].store.StoreID < results[j].existingCandidates[0].store.StoreID
			})
			for i := range tc.expected {
				if !expectedStoreIDsMatch(tc.expected[i].existing, results[i].existingCandidates) ||
					!expectedStoreIDsMatch(tc.expected[i].candidates, results[i].candidates) {
					match = false
					break
				}
			}
		}
		if !match {
			t.Errorf("%d: expected rebalanceCandidates(%v) = %v, but got %v",
				testIdx, tc.existing, tc.expected, results)
		} else {
			// Also verify that RebalanceTarget picks out one of the best options as
			// the final rebalance choice.
			target, details := a.RebalanceTarget(
				context.Background(), zone, nil, rangeInfo, storeFilterThrottled, false)
			var found bool
			if target == nil && len(tc.validTargets) == 0 {
				found = true
			}
			for _, storeID := range tc.validTargets {
				if storeID == target.StoreID {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("%d: expected RebalanceTarget(%v) to be in %v, but got %v; details: %s",
					testIdx, tc.existing, tc.validTargets, target, details)
			}
		}
	}
}

// Test out the load-based lease transfer algorithm against a variety of
// request distributions and inter-node latencies.
func TestAllocatorTransferLeaseTargetLoadBased(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, g, _, storePool, _ := createTestStorePool(
		TestTimeUntilStoreDeadOff, true /* deterministic */, NodeLivenessStatus_LIVE)
	defer stopper.Stop(context.Background())

	// 3 stores where the lease count for each store is equal to 10x the store ID.
	var stores []*roachpb.StoreDescriptor
	for i := 1; i <= 3; i++ {
		stores = append(stores, &roachpb.StoreDescriptor{
			StoreID: roachpb.StoreID(i),
			Node: roachpb.NodeDescriptor{
				NodeID:  roachpb.NodeID(i),
				Address: util.MakeUnresolvedAddr("tcp", strconv.Itoa(i)),
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "l", Value: strconv.Itoa(i)},
					},
				},
			},
			Capacity: roachpb.StoreCapacity{LeaseCount: int32(10 * i)},
		})
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	// Nodes need to have descriptors in gossip for the load-based algorithm to
	// consider transferring a lease to them.
	for _, store := range stores {
		if err := g.SetNodeDescriptor(&store.Node); err != nil {
			t.Fatal(err)
		}
	}

	localities := map[roachpb.NodeID]string{
		1: "l=1",
		2: "l=2",
		3: "l=3",
	}
	localityFn := func(nodeID roachpb.NodeID) string {
		return localities[nodeID]
	}
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)

	// Set up four different load distributions. Record a bunch of requests to
	// the unknown node 99 in evenlyBalanced to verify that requests from
	// unknown localities don't affect the algorithm.
	evenlyBalanced := newReplicaStats(clock, localityFn)
	evenlyBalanced.record(1)
	evenlyBalanced.record(2)
	evenlyBalanced.record(3)
	imbalanced1 := newReplicaStats(clock, localityFn)
	imbalanced2 := newReplicaStats(clock, localityFn)
	imbalanced3 := newReplicaStats(clock, localityFn)
	for i := 0; i < 100*int(MinLeaseTransferStatsDuration.Seconds()); i++ {
		evenlyBalanced.record(99)
		imbalanced1.record(1)
		imbalanced2.record(2)
		imbalanced3.record(3)
	}

	manual.Increment(int64(MinLeaseTransferStatsDuration))

	noLatency := map[string]time.Duration{}
	highLatency := map[string]time.Duration{
		stores[0].Node.Address.String(): 50 * time.Millisecond,
		stores[1].Node.Address.String(): 50 * time.Millisecond,
		stores[2].Node.Address.String(): 50 * time.Millisecond,
	}

	existing := []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: 1},
		{NodeID: 2, StoreID: 2},
		{NodeID: 3, StoreID: 3},
	}

	testCases := []struct {
		leaseholder roachpb.StoreID
		latency     map[string]time.Duration
		stats       *replicaStats
		check       bool
		expected    roachpb.StoreID
	}{
		// No existing lease holder, nothing to do.
		{leaseholder: 0, latency: noLatency, stats: evenlyBalanced, check: true, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: evenlyBalanced, check: true, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: evenlyBalanced, check: false, expected: 2},
		{leaseholder: 2, latency: noLatency, stats: evenlyBalanced, check: true, expected: 1},
		{leaseholder: 2, latency: noLatency, stats: evenlyBalanced, check: false, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: evenlyBalanced, check: true, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: evenlyBalanced, check: false, expected: 1},
		{leaseholder: 0, latency: noLatency, stats: imbalanced1, check: true, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced1, check: true, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced1, check: false, expected: 2},
		{leaseholder: 2, latency: noLatency, stats: imbalanced1, check: true, expected: 1},
		{leaseholder: 2, latency: noLatency, stats: imbalanced1, check: false, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced1, check: true, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced1, check: false, expected: 1},
		{leaseholder: 0, latency: noLatency, stats: imbalanced2, check: true, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced2, check: true, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced2, check: false, expected: 2},
		{leaseholder: 2, latency: noLatency, stats: imbalanced2, check: true, expected: 1},
		{leaseholder: 2, latency: noLatency, stats: imbalanced2, check: false, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced2, check: true, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced2, check: false, expected: 1},
		{leaseholder: 0, latency: noLatency, stats: imbalanced3, check: true, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced3, check: true, expected: 0},
		{leaseholder: 1, latency: noLatency, stats: imbalanced3, check: false, expected: 2},
		{leaseholder: 2, latency: noLatency, stats: imbalanced3, check: true, expected: 1},
		{leaseholder: 2, latency: noLatency, stats: imbalanced3, check: false, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced3, check: true, expected: 1},
		{leaseholder: 3, latency: noLatency, stats: imbalanced3, check: false, expected: 1},
		{leaseholder: 0, latency: highLatency, stats: evenlyBalanced, check: true, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: evenlyBalanced, check: true, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: evenlyBalanced, check: false, expected: 2},
		{leaseholder: 2, latency: highLatency, stats: evenlyBalanced, check: true, expected: 1},
		{leaseholder: 2, latency: highLatency, stats: evenlyBalanced, check: false, expected: 1},
		{leaseholder: 3, latency: highLatency, stats: evenlyBalanced, check: true, expected: 1},
		{leaseholder: 3, latency: highLatency, stats: evenlyBalanced, check: false, expected: 1},
		{leaseholder: 0, latency: highLatency, stats: imbalanced1, check: true, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: imbalanced1, check: true, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: imbalanced1, check: false, expected: 2},
		{leaseholder: 2, latency: highLatency, stats: imbalanced1, check: true, expected: 1},
		{leaseholder: 2, latency: highLatency, stats: imbalanced1, check: false, expected: 1},
		{leaseholder: 3, latency: highLatency, stats: imbalanced1, check: true, expected: 1},
		{leaseholder: 3, latency: highLatency, stats: imbalanced1, check: false, expected: 1},
		{leaseholder: 0, latency: highLatency, stats: imbalanced2, check: true, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: imbalanced2, check: true, expected: 2},
		{leaseholder: 1, latency: highLatency, stats: imbalanced2, check: false, expected: 2},
		{leaseholder: 2, latency: highLatency, stats: imbalanced2, check: true, expected: 0},
		{leaseholder: 2, latency: highLatency, stats: imbalanced2, check: false, expected: 1},
		{leaseholder: 3, latency: highLatency, stats: imbalanced2, check: true, expected: 2},
		{leaseholder: 3, latency: highLatency, stats: imbalanced2, check: false, expected: 2},
		{leaseholder: 0, latency: highLatency, stats: imbalanced3, check: true, expected: 0},
		{leaseholder: 1, latency: highLatency, stats: imbalanced3, check: true, expected: 3},
		{leaseholder: 1, latency: highLatency, stats: imbalanced3, check: false, expected: 3},
		{leaseholder: 2, latency: highLatency, stats: imbalanced3, check: true, expected: 3},
		{leaseholder: 2, latency: highLatency, stats: imbalanced3, check: false, expected: 3},
		{leaseholder: 3, latency: highLatency, stats: imbalanced3, check: true, expected: 0},
		{leaseholder: 3, latency: highLatency, stats: imbalanced3, check: false, expected: 1},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			a := MakeAllocator(storePool, func(addr string) (time.Duration, bool) {
				return c.latency[addr], true
			})
			target := a.TransferLeaseTarget(
				context.Background(),
				config.ZoneConfig{},
				existing,
				c.leaseholder,
				0,
				c.stats,
				c.check,
				true,  /* checkCandidateFullness */
				false, /* alwaysAllowDecisionWithoutStats */
			)
			if c.expected != target.StoreID {
				t.Errorf("expected %d, got %d", c.expected, target.StoreID)
			}
		})
	}
}

func TestLoadBasedLeaseRebalanceScore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	enableLoadBasedLeaseRebalancing.Override(&st.SV, true)

	remoteStore := roachpb.StoreDescriptor{
		Node: roachpb.NodeDescriptor{
			NodeID: 2,
		},
	}
	sourceStore := roachpb.StoreDescriptor{
		Node: roachpb.NodeDescriptor{
			NodeID: 1,
		},
	}

	testCases := []struct {
		remoteWeight  float64
		remoteLatency time.Duration
		remoteLeases  int32
		sourceWeight  float64
		sourceLeases  int32
		meanLeases    float64
		expected      int32
	}{
		// Evenly balanced leases stay balanced if requests are even
		{1, 0 * time.Millisecond, 10, 1, 10, 10, -2},
		{1, 0 * time.Millisecond, 100, 1, 100, 100, -21},
		{1, 0 * time.Millisecond, 1000, 1, 1000, 1000, -200},
		{1, 10 * time.Millisecond, 10, 1, 10, 10, -2},
		{1, 10 * time.Millisecond, 100, 1, 100, 100, -21},
		{1, 10 * time.Millisecond, 1000, 1, 1000, 1000, -200},
		{1, 50 * time.Millisecond, 10, 1, 10, 10, -2},
		{1, 50 * time.Millisecond, 100, 1, 100, 100, -21},
		{1, 50 * time.Millisecond, 1000, 1, 1000, 1000, -200},
		{1000, 0 * time.Millisecond, 10, 1000, 10, 10, -2},
		{1000, 0 * time.Millisecond, 100, 1000, 100, 100, -21},
		{1000, 0 * time.Millisecond, 1000, 1000, 1000, 1000, -200},
		{1000, 10 * time.Millisecond, 10, 1000, 10, 10, -2},
		{1000, 10 * time.Millisecond, 100, 1000, 100, 100, -21},
		{1000, 10 * time.Millisecond, 1000, 1000, 1000, 1000, -200},
		{1000, 50 * time.Millisecond, 10, 1000, 10, 10, -2},
		{1000, 50 * time.Millisecond, 100, 1000, 100, 100, -21},
		{1000, 50 * time.Millisecond, 1000, 1000, 1000, 1000, -200},
		// No latency favors lease balance despite request imbalance
		{10, 0 * time.Millisecond, 100, 1, 100, 100, -21},
		{100, 0 * time.Millisecond, 100, 1, 100, 100, -21},
		{1000, 0 * time.Millisecond, 100, 1, 100, 100, -21},
		{10000, 0 * time.Millisecond, 100, 1, 100, 100, -21},
		// Adding some latency changes that (perhaps a bit too much?)
		{10, 1 * time.Millisecond, 100, 1, 100, 100, -8},
		{100, 1 * time.Millisecond, 100, 1, 100, 100, 6},
		{1000, 1 * time.Millisecond, 100, 1, 100, 100, 20},
		{10000, 1 * time.Millisecond, 100, 1, 100, 100, 34},
		{10, 10 * time.Millisecond, 100, 1, 100, 100, 26},
		{100, 10 * time.Millisecond, 100, 1, 100, 100, 74},
		{1000, 10 * time.Millisecond, 100, 1, 100, 100, 122},
		{10000, 10 * time.Millisecond, 100, 1, 100, 100, 170},
		// Moving from very unbalanced to more balanced
		{1, 1 * time.Millisecond, 0, 1, 500, 200, 459},
		{1, 1 * time.Millisecond, 0, 10, 500, 200, 432},
		{1, 1 * time.Millisecond, 0, 100, 500, 200, 404},
		{1, 10 * time.Millisecond, 0, 1, 500, 200, 459},
		{1, 10 * time.Millisecond, 0, 10, 500, 200, 364},
		{1, 10 * time.Millisecond, 0, 100, 500, 200, 268},
		{1, 50 * time.Millisecond, 0, 1, 500, 200, 459},
		{1, 50 * time.Millisecond, 0, 10, 500, 200, 302},
		{1, 50 * time.Millisecond, 0, 100, 500, 200, 144},
		{1, 1 * time.Millisecond, 50, 1, 500, 250, 400},
		{1, 1 * time.Millisecond, 50, 10, 500, 250, 364},
		{1, 1 * time.Millisecond, 50, 100, 500, 250, 330},
		{1, 10 * time.Millisecond, 50, 1, 500, 250, 400},
		{1, 10 * time.Millisecond, 50, 10, 500, 250, 280},
		{1, 10 * time.Millisecond, 50, 100, 500, 250, 160},
		{1, 50 * time.Millisecond, 50, 1, 500, 250, 400},
		{1, 50 * time.Millisecond, 50, 10, 500, 250, 202},
		{1, 50 * time.Millisecond, 50, 100, 500, 250, 6},
		// Miscellaneous cases with uneven balance
		{10, 1 * time.Millisecond, 100, 1, 50, 67, -56},
		{1, 1 * time.Millisecond, 50, 10, 100, 67, 26},
		{10, 10 * time.Millisecond, 100, 1, 50, 67, -32},
		{1, 10 * time.Millisecond, 50, 10, 100, 67, 4},
		{10, 1 * time.Millisecond, 100, 1, 50, 80, -56},
		{1, 1 * time.Millisecond, 50, 10, 100, 80, 22},
		{10, 10 * time.Millisecond, 100, 1, 50, 80, -28},
		{1, 10 * time.Millisecond, 50, 10, 100, 80, -6},
	}

	for _, c := range testCases {
		remoteStore.Capacity.LeaseCount = c.remoteLeases
		sourceStore.Capacity.LeaseCount = c.sourceLeases
		score := loadBasedLeaseRebalanceScore(
			context.Background(),
			st,
			c.remoteWeight,
			c.remoteLatency,
			remoteStore,
			c.sourceWeight,
			sourceStore,
			c.meanLeases,
		)
		if c.expected != score {
			t.Errorf("%+v: expected %d, got %d", c, c.expected, score)
		}
	}
}

// TestAllocatorRemoveTarget verifies that the replica chosen by RemoveTarget is
// the one with the lowest capacity.
func TestAllocatorRemoveTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// List of replicas that will be passed to RemoveTarget
	replicas := []roachpb.ReplicaDescriptor{
		{
			StoreID:   1,
			NodeID:    1,
			ReplicaID: 1,
		},
		{
			StoreID:   2,
			NodeID:    2,
			ReplicaID: 2,
		},
		{
			StoreID:   3,
			NodeID:    3,
			ReplicaID: 3,
		},
		{
			StoreID:   4,
			NodeID:    4,
			ReplicaID: 4,
		},
		{
			StoreID:   5,
			NodeID:    5,
			ReplicaID: 5,
		},
	}

	// Setup the stores so that store 3 is the worst candidate and store 2 is
	// the 2nd worst.
	stores := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 100, RangeCount: 10},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 65, RangeCount: 14},
		},
		{
			StoreID:  3,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 60, RangeCount: 15},
		},
		{
			StoreID:  4,
			Node:     roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 65, RangeCount: 10},
		},
		{
			StoreID:  5,
			Node:     roachpb.NodeDescriptor{NodeID: 5},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 65, RangeCount: 13},
		},
	}

	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(ctx)
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	st := a.storePool.st
	EnableStatsBasedRebalancing.Override(&st.SV, false)

	// Repeat this test 10 times, it should always be either store 2 or 3.
	for i := 0; i < 10; i++ {
		targetRepl, _, err := a.RemoveTarget(
			ctx,
			config.ZoneConfig{},
			replicas,
			testRangeInfo(replicas, firstRange),
			false,
		)
		if err != nil {
			t.Fatal(err)
		}
		if a, e1, e2 := targetRepl, replicas[1], replicas[2]; a != e1 && a != e2 {
			t.Fatalf("%d: RemoveTarget did not select either expected replica; expected %v or %v, got %v",
				i, e1, e2, a)
		}
	}
}

func TestAllocatorComputeAction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Each test case should describe a repair situation which has a lower
	// priority than the previous test case.
	testCases := []struct {
		zone           config.ZoneConfig
		desc           roachpb.RangeDescriptor
		expectedAction AllocatorAction
	}{
		// Needs three replicas, have two
		{
			zone: config.ZoneConfig{
				NumReplicas:   3,
				Constraints:   []config.Constraints{{Constraints: []config.Constraint{{Value: "us-east", Type: config.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
				},
			},
			expectedAction: AllocatorAdd,
		},
		// Needs Five replicas, have four.
		{
			zone: config.ZoneConfig{
				NumReplicas:   5,
				Constraints:   []config.Constraints{{Constraints: []config.Constraint{{Value: "us-east", Type: config.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
					},
				},
			},
			expectedAction: AllocatorAdd,
		},
		// Needs Five replicas, have four, one is on a dead store
		{
			zone: config.ZoneConfig{
				NumReplicas:   5,
				Constraints:   []config.Constraints{{Constraints: []config.Constraint{{Value: "us-east", Type: config.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
					},
				},
			},
			expectedAction: AllocatorAdd,
		},
		// Needs three replicas, one is on a dead store.
		{
			zone: config.ZoneConfig{
				NumReplicas:   3,
				Constraints:   []config.Constraints{{Constraints: []config.Constraint{{Value: "us-east", Type: config.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
					},
				},
			},
			expectedAction: AllocatorRemoveDead,
		},
		// Needs three replicas, one is dead.
		{
			zone: config.ZoneConfig{
				NumReplicas:   3,
				Constraints:   []config.Constraints{{Constraints: []config.Constraint{{Value: "us-east", Type: config.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   8,
						NodeID:    8,
						ReplicaID: 8,
					},
				},
			},
			expectedAction: AllocatorRemoveDead,
		},
		// Needs five replicas, one is on a dead store.
		{
			zone: config.ZoneConfig{
				NumReplicas:   5,
				Constraints:   []config.Constraints{{Constraints: []config.Constraint{{Value: "us-east", Type: config.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
					},
				},
			},
			expectedAction: AllocatorRemoveDead,
		},
		// Need three replicas, have four.
		{
			zone: config.ZoneConfig{
				NumReplicas:   3,
				Constraints:   []config.Constraints{{Constraints: []config.Constraint{{Value: "us-east", Type: config.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
					},
				},
			},
			expectedAction: AllocatorRemove,
		},
		// Need three replicas, have five.
		{
			zone: config.ZoneConfig{
				NumReplicas:   3,
				Constraints:   []config.Constraints{{Constraints: []config.Constraint{{Value: "us-east", Type: config.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
					},
					{
						StoreID:   5,
						NodeID:    5,
						ReplicaID: 5,
					},
				},
			},
			expectedAction: AllocatorRemove,
		},
		// Need three replicas, two are on dead stores. Should
		// be a noop because there aren't enough live replicas for
		// a quorum.
		{
			zone: config.ZoneConfig{
				NumReplicas:   3,
				Constraints:   []config.Constraints{{Constraints: []config.Constraint{{Value: "us-east", Type: config.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   7,
						NodeID:    7,
						ReplicaID: 7,
					},
					{
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
					},
				},
			},
			expectedAction: AllocatorNoop,
		},
		// Need three replicas, have three, none of the replicas in the store pool.
		{
			zone: config.ZoneConfig{
				NumReplicas:   3,
				Constraints:   []config.Constraints{{Constraints: []config.Constraint{{Value: "us-east", Type: config.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   10,
						NodeID:    10,
						ReplicaID: 10,
					},
					{
						StoreID:   20,
						NodeID:    20,
						ReplicaID: 20,
					},
					{
						StoreID:   30,
						NodeID:    30,
						ReplicaID: 30,
					},
				},
			},
			expectedAction: AllocatorNoop,
		},
		// Need three replicas, have three.
		{
			zone: config.ZoneConfig{
				NumReplicas:   3,
				Constraints:   []config.Constraints{{Constraints: []config.Constraint{{Value: "us-east", Type: config.Constraint_DEPRECATED_POSITIVE}}}},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
				},
			},
			expectedAction: AllocatorConsiderRebalance,
		},
	}

	stopper, _, sp, a, _ := createTestAllocator( /* deterministic */ false)
	ctx := context.Background()
	defer stopper.Stop(ctx)

	// Set up eight stores. Stores six and seven are marked as dead. Replica eight
	// is dead.
	mockStorePool(sp,
		[]roachpb.StoreID{1, 2, 3, 4, 5, 8},
		[]roachpb.StoreID{6, 7},
		nil,
		nil,
		[]roachpb.ReplicaIdent{{
			RangeID: 0,
			Replica: roachpb.ReplicaDescriptor{
				NodeID:    8,
				StoreID:   8,
				ReplicaID: 8,
			},
		}})

	lastPriority := float64(999999999)
	for i, tcase := range testCases {
		action, priority := a.ComputeAction(ctx, tcase.zone, RangeInfo{Desc: &tcase.desc}, false)
		if tcase.expectedAction != action {
			t.Errorf("Test case %d expected action %q, got action %q",
				i, allocatorActionNames[tcase.expectedAction], allocatorActionNames[action])
			continue
		}
		if tcase.expectedAction != AllocatorNoop && priority > lastPriority {
			t.Errorf("Test cases should have descending priority. Case %d had priority %f, previous case had priority %f", i, priority, lastPriority)
		}
		lastPriority = priority
	}
}

// TestAllocatorComputeActionDisableStatsRebalance is used to verify whether the cluster could balance out
// if we disable stats-based-rebalance.
func TestAllocatorRebalanceTargetDisableStatsRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(ctx)

	st := a.storePool.st
	EnableStatsBasedRebalancing.Override(&st.SV, true)
	// Make sure the cluster's initial status is not balanced.
	// store 1, 2, 3 has 200 replicas, store 4 has only 30 replicas.
	// So obviously, if we want the replica is homogeneous within each store,
	// we should rebalance some replicas from store 1, 2, 3 to store 4.
	// However, we set the WritesPerSecond of store 4 is much bigger than other store,
	// so if we enable stats-based rebalance, we couldn't spread out the replicas on store 1, 2, 3.
	stores := []*roachpb.StoreDescriptor{
		{
			StoreID: 1,
			Node:    roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{
				RangeCount:      200,
				WritesPerSecond: 30,
			},
		},
		{
			StoreID: 2,
			Node:    roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{
				RangeCount:      200,
				WritesPerSecond: 30,
			},
		},
		{
			StoreID: 3,
			Node:    roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{
				RangeCount:      200,
				WritesPerSecond: 30,
			},
		},
		{
			StoreID: 4,
			Node:    roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{
				RangeCount:      30,
				WritesPerSecond: 100,
			},
		},
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	desc := roachpb.RangeDescriptor{
		RangeID: firstRange,
		Replicas: []roachpb.ReplicaDescriptor{
			{
				StoreID:   1,
				NodeID:    1,
				ReplicaID: 1,
			},
			{
				StoreID:   2,
				NodeID:    2,
				ReplicaID: 2,
			},
			{
				StoreID:   3,
				NodeID:    3,
				ReplicaID: 3,
			},
		},
	}
	for i := 0; i < 50; i++ {
		target, _ := a.RebalanceTarget(
			context.Background(),
			config.ZoneConfig{},
			nil,
			testRangeInfo(desc.Replicas, desc.RangeID),
			storeFilterThrottled,
			false, /* disableStatsBasedRebalancing */
		)
		if target != nil {
			t.Errorf("expected no balance, but got %d", target.StoreID)
		}
	}

	for i := 0; i < 50; i++ {
		target, _ := a.RebalanceTarget(
			context.Background(),
			config.ZoneConfig{},
			nil,
			testRangeInfo(desc.Replicas, desc.RangeID),
			storeFilterThrottled,
			true, /* disableStatsBasedRebalancing */
		)
		if expectedStore := roachpb.StoreID(4); target.StoreID != expectedStore {
			t.Errorf("expected balance to %d, but got %d", expectedStore, target.StoreID)
		}
	}
}

func TestAllocatorComputeActionRemoveDead(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Each test case should describe a repair situation which has a lower
	// priority than the previous test case.
	testCases := []struct {
		zone           config.ZoneConfig
		desc           roachpb.RangeDescriptor
		expectedAction AllocatorAction
		live           []roachpb.StoreID
		dead           []roachpb.StoreID
	}{
		// Needs three replicas, one is dead, but there is no replacement.
		{
			zone: config.ZoneConfig{
				NumReplicas: 3,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
				},
			},
			expectedAction: AllocatorConsiderRebalance,
			live:           []roachpb.StoreID{1, 2},
			dead:           []roachpb.StoreID{3},
		},
		// Needs three replicas, one is dead, but there is a replacement.
		{
			zone: config.ZoneConfig{
				NumReplicas: 3,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
				},
			},
			expectedAction: AllocatorRemoveDead,
			live:           []roachpb.StoreID{1, 2, 4},
			dead:           []roachpb.StoreID{3},
		},
	}

	stopper, _, sp, a, _ := createTestAllocator( /* deterministic */ false)
	ctx := context.Background()
	defer stopper.Stop(ctx)

	for i, tcase := range testCases {
		mockStorePool(sp, tcase.live, tcase.dead, nil, nil, nil)

		action, _ := a.ComputeAction(ctx, tcase.zone, RangeInfo{Desc: &tcase.desc}, false)
		if tcase.expectedAction != action {
			t.Errorf("Test case %d expected action %d, got action %d", i, tcase.expectedAction, action)
		}
	}
}

func TestAllocatorComputeActionDecommission(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		zone            config.ZoneConfig
		desc            roachpb.RangeDescriptor
		expectedAction  AllocatorAction
		live            []roachpb.StoreID
		dead            []roachpb.StoreID
		decommissioning []roachpb.StoreID
		decommissioned  []roachpb.StoreID
	}{
		// Has three replicas, but one is in decommissioning status
		{
			zone: config.ZoneConfig{
				NumReplicas: 3,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
				},
			},
			expectedAction:  AllocatorAdd,
			live:            []roachpb.StoreID{1, 2},
			dead:            nil,
			decommissioning: []roachpb.StoreID{3},
		},
		// Has three replicas, one is in decommissioning status, and one is on a
		// dead node.
		{
			zone: config.ZoneConfig{
				NumReplicas: 3,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
				},
			},
			expectedAction:  AllocatorAdd,
			live:            []roachpb.StoreID{1},
			dead:            []roachpb.StoreID{2},
			decommissioning: []roachpb.StoreID{3},
		},
		// Needs three replicas, has four, where one is decommissioning and one is
		// dead.
		{
			zone: config.ZoneConfig{
				NumReplicas: 3,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
					},
				},
			},
			expectedAction:  AllocatorRemoveDead,
			live:            []roachpb.StoreID{1, 4},
			dead:            []roachpb.StoreID{2},
			decommissioning: []roachpb.StoreID{3},
		},
		// Needs three replicas, has four, where one is decommissioning and one is
		// decommissioned.
		{
			zone: config.ZoneConfig{
				NumReplicas: 3,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
					},
				},
			},
			expectedAction:  AllocatorRemoveDead,
			live:            []roachpb.StoreID{1, 4},
			dead:            nil,
			decommissioning: []roachpb.StoreID{3},
			decommissioned:  []roachpb.StoreID{2},
		},
		// Needs three replicas, has three, all decommissioning
		{
			zone: config.ZoneConfig{
				NumReplicas: 3,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
				},
			},
			expectedAction:  AllocatorAdd,
			live:            nil,
			dead:            nil,
			decommissioning: []roachpb.StoreID{1, 2, 3},
		},
		// Needs 3. Has 1 live, 3 decommissioning.
		{
			zone: config.ZoneConfig{
				NumReplicas: 3,
			},
			desc: roachpb.RangeDescriptor{
				Replicas: []roachpb.ReplicaDescriptor{
					{
						StoreID:   1,
						NodeID:    1,
						ReplicaID: 1,
					},
					{
						StoreID:   2,
						NodeID:    2,
						ReplicaID: 2,
					},
					{
						StoreID:   3,
						NodeID:    3,
						ReplicaID: 3,
					},
					{
						StoreID:   4,
						NodeID:    4,
						ReplicaID: 4,
					},
				},
			},
			expectedAction:  AllocatorRemoveDecommissioning,
			live:            []roachpb.StoreID{4},
			dead:            nil,
			decommissioning: []roachpb.StoreID{1, 2, 3},
		},
	}

	stopper, _, sp, a, _ := createTestAllocator( /* deterministic */ false)
	ctx := context.Background()
	defer stopper.Stop(ctx)

	for i, tcase := range testCases {
		mockStorePool(sp, tcase.live, tcase.dead, tcase.decommissioning, tcase.decommissioned, nil)

		action, _ := a.ComputeAction(ctx, tcase.zone, RangeInfo{Desc: &tcase.desc}, false)
		if tcase.expectedAction != action {
			t.Errorf("Test case %d expected action %d, got action %d", i, tcase.expectedAction, action)
			continue
		}
	}
}

// TestAllocatorComputeActionNoStorePool verifies that
// ComputeAction returns AllocatorNoop when storePool is nil.
func TestAllocatorComputeActionNoStorePool(t *testing.T) {
	defer leaktest.AfterTest(t)()

	a := MakeAllocator(nil /* storePool */, nil /* rpcContext */)
	action, priority := a.ComputeAction(context.Background(), config.ZoneConfig{}, RangeInfo{}, false)
	if action != AllocatorNoop {
		t.Errorf("expected AllocatorNoop, but got %v", action)
	}
	if priority != 0 {
		t.Errorf("expected priority 0, but got %f", priority)
	}
}

// TestAllocatorError ensures that the correctly formatted error message is
// returned from an allocatorError.
func TestAllocatorError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	constraint := []config.Constraints{
		{Constraints: []config.Constraint{{Value: "one", Type: config.Constraint_REQUIRED}}},
	}
	constraints := []config.Constraints{
		{
			Constraints: []config.Constraint{
				{Value: "one", Type: config.Constraint_REQUIRED},
				{Value: "two", Type: config.Constraint_REQUIRED},
			},
		},
	}

	testCases := []struct {
		ae       allocatorError
		expected string
	}{
		{allocatorError{nil, 1},
			"0 of 1 store with attributes matching []; likely not enough nodes in cluster"},
		{allocatorError{constraint, 1},
			"0 of 1 store with attributes matching [{0 [+one]}]"},
		{allocatorError{constraint, 2},
			"0 of 2 stores with attributes matching [{0 [+one]}]"},
		{allocatorError{constraints, 1},
			"0 of 1 store with attributes matching [{0 [+one +two]}]"},
		{allocatorError{constraints, 2},
			"0 of 2 stores with attributes matching [{0 [+one +two]}]"},
	}

	for i, testCase := range testCases {
		if actual := testCase.ae.Error(); testCase.expected != actual {
			t.Errorf("%d: actual error message \"%s\" does not match expected \"%s\"", i, actual, testCase.expected)
		}
	}
}

// TestAllocatorThrottled ensures that when a store is throttled, the replica
// will not be sent to purgatory.
func TestAllocatorThrottled(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	ctx := context.Background()
	defer stopper.Stop(ctx)

	// First test to make sure we would send the replica to purgatory.
	_, _, err := a.AllocateTarget(
		ctx,
		simpleZoneConfig,
		[]roachpb.ReplicaDescriptor{},
		firstRangeInfo,
		false,
	)
	if _, ok := err.(purgatoryError); !ok {
		t.Fatalf("expected a purgatory error, got: %v", err)
	}

	// Second, test the normal case in which we can allocate to the store.
	gossiputil.NewStoreGossiper(g).GossipStores(singleStore, t)
	result, _, err := a.AllocateTarget(
		ctx,
		simpleZoneConfig,
		[]roachpb.ReplicaDescriptor{},
		firstRangeInfo,
		false,
	)
	if err != nil {
		t.Fatalf("unable to perform allocation: %v", err)
	}
	if result.Node.NodeID != 1 || result.StoreID != 1 {
		t.Errorf("expected NodeID 1 and StoreID 1: %+v", result)
	}

	// Finally, set that store to be throttled and ensure we don't send the
	// replica to purgatory.
	a.storePool.detailsMu.Lock()
	storeDetail, ok := a.storePool.detailsMu.storeDetails[singleStore[0].StoreID]
	if !ok {
		t.Fatalf("store:%d was not found in the store pool", singleStore[0].StoreID)
	}
	storeDetail.throttledUntil = timeutil.Now().Add(24 * time.Hour)
	a.storePool.detailsMu.Unlock()
	_, _, err = a.AllocateTarget(
		ctx,
		simpleZoneConfig,
		[]roachpb.ReplicaDescriptor{},
		firstRangeInfo,
		false,
	)
	if _, ok := err.(purgatoryError); ok {
		t.Fatalf("expected a non purgatory error, got: %v", err)
	}
}

func TestFilterBehindReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		commit            uint64
		leader            uint64
		progress          []uint64
		brandNewReplicaID roachpb.ReplicaID
		expected          []uint64
	}{
		{0, 99, []uint64{0}, 0, nil},
		{1, 99, []uint64{1}, 0, []uint64{1}},
		{2, 99, []uint64{2}, 0, []uint64{2}},
		{1, 99, []uint64{0, 1}, 0, []uint64{1}},
		{1, 99, []uint64{1, 2}, 0, []uint64{1, 2}},
		{2, 99, []uint64{3, 2}, 0, []uint64{3, 2}},
		{1, 99, []uint64{0, 0, 1}, 0, []uint64{1}},
		{1, 99, []uint64{0, 1, 2}, 0, []uint64{1, 2}},
		{2, 99, []uint64{1, 2, 3}, 0, []uint64{2, 3}},
		{3, 99, []uint64{4, 3, 2}, 0, []uint64{4, 3}},
		{1, 99, []uint64{1, 1, 1}, 0, []uint64{1, 1, 1}},
		{1, 99, []uint64{1, 1, 2}, 0, []uint64{1, 1, 2}},
		{2, 99, []uint64{1, 2, 2}, 0, []uint64{2, 2}},
		{2, 99, []uint64{0, 1, 2, 3}, 0, []uint64{2, 3}},
		{2, 99, []uint64{1, 2, 3, 4}, 0, []uint64{2, 3, 4}},
		{3, 99, []uint64{5, 4, 3, 2}, 0, []uint64{5, 4, 3}},
		{3, 99, []uint64{1, 2, 3, 4, 5}, 0, []uint64{3, 4, 5}},
		{4, 99, []uint64{6, 5, 4, 3, 2}, 0, []uint64{6, 5, 4}},
		{4, 99, []uint64{6, 5, 4, 3, 2}, 0, []uint64{6, 5, 4}},
		{0, 1, []uint64{0}, 0, []uint64{0}},
		{0, 1, []uint64{0, 0, 0}, 0, []uint64{0}},
		{1, 1, []uint64{2, 0, 1}, 0, []uint64{2, 1}},
		{1, 2, []uint64{0, 2, 1}, 0, []uint64{2, 1}},
		{1, 99, []uint64{0, 1}, 1, []uint64{0, 1}},
		{1, 99, []uint64{0, 1}, 2, []uint64{1}},
		{9, 99, []uint64{0, 9}, 1, []uint64{0, 9}},
		{9, 99, []uint64{0, 1}, 1, []uint64{0}},
		{1, 1, []uint64{2, 0, 1}, 2, []uint64{2, 0, 1}},
		{1, 1, []uint64{2, 0, 1}, 3, []uint64{2, 1}},
		{4, 99, []uint64{6, 5, 4, 3, 2}, 5, []uint64{6, 5, 4, 2}},
		{4, 99, []uint64{6, 5, 4, 3, 0}, 5, []uint64{6, 5, 4, 0}},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			status := &raft.Status{
				Progress: make(map[uint64]raft.Progress),
			}
			status.Lead = c.leader
			status.Commit = c.commit
			var replicas []roachpb.ReplicaDescriptor
			for j, v := range c.progress {
				p := raft.Progress{
					Match: v,
					State: raft.ProgressStateReplicate,
				}
				if v == 0 {
					p.State = raft.ProgressStateProbe
				}
				replicaID := uint64(j + 1)
				status.Progress[replicaID] = p
				replicas = append(replicas, roachpb.ReplicaDescriptor{
					ReplicaID: roachpb.ReplicaID(replicaID),
					StoreID:   roachpb.StoreID(v),
				})
			}
			candidates := filterBehindReplicas(status, replicas, c.brandNewReplicaID)
			var ids []uint64
			for _, c := range candidates {
				ids = append(ids, uint64(c.StoreID))
			}
			if !reflect.DeepEqual(c.expected, ids) {
				t.Fatalf("expected %d, but got %d", c.expected, ids)
			}
		})
	}
}

func TestFilterUnremovableReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		commit            uint64
		progress          []uint64
		brandNewReplicaID roachpb.ReplicaID
		expected          []uint64
	}{
		{0, []uint64{0}, 0, nil},
		{1, []uint64{1}, 0, nil},
		{1, []uint64{0, 1}, 0, []uint64{0}},
		{1, []uint64{1, 2, 3}, 0, []uint64{1, 2, 3}},
		{2, []uint64{1, 2, 3}, 0, []uint64{1}},
		{3, []uint64{1, 2, 3}, 0, nil},
		{1, []uint64{1, 2, 3, 4}, 0, []uint64{1, 2, 3, 4}},
		{2, []uint64{1, 2, 3, 4}, 0, []uint64{1, 2, 3, 4}},
		{3, []uint64{1, 2, 3, 4}, 0, []uint64{1, 2}},
		{2, []uint64{1, 2, 3, 4, 5}, 0, []uint64{1, 2, 3, 4, 5}},
		{3, []uint64{1, 2, 3, 4, 5}, 0, []uint64{1, 2}},
		{1, []uint64{1, 0}, 2, []uint64{1, 0}},
		{1, []uint64{1, 0}, 1, []uint64{0}},
		{3, []uint64{3, 2, 1}, 3, []uint64{2}},
		{3, []uint64{3, 2, 0}, 3, []uint64{2}},
		{3, []uint64{4, 3, 2, 1}, 4, []uint64{4, 3, 2, 1}},
		{3, []uint64{4, 3, 2, 0}, 3, []uint64{4, 3, 2, 0}},
		{3, []uint64{4, 3, 2, 0}, 4, []uint64{4, 3, 2, 0}},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			status := &raft.Status{
				Progress: make(map[uint64]raft.Progress),
			}
			// Use an invalid replica ID for the leader. TestFilterBehindReplicas covers
			// valid replica IDs.
			status.Lead = 99
			status.Commit = c.commit
			var replicas []roachpb.ReplicaDescriptor
			for j, v := range c.progress {
				p := raft.Progress{
					Match: v,
					State: raft.ProgressStateReplicate,
				}
				if v == 0 {
					p.State = raft.ProgressStateProbe
				}
				replicaID := uint64(j + 1)
				status.Progress[replicaID] = p
				replicas = append(replicas, roachpb.ReplicaDescriptor{
					ReplicaID: roachpb.ReplicaID(replicaID),
					StoreID:   roachpb.StoreID(v),
				})
			}

			candidates := filterUnremovableReplicas(status, replicas, c.brandNewReplicaID)
			var ids []uint64
			for _, c := range candidates {
				ids = append(ids, uint64(c.StoreID))
			}
			if !reflect.DeepEqual(c.expected, ids) {
				t.Fatalf("expected %d, but got %d", c.expected, ids)
			}
		})
	}
}

// TestAllocatorRebalanceAway verifies that when a replica is on a node with a
// bad zone config, the replica will be rebalanced off of it.
func TestAllocatorRebalanceAway(t *testing.T) {
	defer leaktest.AfterTest(t)()

	localityUS := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "datacenter", Value: "us"}}}
	localityEUR := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "datacenter", Value: "eur"}}}
	capacityEmpty := roachpb.StoreCapacity{Capacity: 100, Available: 99, RangeCount: 1}
	stores := []*roachpb.StoreDescriptor{
		{
			StoreID: 1,
			Node: roachpb.NodeDescriptor{
				NodeID:   1,
				Locality: localityUS,
			},
			Capacity: capacityEmpty,
		},
		{
			StoreID: 2,
			Node: roachpb.NodeDescriptor{
				NodeID:   2,
				Locality: localityUS,
			},
			Capacity: capacityEmpty,
		},
		{
			StoreID: 3,
			Node: roachpb.NodeDescriptor{
				NodeID:   3,
				Locality: localityEUR,
			},
			Capacity: capacityEmpty,
		},
		{
			StoreID: 4,
			Node: roachpb.NodeDescriptor{
				NodeID:   4,
				Locality: localityUS,
			},
			Capacity: capacityEmpty,
		},
		{
			StoreID: 5,
			Node: roachpb.NodeDescriptor{
				NodeID:   5,
				Locality: localityEUR,
			},
			Capacity: capacityEmpty,
		},
	}

	existingReplicas := []roachpb.ReplicaDescriptor{
		{StoreID: stores[0].StoreID},
		{StoreID: stores[1].StoreID},
		{StoreID: stores[2].StoreID},
	}
	testCases := []struct {
		constraint config.Constraint
		expected   *roachpb.StoreID
	}{
		{
			constraint: config.Constraint{Key: "datacenter", Value: "us", Type: config.Constraint_REQUIRED},
			expected:   &stores[3].StoreID,
		},
		{
			constraint: config.Constraint{Key: "datacenter", Value: "eur", Type: config.Constraint_PROHIBITED},
			expected:   &stores[3].StoreID,
		},
		{
			constraint: config.Constraint{Key: "datacenter", Value: "eur", Type: config.Constraint_REQUIRED},
			expected:   &stores[4].StoreID,
		},
		{
			constraint: config.Constraint{Key: "datacenter", Value: "us", Type: config.Constraint_PROHIBITED},
			expected:   &stores[4].StoreID,
		},
		{
			constraint: config.Constraint{Key: "datacenter", Value: "other", Type: config.Constraint_REQUIRED},
			expected:   nil,
		},
		{
			constraint: config.Constraint{Key: "datacenter", Value: "other", Type: config.Constraint_PROHIBITED},
			expected:   nil,
		},
		{
			constraint: config.Constraint{Key: "datacenter", Value: "other", Type: config.Constraint_DEPRECATED_POSITIVE},
			expected:   nil,
		},
		{
			constraint: config.Constraint{Key: "datacenter", Value: "us", Type: config.Constraint_DEPRECATED_POSITIVE},
			expected:   nil,
		},
		{
			constraint: config.Constraint{Key: "datacenter", Value: "eur", Type: config.Constraint_DEPRECATED_POSITIVE},
			expected:   nil,
		},
	}

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.TODO())
	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)
	ctx := context.Background()

	for _, tc := range testCases {
		t.Run(tc.constraint.String(), func(t *testing.T) {
			constraints := config.Constraints{
				Constraints: []config.Constraint{
					tc.constraint,
				},
			}

			actual, _ := a.RebalanceTarget(
				ctx,
				config.ZoneConfig{Constraints: []config.Constraints{constraints}},
				nil,
				testRangeInfo(existingReplicas, firstRange),
				storeFilterThrottled,
				false,
			)

			if tc.expected == nil && actual != nil {
				t.Errorf("rebalancing to the incorrect store, expected nil, got %d", actual.StoreID)
			} else if tc.expected != nil && actual == nil {
				t.Errorf("rebalancing to the incorrect store, expected %d, got nil", *tc.expected)
			} else if !(tc.expected == nil && actual == nil) && *tc.expected != actual.StoreID {
				t.Errorf("rebalancing to the incorrect store, expected %d, got %d", tc.expected, actual.StoreID)
			}
		})
	}
}

type testStore struct {
	roachpb.StoreDescriptor
	immediateCompaction bool
}

func (ts *testStore) add(bytes int64) {
	ts.Capacity.RangeCount++
	ts.Capacity.Available -= bytes
	ts.Capacity.Used += bytes
	ts.Capacity.LogicalBytes += bytes
}

func (ts *testStore) rebalance(ots *testStore, bytes int64) {
	if ts.Capacity.RangeCount == 0 || (ts.Capacity.Capacity-ts.Capacity.Available) < bytes {
		return
	}
	// Mimic a real Store's behavior of rejecting preemptive snapshots when full.
	if !maxCapacityCheck(ots.StoreDescriptor) {
		log.Infof(context.Background(),
			"s%d too full to accept snapshot from s%d: %v", ots.StoreID, ts.StoreID, ots.Capacity)
		return
	}
	log.Infof(context.Background(), "s%d accepting snapshot from s%d", ots.StoreID, ts.StoreID)
	ts.Capacity.RangeCount--
	if ts.immediateCompaction {
		ts.Capacity.Available += bytes
		ts.Capacity.Used -= bytes
	}
	ts.Capacity.LogicalBytes -= bytes
	ots.Capacity.RangeCount++
	ots.Capacity.Available -= bytes
	ots.Capacity.Used += bytes
	ots.Capacity.LogicalBytes += bytes
}

func (ts *testStore) compact() {
	ts.Capacity.Used = ts.Capacity.LogicalBytes
	ts.Capacity.Available = ts.Capacity.Capacity - ts.Capacity.Used
}

func TestAllocatorFullDisks(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	st := cluster.MakeTestingClusterSettings()
	EnableStatsBasedRebalancing.Override(&st.SV, false)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)

	// Model a set of stores in a cluster doing rebalancing, with ranges being
	// randomly added occasionally.
	rpcContext := rpc.NewContext(
		log.AmbientContext{Tracer: st.Tracer},
		&base.Config{Insecure: true},
		clock,
		stopper,
		&st.Version,
	)
	server := rpc.NewServer(rpcContext) // never started
	g := gossip.NewTest(1, rpcContext, server, stopper, metric.NewRegistry())

	TimeUntilStoreDead.Override(&st.SV, TestTimeUntilStoreDeadOff)

	mockNodeLiveness := newMockNodeLiveness(NodeLivenessStatus_LIVE)
	sp := NewStorePool(
		log.AmbientContext{Tracer: st.Tracer},
		st,
		g,
		clock,
		mockNodeLiveness.nodeLivenessFunc,
		false, /* deterministic */
	)
	alloc := MakeAllocator(sp, func(string) (time.Duration, bool) {
		return 0, false
	})

	var wg sync.WaitGroup
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix), func(_ string, _ roachpb.Value) { wg.Done() })

	const generations = 100
	const nodes = 20
	const capacity = (1 << 30) + 1
	const rangeSize = 16 << 20
	rangesPerNode := int(math.Floor(capacity * rebalanceToMaxFractionUsedThreshold / rangeSize))
	rangesToAdd := rangesPerNode * nodes

	// Initialize testStores.
	var testStores [nodes]testStore
	for i := 0; i < len(testStores); i++ {
		// Don't immediately reclaim disk space from removed ranges. This mimics
		// range deletions don't immediately reclaim disk space in rocksdb.
		testStores[i].immediateCompaction = false
		testStores[i].StoreID = roachpb.StoreID(i)
		testStores[i].Node = roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)}
		testStores[i].Capacity = roachpb.StoreCapacity{Capacity: capacity, Available: capacity}
	}
	// Initialize the cluster with a single range.
	testStores[0].add(rangeSize)
	rangesAdded := 1

	for i := 0; i < generations; i++ {
		// First loop through test stores and randomly add data.
		for j := 0; j < len(testStores); j++ {
			if mockNodeLiveness.nodeLivenessFunc(roachpb.NodeID(j), time.Time{}, 0) == NodeLivenessStatus_DEAD {
				continue
			}
			ts := &testStores[j]
			// Add [0,3) ranges to the node, simulating splits and data growth.
			toAdd := alloc.randGen.Intn(3)
			for k := 0; k < toAdd; k++ {
				if rangesAdded < rangesToAdd {
					ts.add(rangeSize)
					rangesAdded++
				}
			}
			if ts.Capacity.Available <= 0 {
				t.Errorf("testStore %d ran out of space during generation %d (rangesAdded=%d/%d): %+v",
					j, i, rangesAdded, rangesToAdd, ts.Capacity)
				mockNodeLiveness.setNodeStatus(roachpb.NodeID(j), NodeLivenessStatus_DEAD)
			}
			wg.Add(1)
			if err := g.AddInfoProto(gossip.MakeStoreKey(roachpb.StoreID(j)), &ts.StoreDescriptor, 0); err != nil {
				t.Fatal(err)
			}
		}
		wg.Wait()

		// Loop through each store a number of times and maybe rebalance.
		for j := 0; j < 10; j++ {
			for k := 0; k < len(testStores); k++ {
				if mockNodeLiveness.nodeLivenessFunc(roachpb.NodeID(k), time.Time{}, 0) == NodeLivenessStatus_DEAD {
					continue
				}
				ts := &testStores[k]
				// Rebalance until there's no more rebalancing to do.
				if ts.Capacity.RangeCount > 0 {
					target, details := alloc.RebalanceTarget(
						ctx,
						config.ZoneConfig{},
						nil,
						testRangeInfo([]roachpb.ReplicaDescriptor{{NodeID: ts.Node.NodeID, StoreID: ts.StoreID}}, firstRange),
						storeFilterThrottled,
						false,
					)
					if target != nil {
						log.Infof(ctx, "rebalancing to %v; details: %s", target, details)
						testStores[k].rebalance(&testStores[int(target.StoreID)], rangeSize)
					}
				}
				// Gossip occasionally, as real Stores do when replicas move around.
				if j%3 == 2 {
					wg.Add(1)
					if err := g.AddInfoProto(gossip.MakeStoreKey(roachpb.StoreID(j)), &ts.StoreDescriptor, 0); err != nil {
						t.Fatal(err)
					}
				}
			}
		}

		// Simulate rocksdb compactions freeing up disk space.
		for j := 0; j < len(testStores); j++ {
			if mockNodeLiveness.nodeLivenessFunc(roachpb.NodeID(j), time.Time{}, 0) != NodeLivenessStatus_DEAD {
				ts := &testStores[j]
				if ts.Capacity.Available <= 0 {
					t.Errorf("testStore %d ran out of space during generation %d: %+v", j, i, ts.Capacity)
					mockNodeLiveness.setNodeStatus(roachpb.NodeID(j), NodeLivenessStatus_DEAD)
				} else {
					ts.compact()
				}
			}
		}
	}
}

func Example_rebalancing() {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	st := cluster.MakeTestingClusterSettings()
	EnableStatsBasedRebalancing.Override(&st.SV, false)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)

	// Model a set of stores in a cluster,
	// adding / rebalancing ranges of random sizes.
	rpcContext := rpc.NewContext(
		log.AmbientContext{Tracer: st.Tracer},
		&base.Config{Insecure: true},
		clock,
		stopper,
		&st.Version,
	)
	server := rpc.NewServer(rpcContext) // never started
	g := gossip.NewTest(1, rpcContext, server, stopper, metric.NewRegistry())

	TimeUntilStoreDead.Override(&st.SV, TestTimeUntilStoreDeadOff)
	// Deterministic must be set as this test is comparing the exact output
	// after each rebalance.
	sp := NewStorePool(
		log.AmbientContext{Tracer: st.Tracer},
		st,
		g,
		clock,
		newMockNodeLiveness(NodeLivenessStatus_LIVE).nodeLivenessFunc,
		/* deterministic */ true,
	)
	alloc := MakeAllocator(sp, func(string) (time.Duration, bool) {
		return 0, false
	})

	var wg sync.WaitGroup
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix), func(_ string, _ roachpb.Value) { wg.Done() })

	const generations = 100
	const nodes = 20
	const printGenerations = generations / 2

	// Initialize testStores.
	var testStores [nodes]testStore
	for i := 0; i < len(testStores); i++ {
		testStores[i].immediateCompaction = true
		testStores[i].StoreID = roachpb.StoreID(i)
		testStores[i].Node = roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)}
		testStores[i].Capacity = roachpb.StoreCapacity{Capacity: 1 << 30, Available: 1 << 30}
	}
	// Initialize the cluster with a single range.
	testStores[0].add(alloc.randGen.Int63n(1 << 20))

	table := tablewriter.NewWriter(os.Stdout)
	table.SetAutoFormatHeaders(false)
	table.SetAlignment(tablewriter.ALIGN_RIGHT)

	header := make([]string, len(testStores)+1)
	header[0] = "gen"
	for i := 0; i < len(testStores); i++ {
		header[i+1] = fmt.Sprintf("store %d", i)
	}
	table.SetHeader(header)

	for i := 0; i < generations; i++ {
		// First loop through test stores and add data.
		wg.Add(len(testStores))
		for j := 0; j < len(testStores); j++ {
			// Add a pretend range to the testStore if there's already one.
			if testStores[j].Capacity.RangeCount > 0 {
				testStores[j].add(alloc.randGen.Int63n(1 << 20))
			}
			if err := g.AddInfoProto(gossip.MakeStoreKey(roachpb.StoreID(j)), &testStores[j].StoreDescriptor, 0); err != nil {
				panic(err)
			}
		}
		wg.Wait()

		// Next loop through test stores and maybe rebalance.
		for j := 0; j < len(testStores); j++ {
			ts := &testStores[j]
			target, details := alloc.RebalanceTarget(
				context.Background(),
				config.ZoneConfig{},
				nil,
				testRangeInfo([]roachpb.ReplicaDescriptor{{NodeID: ts.Node.NodeID, StoreID: ts.StoreID}}, firstRange),
				storeFilterThrottled,
				false,
			)
			if target != nil {
				log.Infof(context.TODO(), "rebalancing to %v; details: %s", target, details)
				testStores[j].rebalance(&testStores[int(target.StoreID)], alloc.randGen.Int63n(1<<20))
			}
		}

		if i%(generations/printGenerations) == 0 {
			var totalBytes int64
			for j := 0; j < len(testStores); j++ {
				totalBytes += testStores[j].Capacity.Capacity - testStores[j].Capacity.Available
			}
			row := make([]string, len(testStores)+1)
			row[0] = fmt.Sprintf("%d", i)
			for j := 0; j < len(testStores); j++ {
				ts := testStores[j]
				bytes := ts.Capacity.Capacity - ts.Capacity.Available
				row[j+1] = fmt.Sprintf("%3d %3d%%", ts.Capacity.RangeCount, (100*bytes)/totalBytes)
			}
			table.Append(row)
		}
	}

	var totBytes int64
	var totRanges int32
	for i := 0; i < len(testStores); i++ {
		totBytes += testStores[i].Capacity.Capacity - testStores[i].Capacity.Available
		totRanges += testStores[i].Capacity.RangeCount
	}
	table.Render()
	fmt.Printf("Total bytes=%d, ranges=%d\n", totBytes, totRanges)

	// Output:
	// +-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
	// | gen | store 0  | store 1  | store 2  | store 3  | store 4  | store 5  | store 6  | store 7  | store 8  | store 9  | store 10 | store 11 | store 12 | store 13 | store 14 | store 15 | store 16 | store 17 | store 18 | store 19 |
	// +-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
	// |   0 |   2 100% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   2 |   4 100% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   4 |   6 100% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   6 |   8 100% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   8 |  10 100% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |  10 |  10  68% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   1   2% |   0   0% |   0   0% |   1  11% |   0   0% |   1  18% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |  12 |  10  21% |   1  10% |   0   0% |   1   1% |   1   3% |   1   5% |   2   7% |   1   9% |   1   7% |   0   0% |   0   0% |   1   7% |   1   5% |   1  10% |   0   0% |   1   2% |   1   4% |   1   4% |   0   0% |   1   0% |
	// |  14 |  10   8% |   2   5% |   3   5% |   2   1% |   2   3% |   2   4% |   2   4% |   2   5% |   2   7% |   2   5% |   2   4% |   2   7% |   2   7% |   2   5% |   8  10% |   2   0% |   2   2% |   2   4% |   3   4% |   2   0% |
	// |  16 |  10   5% |   4   4% |   5   4% |   4   1% |   5   6% |   4   5% |   4   4% |   4   4% |   5   7% |   4   4% |   4   4% |   5   9% |   4   5% |   4   5% |   8   4% |   4   2% |   4   4% |   4   5% |   5   3% |   5   5% |
	// |  18 |  10   2% |   7   6% |   7   4% |   6   2% |   7   5% |   6   5% |   6   4% |   7   6% |   7   6% |   7   5% |   6   5% |   7   7% |   6   4% |   6   5% |   8   3% |   6   2% |   7   5% |   6   5% |   7   3% |   7   6% |
	// |  20 |  10   0% |   9   5% |   9   5% |   8   3% |   9   5% |   9   6% |   8   4% |   9   6% |   9   6% |   9   6% |   8   4% |   9   7% |   9   5% |   8   5% |  10   2% |   8   3% |   9   4% |   8   5% |   9   3% |   9   6% |
	// |  22 |  12   1% |  11   4% |  11   5% |  10   3% |  11   5% |  11   5% |  10   4% |  11   6% |  11   6% |  11   6% |  10   4% |  11   6% |  11   6% |  10   5% |  12   2% |  10   3% |  11   4% |  10   5% |  11   4% |  11   6% |
	// |  24 |  14   1% |  13   4% |  13   6% |  12   3% |  13   5% |  13   5% |  12   3% |  13   6% |  13   6% |  13   6% |  12   4% |  13   6% |  13   6% |  12   5% |  14   3% |  12   3% |  13   4% |  12   5% |  13   3% |  13   6% |
	// |  26 |  16   2% |  15   4% |  15   6% |  14   3% |  15   4% |  15   5% |  14   3% |  15   6% |  15   5% |  15   7% |  14   4% |  15   6% |  15   6% |  14   4% |  16   4% |  14   3% |  15   4% |  14   5% |  15   4% |  15   5% |
	// |  28 |  18   2% |  17   4% |  17   6% |  16   3% |  17   5% |  17   5% |  16   3% |  17   5% |  17   5% |  17   7% |  16   4% |  17   5% |  17   6% |  16   4% |  18   4% |  16   3% |  17   5% |  16   5% |  17   4% |  17   5% |
	// |  30 |  20   2% |  19   4% |  19   5% |  18   3% |  19   5% |  19   5% |  18   3% |  19   5% |  19   5% |  19   6% |  18   4% |  19   5% |  19   6% |  18   4% |  20   4% |  18   3% |  19   4% |  18   5% |  19   4% |  19   5% |
	// |  32 |  22   2% |  21   5% |  21   5% |  20   3% |  21   5% |  21   5% |  20   3% |  21   5% |  21   5% |  21   6% |  20   4% |  21   6% |  21   6% |  20   4% |  22   4% |  20   3% |  21   4% |  20   5% |  21   4% |  21   5% |
	// |  34 |  24   3% |  23   5% |  23   5% |  22   3% |  23   5% |  23   5% |  22   3% |  23   5% |  23   5% |  23   5% |  22   4% |  23   6% |  23   6% |  22   4% |  24   4% |  22   3% |  23   4% |  22   5% |  23   4% |  23   6% |
	// |  36 |  26   3% |  25   5% |  25   5% |  24   3% |  25   5% |  25   5% |  24   3% |  25   5% |  25   5% |  25   5% |  24   4% |  25   6% |  25   6% |  24   4% |  26   4% |  24   4% |  25   4% |  24   5% |  25   4% |  25   6% |
	// |  38 |  28   3% |  27   5% |  27   4% |  26   4% |  27   5% |  27   5% |  26   3% |  27   4% |  27   5% |  27   5% |  26   4% |  27   6% |  27   6% |  26   4% |  28   3% |  26   4% |  27   5% |  26   5% |  27   4% |  27   6% |
	// |  40 |  30   3% |  29   5% |  29   4% |  28   4% |  29   5% |  29   6% |  28   4% |  29   4% |  29   5% |  29   5% |  28   4% |  29   6% |  29   6% |  28   4% |  30   3% |  28   4% |  29   5% |  28   5% |  29   4% |  29   6% |
	// |  42 |  32   3% |  31   5% |  31   4% |  30   4% |  31   5% |  31   5% |  30   4% |  31   4% |  31   4% |  31   5% |  30   4% |  31   5% |  31   5% |  30   4% |  32   3% |  30   4% |  31   4% |  30   5% |  31   4% |  31   6% |
	// |  44 |  34   4% |  33   5% |  33   4% |  32   4% |  33   5% |  33   6% |  32   4% |  33   4% |  33   4% |  33   5% |  32   4% |  33   5% |  33   6% |  32   4% |  34   3% |  32   4% |  33   4% |  32   5% |  33   4% |  33   5% |
	// |  46 |  36   4% |  35   5% |  35   4% |  34   4% |  35   5% |  35   6% |  34   4% |  35   4% |  35   4% |  35   5% |  34   4% |  35   5% |  35   5% |  34   4% |  36   4% |  34   4% |  35   4% |  34   5% |  35   4% |  35   5% |
	// |  48 |  38   4% |  37   5% |  37   5% |  36   4% |  37   5% |  37   5% |  36   4% |  37   5% |  37   4% |  37   5% |  36   4% |  37   5% |  37   5% |  36   4% |  38   4% |  36   4% |  37   5% |  36   5% |  37   4% |  37   5% |
	// |  50 |  40   4% |  39   5% |  39   5% |  38   4% |  39   5% |  39   5% |  38   4% |  39   5% |  39   4% |  39   5% |  38   4% |  39   5% |  39   5% |  38   4% |  40   4% |  38   4% |  39   5% |  38   5% |  39   4% |  39   5% |
	// |  52 |  42   4% |  41   5% |  41   5% |  40   4% |  41   5% |  41   5% |  40   4% |  41   5% |  41   4% |  41   5% |  40   4% |  41   5% |  41   5% |  40   4% |  42   4% |  40   4% |  41   5% |  40   5% |  41   4% |  41   5% |
	// |  54 |  44   4% |  43   5% |  43   4% |  42   5% |  43   4% |  43   5% |  42   4% |  43   5% |  43   4% |  43   5% |  42   4% |  43   5% |  43   5% |  42   4% |  44   4% |  42   4% |  43   5% |  42   5% |  43   4% |  43   5% |
	// |  56 |  46   4% |  45   5% |  45   4% |  44   4% |  45   4% |  45   5% |  44   4% |  45   5% |  45   4% |  45   5% |  44   4% |  45   5% |  45   5% |  44   4% |  46   4% |  44   4% |  45   5% |  44   5% |  45   4% |  45   5% |
	// |  58 |  48   4% |  47   5% |  47   5% |  46   4% |  47   4% |  47   5% |  46   4% |  47   5% |  47   4% |  47   5% |  46   4% |  47   5% |  47   5% |  46   4% |  48   4% |  46   4% |  47   5% |  46   5% |  47   4% |  47   5% |
	// |  60 |  50   4% |  49   5% |  49   4% |  48   4% |  49   4% |  49   5% |  48   4% |  49   5% |  49   4% |  49   5% |  48   4% |  49   5% |  49   5% |  48   4% |  50   4% |  48   4% |  49   5% |  48   5% |  49   4% |  49   5% |
	// |  62 |  52   4% |  51   5% |  51   4% |  50   4% |  51   4% |  51   5% |  50   4% |  51   5% |  51   4% |  51   5% |  50   4% |  51   5% |  51   5% |  50   4% |  52   4% |  50   4% |  51   5% |  50   5% |  51   4% |  51   5% |
	// |  64 |  54   4% |  53   5% |  53   4% |  52   4% |  53   4% |  53   5% |  52   4% |  53   5% |  53   4% |  53   5% |  52   4% |  53   5% |  53   5% |  52   4% |  54   4% |  52   4% |  53   5% |  52   5% |  53   4% |  53   5% |
	// |  66 |  56   4% |  55   5% |  55   4% |  54   4% |  55   4% |  55   5% |  54   4% |  55   5% |  55   4% |  55   5% |  54   4% |  55   5% |  55   5% |  54   4% |  56   4% |  54   4% |  55   4% |  54   5% |  55   4% |  55   5% |
	// |  68 |  58   4% |  57   5% |  57   4% |  56   5% |  57   4% |  57   5% |  56   4% |  57   5% |  57   4% |  57   5% |  56   4% |  57   5% |  57   5% |  56   4% |  58   4% |  56   4% |  57   4% |  56   5% |  57   4% |  57   5% |
	// |  70 |  60   4% |  59   5% |  59   4% |  58   4% |  59   4% |  59   5% |  58   4% |  59   5% |  59   4% |  59   5% |  58   4% |  59   5% |  59   5% |  58   4% |  60   4% |  58   4% |  59   4% |  58   5% |  59   4% |  59   5% |
	// |  72 |  62   4% |  61   5% |  61   4% |  60   4% |  61   4% |  61   5% |  60   4% |  61   5% |  61   4% |  61   5% |  60   4% |  61   5% |  61   5% |  60   4% |  62   4% |  60   4% |  61   4% |  60   5% |  61   4% |  61   5% |
	// |  74 |  64   4% |  63   5% |  63   4% |  62   4% |  63   4% |  63   5% |  62   4% |  63   5% |  63   4% |  63   5% |  62   4% |  63   5% |  63   5% |  62   4% |  64   4% |  62   4% |  63   4% |  62   5% |  63   4% |  63   5% |
	// |  76 |  66   4% |  65   5% |  65   4% |  64   4% |  65   4% |  65   5% |  64   4% |  65   5% |  65   4% |  65   5% |  64   4% |  65   5% |  65   5% |  64   4% |  66   4% |  64   5% |  65   4% |  64   5% |  65   4% |  65   5% |
	// |  78 |  68   4% |  67   5% |  67   4% |  66   4% |  67   5% |  67   5% |  66   4% |  67   5% |  67   4% |  67   5% |  66   4% |  67   5% |  67   5% |  66   4% |  68   4% |  66   5% |  67   4% |  66   5% |  67   4% |  67   5% |
	// |  80 |  70   4% |  69   5% |  69   4% |  68   4% |  69   5% |  69   5% |  68   4% |  69   4% |  69   4% |  69   5% |  68   4% |  69   5% |  69   5% |  68   4% |  70   4% |  68   5% |  69   4% |  68   5% |  69   4% |  69   5% |
	// |  82 |  72   4% |  71   4% |  71   4% |  70   4% |  71   5% |  71   5% |  70   4% |  71   4% |  71   4% |  71   5% |  70   4% |  71   5% |  71   5% |  70   4% |  72   4% |  70   5% |  71   4% |  70   5% |  71   4% |  71   5% |
	// |  84 |  74   4% |  73   4% |  73   4% |  72   4% |  73   5% |  73   5% |  72   4% |  73   4% |  73   4% |  73   5% |  72   5% |  73   5% |  73   5% |  72   4% |  74   4% |  72   5% |  73   4% |  72   5% |  73   4% |  73   5% |
	// |  86 |  76   4% |  75   5% |  75   4% |  74   4% |  75   5% |  75   5% |  74   4% |  75   4% |  75   4% |  75   5% |  74   5% |  75   5% |  75   5% |  74   4% |  76   4% |  74   5% |  75   4% |  74   5% |  75   4% |  75   5% |
	// |  88 |  78   4% |  77   5% |  77   4% |  76   4% |  77   5% |  77   5% |  76   4% |  77   4% |  77   4% |  77   5% |  76   5% |  77   5% |  77   5% |  76   4% |  78   4% |  76   5% |  77   4% |  76   5% |  77   4% |  77   5% |
	// |  90 |  80   4% |  79   5% |  79   4% |  78   4% |  79   4% |  79   5% |  78   4% |  79   4% |  79   4% |  79   5% |  78   5% |  79   5% |  79   5% |  78   4% |  80   4% |  78   5% |  79   4% |  78   5% |  79   4% |  79   5% |
	// |  92 |  82   4% |  81   5% |  81   4% |  80   4% |  81   4% |  81   5% |  80   4% |  81   4% |  81   4% |  81   5% |  80   5% |  81   5% |  81   5% |  80   4% |  82   4% |  80   5% |  81   4% |  80   5% |  81   4% |  81   5% |
	// |  94 |  84   4% |  83   4% |  83   4% |  82   4% |  83   4% |  83   5% |  82   4% |  83   4% |  83   4% |  83   5% |  82   5% |  83   5% |  83   5% |  82   4% |  84   4% |  82   5% |  83   4% |  82   5% |  83   4% |  83   5% |
	// |  96 |  86   4% |  85   4% |  85   4% |  84   4% |  85   4% |  85   5% |  84   4% |  85   4% |  85   4% |  85   5% |  84   5% |  85   5% |  85   5% |  84   4% |  86   4% |  84   5% |  85   4% |  84   5% |  85   4% |  85   5% |
	// |  98 |  88   4% |  87   4% |  87   4% |  86   4% |  87   4% |  87   5% |  86   4% |  87   4% |  87   4% |  87   5% |  86   5% |  87   5% |  87   5% |  86   4% |  88   4% |  86   5% |  87   4% |  86   5% |  87   4% |  87   5% |
	// +-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
	// Total bytes=915941810, ranges=1756
}
