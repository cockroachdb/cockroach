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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Kathy Spradlin (kathyspradlin@gmail.com)
// Author: Levon Lloyd (levon.lloyd@gmail.com)

package storage

import (
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const firstRange = roachpb.RangeID(1)
const noStore = roachpb.StoreID(-1)

var simpleZoneConfig = config.ZoneConfig{
	NumReplicas: 1,
	Constraints: config.Constraints{
		Constraints: []config.Constraint{
			{Value: "a"},
			{Value: "ssd"},
		},
	},
}

var multiDCConfig = config.ZoneConfig{
	NumReplicas: 2,
	Constraints: config.Constraints{Constraints: []config.Constraint{{Value: "ssd"}}},
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
			Capacity:  100,
			Available: 200,
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
			Capacity:  100,
			Available: 200,
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
			Capacity:  100,
			Available: 200,
		},
	},
	{
		StoreID: 3,
		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 2,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:  100,
			Available: 200,
		},
	},
	{
		StoreID: 4,
		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 3,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:  100,
			Available: 200,
		},
	},
	{
		StoreID: 5,
		Attrs:   roachpb.Attributes{Attrs: []string{"mem"}},
		Node: roachpb.NodeDescriptor{
			NodeID: 4,
			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
		},
		Capacity: roachpb.StoreCapacity{
			Capacity:  100,
			Available: 200,
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
			Capacity:  100,
			Available: 200,
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
			Capacity:  100,
			Available: 200,
		},
	},
}

// createTestAllocator creates a stopper, gossip, store pool and allocator for
// use in tests. Stopper must be stopped by the caller.
func createTestAllocator(
	deterministic bool,
) (*stop.Stopper, *gossip.Gossip, *StorePool, Allocator, *hlc.ManualClock) {
	stopper, g, manualClock, storePool, _ := createTestStorePool(
		TestTimeUntilStoreDeadOff, deterministic, true /* defaultNodeLiveness */)
	a := MakeAllocator(storePool)
	return stopper, g, storePool, a, manualClock
}

// mockStorePool sets up a collection of a alive and dead stores in the store
// pool for testing purposes. It also adds dead replicas to the stores and
// ranges in deadReplicas.
func mockStorePool(
	storePool *StorePool,
	aliveStoreIDs, deadStoreIDs []roachpb.StoreID,
	deadReplicas []roachpb.ReplicaIdent,
) {
	storePool.mu.Lock()
	defer storePool.mu.Unlock()

	liveNodeSet := map[roachpb.NodeID]struct{}{}
	storePool.mu.storeDetails = map[roachpb.StoreID]*storeDetail{}
	for _, storeID := range aliveStoreIDs {
		liveNodeSet[roachpb.NodeID(storeID)] = struct{}{}
		detail := storePool.getStoreDetailLocked(storeID)
		detail.desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}
	for _, storeID := range deadStoreIDs {
		detail := storePool.getStoreDetailLocked(storeID)
		detail.desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
	}
	for storeID, detail := range storePool.mu.storeDetails {
		for _, replica := range deadReplicas {
			if storeID != replica.Replica.StoreID {
				continue
			}
			detail.deadReplicas[replica.RangeID] = append(detail.deadReplicas[replica.RangeID], replica.Replica)
		}
	}

	// Set the node liveness function using the set we constructed.
	storePool.nodeLivenessFn =
		func(nodeID roachpb.NodeID, now time.Time, threshold time.Duration) bool {
			_, ok := liveNodeSet[nodeID]
			return ok
		}
}

func TestAllocatorSimpleRetrieval(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(singleStore, t)
	result, err := a.AllocateTarget(
		simpleZoneConfig.Constraints,
		[]roachpb.ReplicaDescriptor{},
		firstRange,
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
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(sameDCStores, t)
	const store1ID = roachpb.StoreID(1)

	// Set store 1 to have a dead replica in the store pool.
	sp.mu.Lock()
	sp.mu.storeDetails[store1ID].deadReplicas[firstRange] =
		[]roachpb.ReplicaDescriptor{{
			NodeID:  roachpb.NodeID(1),
			StoreID: store1ID,
		}}
	sp.mu.Unlock()

	result, err := a.AllocateTarget(
		simpleZoneConfig.Constraints,
		[]roachpb.ReplicaDescriptor{},
		firstRange,
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Node.NodeID != 2 || result.StoreID != 2 {
		t.Errorf("expected NodeID 2 and StoreID 2: %+v", result)
	}
}

func TestAllocatorNoAvailableDisks(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, _, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop()
	result, err := a.AllocateTarget(
		simpleZoneConfig.Constraints,
		[]roachpb.ReplicaDescriptor{},
		firstRange,
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
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(multiDCStores, t)
	result1, err := a.AllocateTarget(
		multiDCConfig.Constraints,
		[]roachpb.ReplicaDescriptor{},
		firstRange,
		false,
	)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	result2, err := a.AllocateTarget(
		multiDCConfig.Constraints,
		[]roachpb.ReplicaDescriptor{{
			NodeID:  result1.Node.NodeID,
			StoreID: result1.StoreID,
		}},
		firstRange,
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
	result3, err := a.AllocateTarget(
		multiDCConfig.Constraints,
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
		firstRange,
		false,
	)
	if err == nil {
		t.Errorf("expected error on allocation without available stores: %+v", result3)
	}
}

func TestAllocatorExistingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(sameDCStores, t)
	result, err := a.AllocateTarget(
		config.Constraints{
			Constraints: []config.Constraint{
				{Value: "a"},
				{Value: "hdd"},
			},
		},
		[]roachpb.ReplicaDescriptor{
			{
				NodeID:  2,
				StoreID: 2,
			},
		},
		firstRange,
		false,
	)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	if result.Node.NodeID != 3 || result.StoreID != 4 {
		t.Errorf("expected result to have node 3 and store 4: %+v", result)
	}
}

// TestAllocatorRelaxConstraints verifies that attribute constraints
// will be relaxed in order to match nodes lacking required attributes,
// if necessary to find an allocation target.
func TestAllocatorRelaxConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(multiDCStores, t)

	testCases := []struct {
		name        string
		constraints []config.Constraint
		existing    []int // existing store/node ID
		expID       int   // expected store/node ID on allocate
		expErr      bool
	}{
		// The two stores in the system have attributes:
		//  storeID=1 {"a", "ssd"}
		//  storeID=2 {"b", "ssd"}
		{
			name: "positive constraints (matching store 1)",
			constraints: []config.Constraint{
				{Value: "a"},
				{Value: "ssd"},
			},
			expID: 1,
		},
		{
			name: "positive constraints (matching store 2)",
			constraints: []config.Constraint{
				{Value: "b"},
				{Value: "ssd"},
			},
			expID: 2,
		},
		{
			name: "positive constraints (matching store 1) with existing replica (store 1)",
			constraints: []config.Constraint{
				{Value: "a"},
				{Value: "ssd"},
			},
			existing: []int{1},
			expID:    2,
		},
		{
			name: "positive constraints (matching store 1) with two existing replicas",
			constraints: []config.Constraint{
				{Value: "a"}, /* remove these?*/
				{Value: "ssd"},
			},
			existing: []int{1, 2},
			expErr:   true,
		},
		{
			name: "positive constraints (matching store 2) with two existing replicas",
			constraints: []config.Constraint{
				{Value: "b"},
				{Value: "ssd"},
			},
			existing: []int{1, 2},
			expErr:   true,
		},
		{
			name: "required constraints (matching store 1) with existing replica (store 1)",
			constraints: []config.Constraint{
				{Value: "a", Type: config.Constraint_REQUIRED},
				{Value: "ssd", Type: config.Constraint_REQUIRED},
			},
			existing: []int{1},
			expErr:   true,
		},
		{
			name: "required constraints (matching store 2) with exiting replica (store 2)",
			constraints: []config.Constraint{
				{Value: "b", Type: config.Constraint_REQUIRED},
				{Value: "ssd", Type: config.Constraint_REQUIRED},
			},
			existing: []int{2},
			expErr:   true,
		},
		{
			name: "positive constraints (matching store 2) with existing replica (store 1)",
			constraints: []config.Constraint{
				{Value: "b"},
				{Value: "ssd"},
			},
			existing: []int{1},
			expID:    2,
		},
		{
			name: "positive constraints (matching store 2) with existing replica (store 2)",
			constraints: []config.Constraint{
				{Value: "b"},
				{Value: "ssd"},
			},
			existing: []int{2},
			expID:    1,
		},
		{
			name: "positive constraints (half matching store 2)",
			constraints: []config.Constraint{
				{Value: "b"},
				{Value: "hdd"},
			},
			expID: 2,
		},
		{
			name: "positive constraints (half matching store 2) with existing replica (store 2)",
			constraints: []config.Constraint{
				{Value: "b"},
				{Value: "hdd"},
			},
			existing: []int{2},
			expID:    1,
		},
		{
			name: "required constraints (half matching store 2) with existing replica (store 2)",
			constraints: []config.Constraint{
				{Value: "b", Type: config.Constraint_REQUIRED},
				{Value: "hdd", Type: config.Constraint_REQUIRED},
			},
			existing: []int{2},
			expErr:   true,
		},
		{
			name: "positive constraints (half matching store 2) with two existing replica",
			constraints: []config.Constraint{
				{Value: "b"},
				{Value: "hdd"},
			},
			existing: []int{1, 2},
			expErr:   true,
		},
		{
			name: "positive constraints (2/3 matching store 2)",
			constraints: []config.Constraint{
				{Value: "b"},
				{Value: "ssd"},
				{Value: "gpu"},
			},
			expID: 2,
		},
		{
			name: "positive constraints (1/3 matching store 2)",
			constraints: []config.Constraint{
				{Value: "b"},
				{Value: "hdd"},
				{Value: "gpu"},
			},
			expID: 2,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			var existing []roachpb.ReplicaDescriptor
			for _, id := range test.existing {
				existing = append(existing, roachpb.ReplicaDescriptor{NodeID: roachpb.NodeID(id), StoreID: roachpb.StoreID(id)})
			}
			result, err := a.AllocateTarget(
				config.Constraints{Constraints: test.constraints},
				existing,
				firstRange,
				false,
			)
			if haveErr := (err != nil); haveErr != test.expErr {
				t.Errorf("expected error %t; got %t: %s", test.expErr, haveErr, err)
			} else if err == nil && roachpb.StoreID(test.expID) != result.StoreID {
				t.Errorf("expected result to have store %d; got %+v", test.expID, result)
			}
		})
	}
}

// TestAllocatorRebalance verifies that rebalance targets are chosen
// randomly from amongst stores over the minAvailCapacityThreshold.
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
	defer stopper.Stop()

	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	// Every rebalance target must be either store 1 or 2.
	for i := 0; i < 10; i++ {
		result, err := a.RebalanceTarget(
			config.Constraints{},
			[]roachpb.ReplicaDescriptor{{StoreID: 3}},
			noStore,
			firstRange,
		)
		if err != nil {
			t.Fatal(err)
		}
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
	oneStoreAboveRebalanceTarget := func(mean int32, numStores int) []testStore {
		stores := make([]testStore, numStores)
		for i := range stores {
			stores[i].rangeCount = mean
		}
		surplus := int32(math.Ceil(float64(mean)*rebalanceThreshold + 1))
		stores[0].rangeCount += surplus
		stores[0].shouldRebalanceFrom = true
		for i := 1; i < len(stores); i++ {
			stores[i].rangeCount -= int32(math.Ceil(float64(surplus) / float64(len(stores)-1)))
		}
		return stores
	}

	// Returns a slice of stores with the specified mean such that the first store
	// has few enough replicas to make it a rebalance target.
	oneUnderusedStore := func(mean int32, numStores int) []testStore {
		stores := make([]testStore, numStores)
		for i := range stores {
			stores[i].rangeCount = mean
		}
		// Subtract enough ranges from the first store to make it a suitable
		// rebalance target. To maintain the specified mean, we then add that delta
		// back to the rest of the replicas.
		deficit := int32(math.Ceil(float64(mean)*rebalanceThreshold + 1))
		stores[0].rangeCount -= deficit
		for i := 1; i < len(stores); i++ {
			stores[i].rangeCount += int32(math.Ceil(float64(deficit) / float64(len(stores)-1)))
			stores[i].shouldRebalanceFrom = true
		}
		return stores
	}

	// Each test case defines the range counts for the test stores and whether we
	// should rebalance from the store.
	testCases := []struct {
		name    string
		cluster []testStore
	}{
		// An evenly balanced cluster should not rebalance.
		{"balanced", []testStore{{5, false}, {5, false}, {5, false}, {5, false}}},
		// Adding an empty node to a 3-node cluster triggers rebalancing from
		// existing nodes.
		{"empty-node", []testStore{{100, true}, {100, true}, {100, true}, {0, false}}},
		// A cluster where all range counts are within RebalanceThreshold should
		// not rebalance. This assumes RebalanceThreshold > 2%.
		{"within-threshold", []testStore{{98, false}, {99, false}, {101, false}, {102, false}}},

		{"5-stores-mean-100-one-above", oneStoreAboveRebalanceTarget(100, 5)},
		{"5-stores-mean-1000-one-above", oneStoreAboveRebalanceTarget(1000, 5)},
		{"5-stores-mean-10000-one-above", oneStoreAboveRebalanceTarget(10000, 5)},

		{"5-stores-mean-1000-one-underused", oneUnderusedStore(1000, 5)},
		{"10-stores-mean-1000-one-underused", oneUnderusedStore(1000, 10)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// It doesn't make sense to test sets of stores containing fewer than 4
			// stores, because 4 stores is the minimum number of stores needed to
			// trigger rebalancing with the default replication factor of 3. Also, the
			// above local functions need a minimum number of stores to properly create
			// the desired distribution of range counts.
			const minStores = 4
			if numStores := len(tc.cluster); numStores < minStores {
				t.Fatalf("numStores %d < min %d", numStores, minStores)
			}
			// Deterministic is required when stressing as test case 8 may rebalance
			// to different configurations.
			stopper, g, _, a, _ := createTestAllocator( /* deterministic */ true)
			defer stopper.Stop()

			// Create stores with the range counts from the test case and gossip them.
			var stores []*roachpb.StoreDescriptor
			for j, store := range tc.cluster {
				stores = append(stores, &roachpb.StoreDescriptor{
					StoreID:  roachpb.StoreID(j + 1),
					Node:     roachpb.NodeDescriptor{NodeID: roachpb.NodeID(j + 1)},
					Capacity: roachpb.StoreCapacity{Capacity: 1, Available: 1, RangeCount: store.rangeCount},
				})
			}
			gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

			// Ensure gossiped store descriptor changes have propagated.
			testutils.SucceedsSoon(t, func() error {
				sl, _, _ := a.storePool.getStoreList(firstRange)
				for j, s := range sl.stores {
					if a, e := s.Capacity.RangeCount, tc.cluster[j].rangeCount; a != e {
						return errors.Errorf("range count for %d = %d != expected %d", j, a, e)
					}
				}
				return nil
			})
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
	defer stopper.Stop()

	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	// Every rebalance target must be store 4 (or nil for case of missing the only option).
	for i := 0; i < 10; i++ {
		result, err := a.RebalanceTarget(
			config.Constraints{},
			[]roachpb.ReplicaDescriptor{{StoreID: stores[0].StoreID}},
			stores[0].StoreID,
			firstRange,
		)
		if err != nil {
			t.Fatal(err)
		}
		if result != nil && result.StoreID != 4 {
			t.Errorf("expected store 4; got %d", result.StoreID)
		}
	}
}

func TestAllocatorTransferLeaseTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ true)
	defer stopper.Stop()

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
		{existing: existing, leaseholder: 1, check: false, expected: 0},
		// Store 2 is not a lease transfer source.
		{existing: existing, leaseholder: 2, check: true, expected: 0},
		{existing: existing, leaseholder: 2, check: false, expected: 1},
		// Store 3 is a lease transfer source.
		{existing: existing, leaseholder: 3, check: true, expected: 1},
		{existing: existing, leaseholder: 3, check: false, expected: 1},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			target := a.TransferLeaseTarget(config.Constraints{},
				c.existing, c.leaseholder, 0, c.check)
			if c.expected != target.StoreID {
				t.Fatalf("expected %d, but found %d", c.expected, target.StoreID)
			}
		})
	}
}

func TestAllocatorTransferLeaseTargetMultiStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ true)
	defer stopper.Stop()

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
		{leaseholder: 1, check: false, expected: 0},
		{leaseholder: 3, check: false, expected: 1},
		{leaseholder: 5, check: false, expected: 1},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			target := a.TransferLeaseTarget(config.Constraints{},
				existing, c.leaseholder, 0, c.check)
			if c.expected != target.StoreID {
				t.Fatalf("expected %d, but found %d", c.expected, target.StoreID)
			}
		})
	}
}

func TestAllocatorShouldTransferLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ true)
	defer stopper.Stop()

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

	replicas := func(storeIDs ...roachpb.StoreID) []roachpb.ReplicaDescriptor {
		var r []roachpb.ReplicaDescriptor
		for _, storeID := range storeIDs {
			r = append(r, roachpb.ReplicaDescriptor{
				StoreID: storeID,
			})
		}
		return r
	}

	testCases := []struct {
		leaseholder roachpb.StoreID
		existing    []roachpb.ReplicaDescriptor
		expected    bool
	}{
		{leaseholder: 1, existing: nil, expected: false},
		{leaseholder: 2, existing: nil, expected: false},
		{leaseholder: 3, existing: nil, expected: false},
		{leaseholder: 3, existing: replicas(1), expected: true},
		{leaseholder: 3, existing: replicas(1, 2), expected: true},
		{leaseholder: 3, existing: replicas(2), expected: false},
		{leaseholder: 3, existing: replicas(3), expected: false},
		{leaseholder: 3, existing: replicas(4), expected: false},
		{leaseholder: 4, existing: nil, expected: true},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			result := a.ShouldTransferLease(config.Constraints{}, c.existing, c.leaseholder, 0)
			if c.expected != result {
				t.Fatalf("expected %v, but found %v", c.expected, result)
			}
		})
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
	}

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop()
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	// Exclude store 2 as a removal target so that only store 3 is a candidate.
	targetRepl, err := a.RemoveTarget(config.Constraints{}, replicas, stores[1].StoreID)
	if err != nil {
		t.Fatal(err)
	}
	if a, e := targetRepl, replicas[2]; a != e {
		t.Fatalf("RemoveTarget did not select expected replica; expected %v, got %v", e, a)
	}

	// Now exclude store 3 so that only store 2 is a candidate.
	targetRepl, err = a.RemoveTarget(config.Constraints{}, replicas, stores[2].StoreID)
	if err != nil {
		t.Fatal(err)
	}
	if a, e := targetRepl, replicas[1]; a != e {
		t.Fatalf("RemoveTarget did not select expected replica; expected %v, got %v", e, a)
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
				Constraints:   config.Constraints{Constraints: []config.Constraint{{Value: "us-east"}}},
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
				Constraints:   config.Constraints{Constraints: []config.Constraint{{Value: "us-east"}}},
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
				Constraints:   config.Constraints{Constraints: []config.Constraint{{Value: "us-east"}}},
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
		// Needs three replicas, two are on dead stores.
		{
			zone: config.ZoneConfig{
				NumReplicas:   3,
				Constraints:   config.Constraints{Constraints: []config.Constraint{{Value: "us-east"}}},
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
			expectedAction: AllocatorRemoveDead,
		},
		// Needs three replicas, one is on a dead store.
		{
			zone: config.ZoneConfig{
				NumReplicas:   3,
				Constraints:   config.Constraints{Constraints: []config.Constraint{{Value: "us-east"}}},
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
				Constraints:   config.Constraints{Constraints: []config.Constraint{{Value: "us-east"}}},
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
				Constraints:   config.Constraints{Constraints: []config.Constraint{{Value: "us-east"}}},
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
				Constraints:   config.Constraints{Constraints: []config.Constraint{{Value: "us-east"}}},
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
				Constraints:   config.Constraints{Constraints: []config.Constraint{{Value: "us-east"}}},
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
		// Three replicas have three, none of the replicas in the store pool.
		{
			zone: config.ZoneConfig{
				NumReplicas:   3,
				Constraints:   config.Constraints{Constraints: []config.Constraint{{Value: "us-east"}}},
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
		// Three replicas have three.
		{
			zone: config.ZoneConfig{
				NumReplicas:   3,
				Constraints:   config.Constraints{Constraints: []config.Constraint{{Value: "us-east"}}},
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
			expectedAction: AllocatorNoop,
		},
	}

	stopper, _, sp, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop()

	// Set up eight stores. Stores six and seven are marked as dead. Replica eight
	// is dead.
	mockStorePool(sp,
		[]roachpb.StoreID{1, 2, 3, 4, 5, 8},
		[]roachpb.StoreID{6, 7},
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
		action, priority := a.ComputeAction(tcase.zone, &tcase.desc)
		if tcase.expectedAction != action {
			t.Errorf("Test case %d expected action %d, got action %d", i, tcase.expectedAction, action)
			continue
		}
		if tcase.expectedAction != AllocatorNoop && priority > lastPriority {
			t.Errorf("Test cases should have descending priority. Case %d had priority %f, previous case had priority %f", i, priority, lastPriority)
		}
		lastPriority = priority
	}
}

// TestAllocatorComputeActionNoStorePool verifies that
// ComputeAction returns AllocatorNoop when storePool is nil.
func TestAllocatorComputeActionNoStorePool(t *testing.T) {
	defer leaktest.AfterTest(t)()

	a := MakeAllocator(nil /* storePool */)
	action, priority := a.ComputeAction(config.ZoneConfig{}, nil)
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

	constraint := []config.Constraint{{Value: "one"}}
	constraints := []config.Constraint{{Value: "one"}, {Value: "two"}}

	testCases := []struct {
		ae       allocatorError
		expected string
	}{
		{allocatorError{nil, false, 1},
			"0 of 1 store with all attributes matching []; likely not enough nodes in cluster"},
		{allocatorError{constraint, false, 1},
			"0 of 1 store with all attributes matching [one]"},
		{allocatorError{constraint, true, 1},
			"0 of 1 store with an attribute matching [one]; likely not enough nodes in cluster"},
		{allocatorError{constraint, false, 2},
			"0 of 2 stores with all attributes matching [one]"},
		{allocatorError{constraint, true, 2},
			"0 of 2 stores with an attribute matching [one]; likely not enough nodes in cluster"},
		{allocatorError{constraints, false, 1},
			"0 of 1 store with all attributes matching [one two]"},
		{allocatorError{constraints, true, 1},
			"0 of 1 store with an attribute matching [one two]; likely not enough nodes in cluster"},
		{allocatorError{constraints, false, 2},
			"0 of 2 stores with all attributes matching [one two]"},
		{allocatorError{constraints, true, 2},
			"0 of 2 stores with an attribute matching [one two]; likely not enough nodes in cluster"},
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
	defer stopper.Stop()

	// First test to make sure we would send the replica to purgatory.
	_, err := a.AllocateTarget(
		simpleZoneConfig.Constraints,
		[]roachpb.ReplicaDescriptor{},
		firstRange,
		false,
	)
	if _, ok := err.(purgatoryError); !ok {
		t.Fatalf("expected a purgatory error, got: %v", err)
	}

	// Second, test the normal case in which we can allocate to the store.
	gossiputil.NewStoreGossiper(g).GossipStores(singleStore, t)
	result, err := a.AllocateTarget(
		simpleZoneConfig.Constraints,
		[]roachpb.ReplicaDescriptor{},
		firstRange,
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
	a.storePool.mu.Lock()
	storeDetail, ok := a.storePool.mu.storeDetails[singleStore[0].StoreID]
	if !ok {
		t.Fatalf("store:%d was not found in the store pool", singleStore[0].StoreID)
	}
	storeDetail.throttledUntil = timeutil.Now().Add(24 * time.Hour)
	a.storePool.mu.Unlock()
	_, err = a.AllocateTarget(
		simpleZoneConfig.Constraints,
		[]roachpb.ReplicaDescriptor{},
		firstRange,
		false,
	)
	if _, ok := err.(purgatoryError); ok {
		t.Fatalf("expected a non purgatory error, got: %v", err)
	}
}

func TestFilterBehindReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		commit   uint64
		progress []uint64
		expected []uint64
	}{
		{0, []uint64{0}, nil},
		{1, []uint64{1}, []uint64{1}},
		{2, []uint64{2}, []uint64{2}},
		{1, []uint64{0, 1}, []uint64{1}},
		{1, []uint64{1, 2}, []uint64{1, 2}},
		{2, []uint64{3, 2}, []uint64{3, 2}},
		{1, []uint64{0, 0, 1}, []uint64{1}},
		{1, []uint64{0, 1, 2}, []uint64{1, 2}},
		{2, []uint64{1, 2, 3}, []uint64{2, 3}},
		{3, []uint64{4, 3, 2}, []uint64{4, 3}},
		{1, []uint64{1, 1, 1}, []uint64{1, 1, 1}},
		{1, []uint64{1, 1, 2}, []uint64{1, 1, 2}},
		{2, []uint64{1, 2, 2}, []uint64{2, 2}},
		{2, []uint64{0, 1, 2, 3}, []uint64{2, 3}},
		{2, []uint64{1, 2, 3, 4}, []uint64{2, 3, 4}},
		{3, []uint64{5, 4, 3, 2}, []uint64{5, 4, 3}},
		{3, []uint64{1, 2, 3, 4, 5}, []uint64{3, 4, 5}},
		{4, []uint64{6, 5, 4, 3, 2}, []uint64{6, 5, 4}},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			status := &raft.Status{
				Progress: make(map[uint64]raft.Progress),
			}
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
				status.Progress[uint64(j)] = p
				replicas = append(replicas, roachpb.ReplicaDescriptor{
					ReplicaID: roachpb.ReplicaID(j),
					StoreID:   roachpb.StoreID(v),
				})
			}
			candidates := filterBehindReplicas(status, replicas)
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

	testCases := []struct {
		constraint       config.Constraint
		existing         []int
		expectedStoreIDs []int
	}{
		{
			constraint:       config.Constraint{Key: "datacenter", Value: "us", Type: config.Constraint_REQUIRED},
			existing:         []int{1, 2, 3},
			expectedStoreIDs: []int{4},
		},
		{
			constraint:       config.Constraint{Key: "datacenter", Value: "eur", Type: config.Constraint_PROHIBITED},
			existing:         []int{1, 2, 3},
			expectedStoreIDs: []int{4},
		},
		{
			constraint:       config.Constraint{Key: "datacenter", Value: "us", Type: config.Constraint_POSITIVE},
			existing:         []int{1, 2, 3},
			expectedStoreIDs: []int{4},
		},
		{
			constraint:       config.Constraint{Key: "datacenter", Value: "eur", Type: config.Constraint_REQUIRED},
			existing:         []int{1, 2, 3},
			expectedStoreIDs: []int{5},
		},
		{
			constraint:       config.Constraint{Key: "datacenter", Value: "us", Type: config.Constraint_PROHIBITED},
			existing:         []int{1, 2, 3},
			expectedStoreIDs: []int{5},
		},
		{
			constraint:       config.Constraint{Key: "datacenter", Value: "eur", Type: config.Constraint_POSITIVE},
			existing:         []int{1, 2, 3},
			expectedStoreIDs: []int{5},
		},
		{
			constraint:       config.Constraint{Key: "datacenter", Value: "other", Type: config.Constraint_REQUIRED},
			existing:         []int{1, 2, 3},
			expectedStoreIDs: []int{},
		},
		{
			constraint:       config.Constraint{Key: "datacenter", Value: "other", Type: config.Constraint_PROHIBITED},
			existing:         []int{1, 2, 3},
			expectedStoreIDs: []int{},
		},
		{
			constraint:       config.Constraint{Key: "datacenter", Value: "other", Type: config.Constraint_POSITIVE},
			existing:         []int{1, 2, 3},
			expectedStoreIDs: []int{},
		},
	}

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	intInSlice := func(v int, list []int) bool {
		for _, l := range list {
			if v == l {
				return true
			}
		}
		return false
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s", tc.constraint), func(t *testing.T) {
			existingReplicas := make([]roachpb.ReplicaDescriptor, len(tc.existing), len(tc.existing))
			for i, storeID := range tc.existing {
				existingReplicas[i] = roachpb.ReplicaDescriptor{StoreID: roachpb.StoreID(storeID)}
			}

			constraints := config.Constraints{
				Constraints: []config.Constraint{
					tc.constraint,
				},
			}

			result, err := a.RebalanceTarget(
				constraints,
				existingReplicas,
				noStore,
				firstRange,
			)
			if err != nil {
				t.Fatal(err)
			}
			if result == nil {
				if len(tc.expectedStoreIDs) != 0 {
					t.Errorf("rebalancing to the incorrect range, expected %v, got nil", tc.expectedStoreIDs)
				}
				return
			}
			if !intInSlice(int(result.StoreID), tc.expectedStoreIDs) {
				t.Errorf("rebalancing to the incorrect range, expected %v, actual %d", tc.expectedStoreIDs, result.StoreID)
			}
		})
	}
}

type testStore struct {
	roachpb.StoreDescriptor
}

func (ts *testStore) add(bytes int64) {
	ts.Capacity.RangeCount++
	ts.Capacity.Available -= bytes
}

func (ts *testStore) rebalance(ots *testStore, bytes int64) {
	if ts.Capacity.RangeCount == 0 || (ts.Capacity.Capacity-ts.Capacity.Available) < bytes {
		return
	}
	ts.Capacity.RangeCount--
	ts.Capacity.Available += bytes
	ots.Capacity.RangeCount++
	ots.Capacity.Available -= bytes
}

func Example_rebalancing() {
	stopper := stop.NewStopper()
	defer stopper.Stop()

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)

	// Model a set of stores in a cluster,
	// randomly adding / removing stores and adding bytes.
	rpcContext := rpc.NewContext(
		log.AmbientContext{},
		&base.Config{Insecure: true},
		clock,
		stopper,
	)
	server := rpc.NewServer(rpcContext) // never started
	g := gossip.NewTest(1, rpcContext, server, nil, stopper, metric.NewRegistry())
	// Deterministic must be set as this test is comparing the exact output
	// after each rebalance.
	sp := NewStorePool(
		log.AmbientContext{},
		g,
		clock,
		newMockNodeLiveness(true /* defaultNodeLiveness */).nodeLivenessFunc,
		TestTimeUntilStoreDeadOff,
		/* deterministic */ true,
	)
	alloc := MakeAllocator(sp)

	var wg sync.WaitGroup
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix), func(_ string, _ roachpb.Value) { wg.Done() })

	const generations = 100
	const nodes = 20
	const printGenerations = generations / 2

	// Initialize testStores.
	var testStores [nodes]testStore
	for i := 0; i < len(testStores); i++ {
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
			target, err := alloc.RebalanceTarget(
				config.Constraints{},
				[]roachpb.ReplicaDescriptor{{NodeID: ts.Node.NodeID, StoreID: ts.StoreID}},
				noStore,
				firstRange,
			)
			if err != nil {
				panic(err)
			}
			if target != nil {
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
	// |   0 |   1  48% |   0   0% |   0   0% |   1  51% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   2 |   2  35% |   0   0% |   0   0% |   2  24% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   1  11% |   0   0% |   1  14% |   0   0% |   1  13% |   0   0% |   0   0% |   0   0% |
	// |   4 |   2  19% |   0   0% |   2  14% |   2   8% |   0   0% |   0   0% |   2  10% |   1   3% |   1   1% |   1   1% |   1   0% |   0   0% |   2   5% |   1   1% |   1   1% |   1   9% |   2  12% |   0   0% |   1  10% |   0   0% |
	// |   6 |   2   8% |   2   3% |   2   6% |   2   4% |   7  12% |   1   2% |   2   6% |   3   7% |   3   2% |   3   3% |   1   1% |   3   6% |   3   3% |   2   0% |   1   4% |   1   2% |   2   3% |   7  10% |   2   9% |   3   0% |
	// |   8 |   7  10% |   5   5% |   6   7% |   5   6% |   7   7% |   6   4% |   3   5% |   3   7% |   4   5% |   4   1% |   5   5% |   3   1% |   3   4% |   3   0% |   4   3% |   5   5% |   4   3% |   7   4% |   4   8% |   4   1% |
	// |  10 |   7   9% |   7   3% |   6   4% |   5   5% |   7   5% |   6   2% |   7   6% |   9  10% |   9   7% |   6   3% |   5   1% |   6   2% |   7   6% |   7   4% |   5   2% |   6   3% |   5   3% |   7   3% |   9   9% |   6   2% |
	// |  12 |   7   7% |   7   1% |   7   3% |  11   7% |   7   3% |  10   4% |   7   4% |   9   8% |   9   6% |   8   3% |  11   4% |  14   8% |  10   9% |   7   3% |   8   4% |   7   2% |   8   4% |   7   1% |   9   7% |   9   3% |
	// |  14 |  11   8% |  13   4% |  13   6% |  11   5% |  10   4% |  10   3% |  10   4% |   9   6% |   9   5% |  10   2% |  11   3% |  14   6% |  10   7% |  10   3% |   9   4% |  11   4% |  10   4% |  13   4% |   9   5% |   9   3% |
	// |  16 |  11   7% |  13   3% |  13   6% |  11   5% |  11   3% |  13   5% |  11   4% |  12   6% |  12   5% |  14   3% |  12   3% |  14   5% |  15   7% |  15   4% |  13   4% |  11   3% |  12   4% |  13   4% |  12   5% |  14   5% |
	// |  18 |  17   9% |  14   3% |  14   5% |  14   5% |  15   4% |  13   4% |  13   4% |  15   7% |  17   6% |  14   3% |  18   4% |  14   3% |  15   6% |  15   3% |  14   3% |  14   4% |  14   4% |  13   3% |  15   5% |  14   4% |
	// |  20 |  17   8% |  18   4% |  15   5% |  15   4% |  16   3% |  17   4% |  19   6% |  15   6% |  17   6% |  19   4% |  18   4% |  17   4% |  16   6% |  15   2% |  15   3% |  19   5% |  16   4% |  18   5% |  15   4% |  15   4% |
	// |  22 |  17   7% |  18   4% |  18   4% |  18   4% |  19   4% |  17   4% |  19   5% |  19   6% |  20   5% |  19   3% |  18   3% |  17   3% |  18   6% |  21   4% |  19   4% |  19   4% |  18   4% |  18   5% |  20   6% |  20   5% |
	// |  24 |  23   8% |  21   5% |  20   4% |  20   4% |  19   4% |  22   4% |  19   4% |  20   6% |  20   4% |  19   2% |  21   4% |  21   3% |  21   6% |  21   3% |  20   4% |  19   4% |  25   5% |  21   5% |  20   5% |  20   5% |
	// |  26 |  23   7% |  22   5% |  24   5% |  21   4% |  21   4% |  22   3% |  24   5% |  24   7% |  24   5% |  24   3% |  21   4% |  21   3% |  22   6% |  21   3% |  22   4% |  24   4% |  25   5% |  21   4% |  24   6% |  22   5% |
	// |  28 |  23   6% |  26   5% |  24   5% |  25   5% |  23   4% |  23   3% |  24   4% |  24   6% |  24   4% |  24   3% |  24   4% |  24   4% |  27   6% |  26   4% |  25   4% |  24   4% |  25   4% |  25   4% |  24   5% |  28   6% |
	// |  30 |  28   7% |  26   4% |  27   5% |  25   4% |  29   4% |  29   4% |  26   4% |  25   6% |  30   5% |  25   3% |  26   4% |  27   4% |  27   5% |  26   3% |  25   4% |  25   4% |  25   4% |  25   4% |  28   5% |  28   5% |
	// |  32 |  28   6% |  27   4% |  27   4% |  32   6% |  29   4% |  29   3% |  27   4% |  28   6% |  30   5% |  29   4% |  28   4% |  29   4% |  27   5% |  31   4% |  31   5% |  28   4% |  28   4% |  28   4% |  28   5% |  28   5% |
	// |  34 |  35   7% |  30   4% |  31   5% |  32   5% |  29   3% |  30   3% |  31   5% |  29   5% |  30   5% |  29   3% |  30   5% |  29   4% |  30   5% |  31   4% |  31   4% |  31   5% |  34   5% |  29   3% |  31   5% |  30   5% |
	// |  36 |  35   7% |  31   4% |  31   5% |  32   5% |  32   4% |  31   3% |  31   4% |  34   6% |  34   5% |  33   4% |  31   4% |  34   4% |  32   5% |  33   4% |  31   4% |  31   4% |  34   4% |  34   4% |  31   5% |  37   6% |
	// |  38 |  35   6% |  34   4% |  34   5% |  36   5% |  35   4% |  34   3% |  37   5% |  34   5% |  34   5% |  33   4% |  34   5% |  34   4% |  37   5% |  34   4% |  35   3% |  34   4% |  34   4% |  34   3% |  33   5% |  37   5% |
	// |  40 |  35   6% |  35   4% |  35   5% |  36   5% |  35   4% |  39   4% |  37   4% |  35   5% |  36   5% |  40   4% |  40   6% |  38   5% |  37   5% |  37   4% |  35   3% |  35   4% |  35   4% |  35   3% |  40   5% |  37   5% |
	// |  42 |  37   6% |  38   4% |  38   5% |  41   6% |  38   4% |  39   4% |  38   4% |  41   6% |  40   5% |  40   4% |  40   5% |  38   5% |  38   5% |  37   4% |  39   3% |  38   4% |  38   4% |  37   3% |  40   5% |  37   5% |
	// |  44 |  43   6% |  40   4% |  40   5% |  41   6% |  39   4% |  39   4% |  44   5% |  41   5% |  40   5% |  40   4% |  40   5% |  39   4% |  39   5% |  42   4% |  39   3% |  40   5% |  46   5% |  40   4% |  40   5% |  40   4% |
	// |  46 |  43   5% |  43   4% |  45   6% |  41   5% |  43   4% |  44   4% |  44   4% |  41   5% |  42   5% |  42   4% |  42   5% |  42   4% |  42   5% |  42   4% |  42   3% |  44   5% |  46   5% |  41   3% |  41   5% |  42   4% |
	// |  48 |  43   5% |  44   4% |  45   5% |  44   6% |  44   4% |  44   4% |  44   4% |  46   5% |  44   5% |  45   4% |  44   5% |  45   4% |  44   5% |  44   4% |  46   4% |  44   5% |  46   5% |  44   4% |  48   5% |  44   4% |
	// |  50 |  58   6% |  45   4% |  45   5% |  45   5% |  46   4% |  46   4% |  45   4% |  46   5% |  47   5% |  45   3% |  49   5% |  45   4% |  45   5% |  47   4% |  46   3% |  47   5% |  46   5% |  45   4% |  48   5% |  46   4% |
	// |  52 |  58   6% |  48   4% |  49   5% |  47   5% |  49   4% |  48   4% |  47   4% |  48   5% |  47   5% |  47   3% |  49   5% |  50   5% |  51   5% |  47   4% |  50   4% |  47   5% |  47   4% |  48   4% |  48   4% |  47   4% |
	// |  54 |  58   5% |  49   4% |  49   5% |  51   5% |  50   4% |  50   4% |  49   4% |  52   5% |  51   5% |  50   3% |  49   5% |  50   4% |  51   5% |  55   4% |  50   4% |  49   5% |  50   4% |  50   4% |  50   4% |  49   4% |
	// |  56 |  58   5% |  56   5% |  51   5% |  51   5% |  54   4% |  53   4% |  52   5% |  52   5% |  51   5% |  51   4% |  52   5% |  52   4% |  52   5% |  55   4% |  52   4% |  51   5% |  53   4% |  52   4% |  52   4% |  52   5% |
	// |  58 |  58   5% |  56   4% |  54   5% |  57   5% |  54   4% |  53   4% |  54   5% |  53   5% |  54   5% |  54   3% |  53   5% |  53   4% |  55   5% |  55   4% |  57   4% |  56   6% |  53   4% |  53   4% |  55   4% |  55   5% |
	// |  60 |  58   5% |  56   4% |  57   5% |  57   5% |  56   4% |  56   4% |  57   5% |  55   5% |  59   5% |  59   4% |  56   5% |  55   4% |  56   5% |  56   4% |  57   4% |  56   6% |  56   4% |  60   4% |  55   4% |  55   4% |
	// |  62 |  58   4% |  59   4% |  57   5% |  57   5% |  59   4% |  59   4% |  57   5% |  58   5% |  59   5% |  59   4% |  57   5% |  60   4% |  61   5% |  58   4% |  58   4% |  60   6% |  57   4% |  60   4% |  60   4% |  59   4% |
	// |  64 |  61   4% |  60   5% |  62   5% |  61   5% |  60   4% |  59   4% |  62   5% |  61   5% |  60   5% |  59   4% |  63   5% |  60   4% |  61   5% |  65   4% |  60   4% |  60   6% |  59   4% |  60   4% |  60   4% |  59   4% |
	// |  66 |  61   4% |  63   5% |  62   5% |  61   5% |  62   4% |  63   4% |  62   5% |  61   5% |  64   5% |  62   4% |  63   5% |  61   4% |  62   5% |  65   4% |  61   4% |  63   6% |  65   5% |  64   4% |  63   4% |  64   5% |
	// |  68 |  67   5% |  63   5% |  67   5% |  66   5% |  63   4% |  63   4% |  68   5% |  64   5% |  64   5% |  67   4% |  63   5% |  65   4% |  64   5% |  65   4% |  64   4% |  63   6% |  65   5% |  64   4% |  63   4% |  64   4% |
	// |  70 |  67   4% |  68   5% |  67   5% |  66   5% |  66   4% |  65   4% |  68   5% |  65   5% |  69   5% |  67   4% |  67   5% |  66   4% |  66   5% |  65   4% |  68   4% |  67   6% |  66   5% |  68   4% |  66   4% |  65   4% |
	// |  72 |  67   4% |  68   5% |  68   5% |  68   5% |  72   4% |  68   4% |  68   5% |  72   5% |  69   5% |  68   4% |  67   5% |  68   4% |  70   5% |  69   4% |  68   4% |  67   6% |  73   5% |  68   4% |  67   4% |  67   4% |
	// |  74 |  70   4% |  69   5% |  69   5% |  70   5% |  72   4% |  70   4% |  73   5% |  72   5% |  69   5% |  70   4% |  69   4% |  70   4% |  70   5% |  69   4% |  72   4% |  70   6% |  73   5% |  72   4% |  70   4% |  73   5% |
	// |  76 |  73   5% |  73   5% |  73   5% |  72   5% |  72   4% |  71   4% |  73   5% |  72   5% |  74   5% |  73   4% |  74   5% |  73   4% |  72   5% |  73   4% |  72   4% |  72   6% |  73   4% |  72   4% |  72   4% |  73   4% |
	// |  78 |  74   4% |  74   5% |  74   5% |  74   5% |  74   4% |  82   5% |  74   5% |  74   5% |  74   5% |  74   4% |  74   4% |  74   4% |  75   5% |  74   4% |  75   4% |  74   6% |  74   4% |  75   4% |  74   4% |  75   4% |
	// |  80 |  76   5% |  77   5% |  76   5% |  76   5% |  76   4% |  82   4% |  76   5% |  76   5% |  76   5% |  76   4% |  76   4% |  77   4% |  76   5% |  76   4% |  76   4% |  76   6% |  78   4% |  76   4% |  77   4% |  77   4% |
	// |  82 |  78   4% |  79   5% |  78   5% |  78   5% |  78   4% |  82   4% |  78   5% |  80   5% |  79   5% |  78   4% |  78   5% |  78   4% |  78   5% |  81   5% |  78   4% |  78   6% |  78   4% |  78   4% |  79   4% |  78   4% |
	// |  84 |  80   4% |  82   5% |  80   5% |  80   5% |  81   4% |  82   4% |  81   5% |  80   5% |  82   5% |  80   4% |  82   5% |  80   4% |  80   5% |  81   4% |  80   4% |  80   6% |  80   4% |  81   4% |  80   4% |  80   4% |
	// |  86 |  83   4% |  82   5% |  82   5% |  83   5% |  82   4% |  82   4% |  84   5% |  83   5% |  82   5% |  83   4% |  82   5% |  83   4% |  82   5% |  83   4% |  82   4% |  83   6% |  83   4% |  84   4% |  82   4% |  82   5% |
	// |  88 |  86   4% |  84   5% |  84   5% |  86   5% |  84   4% |  84   4% |  84   5% |  86   5% |  85   5% |  85   4% |  84   4% |  84   4% |  84   5% |  86   5% |  84   4% |  84   6% |  85   4% |  84   4% |  85   4% |  84   5% |
	// |  90 |  86   4% |  88   5% |  86   5% |  86   5% |  86   4% |  86   4% |  87   5% |  86   5% |  88   5% |  88   4% |  87   5% |  86   4% |  87   5% |  86   4% |  86   4% |  86   5% |  86   4% |  86   4% |  87   4% |  88   5% |
	// |  92 |  89   4% |  88   5% |  89   5% |  88   5% |  90   4% |  89   4% |  88   5% |  88   5% |  88   5% |  88   4% |  90   5% |  88   4% |  88   5% |  89   5% |  88   4% |  88   6% |  90   5% |  89   4% |  89   4% |  88   5% |
	// |  94 |  93   4% |  91   5% |  90   5% |  91   5% |  90   4% |  90   4% |  90   5% |  90   5% |  91   5% |  90   4% |  90   5% |  91   4% |  91   5% |  90   4% |  90   4% |  90   5% |  90   5% |  90   4% |  91   4% |  93   5% |
	// |  96 |  93   4% |  92   5% |  92   5% |  93   5% |  92   4% |  92   4% |  92   5% |  92   5% |  95   5% |  92   4% |  93   5% |  93   4% |  93   5% |  93   4% |  93   4% |  92   5% |  92   5% |  92   4% |  93   4% |  93   5% |
	// |  98 |  95   4% |  94   5% |  94   5% |  94   5% |  95   4% |  94   4% |  94   5% |  96   5% |  95   5% |  94   4% |  94   5% |  94   4% |  94   5% |  95   4% |  94   4% |  95   5% |  98   5% |  94   4% |  95   4% |  94   4% |
	// +-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
	// Total bytes=1031739570, ranges=1912
}
