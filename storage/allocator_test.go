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
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

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
func createTestAllocator() (*stop.Stopper, *gossip.Gossip, *StorePool, Allocator, *hlc.ManualClock) {
	stopper, g, manualClock, storePool := createTestStorePool(TestTimeUntilStoreDeadOff)
	manualClock.Set(hlc.UnixNano())
	a := MakeAllocator(storePool, AllocatorOptions{AllowRebalance: true})
	return stopper, g, storePool, a, manualClock
}

// mockStorePool sets up a collection of a alive and dead stores in the store
// pool for testing purposes. It also adds dead replicas to the stores and
// ranges in deadReplicas.
func mockStorePool(storePool *StorePool, aliveStoreIDs, deadStoreIDs []roachpb.StoreID, deadReplicas []roachpb.ReplicaIdent) {
	storePool.mu.Lock()
	defer storePool.mu.Unlock()

	storePool.mu.stores = make(map[roachpb.StoreID]*storeDetail)
	for _, storeID := range aliveStoreIDs {
		detail := newStoreDetail()
		detail.desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
		storePool.mu.stores[storeID] = detail
	}
	for _, storeID := range deadStoreIDs {
		detail := newStoreDetail()
		detail.dead = true
		detail.desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
		storePool.mu.stores[storeID] = detail
	}
	for storeID, detail := range storePool.mu.stores {
		for _, replica := range deadReplicas {
			if storeID != replica.Replica.StoreID {
				continue
			}
			detail.deadReplicas[replica.RangeID] = append(detail.deadReplicas[replica.RangeID], replica.Replica)
		}
	}
}

func TestAllocatorSimpleRetrieval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator()
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(singleStore, t)
	result, err := a.AllocateTarget(simpleZoneConfig.Constraints, []roachpb.ReplicaDescriptor{})
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	if result.Node.NodeID != 1 || result.StoreID != 1 {
		t.Errorf("expected NodeID 1 and StoreID 1: %+v", result)
	}
}

func TestAllocatorNoAvailableDisks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, _, _, a, _ := createTestAllocator()
	defer stopper.Stop()
	result, err := a.AllocateTarget(simpleZoneConfig.Constraints, []roachpb.ReplicaDescriptor{})
	if result != nil {
		t.Errorf("expected nil result: %+v", result)
	}
	if err == nil {
		t.Errorf("allocation succeeded despite there being no available disks: %v", result)
	}
}

func TestAllocatorTwoDatacenters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator()
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(multiDCStores, t)
	result1, err := a.AllocateTarget(multiDCConfig.Constraints, []roachpb.ReplicaDescriptor{})
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	result2, err := a.AllocateTarget(multiDCConfig.Constraints, []roachpb.ReplicaDescriptor{{
		NodeID:  result1.Node.NodeID,
		StoreID: result1.StoreID,
	}})
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	ids := []int{int(result1.Node.NodeID), int(result2.Node.NodeID)}
	sort.Ints(ids)
	if expected := []int{1, 2}; !reflect.DeepEqual(ids, expected) {
		t.Errorf("Expected nodes %+v: %+v vs %+v", expected, result1.Node, result2.Node)
	}
	// Verify that no result is forthcoming if we already have a replica.
	result3, err := a.AllocateTarget(multiDCConfig.Constraints, []roachpb.ReplicaDescriptor{
		{
			NodeID:  result1.Node.NodeID,
			StoreID: result1.StoreID,
		},
		{
			NodeID:  result2.Node.NodeID,
			StoreID: result2.StoreID,
		},
	})
	if err == nil {
		t.Errorf("expected error on allocation without available stores: %+v", result3)
	}
}

func TestAllocatorExistingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator()
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
		})
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
	stopper, g, _, a, _ := createTestAllocator()
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(multiDCStores, t)

	testCases := []struct {
		required []config.Constraint // attribute strings
		existing []int               // existing store/node ID
		expID    int                 // expected store/node ID on allocate
		expErr   bool
	}{
		// The two stores in the system have attributes:
		//  storeID=1 {"a", "ssd"}
		//  storeID=2 {"b", "ssd"}
		{
			[]config.Constraint{
				{Value: "a"},
				{Value: "ssd"},
			},
			[]int{}, 1, false,
		},
		{
			[]config.Constraint{
				{Value: "a"},
				{Value: "ssd"},
			},
			[]int{1}, 2, false,
		},
		{
			[]config.Constraint{
				{Value: "a", Type: config.Constraint_REQUIRED},
				{Value: "ssd", Type: config.Constraint_REQUIRED},
			},
			[]int{1}, 0, true,
		},
		{
			[]config.Constraint{
				{Value: "a"},
				{Value: "ssd"},
			},
			[]int{1, 2}, 0, true,
		},
		{
			[]config.Constraint{
				{Value: "b"},
				{Value: "ssd"},
			},
			[]int{}, 2, false,
		},
		{
			[]config.Constraint{
				{Value: "b"},
				{Value: "ssd"},
			},
			[]int{1}, 2, false,
		},
		{
			[]config.Constraint{
				{Value: "b", Type: config.Constraint_REQUIRED},
				{Value: "ssd", Type: config.Constraint_REQUIRED},
			},
			[]int{2}, 0, true,
		},
		{
			[]config.Constraint{
				{Value: "b"},
				{Value: "ssd"},
			},
			[]int{2}, 1, false,
		},
		{
			[]config.Constraint{
				{Value: "b"},
				{Value: "ssd"},
			},
			[]int{1, 2}, 0, true,
		},
		{
			[]config.Constraint{
				{Value: "b"},
				{Value: "hdd"},
			},
			[]int{}, 2, false,
		},
		{
			[]config.Constraint{
				{Value: "b"},
				{Value: "hdd"},
			},
			[]int{2}, 1, false,
		},
		{
			[]config.Constraint{
				{Value: "b", Type: config.Constraint_REQUIRED},
				{Value: "hdd", Type: config.Constraint_REQUIRED},
			},
			[]int{2}, 0, true,
		},
		{
			[]config.Constraint{
				{Value: "b"},
				{Value: "hdd"},
			},
			[]int{1, 2}, 0, true,
		},
		{
			[]config.Constraint{
				{Value: "b"},
				{Value: "ssd"},
				{Value: "gpu"},
			},
			[]int{}, 2, false,
		},
		{
			[]config.Constraint{
				{Value: "b"},
				{Value: "hdd"},
				{Value: "gpu"},
			},
			[]int{}, 2, false,
		},
	}
	for i, test := range testCases {
		var existing []roachpb.ReplicaDescriptor
		for _, id := range test.existing {
			existing = append(existing, roachpb.ReplicaDescriptor{NodeID: roachpb.NodeID(id), StoreID: roachpb.StoreID(id)})
		}
		constraints := config.Constraints{Constraints: test.required}
		result, err := a.AllocateTarget(constraints, existing)
		if haveErr := (err != nil); haveErr != test.expErr {
			t.Errorf("%d: expected error %t; got %t: %s", i, test.expErr, haveErr, err)
		} else if err == nil && roachpb.StoreID(test.expID) != result.StoreID {
			t.Errorf("%d: expected result to have store %d; got %+v", i, test.expID, result)
		}
	}
}

// TestAllocatorRebalance verifies that rebalance targets are chosen
// randomly from amongst stores over the minAvailCapacityThreshold.
func TestAllocatorRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator()
	defer stopper.Stop()

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
	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	// Every rebalance target must be either stores 1 or 2.
	for i := 0; i < 10; i++ {
		result, err := a.RebalanceTarget(config.Constraints{}, []roachpb.ReplicaDescriptor{{StoreID: 3}}, 0)
		if err != nil {
			t.Fatal(err)
		}
		if result == nil {
			t.Fatal("nil result")
		}
		if result.StoreID != 1 && result.StoreID != 2 {
			t.Errorf("%d: expected store 1 or 2; got %d", i, result.StoreID)
		}
	}

	// Verify shouldRebalance results.
	a.options.Deterministic = true
	for i, store := range stores {
		desc, ok := a.storePool.getStoreDescriptor(store.StoreID)
		if !ok {
			t.Fatalf("%d: unable to get store %d descriptor", i, store.StoreID)
		}
		sl, _, _ := a.storePool.getStoreList()
		result := a.shouldRebalance(desc, sl)
		if expResult := (i >= 2); expResult != result {
			t.Errorf("%d: expected rebalance %t; got %t", i, expResult, result)
		}
	}
}

// TestAllocatorRebalanceByCount verifies that rebalance targets are
// chosen by range counts in the event that available capacities
// exceed the maxAvailCapacityThreshold.
func TestAllocatorRebalanceByCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator()
	defer stopper.Stop()

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
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 98, RangeCount: 5},
		},
	}
	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	// Every rebalance target must be store 4 (or nil for case of missing the only option).
	for i := 0; i < 10; i++ {
		result, err := a.RebalanceTarget(config.Constraints{}, []roachpb.ReplicaDescriptor{{StoreID: 1}}, 0)
		if err != nil {
			t.Fatal(err)
		}
		if result != nil && result.StoreID != 4 {
			t.Errorf("expected store 4; got %d", result.StoreID)
		}
	}

	// Verify shouldRebalance results.
	a.options.Deterministic = true
	for i, store := range stores {
		desc, ok := a.storePool.getStoreDescriptor(store.StoreID)
		if !ok {
			t.Fatalf("%d: unable to get store %d descriptor", i, store.StoreID)
		}
		sl, _, _ := a.storePool.getStoreList()
		result := a.shouldRebalance(desc, sl)
		if expResult := (i < 3); expResult != result {
			t.Errorf("%d: expected rebalance %t; got %t", i, expResult, result)
		}
	}
}

// TestAllocatorRemoveTarget verifies that the replica chosen by RemoveTarget is
// the one with the lowest capacity.
func TestAllocatorRemoveTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator()
	defer stopper.Stop()

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
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 65, RangeCount: 12},
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
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	targetRepl, err := a.RemoveTarget(config.Constraints{}, replicas, stores[0].StoreID)
	if err != nil {
		t.Fatal(err)
	}
	if a, e := targetRepl, replicas[2]; a != e {
		t.Fatalf("RemoveTarget did not select expected replica; expected %v, got %v", e, a)
	}

	// Now perform the same test, but pass in the store ID of store 3 so it's
	// excluded.
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
	stopper, _, sp, a, _ := createTestAllocator()
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

	// Each test case should describe a repair situation which has a lower
	// priority than the previous test case.
	testCases := []struct {
		zone           config.ZoneConfig
		desc           roachpb.RangeDescriptor
		expectedAction AllocatorAction
	}{
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
						StoreID:   6,
						NodeID:    6,
						ReplicaID: 6,
					},
				},
			},
			expectedAction: AllocatorRemoveDead,
		},
		// Needs Three replicas, have two
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
	a := MakeAllocator(nil /* storePool */, AllocatorOptions{})
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
	stopper, g, _, a, _ := createTestAllocator()
	defer stopper.Stop()

	// First test to make sure we would send the replica to purgatory.
	_, err := a.AllocateTarget(
		simpleZoneConfig.Constraints,
		[]roachpb.ReplicaDescriptor{},
	)
	if _, ok := err.(purgatoryError); !ok {
		t.Fatalf("expected a purgatory error, got: %v", err)
	}

	// Second, test the normal case in which we can allocate to the store.
	gossiputil.NewStoreGossiper(g).GossipStores(singleStore, t)
	result, err := a.AllocateTarget(
		simpleZoneConfig.Constraints,
		[]roachpb.ReplicaDescriptor{},
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
	storeDetail, ok := a.storePool.mu.stores[singleStore[0].StoreID]
	if !ok {
		t.Fatalf("store:%d was not found in the store pool", singleStore[0].StoreID)
	}
	storeDetail.throttledUntil = timeutil.Now().Add(24 * time.Hour)
	a.storePool.mu.Unlock()
	_, err = a.AllocateTarget(
		simpleZoneConfig.Constraints,
		[]roachpb.ReplicaDescriptor{},
	)
	if _, ok := err.(purgatoryError); ok {
		t.Fatalf("expected a non purgatory error, got: %v", err)
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

	// Model a set of stores in a cluster,
	// randomly adding / removing stores and adding bytes.
	rpcContext := rpc.NewContext(&base.Context{Insecure: true}, nil, stopper)
	server := rpc.NewServer(rpcContext) // never started
	g := gossip.New(rpcContext, server, nil, stopper, metric.NewRegistry())
	// Have to call g.SetNodeID before call g.AddInfo
	g.SetNodeID(roachpb.NodeID(1))
	sp := NewStorePool(
		g,
		hlc.NewClock(hlc.UnixNano),
		nil,
		/* reservationsEnabled */ true,
		TestTimeUntilStoreDeadOff,
		stopper,
	)
	alloc := MakeAllocator(sp, AllocatorOptions{AllowRebalance: true, Deterministic: true})

	var wg sync.WaitGroup
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix), func(_ string, _ roachpb.Value) { wg.Done() })

	const nodes = 20
	const generations = 100
	const printGenerations = generations / 2
	const generationToStopAdding = generations * 9 / 10

	// Initialize testStores.
	var testStores [nodes]testStore
	for i := 0; i < len(testStores); i++ {
		testStores[i].StoreID = roachpb.StoreID(i)
		testStores[i].Node = roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)}
		testStores[i].Capacity = roachpb.StoreCapacity{Capacity: 1 << 30, Available: 1 << 30}
	}
	// Initialize the cluster with a single range.
	testStores[0].add(alloc.randGen.Int63n(1 << 20))

	for i := 0; i < generations; i++ {
		if i < generationToStopAdding {
			// First loop through test stores and add data.
			for j := 0; j < len(testStores); j++ {
				// Add a pretend range to the testStore if there's already one.
				if testStores[j].Capacity.RangeCount > 0 {
					testStores[j].add(alloc.randGen.Int63n(1 << 20))
				}
			}
		}
		// Gossip the new store info.
		wg.Add(len(testStores))
		for j := 0; j < len(testStores); j++ {
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
				-1)
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
			fmt.Printf("generation %4d: ", i)
			for j := 0; j < len(testStores); j++ {
				if j != 0 && j != len(testStores)-1 {
					fmt.Printf(",")
				}
				ts := testStores[j]
				bytes := ts.Capacity.Capacity - ts.Capacity.Available
				fmt.Printf("%3d %2d%%", ts.Capacity.RangeCount, (100*bytes)/totalBytes)
			}
			fmt.Printf("\n")
		}
	}

	var totBytes int64
	var totRanges int32
	for i := 0; i < len(testStores); i++ {
		totBytes += testStores[i].Capacity.Capacity - testStores[i].Capacity.Available
		totRanges += testStores[i].Capacity.RangeCount
	}
	fmt.Printf("Total bytes=%d, ranges=%d\n", totBytes, totRanges)

	// Output:
	// generation    0:   1 88%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  1 11%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%  0  0%
	// generation    2:   1 19%,  0  0%,  1 24%,  0  0%,  0  0%,  0  0%,  1 18%,  0  0%,  1  7%,  1  1%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  1  5%,  1  9%  1 12%
	// generation    4:   2  7%,  2  8%,  2  9%,  0  0%,  2  3%,  0  0%,  2 15%,  1  0%,  2  1%,  3  3%,  2  8%,  0  0%,  2  7%,  0  0%,  0  0%,  2  2%,  2  7%,  2 10%,  2  8%  2  5%
	// generation    6:   4  6%,  4  5%,  4  7%,  0  0%,  4  5%,  0  0%,  4 11%,  3  3%,  4  3%,  4  2%,  4  6%,  0  0%,  4  8%,  2  2%,  0  0%,  4  4%,  4  7%,  4 10%,  4  6%  4  6%
	// generation    8:   5  3%,  5  3%,  5  4%,  8  6%,  5  3%,  4  6%,  5  9%,  5  3%,  5  3%,  5  2%,  5  5%,  3  3%,  5  6%,  4  2%,  3  2%,  5  4%,  5  7%,  5  8%,  5  6%  5  5%
	// generation   10:   7  4%,  7  3%,  7  4%,  8  4%,  7  4%,  6  6%,  7  7%,  7  4%,  7  3%,  7  3%,  7  5%,  6  4%,  7  7%,  6  2%,  6  3%,  7  3%,  7  6%,  7  7%,  7  5%  7  4%
	// generation   12:   9  4%,  9  4%,  9  4%,  9  3%,  9  4%,  8  6%,  9  7%,  9  4%,  9  4%,  9  4%,  9  5%,  9  5%,  9  6%,  8  3%,  8  4%,  9  3%,  9  5%,  9  7%,  9  6%  9  4%
	// generation   14:  11  4%, 11  3%, 11  4%, 11  3%, 11  4%, 10  5%, 11  6%, 11  4%, 11  4%, 11  4%, 11  5%, 11  5%, 11  6%, 10  3%, 10  5%, 11  3%, 11  5%, 11  7%, 11  5% 11  4%
	// generation   16:  13  4%, 13  3%, 13  4%, 13  3%, 13  4%, 12  5%, 13  5%, 13  4%, 13  4%, 13  4%, 13  5%, 13  5%, 13  5%, 12  3%, 12  5%, 13  4%, 13  5%, 13  7%, 13  5% 13  5%
	// generation   18:  15  5%, 15  3%, 15  4%, 15  4%, 15  4%, 14  5%, 15  6%, 15  5%, 15  4%, 15  4%, 15  5%, 15  4%, 15  5%, 14  3%, 14  5%, 15  4%, 15  5%, 15  7%, 15  5% 15  5%
	// generation   20:  17  5%, 17  3%, 17  4%, 17  4%, 17  4%, 16  4%, 17  6%, 17  5%, 17  4%, 17  4%, 17  5%, 17  4%, 17  6%, 16  4%, 16  5%, 17  4%, 17  5%, 17  6%, 17  4% 17  5%
	// generation   22:  19  4%, 19  3%, 19  4%, 19  4%, 19  4%, 18  5%, 19  5%, 19  5%, 19  4%, 19  4%, 19  5%, 19  4%, 19  6%, 18  3%, 18  5%, 19  4%, 19  6%, 19  6%, 19  4% 19  5%
	// generation   24:  21  4%, 21  3%, 21  4%, 21  4%, 21  4%, 20  5%, 21  5%, 21  5%, 21  4%, 21  4%, 21  5%, 21  4%, 21  5%, 20  4%, 20  5%, 21  4%, 21  6%, 21  6%, 21  4% 21  5%
	// generation   26:  23  4%, 23  4%, 23  3%, 23  4%, 23  5%, 22  5%, 23  5%, 23  5%, 23  4%, 23  4%, 23  5%, 23  4%, 23  5%, 22  4%, 22  5%, 23  4%, 23  6%, 23  6%, 23  5% 23  5%
	// generation   28:  25  4%, 25  4%, 25  3%, 25  4%, 25  4%, 24  5%, 25  5%, 25  5%, 25  4%, 25  4%, 25  5%, 25  4%, 25  5%, 24  4%, 24  5%, 25  4%, 25  6%, 25  5%, 25  5% 25  5%
	// generation   30:  27  4%, 27  4%, 27  4%, 27  4%, 27  4%, 26  5%, 27  5%, 27  5%, 27  4%, 27  4%, 27  5%, 27  4%, 27  5%, 26  4%, 26  5%, 27  4%, 27  6%, 27  5%, 27  5% 27  5%
	// generation   32:  29  4%, 29  4%, 29  4%, 29  4%, 29  4%, 28  5%, 29  5%, 29  4%, 29  4%, 29  4%, 29  5%, 29  4%, 29  5%, 28  3%, 28  5%, 29  4%, 29  5%, 29  5%, 29  4% 29  5%
	// generation   34:  31  4%, 31  4%, 31  4%, 31  5%, 31  4%, 30  5%, 31  5%, 31  4%, 31  4%, 31  4%, 31  5%, 31  4%, 31  5%, 30  3%, 30  5%, 31  4%, 31  6%, 31  5%, 31  5% 31  5%
	// generation   36:  33  4%, 33  4%, 33  4%, 33  5%, 33  4%, 32  5%, 33  5%, 33  4%, 33  4%, 33  4%, 33  5%, 33  4%, 33  5%, 32  4%, 32  5%, 33  4%, 33  6%, 33  5%, 33  5% 33  5%
	// generation   38:  35  4%, 35  4%, 35  4%, 35  5%, 35  4%, 34  5%, 35  5%, 35  4%, 35  4%, 35  4%, 35  5%, 35  4%, 35  5%, 34  4%, 34  5%, 35  4%, 35  6%, 35  5%, 35  4% 35  5%
	// generation   40:  37  4%, 37  4%, 37  4%, 37  5%, 37  4%, 36  5%, 37  5%, 37  4%, 37  4%, 37  4%, 37  5%, 37  4%, 37  5%, 36  4%, 36  5%, 37  4%, 37  6%, 37  5%, 37  4% 37  5%
	// generation   42:  39  4%, 39  4%, 39  4%, 39  5%, 39  4%, 38  5%, 39  5%, 39  4%, 39  4%, 39  4%, 39  5%, 39  4%, 39  5%, 38  4%, 38  5%, 39  4%, 39  6%, 39  5%, 39  5% 39  5%
	// generation   44:  41  4%, 41  4%, 41  4%, 41  5%, 41  4%, 40  5%, 41  5%, 41  4%, 41  4%, 41  4%, 41  5%, 41  4%, 41  5%, 40  4%, 40  5%, 41  4%, 41  5%, 41  5%, 41  5% 41  5%
	// generation   46:  43  4%, 43  4%, 43  4%, 43  5%, 43  4%, 42  5%, 43  5%, 43  4%, 43  4%, 43  4%, 43  5%, 43  4%, 43  5%, 42  4%, 42  5%, 43  4%, 43  5%, 43  5%, 43  4% 43  5%
	// generation   48:  45  4%, 45  4%, 45  4%, 45  5%, 45  4%, 44  5%, 45  5%, 45  4%, 45  4%, 45  4%, 45  5%, 45  4%, 45  5%, 44  4%, 44  5%, 45  4%, 45  5%, 45  5%, 45  4% 45  5%
	// generation   50:  47  4%, 47  4%, 47  4%, 47  5%, 47  4%, 46  5%, 47  5%, 47  4%, 47  4%, 47  4%, 47  5%, 47  4%, 47  5%, 46  4%, 46  5%, 47  5%, 47  5%, 47  5%, 47  5% 47  5%
	// generation   52:  49  4%, 49  4%, 49  4%, 49  5%, 49  4%, 48  5%, 49  5%, 49  4%, 49  4%, 49  4%, 49  5%, 49  4%, 49  5%, 48  4%, 48  5%, 49  4%, 49  5%, 49  5%, 49  5% 49  5%
	// generation   54:  51  4%, 51  4%, 51  4%, 51  5%, 51  4%, 50  5%, 51  5%, 51  4%, 51  4%, 51  4%, 51  5%, 51  4%, 51  5%, 50  4%, 50  5%, 51  5%, 51  5%, 51  5%, 51  5% 51  5%
	// generation   56:  53  4%, 53  4%, 53  4%, 53  5%, 53  4%, 52  5%, 53  5%, 53  4%, 53  4%, 53  4%, 53  5%, 53  4%, 53  5%, 52  4%, 52  5%, 53  5%, 53  5%, 53  5%, 53  5% 53  5%
	// generation   58:  55  4%, 55  4%, 55  4%, 55  5%, 55  4%, 54  5%, 55  5%, 55  4%, 55  4%, 55  4%, 55  5%, 55  4%, 55  5%, 54  4%, 54  5%, 55  5%, 55  5%, 55  5%, 55  5% 55  5%
	// generation   60:  57  4%, 57  4%, 57  4%, 57  5%, 57  4%, 56  5%, 57  5%, 57  4%, 57  4%, 57  4%, 57  5%, 57  4%, 57  5%, 56  4%, 56  5%, 57  4%, 57  5%, 57  5%, 57  5% 57  5%
	// generation   62:  59  4%, 59  4%, 59  4%, 59  5%, 59  4%, 58  5%, 59  5%, 59  4%, 59  4%, 59  4%, 59  5%, 59  4%, 59  5%, 58  4%, 58  5%, 59  4%, 59  5%, 59  5%, 59  5% 59  5%
	// generation   64:  61  4%, 61  4%, 61  4%, 61  5%, 61  4%, 60  5%, 61  5%, 61  4%, 61  4%, 61  4%, 61  5%, 61  4%, 61  5%, 60  4%, 60  5%, 61  5%, 61  5%, 61  5%, 61  5% 61  5%
	// generation   66:  63  4%, 63  4%, 63  4%, 63  5%, 63  4%, 62  5%, 63  5%, 63  4%, 63  4%, 63  4%, 63  5%, 63  4%, 63  5%, 62  4%, 62  5%, 63  5%, 63  5%, 63  5%, 63  4% 63  5%
	// generation   68:  65  4%, 65  4%, 65  4%, 65  5%, 65  4%, 64  5%, 65  5%, 65  4%, 65  4%, 65  4%, 65  5%, 65  4%, 65  5%, 64  4%, 64  5%, 65  5%, 65  5%, 65  5%, 65  4% 65  5%
	// generation   70:  67  4%, 67  4%, 67  4%, 67  5%, 67  4%, 66  5%, 67  5%, 67  4%, 67  4%, 67  4%, 67  5%, 67  4%, 67  5%, 66  4%, 66  5%, 67  5%, 67  5%, 67  5%, 67  4% 67  5%
	// generation   72:  69  4%, 69  4%, 69  4%, 69  5%, 69  4%, 68  5%, 69  5%, 69  4%, 69  4%, 69  4%, 69  5%, 69  4%, 69  5%, 68  4%, 68  5%, 69  5%, 69  5%, 69  5%, 69  4% 69  5%
	// generation   74:  71  4%, 71  4%, 71  4%, 71  5%, 71  4%, 70  5%, 71  5%, 71  4%, 71  4%, 71  4%, 71  5%, 71  4%, 71  5%, 70  4%, 70  5%, 71  5%, 71  5%, 71  5%, 71  4% 71  5%
	// generation   76:  73  4%, 73  4%, 73  4%, 73  5%, 73  4%, 72  5%, 73  5%, 73  4%, 73  4%, 73  4%, 73  5%, 73  4%, 73  5%, 72  4%, 72  5%, 73  5%, 73  5%, 73  5%, 73  4% 73  5%
	// generation   78:  75  4%, 75  4%, 75  4%, 75  5%, 75  4%, 74  5%, 75  5%, 75  4%, 75  4%, 75  4%, 75  5%, 75  4%, 75  5%, 74  4%, 74  5%, 75  5%, 75  5%, 75  5%, 75  4% 75  5%
	// generation   80:  77  4%, 77  4%, 77  4%, 77  5%, 77  4%, 76  5%, 77  5%, 77  4%, 77  4%, 77  5%, 77  5%, 77  4%, 77  5%, 76  4%, 76  5%, 77  5%, 77  5%, 77  5%, 77  4% 77  5%
	// generation   82:  79  4%, 79  4%, 79  4%, 79  5%, 79  4%, 78  5%, 79  5%, 79  4%, 79  4%, 79  5%, 79  5%, 79  4%, 79  5%, 78  4%, 78  5%, 79  5%, 79  5%, 79  5%, 79  4% 79  5%
	// generation   84:  81  4%, 81  4%, 81  4%, 81  5%, 81  4%, 80  5%, 81  5%, 81  4%, 81  4%, 81  5%, 81  5%, 81  5%, 81  5%, 80  4%, 80  5%, 81  5%, 81  5%, 81  5%, 81  4% 81  5%
	// generation   86:  83  4%, 83  4%, 83  4%, 83  5%, 83  4%, 82  5%, 83  5%, 83  4%, 83  4%, 83  5%, 83  5%, 83  5%, 83  5%, 82  4%, 82  5%, 83  5%, 83  5%, 83  5%, 83  4% 83  5%
	// generation   88:  85  4%, 85  4%, 85  4%, 85  5%, 85  4%, 84  5%, 85  5%, 85  4%, 85  4%, 85  5%, 85  4%, 85  5%, 85  5%, 84  4%, 84  5%, 85  4%, 85  5%, 85  5%, 85  4% 85  5%
	// generation   90:  86  4%, 86  4%, 86  4%, 86  5%, 86  4%, 85  5%, 86  5%, 86  4%, 86  4%, 86  5%, 86  5%, 86  4%, 86  5%, 85  4%, 85  5%, 86  4%, 86  5%, 86  5%, 86  4% 86  5%
	// generation   92:  86  4%, 86  4%, 86  4%, 86  5%, 86  4%, 85  5%, 86  5%, 86  4%, 86  4%, 86  5%, 86  5%, 86  4%, 86  5%, 85  4%, 85  5%, 86  4%, 86  5%, 86  5%, 86  4% 86  5%
	// generation   94:  86  4%, 86  4%, 86  4%, 86  5%, 86  4%, 85  5%, 86  5%, 86  4%, 86  4%, 86  5%, 86  5%, 86  4%, 86  5%, 85  4%, 85  5%, 86  4%, 86  5%, 86  5%, 86  4% 86  5%
	// generation   96:  86  4%, 86  4%, 86  4%, 86  5%, 86  4%, 85  5%, 86  5%, 86  4%, 86  4%, 86  5%, 86  5%, 86  4%, 86  5%, 85  4%, 85  5%, 86  4%, 86  5%, 86  5%, 86  4% 86  5%
	// generation   98:  86  4%, 86  4%, 86  4%, 86  5%, 86  4%, 85  5%, 86  5%, 86  4%, 86  4%, 86  5%, 86  5%, 86  4%, 86  5%, 85  4%, 85  5%, 86  4%, 86  5%, 86  5%, 86  4% 86  5%
	// Total bytes=897074388, ranges=1717
}
