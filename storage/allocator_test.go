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
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

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
	rpcContext := rpc.NewContext(context.TODO(), &base.Context{Insecure: true}, nil, stopper)
	server := rpc.NewServer(rpcContext) // never started
	g := gossip.New(context.Background(), rpcContext, server, nil, stopper, metric.NewRegistry())
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
	sp.TestSetDeterministic(true)
	alloc := MakeAllocator(sp, AllocatorOptions{AllowRebalance: true})

	var wg sync.WaitGroup
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix), func(_ string, _ roachpb.Value) { wg.Done() })

	const nodes = 20
	const generations = 100
	const printGenerations = generations / 2
	const generationToStopAdding = generations * 9 / 10

	randGen := rand.New(rand.NewSource(777))

	// Initialize testStores.
	var testStores [nodes]testStore
	for i := 0; i < len(testStores); i++ {
		testStores[i].StoreID = roachpb.StoreID(i)
		testStores[i].Node = roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)}
		testStores[i].Capacity = roachpb.StoreCapacity{Capacity: 1 << 30, Available: 1 << 30}
	}
	// Initialize the cluster with a single range.
	testStores[0].add(randGen.Int63n(1 << 20))

	for i := 0; i < generations; i++ {
		if i < generationToStopAdding {
			// First loop through test stores and add data.
			for j := 0; j < len(testStores); j++ {
				// Add a pretend range to the testStore if there's already one.
				if testStores[j].Capacity.RangeCount > 0 {
					testStores[j].add(randGen.Int63n(1 << 20))
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
				testStores[j].rebalance(&testStores[int(target.StoreID)], randGen.Int63n(1<<20))
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
	// generation    0:   1 88%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%  1 11%
	// generation    2:   1 32%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  2 21%,  3 24%  1 21%
	// generation    4:   1  8%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  5 21%,  4 14%,  2 22%,  3 25%  1  7%
	// generation    6:   2  9%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  7 21%,  4 23%,  5 15%,  4  2%,  2  3%,  3 19%  2  5%
	// generation    8:   3  7%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  6 14%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  8 14%,  7 12%,  4 15%,  5  9%,  4  4%,  3  2%,  3 13%  3  5%
	// generation   10:   4  5%, 10 13%,  0  0%,  0  0%,  0  0%,  0  0%,  6  8%,  0  0%,  0  0%,  0  0%,  0  0%,  7  9%,  8  5%,  7  9%,  4  9%,  5  8%,  4  6%,  4  3%,  4 12%  4  6%
	// generation   12:   5  6%, 10  9%,  7  6%,  0  0%,  0  0%,  0  0%,  6  5%, 12 13%,  0  0%,  0  0%,  0  0%,  7  7%,  8  3%,  7  6%,  5  7%,  5  7%,  5  4%,  5  5%,  5  9%  5  5%
	// generation   14:   7  6%, 10  5%,  7  4%,  7  9%,  0  0%,  0  0%,  7  5%, 12  8%,  7  6%,  0  0%,  0  0%,  7  4%,  8  3%,  7  4%,  7  6%,  7  7%,  7  4%,  7  5%,  7  9%  7  6%
	// generation   16:   8  5%, 10  4%,  8  4%,  8  7%, 16 11%,  4  4%,  8  5%, 12  5%,  8  6%,  0  0%,  0  0%,  8  4%,  8  1%,  8  4%,  8  5%,  8  7%,  8  4%,  8  4%,  8  9%  8  4%
	// generation   18:  10  5%, 10  3%, 10  4%, 10  6%, 16  9%,  6  4%, 10  6%, 12  4%, 10  5%,  3  0%,  4  1%, 10  4%, 10  2%, 10  3%, 10  5%, 10  7%, 10  4%, 10  4%, 10  7% 10  5%
	// generation   20:  12  5%, 12  4%, 12  3%, 12  6%, 16  8%,  8  4%, 12  6%, 12  4%, 12  5%,  7  2%,  8  3%, 12  4%, 12  3%, 12  4%, 12  5%, 12  6%, 12  5%, 12  4%, 12  6% 12  5%
	// generation   22:  14  5%, 14  3%, 14  4%, 14  5%, 16  7%, 11  5%, 14  6%, 14  3%, 14  5%, 10  2%, 10  3%, 14  4%, 14  3%, 14  4%, 14  5%, 14  6%, 14  5%, 14  4%, 14  6% 14  5%
	// generation   24:  16  5%, 16  4%, 16  4%, 16  5%, 16  7%, 13  5%, 16  6%, 16  4%, 16  5%, 13  2%, 13  3%, 16  4%, 16  3%, 16  3%, 16  5%, 16  6%, 16  5%, 16  5%, 16  5% 16  4%
	// generation   26:  18  4%, 18  4%, 18  5%, 18  5%, 18  7%, 15  5%, 18  6%, 18  4%, 18  5%, 15  3%, 15  3%, 18  4%, 18  4%, 18  3%, 18  5%, 18  6%, 18  5%, 18  5%, 18  5% 18  4%
	// generation   28:  20  4%, 20  4%, 20  4%, 20  5%, 20  7%, 17  5%, 20  6%, 20  4%, 20  5%, 17  3%, 17  3%, 20  4%, 20  4%, 20  3%, 20  5%, 20  6%, 20  5%, 20  5%, 20  5% 20  4%
	// generation   30:  22  4%, 22  4%, 22  4%, 22  5%, 22  6%, 19  5%, 22  6%, 22  4%, 22  5%, 19  3%, 19  3%, 22  4%, 22  4%, 22  3%, 22  5%, 22  5%, 22  5%, 22  5%, 22  5% 22  5%
	// generation   32:  24  4%, 24  4%, 24  4%, 24  5%, 24  6%, 21  5%, 24  5%, 24  4%, 24  5%, 21  3%, 21  3%, 24  4%, 24  4%, 24  3%, 24  5%, 24  5%, 24  5%, 24  5%, 24  5% 24  4%
	// generation   34:  26  4%, 26  4%, 26  4%, 26  5%, 26  5%, 23  4%, 26  5%, 26  4%, 26  5%, 23  3%, 23  3%, 26  4%, 26  4%, 26  3%, 26  5%, 26  6%, 26  5%, 26  5%, 26  5% 26  5%
	// generation   36:  28  4%, 28  4%, 28  4%, 28  5%, 28  6%, 25  4%, 28  5%, 28  4%, 28  5%, 25  3%, 25  4%, 28  4%, 28  4%, 28  3%, 28  5%, 28  5%, 28  5%, 28  5%, 28  5% 28  5%
	// generation   38:  30  4%, 30  5%, 30  4%, 30  5%, 30  6%, 27  4%, 30  5%, 30  4%, 30  5%, 27  4%, 27  3%, 30  4%, 30  4%, 30  3%, 30  6%, 30  5%, 30  5%, 30  5%, 30  5% 30  5%
	// generation   40:  32  4%, 32  5%, 32  4%, 32  5%, 32  6%, 29  4%, 32  5%, 32  4%, 32  5%, 29  4%, 29  4%, 32  4%, 32  4%, 32  3%, 32  5%, 32  5%, 32  5%, 32  5%, 32  5% 32  4%
	// generation   42:  34  4%, 34  5%, 34  4%, 34  5%, 34  6%, 31  4%, 34  5%, 34  4%, 34  5%, 31  4%, 31  4%, 34  4%, 34  4%, 34  3%, 34  5%, 34  5%, 34  5%, 34  5%, 34  4% 34  4%
	// generation   44:  36  4%, 36  5%, 36  4%, 36  5%, 36  6%, 33  4%, 36  5%, 36  4%, 36  5%, 33  4%, 33  4%, 36  4%, 36  4%, 36  4%, 36  5%, 36  5%, 36  5%, 36  5%, 36  4% 36  4%
	// generation   46:  38  4%, 38  5%, 38  4%, 38  5%, 38  6%, 35  4%, 38  5%, 38  4%, 38  5%, 35  4%, 35  4%, 38  4%, 38  4%, 38  4%, 38  5%, 38  5%, 38  5%, 38  5%, 38  4% 38  4%
	// generation   48:  40  4%, 40  4%, 40  4%, 40  5%, 40  6%, 37  4%, 40  5%, 40  4%, 40  5%, 37  4%, 37  4%, 40  4%, 40  4%, 40  4%, 40  5%, 40  5%, 40  5%, 40  5%, 40  4% 40  4%
	// generation   50:  42  4%, 42  5%, 42  4%, 42  5%, 42  6%, 39  4%, 42  5%, 42  4%, 42  5%, 39  4%, 39  4%, 42  4%, 42  4%, 42  4%, 42  5%, 42  5%, 42  5%, 42  5%, 42  4% 42  4%
	// generation   52:  44  4%, 44  4%, 44  4%, 44  5%, 44  6%, 41  4%, 44  5%, 44  4%, 44  5%, 41  4%, 41  4%, 44  4%, 44  4%, 44  4%, 44  5%, 44  5%, 44  5%, 44  5%, 44  4% 44  4%
	// generation   54:  46  4%, 46  4%, 46  4%, 46  5%, 46  6%, 43  4%, 46  5%, 46  4%, 46  5%, 43  4%, 43  4%, 46  4%, 46  4%, 46  4%, 46  5%, 46  5%, 46  5%, 46  5%, 46  4% 46  4%
	// generation   56:  48  4%, 48  4%, 48  4%, 48  5%, 48  6%, 45  4%, 48  5%, 48  4%, 48  5%, 45  4%, 45  4%, 48  4%, 48  4%, 48  4%, 48  5%, 48  5%, 48  5%, 48  5%, 48  4% 48  4%
	// generation   58:  50  4%, 50  4%, 50  4%, 50  5%, 50  6%, 47  4%, 50  5%, 50  4%, 50  5%, 47  4%, 47  4%, 50  4%, 50  5%, 50  4%, 50  5%, 50  5%, 50  5%, 50  5%, 50  4% 50  4%
	// generation   60:  52  4%, 52  5%, 52  4%, 52  5%, 52  6%, 49  4%, 52  5%, 52  4%, 52  5%, 49  4%, 49  4%, 52  4%, 52  5%, 52  4%, 52  5%, 52  5%, 52  5%, 52  5%, 52  4% 52  4%
	// generation   62:  54  4%, 54  5%, 54  4%, 54  5%, 54  5%, 51  4%, 54  5%, 54  4%, 54  5%, 51  4%, 51  4%, 54  4%, 54  5%, 54  4%, 54  5%, 54  5%, 54  5%, 54  5%, 54  4% 54  4%
	// generation   64:  56  4%, 56  5%, 56  4%, 56  5%, 56  5%, 53  4%, 56  5%, 56  4%, 56  5%, 53  4%, 53  4%, 56  4%, 56  5%, 56  4%, 56  5%, 56  5%, 56  5%, 56  5%, 56  4% 56  4%
	// generation   66:  58  4%, 58  4%, 58  4%, 58  5%, 58  5%, 55  4%, 58  5%, 58  4%, 58  5%, 55  4%, 55  4%, 58  4%, 58  5%, 58  4%, 58  5%, 58  5%, 58  5%, 58  5%, 58  4% 58  5%
	// generation   68:  60  4%, 60  5%, 60  4%, 60  5%, 60  5%, 57  4%, 60  5%, 60  4%, 60  5%, 57  4%, 57  4%, 60  4%, 60  5%, 60  4%, 60  5%, 60  5%, 60  5%, 60  5%, 60  4% 60  5%
	// generation   70:  62  4%, 62  5%, 62  4%, 62  5%, 62  5%, 59  4%, 62  5%, 62  4%, 62  5%, 59  4%, 59  4%, 62  4%, 62  5%, 62  4%, 62  5%, 62  5%, 62  5%, 62  5%, 62  4% 62  5%
	// generation   72:  64  4%, 64  5%, 64  4%, 64  5%, 64  5%, 61  4%, 64  5%, 64  4%, 64  5%, 61  4%, 61  4%, 64  4%, 64  5%, 64  4%, 64  5%, 64  5%, 64  4%, 64  5%, 64  4% 64  5%
	// generation   74:  66  4%, 66  5%, 66  4%, 66  5%, 66  5%, 63  4%, 66  5%, 66  4%, 66  5%, 63  4%, 63  4%, 66  4%, 66  5%, 66  4%, 66  5%, 66  5%, 66  4%, 66  5%, 66  4% 66  4%
	// generation   76:  68  4%, 68  5%, 68  4%, 68  5%, 68  5%, 65  4%, 68  5%, 68  4%, 68  5%, 65  4%, 65  4%, 68  4%, 68  4%, 68  4%, 68  5%, 68  5%, 68  4%, 68  5%, 68  4% 68  5%
	// generation   78:  70  4%, 70  5%, 70  4%, 70  5%, 70  5%, 67  4%, 70  5%, 70  4%, 70  5%, 67  4%, 67  4%, 70  4%, 70  4%, 70  4%, 70  5%, 70  5%, 70  5%, 70  5%, 70  4% 70  4%
	// generation   80:  72  4%, 72  5%, 72  4%, 72  5%, 72  5%, 69  4%, 72  5%, 72  4%, 72  5%, 69  4%, 69  4%, 72  4%, 72  4%, 72  4%, 72  5%, 72  5%, 72  4%, 72  5%, 72  4% 72  5%
	// generation   82:  74  4%, 74  5%, 74  4%, 74  5%, 74  5%, 71  4%, 74  5%, 74  4%, 74  5%, 71  4%, 71  4%, 74  4%, 74  4%, 74  4%, 74  5%, 74  5%, 74  4%, 74  5%, 74  4% 74  5%
	// generation   84:  76  4%, 76  5%, 76  4%, 76  5%, 76  5%, 73  4%, 76  5%, 76  4%, 76  5%, 73  4%, 73  4%, 76  4%, 76  4%, 76  4%, 76  5%, 76  5%, 76  4%, 76  5%, 76  4% 76  5%
	// generation   86:  78  4%, 78  5%, 78  4%, 78  5%, 78  5%, 75  4%, 78  5%, 78  4%, 78  5%, 75  4%, 75  4%, 78  4%, 78  4%, 78  4%, 78  5%, 78  5%, 78  4%, 78  5%, 78  4% 78  4%
	// generation   88:  80  4%, 80  5%, 80  4%, 80  5%, 80  5%, 77  4%, 80  5%, 80  4%, 80  5%, 77  4%, 77  4%, 80  4%, 80  4%, 80  4%, 80  5%, 80  5%, 80  5%, 80  5%, 80  4% 80  4%
	// generation   90:  81  4%, 81  5%, 81  4%, 81  5%, 81  5%, 78  4%, 81  5%, 81  5%, 81  5%, 78  4%, 78  4%, 81  4%, 81  4%, 81  4%, 81  5%, 81  5%, 81  5%, 81  5%, 81  4% 81  4%
	// generation   92:  81  4%, 81  5%, 81  4%, 81  5%, 81  5%, 78  4%, 81  5%, 81  5%, 81  5%, 78  4%, 78  4%, 81  4%, 81  4%, 81  4%, 81  5%, 81  5%, 81  5%, 81  5%, 81  4% 81  4%
	// generation   94:  81  4%, 81  5%, 81  4%, 81  5%, 81  5%, 78  4%, 81  5%, 81  5%, 81  5%, 78  4%, 78  4%, 81  4%, 81  4%, 81  4%, 81  5%, 81  5%, 81  5%, 81  5%, 81  4% 81  4%
	// generation   96:  81  4%, 81  5%, 81  4%, 81  5%, 81  5%, 78  4%, 81  5%, 81  5%, 81  5%, 78  4%, 78  4%, 81  4%, 81  4%, 81  4%, 81  5%, 81  5%, 81  5%, 81  5%, 81  4% 81  4%
	// generation   98:  81  4%, 81  5%, 81  4%, 81  5%, 81  5%, 78  4%, 81  5%, 81  5%, 81  5%, 78  4%, 78  4%, 81  4%, 81  4%, 81  4%, 81  5%, 81  5%, 81  5%, 81  5%, 81  4% 81  4%
	// Total bytes=839900871, ranges=1611
}
