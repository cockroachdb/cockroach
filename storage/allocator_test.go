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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Kathy Spradlin (kathyspradlin@gmail.com)
// Author: Levon Lloyd (levon.lloyd@gmail.com)

package storage

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

var simpleZoneConfig = config.ZoneConfig{
	ReplicaAttrs: []proto.Attributes{
		{Attrs: []string{"a", "ssd"}},
	},
}

var multiDisksConfig = config.ZoneConfig{
	ReplicaAttrs: []proto.Attributes{
		{Attrs: []string{"a", "ssd"}},
		{Attrs: []string{"a", "hdd"}},
		{Attrs: []string{"a", "mem"}},
	},
}

var multiDCConfig = config.ZoneConfig{
	ReplicaAttrs: []proto.Attributes{
		{Attrs: []string{"a", "ssd"}},
		{Attrs: []string{"b", "ssd"}},
	},
}

var singleStore = []*proto.StoreDescriptor{
	{
		StoreID: 1,
		Attrs:   proto.Attributes{Attrs: []string{"ssd"}},
		Node: proto.NodeDescriptor{
			NodeID: 1,
			Attrs:  proto.Attributes{Attrs: []string{"a"}},
		},
		Capacity: proto.StoreCapacity{
			Capacity:  100,
			Available: 200,
		},
	},
}

var sameDCStores = []*proto.StoreDescriptor{
	{
		StoreID: 1,
		Attrs:   proto.Attributes{Attrs: []string{"ssd"}},
		Node: proto.NodeDescriptor{
			NodeID: 1,
			Attrs:  proto.Attributes{Attrs: []string{"a"}},
		},
		Capacity: proto.StoreCapacity{
			Capacity:  100,
			Available: 200,
		},
	},
	{
		StoreID: 2,
		Attrs:   proto.Attributes{Attrs: []string{"ssd"}},
		Node: proto.NodeDescriptor{
			NodeID: 2,
			Attrs:  proto.Attributes{Attrs: []string{"a"}},
		},
		Capacity: proto.StoreCapacity{
			Capacity:  100,
			Available: 200,
		},
	},
	{
		StoreID: 3,
		Attrs:   proto.Attributes{Attrs: []string{"hdd"}},
		Node: proto.NodeDescriptor{
			NodeID: 2,
			Attrs:  proto.Attributes{Attrs: []string{"a"}},
		},
		Capacity: proto.StoreCapacity{
			Capacity:  100,
			Available: 200,
		},
	},
	{
		StoreID: 4,
		Attrs:   proto.Attributes{Attrs: []string{"hdd"}},
		Node: proto.NodeDescriptor{
			NodeID: 3,
			Attrs:  proto.Attributes{Attrs: []string{"a"}},
		},
		Capacity: proto.StoreCapacity{
			Capacity:  100,
			Available: 200,
		},
	},
	{
		StoreID: 5,
		Attrs:   proto.Attributes{Attrs: []string{"mem"}},
		Node: proto.NodeDescriptor{
			NodeID: 4,
			Attrs:  proto.Attributes{Attrs: []string{"a"}},
		},
		Capacity: proto.StoreCapacity{
			Capacity:  100,
			Available: 200,
		},
	},
}

var multiDCStores = []*proto.StoreDescriptor{
	{
		StoreID: 1,
		Attrs:   proto.Attributes{Attrs: []string{"ssd"}},
		Node: proto.NodeDescriptor{
			NodeID: 1,
			Attrs:  proto.Attributes{Attrs: []string{"a"}},
		},
		Capacity: proto.StoreCapacity{
			Capacity:  100,
			Available: 200,
		},
	},
	{
		StoreID: 2,
		Attrs:   proto.Attributes{Attrs: []string{"ssd"}},
		Node: proto.NodeDescriptor{
			NodeID: 2,
			Attrs:  proto.Attributes{Attrs: []string{"b"}},
		},
		Capacity: proto.StoreCapacity{
			Capacity:  100,
			Available: 200,
		},
	},
}

// createTestAllocator creates a stopper, gossip, store pool and allocator for
// use in tests. Stopper must be stopped by the caller.
func createTestAllocator() (*stop.Stopper, *gossip.Gossip, *StorePool, Allocator) {
	stopper := stop.NewStopper()
	rpcContext := rpc.NewContext(&base.Context{}, hlc.NewClock(hlc.UnixNano), stopper)
	g := gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
	storePool := NewStorePool(g, TestTimeUntilStoreDeadOff, stopper)
	a := MakeAllocator(storePool)
	return stopper, g, storePool, a
}

// mockStorePool sets up a collection of a alive and dead stores in the
// store pool for testing purposes.
func mockStorePool(storePool *StorePool, aliveStoreIDs, deadStoreIDs []proto.StoreID) {
	storePool.mu.Lock()
	defer storePool.mu.Unlock()

	storePool.stores = make(map[proto.StoreID]*storeDetail)
	for _, storeID := range aliveStoreIDs {
		storePool.stores[storeID] = &storeDetail{
			dead: false,
			desc: proto.StoreDescriptor{StoreID: storeID},
		}
	}
	for _, storeID := range deadStoreIDs {
		storePool.stores[storeID] = &storeDetail{
			dead: true,
			desc: proto.StoreDescriptor{StoreID: storeID},
		}
	}
}

func TestAllocatorSimpleRetrieval(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper, g, _, a := createTestAllocator()
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(singleStore, t)
	result, err := a.AllocateTarget(simpleZoneConfig.ReplicaAttrs[0], []proto.Replica{}, false, nil)
	if err != nil {
		t.Errorf("Unable to perform allocation: %v", err)
	}
	if result.Node.NodeID != 1 || result.StoreID != 1 {
		t.Errorf("expected NodeID 1 and StoreID 1: %+v", result)
	}
}

func TestAllocatorNoAvailableDisks(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper, _, _, a := createTestAllocator()
	defer stopper.Stop()
	result, err := a.AllocateTarget(simpleZoneConfig.ReplicaAttrs[0], []proto.Replica{}, false, nil)
	if result != nil {
		t.Errorf("expected nil result: %+v", result)
	}
	if err == nil {
		t.Errorf("allocation succeeded despite there being no available disks: %v", result)
	}
}

func TestAllocatorThreeDisksSameDC(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper, g, _, a := createTestAllocator()
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(sameDCStores, t)
	result1, err := a.AllocateTarget(multiDisksConfig.ReplicaAttrs[0], []proto.Replica{}, false, nil)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	if result1.StoreID != 1 && result1.StoreID != 2 {
		t.Errorf("Expected store 1 or 2; got %+v", result1)
	}
	exReplicas := []proto.Replica{
		{
			NodeID:  result1.Node.NodeID,
			StoreID: result1.StoreID,
		},
	}
	result2, err := a.AllocateTarget(multiDisksConfig.ReplicaAttrs[1], exReplicas, false, nil)
	if err != nil {
		t.Errorf("Unable to perform allocation: %v", err)
	}
	if result2.StoreID != 3 && result2.StoreID != 4 {
		t.Errorf("Expected store 3 or 4; got %+v", result2)
	}
	if result1.Node.NodeID == result2.Node.NodeID {
		t.Errorf("Expected node ids to be different %+v vs %+v", result1, result2)
	}
	result3, err := a.AllocateTarget(multiDisksConfig.ReplicaAttrs[2], []proto.Replica{}, false, nil)
	if err != nil {
		t.Errorf("Unable to perform allocation: %v", err)
	}
	if result3.Node.NodeID != 4 || result3.StoreID != 5 {
		t.Errorf("Expected node 4, store 5; got %+v", result3)
	}
}

func TestAllocatorTwoDatacenters(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper, g, _, a := createTestAllocator()
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(multiDCStores, t)
	result1, err := a.AllocateTarget(multiDCConfig.ReplicaAttrs[0], []proto.Replica{}, false, nil)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	result2, err := a.AllocateTarget(multiDCConfig.ReplicaAttrs[1], []proto.Replica{}, false, nil)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	if result1.Node.NodeID != 1 || result2.Node.NodeID != 2 {
		t.Errorf("Expected nodes 1 & 2: %+v vs %+v", result1.Node, result2.Node)
	}
	// Verify that no result is forthcoming if we already have a replica.
	_, err = a.AllocateTarget(multiDCConfig.ReplicaAttrs[1], []proto.Replica{
		{
			NodeID:  result2.Node.NodeID,
			StoreID: result2.StoreID,
		},
	}, false, nil)
	if err == nil {
		t.Errorf("expected error on allocation without available stores")
	}
}

func TestAllocatorExistingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper, g, _, a := createTestAllocator()
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(sameDCStores, t)
	result, err := a.AllocateTarget(multiDisksConfig.ReplicaAttrs[1], []proto.Replica{
		{
			NodeID:  2,
			StoreID: 2,
		},
	}, false, nil)
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
	defer leaktest.AfterTest(t)
	stopper, g, _, a := createTestAllocator()
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(multiDCStores, t)

	testCases := []struct {
		required         []string // attribute strings
		existing         []int    // existing store/node ID
		relaxConstraints bool     // allow constraints to be relaxed?
		expID            int      // expected store/node ID on allocate
		expErr           bool
	}{
		// The two stores in the system have attributes:
		//  storeID=1 {"a", "ssd"}
		//  storeID=2 {"b", "ssd"}
		{[]string{"a", "ssd"}, []int{}, true, 1, false},
		{[]string{"a", "ssd"}, []int{1}, true, 2, false},
		{[]string{"a", "ssd"}, []int{1}, false, 0, true},
		{[]string{"a", "ssd"}, []int{1, 2}, true, 0, true},
		{[]string{"b", "ssd"}, []int{}, true, 2, false},
		{[]string{"b", "ssd"}, []int{1}, true, 2, false},
		{[]string{"b", "ssd"}, []int{2}, false, 0, true},
		{[]string{"b", "ssd"}, []int{2}, true, 1, false},
		{[]string{"b", "ssd"}, []int{1, 2}, true, 0, true},
		{[]string{"b", "hdd"}, []int{}, true, 2, false},
		{[]string{"b", "hdd"}, []int{2}, true, 1, false},
		{[]string{"b", "hdd"}, []int{2}, false, 0, true},
		{[]string{"b", "hdd"}, []int{1, 2}, true, 0, true},
		{[]string{"b", "ssd", "gpu"}, []int{}, true, 2, false},
		{[]string{"b", "hdd", "gpu"}, []int{}, true, 2, false},
	}
	for i, test := range testCases {
		var existing []proto.Replica
		for _, id := range test.existing {
			existing = append(existing, proto.Replica{NodeID: proto.NodeID(id), StoreID: proto.StoreID(id)})
		}
		result, err := a.AllocateTarget(proto.Attributes{Attrs: test.required}, existing, test.relaxConstraints, nil)
		if haveErr := (err != nil); haveErr != test.expErr {
			t.Errorf("%d: expected error %t; got %t: %s", i, test.expErr, haveErr, err)
		} else if err == nil && proto.StoreID(test.expID) != result.StoreID {
			t.Errorf("%d: expected result to have store %d; got %+v", i, test.expID, result)
		}
	}
}

// TestAllocatorRandomAllocation verifies that allocations bias
// towards least loaded stores.
func TestAllocatorRandomAllocation(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper, g, _, a := createTestAllocator()
	defer stopper.Stop()

	stores := []*proto.StoreDescriptor{
		{
			StoreID:  1,
			Node:     proto.NodeDescriptor{NodeID: 1},
			Capacity: proto.StoreCapacity{Capacity: 200, Available: 200},
		},
		{
			StoreID:  2,
			Node:     proto.NodeDescriptor{NodeID: 2},
			Capacity: proto.StoreCapacity{Capacity: 200, Available: 150},
		},
		{
			StoreID:  3,
			Node:     proto.NodeDescriptor{NodeID: 3},
			Capacity: proto.StoreCapacity{Capacity: 200, Available: 50},
		},
		{
			StoreID:  4,
			Node:     proto.NodeDescriptor{NodeID: 4},
			Capacity: proto.StoreCapacity{Capacity: 200, Available: 0},
		},
	}
	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	// Every allocation will randomly choose 3 of the 4, meaning either
	// store 1 or store 2 will be chosen, as the least loaded of the
	// three random choices is returned.
	for i := 0; i < 10; i++ {
		result, err := a.AllocateTarget(proto.Attributes{}, []proto.Replica{}, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		if result.StoreID != 1 && result.StoreID != 2 {
			t.Errorf("expected store 1 or 2; got %d", result.StoreID)
		}
	}
}

// TestAllocatorRebalance verifies that rebalance targets are chosen
// randomly from amongst stores over the minAvailCapacityThreshold.
func TestAllocatorRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper, g, _, a := createTestAllocator()
	defer stopper.Stop()

	stores := []*proto.StoreDescriptor{
		{
			StoreID:  1,
			Node:     proto.NodeDescriptor{NodeID: 1},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 100},
		},
		{
			StoreID:  2,
			Node:     proto.NodeDescriptor{NodeID: 2},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 50},
		},
		{
			StoreID:  3,
			Node:     proto.NodeDescriptor{NodeID: 3},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 100 - int64(100*maxFractionUsedThreshold)},
		},
		{
			StoreID:  4,
			Node:     proto.NodeDescriptor{NodeID: 4},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: (100 - int64(100*maxFractionUsedThreshold)) / 2},
		},
	}
	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	// Every rebalance target must be either stores 1 or 2.
	for i := 0; i < 10; i++ {
		result := a.RebalanceTarget(proto.Attributes{}, []proto.Replica{})
		if result == nil {
			t.Fatal("nil result")
		}
		if result.StoreID != 1 && result.StoreID != 2 {
			t.Errorf("%d: expected store 1 or 2; got %d", i, result.StoreID)
		}
	}

	// Verify shouldRebalance results.
	for i, store := range stores {
		result := a.shouldRebalance(store)
		if expResult := (i >= 2); expResult != result {
			t.Errorf("%d: expected rebalance %t; got %t", i, expResult, result)
		}
	}
}

// TestAllocatorRebalance verifies that only rebalance targets within
// a standard deviation of the mean are chosen.
func TestAllocatorRebalanceByCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper, g, _, a := createTestAllocator()
	defer stopper.Stop()

	stores := []*proto.StoreDescriptor{
		{
			StoreID:  1,
			Node:     proto.NodeDescriptor{NodeID: 1},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 50},
		},
		{
			StoreID:  2,
			Node:     proto.NodeDescriptor{NodeID: 2},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 50},
		},
		{
			StoreID:  3,
			Node:     proto.NodeDescriptor{NodeID: 3},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 50},
		},
		{
			StoreID:  4,
			Node:     proto.NodeDescriptor{NodeID: 4},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 80},
		},
	}
	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	// Every rebalance target must be store 4 (if not nil).
	for i := 0; i < 10; i++ {
		result := a.RebalanceTarget(proto.Attributes{}, []proto.Replica{})
		if result != nil && result.StoreID != 4 {
			t.Errorf("expected store 4; got %d", result.StoreID)
		}
	}

	// Verify shouldRebalance results.
	for i, store := range stores {
		result := a.shouldRebalance(store)
		if expResult := (i < 3); expResult != result {
			t.Errorf("%d: expected rebalance %t; got %t", i, expResult, result)
		}
	}
}

// TestAllocatorRebalanceByCount verifies that rebalance targets are
// chosen by range counts in the event that available capacities
// exceed the maxAvailCapacityThreshold.
func TestAllocatorRebalanceByCount(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper, g, _, a := createTestAllocator()
	defer stopper.Stop()

	// Setup the stores so that only one is below the standard deviation threshold.
	stores := []*proto.StoreDescriptor{
		{
			StoreID:  1,
			Node:     proto.NodeDescriptor{NodeID: 1},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 100, RangeCount: 10},
		},
		{
			StoreID:  2,
			Node:     proto.NodeDescriptor{NodeID: 2},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 99, RangeCount: 10},
		},
		{
			StoreID:  3,
			Node:     proto.NodeDescriptor{NodeID: 3},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 98, RangeCount: 10},
		},
		{
			StoreID:  4,
			Node:     proto.NodeDescriptor{NodeID: 4},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 97, RangeCount: 5},
		},
	}
	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	// Every rebalance target must be store 4 (or nil for case of missing the only option).
	for i := 0; i < 10; i++ {
		result := a.RebalanceTarget(proto.Attributes{}, []proto.Replica{})
		if result != nil && result.StoreID != 4 {
			t.Errorf("expected store 4; got %d", result.StoreID)
		}
	}

	// Verify shouldRebalance results.
	for i, store := range stores {
		result := a.shouldRebalance(store)
		if expResult := (i < 3); expResult != result {
			t.Errorf("%d: expected rebalance %t; got %t", i, expResult, result)
		}
	}
}

// TestAllocatorRemoveTarget verifies that the replica chosen by RemoveTarget is
// the one with the lowest capacity.
func TestAllocatorRemoveTarget(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper, g, _, a := createTestAllocator()
	defer stopper.Stop()

	// List of replicas that will be passed to RemoveTarget
	replicas := []proto.Replica{
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

	// Setup the stores so that store 3 is the worst candidate.
	stores := []*proto.StoreDescriptor{
		{
			StoreID:  1,
			Node:     proto.NodeDescriptor{NodeID: 1},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 100, RangeCount: 10},
		},
		{
			StoreID:  2,
			Node:     proto.NodeDescriptor{NodeID: 2},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 80, RangeCount: 10},
		},
		{
			StoreID:  3,
			Node:     proto.NodeDescriptor{NodeID: 3},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 60, RangeCount: 10},
		},
		{
			StoreID:  4,
			Node:     proto.NodeDescriptor{NodeID: 4},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 65, RangeCount: 5},
		},
	}
	sg := gossiputil.NewStoreGossiper(g)
	sg.GossipStores(stores, t)

	targetRepl, err := a.RemoveTarget(replicas)
	if err != nil {
		t.Fatal(err)
	}
	if a, e := targetRepl, replicas[2]; a != e {
		t.Fatalf("RemoveTarget did not select expected replica; expected %v, got %v", e, a)
	}

	// Setup the stores again so that store 2 is the worst, but with very low
	// used capacity to force the range count criteria to be used.
	stores = []*proto.StoreDescriptor{
		{
			StoreID:  1,
			Node:     proto.NodeDescriptor{NodeID: 1},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 100, RangeCount: 10},
		},
		{
			StoreID:  2,
			Node:     proto.NodeDescriptor{NodeID: 2},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 100, RangeCount: 18},
		},
		{
			StoreID:  3,
			Node:     proto.NodeDescriptor{NodeID: 3},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 100, RangeCount: 10},
		},
		{
			StoreID:  4,
			Node:     proto.NodeDescriptor{NodeID: 4},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: 100, RangeCount: 5},
		},
	}
	sg.GossipStores(stores, t)

	targetRepl, err = a.RemoveTarget(replicas)
	if err != nil {
		t.Fatal(err)
	}
	if a, e := targetRepl, replicas[1]; a != e {
		t.Fatalf("RemoveTarget did not select expected replica; expected %v, got %v", e, a)
	}
}

func TestAllocatorComputeAction(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper, _, sp, a := createTestAllocator()
	defer stopper.Stop()

	// Set up seven stores. Stores six and seven are marked as dead.
	mockStorePool(sp, []proto.StoreID{1, 2, 3, 4, 5}, []proto.StoreID{6, 7})

	// Each test case should describe a repair situation which has a lower
	// priority than the previous test case.
	testCases := []struct {
		zone           config.ZoneConfig
		desc           proto.RangeDescriptor
		expectedAction AllocatorAction
	}{
		// Needs Three replicas, two are on dead stores.
		{
			zone: config.ZoneConfig{
				ReplicaAttrs: []proto.Attributes{
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
				},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: proto.RangeDescriptor{
				Replicas: []proto.Replica{
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
			expectedAction: aaRemoveDead,
		},
		// Needs Three replicas, one is on a dead store.
		{
			zone: config.ZoneConfig{
				ReplicaAttrs: []proto.Attributes{
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
				},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: proto.RangeDescriptor{
				Replicas: []proto.Replica{
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
			expectedAction: aaRemoveDead,
		},
		// Needs five replicas, one is on a dead store.
		{
			zone: config.ZoneConfig{
				ReplicaAttrs: []proto.Attributes{
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
				},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: proto.RangeDescriptor{
				Replicas: []proto.Replica{
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
			expectedAction: aaRemoveDead,
		},
		// Needs Three replicas, have two
		{
			zone: config.ZoneConfig{
				ReplicaAttrs: []proto.Attributes{
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
				},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: proto.RangeDescriptor{
				Replicas: []proto.Replica{
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
			expectedAction: aaAdd,
		},
		// Needs Five replicas, have four.
		{
			zone: config.ZoneConfig{
				ReplicaAttrs: []proto.Attributes{
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
				},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: proto.RangeDescriptor{
				Replicas: []proto.Replica{
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
			expectedAction: aaAdd,
		},
		// Need three replicas, have four.
		{
			zone: config.ZoneConfig{
				ReplicaAttrs: []proto.Attributes{
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
				},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: proto.RangeDescriptor{
				Replicas: []proto.Replica{
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
			expectedAction: aaRemove,
		},
		// Need three replicas, have five.
		{
			zone: config.ZoneConfig{
				ReplicaAttrs: []proto.Attributes{
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
				},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: proto.RangeDescriptor{
				Replicas: []proto.Replica{
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
			expectedAction: aaRemove,
		},
		// Three replicas have three, none of the replicas in the store pool.
		{
			zone: config.ZoneConfig{
				ReplicaAttrs: []proto.Attributes{
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
				},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: proto.RangeDescriptor{
				Replicas: []proto.Replica{
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
			expectedAction: aaNoop,
		},
		// Three replicas have three.
		{
			zone: config.ZoneConfig{
				ReplicaAttrs: []proto.Attributes{
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
					{
						Attrs: []string{"us-east"},
					},
				},
				RangeMinBytes: 0,
				RangeMaxBytes: 64000,
			},
			desc: proto.RangeDescriptor{
				Replicas: []proto.Replica{
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
			expectedAction: aaNoop,
		},
	}

	lastPriority := float64(999999999)
	for i, tcase := range testCases {
		action, priority := a.ComputeAction(tcase.zone, &tcase.desc)
		if tcase.expectedAction != action {
			t.Errorf("Test case %d expected action %d, got action %d", i, tcase.expectedAction, action)
			continue
		}
		if tcase.expectedAction != aaNoop && priority >= lastPriority {
			t.Errorf("Test cases should have descending priority. Case %d had priority %f, previous case had priority %f", i, priority, lastPriority)
		}
		lastPriority = priority
	}
}

type testStore struct {
	proto.StoreDescriptor
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
	// Model a set of stores in a cluster,
	// randomly adding / removing stores and adding bytes.
	g := gossip.New(nil, 0, nil)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	sp := NewStorePool(g, TestTimeUntilStoreDeadOff, stopper)
	alloc := MakeAllocator(sp)
	alloc.randGen = rand.New(rand.NewSource(0))
	alloc.deterministic = true

	var wg sync.WaitGroup
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix), func(_ string, _ []byte) { wg.Done() })

	const generations = 100
	const nodes = 20

	// Initialize testStores.
	var testStores [nodes]testStore
	for i := 0; i < len(testStores); i++ {
		testStores[i].StoreID = proto.StoreID(i)
		testStores[i].Node = proto.NodeDescriptor{NodeID: proto.NodeID(i)}
		testStores[i].Capacity = proto.StoreCapacity{Capacity: 1 << 30, Available: 1 << 30}
	}
	// Initialize the cluster with a single range.
	testStores[0].add(alloc.randGen.Int63n(1 << 20))

	for i := 0; i < generations; i++ {
		// First loop through test stores and add data.
		wg.Add(len(testStores))
		for j := 0; j < len(testStores); j++ {
			// Add a pretend range to the testStore if there's already one.
			if testStores[j].Capacity.RangeCount > 0 {
				testStores[j].add(alloc.randGen.Int63n(1 << 20))
			}
			key := gossip.MakeStoreKey(proto.StoreID(j))
			if err := g.AddInfoProto(key, &testStores[j].StoreDescriptor, 0); err != nil {
				panic(err)
			}
		}
		wg.Wait()

		// Next loop through test stores and maybe rebalance.
		for j := 0; j < len(testStores); j++ {
			ts := &testStores[j]
			if alloc.shouldRebalance(&testStores[j].StoreDescriptor) {
				target := alloc.RebalanceTarget(proto.Attributes{}, []proto.Replica{{NodeID: ts.Node.NodeID, StoreID: ts.StoreID}})
				if target != nil {
					testStores[j].rebalance(&testStores[int(target.StoreID)], alloc.randGen.Int63n(1<<20))
				}
			}
		}

		// Output store capacities as hexidecimal 2-character values.
		if i%(generations/50) == 0 {
			var maxBytes int64
			for j := 0; j < len(testStores); j++ {
				bytes := testStores[j].Capacity.Capacity - testStores[j].Capacity.Available
				if bytes > maxBytes {
					maxBytes = bytes
				}
			}
			if maxBytes > 0 {
				for j := 0; j < len(testStores); j++ {
					endStr := " "
					if j == len(testStores)-1 {
						endStr = ""
					}
					bytes := testStores[j].Capacity.Capacity - testStores[j].Capacity.Available
					fmt.Printf("%03d%s", (999*bytes)/maxBytes, endStr)
				}
				fmt.Printf("\n")
			}
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
	// 999 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 739 000 000
	// 999 107 000 000 000 000 000 000 000 000 177 000 000 000 204 000 000 734 000 000
	// 929 288 000 168 000 057 623 000 114 272 471 000 000 565 385 000 000 999 000 284
	// 683 367 133 087 000 527 381 607 379 380 502 000 188 824 490 295 420 999 000 490
	// 540 443 380 319 000 438 382 534 599 579 602 000 268 859 601 374 450 999 000 532
	// 412 428 539 429 170 332 424 696 505 439 503 691 327 752 427 437 451 999 076 441
	// 496 583 662 586 280 431 499 714 564 578 540 661 431 784 548 516 547 999 329 589
	// 502 563 646 541 430 428 576 693 633 578 537 577 455 803 573 596 528 999 402 639
	// 603 641 764 638 764 521 650 764 713 683 648 652 579 860 610 731 665 999 463 749
	// 615 642 779 688 813 459 650 791 728 702 743 614 526 829 600 767 760 999 497 700
	// 677 677 879 787 867 518 700 852 775 801 793 666 526 820 601 843 767 999 544 772
	// 723 696 866 838 853 589 730 882 800 768 782 695 567 776 656 836 832 999 613 832
	// 830 764 936 879 976 673 824 974 864 825 835 761 703 874 700 909 888 999 635 957
	// 832 766 949 842 995 730 839 965 870 843 790 765 693 931 706 936 936 999 683 948
	// 866 787 990 851 999 780 867 968 892 847 783 787 708 912 768 963 951 954 681 942
	// 844 768 995 866 999 811 836 952 931 849 764 763 716 923 758 980 907 921 705 927
	// 866 780 999 897 979 844 858 975 968 897 762 772 739 939 731 984 909 912 752 931
	// 895 818 999 918 991 884 916 936 942 899 790 780 775 930 779 976 963 893 752 920
	// 832 782 999 865 978 854 908 902 887 886 760 785 747 903 762 924 911 830 750 876
	// 864 804 999 856 987 855 939 911 905 909 735 818 808 909 757 941 956 807 761 875
	// 892 839 999 901 977 871 933 930 931 910 766 812 831 908 768 969 935 861 793 867
	// 908 858 982 911 994 872 947 951 960 935 798 832 835 895 786 999 951 873 790 863
	// 881 863 963 908 991 845 919 941 959 939 801 815 832 876 783 999 959 877 798 822
	// 907 929 962 930 942 904 901 925 955 940 871 985 921 902 873 999 991 894 908 926
	// 892 923 956 939 976 961 904 933 958 942 914 979 925 907 940 999 982 906 929 922
	// 913 928 940 921 930 913 913 929 925 918 920 963 923 923 940 999 928 909 923 948
	// 924 950 910 947 926 918 917 933 944 920 907 917 911 920 914 999 921 909 915 939
	// 908 933 920 946 940 936 935 931 957 924 914 914 915 936 929 999 929 934 924 906
	// 946 943 954 968 961 966 966 935 951 940 935 959 949 950 938 996 941 981 955 999
	// 940 926 938 941 930 935 934 935 958 897 941 955 931 943 944 971 949 941 972 999
	// 950 952 944 957 935 953 951 938 972 932 957 988 943 950 970 961 947 968 970 999
	// 966 959 957 945 956 964 973 947 991 946 966 985 966 980 986 962 967 958 959 999
	// 941 964 956 979 959 983 992 937 999 948 976 961 950 996 987 944 968 949 961 968
	// 982 975 932 950 960 969 955 980 977 944 956 944 920 943 999 940 947 938 937 951
	// 977 970 935 953 960 965 980 957 991 943 938 956 945 971 999 936 939 952 948 944
	// 993 969 987 951 988 979 999 966 983 957 948 971 971 956 990 958 967 955 979 983
	// 969 935 955 952 936 961 979 943 975 954 938 957 967 959 950 977 940 999 956 964
	// 964 953 975 956 923 972 978 952 974 962 939 946 952 947 952 984 944 999 947 974
	// 957 966 969 951 950 977 984 935 940 954 962 956 957 953 952 965 943 999 957 975
	// 974 984 982 953 947 968 999 953 940 956 972 946 973 954 948 958 953 987 970 961
	// 976 980 976 948 946 955 999 963 952 962 962 941 955 946 926 944 952 951 957 944
	// 988 975 951 938 957 972 999 963 973 963 968 948 958 948 945 954 963 943 945 959
	// 999 977 958 930 955 957 993 954 983 955 952 945 953 947 923 963 945 939 972 953
	// 999 970 928 975 945 938 983 956 954 933 939 938 940 921 923 958 935 933 962 938
	// 999 970 915 961 935 923 993 932 929 926 935 925 927 931 917 947 938 929 959 921
	// 999 949 953 933 929 952 984 922 935 943 931 942 932 951 934 957 935 926 947 920
	// 999 957 948 937 943 951 992 926 927 938 930 934 943 953 946 945 938 935 953 941
	// 999 958 962 932 934 938 989 935 930 921 927 935 932 957 926 945 935 921 949 932
	// 989 962 950 942 927 941 999 943 926 936 931 930 926 959 923 943 931 928 951 945
	// 999 960 952 934 933 935 995 939 928 938 924 929 971 946 927 950 932 933 943 947
	// Total bytes=1001793961, ranges=1898
}
