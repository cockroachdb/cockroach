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
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

var simpleZoneConfig = config.ZoneConfig{
	ReplicaAttrs: []roachpb.Attributes{
		{Attrs: []string{"a", "ssd"}},
	},
}

var multiDisksConfig = config.ZoneConfig{
	ReplicaAttrs: []roachpb.Attributes{
		{Attrs: []string{"a", "ssd"}},
		{Attrs: []string{"a", "hdd"}},
		{Attrs: []string{"a", "mem"}},
	},
}

var multiDCConfig = config.ZoneConfig{
	ReplicaAttrs: []roachpb.Attributes{
		{Attrs: []string{"a", "ssd"}},
		{Attrs: []string{"b", "ssd"}},
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
func createTestAllocator() (*stop.Stopper, *gossip.Gossip, *StorePool, Allocator) {
	stopper := stop.NewStopper()
	rpcContext := rpc.NewContext(&base.Context{}, hlc.NewClock(hlc.UnixNano), stopper)
	g := gossip.New(rpcContext, gossip.TestBootstrap)
	storePool := NewStorePool(g, TestTimeUntilStoreDeadOff, stopper)
	a := MakeAllocator(storePool, RebalancingOptions{AllowRebalance: true})
	return stopper, g, storePool, a
}

// mockStorePool sets up a collection of a alive and dead stores in the
// store pool for testing purposes.
func mockStorePool(storePool *StorePool, aliveStoreIDs, deadStoreIDs []roachpb.StoreID) {
	storePool.mu.Lock()
	defer storePool.mu.Unlock()

	storePool.stores = make(map[roachpb.StoreID]*storeDetail)
	for _, storeID := range aliveStoreIDs {
		storePool.stores[storeID] = &storeDetail{
			dead: false,
			desc: roachpb.StoreDescriptor{StoreID: storeID},
		}
	}
	for _, storeID := range deadStoreIDs {
		storePool.stores[storeID] = &storeDetail{
			dead: true,
			desc: roachpb.StoreDescriptor{StoreID: storeID},
		}
	}
}

func TestAllocatorSimpleRetrieval(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper, g, _, a := createTestAllocator()
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(singleStore, t)
	result, err := a.AllocateTarget(simpleZoneConfig.ReplicaAttrs[0], []roachpb.ReplicaDescriptor{}, false, nil)
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
	result, err := a.AllocateTarget(simpleZoneConfig.ReplicaAttrs[0], []roachpb.ReplicaDescriptor{}, false, nil)
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
	result1, err := a.AllocateTarget(multiDisksConfig.ReplicaAttrs[0], []roachpb.ReplicaDescriptor{}, false, nil)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	if result1.StoreID != 1 && result1.StoreID != 2 {
		t.Errorf("Expected store 1 or 2; got %+v", result1)
	}
	exReplicas := []roachpb.ReplicaDescriptor{
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
	result3, err := a.AllocateTarget(multiDisksConfig.ReplicaAttrs[2], []roachpb.ReplicaDescriptor{}, false, nil)
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
	result1, err := a.AllocateTarget(multiDCConfig.ReplicaAttrs[0], []roachpb.ReplicaDescriptor{}, false, nil)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	result2, err := a.AllocateTarget(multiDCConfig.ReplicaAttrs[1], []roachpb.ReplicaDescriptor{}, false, nil)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	if result1.Node.NodeID != 1 || result2.Node.NodeID != 2 {
		t.Errorf("Expected nodes 1 & 2: %+v vs %+v", result1.Node, result2.Node)
	}
	// Verify that no result is forthcoming if we already have a replica.
	_, err = a.AllocateTarget(multiDCConfig.ReplicaAttrs[1], []roachpb.ReplicaDescriptor{
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
	result, err := a.AllocateTarget(multiDisksConfig.ReplicaAttrs[1], []roachpb.ReplicaDescriptor{
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
		var existing []roachpb.ReplicaDescriptor
		for _, id := range test.existing {
			existing = append(existing, roachpb.ReplicaDescriptor{NodeID: roachpb.NodeID(id), StoreID: roachpb.StoreID(id)})
		}
		result, err := a.AllocateTarget(roachpb.Attributes{Attrs: test.required}, existing, test.relaxConstraints, nil)
		if haveErr := (err != nil); haveErr != test.expErr {
			t.Errorf("%d: expected error %t; got %t: %s", i, test.expErr, haveErr, err)
		} else if err == nil && roachpb.StoreID(test.expID) != result.StoreID {
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

	stores := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 200},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 150},
		},
		{
			StoreID:  3,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 50},
		},
		{
			StoreID:  4,
			Node:     roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{Capacity: 200, Available: 0},
		},
	}
	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	// Every allocation will randomly choose 3 of the 4, meaning either
	// store 1 or store 2 will be chosen, as the least loaded of the
	// three random choices is returned.
	for i := 0; i < 10; i++ {
		result, err := a.AllocateTarget(roachpb.Attributes{}, []roachpb.ReplicaDescriptor{}, false, nil)
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

	stores := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 100},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 50},
		},
		{
			StoreID:  3,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 100 - int64(100*maxFractionUsedThreshold)},
		},
		{
			StoreID:  4,
			Node:     roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: (100 - int64(100*maxFractionUsedThreshold)) / 2},
		},
	}
	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	// Every rebalance target must be either stores 1 or 2.
	for i := 0; i < 10; i++ {
		result := a.RebalanceTarget(3, roachpb.Attributes{}, []roachpb.ReplicaDescriptor{})
		if result == nil {
			t.Fatal("nil result")
		}
		if result.StoreID != 1 && result.StoreID != 2 {
			t.Errorf("%d: expected store 1 or 2; got %d", i, result.StoreID)
		}
	}

	// Verify ShouldRebalance results.
	a.options.Deterministic = true
	for i, store := range stores {
		result := a.ShouldRebalance(store.StoreID)
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

	stores := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 50},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 50},
		},
		{
			StoreID:  3,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 50},
		},
		{
			StoreID:  4,
			Node:     roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 80},
		},
	}
	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	// Every rebalance target must be store 4 (if not nil).
	for i := 0; i < 10; i++ {
		result := a.RebalanceTarget(1, roachpb.Attributes{}, []roachpb.ReplicaDescriptor{})
		if result != nil && result.StoreID != 4 {
			t.Errorf("expected store 4; got %d", result.StoreID)
		}
	}

	// Verify ShouldRebalance results.
	a.options.Deterministic = true
	for i, store := range stores {
		result := a.ShouldRebalance(store.StoreID)
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
		result := a.RebalanceTarget(1, roachpb.Attributes{}, []roachpb.ReplicaDescriptor{})
		if result != nil && result.StoreID != 4 {
			t.Errorf("expected store 4; got %d", result.StoreID)
		}
	}

	// Verify ShouldRebalance results.
	a.options.Deterministic = true
	for i, store := range stores {
		result := a.ShouldRebalance(store.StoreID)
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

	// Setup the stores so that store 3 is the worst candidate.
	stores := []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 100, RangeCount: 10},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 80, RangeCount: 10},
		},
		{
			StoreID:  3,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 60, RangeCount: 10},
		},
		{
			StoreID:  4,
			Node:     roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 65, RangeCount: 5},
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
	stores = []*roachpb.StoreDescriptor{
		{
			StoreID:  1,
			Node:     roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 100, RangeCount: 10},
		},
		{
			StoreID:  2,
			Node:     roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 100, RangeCount: 18},
		},
		{
			StoreID:  3,
			Node:     roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 100, RangeCount: 10},
		},
		{
			StoreID:  4,
			Node:     roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 100, RangeCount: 5},
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
	mockStorePool(sp, []roachpb.StoreID{1, 2, 3, 4, 5}, []roachpb.StoreID{6, 7})

	// Each test case should describe a repair situation which has a lower
	// priority than the previous test case.
	testCases := []struct {
		zone           config.ZoneConfig
		desc           roachpb.RangeDescriptor
		expectedAction AllocatorAction
	}{
		// Needs Three replicas, two are on dead stores.
		{
			zone: config.ZoneConfig{
				ReplicaAttrs: []roachpb.Attributes{
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
		// Needs Three replicas, one is on a dead store.
		{
			zone: config.ZoneConfig{
				ReplicaAttrs: []roachpb.Attributes{
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
		// Needs five replicas, one is on a dead store.
		{
			zone: config.ZoneConfig{
				ReplicaAttrs: []roachpb.Attributes{
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
				ReplicaAttrs: []roachpb.Attributes{
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
				ReplicaAttrs: []roachpb.Attributes{
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
				ReplicaAttrs: []roachpb.Attributes{
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
				ReplicaAttrs: []roachpb.Attributes{
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
				ReplicaAttrs: []roachpb.Attributes{
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
				ReplicaAttrs: []roachpb.Attributes{
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
		if tcase.expectedAction != AllocatorNoop && priority >= lastPriority {
			t.Errorf("Test cases should have descending priority. Case %d had priority %f, previous case had priority %f", i, priority, lastPriority)
		}
		lastPriority = priority
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
	// Model a set of stores in a cluster,
	// randomly adding / removing stores and adding bytes.
	g := gossip.New(nil, nil)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	sp := NewStorePool(g, TestTimeUntilStoreDeadOff, stopper)
	alloc := MakeAllocator(sp, RebalancingOptions{AllowRebalance: true, Deterministic: true})
	alloc.randGen = rand.New(rand.NewSource(0))

	var wg sync.WaitGroup
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix), func(_ string, _ []byte) { wg.Done() })

	const generations = 100
	const nodes = 20

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
		// First loop through test stores and add data.
		wg.Add(len(testStores))
		for j := 0; j < len(testStores); j++ {
			// Add a pretend range to the testStore if there's already one.
			if testStores[j].Capacity.RangeCount > 0 {
				testStores[j].add(alloc.randGen.Int63n(1 << 20))
			}
			key := gossip.MakeStoreKey(roachpb.StoreID(j))
			if err := g.AddInfoProto(key, &testStores[j].StoreDescriptor, 0); err != nil {
				panic(err)
			}
		}
		wg.Wait()

		// Next loop through test stores and maybe rebalance.
		for j := 0; j < len(testStores); j++ {
			ts := &testStores[j]
			if alloc.ShouldRebalance(ts.StoreID) {
				target := alloc.RebalanceTarget(ts.StoreID, roachpb.Attributes{}, []roachpb.ReplicaDescriptor{{NodeID: ts.Node.NodeID, StoreID: ts.StoreID}})
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
	// 138 000 000 000 000 000 000 999 000 000 000 000 000 000 000 000 000 000 000 000
	// 922 319 000 000 000 239 000 999 000 000 000 000 000 214 073 000 000 000 190 000
	// 999 505 480 000 634 352 421 644 212 331 396 144 000 242 419 275 000 000 727 028
	// 999 678 908 705 350 558 549 714 651 824 895 694 000 373 610 490 372 106 492 796
	// 932 701 763 999 660 706 571 702 787 945 848 678 062 692 762 413 603 252 513 882
	// 937 656 875 984 734 717 676 685 910 895 847 841 349 754 864 463 722 377 655 999
	// 885 701 805 999 647 744 802 659 778 834 830 725 569 761 922 587 684 458 693 935
	// 813 650 709 931 583 733 843 619 793 881 768 658 565 713 956 598 733 594 656 999
	// 873 727 721 999 544 812 848 666 817 943 831 658 556 769 927 554 799 733 670 869
	// 937 765 827 999 543 875 907 670 929 997 913 768 621 853 922 618 878 832 733 937
	// 902 819 744 988 547 904 922 688 879 999 812 710 554 789 890 591 808 865 658 932
	// 870 873 846 997 596 937 899 765 864 969 855 751 577 824 951 579 858 908 653 999
	// 880 833 856 999 640 918 932 774 920 930 869 739 686 784 853 553 885 941 685 986
	// 874 797 808 999 645 925 928 781 920 956 859 762 678 761 819 627 899 941 725 959
	// 886 801 835 999 638 984 927 825 968 958 860 760 813 716 800 638 908 908 798 945
	// 860 840 836 973 634 999 944 834 977 923 848 769 846 728 836 605 865 915 781 896
	// 859 864 891 993 633 961 999 863 951 885 857 791 864 731 871 656 868 920 790 879
	// 866 845 896 999 688 966 998 904 942 864 861 815 867 756 879 704 919 940 804 888
	// 825 850 876 983 712 945 999 885 943 870 854 838 848 771 825 701 939 940 809 885
	// 821 872 915 999 711 927 968 928 963 898 846 865 863 814 858 719 935 951 818 877
	// 829 868 940 999 729 919 938 911 957 905 846 872 860 844 872 724 920 941 844 832
	// 825 848 901 999 736 882 911 926 937 935 876 901 824 870 892 714 902 927 844 846
	// 837 861 921 999 872 890 875 911 894 939 868 921 871 894 887 740 905 948 881 879
	// 876 879 956 999 893 889 875 910 910 953 880 898 900 895 905 736 920 965 918 916
	// 921 897 909 999 924 907 895 936 955 974 901 902 933 937 929 763 909 997 914 944
	// 930 882 892 995 925 910 907 942 911 952 915 922 936 911 944 819 891 999 906 953
	// 904 867 889 989 913 890 877 932 931 937 936 927 939 915 936 843 901 999 937 915
	// 936 916 872 937 920 901 900 917 928 972 949 936 917 923 934 897 896 999 913 907
	// 979 963 909 954 923 950 953 942 969 999 975 970 925 942 971 948 933 993 958 952
	// 978 950 903 942 905 949 937 927 948 991 980 948 921 929 932 946 920 999 942 937
	// 962 958 937 947 910 967 949 934 945 983 966 951 917 918 957 965 923 999 961 957
	// 972 953 942 921 891 955 949 921 964 977 969 951 927 930 953 953 928 999 950 936
	// 965 978 953 928 896 963 964 928 983 986 948 972 963 921 953 970 928 999 937 941
	// 961 964 936 930 930 969 964 930 964 973 965 983 974 928 958 969 949 999 941 961
	// 977 963 931 955 959 979 970 942 941 965 963 980 951 948 966 971 954 999 962 950
	// 988 966 932 960 951 973 978 942 953 964 967 989 941 959 986 956 950 999 962 954
	// 989 950 955 951 940 985 964 958 941 982 970 981 946 968 999 965 959 984 979 939
	// 976 959 962 956 959 975 955 974 966 988 980 991 956 967 992 953 961 999 959 925
	// 983 979 962 973 970 983 962 970 997 999 987 997 970 969 997 973 972 996 981 940
	// 980 959 958 968 954 983 952 966 973 974 978 981 975 947 995 977 956 999 972 932
	// 960 973 955 982 956 970 944 947 979 978 985 977 962 939 999 986 945 995 965 922
	// 956 983 941 982 957 958 945 950 985 973 987 986 944 947 984 999 948 959 961 914
	// 970 972 946 968 968 978 951 945 980 973 988 981 956 957 967 999 955 945 966 913
	// 967 976 943 974 973 969 951 948 994 963 986 984 949 958 984 999 951 945 980 926
	// 960 955 933 954 973 964 943 929 973 943 988 991 948 959 981 999 950 951 978 927
	// 954 948 912 953 960 968 937 931 977 936 979 978 945 950 970 999 945 949 971 925
	// 953 965 922 947 963 966 933 919 992 943 973 991 933 953 975 999 949 947 970 907
	// 951 983 923 958 979 972 945 936 999 941 982 987 946 960 973 975 959 940 970 925
	// 954 982 909 948 973 967 945 938 999 950 994 976 957 960 970 968 964 952 966 932
	// 956 964 896 944 963 953 924 940 999 936 981 961 939 956 965 951 937 937 959 908
	// Total bytes=1042395713, ranges=1919
}
