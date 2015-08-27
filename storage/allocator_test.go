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
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/leaktest"
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

// storeGossiper allows tests to push storeDescriptors into gossip and
// synchronize on their callbacks. There can only be one storeGossiper used per
// gossip instance.
type storeGossiper struct {
	g  *gossip.Gossip
	wg sync.WaitGroup
	mu sync.Mutex
}

func newStoreGossiper(g *gossip.Gossip) *storeGossiper {
	sg := &storeGossiper{
		g: g,
	}
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix), func(_ string, _ []byte) { sg.wg.Done() })
	return sg
}

func (sg *storeGossiper) gossipStores(stores []*proto.StoreDescriptor, t *testing.T) {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	sg.wg.Add(len(stores))
	for _, s := range stores {
		keyStoreGossip := gossip.MakeStoreKey(s.StoreID)
		// Gossip store descriptor.
		err := sg.g.AddInfoProto(keyStoreGossip, s, 0)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait for all gossip callbacks to be invoked.
	sg.wg.Wait()
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

func TestAllocatorSimpleRetrieval(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, _, stopper := createTestStore(t)
	defer stopper.Stop()
	newStoreGossiper(s.Gossip()).gossipStores(singleStore, t)
	result, err := s.allocator().AllocateTarget(simpleZoneConfig.ReplicaAttrs[0], []proto.Replica{}, false)
	if err != nil {
		t.Errorf("Unable to perform allocation: %v", err)
	}
	if result.Node.NodeID != 1 || result.StoreID != 1 {
		t.Errorf("expected NodeID 1 and StoreID 1: %+v", result)
	}
}

func TestAllocatorNoAvailableDisks(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, _, stopper := createTestStore(t)
	defer stopper.Stop()
	result, err := s.allocator().AllocateTarget(simpleZoneConfig.ReplicaAttrs[0], []proto.Replica{}, false)
	if result != nil {
		t.Errorf("expected nil result: %+v", result)
	}
	if err == nil {
		t.Errorf("allocation succeeded despite there being no available disks: %v", result)
	}
}

func TestAllocatorThreeDisksSameDC(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, _, stopper := createTestStore(t)
	defer stopper.Stop()
	newStoreGossiper(s.Gossip()).gossipStores(sameDCStores, t)
	result1, err := s.allocator().AllocateTarget(multiDisksConfig.ReplicaAttrs[0], []proto.Replica{}, false)
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
	result2, err := s.allocator().AllocateTarget(multiDisksConfig.ReplicaAttrs[1], exReplicas, false)
	if err != nil {
		t.Errorf("Unable to perform allocation: %v", err)
	}
	if result2.StoreID != 3 && result2.StoreID != 4 {
		t.Errorf("Expected store 3 or 4; got %+v", result2)
	}
	if result1.Node.NodeID == result2.Node.NodeID {
		t.Errorf("Expected node ids to be different %+v vs %+v", result1, result2)
	}
	result3, err := s.allocator().AllocateTarget(multiDisksConfig.ReplicaAttrs[2], []proto.Replica{}, false)
	if err != nil {
		t.Errorf("Unable to perform allocation: %v", err)
	}
	if result3.Node.NodeID != 4 || result3.StoreID != 5 {
		t.Errorf("Expected node 4, store 5; got %+v", result3)
	}
}

func TestAllocatorTwoDatacenters(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, _, stopper := createTestStore(t)
	defer stopper.Stop()
	newStoreGossiper(s.Gossip()).gossipStores(multiDCStores, t)
	result1, err := s.allocator().AllocateTarget(multiDCConfig.ReplicaAttrs[0], []proto.Replica{}, false)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	result2, err := s.allocator().AllocateTarget(multiDCConfig.ReplicaAttrs[1], []proto.Replica{}, false)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	if result1.Node.NodeID != 1 || result2.Node.NodeID != 2 {
		t.Errorf("Expected nodes 1 & 2: %+v vs %+v", result1.Node, result2.Node)
	}
	// Verify that no result is forthcoming if we already have a replica.
	_, err = s.allocator().AllocateTarget(multiDCConfig.ReplicaAttrs[1], []proto.Replica{
		{
			NodeID:  result2.Node.NodeID,
			StoreID: result2.StoreID,
		},
	}, false)
	if err == nil {
		t.Errorf("expected error on allocation without available stores")
	}
}

func TestAllocatorExistingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, _, stopper := createTestStore(t)
	defer stopper.Stop()
	newStoreGossiper(s.Gossip()).gossipStores(sameDCStores, t)
	result, err := s.allocator().AllocateTarget(multiDisksConfig.ReplicaAttrs[1], []proto.Replica{
		{
			NodeID:  2,
			StoreID: 2,
		},
	}, false)
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
	s, _, stopper := createTestStore(t)
	defer stopper.Stop()
	newStoreGossiper(s.Gossip()).gossipStores(multiDCStores, t)

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
		result, err := s.allocator().AllocateTarget(proto.Attributes{Attrs: test.required}, existing, test.relaxConstraints)
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
	s, _, stopper := createTestStore(t)
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
	newStoreGossiper(s.Gossip()).gossipStores(stores, t)

	// Every allocation will randomly choose 3 of the 4, meaning either
	// store 1 or store 2 will be chosen, as the least loaded of the
	// three random choices is returned.
	for i := 0; i < 10; i++ {
		result, err := s.allocator().AllocateTarget(proto.Attributes{}, []proto.Replica{}, false)
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
	s, _, stopper := createTestStore(t)
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
	newStoreGossiper(s.Gossip()).gossipStores(stores, t)

	// Every rebalance target must be either stores 1 or 2.
	for i := 0; i < 10; i++ {
		result := s.allocator().RebalanceTarget(proto.Attributes{}, []proto.Replica{})
		if result == nil {
			t.Fatal("nil result")
		}
		if result.StoreID != 1 && result.StoreID != 2 {
			t.Errorf("%d: expected store 1 or 2; got %d", i, result.StoreID)
		}
	}

	// Verify shouldRebalance results.
	for i, store := range stores {
		result := s.allocator().ShouldRebalance(store)
		if expResult := (i >= 2); expResult != result {
			t.Errorf("%d: expected rebalance %t; got %t", i, expResult, result)
		}
	}
}

// TestAllocatorRebalance verifies that only rebalance targets within
// a standard deviation of the mean are chosen.
func TestAllocatorRebalanceByCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, _, stopper := createTestStore(t)
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
	newStoreGossiper(s.Gossip()).gossipStores(stores, t)

	// Every rebalance target must be store 4 (if not nil).
	for i := 0; i < 10; i++ {
		result := s.allocator().RebalanceTarget(proto.Attributes{}, []proto.Replica{})
		if result != nil && result.StoreID != 4 {
			t.Errorf("expected store 4; got %d", result.StoreID)
		}
	}

	// Verify shouldRebalance results.
	for i, store := range stores {
		result := s.allocator().ShouldRebalance(store)
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
	s, _, stopper := createTestStore(t)
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
	newStoreGossiper(s.Gossip()).gossipStores(stores, t)

	// Every rebalance target must be store 4 (or nil for case of missing the only option).
	for i := 0; i < 10; i++ {
		result := s.allocator().RebalanceTarget(proto.Attributes{}, []proto.Replica{})
		if result != nil && result.StoreID != 4 {
			t.Errorf("expected store 4; got %d", result.StoreID)
		}
	}

	// Verify shouldRebalance results.
	for i, store := range stores {
		result := s.allocator().ShouldRebalance(store)
		if expResult := (i < 3); expResult != result {
			t.Errorf("%d: expected rebalance %t; got %t", i, expResult, result)
		}
	}
}

// TestAllocatorRemoveTarget verifies that the replica chosen by RemoveTarget is
// the one with the lowest capacity.
func TestAllocatorRemoveTarget(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, _, stopper := createTestStore(t)
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
	sg := newStoreGossiper(s.Gossip())
	sg.gossipStores(stores, t)

	targetRepl, err := s.allocator().RemoveTarget(replicas)
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
	sg.gossipStores(stores, t)

	targetRepl, err = s.allocator().RemoveTarget(replicas)
	if err != nil {
		t.Fatal(err)
	}
	if a, e := targetRepl, replicas[1]; a != e {
		t.Fatalf("RemoveTarget did not select expected replica; expected %v, got %v", e, a)
	}
}

func TestAllocatorCapacityGossipUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, _, stopper := createTestStore(t)
	defer stopper.Stop()

	// Order and value of contentsChanged shouldn't matter.
	key := "testkey"
	s.allocator().storeGossipUpdate(key, nil)
	s.allocator().storeGossipUpdate(key, nil)

	expectedKeys := map[string]struct{}{key: {}}
	s.allocator().Lock()
	actualKeys := s.allocator().storeKeys
	s.allocator().Unlock()

	if !reflect.DeepEqual(expectedKeys, actualKeys) {
		t.Errorf("expected to fetch %+v, instead %+v", expectedKeys, actualKeys)
	}
}

func TestAllocatorGetStoreList(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, _, stopper := createTestStore(t)
	defer stopper.Stop()
	required := []string{"ssd", "dc"}
	// Nothing yet.
	if sl := s.allocator().getStoreList(proto.Attributes{Attrs: required}); len(sl.stores) != 0 {
		t.Errorf("expected no stores, instead %+v", sl.stores)
	}

	matchingStore := proto.StoreDescriptor{
		StoreID: 1,
		Node:    proto.NodeDescriptor{NodeID: 1},
		Attrs:   proto.Attributes{Attrs: required},
	}
	supersetStore := proto.StoreDescriptor{
		StoreID: 2,
		Node:    proto.NodeDescriptor{NodeID: 1},
		Attrs:   proto.Attributes{Attrs: append(required, "db")},
	}
	unmatchingStore := proto.StoreDescriptor{
		StoreID: 3,
		Node:    proto.NodeDescriptor{NodeID: 1},
		Attrs:   proto.Attributes{Attrs: []string{"ssd", "otherdc"}},
	}
	emptyStore := proto.StoreDescriptor{
		StoreID: 4,
		Node:    proto.NodeDescriptor{NodeID: 1},
		Attrs:   proto.Attributes{},
	}

	newStoreGossiper(s.Gossip()).gossipStores([]*proto.StoreDescriptor{
		&matchingStore,
		&supersetStore,
		&unmatchingStore,
		&emptyStore,
	}, t)
	expected := []string{matchingStore.Attrs.SortedString(), supersetStore.Attrs.SortedString()}
	sort.Strings(expected)

	verifyFunc := func() {
		var actual []string
		sl := s.allocator().getStoreList(proto.Attributes{Attrs: required})
		for _, store := range sl.stores {
			actual = append(actual, store.Attrs.SortedString())
		}
		sort.Strings(actual)
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected %+v Attrs, instead %+v", expected, actual)
		}
	}

	// Check expected matches actual result.
	verifyFunc()

	// NOTE: this is cheating, but we're going to directly modify the
	// attributes of unmatchingStore to make it match and verify that
	// we still don't get the unmatching store because the storeList
	// result is cached by the allocator.
	unmatchingStore.Attrs.Attrs = append([]string(nil), required...)
	verifyFunc()
}

// TestAllocatorGarbageCollection ensures removal of capacity gossip keys in
// the map, if their gossip does not exist when we try to retrieve them.
func TestAllocatorGarbageCollection(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, _, stopper := createTestStore(t)
	defer stopper.Stop()

	s.allocator().storeKeys = map[string]struct{}{
		"key0": {},
		"key1": {},
	}
	required := []string{}

	// No gossip added for either key, so they should be removed.
	sl := s.allocator().getStoreList(proto.Attributes{Attrs: required})
	if len(sl.stores) != 0 {
		t.Errorf("expected no stores found, instead %+v", sl.stores)
	}
	if len(s.allocator().storeKeys) != 0 {
		t.Errorf("expected keys to be cleared, instead are %+v", s.allocator().storeKeys)
	}
}

type testStore struct {
	proto.StoreDescriptor
}

func (ts *testStore) Add(bytes int64) {
	ts.Capacity.RangeCount++
	ts.Capacity.Available -= bytes
}

func (ts *testStore) Rebalance(ots *testStore, bytes int64) {
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
	alloc := newAllocator(g)
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
	testStores[0].Add(alloc.randGen.Int63n(1 << 20))

	for i := 0; i < generations; i++ {
		// First loop through test stores and add data.
		wg.Add(len(testStores))
		for j := 0; j < len(testStores); j++ {
			// Add a pretend range to the testStore if there's already one.
			if testStores[j].Capacity.RangeCount > 0 {
				testStores[j].Add(alloc.randGen.Int63n(1 << 20))
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
			if alloc.ShouldRebalance(&testStores[j].StoreDescriptor) {
				target := alloc.RebalanceTarget(proto.Attributes{}, []proto.Replica{{NodeID: ts.Node.NodeID, StoreID: ts.StoreID}})
				if target != nil {
					testStores[j].Rebalance(&testStores[int(target.StoreID)], alloc.randGen.Int63n(1<<20))
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
	// 999 000 000 000 000 000 000 739 000 000 000 000 000 000 000 000 000 000 000 000
	// 999 107 000 000 204 000 000 375 000 000 000 000 000 000 000 000 000 000 536 000
	// 999 310 000 262 872 000 000 208 000 705 000 526 000 000 439 000 000 607 933 000
	// 812 258 000 220 999 673 402 480 000 430 516 374 000 431 318 000 551 714 917 000
	// 582 625 185 334 720 589 647 619 000 300 483 352 279 502 208 665 816 684 999 374
	// 751 617 771 542 738 676 665 525 309 435 612 449 457 616 306 837 993 754 999 445
	// 759 659 828 478 693 622 594 591 349 458 630 538 526 613 462 827 879 787 999 550
	// 861 658 828 559 801 660 681 560 487 529 652 686 642 716 575 999 989 875 989 581
	// 775 647 724 557 779 662 670 494 535 502 681 676 624 695 561 961 999 772 888 592
	// 856 712 753 661 767 658 717 606 529 615 755 699 672 700 576 955 999 755 861 671
	// 882 735 776 685 844 643 740 578 610 688 787 741 661 767 587 999 955 809 803 731
	// 958 716 789 719 861 689 821 608 634 724 800 782 694 799 619 994 999 851 812 818
	// 949 726 788 664 873 633 749 599 680 714 790 728 663 842 628 999 978 816 823 791
	// 923 698 792 712 816 605 774 651 661 728 802 718 670 819 714 999 966 801 829 791
	// 962 779 847 737 900 675 811 691 745 778 835 812 680 894 790 999 989 872 923 799
	// 967 812 826 772 891 685 828 683 761 808 864 820 643 873 783 969 999 873 910 781
	// 923 813 837 739 867 672 792 664 773 772 879 803 610 845 740 957 999 867 912 732
	// 952 803 866 759 881 655 765 668 803 772 929 762 601 844 751 973 999 892 864 731
	// 970 777 867 800 859 639 774 662 787 760 906 751 595 854 732 989 999 853 859 762
	// 943 776 872 787 861 686 780 663 789 793 926 784 612 832 733 999 968 868 827 767
	// 914 801 912 802 878 704 800 685 818 808 939 759 627 844 717 999 976 872 828 757
	// 935 806 911 797 887 710 798 711 826 824 938 775 614 870 716 999 986 886 803 767
	// 991 851 898 856 872 795 828 782 826 852 963 797 710 868 775 994 999 923 896 794
	// 999 924 866 877 884 883 886 836 846 869 953 851 762 887 858 985 949 900 917 836
	// 999 910 887 878 897 890 906 868 906 903 983 947 801 895 913 976 924 890 904 898
	// 955 884 888 916 886 879 901 872 898 883 999 874 829 888 892 937 918 889 891 862
	// 974 952 957 990 950 976 945 946 980 961 999 975 942 926 957 994 965 946 960 960
	// 949 929 952 999 929 961 943 946 993 918 984 961 952 919 953 950 952 941 949 934
	// 907 999 916 935 903 903 909 907 960 939 973 912 901 885 916 910 941 911 906 913
	// 939 999 948 948 945 962 951 954 952 964 996 942 975 962 962 956 971 969 975 969
	// 940 974 964 947 971 975 949 954 953 970 992 971 981 973 948 962 999 969 978 975
	// 950 971 953 938 962 967 930 964 953 978 999 945 974 972 951 950 998 951 949 962
	// 934 946 943 936 942 949 929 956 928 970 989 944 945 923 987 927 999 942 931 944
	// 939 957 942 958 951 970 937 946 930 950 940 959 963 937 973 943 999 931 949 940
	// 933 935 945 929 933 960 937 935 919 918 930 931 950 924 969 935 999 943 949 926
	// 959 941 948 952 948 957 936 937 943 930 955 962 953 949 980 948 999 934 980 942
	// 950 973 954 962 949 964 935 949 925 936 951 962 979 962 999 942 990 948 969 959
	// 937 993 958 949 960 960 942 954 969 950 951 952 974 970 999 927 979 964 975 944
	// 981 986 971 968 964 984 954 959 985 979 966 963 994 963 999 970 991 971 988 965
	// 967 997 961 957 959 985 956 940 955 955 957 955 970 952 979 964 999 951 960 968
	// 937 969 931 950 945 954 932 925 954 946 944 926 955 938 957 949 999 934 947 938
	// 958 967 954 955 971 973 946 934 979 947 944 958 954 954 960 948 999 936 960 951
	// 950 948 940 958 937 955 928 927 953 923 935 939 934 921 934 934 999 922 940 938
	// 960 960 929 962 955 955 926 935 957 928 939 941 938 926 941 924 999 923 957 942
	// 979 958 947 987 980 972 945 943 984 939 951 943 944 946 942 942 999 928 970 943
	// 981 941 931 961 969 962 927 935 985 925 964 945 946 939 946 938 999 933 964 928
	// 980 944 929 970 973 955 942 937 977 920 955 929 937 946 935 933 999 947 956 926
	// 980 948 926 981 938 939 936 936 963 949 965 935 943 946 933 933 999 947 955 943
	// 968 959 945 941 929 926 924 941 970 951 959 941 924 952 931 943 999 941 951 950
	// 961 946 930 923 933 932 953 937 954 940 964 944 931 952 939 935 999 936 945 948
	// Total bytes=996294324, ranges=1897
}
