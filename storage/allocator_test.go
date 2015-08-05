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

func gossipStores(g *gossip.Gossip, stores []*proto.StoreDescriptor, t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(len(stores))
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyCapacityPrefix), func(_ string, _ bool) { wg.Done() })

	for _, s := range stores {
		keyMaxCapacity := gossip.MakeCapacityKey(s.Node.NodeID, s.StoreID)
		// Gossip store descriptor.
		err := g.AddInfo(keyMaxCapacity, *s, 0)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait for all gossip callbacks to be invoked.
	wg.Wait()
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
	gossipStores(s.Gossip(), singleStore, t)
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
	gossipStores(s.Gossip(), sameDCStores, t)
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
	gossipStores(s.Gossip(), multiDCStores, t)
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
	gossipStores(s.Gossip(), sameDCStores, t)
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
	gossipStores(s.Gossip(), multiDCStores, t)

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
	gossipStores(s.Gossip(), stores, t)

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
	gossipStores(s.Gossip(), stores, t)

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
	gossipStores(s.Gossip(), stores, t)

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
	gossipStores(s.Gossip(), stores, t)

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

func TestAllocatorCapacityGossipUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, _, stopper := createTestStore(t)
	defer stopper.Stop()

	// Order and value of contentsChanged shouldn't matter.
	key := "testkey"
	s.allocator().capacityGossipUpdate(key, true)
	s.allocator().capacityGossipUpdate(key, false)

	expectedKeys := map[string]struct{}{key: {}}
	s.allocator().Lock()
	actualKeys := s.allocator().capacityKeys
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

	gossipStores(s.Gossip(), []*proto.StoreDescriptor{
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

	s.allocator().capacityKeys = map[string]struct{}{
		"key0": {},
		"key1": {},
	}
	required := []string{}

	// No gossip added for either key, so they should be removed.
	sl := s.allocator().getStoreList(proto.Attributes{Attrs: required})
	if len(sl.stores) != 0 {
		t.Errorf("expected no stores found, instead %+v", sl.stores)
	}
	if len(s.allocator().capacityKeys) != 0 {
		t.Errorf("expected keys to be cleared, instead are %+v", s.allocator().capacityKeys)
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
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyCapacityPrefix), func(_ string, _ bool) { wg.Done() })

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
			key := gossip.MakeCapacityKey(proto.NodeID(j), proto.StoreID(j))
			if err := g.AddInfo(key, testStores[j].StoreDescriptor, 0); err != nil {
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
	// 999 000 000 000 204 000 000 375 000 000 107 000 000 000 000 000 000 000 000 536
	// 942 000 000 463 140 000 000 646 000 288 288 000 442 000 058 647 000 000 316 999
	// 880 000 412 630 365 745 445 565 122 407 380 570 276 000 271 709 000 718 299 999
	// 925 000 667 600 555 975 704 552 272 491 773 890 584 000 407 974 000 930 476 999
	// 990 967 793 579 493 999 698 453 616 608 777 755 709 425 455 984 483 698 267 931
	// 965 999 869 606 635 908 630 585 567 577 818 870 740 621 550 868 805 790 411 913
	// 953 995 990 624 617 947 562 609 670 658 909 952 835 851 641 958 924 999 526 987
	// 999 923 901 571 687 915 636 636 674 685 831 881 847 820 702 905 897 983 509 981
	// 999 884 809 585 691 826 640 572 748 641 754 887 758 848 643 927 865 897 541 956
	// 999 856 891 594 691 745 602 615 766 663 814 834 719 886 733 925 882 911 593 926
	// 999 890 900 653 707 759 642 697 771 732 851 858 748 869 842 953 903 928 655 923
	// 999 924 909 696 748 797 693 689 806 766 841 902 705 897 874 914 913 916 730 892
	// 999 948 892 704 740 821 685 656 859 772 893 911 690 878 824 935 928 941 741 860
	// 999 948 931 697 770 782 697 666 893 761 944 869 658 902 816 925 923 983 742 831
	// 999 878 901 736 750 737 677 647 869 731 930 825 631 880 775 947 949 930 687 810
	// 999 890 910 764 778 757 709 663 849 777 964 837 672 891 814 978 944 946 721 868
	// 985 895 968 806 791 791 720 694 883 819 999 847 652 888 790 995 950 947 692 843
	// 960 903 956 794 815 779 746 706 891 824 958 830 665 886 757 999 931 969 701 861
	// 999 928 954 805 807 822 764 734 910 829 952 827 678 927 785 980 936 962 677 836
	// 999 903 924 800 769 822 776 730 886 815 935 781 668 890 805 948 929 965 676 837
	// 999 926 935 836 782 836 809 756 897 835 937 781 690 894 804 979 951 978 667 832
	// 999 937 936 875 843 872 854 793 908 873 950 808 714 901 860 981 975 962 693 866
	// 988 957 938 898 922 912 916 886 905 912 964 867 764 915 911 992 999 985 776 896
	// 945 959 922 910 937 913 938 944 957 921 993 916 898 957 928 999 976 997 855 957
	// 980 986 944 956 963 920 966 967 999 966 991 956 981 973 955 998 990 954 994 981
	// 956 985 942 945 950 900 933 949 981 969 946 935 963 951 931 999 936 941 972 963
	// 940 999 964 949 941 974 967 937 970 975 965 951 976 968 949 993 944 949 977 964
	// 926 999 973 932 944 952 933 944 963 965 927 940 964 960 938 995 932 935 968 951
	// 907 999 919 957 941 958 934 935 930 941 940 926 966 933 920 973 937 923 938 946
	// 924 999 914 963 976 945 911 936 929 951 930 930 972 935 941 977 932 960 939 958
	// 942 999 950 961 987 942 928 945 938 941 939 936 985 937 969 985 952 958 957 948
	// 956 999 950 947 943 939 949 934 929 935 940 942 943 957 988 974 933 936 938 951
	// 967 990 950 949 964 952 951 922 943 940 954 956 962 946 982 999 945 949 940 954
	// 970 999 952 959 970 955 957 974 937 965 968 947 950 958 947 993 953 938 958 950
	// 945 964 954 963 965 959 967 961 925 978 954 944 968 937 960 999 947 947 961 960
	// 930 957 938 974 956 944 968 930 944 972 930 946 958 974 940 999 961 945 953 947
	// 966 980 954 989 979 960 969 995 961 986 954 980 980 971 968 999 968 977 979 972
	// 963 953 958 986 990 947 973 955 955 983 974 981 961 964 977 999 984 982 966 964
	// 964 968 975 993 999 955 965 958 972 995 978 981 956 966 981 987 978 976 985 966
	// 967 957 954 999 963 940 968 966 941 966 971 969 957 961 949 940 968 963 988 947
	// 951 939 952 980 937 948 964 970 941 965 979 966 941 940 952 938 973 955 999 934
	// 939 958 941 998 942 951 962 942 962 951 972 978 946 935 958 935 950 947 999 953
	// 959 952 938 999 936 957 961 950 937 954 975 971 958 930 938 930 944 939 978 950
	// 957 943 963 999 947 965 953 937 966 953 978 972 963 937 933 945 944 937 979 952
	// 945 951 956 999 926 948 958 923 947 934 951 961 955 941 949 936 945 929 960 947
	// 956 960 975 999 945 977 956 934 954 943 961 956 956 954 960 954 958 929 969 938
	// 947 966 993 999 944 963 942 939 963 935 952 957 968 947 962 946 962 947 959 942
	// 940 961 999 992 935 946 938 932 968 939 957 938 970 949 964 934 948 957 952 939
	// 944 955 999 978 940 932 937 944 957 936 957 945 958 955 947 933 956 948 947 942
	// Total bytes=1003302292, ranges=1899
}
