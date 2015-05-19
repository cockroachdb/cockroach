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

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

var simpleZoneConfig = proto.ZoneConfig{
	ReplicaAttrs: []proto.Attributes{
		{Attrs: []string{"a", "ssd"}},
	},
}

var multiDisksConfig = proto.ZoneConfig{
	ReplicaAttrs: []proto.Attributes{
		{Attrs: []string{"a", "ssd"}},
		{Attrs: []string{"a", "hdd"}},
		{Attrs: []string{"a", "mem"}},
	},
}

var multiDCConfig = proto.ZoneConfig{
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
	result, err := s._allocator.AllocateTarget(simpleZoneConfig.ReplicaAttrs[0], []proto.Replica{})
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
	result, err := s._allocator.AllocateTarget(simpleZoneConfig.ReplicaAttrs[0], []proto.Replica{})
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
	result1, err := s._allocator.AllocateTarget(multiDisksConfig.ReplicaAttrs[0], []proto.Replica{})
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
			Attrs:   multiDisksConfig.ReplicaAttrs[0],
		},
	}
	result2, err := s._allocator.AllocateTarget(multiDisksConfig.ReplicaAttrs[1], exReplicas)
	if err != nil {
		t.Errorf("Unable to perform allocation: %v", err)
	}
	if result2.StoreID != 3 && result2.StoreID != 4 {
		t.Errorf("Expected store 3 or 4; got %+v", result2)
	}
	if result1.Node.NodeID == result2.Node.NodeID {
		t.Errorf("Expected node ids to be different %+v vs %+v", result1, result2)
	}
	result3, err := s._allocator.AllocateTarget(multiDisksConfig.ReplicaAttrs[2], []proto.Replica{})
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
	result1, err := s._allocator.AllocateTarget(multiDCConfig.ReplicaAttrs[0], []proto.Replica{})
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	result2, err := s._allocator.AllocateTarget(multiDCConfig.ReplicaAttrs[1], []proto.Replica{})
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	if result1.Node.NodeID != 1 || result2.Node.NodeID != 2 {
		t.Errorf("Expected nodes 1 & 2: %+v vs %+v", result1.Node, result2.Node)
	}
	// Verify that no result is forthcoming if we already have a replica.
	_, err = s._allocator.AllocateTarget(multiDCConfig.ReplicaAttrs[1], []proto.Replica{
		{
			NodeID:  result2.Node.NodeID,
			StoreID: result2.StoreID,
			Attrs:   multiDCConfig.ReplicaAttrs[1],
		},
	})
	if err == nil {
		t.Errorf("expected error on allocation without available stores")
	}
}

func TestAllocatorExistingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, _, stopper := createTestStore(t)
	defer stopper.Stop()
	gossipStores(s.Gossip(), sameDCStores, t)
	result, err := s._allocator.AllocateTarget(multiDisksConfig.ReplicaAttrs[1], []proto.Replica{
		{
			NodeID:  2,
			StoreID: 2,
			Attrs:   multiDisksConfig.ReplicaAttrs[0],
		},
	})
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	if result.Node.NodeID != 3 || result.StoreID != 4 {
		t.Errorf("expected result to have node 3 and store 4: %+v", result)
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
		result, err := s._allocator.AllocateTarget(proto.Attributes{}, []proto.Replica{})
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
		result, err := s._allocator.RebalanceTarget(proto.Attributes{}, []proto.Replica{})
		if err != nil {
			t.Fatalf("%d: %s", i, err)
		}
		if result.StoreID != 1 && result.StoreID != 2 {
			t.Errorf("%d: expected store 1 or 2; got %d", i, result.StoreID)
		}
	}

	// Verify shouldRebalance results.
	for i, store := range stores {
		result := s._allocator.ShouldRebalance(store)
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

	// Every rebalance target must be store 4.
	for i := 0; i < 10; i++ {
		result, err := s._allocator.RebalanceTarget(proto.Attributes{}, []proto.Replica{})
		if err != nil {
			t.Fatal(err)
		}
		if result.StoreID != 4 {
			t.Errorf("expected store 4; got %d", result.StoreID)
		}
	}

	// Verify shouldRebalance results.
	for i, store := range stores {
		result := s._allocator.ShouldRebalance(store)
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

	// Every rebalance target must be store 4.
	for i := 0; i < 10; i++ {
		result, err := s._allocator.RebalanceTarget(proto.Attributes{}, []proto.Replica{})
		if err != nil {
			t.Fatal(err)
		}
		if result.StoreID != 4 {
			t.Errorf("expected store 4; got %d", result.StoreID)
		}
	}

	// Verify shouldRebalance results.
	for i, store := range stores {
		result := s._allocator.ShouldRebalance(store)
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
	s._allocator.capacityGossipUpdate(key, true)
	s._allocator.capacityGossipUpdate(key, false)

	expectedKeys := map[string]struct{}{key: struct{}{}}
	s._allocator.Lock()
	actualKeys := s._allocator.capacityKeys
	s._allocator.Unlock()

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
	if sl := s._allocator.getStoreList(proto.Attributes{Attrs: required}); len(sl.stores) != 0 {
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
		sl := s._allocator.getStoreList(proto.Attributes{Attrs: required})
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

	s._allocator.capacityKeys = map[string]struct{}{
		"key0": struct{}{},
		"key1": struct{}{},
	}
	required := []string{}

	// No gossip added for either key, so they should be removed.
	sl := s._allocator.getStoreList(proto.Attributes{Attrs: required})
	if len(sl.stores) != 0 {
		t.Errorf("expected no stores found, instead %+v", sl.stores)
	}
	if len(s._allocator.capacityKeys) != 0 {
		t.Errorf("expected keys to be cleared, instead are %+v", s._allocator.capacityKeys)
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

// ExampleAllocatorRebalancing models a set of stores in a cluster,
// randomly adding / removing stores and adding bytes.
func ExampleAllocatorRebalancing() {
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

		// Next loop through test stores and maybe rebalance. Only one rebalance
		// can occur to a node per cycle.
		active := map[proto.StoreID]struct{}{}
		for j := 0; j < len(testStores); j++ {
			ts := &testStores[j]
			if alloc.ShouldRebalance(&testStores[j].StoreDescriptor) {
				target, err := alloc.RebalanceTarget(proto.Attributes{}, []proto.Replica{{NodeID: ts.Node.NodeID, StoreID: ts.StoreID}})
				if err == nil {
					if _, ok := active[target.StoreID]; !ok {
						active[target.StoreID] = struct{}{}
						testStores[j].Rebalance(&testStores[int(target.StoreID)], alloc.randGen.Int63n(1<<20))
					}
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
	// 999 000 000 918 305 000 000 143 000 000 000 000 000 376 000 195 000 000 000 633
	// 684 531 382 824 097 000 097 737 210 075 125 543 363 999 060 627 054 432 000 359
	// 972 662 312 672 431 847 381 596 548 999 329 842 667 978 547 588 529 663 530 797
	// 914 866 775 966 380 528 344 666 446 999 337 641 670 805 700 584 610 603 734 654
	// 896 863 795 921 376 547 591 662 501 999 333 859 640 930 885 596 598 562 850 712
	// 804 682 992 999 463 609 625 600 578 875 419 785 670 943 988 700 742 687 801 796
	// 729 599 996 999 395 719 608 741 612 896 481 899 734 961 914 763 672 745 835 716
	// 875 725 890 802 449 709 739 609 587 816 488 797 683 999 898 692 653 642 825 683
	// 888 787 926 820 524 791 674 710 674 811 481 892 740 999 924 738 693 718 872 720
	// 790 744 999 867 563 821 718 753 628 805 545 896 759 940 861 741 699 745 898 771
	// 773 761 970 754 486 757 696 789 627 744 507 847 753 999 878 688 762 775 861 788
	// 809 772 982 724 620 716 787 832 570 751 569 817 783 999 864 661 767 823 878 765
	// 800 774 933 703 575 815 802 834 648 774 549 820 766 999 899 671 753 842 880 769
	// 820 775 974 744 647 814 826 867 704 780 617 834 786 999 931 715 788 915 943 799
	// 825 798 965 744 671 821 807 862 666 757 646 780 765 999 887 720 793 910 909 804
	// 881 748 923 771 695 788 802 831 659 755 644 736 788 999 913 695 817 880 907 785
	// 860 780 918 811 709 840 792 841 710 809 679 768 841 999 922 748 861 891 895 790
	// 838 822 854 819 745 883 733 816 711 783 703 754 866 999 901 733 869 862 902 809
	// 868 871 911 829 788 867 760 862 744 830 729 803 931 999 904 747 885 911 902 832
	// 862 790 895 807 786 849 780 814 726 819 739 771 898 999 862 714 866 888 882 781
	// 883 770 860 825 841 804 819 742 767 833 738 789 913 999 871 720 836 892 868 785
	// 945 866 893 875 896 857 865 873 874 900 837 849 969 999 922 803 886 934 926 854
	// 896 873 871 875 864 850 894 897 869 850 855 897 939 999 870 857 873 886 936 896
	// 879 845 886 888 893 865 879 862 870 832 869 872 903 999 891 834 864 897 911 883
	// 930 921 915 929 942 927 936 930 949 932 935 935 923 999 951 915 937 948 970 939
	// 924 955 942 922 940 942 935 924 953 960 925 930 952 999 969 932 923 930 985 943
	// 928 919 912 914 926 919 941 917 927 942 932 917 946 976 938 933 938 925 999 958
	// 928 927 917 919 929 927 916 921 912 951 936 924 931 943 963 922 943 922 999 957
	// 922 925 902 912 904 915 918 922 916 929 917 928 915 950 972 903 923 933 999 945
	// 925 924 913 934 920 916 910 913 926 924 930 922 924 967 998 899 938 939 999 948
	// 912 936 915 929 916 911 926 927 902 920 927 900 912 920 999 912 939 936 969 940
	// 924 915 918 930 931 940 929 936 913 918 929 918 935 926 999 924 947 923 987 975
	// 917 912 913 909 919 921 908 919 909 928 910 912 917 934 999 911 939 915 960 958
	// 943 933 926 934 947 924 922 926 942 949 928 930 942 969 999 930 951 928 990 971
	// 946 931 943 933 933 948 939 946 936 965 940 932 939 980 990 935 953 939 999 985
	// 948 950 938 945 937 940 940 961 949 975 960 954 953 990 982 955 967 962 999 975
	// 947 955 960 964 969 964 964 971 959 958 956 970 964 995 996 958 985 981 999 998
	// 956 950 962 947 964 946 966 958 956 961 951 966 943 986 999 957 965 975 996 991
	// 943 956 969 956 960 939 970 945 944 945 946 965 941 985 986 946 976 985 999 981
	// 944 944 972 942 960 945 958 962 936 930 945 963 942 994 994 956 976 999 991 987
	// 938 957 955 951 948 945 961 977 944 941 963 963 956 996 968 968 980 997 992 999
	// 967 959 952 946 968 951 945 948 953 966 981 968 964 998 969 970 990 982 999 989
	// 947 957 958 940 981 955 949 950 949 981 969 958 953 985 957 977 986 990 999 984
	// 957 940 956 942 962 948 938 947 950 965 963 950 940 994 959 976 995 979 999 976
	// 958 961 952 941 963 948 953 955 947 946 952 952 947 992 950 980 999 990 994 965
	// 955 954 938 924 935 939 931 945 930 941 941 933 953 973 935 958 999 985 976 941
	// 945 961 940 913 915 930 929 943 924 937 935 945 944 976 932 944 999 977 968 940
	// 945 950 964 929 933 938 948 968 929 950 949 939 961 994 938 954 999 985 985 954
	// 959 963 964 950 948 951 955 983 939 954 969 952 976 997 952 967 999 989 980 974
	// Total bytes=1016398823, ranges=1918
}
