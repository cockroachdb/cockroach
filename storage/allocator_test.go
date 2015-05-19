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
	result, err := s._allocator.Allocate(simpleZoneConfig.ReplicaAttrs[0], []proto.Replica{})
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
	result, err := s._allocator.Allocate(simpleZoneConfig.ReplicaAttrs[0], []proto.Replica{})
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
	result1, err := s._allocator.Allocate(multiDisksConfig.ReplicaAttrs[0], []proto.Replica{})
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
	result2, err := s._allocator.Allocate(multiDisksConfig.ReplicaAttrs[1], exReplicas)
	if err != nil {
		t.Errorf("Unable to perform allocation: %v", err)
	}
	if result2.StoreID != 4 {
		t.Errorf("Expected store 4; got %+v", result2)
	}
	if result1.Node.NodeID == result2.Node.NodeID {
		t.Errorf("Expected node ids to be different %+v vs %+v", result1, result2)
	}
	result3, err := s._allocator.Allocate(multiDisksConfig.ReplicaAttrs[2], []proto.Replica{})
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
	result1, err := s._allocator.Allocate(multiDCConfig.ReplicaAttrs[0], []proto.Replica{})
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	result2, err := s._allocator.Allocate(multiDCConfig.ReplicaAttrs[1], []proto.Replica{})
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	if result1.Node.NodeID != 1 || result2.Node.NodeID != 2 {
		t.Errorf("Expected nodes 1 & 2: %+v vs %+v", result1.Node, result2.Node)
	}
	// Verify that no result is forthcoming if we already have a replica.
	_, err = s._allocator.Allocate(multiDCConfig.ReplicaAttrs[1], []proto.Replica{
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
	result, err := s._allocator.Allocate(multiDisksConfig.ReplicaAttrs[1], []proto.Replica{
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
		result, err := s._allocator.Allocate(proto.Attributes{}, []proto.Replica{})
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
			Capacity: proto.StoreCapacity{Capacity: 100, Available: int64(100 * minAvailCapacityThreshold)},
		},
		{
			StoreID:  4,
			Node:     proto.NodeDescriptor{NodeID: 4},
			Capacity: proto.StoreCapacity{Capacity: 100, Available: int64(100*minAvailCapacityThreshold) / 2},
		},
	}
	gossipStores(s.Gossip(), stores, t)

	// Every rebalance target must be either stores 1 or 2.
	for i := 0; i < 10; i++ {
		result, err := s._allocator.Rebalance(proto.Attributes{}, []proto.Replica{})
		if err != nil {
			t.Fatal(err)
		}
		if result.StoreID != 1 && result.StoreID != 2 {
			t.Errorf("expected store 1 or 2; got %d", result.StoreID)
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
		result, err := s._allocator.Rebalance(proto.Attributes{}, []proto.Replica{})
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
		result, err := s._allocator.Rebalance(proto.Attributes{}, []proto.Replica{})
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

func TestAllocatorFindStores(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, _, stopper := createTestStore(t)
	defer stopper.Stop()
	required := []string{"ssd", "dc"}
	// Nothing yet.
	if sl := s._allocator.findStores(proto.Attributes{Attrs: required}); len(sl.stores) != 0 {
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
		sl := s._allocator.findStores(proto.Attributes{Attrs: required})
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
	sl := s._allocator.findStores(proto.Attributes{Attrs: required})
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
				target, err := alloc.Rebalance(proto.Attributes{}, []proto.Replica{{NodeID: ts.Node.NodeID, StoreID: ts.StoreID}})
				if err == nil {
					if _, ok := active[target.StoreID]; !ok {
						active[target.StoreID] = struct{}{}
						testStores[j].Rebalance(&testStores[int(target.StoreID)], alloc.randGen.Int63n(1<<20))
					}
				}
			}
		}

		// Output store capacities as hexidecimal 2-character values.
		if i%(generations/20) == 0 {
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
					fmt.Printf("%02x%s", (255*bytes)/maxBytes, endStr)
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
	// ff 00 00 00 00 00 00 bc 00 00 00 00 00 00 00 00 00 00 00 00
	// e9 bb 34 df 1e 79 68 80 70 6b a3 ff c1 e1 60 a1 97 d5 49 a4
	// e4 dc ca eb 60 8b 96 a9 7f ff 55 db a3 ed e2 98 98 8f d9 b5
	// df b7 e4 d1 78 bb ba ae a5 d7 7e df c2 ff f1 b5 b6 b1 e2 b7
	// c9 be ff dd 8f d1 b7 c0 a0 cd 8b e4 c1 f0 db bd b2 be e5 c4
	// c8 bf f7 ae 8e d2 d4 dd a7 c2 94 db c2 ff e6 b5 b8 dd e7 ce
	// d2 cb f6 bd ab d1 ce dc aa c1 a4 c7 c3 ff e2 b7 ca e8 e8 cd
	// d4 ca e4 c9 bd dc c9 d2 ac cd af bf db ff e8 bc e1 e1 e3 cc
	// dc c9 e4 ce c8 d8 c7 cf b9 d1 bc c4 e5 ff dc b6 dd e2 e1 c7
	// ea c6 da dc da cc d9 c5 c3 db c1 c3 ef ff e8 c0 d8 e2 e7 c6
	// f6 cc e2 eb ed d8 e0 c8 d2 ed ca c7 f5 f8 ea cc e6 f1 ff c8
	// ff cc da e8 f2 d7 d7 d3 d4 e9 ce cb ec f5 eb cd dc e7 fc bd
	// ff d3 e4 f2 ee da dc d9 d7 f0 da c9 f2 f3 f0 d6 de e6 fd cd
	// ff ce e5 f0 f7 df d8 e4 d3 f0 d8 c7 f0 eb f1 db db e1 fe c9
	// f9 c8 e6 f6 f7 e3 d8 e4 cd f2 d7 c8 ee f2 f4 df d4 e4 ff cd
	// fd d1 e9 f6 f7 e2 d7 e0 cf ef d8 ce f6 f2 f9 e0 dc ec ff d0
	// fc d2 e8 f5 fd e7 da e7 d6 f6 e2 d3 ff f8 f9 e7 e0 f2 f9 d0
	// ee d8 df f2 f8 e4 dc e0 d5 f2 dd d2 ff f2 f3 e7 de e5 f9 d3
	// ee e0 e4 f0 ff ec db e4 d6 f4 dc d7 fd ef ee eb df e9 fd cf
	// f1 e4 e7 ec fa ee e0 e9 d5 fa dd dd ff f0 f2 ef e3 f0 fc d1
	// Total bytes=1021384178, ranges=1918
}
