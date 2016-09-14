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
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
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
func createTestAllocator() (*stop.Stopper, *gossip.Gossip, *StorePool, Allocator, *hlc.ManualClock) {
	stopper := stop.NewStopper()
	manualClock := hlc.NewManualClock(hlc.UnixNano())
	clock := hlc.NewClock(manualClock.UnixNano)
	rpcContext := rpc.NewContext(context.TODO(), &base.Context{Insecure: true}, clock, stopper)
	server := rpc.NewServer(rpcContext) // never started
	g := gossip.New(context.Background(), rpcContext, server, nil, stopper, metric.NewRegistry())
	// Have to call g.SetNodeID before call g.AddInfo
	g.SetNodeID(roachpb.NodeID(1))
	storePool := NewStorePool(
		g,
		clock,
		rpcContext,
		/* reservationsEnabled */ true,
		TestTimeUntilStoreDeadOff,
		stopper,
	)
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
		detail.desc = &roachpb.StoreDescriptor{StoreID: storeID}
		storePool.mu.stores[storeID] = detail
	}
	for _, storeID := range deadStoreIDs {
		detail := newStoreDetail()
		detail.dead = true
		detail.desc = &roachpb.StoreDescriptor{StoreID: storeID}
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
	result, err := a.AllocateTarget(simpleZoneConfig.ReplicaAttrs[0], []roachpb.ReplicaDescriptor{}, false)
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
	result, err := a.AllocateTarget(simpleZoneConfig.ReplicaAttrs[0], []roachpb.ReplicaDescriptor{}, false)
	if result != nil {
		t.Errorf("expected nil result: %+v", result)
	}
	if err == nil {
		t.Errorf("allocation succeeded despite there being no available disks: %v", result)
	}
}

func TestAllocatorThreeDisksSameDC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator()
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(sameDCStores, t)
	result1, err := a.AllocateTarget(multiDisksConfig.ReplicaAttrs[0], []roachpb.ReplicaDescriptor{}, false)
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
	result2, err := a.AllocateTarget(multiDisksConfig.ReplicaAttrs[1], exReplicas, false)
	if err != nil {
		t.Errorf("Unable to perform allocation: %v", err)
	}
	if result2.StoreID != 3 && result2.StoreID != 4 {
		t.Errorf("Expected store 3 or 4; got %+v", result2)
	}
	if result1.Node.NodeID == result2.Node.NodeID {
		t.Errorf("Expected node ids to be different %+v vs %+v", result1, result2)
	}
	result3, err := a.AllocateTarget(multiDisksConfig.ReplicaAttrs[2], []roachpb.ReplicaDescriptor{}, false)
	if err != nil {
		t.Errorf("Unable to perform allocation: %v", err)
	}
	if result3.Node.NodeID != 4 || result3.StoreID != 5 {
		t.Errorf("Expected node 4, store 5; got %+v", result3)
	}
}

func TestAllocatorTwoDatacenters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator()
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(multiDCStores, t)
	result1, err := a.AllocateTarget(multiDCConfig.ReplicaAttrs[0], []roachpb.ReplicaDescriptor{}, false)
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	result2, err := a.AllocateTarget(multiDCConfig.ReplicaAttrs[1], []roachpb.ReplicaDescriptor{}, false)
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
	}, false)
	if err == nil {
		t.Errorf("expected error on allocation without available stores")
	}
}

func TestAllocatorExistingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator()
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(sameDCStores, t)
	result, err := a.AllocateTarget(multiDisksConfig.ReplicaAttrs[1], []roachpb.ReplicaDescriptor{
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
	defer leaktest.AfterTest(t)()
	stopper, g, _, a, _ := createTestAllocator()
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
		result, err := a.AllocateTarget(roachpb.Attributes{Attrs: test.required}, existing, test.relaxConstraints)
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
		result := a.RebalanceTarget(roachpb.Attributes{},
			[]roachpb.ReplicaDescriptor{{StoreID: 3}}, 0)
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
		sl, _, _ := a.storePool.getStoreList(roachpb.Attributes{}, true)
		result := a.shouldRebalance(desc, sl)
		if expResult := (i >= 2); expResult != result {
			t.Errorf("%d: expected rebalance %t; got %t", i, expResult, result)
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

	// Returns a slice of stores with the specified mean. One replica will have
	// a range count that's above the target range count for the rebalancer, so it
	// should be rebalanced from.
	oneStoreAboveRebalanceTarget := func(mean int32, numStores int) []testStore {
		if minStores := 4; numStores < minStores {
			t.Fatalf("# of stores %d < min %d", numStores, minStores)
		}
		stores := make([]testStore, numStores)
		for i := range stores {
			stores[i].rangeCount = mean
		}
		thresholdDelta := int32(float64(mean)*rebalanceThreshold + 1)
		stores[0].rangeCount -= thresholdDelta
		stores[len(stores)-1].rangeCount += thresholdDelta
		stores[len(stores)-1].shouldRebalanceFrom = true
		return stores
	}

	// Returns a slice of stores with the specified mean (fill in TBD)
	oneUnderusedStore := func(mean int32, numStores int) []testStore {
		if minStores := 4; numStores < minStores {
			t.Fatalf("# of stores %d < min %d", numStores, minStores)
		}
		stores := make([]testStore, 10)
		for i := range stores {
			stores[i].rangeCount = mean
		}
		// Subtract a large number of ranges from the first store and add that delta
		// back to the rest of the replicas to maintain the specified mean.
		deficit := int32(float64(mean)*rebalanceThreshold + 1)
		stores[0].rangeCount -= deficit
		for i := 1; i < len(stores); i++ {
			stores[i].rangeCount += deficit / int32(len(stores)-1)
			// TODO(cuongdo): The following must be changed to true when the allocator
			// starts consistently rebalancing to severely underused stores.
			stores[i].shouldRebalanceFrom = false
		}
		return stores
	}

	// Each test case defines the range counts for the test stores and whether we
	// should rebalance from the store.
	testCases := [][]testStore{
		// An evenly balanced cluster should not rebalance.
		{{5, false}, {5, false}, {5, false}, {5, false}},
		// A very nearly balanced cluster should not rebalance.
		{{5, false}, {5, false}, {5, false}, {6, false}},
		// Adding an empty node to a 3-node cluster triggers rebalancing from
		// existing nodes.
		{{100, true}, {100, true}, {100, true}, {0, false}},
		// A cluster where all range counts are within rebalanceThreshold should
		// not rebalance. This assumes rebalanceThreshold > 2%.
		{{98, false}, {99, false}, {101, false}, {102, false}},

		// 5-nodes, each with a single store above the rebalancer target range
		// count.
		oneStoreAboveRebalanceTarget(100, 5),
		oneStoreAboveRebalanceTarget(1000, 5),
		oneStoreAboveRebalanceTarget(10000, 5),

		oneUnderusedStore(1000, 5),
		oneUnderusedStore(1000, 10),
	}
	for i, tc := range testCases {
		t.Logf("test case %d: %v", i, tc)
		stopper, g, _, a, _ := createTestAllocator()
		a.options.Deterministic = true
		defer stopper.Stop()

		// Create stores with the range counts from the test case and gossip them.
		var stores []*roachpb.StoreDescriptor
		for j, store := range tc {
			stores = append(stores, &roachpb.StoreDescriptor{
				StoreID:  roachpb.StoreID(j + 1),
				Node:     roachpb.NodeDescriptor{NodeID: roachpb.NodeID(j + 1)},
				Capacity: roachpb.StoreCapacity{Capacity: 1, Available: 1, RangeCount: store.rangeCount},
			})
		}
		gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

		// Ensure gossiped store descriptor changes have propagated.
		util.SucceedsSoon(t, func() error {
			sl, _, _ := a.storePool.getStoreList(roachpb.Attributes{}, true)
			for j, s := range sl.stores {
				if a, e := s.Capacity.RangeCount, tc[j].rangeCount; a != e {
					return errors.Errorf("tc %d: range count for %d = %d != expected %d", i, j, a, e)
				}
			}
			return nil
		})
		sl, _, _ := a.storePool.getStoreList(roachpb.Attributes{}, true)

		// Verify shouldRebalance returns the expected value.
		for j, store := range stores {
			desc, ok := a.storePool.getStoreDescriptor(store.StoreID)
			if !ok {
				t.Fatalf("[tc %d,store %d]: unable to get store %d descriptor", i, j, store.StoreID)
			}
			if a, e := a.shouldRebalance(desc, sl), tc[j].shouldRebalanceFrom; a != e {
				t.Errorf("[tc %d,store %d]: shouldRebalance %t != expected %t", i, store.StoreID, a, e)
			}
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
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 98, RangeCount: 2},
		},
	}
	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

	// Every rebalance target must be store 4 (or nil for case of missing the only option).
	for i := 0; i < 10; i++ {
		result := a.RebalanceTarget(roachpb.Attributes{},
			[]roachpb.ReplicaDescriptor{{StoreID: 1}}, 0)
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
		sl, _, _ := a.storePool.getStoreList(roachpb.Attributes{}, true)
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

	targetRepl, err := a.RemoveTarget(replicas, stores[0].StoreID)
	if err != nil {
		t.Fatal(err)
	}
	if a, e := targetRepl, replicas[2]; a != e {
		t.Fatalf("RemoveTarget did not select expected replica; expected %v, got %v", e, a)
	}

	// Now perform the same test, but pass in the store ID of store 3 so it's
	// excluded.
	targetRepl, err = a.RemoveTarget(replicas, stores[2].StoreID)
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
		// Needs three replicas, one is on a dead store.
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
		// Needs three replicas, one is dead.
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

	attribute := roachpb.Attributes{Attrs: []string{"one"}}
	attributes := roachpb.Attributes{Attrs: []string{"one", "two"}}

	testCases := []struct {
		ae       allocatorError
		expected string
	}{
		{allocatorError{roachpb.Attributes{}, false, 1},
			"0 of 1 store with all attributes matching []; likely not enough nodes in cluster"},
		{allocatorError{attribute, false, 1},
			"0 of 1 store with all attributes matching [one]"},
		{allocatorError{attribute, true, 1},
			"0 of 1 store with an attribute matching [one]; likely not enough nodes in cluster"},
		{allocatorError{attribute, false, 2},
			"0 of 2 stores with all attributes matching [one]"},
		{allocatorError{attribute, true, 2},
			"0 of 2 stores with an attribute matching [one]; likely not enough nodes in cluster"},
		{allocatorError{attributes, false, 1},
			"0 of 1 store with all attributes matching [one,two]"},
		{allocatorError{attributes, true, 1},
			"0 of 1 store with an attribute matching [one,two]; likely not enough nodes in cluster"},
		{allocatorError{attributes, false, 2},
			"0 of 2 stores with all attributes matching [one,two]"},
		{allocatorError{attributes, true, 2},
			"0 of 2 stores with an attribute matching [one,two]; likely not enough nodes in cluster"},
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
		simpleZoneConfig.ReplicaAttrs[0],
		[]roachpb.ReplicaDescriptor{},
		false)
	if _, ok := err.(purgatoryError); !ok {
		t.Fatalf("expected a purgatory error, got: %v", err)
	}

	// Second, test the normal case in which we can allocate to the store.
	gossiputil.NewStoreGossiper(g).GossipStores(singleStore, t)
	result, err := a.AllocateTarget(
		simpleZoneConfig.ReplicaAttrs[0],
		[]roachpb.ReplicaDescriptor{},
		false)
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
		simpleZoneConfig.ReplicaAttrs[0],
		[]roachpb.ReplicaDescriptor{},
		false)
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
	alloc := MakeAllocator(sp, AllocatorOptions{AllowRebalance: true, Deterministic: true})

	var wg sync.WaitGroup
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix), func(_ string, _ roachpb.Value) { wg.Done() })

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
			if err := g.AddInfoProto(gossip.MakeStoreKey(roachpb.StoreID(j)), &testStores[j].StoreDescriptor, 0); err != nil {
				panic(err)
			}
		}
		wg.Wait()

		// Next loop through test stores and maybe rebalance.
		for j := 0; j < len(testStores); j++ {
			ts := &testStores[j]
			target := alloc.RebalanceTarget(
				roachpb.Attributes{},
				[]roachpb.ReplicaDescriptor{{NodeID: ts.Node.NodeID, StoreID: ts.StoreID}},
				-1)
			if target != nil {
				testStores[j].rebalance(&testStores[int(target.StoreID)], alloc.randGen.Int63n(1<<20))
			}
		}

		// Output store capacities as hexadecimal 2-character values.
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
	// 999 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000
	// 999 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000
	// 999 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000
	// 999 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000
	// 999 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000
	// 999 000 000 000 014 000 000 118 000 000 000 000 111 000 000 000 000 000 000 000
	// 999 204 091 000 037 000 000 241 000 000 000 182 247 000 060 000 027 000 000 000
	// 999 308 331 149 124 144 253 495 047 280 184 497 337 000 186 169 217 094 000 000
	// 999 651 505 179 385 407 420 726 235 352 516 784 697 000 381 445 432 333 086 170
	// 999 693 530 286 579 461 624 795 368 479 631 862 921 000 688 620 478 535 259 437
	// 793 725 645 432 663 558 677 841 457 512 693 909 999 075 685 689 500 726 294 619
	// 646 715 716 509 736 733 623 881 612 552 777 828 999 197 693 774 523 743 436 659
	// 616 642 790 564 722 631 612 858 641 591 679 771 999 259 696 782 504 674 538 647
	// 716 705 894 626 811 629 719 904 701 657 648 811 999 311 729 814 482 660 674 720
	// 739 749 913 704 813 688 780 993 806 723 663 868 999 373 821 851 578 694 747 806
	// 746 798 887 732 844 647 809 930 794 767 702 761 999 432 757 797 592 699 783 738
	// 810 844 954 822 911 715 855 971 852 776 748 817 999 428 833 853 616 714 773 764
	// 746 769 880 831 845 722 847 935 850 706 737 789 999 440 787 821 584 702 761 763
	// 784 807 861 857 809 765 865 940 809 702 772 758 999 495 751 783 617 726 800 827
	// 823 864 886 851 835 819 903 952 813 736 848 751 999 564 784 808 659 764 835 883
	// 826 874 854 884 890 804 953 940 818 754 872 729 999 596 778 841 690 781 867 906
	// 803 882 833 912 874 790 964 881 783 799 902 740 999 613 754 838 733 780 853 913
	// 793 865 807 900 885 781 920 852 794 824 883 709 999 594 790 838 749 788 817 915
	// 796 870 823 904 930 824 910 851 785 828 896 762 999 650 808 851 773 826 842 948
	// 831 898 875 911 956 818 911 859 824 845 911 815 999 666 786 862 806 858 877 920
	// 828 925 853 876 936 818 950 875 845 849 905 837 999 683 812 852 844 850 858 914
	// 858 927 878 859 950 817 961 884 885 877 910 830 999 686 813 888 858 868 873 892
	// 871 918 873 864 954 814 957 889 906 863 911 835 999 680 815 873 836 902 890 868
	// 839 903 847 857 943 785 948 867 888 852 888 788 999 674 817 867 807 867 880 837
	// 844 872 828 847 932 789 937 841 885 816 899 789 999 677 813 868 833 838 869 838
	// 862 866 850 847 921 797 934 855 912 829 896 788 999 685 848 869 817 842 876 832
	// 855 873 829 842 895 815 904 863 902 813 884 776 999 679 859 870 815 826 882 814
	// 860 861 854 847 872 819 902 891 901 822 858 782 999 712 867 883 809 840 907 828
	// 834 841 826 812 851 805 859 878 884 800 826 755 999 708 849 863 790 807 899 790
	// 835 855 845 830 838 804 847 890 896 794 816 779 999 725 861 868 790 821 909 817
	// 838 862 826 844 817 800 852 895 903 795 815 779 999 749 847 848 802 824 926 817
	// 858 874 834 851 814 816 842 891 920 789 795 780 999 765 834 843 804 818 912 804
	// 878 885 867 850 845 842 864 895 927 803 808 808 999 789 846 870 821 849 909 817
	// 854 890 866 867 856 832 859 901 926 807 796 810 999 789 839 876 820 855 923 831
	// 833 864 858 849 844 822 837 872 898 780 789 798 999 783 824 860 802 838 909 827
	// 839 864 859 848 842 817 852 867 910 790 783 798 999 787 829 881 821 839 905 814
	// 825 859 844 855 823 794 866 853 901 790 796 805 999 798 833 896 817 824 886 815
	// 850 854 851 866 818 807 879 857 912 804 809 810 999 818 826 915 829 820 883 833
	// 863 845 853 865 820 821 880 838 910 803 835 833 999 820 820 917 854 830 887 831
	// 883 840 873 864 826 835 886 844 907 798 824 843 999 834 839 919 855 824 894 844
	// 875 835 874 848 831 832 875 839 896 801 819 840 999 821 835 926 850 818 905 837
	// 861 839 868 831 820 839 875 842 886 792 823 836 999 807 829 942 839 836 902 841
	// 886 854 889 850 844 865 884 859 886 813 837 852 999 838 857 954 838 856 922 852
	// 904 879 901 862 852 881 911 877 919 843 854 878 999 842 865 972 875 871 927 884
	// 900 873 911 859 843 886 918 861 913 836 851 877 999 861 846 958 878 869 918 879
	// Total bytes=911890409, ranges=1739
}
