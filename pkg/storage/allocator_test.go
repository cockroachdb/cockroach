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
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/util"
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
func createTestAllocator() (*stop.Stopper, *gossip.Gossip, *StorePool, Allocator, *hlc.ManualClock) {
	stopper, g, manualClock, storePool := createTestStorePool(TestTimeUntilStoreDeadOff)
	manualClock.Set(hlc.UnixNano())
	a := MakeAllocator(storePool, AllocatorOptions{AllowRebalance: true})
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

	storePool.mu.storeDetails = make(map[roachpb.StoreID]*storeDetail)
	for _, storeID := range aliveStoreIDs {
		detail := newStoreDetail(context.Background())
		detail.desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
		storePool.mu.storeDetails[storeID] = detail
	}
	for _, storeID := range deadStoreIDs {
		detail := newStoreDetail(context.TODO())
		detail.dead = true
		detail.desc = &roachpb.StoreDescriptor{
			StoreID: storeID,
			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		}
		storePool.mu.storeDetails[storeID] = detail
	}
	for storeID, detail := range storePool.mu.storeDetails {
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
	result, err := a.AllocateTarget(
		simpleZoneConfig.Constraints,
		[]roachpb.ReplicaDescriptor{},
		firstRange,
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
	stopper, g, sp, a, _ := createTestAllocator()
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
	stopper, _, _, a, _ := createTestAllocator()
	defer stopper.Stop()
	result, err := a.AllocateTarget(
		simpleZoneConfig.Constraints,
		[]roachpb.ReplicaDescriptor{},
		firstRange,
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
	stopper, g, _, a, _ := createTestAllocator()
	defer stopper.Stop()
	gossiputil.NewStoreGossiper(g).GossipStores(multiDCStores, t)
	result1, err := a.AllocateTarget(
		multiDCConfig.Constraints,
		[]roachpb.ReplicaDescriptor{},
		firstRange,
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
	)
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
		},
		firstRange,
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
		result, err := a.AllocateTarget(constraints, existing, firstRange)
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
		sl, _, _ := a.storePool.getStoreList(firstRange)
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

	// Returns a slice of stores with the specified mean. The first replica will
	// have a range count that's above the target range count for the rebalancer,
	// so it should be rebalanced from.
	oneStoreAboveRebalanceTarget := func(mean int32, numStores int) []testStore {
		stores := make([]testStore, numStores)
		for i := range stores {
			stores[i].rangeCount = mean
		}
		surplus := int32(math.Ceil(float64(mean)*RebalanceThreshold + 1))
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
		deficit := int32(math.Ceil(float64(mean)*RebalanceThreshold + 1))
		stores[0].rangeCount -= deficit
		for i := 1; i < len(stores); i++ {
			stores[i].rangeCount += int32(math.Ceil(float64(deficit) / float64(len(stores)-1)))
			stores[i].shouldRebalanceFrom = true
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
		// A cluster where all range counts are within RebalanceThreshold should
		// not rebalance. This assumes RebalanceThreshold > 2%.
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

		// It doesn't make sense to test sets of stores containing fewer than 4
		// stores, because 4 stores is the minimum number of stores needed to
		// trigger rebalancing with the default replication factor of 3. Also, the
		// above local functions need a minimum number of stores to properly create
		// the desired distribution of range counts.
		const minStores = 4
		if numStores := len(tc); numStores < minStores {
			t.Fatalf("%d: numStores %d < min %d", i, numStores, minStores)
		}
		stopper, g, _, a, _ := createTestAllocator()
		defer stopper.Stop()

		a.storePool.mu.Lock()
		a.storePool.mu.deterministic = true
		a.storePool.mu.Unlock()

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
			sl, _, _ := a.storePool.getStoreList(firstRange)
			for j, s := range sl.stores {
				if a, e := s.Capacity.RangeCount, tc[j].rangeCount; a != e {
					return errors.Errorf("tc %d: range count for %d = %d != expected %d", i, j, a, e)
				}
			}
			return nil
		})
		sl, _, _ := a.storePool.getStoreList(firstRange)

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
		result, err := a.RebalanceTarget(
			config.Constraints{},
			[]roachpb.ReplicaDescriptor{{StoreID: 1}},
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

	// Verify shouldRebalance results.
	for i, store := range stores {
		desc, ok := a.storePool.getStoreDescriptor(store.StoreID)
		if !ok {
			t.Fatalf("%d: unable to get store %d descriptor", i, store.StoreID)
		}
		sl, _, _ := a.storePool.getStoreList(firstRange)
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
		firstRange,
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
	rpcContext := rpc.NewContext(log.AmbientContext{}, &base.Config{Insecure: true}, nil, stopper)
	server := rpc.NewServer(rpcContext) // never started
	g := gossip.NewTest(1, rpcContext, server, nil, stopper, metric.NewRegistry())
	sp := NewStorePool(
		log.AmbientContext{},
		g,
		hlc.NewClock(hlc.UnixNano),
		nil,
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
				noStore,
				firstRange,
			)
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
	// generation    6:   1  3%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  7 28%,  5 26%,  5  8%,  4 10%,  2 11%,  3  9%  2  1%
	// generation    8:   2  2%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  9 15%,  0  0%,  0  0%,  0  0%,  0  0%,  0  0%,  7 12%,  7 21%,  5 15%,  5  6%,  4  4%,  2 11%,  3  6%  2  3%
	// generation   10:   3  2%, 11 15%,  0  0%,  0  0%,  0  0%,  0  0%,  9 10%,  0  0%,  0  0%,  0  0%,  0  0%,  7  9%,  7  8%,  7 14%,  5 10%,  5  5%,  4  6%,  3 11%,  3  2%  3  3%
	// generation   12:   5  4%, 11 11%,  0  0%,  0  0%,  0  0%,  0  0%,  9  5%,  8  7%,  0  0%,  0  0%,  8  8%,  7  6%,  7  5%,  7 12%,  5  8%,  5  3%,  5  6%,  5 10%,  5  5%  5  4%
	// generation   14:   6  3%, 11  8%, 14 12%,  0  0%,  0  0%,  0  0%,  9  4%,  8  6%,  8  4%,  0  0%,  8  6%,  7  5%,  7  3%,  7  9%,  6  7%,  6  4%,  6  4%,  6  9%,  6  5%  6  3%
	// generation   16:   8  2%, 11  6%, 14  9%,  9  5%,  0  0%,  0  0%,  9  4%,  8  4%,  8  3%,  7  5%,  8  4%,  8  4%,  8  4%,  8  7%,  8  6%,  8  5%,  8  5%,  8  8%,  8  6%  8  4%
	// generation   18:  10  3%, 11  5%, 14  7%, 10  4%,  5  3%,  2  1%, 10  3%, 10  4%, 10  4%,  9  5%, 10  5%, 10  4%, 10  4%, 10  6%, 10  6%, 10  5%, 10  5%, 10  7%, 10  6% 10  4%
	// generation   20:  12  4%, 12  5%, 14  5%, 12  4%,  7  3%,  7  2%, 12  4%, 12  5%, 12  3%, 11  5%, 12  5%, 12  4%, 12  4%, 12  6%, 12  6%, 12  5%, 12  5%, 12  6%, 12  5% 12  4%
	// generation   22:  14  4%, 14  5%, 14  4%, 14  4%, 10  4%, 10  3%, 14  4%, 14  5%, 14  3%, 13  6%, 14  5%, 14  4%, 14  4%, 14  6%, 14  5%, 14  5%, 14  5%, 14  6%, 14  5% 14  4%
	// generation   24:  16  5%, 16  5%, 16  4%, 16  4%, 12  4%, 12  3%, 16  4%, 16  5%, 16  3%, 15  5%, 16  6%, 16  4%, 16  4%, 16  6%, 16  5%, 16  4%, 16  5%, 16  6%, 16  5% 16  4%
	// generation   26:  18  5%, 18  5%, 18  4%, 18  4%, 14  4%, 14  3%, 18  4%, 18  4%, 18  3%, 17  5%, 18  5%, 18  4%, 18  4%, 18  6%, 18  5%, 18  4%, 18  5%, 18  5%, 18  5% 18  5%
	// generation   28:  20  5%, 20  5%, 20  4%, 20  4%, 16  4%, 16  3%, 20  4%, 20  4%, 20  3%, 19  5%, 20  5%, 20  4%, 20  4%, 20  6%, 20  5%, 20  5%, 20  5%, 20  5%, 20  5% 20  4%
	// generation   30:  22  5%, 22  4%, 22  4%, 22  4%, 18  4%, 18  4%, 22  4%, 22  4%, 22  3%, 21  5%, 22  5%, 22  4%, 22  4%, 22  6%, 22  5%, 22  5%, 22  5%, 22  5%, 22  5% 22  4%
	// generation   32:  24  5%, 24  4%, 24  4%, 24  4%, 20  4%, 20  4%, 24  4%, 24  4%, 24  3%, 23  5%, 24  5%, 24  5%, 24  4%, 24  5%, 24  5%, 24  5%, 24  5%, 24  5%, 24  5% 24  4%
	// generation   34:  26  5%, 26  4%, 26  4%, 26  4%, 22  4%, 22  4%, 26  4%, 26  4%, 26  3%, 25  5%, 26  5%, 26  5%, 26  4%, 26  5%, 26  5%, 26  5%, 26  5%, 26  5%, 26  5% 26  4%
	// generation   36:  28  5%, 28  4%, 28  4%, 28  4%, 24  4%, 24  4%, 28  4%, 28  5%, 28  3%, 27  5%, 28  5%, 28  5%, 28  4%, 28  5%, 28  5%, 28  4%, 28  5%, 28  5%, 28  5% 28  4%
	// generation   38:  30  5%, 30  5%, 30  4%, 30  4%, 26  4%, 26  4%, 30  4%, 30  4%, 30  3%, 29  5%, 30  4%, 30  5%, 30  4%, 30  5%, 30  5%, 30  4%, 30  5%, 30  5%, 30  6% 30  4%
	// generation   40:  32  5%, 32  5%, 32  4%, 32  5%, 28  4%, 28  4%, 32  4%, 32  4%, 32  3%, 31  5%, 32  5%, 32  5%, 32  4%, 32  5%, 32  5%, 32  4%, 32  5%, 32  5%, 32  6% 32  4%
	// generation   42:  34  5%, 34  5%, 34  4%, 34  4%, 30  4%, 30  4%, 34  4%, 34  4%, 34  3%, 33  5%, 34  4%, 34  5%, 34  4%, 34  5%, 34  5%, 34  4%, 34  4%, 34  5%, 34  6% 34  4%
	// generation   44:  36  5%, 36  5%, 36  4%, 36  4%, 32  4%, 32  4%, 36  4%, 36  5%, 36  3%, 35  5%, 36  5%, 36  5%, 36  4%, 36  5%, 36  5%, 36  4%, 36  4%, 36  5%, 36  5% 36  4%
	// generation   46:  38  5%, 38  5%, 38  4%, 38  4%, 34  4%, 34  4%, 38  4%, 38  5%, 38  4%, 37  5%, 38  5%, 38  5%, 38  4%, 38  5%, 38  5%, 38  4%, 38  4%, 38  5%, 38  5% 38  4%
	// generation   48:  40  5%, 40  5%, 40  4%, 40  4%, 36  4%, 36  4%, 40  4%, 40  4%, 40  4%, 39  5%, 40  5%, 40  5%, 40  4%, 40  5%, 40  5%, 40  4%, 40  4%, 40  5%, 40  5% 40  4%
	// generation   50:  42  5%, 42  5%, 42  4%, 42  4%, 38  4%, 38  4%, 42  4%, 42  4%, 42  4%, 41  5%, 42  5%, 42  4%, 42  4%, 42  5%, 42  5%, 42  4%, 42  4%, 42  5%, 42  5% 42  4%
	// generation   52:  44  5%, 44  5%, 44  3%, 44  4%, 40  4%, 40  4%, 44  4%, 44  4%, 44  4%, 43  5%, 44  5%, 44  4%, 44  4%, 44  5%, 44  5%, 44  4%, 44  4%, 44  5%, 44  5% 44  4%
	// generation   54:  46  5%, 46  5%, 46  4%, 46  4%, 42  4%, 42  4%, 46  4%, 46  4%, 46  4%, 45  5%, 46  5%, 46  4%, 46  4%, 46  5%, 46  5%, 46  4%, 46  4%, 46  4%, 46  5% 46  4%
	// generation   56:  48  5%, 48  5%, 48  4%, 48  4%, 44  4%, 44  4%, 48  4%, 48  5%, 48  4%, 47  5%, 48  5%, 48  4%, 48  4%, 48  5%, 48  5%, 48  4%, 48  4%, 48  4%, 48  5% 48  4%
	// generation   58:  50  5%, 50  5%, 50  4%, 50  4%, 46  4%, 46  4%, 50  4%, 50  4%, 50  4%, 49  5%, 50  5%, 50  4%, 50  4%, 50  5%, 50  5%, 50  4%, 50  4%, 50  4%, 50  5% 50  4%
	// generation   60:  52  5%, 52  5%, 52  4%, 52  4%, 48  4%, 48  4%, 52  4%, 52  4%, 52  4%, 51  5%, 52  5%, 52  4%, 52  5%, 52  5%, 52  5%, 52  4%, 52  4%, 52  4%, 52  5% 52  4%
	// generation   62:  54  5%, 54  5%, 54  4%, 54  4%, 50  4%, 50  4%, 54  5%, 54  5%, 54  4%, 53  5%, 54  5%, 54  5%, 54  4%, 54  5%, 54  5%, 54  4%, 54  4%, 54  4%, 54  5% 54  4%
	// generation   64:  56  5%, 56  5%, 56  4%, 56  4%, 52  4%, 52  4%, 56  4%, 56  5%, 56  4%, 55  5%, 56  5%, 56  5%, 56  5%, 56  5%, 56  5%, 56  4%, 56  5%, 56  4%, 56  5% 56  4%
	// generation   66:  58  5%, 58  5%, 58  4%, 58  4%, 54  4%, 54  4%, 58  4%, 58  5%, 58  4%, 57  5%, 58  5%, 58  4%, 58  5%, 58  5%, 58  5%, 58  4%, 58  5%, 58  4%, 58  5% 58  4%
	// generation   68:  60  5%, 60  5%, 60  4%, 60  4%, 56  4%, 56  4%, 60  4%, 60  5%, 60  4%, 59  5%, 60  5%, 60  4%, 60  5%, 60  5%, 60  5%, 60  4%, 60  5%, 60  4%, 60  5% 60  4%
	// generation   70:  62  5%, 62  5%, 62  4%, 62  4%, 58  4%, 58  5%, 62  4%, 62  5%, 62  4%, 61  5%, 62  5%, 62  5%, 62  5%, 62  5%, 62  5%, 62  4%, 62  5%, 62  4%, 62  5% 62  4%
	// generation   72:  64  5%, 64  5%, 64  4%, 64  4%, 60  4%, 60  5%, 64  4%, 64  5%, 64  4%, 63  5%, 64  5%, 64  4%, 64  5%, 64  5%, 64  5%, 64  4%, 64  5%, 64  4%, 64  5% 64  4%
	// generation   74:  66  5%, 66  5%, 66  4%, 66  4%, 62  4%, 62  4%, 66  4%, 66  5%, 66  4%, 65  5%, 66  5%, 66  4%, 66  5%, 66  5%, 66  5%, 66  4%, 66  5%, 66  4%, 66  5% 66  4%
	// generation   76:  68  5%, 68  5%, 68  4%, 68  4%, 64  4%, 64  4%, 68  4%, 68  5%, 68  4%, 67  5%, 68  5%, 68  4%, 68  5%, 68  5%, 68  5%, 68  4%, 68  5%, 68  4%, 68  5% 68  4%
	// generation   78:  70  5%, 70  5%, 70  4%, 70  4%, 66  4%, 66  4%, 70  4%, 70  5%, 70  4%, 69  5%, 70  5%, 70  4%, 70  5%, 70  5%, 70  5%, 70  4%, 70  4%, 70  4%, 70  5% 70  4%
	// generation   80:  72  5%, 72  5%, 72  4%, 72  4%, 68  4%, 68  4%, 72  4%, 72  5%, 72  4%, 71  5%, 72  5%, 72  4%, 72  5%, 72  5%, 72  5%, 72  4%, 72  5%, 72  4%, 72  5% 72  4%
	// generation   82:  74  5%, 74  5%, 74  4%, 74  4%, 70  4%, 70  4%, 74  4%, 74  5%, 74  4%, 73  5%, 74  5%, 74  4%, 74  5%, 74  5%, 74  5%, 74  4%, 74  5%, 74  4%, 74  5% 74  4%
	// generation   84:  76  5%, 76  5%, 76  4%, 76  4%, 72  4%, 72  4%, 76  4%, 76  4%, 76  4%, 75  5%, 76  5%, 76  4%, 76  5%, 76  4%, 76  5%, 76  4%, 76  5%, 76  4%, 76  5% 76  4%
	// generation   86:  78  5%, 78  5%, 78  4%, 78  4%, 74  4%, 74  4%, 78  4%, 78  5%, 78  4%, 77  5%, 78  5%, 78  4%, 78  5%, 78  5%, 78  5%, 78  4%, 78  5%, 78  4%, 78  5% 78  4%
	// generation   88:  80  5%, 80  5%, 80  4%, 80  4%, 76  4%, 76  4%, 80  5%, 80  5%, 80  4%, 79  5%, 80  5%, 80  4%, 80  5%, 80  5%, 80  5%, 80  4%, 80  4%, 80  4%, 80  5% 80  4%
	// generation   90:  81  5%, 81  5%, 81  4%, 81  4%, 77  5%, 77  4%, 81  4%, 81  4%, 81  4%, 80  5%, 81  5%, 81  4%, 81  5%, 81  5%, 81  5%, 81  4%, 81  4%, 81  4%, 81  5% 81  4%
	// generation   92:  81  5%, 81  5%, 81  4%, 81  4%, 77  5%, 77  4%, 81  4%, 81  4%, 81  4%, 80  5%, 81  5%, 81  4%, 81  5%, 81  5%, 81  5%, 81  4%, 81  4%, 81  4%, 81  5% 81  4%
	// generation   94:  81  5%, 81  5%, 81  4%, 81  4%, 77  5%, 77  4%, 81  4%, 81  4%, 81  4%, 80  5%, 81  5%, 81  4%, 81  5%, 81  5%, 81  5%, 81  4%, 81  4%, 81  4%, 81  5% 81  4%
	// generation   96:  81  5%, 81  5%, 81  4%, 81  4%, 77  5%, 77  4%, 81  4%, 81  4%, 81  4%, 80  5%, 81  5%, 81  4%, 81  5%, 81  5%, 81  5%, 81  4%, 81  4%, 81  4%, 81  5% 81  4%
	// generation   98:  81  5%, 81  5%, 81  4%, 81  4%, 77  5%, 77  4%, 81  4%, 81  4%, 81  4%, 80  5%, 81  5%, 81  4%, 81  5%, 81  5%, 81  5%, 81  4%, 81  4%, 81  4%, 81  5% 81  4%
	// Total bytes=842607128, ranges=1611
}
