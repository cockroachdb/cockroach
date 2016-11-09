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
	"os"
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
	"github.com/olekukonko/tablewriter"
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
func createTestAllocator(
	deterministic bool, useRuleSolver bool,
) (*stop.Stopper, *gossip.Gossip, *StorePool, Allocator, *hlc.ManualClock) {
	stopper, g, manualClock, storePool := createTestStorePool(TestTimeUntilStoreDeadOff, deterministic)
	a := MakeAllocator(storePool, AllocatorOptions{
		AllowRebalance: true,
		UseRuleSolver:  useRuleSolver,
	})
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
		detail := newStoreDetail(context.TODO())
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

func runToggleRuleSolver(t *testing.T, test func(useRuleSolver bool, t *testing.T)) {
	t.Run("without rule solver", func(t *testing.T) {
		test(false, t)
	})
	t.Run("with rule solver", func(t *testing.T) {
		test(true, t)
	})
}

func TestAllocatorSimpleRetrieval(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runToggleRuleSolver(t, func(useRuleSolver bool, t *testing.T) {
		stopper, g, _, a, _ := createTestAllocator(
			/* deterministic */ false,
			/* UseRuleSolver */ useRuleSolver,
		)
		defer stopper.Stop()
		gossiputil.NewStoreGossiper(g).GossipStores(singleStore, t)
		result, err := a.AllocateTarget(
			simpleZoneConfig.Constraints,
			[]roachpb.ReplicaDescriptor{},
			firstRange,
			false,
		)
		if err != nil {
			t.Fatalf("Unable to perform allocation: %v", err)
		}
		if result.Node.NodeID != 1 || result.StoreID != 1 {
			t.Errorf("expected NodeID 1 and StoreID 1: %+v", result)
		}
	})
}

// TestAllocatorCorruptReplica ensures that the allocator never attempts to
// allocate a new replica on top of a dead (corrupt) one.
func TestAllocatorCorruptReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runToggleRuleSolver(t, func(useRuleSolver bool, t *testing.T) {
		stopper, g, sp, a, _ := createTestAllocator(
			/* deterministic */ false,
			/* useRuleSolver */ useRuleSolver,
		)
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
			true,
		)
		if err != nil {
			t.Fatal(err)
		}
		if result.Node.NodeID != 2 || result.StoreID != 2 {
			t.Errorf("expected NodeID 2 and StoreID 2: %+v", result)
		}
	})
}

func TestAllocatorNoAvailableDisks(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runToggleRuleSolver(t, func(useRuleSolver bool, t *testing.T) {
		stopper, _, _, a, _ := createTestAllocator(
			/* deterministic */ false,
			/* useRuleSolver */ useRuleSolver,
		)
		defer stopper.Stop()
		result, err := a.AllocateTarget(
			simpleZoneConfig.Constraints,
			[]roachpb.ReplicaDescriptor{},
			firstRange,
			false,
		)
		if result != nil {
			t.Errorf("expected nil result: %+v", result)
		}
		if err == nil {
			t.Errorf("allocation succeeded despite there being no available disks: %v", result)
		}
	})
}

func TestAllocatorTwoDatacenters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runToggleRuleSolver(t, func(useRuleSolver bool, t *testing.T) {
		stopper, g, _, a, _ := createTestAllocator(
			/* deterministic */ false,
			/* useRuleSolver */ useRuleSolver,
		)
		defer stopper.Stop()
		gossiputil.NewStoreGossiper(g).GossipStores(multiDCStores, t)
		result1, err := a.AllocateTarget(
			multiDCConfig.Constraints,
			[]roachpb.ReplicaDescriptor{},
			firstRange,
			false,
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
			false,
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
			false,
		)
		if err == nil {
			t.Errorf("expected error on allocation without available stores: %+v", result3)
		}
	})
}

func TestAllocatorExistingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runToggleRuleSolver(t, func(useRuleSolver bool, t *testing.T) {
		stopper, g, _, a, _ := createTestAllocator(
			/* deterministic */ false,
			/* useRuleSolver */ useRuleSolver,
		)
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
			false,
		)
		if err != nil {
			t.Fatalf("Unable to perform allocation: %v", err)
		}
		if result.Node.NodeID != 3 || result.StoreID != 4 {
			t.Errorf("expected result to have node 3 and store 4: %+v", result)
		}
	})
}

// TestAllocatorRelaxConstraints verifies that attribute constraints
// will be relaxed in order to match nodes lacking required attributes,
// if necessary to find an allocation target.
func TestAllocatorRelaxConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("without rule solver", func(t *testing.T) {
		stopper, g, _, a, _ := createTestAllocator(
			/* deterministic */ false,
			/* useRuleSolver */ false,
		)
		defer stopper.Stop()
		gossiputil.NewStoreGossiper(g).GossipStores(multiDCStores, t)

		testCases := []struct {
			required         []config.Constraint // attribute strings
			existing         []int               // existing store/node ID
			relaxConstraints bool                // allow constraints to be relaxed?
			expID            int                 // expected store/node ID on allocate
			expErr           bool
		}{
			// The two stores in the system have attributes:
			//  storeID=1 {"a", "ssd"}
			//  storeID=2 {"b", "ssd"}
			{[]config.Constraint{{Value: "a"}, {Value: "ssd"}}, []int{}, true, 1, false},
			{[]config.Constraint{{Value: "a"}, {Value: "ssd"}}, []int{1}, true, 2, false},
			{[]config.Constraint{{Value: "a"}, {Value: "ssd"}}, []int{1}, false, 0, true},
			{[]config.Constraint{{Value: "a"}, {Value: "ssd"}}, []int{1, 2}, true, 0, true},
			{[]config.Constraint{{Value: "b"}, {Value: "ssd"}}, []int{}, true, 2, false},
			{[]config.Constraint{{Value: "b"}, {Value: "ssd"}}, []int{1}, true, 2, false},
			{[]config.Constraint{{Value: "b"}, {Value: "ssd"}}, []int{2}, false, 0, true},
			{[]config.Constraint{{Value: "b"}, {Value: "ssd"}}, []int{2}, true, 1, false},
			{[]config.Constraint{{Value: "b"}, {Value: "ssd"}}, []int{1, 2}, true, 0, true},
			{[]config.Constraint{{Value: "b"}, {Value: "hdd"}}, []int{}, true, 2, false},
			{[]config.Constraint{{Value: "b"}, {Value: "hdd"}}, []int{2}, true, 1, false},
			{[]config.Constraint{{Value: "b"}, {Value: "hdd"}}, []int{2}, false, 0, true},
			{[]config.Constraint{{Value: "b"}, {Value: "hdd"}}, []int{1, 2}, true, 0, true},
			{[]config.Constraint{{Value: "b"}, {Value: "ssd"}, {Value: "gpu"}}, []int{}, true, 2, false},
			{[]config.Constraint{{Value: "b"}, {Value: "hdd"}, {Value: "gpu"}}, []int{}, true, 2, false},
		}
		for i, test := range testCases {
			var existing []roachpb.ReplicaDescriptor
			for _, id := range test.existing {
				existing = append(existing, roachpb.ReplicaDescriptor{NodeID: roachpb.NodeID(id), StoreID: roachpb.StoreID(id)})
			}
			constraints := config.Constraints{Constraints: test.required}
			result, err := a.AllocateTarget(
				constraints,
				existing,
				firstRange,
				test.relaxConstraints,
			)
			if haveErr := (err != nil); haveErr != test.expErr {
				t.Errorf("%d: expected error %t; got %t: %s", i, test.expErr, haveErr, err)
			} else if err == nil && roachpb.StoreID(test.expID) != result.StoreID {
				t.Errorf("%d: expected result to have store %d; got %+v", i, test.expID, result)
			}
		}
	})

	t.Run("with rule solver", func(t *testing.T) {
		stopper, g, _, a, _ := createTestAllocator(
			/* deterministic */ false,
			/* useRuleSolver */ true,
		)
		defer stopper.Stop()
		gossiputil.NewStoreGossiper(g).GossipStores(multiDCStores, t)

		testCases := []struct {
			name        string
			constraints []config.Constraint
			existing    []int // existing store/node ID
			expID       int   // expected store/node ID on allocate
			expErr      bool
		}{
			// The two stores in the system have attributes:
			//  storeID=1 {"a", "ssd"}
			//  storeID=2 {"b", "ssd"}
			{
				name: "positive constraints (matching store 1)",
				constraints: []config.Constraint{
					{Value: "a"},
					{Value: "ssd"},
				},
				expID: 1,
			},
			{
				name: "positive constraints (matching store 2)",
				constraints: []config.Constraint{
					{Value: "b"},
					{Value: "ssd"},
				},
				expID: 2,
			},
			{
				name: "positive constraints (matching store 1) with existing replica (store 1)",
				constraints: []config.Constraint{
					{Value: "a"},
					{Value: "ssd"},
				},
				existing: []int{1},
				expID:    2,
			},
			{
				name: "positive constraints (matching store 1) with two existing replicas",
				constraints: []config.Constraint{
					{Value: "a"}, /* remove these?*/
					{Value: "ssd"},
				},
				existing: []int{1, 2},
				expErr:   true,
			},
			{
				name: "positive constraints (matching store 2) with two existing replicas",
				constraints: []config.Constraint{
					{Value: "b"},
					{Value: "ssd"},
				},
				existing: []int{1, 2},
				expErr:   true,
			},
			{
				name: "required constraints (matching store 1) with existing replica (store 1)",
				constraints: []config.Constraint{
					{Value: "a", Type: config.Constraint_REQUIRED},
					{Value: "ssd", Type: config.Constraint_REQUIRED},
				},
				existing: []int{1},
				expErr:   true,
			},
			{
				name: "required constraints (matching store 2) with exiting replica (store 2)",
				constraints: []config.Constraint{
					{Value: "b", Type: config.Constraint_REQUIRED},
					{Value: "ssd", Type: config.Constraint_REQUIRED},
				},
				existing: []int{2},
				expErr:   true,
			},
			{
				name: "positive constraints (matching store 2) with existing replica (store 1)",
				constraints: []config.Constraint{
					{Value: "b"},
					{Value: "ssd"},
				},
				existing: []int{1},
				expID:    2,
			},
			{
				name: "positive constraints (matching store 2) with existing replica (store 2)",
				constraints: []config.Constraint{
					{Value: "b"},
					{Value: "ssd"},
				},
				existing: []int{2},
				expID:    1,
			},
			{
				name: "positive constraints (half matching store 2)",
				constraints: []config.Constraint{
					{Value: "b"},
					{Value: "hdd"},
				},
				expID: 2,
			},
			{
				name: "positive constraints (half matching store 2) with existing replica (store 2)",
				constraints: []config.Constraint{
					{Value: "b"},
					{Value: "hdd"},
				},
				existing: []int{2},
				expID:    1,
			},
			{
				name: "required constraints (half matching store 2) with existing replica (store 2)",
				constraints: []config.Constraint{
					{Value: "b", Type: config.Constraint_REQUIRED},
					{Value: "hdd", Type: config.Constraint_REQUIRED},
				},
				existing: []int{2},
				expErr:   true,
			},
			{
				name: "positive constraints (half matching store 2) with two existing replica",
				constraints: []config.Constraint{
					{Value: "b"},
					{Value: "hdd"},
				},
				existing: []int{1, 2},
				expErr:   true,
			},
			{
				name: "positive constraints (2/3 matching store 2)",
				constraints: []config.Constraint{
					{Value: "b"},
					{Value: "ssd"},
					{Value: "gpu"},
				},
				expID: 2,
			},
			{
				name: "positive constraints (1/3 matching store 2)",
				constraints: []config.Constraint{
					{Value: "b"},
					{Value: "hdd"},
					{Value: "gpu"},
				},
				expID: 2,
			},
		}
		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				var existing []roachpb.ReplicaDescriptor
				for _, id := range test.existing {
					existing = append(existing, roachpb.ReplicaDescriptor{NodeID: roachpb.NodeID(id), StoreID: roachpb.StoreID(id)})
				}
				result, err := a.AllocateTarget(
					config.Constraints{Constraints: test.constraints},
					existing,
					firstRange,
					false,
				)
				if haveErr := (err != nil); haveErr != test.expErr {
					t.Errorf("expected error %t; got %t: %s", test.expErr, haveErr, err)
				} else if err == nil && roachpb.StoreID(test.expID) != result.StoreID {
					t.Errorf("expected result to have store %d; got %+v", test.expID, result)
				}
			})
		}
	})

}

// TestAllocatorRebalance verifies that rebalance targets are chosen
// randomly from amongst stores over the minAvailCapacityThreshold.
func TestAllocatorRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	runToggleRuleSolver(t, func(useRuleSolver bool, t *testing.T) {
		stopper, g, _, a, _ := createTestAllocator(
			/* deterministic */ false,
			/* useRuleSolver */ useRuleSolver,
		)
		defer stopper.Stop()

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
	})
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
		surplus := int32(math.Ceil(float64(mean)*rebalanceThreshold + 1))
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
		deficit := int32(math.Ceil(float64(mean)*rebalanceThreshold + 1))
		stores[0].rangeCount -= deficit
		for i := 1; i < len(stores); i++ {
			stores[i].rangeCount += int32(math.Ceil(float64(deficit) / float64(len(stores)-1)))
			stores[i].shouldRebalanceFrom = true
		}
		return stores
	}

	runToggleRuleSolver(t, func(useRuleSolver bool, t *testing.T) {
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
			// Deterministic is required when stressing as test case 8 may rebalance
			// to different configurations.
			stopper, g, _, a, _ := createTestAllocator(
				/* deterministic */ true,
				/* UseRuleSolver */ useRuleSolver,
			)
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
	})
}

// TestAllocatorRebalanceByCount verifies that rebalance targets are
// chosen by range counts in the event that available capacities
// exceed the maxAvailCapacityThreshold.
func TestAllocatorRebalanceByCount(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	runToggleRuleSolver(t, func(useRuleSolver bool, t *testing.T) {
		stopper, g, _, a, _ := createTestAllocator(
			/* deterministic */ false,
			/* useRuleSolver */ useRuleSolver,
		)
		defer stopper.Stop()

		gossiputil.NewStoreGossiper(g).GossipStores(stores, t)

		// Every rebalance target must be store 4 (or nil for case of missing the only option).
		for i := 0; i < 10; i++ {
			result, err := a.RebalanceTarget(
				config.Constraints{},
				[]roachpb.ReplicaDescriptor{{StoreID: stores[0].StoreID}},
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
	})
}

// TestAllocatorRemoveTarget verifies that the replica chosen by RemoveTarget is
// the one with the lowest capacity.
func TestAllocatorRemoveTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
			Capacity: roachpb.StoreCapacity{Capacity: 100, Available: 65, RangeCount: 14},
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

	runToggleRuleSolver(t, func(useRuleSolver bool, t *testing.T) {
		stopper, g, _, a, _ := createTestAllocator(
			/* deterministic */ false,
			/* useRuleSolver */ useRuleSolver,
		)
		defer stopper.Stop()
		sg := gossiputil.NewStoreGossiper(g)
		sg.GossipStores(stores, t)

		// Exclude store 2 as a removal target so that only store 3 is a candidate.
		targetRepl, err := a.RemoveTarget(config.Constraints{}, replicas, stores[1].StoreID)
		if err != nil {
			t.Fatal(err)
		}
		if a, e := targetRepl, replicas[2]; a != e {
			t.Fatalf("RemoveTarget did not select expected replica; expected %v, got %v", e, a)
		}

		// Now exclude store 3 so that only store 2 is a candidate.
		targetRepl, err = a.RemoveTarget(config.Constraints{}, replicas, stores[2].StoreID)
		if err != nil {
			t.Fatal(err)
		}
		if a, e := targetRepl, replicas[1]; a != e {
			t.Fatalf("RemoveTarget did not select expected replica; expected %v, got %v", e, a)
		}

		if !useRuleSolver {
			var counts [4]int
			for i := 0; i < 100; i++ {
				// Exclude store 1 as a removal target. We should see stores 2 and 3
				// randomly selected as the removal target.
				targetRepl, err := a.RemoveTarget(config.Constraints{}, replicas, stores[0].StoreID)
				if err != nil {
					t.Fatal(err)
				}
				counts[targetRepl.StoreID-1]++
			}
			if counts[0] != 0 || counts[1] == 0 || counts[2] == 0 || counts[3] != 0 {
				t.Fatalf("unexpected removal target counts: %d", counts)
			}
		}
	})
}

func TestAllocatorComputeAction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Each test case should describe a repair situation which has a lower
	// priority than the previous test case.
	testCases := []struct {
		zone           config.ZoneConfig
		desc           roachpb.RangeDescriptor
		expectedAction AllocatorAction
	}{
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

	runToggleRuleSolver(t, func(useRuleSolver bool, t *testing.T) {
		stopper, _, sp, a, _ := createTestAllocator(
			/* deterministic */ false,
			/* useRuleSolver */ useRuleSolver,
		)
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
	})
}

// TestAllocatorComputeActionNoStorePool verifies that
// ComputeAction returns AllocatorNoop when storePool is nil.
func TestAllocatorComputeActionNoStorePool(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runToggleRuleSolver(t, func(useRuleSolver bool, t *testing.T) {
		a := MakeAllocator(nil /* storePool */, AllocatorOptions{UseRuleSolver: useRuleSolver})
		action, priority := a.ComputeAction(config.ZoneConfig{}, nil)
		if action != AllocatorNoop {
			t.Errorf("expected AllocatorNoop, but got %v", action)
		}
		if priority != 0 {
			t.Errorf("expected priority 0, but got %f", priority)
		}
	})
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

	runToggleRuleSolver(t, func(useRuleSolver bool, t *testing.T) {
		stopper, g, _, a, _ := createTestAllocator(
			/* deterministic */ false,
			/* useRuleSolver */ false,
		)
		defer stopper.Stop()

		// First test to make sure we would send the replica to purgatory.
		_, err := a.AllocateTarget(
			simpleZoneConfig.Constraints,
			[]roachpb.ReplicaDescriptor{},
			firstRange,
			false,
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
			false,
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
			false,
		)
		if _, ok := err.(purgatoryError); ok {
			t.Fatalf("expected a non purgatory error, got: %v", err)
		}
	})
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

func exampleRebalancingCore(useRuleSolver bool) {
	stopper := stop.NewStopper()
	defer stopper.Stop()

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)

	// Model a set of stores in a cluster,
	// randomly adding / removing stores and adding bytes.
	rpcContext := rpc.NewContext(
		log.AmbientContext{},
		&base.Config{Insecure: true},
		clock,
		stopper,
	)
	server := rpc.NewServer(rpcContext) // never started
	g := gossip.NewTest(1, rpcContext, server, nil, stopper, metric.NewRegistry())
	// Deterministic must be set as this test is comparing the exact output
	// after each rebalance.
	sp := NewStorePool(
		log.AmbientContext{},
		g,
		clock,
		nil,
		TestTimeUntilStoreDeadOff,
		stopper,
		/* deterministic */ true,
	)
	alloc := MakeAllocator(sp, AllocatorOptions{
		AllowRebalance: true,
		UseRuleSolver:  useRuleSolver,
	})

	var wg sync.WaitGroup
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix), func(_ string, _ roachpb.Value) { wg.Done() })

	const generations = 100
	const nodes = 20
	const printGenerations = generations / 2

	// Initialize testStores.
	var testStores [nodes]testStore
	for i := 0; i < len(testStores); i++ {
		testStores[i].StoreID = roachpb.StoreID(i)
		testStores[i].Node = roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)}
		testStores[i].Capacity = roachpb.StoreCapacity{Capacity: 1 << 30, Available: 1 << 30}
	}
	// Initialize the cluster with a single range.
	testStores[0].add(alloc.randGen.Int63n(1 << 20))

	table := tablewriter.NewWriter(os.Stdout)
	table.SetAutoFormatHeaders(false)
	table.SetAlignment(tablewriter.ALIGN_RIGHT)

	header := make([]string, len(testStores)+1)
	header[0] = "gen"
	for i := 0; i < len(testStores); i++ {
		header[i+1] = fmt.Sprintf("store %d", i)
	}
	table.SetHeader(header)

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
				testStores[j].rebalance(&testStores[int(target.StoreID)], alloc.randGen.Int63n(1<<20))
			}
		}

		if i%(generations/printGenerations) == 0 {
			var totalBytes int64
			for j := 0; j < len(testStores); j++ {
				totalBytes += testStores[j].Capacity.Capacity - testStores[j].Capacity.Available
			}
			row := make([]string, len(testStores)+1)
			row[0] = fmt.Sprintf("%d", i)
			for j := 0; j < len(testStores); j++ {
				ts := testStores[j]
				bytes := ts.Capacity.Capacity - ts.Capacity.Available
				row[j+1] = fmt.Sprintf("%3d %3d%%", ts.Capacity.RangeCount, (100*bytes)/totalBytes)
			}
			table.Append(row)
		}
	}

	var totBytes int64
	var totRanges int32
	for i := 0; i < len(testStores); i++ {
		totBytes += testStores[i].Capacity.Capacity - testStores[i].Capacity.Available
		totRanges += testStores[i].Capacity.RangeCount
	}
	table.Render()
	fmt.Printf("Total bytes=%d, ranges=%d\n", totBytes, totRanges)
}

func Example_rebalancing() {
	exampleRebalancingCore(false /* useRuleSolver */)
	// Output:
	// +-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
	// | gen | store 0  | store 1  | store 2  | store 3  | store 4  | store 5  | store 6  | store 7  | store 8  | store 9  | store 10 | store 11 | store 12 | store 13 | store 14 | store 15 | store 16 | store 17 | store 18 | store 19 |
	// +-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
	// |   0 |   2 100% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   2 |   4 100% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   4 |   6 100% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   6 |   8 100% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   8 |  10 100% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |  10 |  10  80% |   0   0% |   0   0% |   0   0% |   1   1% |   0   0% |   0   0% |   1   9% |   0   0% |   0   0% |   0   0% |   0   0% |   1   8% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |  12 |  10  47% |   1   5% |   2   4% |   0   0% |   2   3% |   1   3% |   0   0% |   1  10% |   1   0% |   0   0% |   1   0% |   1   8% |   1   8% |   0   0% |   1   2% |   0   0% |   1   1% |   0   0% |   1   2% |   0   0% |
	// |  14 |  10  17% |   3   6% |   3   3% |   0   0% |   3   5% |   3   6% |   0   0% |   3   9% |   3   4% |   2   2% |   3   4% |   3   6% |   3   7% |   2   4% |   3   4% |   0   0% |   3   4% |   1   3% |   3   3% |   2   5% |
	// |  16 |  10  12% |   4   5% |   4   3% |   4   3% |   4   4% |   4   3% |   4   3% |   4   7% |   4   3% |   4   5% |   4   4% |   4   5% |   4   6% |   4   5% |   4   1% |   6   6% |   4   2% |   4   4% |   4   3% |   4   7% |
	// |  18 |  12  10% |   6   4% |   6   4% |   6   3% |   6   4% |   6   5% |   6   4% |   6   6% |   6   3% |   6   4% |   6   5% |   6   6% |   6   6% |   6   5% |   6   2% |   8   5% |   6   2% |   6   4% |   6   4% |   6   5% |
	// |  20 |  14   8% |   8   4% |   8   5% |   8   3% |   8   5% |   8   4% |   8   4% |   8   6% |   8   3% |   8   4% |   8   5% |   8   5% |   8   7% |   8   4% |   8   2% |  10   6% |   8   3% |   8   3% |   8   4% |   8   5% |
	// |  22 |  16   7% |  10   4% |  10   5% |  10   4% |  10   5% |  10   4% |  10   4% |  10   6% |  10   4% |  10   4% |  10   5% |  10   5% |  10   6% |  10   4% |  10   3% |  12   5% |  10   4% |  10   4% |  10   3% |  10   5% |
	// |  24 |  18   7% |  12   4% |  12   5% |  12   4% |  12   5% |  12   5% |  12   4% |  12   6% |  12   4% |  12   4% |  12   5% |  12   5% |  12   6% |  12   4% |  12   3% |  14   5% |  12   4% |  12   4% |  12   4% |  12   5% |
	// |  26 |  20   7% |  14   4% |  14   5% |  14   3% |  14   5% |  14   5% |  14   4% |  14   5% |  14   4% |  14   4% |  14   5% |  14   4% |  14   6% |  14   4% |  14   3% |  16   5% |  14   4% |  14   5% |  14   4% |  14   5% |
	// |  28 |  22   6% |  16   4% |  16   5% |  16   3% |  16   5% |  16   5% |  16   4% |  16   5% |  16   4% |  16   4% |  16   4% |  16   4% |  16   6% |  16   4% |  16   3% |  18   5% |  16   4% |  16   5% |  16   4% |  16   5% |
	// |  30 |  24   6% |  18   4% |  18   5% |  18   3% |  18   5% |  18   5% |  18   4% |  18   5% |  18   4% |  18   4% |  18   4% |  18   4% |  18   6% |  18   4% |  18   3% |  20   5% |  18   4% |  18   5% |  18   4% |  18   5% |
	// |  32 |  26   6% |  20   4% |  20   6% |  20   3% |  20   5% |  20   5% |  20   4% |  20   5% |  20   4% |  20   4% |  20   4% |  20   4% |  20   6% |  20   4% |  20   4% |  22   4% |  20   5% |  20   5% |  20   4% |  20   5% |
	// |  34 |  28   6% |  22   4% |  22   6% |  22   3% |  22   5% |  22   5% |  22   4% |  22   4% |  22   4% |  22   4% |  22   4% |  22   5% |  22   6% |  22   4% |  22   3% |  24   4% |  22   5% |  22   5% |  22   4% |  22   5% |
	// |  36 |  30   6% |  24   4% |  24   6% |  24   4% |  24   5% |  24   4% |  24   4% |  24   5% |  24   3% |  24   4% |  24   4% |  24   5% |  24   6% |  24   4% |  24   3% |  26   4% |  24   5% |  24   5% |  24   4% |  24   5% |
	// |  38 |  32   6% |  26   4% |  26   6% |  26   4% |  26   5% |  26   4% |  26   4% |  26   4% |  26   3% |  26   4% |  26   4% |  26   5% |  26   6% |  26   3% |  26   3% |  28   4% |  26   5% |  26   6% |  26   4% |  26   5% |
	// |  40 |  34   6% |  28   4% |  28   6% |  28   4% |  28   5% |  28   4% |  28   4% |  28   5% |  28   4% |  28   4% |  28   4% |  28   4% |  28   6% |  28   3% |  28   3% |  30   4% |  28   5% |  28   5% |  28   4% |  28   5% |
	// |  42 |  36   6% |  30   4% |  30   5% |  30   4% |  30   4% |  30   4% |  30   4% |  30   5% |  30   4% |  30   4% |  30   4% |  30   4% |  30   6% |  30   4% |  30   3% |  32   4% |  30   5% |  30   6% |  30   4% |  30   5% |
	// |  44 |  38   6% |  32   4% |  32   5% |  32   4% |  32   5% |  32   4% |  32   4% |  32   5% |  32   4% |  32   4% |  32   4% |  32   4% |  32   6% |  32   3% |  32   4% |  34   4% |  32   5% |  32   6% |  32   4% |  32   5% |
	// |  46 |  40   6% |  34   4% |  34   5% |  34   4% |  34   5% |  34   4% |  34   4% |  34   5% |  34   4% |  34   4% |  34   4% |  34   4% |  34   6% |  34   3% |  34   4% |  36   4% |  34   5% |  34   5% |  34   4% |  34   5% |
	// |  48 |  42   6% |  36   4% |  36   5% |  36   4% |  36   4% |  36   4% |  36   4% |  36   5% |  36   4% |  36   4% |  36   4% |  36   5% |  36   5% |  36   3% |  36   4% |  38   4% |  36   5% |  36   5% |  36   4% |  36   5% |
	// |  50 |  44   6% |  38   4% |  38   5% |  38   4% |  38   5% |  38   4% |  38   4% |  38   4% |  38   4% |  38   4% |  38   4% |  38   4% |  38   5% |  38   3% |  38   4% |  40   5% |  38   5% |  38   5% |  38   4% |  38   5% |
	// |  52 |  46   6% |  40   5% |  40   5% |  40   4% |  40   5% |  40   4% |  40   4% |  40   5% |  40   4% |  40   4% |  40   4% |  40   4% |  40   5% |  40   3% |  40   4% |  42   5% |  40   5% |  40   5% |  40   4% |  40   5% |
	// |  54 |  48   6% |  42   5% |  42   5% |  42   4% |  42   5% |  42   4% |  42   4% |  42   4% |  42   4% |  42   4% |  42   4% |  42   4% |  42   5% |  42   3% |  42   4% |  44   5% |  42   5% |  42   5% |  42   4% |  42   5% |
	// |  56 |  50   6% |  44   5% |  44   5% |  44   4% |  44   5% |  44   5% |  44   4% |  44   5% |  44   4% |  44   4% |  44   4% |  44   5% |  44   5% |  44   3% |  44   4% |  46   4% |  44   5% |  44   5% |  44   4% |  44   5% |
	// |  58 |  52   6% |  46   5% |  46   5% |  46   4% |  46   5% |  46   5% |  46   4% |  46   5% |  46   4% |  46   4% |  46   5% |  46   4% |  46   5% |  46   3% |  46   4% |  48   4% |  46   5% |  46   5% |  46   4% |  46   5% |
	// |  60 |  54   5% |  48   4% |  48   5% |  48   4% |  48   5% |  48   5% |  48   4% |  48   5% |  48   4% |  48   4% |  48   4% |  48   4% |  48   5% |  48   4% |  48   4% |  50   4% |  48   5% |  48   5% |  48   4% |  48   5% |
	// |  62 |  56   5% |  50   4% |  50   5% |  50   4% |  50   5% |  50   4% |  50   4% |  50   5% |  50   4% |  50   4% |  50   4% |  50   4% |  50   5% |  50   4% |  50   4% |  52   4% |  50   5% |  50   5% |  50   4% |  50   5% |
	// |  64 |  58   5% |  52   5% |  52   5% |  52   4% |  52   5% |  52   4% |  52   4% |  52   5% |  52   4% |  52   4% |  52   4% |  52   4% |  52   5% |  52   4% |  52   4% |  54   4% |  52   5% |  52   5% |  52   4% |  52   5% |
	// |  66 |  60   5% |  54   5% |  54   5% |  54   4% |  54   5% |  54   4% |  54   4% |  54   5% |  54   4% |  54   4% |  54   4% |  54   4% |  54   5% |  54   4% |  54   4% |  56   4% |  54   5% |  54   5% |  54   4% |  54   5% |
	// |  68 |  62   5% |  56   5% |  56   5% |  56   4% |  56   5% |  56   4% |  56   4% |  56   5% |  56   4% |  56   5% |  56   4% |  56   4% |  56   5% |  56   4% |  56   4% |  58   4% |  56   5% |  56   5% |  56   4% |  56   5% |
	// |  70 |  64   5% |  58   5% |  58   5% |  58   4% |  58   5% |  58   4% |  58   4% |  58   5% |  58   4% |  58   4% |  58   4% |  58   4% |  58   5% |  58   4% |  58   4% |  60   4% |  58   5% |  58   5% |  58   4% |  58   5% |
	// |  72 |  66   5% |  60   5% |  60   5% |  60   4% |  60   5% |  60   4% |  60   4% |  60   5% |  60   4% |  60   4% |  60   4% |  60   4% |  60   5% |  60   4% |  60   4% |  62   4% |  60   5% |  60   5% |  60   4% |  60   5% |
	// |  74 |  68   5% |  62   5% |  62   5% |  62   4% |  62   5% |  62   4% |  62   4% |  62   5% |  62   4% |  62   5% |  62   4% |  62   4% |  62   5% |  62   4% |  62   4% |  64   4% |  62   5% |  62   5% |  62   4% |  62   5% |
	// |  76 |  70   5% |  64   5% |  64   5% |  64   4% |  64   5% |  64   4% |  64   4% |  64   5% |  64   4% |  64   5% |  64   4% |  64   4% |  64   5% |  64   4% |  64   4% |  66   4% |  64   5% |  64   5% |  64   4% |  64   5% |
	// |  78 |  72   5% |  66   5% |  66   5% |  66   4% |  66   5% |  66   4% |  66   4% |  66   5% |  66   4% |  66   5% |  66   5% |  66   4% |  66   5% |  66   4% |  66   4% |  68   4% |  66   5% |  66   5% |  66   4% |  66   5% |
	// |  80 |  74   5% |  68   5% |  68   5% |  68   4% |  68   5% |  68   4% |  68   4% |  68   5% |  68   4% |  68   5% |  68   5% |  68   4% |  68   5% |  68   4% |  68   4% |  70   4% |  68   5% |  68   5% |  68   4% |  68   5% |
	// |  82 |  76   5% |  70   5% |  70   5% |  70   4% |  70   5% |  70   4% |  70   5% |  70   5% |  70   4% |  70   5% |  70   4% |  70   4% |  70   5% |  70   4% |  70   4% |  72   4% |  70   5% |  70   5% |  70   4% |  70   5% |
	// |  84 |  78   5% |  72   5% |  72   5% |  72   4% |  72   5% |  72   4% |  72   5% |  72   5% |  72   4% |  72   5% |  72   4% |  72   4% |  72   5% |  72   4% |  72   4% |  74   4% |  72   5% |  72   5% |  72   4% |  72   5% |
	// |  86 |  80   5% |  74   5% |  74   5% |  74   4% |  74   4% |  74   4% |  74   5% |  74   5% |  74   4% |  74   5% |  74   4% |  74   4% |  74   5% |  74   4% |  74   4% |  76   5% |  74   5% |  74   5% |  74   4% |  74   5% |
	// |  88 |  82   5% |  76   5% |  76   5% |  76   4% |  76   4% |  76   4% |  76   5% |  76   5% |  76   4% |  76   5% |  76   4% |  76   4% |  76   5% |  76   4% |  76   4% |  78   5% |  76   5% |  76   5% |  76   4% |  76   5% |
	// |  90 |  84   5% |  78   5% |  78   5% |  78   4% |  78   4% |  78   4% |  78   5% |  78   5% |  78   4% |  78   5% |  78   4% |  78   4% |  78   5% |  78   4% |  78   4% |  80   5% |  78   5% |  78   5% |  78   4% |  78   5% |
	// |  92 |  86   5% |  80   5% |  80   5% |  80   4% |  80   4% |  80   4% |  80   4% |  80   5% |  80   4% |  80   5% |  80   4% |  80   4% |  80   5% |  80   4% |  80   4% |  82   5% |  80   5% |  80   5% |  80   4% |  80   5% |
	// |  94 |  88   5% |  82   5% |  82   5% |  82   4% |  82   5% |  82   4% |  82   4% |  82   5% |  82   4% |  82   5% |  82   4% |  82   4% |  82   5% |  82   4% |  82   4% |  84   4% |  82   5% |  82   5% |  82   4% |  82   5% |
	// |  96 |  90   5% |  84   5% |  84   5% |  84   4% |  84   4% |  84   4% |  84   5% |  84   5% |  84   4% |  84   5% |  84   4% |  84   4% |  84   5% |  84   4% |  84   4% |  86   5% |  84   5% |  84   5% |  84   4% |  84   5% |
	// |  98 |  92   5% |  86   5% |  86   5% |  86   4% |  86   4% |  86   4% |  86   5% |  86   5% |  86   4% |  86   5% |  86   4% |  86   4% |  86   5% |  86   4% |  86   4% |  88   4% |  86   5% |  86   5% |  86   4% |  86   5% |
	// +-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
	// Total bytes=915403982, ranges=1748
}

func Example_rebalancingWithRuleSolver() {
	exampleRebalancingCore(true /* useRuleSolver */)

	// Output:
	// +-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
	// | gen | store 0  | store 1  | store 2  | store 3  | store 4  | store 5  | store 6  | store 7  | store 8  | store 9  | store 10 | store 11 | store 12 | store 13 | store 14 | store 15 | store 16 | store 17 | store 18 | store 19 |
	// +-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
	// |   0 |   1  88% |   1  11% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |
	// |   2 |   1  32% |   2  24% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   2  21% |   2  20% |
	// |   4 |   2   9% |   2  22% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   0   0% |   3   5% |   1   0% |   2   9% |   2  19% |   2  21% |   3   9% |
	// |   6 |   3   5% |   2   5% |   0   0% |   0   0% |   0   0% |   0   0% |   6  15% |   1   3% |   0   0% |   0   0% |   0   0% |   0   0% |   2   4% |   7  27% |   3   1% |   2   3% |   2   6% |   2  10% |   2   9% |   3   6% |
	// |   8 |   3   4% |   3   5% |   0   0% |   0   0% |   0   0% |   0   0% |   6   9% |   3   2% |   7   8% |   7   6% |   3   7% |   0   0% |   3   4% |   7  17% |   4   3% |   3   2% |   3   6% |   3   9% |   3   6% |   3   5% |
	// |  10 |   5   3% |   5   4% |   0   0% |   0   0% |   5   4% |   5   6% |   6   6% |   5   4% |   7   5% |   7   5% |   5   8% |   0   0% |   5   5% |   7  11% |   5   3% |   5   4% |   5   5% |   5   9% |   5   5% |   5   5% |
	// |  12 |   6   3% |   6   3% |   5   5% |  15  14% |   6   3% |   6   6% |   6   4% |   6   3% |   7   2% |   7   3% |   6   7% |   3   1% |   6   4% |   7   7% |   6   3% |   6   2% |   6   3% |   6   7% |   6   4% |   6   5% |
	// |  14 |   8   4% |   8   4% |   8   6% |  15  10% |   8   2% |   8   4% |   8   5% |   8   4% |   8   3% |   8   2% |   8   6% |   9   5% |   8   3% |   8   5% |   8   4% |   8   3% |   8   4% |   8   7% |   8   4% |   8   6% |
	// |  16 |  11   5% |  10   4% |  10   5% |  15   8% |  10   3% |  10   5% |  10   4% |  10   4% |  10   3% |  10   2% |  10   5% |  11   4% |  10   4% |  10   5% |  10   4% |  10   4% |  10   4% |  10   6% |  10   4% |  11   6% |
	// |  18 |  13   4% |  12   4% |  12   6% |  15   7% |  12   3% |  12   5% |  12   4% |  12   4% |  12   4% |  12   2% |  12   6% |  13   5% |  12   4% |  12   5% |  12   5% |  12   4% |  13   5% |  13   6% |  12   4% |  13   6% |
	// |  20 |  15   4% |  14   4% |  14   6% |  16   6% |  14   3% |  14   5% |  14   5% |  14   4% |  14   4% |  14   3% |  14   6% |  15   4% |  14   4% |  14   4% |  14   5% |  15   4% |  15   5% |  15   6% |  14   4% |  15   5% |
	// |  22 |  17   4% |  16   4% |  16   6% |  18   6% |  16   3% |  16   5% |  16   5% |  16   4% |  16   4% |  16   3% |  16   6% |  17   4% |  16   4% |  16   4% |  16   5% |  17   4% |  17   5% |  17   6% |  16   5% |  17   5% |
	// |  24 |  19   4% |  18   4% |  18   6% |  20   6% |  18   3% |  18   5% |  18   5% |  18   4% |  18   4% |  18   3% |  18   5% |  19   4% |  18   4% |  18   4% |  18   5% |  19   4% |  19   5% |  19   6% |  18   5% |  19   5% |
	// |  26 |  21   4% |  20   4% |  20   5% |  22   5% |  20   3% |  20   5% |  20   4% |  20   4% |  20   4% |  20   4% |  20   5% |  21   5% |  20   4% |  20   4% |  20   5% |  21   3% |  21   5% |  21   6% |  20   5% |  21   5% |
	// |  28 |  23   4% |  22   4% |  22   5% |  24   5% |  22   3% |  22   5% |  22   5% |  22   4% |  22   4% |  22   4% |  22   5% |  23   5% |  22   4% |  22   4% |  22   5% |  23   3% |  23   5% |  23   5% |  22   5% |  23   5% |
	// |  30 |  25   4% |  24   4% |  24   5% |  26   5% |  24   3% |  24   5% |  24   4% |  24   4% |  24   4% |  24   4% |  24   5% |  25   4% |  24   4% |  24   5% |  24   5% |  25   4% |  25   5% |  25   6% |  24   5% |  25   5% |
	// |  32 |  27   4% |  26   5% |  26   5% |  28   5% |  26   3% |  26   5% |  26   4% |  26   4% |  26   4% |  26   4% |  26   5% |  27   4% |  26   4% |  26   4% |  26   5% |  27   4% |  27   5% |  27   5% |  26   5% |  27   4% |
	// |  34 |  29   4% |  28   4% |  28   5% |  30   5% |  28   3% |  28   5% |  28   4% |  28   4% |  28   4% |  28   4% |  28   5% |  29   5% |  28   4% |  28   4% |  28   5% |  29   4% |  29   5% |  29   5% |  28   5% |  29   5% |
	// |  36 |  31   4% |  30   4% |  30   5% |  32   5% |  30   3% |  30   5% |  30   4% |  30   4% |  30   4% |  30   5% |  30   5% |  31   4% |  30   4% |  30   4% |  30   5% |  31   4% |  31   5% |  31   5% |  30   5% |  31   5% |
	// |  38 |  33   4% |  32   4% |  32   5% |  34   5% |  32   4% |  32   4% |  32   4% |  32   4% |  32   4% |  32   5% |  32   5% |  33   4% |  32   5% |  32   4% |  32   5% |  33   4% |  33   6% |  33   5% |  32   5% |  33   5% |
	// |  40 |  35   4% |  34   5% |  34   5% |  36   5% |  34   4% |  34   5% |  34   4% |  34   4% |  34   4% |  34   5% |  34   5% |  35   4% |  34   5% |  34   4% |  34   5% |  35   4% |  35   6% |  35   5% |  34   5% |  35   5% |
	// |  42 |  37   4% |  36   4% |  36   5% |  38   5% |  36   4% |  36   5% |  36   4% |  36   4% |  36   4% |  36   4% |  36   5% |  37   5% |  36   5% |  36   4% |  36   5% |  37   4% |  37   5% |  37   5% |  36   5% |  37   5% |
	// |  44 |  39   4% |  38   4% |  38   5% |  40   5% |  38   4% |  38   5% |  38   4% |  38   4% |  38   4% |  38   4% |  38   5% |  39   4% |  38   5% |  38   4% |  38   5% |  39   4% |  39   5% |  39   5% |  38   5% |  39   5% |
	// |  46 |  41   4% |  40   4% |  40   5% |  42   5% |  40   4% |  40   5% |  40   4% |  40   4% |  40   4% |  40   4% |  40   5% |  41   4% |  40   5% |  40   4% |  40   5% |  41   4% |  41   5% |  41   5% |  40   5% |  41   5% |
	// |  48 |  43   4% |  42   4% |  42   5% |  44   5% |  42   4% |  42   5% |  42   5% |  42   4% |  42   4% |  42   4% |  42   5% |  43   4% |  42   5% |  42   4% |  42   5% |  43   4% |  43   5% |  43   5% |  42   5% |  43   5% |
	// |  50 |  45   4% |  44   4% |  44   5% |  46   5% |  44   4% |  44   5% |  44   5% |  44   4% |  44   4% |  44   4% |  44   5% |  45   4% |  44   5% |  44   4% |  44   4% |  45   4% |  45   5% |  45   5% |  44   5% |  45   5% |
	// |  52 |  47   4% |  46   4% |  46   5% |  48   5% |  46   4% |  46   5% |  46   5% |  46   4% |  46   4% |  46   4% |  46   5% |  47   5% |  46   5% |  46   4% |  46   4% |  47   4% |  47   5% |  47   5% |  46   5% |  47   5% |
	// |  54 |  49   4% |  48   4% |  48   5% |  50   5% |  48   4% |  48   5% |  48   5% |  48   4% |  48   4% |  48   4% |  48   5% |  49   5% |  48   5% |  48   4% |  48   4% |  49   4% |  49   5% |  49   5% |  48   5% |  49   5% |
	// |  56 |  51   4% |  50   4% |  50   5% |  52   5% |  50   4% |  50   5% |  50   5% |  50   5% |  50   4% |  50   4% |  50   5% |  51   5% |  50   5% |  50   4% |  50   4% |  51   4% |  51   5% |  51   5% |  50   5% |  51   5% |
	// |  58 |  53   4% |  52   4% |  52   5% |  54   5% |  52   4% |  52   5% |  52   5% |  52   5% |  52   4% |  52   4% |  52   5% |  53   5% |  52   5% |  52   4% |  52   4% |  53   4% |  53   5% |  53   5% |  52   5% |  53   5% |
	// |  60 |  55   4% |  54   4% |  54   5% |  56   5% |  54   4% |  54   5% |  54   5% |  54   5% |  54   4% |  54   4% |  54   5% |  55   5% |  54   5% |  54   4% |  54   4% |  55   4% |  55   5% |  55   5% |  54   5% |  55   5% |
	// |  62 |  57   4% |  56   4% |  56   5% |  58   5% |  56   4% |  56   5% |  56   5% |  56   5% |  56   4% |  56   4% |  56   5% |  57   5% |  56   5% |  56   4% |  56   5% |  57   4% |  57   5% |  57   5% |  56   5% |  57   5% |
	// |  64 |  59   4% |  58   4% |  58   5% |  60   5% |  58   4% |  58   5% |  58   4% |  58   5% |  58   4% |  58   4% |  58   5% |  59   4% |  58   5% |  58   4% |  58   5% |  59   4% |  59   5% |  59   5% |  58   5% |  59   5% |
	// |  66 |  61   4% |  60   4% |  60   5% |  62   5% |  60   4% |  60   5% |  60   4% |  60   5% |  60   4% |  60   4% |  60   5% |  61   4% |  60   5% |  60   4% |  60   5% |  61   4% |  61   5% |  61   5% |  60   5% |  61   5% |
	// |  68 |  63   4% |  62   4% |  62   5% |  64   5% |  62   4% |  62   5% |  62   4% |  62   5% |  62   4% |  62   4% |  62   5% |  63   4% |  62   5% |  62   4% |  62   5% |  63   4% |  63   5% |  63   5% |  62   5% |  63   5% |
	// |  70 |  65   4% |  64   4% |  64   5% |  66   5% |  64   4% |  64   5% |  64   4% |  64   5% |  64   5% |  64   4% |  64   5% |  65   4% |  64   5% |  64   4% |  64   5% |  65   4% |  65   5% |  65   5% |  64   5% |  65   5% |
	// |  72 |  67   4% |  66   4% |  66   5% |  68   5% |  66   4% |  66   5% |  66   4% |  66   5% |  66   5% |  66   4% |  66   5% |  67   4% |  66   5% |  66   4% |  66   5% |  67   4% |  67   5% |  67   5% |  66   5% |  67   5% |
	// |  74 |  69   4% |  68   4% |  68   5% |  70   5% |  68   4% |  68   5% |  68   4% |  68   5% |  68   5% |  68   4% |  68   5% |  69   4% |  68   5% |  68   4% |  68   5% |  69   4% |  69   5% |  69   5% |  68   5% |  69   5% |
	// |  76 |  71   4% |  70   4% |  70   5% |  72   5% |  70   4% |  70   5% |  70   4% |  70   4% |  70   5% |  70   4% |  70   5% |  71   4% |  70   5% |  70   4% |  70   5% |  71   4% |  71   5% |  71   5% |  70   5% |  71   5% |
	// |  78 |  73   4% |  72   4% |  72   5% |  74   5% |  72   4% |  72   5% |  72   4% |  72   4% |  72   5% |  72   4% |  72   5% |  73   4% |  72   5% |  72   4% |  72   5% |  73   4% |  73   5% |  73   5% |  72   5% |  73   5% |
	// |  80 |  75   4% |  74   4% |  74   5% |  76   5% |  74   4% |  74   5% |  74   4% |  74   5% |  74   5% |  74   4% |  74   5% |  75   4% |  74   5% |  74   4% |  74   5% |  75   4% |  75   5% |  75   5% |  74   5% |  75   5% |
	// |  82 |  77   4% |  76   4% |  76   5% |  78   5% |  76   4% |  76   5% |  76   4% |  76   5% |  76   5% |  76   4% |  76   5% |  77   4% |  76   5% |  76   4% |  76   5% |  77   4% |  77   5% |  77   5% |  76   5% |  77   5% |
	// |  84 |  79   4% |  78   4% |  78   5% |  80   5% |  78   4% |  78   5% |  78   4% |  78   4% |  78   5% |  78   4% |  78   5% |  79   4% |  78   5% |  78   4% |  78   5% |  79   4% |  79   5% |  79   5% |  78   5% |  79   5% |
	// |  86 |  81   4% |  80   4% |  80   5% |  82   5% |  80   4% |  80   5% |  80   4% |  80   4% |  80   4% |  80   4% |  80   5% |  81   4% |  80   5% |  80   4% |  80   5% |  81   4% |  81   5% |  81   5% |  80   5% |  81   5% |
	// |  88 |  83   4% |  82   4% |  82   5% |  84   5% |  82   4% |  82   5% |  82   4% |  82   5% |  82   4% |  82   4% |  82   5% |  83   4% |  82   5% |  82   4% |  82   5% |  83   4% |  83   5% |  83   5% |  82   5% |  83   5% |
	// |  90 |  85   4% |  84   4% |  84   5% |  86   5% |  84   4% |  84   5% |  84   4% |  84   4% |  84   4% |  84   4% |  84   5% |  85   4% |  84   5% |  84   4% |  84   4% |  85   4% |  85   5% |  85   5% |  84   5% |  85   5% |
	// |  92 |  87   4% |  86   4% |  86   5% |  88   5% |  86   4% |  86   5% |  86   4% |  86   4% |  86   4% |  86   4% |  86   5% |  87   4% |  86   5% |  86   4% |  86   4% |  87   4% |  87   5% |  87   5% |  86   5% |  87   5% |
	// |  94 |  89   4% |  88   4% |  88   5% |  90   5% |  88   4% |  88   5% |  88   4% |  88   5% |  88   4% |  88   4% |  88   5% |  89   4% |  88   5% |  88   4% |  88   4% |  89   4% |  89   5% |  89   5% |  88   5% |  89   5% |
	// |  96 |  91   4% |  90   4% |  90   5% |  92   5% |  90   4% |  90   5% |  90   4% |  90   5% |  90   4% |  90   4% |  90   5% |  91   4% |  90   5% |  90   4% |  90   4% |  91   4% |  91   5% |  91   5% |  90   5% |  91   5% |
	// |  98 |  93   4% |  92   4% |  92   5% |  94   5% |  92   4% |  92   5% |  92   4% |  92   4% |  92   4% |  92   4% |  92   5% |  93   4% |  92   5% |  92   4% |  92   4% |  93   4% |  93   5% |  93   5% |  92   5% |  93   5% |
	// +-----+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
	// Total bytes=969243192, ranges=1868
}
