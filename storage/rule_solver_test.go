// Copyright 2016 The Cockroach Authors.
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
// Author: Tristan Rice (rice@fn.lc)

package storage

import (
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

type byScoreAndID []candidate

func (c byScoreAndID) Len() int { return len(c) }
func (c byScoreAndID) Less(i, j int) bool {
	if c[i].score == c[j].score {
		return c[i].store.StoreID < c[j].store.StoreID
	}
	return c[i].score > c[j].score
}
func (c byScoreAndID) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

// TestRuleSolver tests the mechanics of ruleSolver.
func TestRuleSolver(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, _, _, storePool := createTestStorePool(TestTimeUntilStoreDeadOff)
	defer stopper.Stop()
	// 3 alive replicas, 1 dead
	mockStorePool(storePool, []roachpb.StoreID{1, 2, 3, 5}, []roachpb.StoreID{4}, nil)

	storePool.mu.Lock()
	storePool.mu.stores[1].desc.Attrs.Attrs = []string{"a"}
	storePool.mu.stores[2].desc.Attrs.Attrs = []string{"a", "b"}
	storePool.mu.stores[3].desc.Attrs.Attrs = []string{"a", "b", "c"}

	storePool.mu.stores[1].desc.Locality.Tiers = []roachpb.Tier{
		{Key: "datacenter", Value: "us"},
		{Key: "rack", Value: "1"},
		{Key: "slot", Value: "5"},
	}
	storePool.mu.stores[2].desc.Locality.Tiers = []roachpb.Tier{
		{Key: "datacenter", Value: "us"},
		{Key: "rack", Value: "1"},
	}
	storePool.mu.stores[3].desc.Locality.Tiers = []roachpb.Tier{
		{Key: "datacenter", Value: "us"},
		{Key: "floor", Value: "1"},
		{Key: "rack", Value: "2"},
	}
	storePool.mu.stores[5].desc.Locality.Tiers = []roachpb.Tier{
		{Key: "datacenter", Value: "eur"},
		{Key: "rack", Value: "1"},
	}

	storePool.mu.stores[1].desc.Capacity = roachpb.StoreCapacity{
		Capacity:   100,
		Available:  1,
		RangeCount: 99,
	}
	storePool.mu.stores[2].desc.Capacity = roachpb.StoreCapacity{
		Capacity:   100,
		Available:  100,
		RangeCount: 0,
	}
	storePool.mu.stores[3].desc.Capacity = roachpb.StoreCapacity{
		Capacity:   100,
		Available:  50,
		RangeCount: 50,
	}
	storePool.mu.stores[5].desc.Capacity = roachpb.StoreCapacity{
		Capacity:   100,
		Available:  60,
		RangeCount: 40,
	}
	storePool.mu.Unlock()

	testCases := []struct {
		rules    []rule
		c        config.Constraints
		existing []roachpb.ReplicaDescriptor
		expected []roachpb.StoreID
	}{
		// No constraints or rules.
		{
			expected: []roachpb.StoreID{1, 2, 3, 5},
		},
		// Store 1: score 0; Store 3: score 1; everything else fails.
		{
			rules: []rule{
				{
					weight: 1,
					run: func(state solveState) (candidate bool, score float64) {
						switch state.store.StoreID {
						case 1:
							return true, 0
						case 3:
							return true, 1
						default:
							return false, 0
						}
					},
				},
			},
			expected: []roachpb.StoreID{3, 1},
		},
		// Don't put a replica on the same node as another.
		{
			rules: []rule{{weight: 1, run: ruleReplicasUniqueNodes}},
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: 1},
				{NodeID: 3},
			},
			expected: []roachpb.StoreID{2, 5},
		},
		{
			rules: []rule{{weight: 1, run: ruleReplicasUniqueNodes}},
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: 1},
				{NodeID: 2},
				{NodeID: 3},
				{NodeID: 5},
			},
			expected: nil,
		},
		// Only put replicas on nodes with required constraints.
		{
			rules: []rule{{weight: 1, run: ruleConstraints}},
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Value: "b", Type: config.Constraint_REQUIRED},
				},
			},
			expected: []roachpb.StoreID{2, 3},
		},
		// Required locality constraints.
		{
			rules: []rule{{weight: 1, run: ruleConstraints}},
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Key: "datacenter", Value: "us", Type: config.Constraint_REQUIRED},
				},
			},
			expected: []roachpb.StoreID{1, 2, 3},
		},
		// Don't put a replica on a node with a prohibited constraint.
		{
			rules: []rule{{weight: 1, run: ruleConstraints}},
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Value: "b", Type: config.Constraint_PROHIBITED},
				},
			},
			expected: []roachpb.StoreID{1, 5},
		},
		// Prohibited locality constraints.
		{
			rules: []rule{{weight: 1, run: ruleConstraints}},
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Key: "datacenter", Value: "us", Type: config.Constraint_PROHIBITED},
				},
			},
			expected: []roachpb.StoreID{5},
		},
		// Positive constraints ordered by number of matches.
		{
			rules: []rule{{weight: 1, run: ruleConstraints}},
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Value: "a"},
					{Value: "b"},
					{Value: "c"},
				},
			},
			expected: []roachpb.StoreID{3, 2, 1, 5},
		},
		// Positive locality constraints.
		{
			rules: []rule{{weight: 1, run: ruleConstraints}},
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Key: "datacenter", Value: "eur"},
				},
			},
			expected: []roachpb.StoreID{5, 1, 2, 3},
		},
		// Diversity with no existing.
		{
			rules:    []rule{{weight: 1, run: ruleDiversity}},
			existing: nil,
			expected: []roachpb.StoreID{1, 2, 3, 5},
		},
		// Diversity with one existing.
		{
			rules: []rule{{weight: 1, run: ruleDiversity}},
			existing: []roachpb.ReplicaDescriptor{
				{StoreID: 1},
			},
			expected: []roachpb.StoreID{5, 3, 1, 2},
		},
		// Prioritize lower capacity nodes, and don't overfill.
		{
			rules:    []rule{{weight: 1, run: ruleCapacity}},
			expected: []roachpb.StoreID{2, 5, 3},
		},
	}

	for i, tc := range testCases {
		solver := makeRuleSolver(storePool, tc.rules)
		candidates, err := solver.Solve(tc.c, tc.existing)
		if err != nil {
			t.Fatal(err)
		}
		sort.Sort(byScoreAndID(candidates))
		if len(candidates) != len(tc.expected) {
			t.Errorf("%d: length of %+v should match %+v", i, candidates, tc.expected)
			continue
		}
		for j, expected := range tc.expected {
			if out := candidates[j].store.StoreID; out != expected {
				t.Errorf("%d: candidates[%d].store.StoreID = %d; not %d; %+v", i, j, out, expected, candidates)
			}
		}
	}
}

func TestCanonicalTierOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		stores [][]roachpb.Tier
		want   []roachpb.Tier
	}{
		{
			nil,
			[]roachpb.Tier{},
		},
		{
			[][]roachpb.Tier{nil, nil},
			[]roachpb.Tier{},
		},
		{
			[][]roachpb.Tier{
				{
					{Key: "a"},
					{Key: "b"},
					{Key: "c"},
				},
			},
			[]roachpb.Tier{
				{Key: "a"},
				{Key: "b"},
				{Key: "c"},
			},
		},
		{
			[][]roachpb.Tier{
				{{Key: "a"},
					{Key: "b"},
					{Key: "c"},
				},
				{
					{Key: "a"},
					{Key: "b"},
					{Key: "c"},
				},
				{
					{Key: "b"},
					{Key: "c"},
					{Key: "a"},
					{Key: "d"},
				},
			},
			[]roachpb.Tier{
				{Key: "a"},
				{Key: "b"},
				{Key: "c"},
			},
		},
		{
			[][]roachpb.Tier{
				{
					{Key: "a"},
					{Key: "b"},
					{Key: "c"},
				},
				{
					{Key: "e"},
					{Key: "f"},
					{Key: "g"},
				},
			},
			[]roachpb.Tier{
				{Key: "a"},
				{Key: "b"},
				{Key: "c"},
			},
		},
	}

	for i, tc := range testCases {
		sl := StoreList{}
		for _, tiers := range tc.stores {
			sl.stores = append(sl.stores, roachpb.StoreDescriptor{
				Locality: roachpb.Locality{Tiers: tiers},
			})
		}

		if out := canonicalTierOrder(sl); !reflect.DeepEqual(out, tc.want) {
			t.Errorf("%d: canonicalTierOrder(%+v) = %+v; not %+v", i, tc.stores, out, tc.want)
		}
	}
}
