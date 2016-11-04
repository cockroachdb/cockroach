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

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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

// TestRuleSolver tests that ruleCapacity, ruleDiversity, ruleConstraints,
// ruleReplicasUniqueNodes and that the solver itself functions as expected.
// TODO(bram): This test suite is not even close to exhaustive. The scores are
// not checked and each rule should have many more test cases. Also add a
// corrupt replica test and remove the 0 range ID used when calling
// getStoreList.
func TestRuleSolver(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, _, _, storePool := createTestStorePool(
		TestTimeUntilStoreDeadOff,
		/* deterministic */ false,
	)
	defer stopper.Stop()
	// 4 alive replicas, 1 dead
	mockStorePool(storePool, []roachpb.StoreID{1, 2, 3, 5}, []roachpb.StoreID{4}, nil)

	// tierSetup returns a tier struct constructed using the passed in values.
	// If slot is an empty string, it is not included.
	tierSetup := func(datacenter, floor, rack, slot string) []roachpb.Tier {
		tiers := []roachpb.Tier{}
		if datacenter != "" {
			tiers = append(tiers, roachpb.Tier{Key: "datacenter", Value: datacenter})
		}
		if floor != "" {
			tiers = append(tiers, roachpb.Tier{Key: "floor", Value: floor})
		}
		if rack != "" {
			tiers = append(tiers, roachpb.Tier{Key: "rack", Value: rack})
		}
		if slot != "" {
			tiers = append(tiers, roachpb.Tier{Key: "slot", Value: slot})
		}
		return tiers
	}

	// capacitySetup returns a store capacity in which the total capacity is
	// always 100 and available is passed in.
	capacitySetup := func(available int64) roachpb.StoreCapacity {
		return roachpb.StoreCapacity{
			Capacity:   100,
			Available:  available,
			RangeCount: int32(100 - available),
		}
	}

	storePool.mu.Lock()

	storePool.mu.storeDetails[1].desc.Attrs.Attrs = []string{"a"}
	storePool.mu.storeDetails[1].desc.Node.Locality.Tiers = tierSetup("us", "", "1", "5")
	storePool.mu.storeDetails[1].desc.Capacity = capacitySetup(1)

	storePool.mu.storeDetails[2].desc.Attrs.Attrs = []string{"a", "b"}
	storePool.mu.storeDetails[2].desc.Node.Locality.Tiers = tierSetup("us", "", "1", "")
	storePool.mu.storeDetails[2].desc.Capacity = capacitySetup(100)

	storePool.mu.storeDetails[3].desc.Attrs.Attrs = []string{"a", "b", "c"}
	storePool.mu.storeDetails[3].desc.Node.Locality.Tiers = tierSetup("us", "1", "2", "")
	storePool.mu.storeDetails[3].desc.Capacity = capacitySetup(50)

	storePool.mu.storeDetails[5].desc.Node.Locality.Tiers = tierSetup("eur", "", "1", "")
	storePool.mu.storeDetails[5].desc.Capacity = capacitySetup(60)

	storePool.mu.Unlock()

	testCases := []struct {
		name     string
		rule     rule
		c        config.Constraints
		existing []roachpb.ReplicaDescriptor
		expected []roachpb.StoreID
	}{
		{
			name:     "no constraints or rules",
			expected: []roachpb.StoreID{1, 2, 3, 5},
		},
		// Test an arbitrary rule that sets only stores 1 and 3 valid.
		// Store 1: score 0; Store 3: score 1; everything else fails.
		{
			name: "arbitrary rule",
			rule: rule{
				weight: 1,
				run: func(state solveState) (float64, bool) {
					switch state.store.StoreID {
					case 1:
						return 0, true
					case 3:
						return 1, true
					default:
						return 0, false
					}
				},
			},
			expected: []roachpb.StoreID{3, 1},
		},
		// Test that ruleReplicasUniqueNodes correctly avoids overlapping
		// replicas on the same node.
		{
			name: "ruleReplicasUniqueNodes - 2 available nodes",
			rule: rule{weight: 1, run: ruleReplicasUniqueNodes},
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: 1},
				{NodeID: 3},
			},
			expected: []roachpb.StoreID{2, 5},
		},
		{
			name: "ruleReplicasUniqueNodes - 0 available nodes",
			rule: rule{weight: 1, run: ruleReplicasUniqueNodes},
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: 1},
				{NodeID: 2},
				{NodeID: 3},
				{NodeID: 5},
			},
			expected: nil,
		},
		// Test ruleConstraints in a number of different scenarios.
		{
			name: "ruleConstraints - required constraints",
			rule: rule{weight: 1, run: ruleConstraints},
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Value: "b", Type: config.Constraint_REQUIRED},
				},
			},
			expected: []roachpb.StoreID{2, 3},
		},
		{
			name: "ruleConstraints - required locality constraints",
			rule: rule{weight: 1, run: ruleConstraints},
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Key: "datacenter", Value: "us", Type: config.Constraint_REQUIRED},
				},
			},
			expected: []roachpb.StoreID{1, 2, 3},
		},
		{
			name: "ruleConstraints - prohibited constraints",
			rule: rule{weight: 1, run: ruleConstraints},
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Value: "b", Type: config.Constraint_PROHIBITED},
				},
			},
			expected: []roachpb.StoreID{1, 5},
		},
		{
			name: "ruleConstraints - prohibited locality constraints",
			rule: rule{weight: 1, run: ruleConstraints},
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Key: "datacenter", Value: "us", Type: config.Constraint_PROHIBITED},
				},
			},
			expected: []roachpb.StoreID{5},
		},
		{
			name: "ruleConstraints - positive constraints",
			rule: rule{weight: 1, run: ruleConstraints},
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Value: "a"},
					{Value: "b"},
					{Value: "c"},
				},
			},
			expected: []roachpb.StoreID{3, 2, 1, 5},
		},
		{
			name: "ruleConstraints - positive locality constraints",
			rule: rule{weight: 1, run: ruleConstraints},
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Key: "datacenter", Value: "eur"},
				},
			},
			expected: []roachpb.StoreID{5, 1, 2, 3},
		},
		// test ruleDiversity
		{
			name:     "ruleDiversity - no existing replicas",
			rule:     rule{weight: 1, run: ruleDiversity},
			existing: nil,
			expected: []roachpb.StoreID{1, 2, 3, 5},
		},
		{
			name: "ruleDiversity - one existing replicas",
			rule: rule{weight: 1, run: ruleDiversity},
			existing: []roachpb.ReplicaDescriptor{
				{StoreID: 1},
			},
			expected: []roachpb.StoreID{5, 3, 1, 2},
		},
		// Test prioritizing lower capacity nodes and reject stores that would
		// become overfilled.
		{
			name:     "ruleCapacity",
			rule:     rule{weight: 1, run: ruleCapacity},
			expected: []roachpb.StoreID{2, 5, 3},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var rules []rule
			if tc.rule.run != nil {
				rules = []rule{tc.rule}
			}
			solver := makeRuleSolver(rules)
			sl, _, _ := storePool.getStoreList(roachpb.RangeID(0))
			candidates, err := solver.Solve(sl, tc.c, tc.existing)
			if err != nil {
				t.Fatal(err)
			}
			sort.Sort(byScoreAndID(candidates))
			if len(candidates) != len(tc.expected) {
				t.Errorf("length of %+v should match %+v", candidates, tc.expected)
				return
			}
			for i, expected := range tc.expected {
				if actual := candidates[i].store.StoreID; actual != expected {
					t.Errorf("candidates[%d].store.StoreID = %d; not %d; %+v",
						i, actual, expected, candidates)
				}
			}
		})
	}
}

func TestCanonicalTierOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name          string
		tiersPerStore [][]roachpb.Tier
		expected      []roachpb.Tier
	}{
		{
			"no tiers at all",
			nil,
			[]roachpb.Tier{},
		},
		{
			"one store with two empty tiers",
			[][]roachpb.Tier{nil, nil},
			[]roachpb.Tier{},
		},
		{
			"one store with three tiers",
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
			"3 stores with the same tiers, one with an extra one",
			[][]roachpb.Tier{
				{
					{Key: "a"},
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
			"two stores with completely different tiers",
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

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var descriptors []roachpb.StoreDescriptor
			for _, tiers := range tc.tiersPerStore {
				descriptors = append(descriptors, roachpb.StoreDescriptor{
					Node: roachpb.NodeDescriptor{
						Locality: roachpb.Locality{Tiers: tiers},
					},
				})
			}

			sl := makeStoreList(descriptors)
			if actual := canonicalTierOrder(sl); !reflect.DeepEqual(actual, tc.expected) {
				t.Errorf("canonicalTierOrder(%+v) = %+v; not %+v",
					tc.tiersPerStore, actual, tc.expected)
			}
		})
	}
}
