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

	storeRSa := roachpb.StoreID(1)   // datacenter (us), rack, slot + a
	storeRab := roachpb.StoreID(2)   // datacenter (us), rack + a,b
	storeFRabc := roachpb.StoreID(3) // datacenter (us), floor, rack + a,b,c
	storeDead := roachpb.StoreID(4)
	storeEurope := roachpb.StoreID(5) // datacenter (eur), rack

	mockStorePool(storePool, []roachpb.StoreID{storeRSa, storeRab, storeFRabc, storeEurope}, []roachpb.StoreID{storeDead}, nil)

	// tierSetup returns a tier struct constructed using the passed in values.
	// If any value is an empty string, it is not included.
	tierSetup := func(datacenter, floor, rack, slot string) []roachpb.Tier {
		var tiers []roachpb.Tier
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
	// always 100 and available and range count are passed in.
	capacitySetup := func(available int64, rangeCount int32) roachpb.StoreCapacity {
		return roachpb.StoreCapacity{
			Capacity:   100,
			Available:  available,
			RangeCount: rangeCount,
		}
	}

	storePool.mu.Lock()

	storePool.mu.storeDetails[storeRSa].desc.Attrs.Attrs = []string{"a"}
	storePool.mu.storeDetails[storeRSa].desc.Node.Locality.Tiers = tierSetup("us", "", "1", "5")
	storePool.mu.storeDetails[storeRSa].desc.Capacity = capacitySetup(1, 99)

	storePool.mu.storeDetails[storeRab].desc.Attrs.Attrs = []string{"a", "b"}
	storePool.mu.storeDetails[storeRab].desc.Node.Locality.Tiers = tierSetup("us", "", "1", "")
	storePool.mu.storeDetails[storeRab].desc.Capacity = capacitySetup(100, 0)

	storePool.mu.storeDetails[storeFRabc].desc.Attrs.Attrs = []string{"a", "b", "c"}
	storePool.mu.storeDetails[storeFRabc].desc.Node.Locality.Tiers = tierSetup("us", "1", "2", "")
	storePool.mu.storeDetails[storeFRabc].desc.Capacity = capacitySetup(50, 50)

	storePool.mu.storeDetails[storeEurope].desc.Node.Locality.Tiers = tierSetup("eur", "", "1", "")
	storePool.mu.storeDetails[storeEurope].desc.Capacity = capacitySetup(60, 40)

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
			expected: []roachpb.StoreID{storeRSa, storeRab, storeFRabc, storeEurope},
		},
		{
			name: "white list rule",
			rule: func(state solveState) (float64, bool) {
				switch state.store.StoreID {
				case storeRSa:
					return 0, true
				case storeFRabc:
					return 1, true
				default:
					return 0, false
				}
			},
			expected: []roachpb.StoreID{storeFRabc, storeRSa},
		},
		{
			name: "ruleReplicasUniqueNodes - 2 available nodes",
			rule: ruleReplicasUniqueNodes,
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: roachpb.NodeID(storeRSa)},
				{NodeID: roachpb.NodeID(storeFRabc)},
			},
			expected: []roachpb.StoreID{storeRab, storeEurope},
		},
		{
			name: "ruleReplicasUniqueNodes - 0 available nodes",
			rule: ruleReplicasUniqueNodes,
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: roachpb.NodeID(storeRSa)},
				{NodeID: roachpb.NodeID(storeRab)},
				{NodeID: roachpb.NodeID(storeFRabc)},
				{NodeID: roachpb.NodeID(storeEurope)},
			},
			expected: nil,
		},
		{
			name: "ruleConstraints - required constraints",
			rule: ruleConstraints,
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Value: "b", Type: config.Constraint_REQUIRED},
				},
			},
			expected: []roachpb.StoreID{storeRab, storeFRabc},
		},
		{
			name: "ruleConstraints - required locality constraints",
			rule: ruleConstraints,
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Key: "datacenter", Value: "us", Type: config.Constraint_REQUIRED},
				},
			},
			expected: []roachpb.StoreID{storeRSa, storeRab, storeFRabc},
		},
		{
			name: "ruleConstraints - prohibited constraints",
			rule: ruleConstraints,
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Value: "b", Type: config.Constraint_PROHIBITED},
				},
			},
			expected: []roachpb.StoreID{storeRSa, storeEurope},
		},
		{
			name: "ruleConstraints - prohibited locality constraints",
			rule: ruleConstraints,
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Key: "datacenter", Value: "us", Type: config.Constraint_PROHIBITED},
				},
			},
			expected: []roachpb.StoreID{storeEurope},
		},
		{
			name: "ruleConstraints - positive constraints",
			rule: ruleConstraints,
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Value: "a"},
					{Value: "b"},
					{Value: "c"},
				},
			},
			expected: []roachpb.StoreID{storeFRabc, storeRab, storeRSa, storeEurope},
		},
		{
			name: "ruleConstraints - positive locality constraints",
			rule: ruleConstraints,
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Key: "datacenter", Value: "eur"},
				},
			},
			expected: []roachpb.StoreID{storeEurope, storeRSa, storeRab, storeFRabc},
		},
		{
			name:     "ruleDiversity - no existing replicas",
			rule:     ruleDiversity,
			existing: nil,
			expected: []roachpb.StoreID{storeRSa, storeRab, storeFRabc, storeEurope},
		},
		{
			name: "ruleDiversity - one existing replicas",
			rule: ruleDiversity,
			existing: []roachpb.ReplicaDescriptor{
				{StoreID: storeRSa},
			},
			expected: []roachpb.StoreID{storeEurope, storeFRabc, storeRSa, storeRab},
		},
		{
			name:     "ruleCapacity",
			rule:     ruleCapacity,
			expected: []roachpb.StoreID{storeRab, storeEurope, storeFRabc},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var solver ruleSolver
			if tc.rule != nil {
				solver = ruleSolver{tc.rule}
			}
			sl, _, _ := storePool.getStoreList(roachpb.RangeID(0))
			candidates, err := solver.Solve(sl, tc.c, tc.existing)
			if err != nil {
				t.Fatal(err)
			}
			sort.Sort(byScoreAndID(candidates))
			if len(candidates) != len(tc.expected) {
				t.Fatalf("length of %+v should match %+v", candidates, tc.expected)
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
		expected      []string
	}{
		{
			name:          "no tiers at all",
			tiersPerStore: nil,
			expected:      []string{},
		},
		{
			name:          "one store with two empty tiers",
			tiersPerStore: [][]roachpb.Tier{nil, nil},
			expected:      []string{},
		},
		{
			name: "one store with three tiers",
			tiersPerStore: [][]roachpb.Tier{
				{
					{Key: "a"},
					{Key: "b"},
					{Key: "c"},
				},
			},
			expected: []string{"a", "b", "c"},
		},
		{
			name: "3 stores with the same tiers, one with an extra one",
			tiersPerStore: [][]roachpb.Tier{
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
			expected: []string{"a", "b", "c"},
		},
		{
			name: "two stores with completely different tiers",
			tiersPerStore: [][]roachpb.Tier{
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
			expected: []string{"a", "b", "c"},
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
