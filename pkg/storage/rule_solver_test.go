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
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type byScoreAndID []candidate

func (c byScoreAndID) Len() int { return len(c) }
func (c byScoreAndID) Less(i, j int) bool {
	if c[i].constraint == c[j].constraint && c[i].balance == c[j].balance {
		return c[i].store.StoreID < c[j].store.StoreID
	}
	return c[j].less(c[i])
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

	storeUSa15 := roachpb.StoreID(1) // us-a-1-5
	storeUSa1 := roachpb.StoreID(2)  // us-a-1
	storeUSb := roachpb.StoreID(3)   // us-b
	storeDead := roachpb.StoreID(4)
	storeEurope := roachpb.StoreID(5) // eur-a-1-5

	mockStorePool(storePool, []roachpb.StoreID{storeUSa15, storeUSa1, storeUSb, storeEurope}, []roachpb.StoreID{storeDead}, nil)

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

	storePool.mu.storeDetails[storeUSa15].desc.Attrs.Attrs = []string{"a"}
	storePool.mu.storeDetails[storeUSa15].desc.Node.Locality.Tiers = tierSetup("us", "a", "1", "5")
	storePool.mu.storeDetails[storeUSa15].desc.Capacity = capacitySetup(1, 99)
	storePool.mu.nodeLocalities[roachpb.NodeID(storeUSa15)] = storePool.mu.storeDetails[storeUSa15].desc.Node.Locality

	storePool.mu.storeDetails[storeUSa1].desc.Attrs.Attrs = []string{"a", "b"}
	storePool.mu.storeDetails[storeUSa1].desc.Node.Locality.Tiers = tierSetup("us", "a", "1", "")
	storePool.mu.storeDetails[storeUSa1].desc.Capacity = capacitySetup(100, 0)
	storePool.mu.nodeLocalities[roachpb.NodeID(storeUSa1)] = storePool.mu.storeDetails[storeUSa1].desc.Node.Locality

	storePool.mu.storeDetails[storeUSb].desc.Attrs.Attrs = []string{"a", "b", "c"}
	storePool.mu.storeDetails[storeUSb].desc.Node.Locality.Tiers = tierSetup("us", "b", "", "")
	storePool.mu.storeDetails[storeUSb].desc.Capacity = capacitySetup(50, 50)
	storePool.mu.nodeLocalities[roachpb.NodeID(storeUSb)] = storePool.mu.storeDetails[storeUSb].desc.Node.Locality

	storePool.mu.storeDetails[storeEurope].desc.Node.Locality.Tiers = tierSetup("eur", "a", "1", "5")
	storePool.mu.storeDetails[storeEurope].desc.Capacity = capacitySetup(60, 40)
	storePool.mu.nodeLocalities[roachpb.NodeID(storeEurope)] = storePool.mu.storeDetails[storeEurope].desc.Node.Locality

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
			expected: []roachpb.StoreID{storeUSa15, storeUSa1, storeUSb, storeEurope},
		},
		{
			name: "white list rule",
			rule: func(store roachpb.StoreDescriptor, _ solveState) (bool, float64, float64) {
				switch store.StoreID {
				case storeUSa15:
					return true, 0, 0
				case storeUSb:
					return true, 1, 0
				default:
					return false, 0, 0
				}
			},
			expected: []roachpb.StoreID{storeUSb, storeUSa15},
		},
		{
			name: "ruleReplicasUniqueNodes - 2 available nodes",
			rule: ruleReplicasUniqueNodes,
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: roachpb.NodeID(storeUSa15)},
				{NodeID: roachpb.NodeID(storeUSb)},
			},
			expected: []roachpb.StoreID{storeUSa1, storeEurope},
		},
		{
			name: "ruleReplicasUniqueNodes - 0 available nodes",
			rule: ruleReplicasUniqueNodes,
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: roachpb.NodeID(storeUSa15)},
				{NodeID: roachpb.NodeID(storeUSa1)},
				{NodeID: roachpb.NodeID(storeUSb)},
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
			expected: []roachpb.StoreID{storeUSa1, storeUSb},
		},
		{
			name: "ruleConstraints - required locality constraints",
			rule: ruleConstraints,
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Key: "datacenter", Value: "us", Type: config.Constraint_REQUIRED},
				},
			},
			expected: []roachpb.StoreID{storeUSa15, storeUSa1, storeUSb},
		},
		{
			name: "ruleConstraints - prohibited constraints",
			rule: ruleConstraints,
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Value: "b", Type: config.Constraint_PROHIBITED},
				},
			},
			expected: []roachpb.StoreID{storeUSa15, storeEurope},
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
			expected: []roachpb.StoreID{storeUSb, storeUSa1, storeUSa15, storeEurope},
		},
		{
			name: "ruleConstraints - positive locality constraints",
			rule: ruleConstraints,
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Key: "datacenter", Value: "eur"},
				},
			},
			expected: []roachpb.StoreID{storeEurope, storeUSa15, storeUSa1, storeUSb},
		},
		{
			name:     "ruleDiversity - no existing replicas",
			rule:     ruleDiversity,
			existing: nil,
			expected: []roachpb.StoreID{storeUSa15, storeUSa1, storeUSb, storeEurope},
		},
		{
			name: "ruleDiversity - one existing replicas",
			rule: ruleDiversity,
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: roachpb.NodeID(storeUSa15)},
			},
			expected: []roachpb.StoreID{storeEurope, storeUSb, storeUSa15, storeUSa1},
		},
		{
			name: "ruleDiversity - two existing replicas",
			rule: ruleDiversity,
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: roachpb.NodeID(storeUSa15)},
				{NodeID: roachpb.NodeID(storeEurope)},
			},
			expected: []roachpb.StoreID{storeUSb, storeUSa15, storeUSa1, storeEurope},
		},
		{
			name:     "ruleCapacity",
			rule:     ruleCapacity,
			expected: []roachpb.StoreID{storeUSa1, storeEurope, storeUSb},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var solver ruleSolver
			if tc.rule != nil {
				solver = ruleSolver{tc.rule}
			}
			sl, _, _ := storePool.getStoreList(roachpb.RangeID(0))
			candidates, err := solver.Solve(
				sl,
				tc.c,
				tc.existing,
				storePool.getNodeLocalities(tc.existing),
			)
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
