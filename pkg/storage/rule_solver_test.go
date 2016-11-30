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
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/kr/pretty"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TODO(bram): This test suite is not even close to exhaustive. The scores are
// not checked and each rule should have many more test cases. Also add a
// corrupt replica test and remove the 0 range ID used when calling
// getStoreList.
func TestRuleSolver(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper, _, _, storePool, _ := createTestStorePool(
		TestTimeUntilStoreDeadOff,
		/* deterministic */ true,
		/* defaultNodeLiveness */ true,
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
		name            string
		rule            rule
		c               config.Constraints
		existing        []roachpb.ReplicaDescriptor
		expectedValid   []roachpb.StoreID
		expectedInvalid []roachpb.StoreID
	}{
		{
			name:          "no constraints or rules",
			expectedValid: []roachpb.StoreID{storeEurope, storeUSb, storeUSa1, storeUSa15},
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
			expectedValid:   []roachpb.StoreID{storeUSb, storeUSa15},
			expectedInvalid: []roachpb.StoreID{storeEurope, storeUSa1},
		},
		{
			name: "ruleReplicasUniqueNodes - 2 available nodes",
			rule: ruleReplicasUniqueNodes,
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: roachpb.NodeID(storeUSa15)},
				{NodeID: roachpb.NodeID(storeUSb)},
			},
			expectedValid:   []roachpb.StoreID{storeEurope, storeUSa1},
			expectedInvalid: []roachpb.StoreID{storeUSb, storeUSa15},
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
			expectedValid:   nil,
			expectedInvalid: []roachpb.StoreID{storeEurope, storeUSb, storeUSa1, storeUSa15},
		},
		{
			name: "ruleConstraints - required constraints",
			rule: ruleConstraints,
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Value: "b", Type: config.Constraint_REQUIRED},
				},
			},
			expectedValid:   []roachpb.StoreID{storeUSb, storeUSa1},
			expectedInvalid: []roachpb.StoreID{storeEurope, storeUSa15},
		},
		{
			name: "ruleConstraints - required locality constraints",
			rule: ruleConstraints,
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Key: "datacenter", Value: "us", Type: config.Constraint_REQUIRED},
				},
			},
			expectedValid:   []roachpb.StoreID{storeUSb, storeUSa1, storeUSa15},
			expectedInvalid: []roachpb.StoreID{storeEurope},
		},
		{
			name: "ruleConstraints - prohibited constraints",
			rule: ruleConstraints,
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Value: "b", Type: config.Constraint_PROHIBITED},
				},
			},
			expectedValid:   []roachpb.StoreID{storeEurope, storeUSa15},
			expectedInvalid: []roachpb.StoreID{storeUSb, storeUSa1},
		},
		{
			name: "ruleConstraints - prohibited locality constraints",
			rule: ruleConstraints,
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Key: "datacenter", Value: "us", Type: config.Constraint_PROHIBITED},
				},
			},
			expectedValid:   []roachpb.StoreID{storeEurope},
			expectedInvalid: []roachpb.StoreID{storeUSb, storeUSa1, storeUSa15},
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
			expectedValid: []roachpb.StoreID{storeUSb, storeUSa1, storeUSa15, storeEurope},
		},
		{
			name: "ruleConstraints - positive locality constraints",
			rule: ruleConstraints,
			c: config.Constraints{
				Constraints: []config.Constraint{
					{Key: "datacenter", Value: "eur"},
				},
			},
			expectedValid: []roachpb.StoreID{storeEurope, storeUSb, storeUSa1, storeUSa15},
		},
		{
			name:          "ruleDiversity - no existing replicas",
			rule:          ruleDiversity,
			expectedValid: []roachpb.StoreID{storeEurope, storeUSb, storeUSa1, storeUSa15},
		},
		{
			name: "ruleDiversity - one existing replicas",
			rule: ruleDiversity,
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: roachpb.NodeID(storeUSa15)},
			},
			expectedValid: []roachpb.StoreID{storeEurope, storeUSb, storeUSa1, storeUSa15},
		},
		{
			name: "ruleDiversity - two existing replicas",
			rule: ruleDiversity,
			existing: []roachpb.ReplicaDescriptor{
				{NodeID: roachpb.NodeID(storeUSa15)},
				{NodeID: roachpb.NodeID(storeEurope)},
			},
			expectedValid: []roachpb.StoreID{storeUSb, storeUSa1, storeEurope, storeUSa15},
		},
		{
			name:            "ruleCapacityMax",
			rule:            ruleCapacityMax,
			expectedValid:   []roachpb.StoreID{storeEurope, storeUSb, storeUSa1},
			expectedInvalid: []roachpb.StoreID{storeUSa15},
		},
		{
			name:          "ruleCapacity",
			rule:          ruleCapacity,
			expectedValid: []roachpb.StoreID{storeUSa1, storeEurope, storeUSb, storeUSa15},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var solver ruleSolver
			if tc.rule != nil {
				solver = ruleSolver{tc.rule}
			}
			sl, _, _ := storePool.getStoreList(roachpb.RangeID(0))
			candidates := solver.Solve(
				sl,
				tc.c,
				tc.existing,
				storePool.getNodeLocalities(tc.existing),
				storePool.deterministic,
			)
			valid := candidates.onlyValid()
			invalid := candidates[len(valid):]

			if len(valid) != len(tc.expectedValid) {
				t.Fatalf("length of valid %+v should match %+v", valid, tc.expectedValid)
			}
			for i, expected := range tc.expectedValid {
				if actual := valid[i].store.StoreID; actual != expected {
					t.Errorf("valid[%d].store.StoreID = %d; not %d; %+v",
						i, actual, expected, valid)
				}
			}
			if len(invalid) != len(tc.expectedInvalid) {
				t.Fatalf("length of invalids %+v should match %+v", invalid, tc.expectedInvalid)
			}
			for i, expected := range tc.expectedInvalid {
				if actual := invalid[i].store.StoreID; actual != expected {
					t.Errorf("invalid[%d].store.StoreID = %d; not %d; %+v",
						i, actual, expected, invalid)
				}
			}
		})
	}
}

func TestOnlyValid(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		valid, invalid int
	}{
		{0, 0},
		{1, 0},
		{0, 1},
		{1, 1},
		{2, 0},
		{2, 1},
		{2, 2},
		{1, 2},
		{0, 2},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d,%d", tc.valid, tc.invalid), func(t *testing.T) {
			var cl candidateList
			// Order these in backward to ensure sorting works correctly.
			for i := 0; i < tc.invalid; i++ {
				cl = append(cl, candidate{})
			}
			for i := 0; i < tc.valid; i++ {
				cl = append(cl, candidate{valid: true})
			}
			sort.Sort(sort.Reverse(byScore(cl)))

			valid := cl.onlyValid()
			if a, e := len(valid), tc.valid; a != e {
				t.Errorf("expected %d valid, actual %d", e, a)
			}
			if a, e := len(cl)-len(valid), tc.invalid; a != e {
				t.Errorf("expected %d invalid, actual %d", e, a)
			}
		})
	}
}

// TestCandidateSelection tests select{good,bad} and {best,worst}constraints.
func TestCandidateSelection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type scoreTuple struct {
		constraint int
		balance    int
	}
	genCandidates := func(scores []scoreTuple) candidateList {
		var cl candidateList
		for _, score := range scores {
			cl = append(cl, candidate{
				constraint: float64(score.constraint),
				balance:    float64(score.balance),
				valid:      true,
			})
		}
		sort.Sort(sort.Reverse(byScore(cl)))
		return cl
	}

	formatter := func(cl candidateList) string {
		var buffer bytes.Buffer
		for i, c := range cl {
			if i != 0 {
				buffer.WriteRune(',')
			}
			buffer.WriteString(fmt.Sprintf("%d:%d", int(c.constraint), int(c.balance)))
		}
		return buffer.String()
	}

	testCases := []struct {
		candidates []scoreTuple
		best       []scoreTuple
		worst      []scoreTuple
		good       scoreTuple
		bad        scoreTuple
	}{
		{
			candidates: []scoreTuple{{0, 0}},
			best:       []scoreTuple{{0, 0}},
			worst:      []scoreTuple{{0, 0}},
			good:       scoreTuple{0, 0},
			bad:        scoreTuple{0, 0},
		},
		{
			candidates: []scoreTuple{{0, 1}, {0, 0}},
			best:       []scoreTuple{{0, 1}, {0, 0}},
			worst:      []scoreTuple{{0, 1}, {0, 0}},
			good:       scoreTuple{0, 1},
			bad:        scoreTuple{0, 0},
		},
		{
			candidates: []scoreTuple{{0, 2}, {0, 1}, {0, 0}},
			best:       []scoreTuple{{0, 2}, {0, 1}, {0, 0}},
			worst:      []scoreTuple{{0, 2}, {0, 1}, {0, 0}},
			good:       scoreTuple{0, 1},
			bad:        scoreTuple{0, 0},
		},
		{
			candidates: []scoreTuple{{1, 0}, {0, 1}},
			best:       []scoreTuple{{1, 0}},
			worst:      []scoreTuple{{0, 1}},
			good:       scoreTuple{1, 0},
			bad:        scoreTuple{0, 1},
		},
		{
			candidates: []scoreTuple{{1, 0}, {0, 2}, {0, 1}},
			best:       []scoreTuple{{1, 0}},
			worst:      []scoreTuple{{0, 2}, {0, 1}},
			good:       scoreTuple{1, 0},
			bad:        scoreTuple{0, 1},
		},
		{
			candidates: []scoreTuple{{1, 1}, {1, 0}, {0, 2}},
			best:       []scoreTuple{{1, 1}, {1, 0}},
			worst:      []scoreTuple{{0, 2}},
			good:       scoreTuple{1, 1},
			bad:        scoreTuple{0, 2},
		},
		{
			candidates: []scoreTuple{{1, 1}, {1, 0}, {0, 3}, {0, 2}},
			best:       []scoreTuple{{1, 1}, {1, 0}},
			worst:      []scoreTuple{{0, 3}, {0, 2}},
			good:       scoreTuple{1, 1},
			bad:        scoreTuple{0, 2},
		},
	}

	allocRand := makeAllocatorRand(rand.NewSource(0))
	for _, tc := range testCases {
		cl := genCandidates(tc.candidates)
		t.Run(fmt.Sprintf("best-%s", formatter(cl)), func(t *testing.T) {
			if a, e := cl.best(), genCandidates(tc.best); !reflect.DeepEqual(a, e) {
				t.Errorf("expected:%s actual:%s diff:%v", formatter(e), formatter(a), pretty.Diff(e, a))
			}
		})
		t.Run(fmt.Sprintf("worst-%s", formatter(cl)), func(t *testing.T) {
			if a, e := cl.worst(), genCandidates(tc.worst); !reflect.DeepEqual(a, e) {
				t.Errorf("expected:%s actual:%s diff:%v", formatter(e), formatter(a), pretty.Diff(e, a))
			}
		})
		t.Run(fmt.Sprintf("good-%s", formatter(cl)), func(t *testing.T) {
			good := cl.selectGood(allocRand)
			actual := scoreTuple{int(good.constraint), int(good.balance)}
			if actual != tc.good {
				t.Errorf("expected:%v actual:%v", tc.good, actual)
			}
		})
		t.Run(fmt.Sprintf("bad-%s", formatter(cl)), func(t *testing.T) {
			bad := cl.selectBad(allocRand)
			actual := scoreTuple{int(bad.constraint), int(bad.balance)}
			if actual != tc.bad {
				t.Errorf("expected:%v actual:%v", tc.bad, actual)
			}
		})
	}
}

func TestBetterThan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCandidateList := candidateList{
		{
			valid:      true,
			constraint: 1,
			balance:    1,
		},
		{
			valid:      true,
			constraint: 1,
			balance:    1,
		},
		{
			valid:      true,
			constraint: 1,
			balance:    0,
		},
		{
			valid:      true,
			constraint: 1,
			balance:    0,
		},
		{
			valid:      true,
			constraint: 0,
			balance:    1,
		},
		{
			valid:      true,
			constraint: 0,
			balance:    1,
		},
		{
			valid:      true,
			constraint: 0,
			balance:    0,
		},
		{
			valid:      true,
			constraint: 0,
			balance:    0,
		},
		{
			valid:      false,
			constraint: 1,
			balance:    0.5,
		},
		{
			valid:      false,
			constraint: 0,
			balance:    0.5,
		},
		{
			valid:      false,
			constraint: 0,
			balance:    0,
		},
	}

	expectedResults := []int{0, 0, 2, 2, 4, 4, 6, 6, 8, 8, 8}

	for i := 0; i < len(testCandidateList); i++ {
		betterThan := testCandidateList.betterThan(testCandidateList[i])
		if e, a := expectedResults[i], len(betterThan); e != a {
			t.Errorf("expected %d results, actual %d", e, a)
		}
	}
}
