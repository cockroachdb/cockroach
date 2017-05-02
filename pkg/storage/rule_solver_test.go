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
		rangeCount int
	}
	genCandidates := func(scores []scoreTuple, idShift int) candidateList {
		var cl candidateList
		for i, score := range scores {
			cl = append(cl, candidate{
				store: roachpb.StoreDescriptor{
					StoreID: roachpb.StoreID(i + idShift),
				},
				constraintScore: float64(score.constraint),
				rangeCount:      score.rangeCount,
				valid:           true,
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
			buffer.WriteString(fmt.Sprintf("%d:%d", int(c.constraintScore), c.rangeCount))
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
			candidates: []scoreTuple{{0, 0}, {0, 1}},
			best:       []scoreTuple{{0, 0}, {0, 1}},
			worst:      []scoreTuple{{0, 0}, {0, 1}},
			good:       scoreTuple{0, 0},
			bad:        scoreTuple{0, 1},
		},
		{
			candidates: []scoreTuple{{0, 0}, {0, 1}, {0, 2}},
			best:       []scoreTuple{{0, 0}, {0, 1}, {0, 2}},
			worst:      []scoreTuple{{0, 0}, {0, 1}, {0, 2}},
			good:       scoreTuple{0, 1},
			bad:        scoreTuple{0, 2},
		},
		{
			candidates: []scoreTuple{{1, 0}, {0, 1}},
			best:       []scoreTuple{{1, 0}},
			worst:      []scoreTuple{{0, 1}},
			good:       scoreTuple{1, 0},
			bad:        scoreTuple{0, 1},
		},
		{
			candidates: []scoreTuple{{1, 0}, {0, 1}, {0, 2}},
			best:       []scoreTuple{{1, 0}},
			worst:      []scoreTuple{{0, 1}, {0, 2}},
			good:       scoreTuple{1, 0},
			bad:        scoreTuple{0, 2},
		},
		{
			candidates: []scoreTuple{{1, 0}, {1, 1}, {0, 2}},
			best:       []scoreTuple{{1, 0}, {1, 1}},
			worst:      []scoreTuple{{0, 2}},
			good:       scoreTuple{1, 0},
			bad:        scoreTuple{0, 2},
		},
		{
			candidates: []scoreTuple{{1, 0}, {1, 1}, {0, 2}, {0, 3}},
			best:       []scoreTuple{{1, 0}, {1, 1}},
			worst:      []scoreTuple{{0, 2}, {0, 3}},
			good:       scoreTuple{1, 0},
			bad:        scoreTuple{0, 3},
		},
	}

	pickResult := func(cl candidateList, storeID roachpb.StoreID) *candidate {
		for _, c := range cl {
			if c.store.StoreID == storeID {
				return &c
			}
		}
		return nil
	}

	allocRand := makeAllocatorRand(rand.NewSource(0))
	for _, tc := range testCases {
		cl := genCandidates(tc.candidates, 1)
		t.Run(fmt.Sprintf("best-%s", formatter(cl)), func(t *testing.T) {
			if a, e := cl.best(), genCandidates(tc.best, 1); !reflect.DeepEqual(a, e) {
				t.Errorf("expected:%s actual:%s diff:%v", formatter(e), formatter(a), pretty.Diff(e, a))
			}
		})
		t.Run(fmt.Sprintf("worst-%s", formatter(cl)), func(t *testing.T) {
			// Shifting the ids is required to match the end of the list.
			if a, e := cl.worst(), genCandidates(
				tc.worst,
				len(tc.candidates)-len(tc.worst)+1,
			); !reflect.DeepEqual(a, e) {
				t.Errorf("expected:%s actual:%s diff:%v", formatter(e), formatter(a), pretty.Diff(e, a))
			}
		})
		t.Run(fmt.Sprintf("good-%s", formatter(cl)), func(t *testing.T) {
			goodStore := cl.selectGood(allocRand)
			if goodStore == nil {
				t.Fatalf("no good store found")
			}
			good := pickResult(cl, goodStore.StoreID)
			if good == nil {
				t.Fatalf("candidate for store %d not found in candidate list: %s", goodStore.StoreID, cl)
			}
			actual := scoreTuple{int(good.constraintScore), good.rangeCount}
			if actual != tc.good {
				t.Errorf("expected:%v actual:%v", tc.good, actual)
			}
		})
		t.Run(fmt.Sprintf("bad-%s", formatter(cl)), func(t *testing.T) {
			badStore := cl.selectBad(allocRand)
			if badStore == nil {
				t.Fatalf("no bad store found")
			}
			bad := pickResult(cl, badStore.StoreID)
			if bad == nil {
				t.Fatalf("candidate for store %d not found in candidate list: %s", badStore.StoreID, cl)
			}
			actual := scoreTuple{int(bad.constraintScore), bad.rangeCount}
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
			valid:           true,
			constraintScore: 1,
			rangeCount:      0,
		},
		{
			valid:           true,
			constraintScore: 1,
			rangeCount:      0,
		},
		{
			valid:           true,
			constraintScore: 1,
			rangeCount:      1,
		},
		{
			valid:           true,
			constraintScore: 1,
			rangeCount:      1,
		},
		{
			valid:           true,
			constraintScore: 0,
			rangeCount:      0,
		},
		{
			valid:           true,
			constraintScore: 0,
			rangeCount:      0,
		},
		{
			valid:           true,
			constraintScore: 0,
			rangeCount:      1,
		},
		{
			valid:           true,
			constraintScore: 0,
			rangeCount:      1,
		},
		{
			valid:           false,
			constraintScore: 1,
			rangeCount:      0,
		},
		{
			valid:           false,
			constraintScore: 0,
			rangeCount:      0,
		},
		{
			valid:           false,
			constraintScore: 0,
			rangeCount:      1,
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

func TestPreexistingReplicaCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var existing []roachpb.ReplicaDescriptor
	for i := 2; i < 10; i += 2 {
		existing = append(existing, roachpb.ReplicaDescriptor{NodeID: roachpb.NodeID(i)})
	}
	for i := 1; i < 10; i++ {
		if e, a := i%2 != 0, preexistingReplicaCheck(roachpb.NodeID(i), existing); e != a {
			t.Errorf("NodeID %d expected to be %t, got %t", i, e, a)
		}
	}
}

// testStoreTierSetup returns a tier struct constructed using the passed in values.
// If any value is an empty string, it is not included.
func testStoreTierSetup(datacenter, floor, rack, slot string) []roachpb.Tier {
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

// testStoreCapacitySetup returns a store capacity in which the total capacity
// is always 100 and available and range count are passed in.
func testStoreCapacitySetup(available int64, rangeCount int32) roachpb.StoreCapacity {
	return roachpb.StoreCapacity{
		Capacity:   100,
		Available:  available,
		RangeCount: rangeCount,
	}
}

// This is a collection of test stores used by a suite of tests.
var (
	testStoreUSa15  = roachpb.StoreID(1) // us-a-1-5
	testStoreUSa1   = roachpb.StoreID(2) // us-a-1
	testStoreUSb    = roachpb.StoreID(3) // us-b
	testStoreEurope = roachpb.StoreID(4) // eur-a-1-5

	testStores = []roachpb.StoreDescriptor{
		{
			StoreID: testStoreUSa15,
			Attrs: roachpb.Attributes{
				Attrs: []string{"a"},
			},
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(testStoreUSa15),
				Locality: roachpb.Locality{
					Tiers: testStoreTierSetup("us", "a", "1", "5"),
				},
			},
			Capacity: testStoreCapacitySetup(1, 99),
		},
		{
			StoreID: testStoreUSa1,
			Attrs: roachpb.Attributes{
				Attrs: []string{"a", "b"},
			},
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(testStoreUSa1),
				Locality: roachpb.Locality{
					Tiers: testStoreTierSetup("us", "a", "1", ""),
				},
			},
			Capacity: testStoreCapacitySetup(100, 0),
		},
		{
			StoreID: testStoreUSb,
			Attrs: roachpb.Attributes{
				Attrs: []string{"a", "b", "c"},
			},
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(testStoreUSb),
				Locality: roachpb.Locality{
					Tiers: testStoreTierSetup("us", "b", "", ""),
				},
			},
			Capacity: testStoreCapacitySetup(50, 50),
		},
		{
			StoreID: testStoreEurope,
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(testStoreEurope),
				Locality: roachpb.Locality{
					Tiers: testStoreTierSetup("eur", "a", "1", "5"),
				},
			},
			Capacity: testStoreCapacitySetup(60, 40),
		},
	}
)

func TestConstraintCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name        string
		constraints []config.Constraint
		expected    map[roachpb.StoreID]int
	}{
		{
			name: "required constraint",
			constraints: []config.Constraint{
				{Value: "b", Type: config.Constraint_REQUIRED},
			},
			expected: map[roachpb.StoreID]int{
				testStoreUSa1: 0,
				testStoreUSb:  0,
			},
		},
		{
			name: "required locality constraints",
			constraints: []config.Constraint{
				{Key: "datacenter", Value: "us", Type: config.Constraint_REQUIRED},
			},
			expected: map[roachpb.StoreID]int{
				testStoreUSa15: 0,
				testStoreUSa1:  0,
				testStoreUSb:   0,
			},
		},
		{
			name: "prohibited constraints",
			constraints: []config.Constraint{
				{Value: "b", Type: config.Constraint_PROHIBITED},
			},
			expected: map[roachpb.StoreID]int{
				testStoreUSa15:  0,
				testStoreEurope: 0,
			},
		},
		{
			name: "prohibited locality constraints",
			constraints: []config.Constraint{
				{Key: "datacenter", Value: "us", Type: config.Constraint_PROHIBITED},
			},
			expected: map[roachpb.StoreID]int{
				testStoreEurope: 0,
			},
		},
		{
			name: "positive constraints",
			constraints: []config.Constraint{
				{Value: "a"},
				{Value: "b"},
				{Value: "c"},
			},
			expected: map[roachpb.StoreID]int{
				testStoreUSa15:  1,
				testStoreUSa1:   2,
				testStoreUSb:    3,
				testStoreEurope: 0,
			},
		},
		{
			name: "positive locality constraints",
			constraints: []config.Constraint{
				{Key: "datacenter", Value: "eur"},
			},
			expected: map[roachpb.StoreID]int{
				testStoreUSa15:  0,
				testStoreUSa1:   0,
				testStoreUSb:    0,
				testStoreEurope: 1,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, s := range testStores {
				valid, positive := constraintCheck(s, config.Constraints{Constraints: tc.constraints})
				expectedPositive, ok := tc.expected[s.StoreID]
				if valid != ok {
					t.Errorf("expected store %d to be %t, but got %t", s.StoreID, ok, valid)
					continue
				}
				if positive != expectedPositive {
					t.Errorf("expected store %d to have %d positives, but got %d", s.StoreID, expectedPositive, positive)
				}
			}
		})
	}
}

func TestDiversityScore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		existing []roachpb.NodeID
		expected map[roachpb.StoreID]float64
	}{
		{
			name: "no existing replicas",
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:  1,
				testStoreUSa1:   1,
				testStoreUSb:    1,
				testStoreEurope: 1,
			},
		},
		{
			name: "one existing replicas",
			existing: []roachpb.NodeID{
				roachpb.NodeID(testStoreUSa15),
			},
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:  0,
				testStoreUSa1:   1.0 / 4.0,
				testStoreUSb:    1.0 / 2.0,
				testStoreEurope: 1,
			},
		},
		{
			name: "two existing replicas",
			existing: []roachpb.NodeID{
				roachpb.NodeID(testStoreUSa15),
				roachpb.NodeID(testStoreEurope),
			},
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:  0,
				testStoreUSa1:   1.0 / 4.0,
				testStoreUSb:    1.0 / 2.0,
				testStoreEurope: 0,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			existingNodeLocalities := make(map[roachpb.NodeID]roachpb.Locality)
			for _, nodeID := range tc.existing {
				for _, s := range testStores {
					if s.Node.NodeID == nodeID {
						existingNodeLocalities[roachpb.NodeID(s.Node.NodeID)] = s.Node.Locality
					}
				}
			}
			for _, s := range testStores {
				actualScore := diversityScore(s, existingNodeLocalities)
				expectedScore, ok := tc.expected[s.StoreID]
				if !ok {
					t.Fatalf("no expected score found for storeID %d", s.StoreID)
				}
				if actualScore != expectedScore {
					t.Errorf("store %d expected diversity score: %.2f, actual %.2f", s.StoreID, expectedScore, actualScore)
				}
			}
		})
	}
}

func TestDiversityRemovalScore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		expected map[roachpb.StoreID]float64
	}{
		{
			name: "four existing replicas",
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:  1,
				testStoreUSa1:   1,
				testStoreUSb:    1,
				testStoreEurope: 1.0 / 2.0,
			},
		},
		{
			name: "three existing replicas - testStoreUSa15",
			expected: map[roachpb.StoreID]float64{
				testStoreUSa1:   1,
				testStoreUSb:    1,
				testStoreEurope: 1.0 / 2.0,
			},
		},
		{
			name: "three existing replicas - testStoreUSa1",
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:  1,
				testStoreUSb:    1,
				testStoreEurope: 1.0 / 2.0,
			},
		},
		{
			name: "three existing replicas - testStoreUSb",
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:  1,
				testStoreUSa1:   1,
				testStoreEurope: 1.0 / 4.0,
			},
		},
		{
			name: "three existing replicas - testStoreEurope",
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15: 1.0 / 2.0,
				testStoreUSa1:  1.0 / 2.0,
				testStoreUSb:   1.0 / 4.0,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			existingNodeLocalities := make(map[roachpb.NodeID]roachpb.Locality)
			for _, s := range testStores {
				if _, ok := tc.expected[s.StoreID]; ok {
					existingNodeLocalities[roachpb.NodeID(s.Node.NodeID)] = s.Node.Locality
				}
			}
			for _, s := range testStores {
				if _, ok := tc.expected[s.StoreID]; !ok {
					continue
				}
				actualScore := diversityRemovalScore(s.Node.NodeID, existingNodeLocalities)
				expectedScore, ok := tc.expected[s.StoreID]
				if !ok {
					t.Fatalf("no expected score found for storeID %d", s.StoreID)
				}
				if actualScore != expectedScore {
					t.Errorf("store %d expected diversity removal score: %.2f, actual %.2f", s.StoreID, expectedScore, actualScore)
				}
			}
		})
	}
}

func TestMaxCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)()

	expectedCheck := map[roachpb.StoreID]bool{
		testStoreUSa15:  false,
		testStoreUSa1:   true,
		testStoreUSb:    true,
		testStoreEurope: true,
	}

	for _, s := range testStores {
		if e, a := expectedCheck[s.StoreID], maxCapacityCheck(s); e != a {
			t.Errorf("store %d expected max capacity check: %t, actual %t", s.StoreID, e, a)
		}
	}
}
