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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
			good := cl.selectGood(allocRand)
			if good == nil {
				t.Fatalf("no good candidate found")
			}
			actual := scoreTuple{int(good.constraintScore), good.rangeCount}
			if actual != tc.good {
				t.Errorf("expected:%v actual:%v", tc.good, actual)
			}
		})
		t.Run(fmt.Sprintf("bad-%s", formatter(cl)), func(t *testing.T) {
			bad := cl.selectBad(allocRand)
			if bad == nil {
				t.Fatalf("no bad candidate found")
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
		Capacity:     100,
		Available:    available,
		LogicalBytes: 100 - available,
		RangeCount:   rangeCount,
	}
}

// This is a collection of test stores used by a suite of tests.
var (
	testStoreUSa15     = roachpb.StoreID(1) // us-a-1-5
	testStoreUSa15Dupe = roachpb.StoreID(2) // us-a-1-5
	testStoreUSa1      = roachpb.StoreID(3) // us-a-1
	testStoreUSb       = roachpb.StoreID(4) // us-b
	testStoreEurope    = roachpb.StoreID(5) // eur-a-1-5

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
			StoreID: testStoreUSa15Dupe,
			Attrs: roachpb.Attributes{
				Attrs: []string{"a"},
			},
			Node: roachpb.NodeDescriptor{
				NodeID: roachpb.NodeID(testStoreUSa15Dupe),
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
				testStoreUSa15:     0,
				testStoreUSa15Dupe: 0,
				testStoreUSa1:      0,
				testStoreUSb:       0,
			},
		},
		{
			name: "prohibited constraints",
			constraints: []config.Constraint{
				{Value: "b", Type: config.Constraint_PROHIBITED},
			},
			expected: map[roachpb.StoreID]int{
				testStoreUSa15:     0,
				testStoreUSa15Dupe: 0,
				testStoreEurope:    0,
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
				testStoreUSa15:     1,
				testStoreUSa15Dupe: 1,
				testStoreUSa1:      2,
				testStoreUSb:       3,
				testStoreEurope:    0,
			},
		},
		{
			name: "positive locality constraints",
			constraints: []config.Constraint{
				{Key: "datacenter", Value: "eur"},
			},
			expected: map[roachpb.StoreID]int{
				testStoreUSa15:     0,
				testStoreUSa15Dupe: 0,
				testStoreUSa1:      0,
				testStoreUSb:       0,
				testStoreEurope:    1,
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
				testStoreUSa15:     1,
				testStoreUSa15Dupe: 1,
				testStoreUSa1:      1,
				testStoreUSb:       1,
				testStoreEurope:    1,
			},
		},
		{
			name: "one existing replica",
			existing: []roachpb.NodeID{
				roachpb.NodeID(testStoreUSa15),
			},
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:     0,
				testStoreUSa15Dupe: 0,
				testStoreUSa1:      1.0 / 4.0,
				testStoreUSb:       1.0 / 2.0,
				testStoreEurope:    1,
			},
		},
		{
			name: "two existing replicas",
			existing: []roachpb.NodeID{
				roachpb.NodeID(testStoreUSa15),
				roachpb.NodeID(testStoreEurope),
			},
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:     0,
				testStoreUSa15Dupe: 0,
				testStoreUSa1:      1.0 / 4.0,
				testStoreUSb:       1.0 / 2.0,
				testStoreEurope:    0,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			existingNodeLocalities := make(map[roachpb.NodeID]roachpb.Locality)
			for _, nodeID := range tc.existing {
				for _, s := range testStores {
					if s.Node.NodeID == nodeID {
						existingNodeLocalities[s.Node.NodeID] = s.Node.Locality
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

func TestRebalanceToDiversityScore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		existing []roachpb.NodeID
		expected map[roachpb.StoreID]float64
	}{
		{
			name: "no existing replicas",
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:     1,
				testStoreUSa15Dupe: 1,
				testStoreUSa1:      1,
				testStoreUSb:       1,
				testStoreEurope:    1,
			},
		},
		{
			name: "one existing replica",
			existing: []roachpb.NodeID{
				roachpb.NodeID(testStoreUSa15),
			},
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:     1,
				testStoreUSa15Dupe: 1,
				testStoreUSa1:      1,
				testStoreUSb:       1,
				testStoreEurope:    1,
			},
		},
		{
			name: "two existing replicas",
			existing: []roachpb.NodeID{
				roachpb.NodeID(testStoreUSa15),
				roachpb.NodeID(testStoreUSa1),
			},
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:     1.0 / 4.0,
				testStoreUSa15Dupe: 1.0 / 4.0,
				testStoreUSa1:      1.0 / 4.0,
				testStoreUSb:       1.0 / 2.0,
				testStoreEurope:    1,
			},
		},
		{
			name: "three existing replicas",
			existing: []roachpb.NodeID{
				roachpb.NodeID(testStoreUSa15),
				roachpb.NodeID(testStoreUSa1),
				roachpb.NodeID(testStoreEurope),
			},
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:     1.0 / 4.0,
				testStoreUSa15Dupe: 1.0 / 4.0,
				testStoreUSa1:      1.0 / 4.0,
				testStoreUSb:       1.0 / 2.0,
				testStoreEurope:    1.0,
			},
		},
		{
			name: "three existing replicas with duplicate",
			existing: []roachpb.NodeID{
				roachpb.NodeID(testStoreUSa15),
				roachpb.NodeID(testStoreUSa15Dupe),
				roachpb.NodeID(testStoreUSa1),
			},
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:     0,
				testStoreUSa15Dupe: 0,
				testStoreUSa1:      1.0 / 4.0,
				testStoreUSb:       1.0 / 2.0,
				testStoreEurope:    1.0,
			},
		},
		{
			name: "four existing replicas",
			existing: []roachpb.NodeID{
				roachpb.NodeID(testStoreUSa15),
				roachpb.NodeID(testStoreUSa1),
				roachpb.NodeID(testStoreUSb),
				roachpb.NodeID(testStoreEurope),
			},
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:     1.0 / 4.0,
				testStoreUSa15Dupe: 1.0 / 4.0,
				testStoreUSa1:      1.0 / 4.0,
				testStoreUSb:       1.0 / 2.0,
				testStoreEurope:    1.0,
			},
		},
		{
			name: "four existing replicas with duplicate",
			existing: []roachpb.NodeID{
				roachpb.NodeID(testStoreUSa15),
				roachpb.NodeID(testStoreUSa15Dupe),
				roachpb.NodeID(testStoreUSa1),
				roachpb.NodeID(testStoreUSb),
			},
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:     0,
				testStoreUSa15Dupe: 0,
				testStoreUSa1:      1.0 / 4.0,
				testStoreUSb:       1.0 / 2.0,
				testStoreEurope:    1.0,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			existingNodeLocalities := make(map[roachpb.NodeID]roachpb.Locality)
			for _, nodeID := range tc.existing {
				for _, s := range testStores {
					if s.Node.NodeID == nodeID {
						existingNodeLocalities[s.Node.NodeID] = s.Node.Locality
					}
				}
			}
			for _, s := range testStores {
				actualScore := rebalanceToDiversityScore(s, existingNodeLocalities)
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
				testStoreUSa15:  1.0 / 4.0,
				testStoreUSa1:   1.0 / 4.0,
				testStoreUSb:    1.0 / 2.0,
				testStoreEurope: 1,
			},
		},
		{
			name: "four existing replicas with duplicate",
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:     0,
				testStoreUSa15Dupe: 0,
				testStoreUSb:       1.0 / 2.0,
				testStoreEurope:    1,
			},
		},
		{
			name: "three existing replicas - excluding testStoreUSa15",
			expected: map[roachpb.StoreID]float64{
				testStoreUSa1:   1.0 / 2.0,
				testStoreUSb:    1.0 / 2.0,
				testStoreEurope: 1,
			},
		},
		{
			name: "three existing replicas - excluding testStoreUSa1",
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:  1.0 / 2.0,
				testStoreUSb:    1.0 / 2.0,
				testStoreEurope: 1,
			},
		},
		{
			name: "three existing replicas - excluding testStoreUSb",
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15:  1.0 / 4.0,
				testStoreUSa1:   1.0 / 4.0,
				testStoreEurope: 1.0,
			},
		},
		{
			name: "three existing replicas - excluding testStoreEurope",
			expected: map[roachpb.StoreID]float64{
				testStoreUSa15: 1.0 / 4.0,
				testStoreUSa1:  1.0 / 4.0,
				testStoreUSb:   1.0 / 2.0,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			existingNodeLocalities := make(map[roachpb.NodeID]roachpb.Locality)
			for _, s := range testStores {
				if _, ok := tc.expected[s.StoreID]; ok {
					existingNodeLocalities[s.Node.NodeID] = s.Node.Locality
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

func TestBalanceScore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	EnableStatsBasedRebalancing.Override(&st.SV, true)

	storeList := StoreList{
		candidateRanges:          stat{mean: 1000},
		candidateLogicalBytes:    stat{mean: 512 * 1024 * 1024},
		candidateWritesPerSecond: stat{mean: 1000},
	}

	sEmpty := roachpb.StoreCapacity{
		Capacity:     1024 * 1024 * 1024,
		Available:    1024 * 1024 * 1024,
		LogicalBytes: 0,
	}
	sMean := roachpb.StoreCapacity{
		Capacity:        1024 * 1024 * 1024,
		Available:       512 * 1024 * 1024,
		LogicalBytes:    512 * 1024 * 1024,
		RangeCount:      1000,
		WritesPerSecond: 1000,
		BytesPerReplica: roachpb.Percentiles{
			P10: 100 * 1024,
			P25: 250 * 1024,
			P50: 500 * 1024,
			P75: 750 * 1024,
			P90: 1000 * 1024,
		},
		WritesPerReplica: roachpb.Percentiles{
			P10: 1,
			P25: 2.5,
			P50: 5,
			P75: 7.5,
			P90: 10,
		},
	}
	sRangesOverfull := sMean
	sRangesOverfull.RangeCount = 1500
	sRangesUnderfull := sMean
	sRangesUnderfull.RangeCount = 500
	sBytesOverfull := sMean
	sBytesOverfull.Available = 256 * 1024 * 1024
	sBytesOverfull.LogicalBytes = sBytesOverfull.Capacity - sBytesOverfull.Available
	sBytesUnderfull := sMean
	sBytesUnderfull.Available = 768 * 1024 * 1024
	sBytesUnderfull.LogicalBytes = sBytesUnderfull.Capacity - sBytesUnderfull.Available
	sRangesOverfullBytesOverfull := sRangesOverfull
	sRangesOverfullBytesOverfull.Available = 256 * 1024 * 1024
	sRangesOverfullBytesOverfull.LogicalBytes =
		sRangesOverfullBytesOverfull.Capacity - sRangesOverfullBytesOverfull.Available
	sRangesUnderfullBytesUnderfull := sRangesUnderfull
	sRangesUnderfullBytesUnderfull.Available = 768 * 1024 * 1024
	sRangesUnderfullBytesUnderfull.LogicalBytes =
		sRangesUnderfullBytesUnderfull.Capacity - sRangesUnderfullBytesUnderfull.Available
	sRangesUnderfullBytesOverfull := sRangesUnderfull
	sRangesUnderfullBytesOverfull.Available = 256 * 1024 * 1024
	sRangesUnderfullBytesOverfull.LogicalBytes =
		sRangesUnderfullBytesOverfull.Capacity - sRangesUnderfullBytesOverfull.Available
	sRangesOverfullBytesUnderfull := sRangesOverfull
	sRangesOverfullBytesUnderfull.Available = 768 * 1024 * 1024
	sRangesOverfullBytesUnderfull.LogicalBytes =
		sRangesOverfullBytesUnderfull.Capacity - sRangesOverfullBytesUnderfull.Available
	sRangesUnderfullBytesOverfullWritesOverfull := sRangesUnderfullBytesOverfull
	sRangesUnderfullBytesOverfullWritesOverfull.WritesPerSecond = 1500
	sRangesUnderfullBytesUnderfullWritesOverfull := sRangesUnderfullBytesUnderfull
	sRangesUnderfullBytesUnderfullWritesOverfull.WritesPerSecond = 1500

	rEmpty := RangeInfo{}
	rMedian := RangeInfo{
		LogicalBytes:    500 * 1024,
		WritesPerSecond: 5,
	}
	rHighBytes := rMedian
	rHighBytes.LogicalBytes = 2000 * 1024
	rLowBytes := rMedian
	rLowBytes.LogicalBytes = 50 * 1024
	rHighBytesHighWrites := rHighBytes
	rHighBytesHighWrites.WritesPerSecond = 20
	rHighBytesLowWrites := rHighBytes
	rHighBytesLowWrites.WritesPerSecond = 0.5
	rLowBytesHighWrites := rLowBytes
	rLowBytesHighWrites.WritesPerSecond = 20
	rLowBytesLowWrites := rLowBytes
	rLowBytesLowWrites.WritesPerSecond = 0.5
	rHighWrites := rMedian
	rHighWrites.WritesPerSecond = 20
	rLowWrites := rMedian
	rLowWrites.WritesPerSecond = 0.5

	testCases := []struct {
		sc       roachpb.StoreCapacity
		ri       RangeInfo
		expected float64
	}{
		{sEmpty, rEmpty, 3},
		{sEmpty, rMedian, 2},
		{sEmpty, rHighBytes, 2},
		{sEmpty, rLowBytes, 2},
		{sMean, rEmpty, 0},
		{sMean, rMedian, 0},
		{sMean, rHighBytes, 0},
		{sMean, rLowBytes, 0},
		{sRangesOverfull, rEmpty, -1},
		{sRangesOverfull, rMedian, -1},
		{sRangesOverfull, rHighBytes, -1},
		{sRangesOverfull, rLowBytes, -1},
		{sRangesUnderfull, rEmpty, 1},
		{sRangesUnderfull, rMedian, 1},
		{sRangesUnderfull, rHighBytes, 1},
		{sRangesUnderfull, rLowBytes, 1},
		{sBytesOverfull, rEmpty, 1},
		{sBytesOverfull, rMedian, 0},
		{sBytesOverfull, rHighBytes, -1},
		{sBytesOverfull, rLowBytes, 1},
		{sBytesUnderfull, rEmpty, -1},
		{sBytesUnderfull, rMedian, 0},
		{sBytesUnderfull, rHighBytes, 1},
		{sBytesUnderfull, rLowBytes, -1},
		{sRangesOverfullBytesOverfull, rEmpty, -.5},
		{sRangesOverfullBytesOverfull, rMedian, -2},
		{sRangesOverfullBytesOverfull, rHighBytes, -1.5},
		{sRangesOverfullBytesOverfull, rLowBytes, -.5},
		{sRangesUnderfullBytesUnderfull, rEmpty, .5},
		{sRangesUnderfullBytesUnderfull, rMedian, 2},
		{sRangesUnderfullBytesUnderfull, rHighBytes, 1.5},
		{sRangesUnderfullBytesUnderfull, rLowBytes, .5},
		{sRangesUnderfullBytesOverfull, rEmpty, 2},
		{sRangesUnderfullBytesOverfull, rMedian, 1},
		{sRangesUnderfullBytesOverfull, rHighBytes, 0},
		{sRangesUnderfullBytesOverfull, rLowBytes, 2},
		{sRangesOverfullBytesUnderfull, rEmpty, -2},
		{sRangesOverfullBytesUnderfull, rMedian, -1},
		{sRangesOverfullBytesUnderfull, rHighBytes, 0},
		{sRangesOverfullBytesUnderfull, rLowBytes, -2},
		{sRangesUnderfullBytesOverfullWritesOverfull, rEmpty, 3},
		{sRangesUnderfullBytesOverfullWritesOverfull, rMedian, 1},
		{sRangesUnderfullBytesOverfullWritesOverfull, rHighBytes, 0},
		{sRangesUnderfullBytesOverfullWritesOverfull, rHighBytesHighWrites, -1},
		{sRangesUnderfullBytesOverfullWritesOverfull, rHighBytesLowWrites, 1},
		{sRangesUnderfullBytesOverfullWritesOverfull, rLowBytes, 2},
		{sRangesUnderfullBytesOverfullWritesOverfull, rLowBytesHighWrites, 1},
		{sRangesUnderfullBytesOverfullWritesOverfull, rLowBytesLowWrites, 3},
		{sRangesUnderfullBytesUnderfullWritesOverfull, rEmpty, 1.5},
		{sRangesUnderfullBytesUnderfullWritesOverfull, rMedian, 2},
		{sRangesUnderfullBytesUnderfullWritesOverfull, rHighBytes, 1.5},
		{sRangesUnderfullBytesUnderfullWritesOverfull, rHighBytesHighWrites, 0.5},
		{sRangesUnderfullBytesUnderfullWritesOverfull, rHighBytesLowWrites, 2.5},
		{sRangesUnderfullBytesUnderfullWritesOverfull, rLowBytes, 0.5},
		{sRangesUnderfullBytesUnderfullWritesOverfull, rLowBytesHighWrites, -0.5},
		{sRangesUnderfullBytesUnderfullWritesOverfull, rLowBytesLowWrites, 1.5},
		{sRangesUnderfullBytesUnderfullWritesOverfull, rHighWrites, 1},
		{sRangesUnderfullBytesUnderfullWritesOverfull, rLowWrites, 3},
	}
	for i, tc := range testCases {
		if a, e := balanceScore(st, storeList, tc.sc, tc.ri, false), tc.expected; a.totalScore() != e {
			t.Errorf("%d: balanceScore(storeList, %+v, %+v) got %s; want %.2f", i, tc.sc, tc.ri, a, e)
		}
	}
}

func TestRebalanceConvergesOnMean(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	EnableStatsBasedRebalancing.Override(&st.SV, true)

	const diskCapacity = 2000
	storeList := StoreList{
		candidateRanges:          stat{mean: 1000},
		candidateLogicalBytes:    stat{mean: 1000},
		candidateWritesPerSecond: stat{mean: 1000},
	}
	emptyRange := RangeInfo{}
	normalRange := RangeInfo{
		LogicalBytes:    10,
		WritesPerSecond: 10,
	}
	outlierRange := RangeInfo{
		LogicalBytes:    10,
		WritesPerSecond: 10000,
	}

	testCases := []struct {
		rangeCount      int32
		liveBytes       int64
		writesPerSecond float64
		ri              RangeInfo
		toConverges     bool
		fromConverges   bool
	}{
		{0, 0, 0, emptyRange, true, false},
		{900, 900, 900, emptyRange, true, false},
		{900, 900, 2000, emptyRange, true, false},
		{999, 1000, 1000, emptyRange, true, false},
		{1000, 1000, 1000, emptyRange, false, false},
		{1001, 1000, 1000, emptyRange, false, true},
		{2000, 2000, 2000, emptyRange, false, true},
		{900, 2000, 2000, emptyRange, true, false},
		{0, 0, 0, normalRange, true, false},
		{900, 900, 900, normalRange, true, false},
		{900, 900, 2000, normalRange, true, false},
		{999, 1000, 1000, normalRange, false, false},
		{2000, 2000, 2000, normalRange, false, true},
		{900, 2000, 2000, normalRange, false, true},
		{1000, 990, 990, normalRange, true, false},
		{1000, 994, 994, normalRange, true, false},
		{1000, 990, 995, normalRange, false, false},
		{1000, 1010, 1010, normalRange, false, true},
		{1000, 1010, 1005, normalRange, false, false},
		{0, 0, 0, outlierRange, true, false},
		{900, 900, 900, outlierRange, true, false},
		{900, 900, 2000, outlierRange, true, false},
		{999, 1000, 1000, outlierRange, false, false},
		{2000, 2000, 10000, outlierRange, false, true},
		{900, 2000, 10000, outlierRange, false, true},
		{1000, 990, 990, outlierRange, false, false},
		{1000, 1000, 10000, outlierRange, false, false},
		{1000, 1010, 10000, outlierRange, false, true},
		{1001, 1010, 1005, outlierRange, false, true},
	}
	for i, tc := range testCases {
		sc := roachpb.StoreCapacity{
			Capacity:        diskCapacity,
			Available:       diskCapacity - tc.liveBytes,
			LogicalBytes:    tc.liveBytes,
			RangeCount:      tc.rangeCount,
			WritesPerSecond: tc.writesPerSecond,
		}
		if a, e := rebalanceToConvergesOnMean(st, storeList, sc, tc.ri, false), tc.toConverges; a != e {
			t.Errorf("%d: rebalanceToConvergesOnMean(storeList, %+v, %+v) got %t; want %t", i, sc, tc.ri, a, e)
		}
		if a, e := rebalanceFromConvergesOnMean(st, storeList, sc, tc.ri, false), tc.fromConverges; a != e {
			t.Errorf("%d: rebalanceFromConvergesOnMean(storeList, %+v, %+v) got %t; want %t", i, sc, tc.ri, a, e)
		}
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
