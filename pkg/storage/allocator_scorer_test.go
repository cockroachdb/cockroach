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
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/copysets"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

const (
	// testCopysetsIdleScoreDifferenceThreshold is the
	// copysetsIdleScoreDifferenceThreshold used in tests.
	testCopysetsIdleScoreDifferenceThreshold = 0.15
)

func slToMap(storeList []roachpb.StoreID) map[roachpb.StoreID]bool {
	stores := make(map[roachpb.StoreID]bool)
	for _, storeID := range storeList {
		stores[storeID] = true
	}
	return stores
}

type storeScore struct {
	storeID roachpb.StoreID
	score   float64
}

type storeScores []storeScore

func (s storeScores) Len() int { return len(s) }
func (s storeScores) Less(i, j int) bool {
	if s[i].score == s[j].score {
		// Ensure a deterministic ordering of equivalent scores
		return s[i].storeID > s[j].storeID
	}
	return s[i].score < s[j].score
}
func (s storeScores) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func TestOnlyValidAndNotFull(t *testing.T) {
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

			valid := cl.onlyValidAndNotFull()
			if a, e := len(valid), tc.valid; a != e {
				t.Errorf("expected %d valid, actual %d", e, a)
			}
			if a, e := len(cl)-len(valid), tc.invalid; a != e {
				t.Errorf("expected %d invalid, actual %d", e, a)
			}
		})
	}
}

// TestSelectGoodPanic is a basic regression test against a former panic in
// selectGood when called with just invalid/full stores.
func TestSelectGoodPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cl := candidateList{
		candidate{
			valid: false,
		},
	}
	allocRand := makeAllocatorRand(rand.NewSource(0))
	if good := cl.selectGood(allocRand); good != nil {
		t.Errorf("cl.selectGood() got %v, want nil", good)
	}
}

// TestCandidateSelection tests select{good,bad} and {best,worst}constraints.
func TestCandidateSelection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type scoreTuple struct {
		copyset    int
		diversity  int
		rangeCount int
	}
	genCandidates := func(scores []scoreTuple, idShift int) candidateList {
		var cl candidateList
		for i, score := range scores {
			cl = append(cl, candidate{
				store: roachpb.StoreDescriptor{
					StoreID: roachpb.StoreID(i + idShift),
				},
				copysetScore:   float64(score.copyset),
				diversityScore: float64(score.diversity),
				rangeCount:     score.rangeCount,
				valid:          true,
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
			buffer.WriteString(
				fmt.Sprintf(
					"%d-%d-%d",
					int(c.copysetScore),
					int(c.diversityScore),
					c.rangeCount,
				),
			)
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
			candidates: []scoreTuple{{0, 0, 0}},
			best:       []scoreTuple{{0, 0, 0}},
			worst:      []scoreTuple{{0, 0, 0}},
			good:       scoreTuple{0, 0, 0},
			bad:        scoreTuple{0, 0, 0},
		},
		{
			candidates: []scoreTuple{{0, 0, 0}, {0, 0, 1}},
			best:       []scoreTuple{{0, 0, 0}, {0, 0, 1}},
			worst:      []scoreTuple{{0, 0, 0}, {0, 0, 1}},
			good:       scoreTuple{0, 0, 0},
			bad:        scoreTuple{0, 0, 1},
		},
		{
			candidates: []scoreTuple{{0, 0, 0}, {0, 0, 1}, {0, 0, 2}},
			best:       []scoreTuple{{0, 0, 0}, {0, 0, 1}, {0, 0, 2}},
			worst:      []scoreTuple{{0, 0, 0}, {0, 0, 1}, {0, 0, 2}},
			good:       scoreTuple{0, 0, 1},
			bad:        scoreTuple{0, 0, 2},
		},
		{
			candidates: []scoreTuple{{0, 1, 0}, {0, 0, 1}},
			best:       []scoreTuple{{0, 1, 0}},
			worst:      []scoreTuple{{0, 0, 1}},
			good:       scoreTuple{0, 1, 0},
			bad:        scoreTuple{0, 0, 1},
		},
		{
			candidates: []scoreTuple{{0, 1, 0}, {0, 0, 1}, {0, 0, 2}},
			best:       []scoreTuple{{0, 1, 0}},
			worst:      []scoreTuple{{0, 0, 1}, {0, 0, 2}},
			good:       scoreTuple{0, 1, 0},
			bad:        scoreTuple{0, 0, 2},
		},
		{
			candidates: []scoreTuple{{0, 1, 0}, {0, 1, 1}, {0, 0, 2}},
			best:       []scoreTuple{{0, 1, 0}, {0, 1, 1}},
			worst:      []scoreTuple{{0, 0, 2}},
			good:       scoreTuple{0, 1, 0},
			bad:        scoreTuple{0, 0, 2},
		},
		{
			candidates: []scoreTuple{{0, 1, 0}, {0, 1, 1}, {0, 0, 2}, {0, 0, 3}},
			best:       []scoreTuple{{0, 1, 0}, {0, 1, 1}},
			worst:      []scoreTuple{{0, 0, 2}, {0, 0, 3}},
			good:       scoreTuple{0, 1, 0},
			bad:        scoreTuple{0, 0, 3},
		},
		{
			candidates: []scoreTuple{{1, 0, 0}, {0, 0, 0}, {0, 0, 1}},
			best:       []scoreTuple{{1, 0, 0}},
			worst:      []scoreTuple{{0, 0, 0}, {0, 0, 1}},
			good:       scoreTuple{1, 0, 0},
			bad:        scoreTuple{0, 0, 1},
		},
		{
			candidates: []scoreTuple{{1, 0, 0}, {1, 0, 1}, {0, 0, 1}},
			best:       []scoreTuple{{1, 0, 0}, {1, 0, 1}},
			worst:      []scoreTuple{{0, 0, 1}},
			good:       scoreTuple{1, 0, 0},
			bad:        scoreTuple{0, 0, 1},
		},
		{
			candidates: []scoreTuple{{1, 1, 0}, {1, 0, 1}, {0, 0, 1}},
			best:       []scoreTuple{{1, 1, 0}},
			worst:      []scoreTuple{{0, 0, 1}},
			good:       scoreTuple{1, 1, 0},
			bad:        scoreTuple{0, 0, 1},
		},
	}

	allocRand := makeAllocatorRand(rand.NewSource(0))
	for _, tc := range testCases {
		cl := genCandidates(tc.candidates, 1)
		t.Run(fmt.Sprintf("best-%s", formatter(cl)), func(t *testing.T) {
			a := assert.New(t)
			a.Equal(genCandidates(tc.best, 1), cl.best())

		})
		t.Run(fmt.Sprintf("worst-%s", formatter(cl)), func(t *testing.T) {
			// Shifting the ids is required to match the end of the list.
			a := assert.New(t)
			a.Equal(genCandidates(
				tc.worst,
				len(tc.candidates)-len(tc.worst)+1,
			), cl.worst())

		})
		t.Run(fmt.Sprintf("good-%s", formatter(cl)), func(t *testing.T) {
			a := assert.New(t)
			good := cl.selectGood(allocRand)
			if a.NotNil(good) {
				actual := scoreTuple{
					int(good.copysetScore), int(good.diversityScore), good.rangeCount,
				}
				a.Equal(tc.good, actual)

			}
		})
		t.Run(fmt.Sprintf("bad-%s", formatter(cl)), func(t *testing.T) {
			a := assert.New(t)
			bad := cl.selectBad(allocRand)
			if a.NotNil(bad) {
				actual := scoreTuple{
					int(bad.copysetScore), int(bad.diversityScore), bad.rangeCount,
				}
				a.Equal(tc.bad, actual)

			}
		})
	}
}

func TestBetterThan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCandidateList := candidateList{
		{
			valid:          true,
			copysetScore:   1,
			diversityScore: 1,
			rangeCount:     0,
		},
		{
			valid:          true,
			copysetScore:   1,
			diversityScore: 0,
			rangeCount:     0,
		},
		{
			valid:          true,
			diversityScore: 1,
			rangeCount:     0,
		},
		{
			valid:          true,
			diversityScore: 1,
			rangeCount:     0,
		},
		{
			valid:          true,
			diversityScore: 1,
			rangeCount:     1,
		},
		{
			valid:          true,
			diversityScore: 1,
			rangeCount:     1,
		},
		{
			valid:          true,
			diversityScore: 0,
			rangeCount:     0,
		},
		{
			valid:          true,
			diversityScore: 0,
			rangeCount:     0,
		},
		{
			valid:          true,
			diversityScore: 0,
			rangeCount:     1,
		},
		{
			valid:          true,
			diversityScore: 0,
			rangeCount:     1,
		},
		{
			valid:          false,
			copysetScore:   1,
			diversityScore: 1,
			rangeCount:     0,
		},
		{
			valid:          false,
			diversityScore: 1,
			rangeCount:     0,
		},
		{
			valid:          false,
			diversityScore: 0,
			rangeCount:     0,
		},
		{
			valid:          false,
			diversityScore: 0,
			rangeCount:     1,
		},
	}

	expectedResults := []int{0, 1, 2, 2, 4, 4, 6, 6, 8, 8, 10, 10, 10, 10}

	for i := 0; i < len(testCandidateList); i++ {
		betterThan := testCandidateList.betterThan(testCandidateList[i])
		if e, a := expectedResults[i], len(betterThan); e != a {
			t.Errorf("expected %d results, actual %d", e, a)
		}
	}
}

// TestBestRebalanceTarget constructs a hypothetical output of
// rebalanceCandidates and verifies that bestRebalanceTarget properly returns
// the candidates in the ideal order of preference and omits any that aren't
// desirable.
func TestBestRebalanceTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()

	candidates := []rebalanceOptions{
		{
			candidates: []candidate{
				{
					store:          roachpb.StoreDescriptor{StoreID: 11},
					valid:          true,
					necessary:      true,
					diversityScore: 1.0,
					rangeCount:     11,
				},
				{
					store:          roachpb.StoreDescriptor{StoreID: 12},
					valid:          true,
					necessary:      true,
					diversityScore: 1.0,
					rangeCount:     12,
				},
			},
			existingCandidates: []candidate{
				{
					store:          roachpb.StoreDescriptor{StoreID: 1},
					valid:          true,
					necessary:      true,
					diversityScore: 0,
					rangeCount:     1,
				},
			},
		},
		{
			candidates: []candidate{
				{
					store:          roachpb.StoreDescriptor{StoreID: 13},
					valid:          true,
					necessary:      true,
					diversityScore: 1.0,
					rangeCount:     13,
				},
				{
					store:          roachpb.StoreDescriptor{StoreID: 14},
					valid:          false,
					necessary:      false,
					diversityScore: 0.0,
					rangeCount:     14,
				},
			},
			existingCandidates: []candidate{
				{
					store:          roachpb.StoreDescriptor{StoreID: 2},
					valid:          true,
					necessary:      false,
					diversityScore: 0,
					rangeCount:     2,
				},
				{
					store:          roachpb.StoreDescriptor{StoreID: 3},
					valid:          false,
					necessary:      false,
					diversityScore: 0,
					rangeCount:     3,
				},
			},
		},
	}

	expected := []roachpb.StoreID{13, 11, 12}
	allocRand := makeAllocatorRand(rand.NewSource(0))
	var i int
	for {
		i++
		target, existing := bestRebalanceTarget(allocRand, candidates)
		if len(expected) == 0 {
			if target == nil {
				break
			}
			t.Errorf("round %d: expected nil, got target=%+v, existing=%+v", i, target, existing)
			continue
		}
		if target == nil {
			t.Errorf("round %d: expected s%d, got nil", i, expected[0])
		} else if target.store.StoreID != expected[0] {
			t.Errorf("round %d: expected s%d, got target=%+v, existing=%+v",
				i, expected[0], target, existing)
		}
		expected = expected[1:]
	}
}

func TestStoreHasReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var existing []roachpb.ReplicaDescriptor
	for i := 2; i < 10; i += 2 {
		existing = append(existing, roachpb.ReplicaDescriptor{StoreID: roachpb.StoreID(i)})
	}
	for i := 1; i < 10; i++ {
		if e, a := i%2 == 0, storeHasReplica(roachpb.StoreID(i), existing); e != a {
			t.Errorf("StoreID %d expected to be %t, got %t", i, e, a)
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

	testStores = map[roachpb.StoreID]roachpb.StoreDescriptor{
		testStoreUSa15: {
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
		testStoreUSa15Dupe: {
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
		testStoreUSa1: {
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
		testStoreUSb: {
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
		testStoreEurope: {
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

func getTestStoreDesc(storeID roachpb.StoreID) (roachpb.StoreDescriptor, bool) {
	desc, ok := testStores[storeID]
	return desc, ok
}

func testStoreReplicas(storeIDs []roachpb.StoreID) []roachpb.ReplicaDescriptor {
	var result []roachpb.ReplicaDescriptor
	for _, storeID := range storeIDs {
		result = append(result, roachpb.ReplicaDescriptor{
			NodeID:  roachpb.NodeID(storeID),
			StoreID: storeID,
		})
	}
	return result
}

func TestConstraintsCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name        string
		constraints []config.Constraints
		expected    map[roachpb.StoreID]bool
	}{
		{
			name: "required constraint",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Value: "b", Type: config.Constraint_REQUIRED},
					},
				},
			},
			expected: map[roachpb.StoreID]bool{
				testStoreUSa1: true,
				testStoreUSb:  true,
			},
		},
		{
			name: "required locality constraints",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Key: "datacenter", Value: "us", Type: config.Constraint_REQUIRED},
					},
				},
			},
			expected: map[roachpb.StoreID]bool{
				testStoreUSa15:     true,
				testStoreUSa15Dupe: true,
				testStoreUSa1:      true,
				testStoreUSb:       true,
			},
		},
		{
			name: "prohibited constraints",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Value: "b", Type: config.Constraint_PROHIBITED},
					},
				},
			},
			expected: map[roachpb.StoreID]bool{
				testStoreUSa15:     true,
				testStoreUSa15Dupe: true,
				testStoreEurope:    true,
			},
		},
		{
			name: "prohibited locality constraints",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Key: "datacenter", Value: "us", Type: config.Constraint_PROHIBITED},
					},
				},
			},
			expected: map[roachpb.StoreID]bool{
				testStoreEurope: true,
			},
		},
		{
			name: "positive constraints are ignored",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Value: "a", Type: config.Constraint_DEPRECATED_POSITIVE},
						{Value: "b", Type: config.Constraint_DEPRECATED_POSITIVE},
						{Value: "c", Type: config.Constraint_DEPRECATED_POSITIVE},
					},
				},
			},
			expected: map[roachpb.StoreID]bool{
				testStoreUSa15:     true,
				testStoreUSa15Dupe: true,
				testStoreUSa1:      true,
				testStoreUSb:       true,
				testStoreEurope:    true,
			},
		},
		{
			name: "positive locality constraints are ignored",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Key: "datacenter", Value: "eur", Type: config.Constraint_DEPRECATED_POSITIVE},
					},
				},
			},
			expected: map[roachpb.StoreID]bool{
				testStoreUSa15:     true,
				testStoreUSa15Dupe: true,
				testStoreUSa1:      true,
				testStoreUSb:       true,
				testStoreEurope:    true,
			},
		},
		{
			name: "NumReplicas doesn't affect constraint checking",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Key: "datacenter", Value: "eur", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
			},
			expected: map[roachpb.StoreID]bool{
				testStoreEurope: true,
			},
		},
		{
			name: "multiple per-replica constraints are respected",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Key: "datacenter", Value: "eur", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
				{
					Constraints: []config.Constraint{
						{Value: "b", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
			},
			expected: map[roachpb.StoreID]bool{
				testStoreUSa1:   true,
				testStoreUSb:    true,
				testStoreEurope: true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, s := range testStores {
				valid := constraintsCheck(s, tc.constraints)
				ok := tc.expected[s.StoreID]
				if valid != ok {
					t.Errorf("expected store %d to be %t, but got %t", s.StoreID, ok, valid)
					continue
				}
			}
		})
	}
}

func TestAllocateConstraintsCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name              string
		constraints       []config.Constraints
		zoneNumReplicas   int32
		existing          []roachpb.StoreID
		expectedValid     map[roachpb.StoreID]bool
		expectedNecessary map[roachpb.StoreID]bool
	}{
		{
			name: "prohibited constraint",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Value: "b", Type: config.Constraint_PROHIBITED},
					},
				},
			},
			existing: nil,
			expectedValid: map[roachpb.StoreID]bool{
				testStoreUSa15:     true,
				testStoreUSa15Dupe: true,
				testStoreEurope:    true,
			},
			expectedNecessary: map[roachpb.StoreID]bool{},
		},
		{
			name: "required constraint",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Value: "b", Type: config.Constraint_REQUIRED},
					},
				},
			},
			existing: nil,
			expectedValid: map[roachpb.StoreID]bool{
				testStoreUSa1: true,
				testStoreUSb:  true,
			},
			expectedNecessary: map[roachpb.StoreID]bool{},
		},
		{
			name: "required constraint with NumReplicas",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Value: "b", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 3,
				},
			},
			existing: nil,
			expectedValid: map[roachpb.StoreID]bool{
				testStoreUSa1: true,
				testStoreUSb:  true,
			},
			expectedNecessary: map[roachpb.StoreID]bool{
				testStoreUSa1: true,
				testStoreUSb:  true,
			},
		},
		{
			name: "multiple required constraints with NumReplicas",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Value: "a", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
				{
					Constraints: []config.Constraint{
						{Value: "b", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
			},
			existing: nil,
			expectedValid: map[roachpb.StoreID]bool{
				testStoreUSa15:     true,
				testStoreUSa15Dupe: true,
				testStoreUSa1:      true,
				testStoreUSb:       true,
			},
			expectedNecessary: map[roachpb.StoreID]bool{
				testStoreUSa15:     true,
				testStoreUSa15Dupe: true,
				testStoreUSa1:      true,
				testStoreUSb:       true,
			},
		},
		{
			name: "multiple required constraints with NumReplicas and existing replicas",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Value: "a", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
				{
					Constraints: []config.Constraint{
						{Value: "b", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
			},
			existing: []roachpb.StoreID{testStoreUSa1},
			expectedValid: map[roachpb.StoreID]bool{
				testStoreUSa15:     true,
				testStoreUSa15Dupe: true,
				testStoreUSa1:      true,
				testStoreUSb:       true,
			},
			expectedNecessary: map[roachpb.StoreID]bool{},
		},
		{
			name: "multiple required constraints with NumReplicas and not enough existing replicas",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Value: "a", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
				{
					Constraints: []config.Constraint{
						{Value: "b", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 2,
				},
			},
			existing: []roachpb.StoreID{testStoreUSa1},
			expectedValid: map[roachpb.StoreID]bool{
				testStoreUSa15:     true,
				testStoreUSa15Dupe: true,
				testStoreUSa1:      true,
				testStoreUSb:       true,
			},
			expectedNecessary: map[roachpb.StoreID]bool{
				testStoreUSa1: true,
				testStoreUSb:  true,
			},
		},
		{
			name: "multiple required constraints with NumReplicas and sum(NumReplicas) < zone.NumReplicas",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Value: "a", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
				{
					Constraints: []config.Constraint{
						{Value: "b", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
			},
			zoneNumReplicas: 3,
			existing:        nil,
			expectedValid: map[roachpb.StoreID]bool{
				testStoreUSa15:     true,
				testStoreUSa15Dupe: true,
				testStoreUSa1:      true,
				testStoreUSb:       true,
				testStoreEurope:    true,
			},
			expectedNecessary: map[roachpb.StoreID]bool{
				testStoreUSa15:     true,
				testStoreUSa15Dupe: true,
				testStoreUSa1:      true,
				testStoreUSb:       true,
			},
		},
		{
			name: "multiple required constraints with sum(NumReplicas) < zone.NumReplicas and not enough existing replicas",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Value: "a", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
				{
					Constraints: []config.Constraint{
						{Value: "b", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 2,
				},
			},
			zoneNumReplicas: 5,
			existing:        []roachpb.StoreID{testStoreUSa1},
			expectedValid: map[roachpb.StoreID]bool{
				testStoreUSa15:     true,
				testStoreUSa15Dupe: true,
				testStoreUSa1:      true,
				testStoreUSb:       true,
				testStoreEurope:    true,
			},
			expectedNecessary: map[roachpb.StoreID]bool{
				testStoreUSa1: true,
				testStoreUSb:  true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			zone := &config.ZoneConfig{
				Constraints: tc.constraints,
				NumReplicas: proto.Int32(tc.zoneNumReplicas),
			}
			analyzed := analyzeConstraints(
				context.Background(), getTestStoreDesc, testStoreReplicas(tc.existing), zone)
			for _, s := range testStores {
				valid, necessary := allocateConstraintsCheck(s, analyzed)
				if e, a := tc.expectedValid[s.StoreID], valid; e != a {
					t.Errorf("expected allocateConstraintsCheck(s%d).valid to be %t, but got %t",
						s.StoreID, e, a)
				}
				if e, a := tc.expectedNecessary[s.StoreID], necessary; e != a {
					t.Errorf("expected allocateConstraintsCheck(s%d).necessary to be %t, but got %t",
						s.StoreID, e, a)
				}
			}
		})
	}
}

func TestRemoveConstraintsCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type expected struct {
		valid, necessary bool
	}
	testCases := []struct {
		name            string
		constraints     []config.Constraints
		zoneNumReplicas int32
		expected        map[roachpb.StoreID]expected
	}{
		{
			name: "prohibited constraint",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Value: "b", Type: config.Constraint_PROHIBITED},
					},
				},
			},
			expected: map[roachpb.StoreID]expected{
				testStoreUSa15:     {true, false},
				testStoreUSa15Dupe: {true, false},
				testStoreEurope:    {true, false},
				testStoreUSa1:      {false, false},
			},
		},
		{
			name: "required constraint",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Value: "b", Type: config.Constraint_REQUIRED},
					},
				},
			},
			expected: map[roachpb.StoreID]expected{
				testStoreUSa15:     {false, false},
				testStoreUSa15Dupe: {false, false},
				testStoreEurope:    {false, false},
				testStoreUSa1:      {true, false},
			},
		},
		{
			name: "required constraint with NumReplicas",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Value: "b", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 2,
				},
			},
			expected: map[roachpb.StoreID]expected{
				testStoreUSa15:  {false, false},
				testStoreEurope: {false, false},
				testStoreUSa1:   {true, true},
				testStoreUSb:    {true, true},
			},
		},
		{
			name: "multiple required constraints with NumReplicas",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Value: "a", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
				{
					Constraints: []config.Constraint{
						{Value: "b", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
			},
			expected: map[roachpb.StoreID]expected{
				testStoreUSa15:  {true, false},
				testStoreUSa1:   {true, true},
				testStoreEurope: {false, false},
			},
		},
		{
			name: "required constraint with NumReplicas and sum(NumReplicas) < zone.NumReplicas",
			constraints: []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Value: "b", Type: config.Constraint_REQUIRED},
					},
					NumReplicas: 2,
				},
			},
			zoneNumReplicas: 3,
			expected: map[roachpb.StoreID]expected{
				testStoreUSa15:  {true, false},
				testStoreEurope: {true, false},
				testStoreUSa1:   {true, true},
				testStoreUSb:    {true, true},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var existing []roachpb.ReplicaDescriptor
			for storeID := range tc.expected {
				existing = append(existing, roachpb.ReplicaDescriptor{
					NodeID:  roachpb.NodeID(storeID),
					StoreID: storeID,
				})
			}
			zone := &config.ZoneConfig{
				Constraints: tc.constraints,
				NumReplicas: proto.Int32(tc.zoneNumReplicas),
			}
			analyzed := analyzeConstraints(context.Background(), getTestStoreDesc, existing, zone)
			for storeID, expected := range tc.expected {
				valid, necessary := removeConstraintsCheck(testStores[storeID], analyzed)
				if e, a := expected.valid, valid; e != a {
					t.Errorf("expected removeConstraintsCheck(s%d).valid to be %t, but got %t",
						storeID, e, a)
				}
				if e, a := expected.necessary, necessary; e != a {
					t.Errorf("expected removeConstraintsCheck(s%d).necessary to be %t, but got %t",
						storeID, e, a)
				}
			}
		})
	}
}

func TestShouldRebalanceDiversity(t *testing.T) {
	defer leaktest.AfterTest(t)()

	options := scorerOptions{}
	newStore := func(id int, locality roachpb.Locality) roachpb.StoreDescriptor {
		return roachpb.StoreDescriptor{
			StoreID: roachpb.StoreID(id),
			Node: roachpb.NodeDescriptor{
				NodeID:   roachpb.NodeID(id),
				Locality: locality,
			},
		}
	}
	localityForNodeID := func(sl StoreList, id roachpb.NodeID) roachpb.Locality {
		for _, store := range sl.stores {
			if store.Node.NodeID == id {
				return store.Node.Locality
			}
		}
		t.Fatalf("no locality for n%d in StoreList %+v", id, sl)
		return roachpb.Locality{}
	}
	locUS := roachpb.Locality{
		Tiers: testStoreTierSetup("us", "", "", ""),
	}
	locEU := roachpb.Locality{
		Tiers: testStoreTierSetup("eu", "", "", ""),
	}
	locAS := roachpb.Locality{
		Tiers: testStoreTierSetup("as", "", "", ""),
	}
	locAU := roachpb.Locality{
		Tiers: testStoreTierSetup("au", "", "", ""),
	}
	sl3by3 := StoreList{
		stores: []roachpb.StoreDescriptor{
			newStore(1, locUS), newStore(2, locUS), newStore(3, locUS),
			newStore(4, locEU), newStore(5, locEU), newStore(6, locEU),
			newStore(7, locAS), newStore(8, locAS), newStore(9, locAS),
		},
	}
	sl4by3 := StoreList{
		stores: []roachpb.StoreDescriptor{
			newStore(1, locUS), newStore(2, locUS), newStore(3, locUS),
			newStore(4, locEU), newStore(5, locEU), newStore(6, locEU),
			newStore(7, locAS), newStore(8, locAS), newStore(9, locAS),
			newStore(10, locAU), newStore(11, locAU), newStore(12, locAU),
		},
	}

	testCases := []struct {
		s               roachpb.StoreDescriptor
		sl              StoreList
		existingNodeIDs []roachpb.NodeID
		expected        bool
	}{
		{
			s:               newStore(1, locUS),
			sl:              sl3by3,
			existingNodeIDs: []roachpb.NodeID{1, 2, 3},
			expected:        true,
		},
		{
			s:               newStore(1, locUS),
			sl:              sl3by3,
			existingNodeIDs: []roachpb.NodeID{1, 4, 7},
			expected:        false,
		},
		{
			s:               newStore(1, locUS),
			sl:              sl3by3,
			existingNodeIDs: []roachpb.NodeID{1, 4, 5},
			expected:        false,
		},
		{
			s:               newStore(4, locEU),
			sl:              sl3by3,
			existingNodeIDs: []roachpb.NodeID{1, 4, 5},
			expected:        true,
		},
		{
			s:               newStore(1, locUS),
			sl:              sl4by3,
			existingNodeIDs: []roachpb.NodeID{1, 2, 3},
			expected:        true,
		},
		{
			s:               newStore(1, locUS),
			sl:              sl4by3,
			existingNodeIDs: []roachpb.NodeID{1, 4, 7},
			expected:        false,
		},
		{
			s:               newStore(1, locUS),
			sl:              sl4by3,
			existingNodeIDs: []roachpb.NodeID{1, 4, 7, 10},
			expected:        false,
		},
		{
			s:               newStore(1, locUS),
			sl:              sl4by3,
			existingNodeIDs: []roachpb.NodeID{1, 2, 4, 7, 10},
			expected:        false,
		},
		{
			s:               newStore(1, locUS),
			sl:              sl4by3,
			existingNodeIDs: []roachpb.NodeID{1, 4, 5, 7, 10},
			expected:        false,
		},
	}
	for i, tc := range testCases {
		removeStore := func(sl StoreList, nodeID roachpb.NodeID) StoreList {
			for i, s := range sl.stores {
				if s.Node.NodeID == nodeID {
					return makeStoreList(append(sl.stores[:i], sl.stores[i+1:]...))
				}
			}
			return sl
		}
		filteredSL := tc.sl
		filteredSL.stores = append([]roachpb.StoreDescriptor(nil), filteredSL.stores...)
		existingNodeLocalities := make(map[roachpb.NodeID]roachpb.Locality)
		rangeInfo := RangeInfo{
			Desc: &roachpb.RangeDescriptor{},
		}
		for _, nodeID := range tc.existingNodeIDs {
			rangeInfo.Desc.Replicas = append(rangeInfo.Desc.Replicas, roachpb.ReplicaDescriptor{
				NodeID:  nodeID,
				StoreID: roachpb.StoreID(nodeID),
			})
			existingNodeLocalities[nodeID] = localityForNodeID(tc.sl, nodeID)
			// For the sake of testing, remove all other existing stores from the
			// store list to only test whether we want to remove the replica on tc.s.
			if nodeID != tc.s.Node.NodeID {
				filteredSL = removeStore(filteredSL, nodeID)
			}
		}

		targets := rebalanceCandidates(
			context.Background(),
			filteredSL,
			analyzedConstraints{},
			rangeInfo,
			existingNodeLocalities,
			func(nodeID roachpb.NodeID) string {
				locality := localityForNodeID(tc.sl, nodeID)
				return locality.String()
			},
			options,
			nil)
		actual := len(targets) > 0
		if actual != tc.expected {
			t.Errorf("%d: shouldRebalance on s%d with replicas on %v got %t, expected %t",
				i, tc.s.StoreID, tc.existingNodeIDs, actual, tc.expected)
		}
	}
}

func TestAllocateDiversityScore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Given a range that's located on stores, rank order which of the testStores
	// are the best fit for allocating a new replica on.
	testCases := []struct {
		name     string
		stores   []roachpb.StoreID
		expected []roachpb.StoreID
	}{
		{
			name:     "no existing replicas",
			expected: []roachpb.StoreID{testStoreUSa15, testStoreUSa15Dupe, testStoreUSa1, testStoreUSb, testStoreEurope},
		},
		{
			name:     "one existing replicas",
			stores:   []roachpb.StoreID{testStoreUSa15},
			expected: []roachpb.StoreID{testStoreEurope, testStoreUSb, testStoreUSa1, testStoreUSa15Dupe},
		},
		{
			name:     "two existing replicas",
			stores:   []roachpb.StoreID{testStoreUSa15, testStoreEurope},
			expected: []roachpb.StoreID{testStoreUSb, testStoreUSa1, testStoreUSa15Dupe},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			existingNodeLocalities := make(map[roachpb.NodeID]roachpb.Locality)
			for _, s := range tc.stores {
				existingNodeLocalities[testStores[s].Node.NodeID] = testStores[s].Node.Locality
			}
			var scores storeScores
			for _, s := range testStores {
				if _, ok := existingNodeLocalities[s.Node.NodeID]; ok {
					continue
				}
				var score storeScore
				actualScore := diversityAllocateScore(s, existingNodeLocalities)
				score.storeID = s.StoreID
				score.score = actualScore
				scores = append(scores, score)
			}
			sort.Sort(sort.Reverse(scores))
			for i := 0; i < len(scores); {
				if scores[i].storeID != tc.expected[i] {
					t.Fatalf("expected the result store order to be %v, but got %v", tc.expected, scores)
				}
				i++
			}
		})
	}
}

func TestRebalanceToDiversityScore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Given a range that's located on stores, rank order which of the testStores
	// are the best fit for rebalancing to.
	testCases := []struct {
		name     string
		stores   []roachpb.StoreID
		expected []roachpb.StoreID
	}{
		{
			name:     "no existing replicas",
			expected: []roachpb.StoreID{testStoreUSa15, testStoreUSa15Dupe, testStoreUSa1, testStoreUSb, testStoreEurope},
		},
		{
			name:     "one existing replica",
			stores:   []roachpb.StoreID{testStoreUSa15},
			expected: []roachpb.StoreID{testStoreUSa15Dupe, testStoreUSa1, testStoreUSb, testStoreEurope},
		},
		{
			name:     "two existing replicas",
			stores:   []roachpb.StoreID{testStoreUSa15, testStoreUSa1},
			expected: []roachpb.StoreID{testStoreEurope, testStoreUSb, testStoreUSa15Dupe},
		},
		{
			name:     "three existing replicas",
			stores:   []roachpb.StoreID{testStoreUSa15, testStoreUSa1, testStoreEurope},
			expected: []roachpb.StoreID{testStoreUSb, testStoreUSa15Dupe},
		},
		{
			name:     "three existing replicas with duplicate",
			stores:   []roachpb.StoreID{testStoreUSa15, testStoreUSa15Dupe, testStoreUSa1},
			expected: []roachpb.StoreID{testStoreEurope, testStoreUSb},
		},
		{
			name:     "four existing replicas",
			stores:   []roachpb.StoreID{testStoreUSa15, testStoreUSa1, testStoreUSb, testStoreEurope},
			expected: []roachpb.StoreID{testStoreUSa15Dupe},
		},
		{
			name:     "four existing replicas with duplicate",
			stores:   []roachpb.StoreID{testStoreUSa15, testStoreUSa15Dupe, testStoreUSa1, testStoreUSb},
			expected: []roachpb.StoreID{testStoreEurope},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			existingNodeLocalities := make(map[roachpb.NodeID]roachpb.Locality)
			for _, s := range tc.stores {
				existingNodeLocalities[testStores[s].Node.NodeID] = testStores[s].Node.Locality
			}
			var scores storeScores
			for _, s := range testStores {
				if _, ok := existingNodeLocalities[s.Node.NodeID]; ok {
					continue
				}
				var score storeScore
				actualScore := diversityRebalanceScore(s, existingNodeLocalities)
				score.storeID = s.StoreID
				score.score = actualScore
				scores = append(scores, score)
			}
			sort.Sort(sort.Reverse(scores))
			for i := 0; i < len(scores); {
				if scores[i].storeID != tc.expected[i] {
					t.Fatalf("expected the result store order to be %v, but got %v", tc.expected, scores)
				}
				i++
			}
		})
	}
}

func TestRemovalDiversityScore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Given a range that's located on stores, rank order which of the replicas
	// should be removed.
	testCases := []struct {
		name     string
		stores   []roachpb.StoreID
		expected []roachpb.StoreID
	}{
		{
			name:     "four existing replicas",
			stores:   []roachpb.StoreID{testStoreUSa15, testStoreUSa1, testStoreUSb, testStoreEurope},
			expected: []roachpb.StoreID{testStoreEurope, testStoreUSb, testStoreUSa15, testStoreUSa1},
		},
		{
			name:     "four existing replicas with duplicate",
			stores:   []roachpb.StoreID{testStoreUSa15, testStoreUSa15Dupe, testStoreUSb, testStoreEurope},
			expected: []roachpb.StoreID{testStoreEurope, testStoreUSb, testStoreUSa15, testStoreUSa15Dupe},
		},
		{
			name:     "three existing replicas - excluding testStoreUSa15",
			stores:   []roachpb.StoreID{testStoreUSa1, testStoreUSb, testStoreEurope},
			expected: []roachpb.StoreID{testStoreEurope, testStoreUSa1, testStoreUSb},
		},
		{
			name:     "three existing replicas - excluding testStoreUSa1",
			stores:   []roachpb.StoreID{testStoreUSa15, testStoreUSb, testStoreEurope},
			expected: []roachpb.StoreID{testStoreEurope, testStoreUSa15, testStoreUSb},
		},
		{
			name:     "three existing replicas - excluding testStoreUSb",
			stores:   []roachpb.StoreID{testStoreUSa15, testStoreUSa1, testStoreEurope},
			expected: []roachpb.StoreID{testStoreEurope, testStoreUSa15, testStoreUSa1},
		},
		{
			name:     "three existing replicas - excluding testStoreEurope",
			stores:   []roachpb.StoreID{testStoreUSa15, testStoreUSa1, testStoreUSb},
			expected: []roachpb.StoreID{testStoreUSb, testStoreUSa15, testStoreUSa1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			existingNodeLocalities := make(map[roachpb.NodeID]roachpb.Locality)
			for _, s := range tc.stores {
				existingNodeLocalities[testStores[s].Node.NodeID] = testStores[s].Node.Locality
			}
			var scores storeScores
			for _, storeID := range tc.stores {
				s := testStores[storeID]
				var score storeScore
				actualScore := diversityRemovalScore(s.Node.NodeID, existingNodeLocalities)
				score.storeID = s.StoreID
				score.score = actualScore
				scores = append(scores, score)
			}
			sort.Sort(sort.Reverse(scores))
			for i := 0; i < len(scores); {
				if scores[i].storeID != tc.expected[i] {
					t.Fatalf("expected the result store order to be %v, but got %v", tc.expected, scores)
				}
				i++
			}
		})
	}
}

func TestDiversityScoreEquivalence(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		stores   []roachpb.StoreID
		expected float64
	}{
		{[]roachpb.StoreID{}, 1.0},
		{[]roachpb.StoreID{testStoreUSa15}, 1.0},
		{[]roachpb.StoreID{testStoreUSa15, testStoreUSa15Dupe}, 0.0},
		{[]roachpb.StoreID{testStoreUSa15, testStoreUSa1}, 0.25},
		{[]roachpb.StoreID{testStoreUSa15, testStoreUSb}, 0.5},
		{[]roachpb.StoreID{testStoreUSa15, testStoreEurope}, 1.0},
		{[]roachpb.StoreID{testStoreUSa15, testStoreUSa15Dupe, testStoreUSa1}, 1.0 / 6.0},
		{[]roachpb.StoreID{testStoreUSa15, testStoreUSa15Dupe, testStoreUSb}, 1.0 / 3.0},
		{[]roachpb.StoreID{testStoreUSa15, testStoreUSa15Dupe, testStoreEurope}, 2.0 / 3.0},
		{[]roachpb.StoreID{testStoreUSa15, testStoreUSa1, testStoreUSb}, 5.0 / 12.0},
		{[]roachpb.StoreID{testStoreUSa15, testStoreUSa1, testStoreEurope}, 3.0 / 4.0},
		{[]roachpb.StoreID{testStoreUSa1, testStoreUSb, testStoreEurope}, 5.0 / 6.0},
		{[]roachpb.StoreID{testStoreUSa15, testStoreUSa15Dupe, testStoreUSa1, testStoreUSb}, 1.0 / 3.0},
		{[]roachpb.StoreID{testStoreUSa15, testStoreUSa15Dupe, testStoreUSa1, testStoreEurope}, 7.0 / 12.0},
		{[]roachpb.StoreID{testStoreUSa15, testStoreUSa15Dupe, testStoreUSb, testStoreEurope}, 2.0 / 3.0},
		{[]roachpb.StoreID{testStoreUSa15, testStoreUSa1, testStoreUSb, testStoreEurope}, 17.0 / 24.0},
		{[]roachpb.StoreID{testStoreUSa15, testStoreUSa15Dupe, testStoreUSa1, testStoreUSb, testStoreEurope}, 3.0 / 5.0},
	}

	// Ensure that rangeDiversityScore and diversityRebalanceFromScore return
	// the same results for the same configurations, enabling their results
	// to be directly compared with each other. The same is not true for
	// diversityAllocateScore and diversityRemovalScore as of their initial
	// creation or else we would test them here as well.
	for _, tc := range testCases {
		existingLocalities := make(map[roachpb.NodeID]roachpb.Locality)
		for _, storeID := range tc.stores {
			s := testStores[storeID]
			existingLocalities[s.Node.NodeID] = s.Node.Locality
		}
		rangeScore := rangeDiversityScore(existingLocalities)
		if a, e := rangeScore, tc.expected; a != e {
			t.Errorf("rangeDiversityScore(%v) got %f, want %f", existingLocalities, a, e)
		}
		for _, storeID := range tc.stores {
			s := testStores[storeID]
			fromNodeID := s.Node.NodeID
			s.Node.NodeID = 99
			rebalanceScore := diversityRebalanceFromScore(s, fromNodeID, existingLocalities)
			if a, e := rebalanceScore, tc.expected; a != e {
				t.Errorf("diversityRebalanceFromScore(%v, %d, %v) got %f, want %f",
					s, fromNodeID, existingLocalities, a, e)
			}
			if a, e := rebalanceScore, rangeScore; a != e {
				t.Errorf("diversityRebalanceFromScore(%v, %d, %v)=%f not equal to rangeDiversityScore(%v)=%f",
					s, fromNodeID, existingLocalities, a, existingLocalities, e)
			}
		}
	}
}

func TestBalanceScore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	options := scorerOptions{}
	storeList := StoreList{
		candidateRanges: stat{mean: 1000},
	}

	sEmpty := roachpb.StoreCapacity{
		Capacity:     1024 * 1024 * 1024,
		Available:    1024 * 1024 * 1024,
		LogicalBytes: 0,
	}
	sMean := roachpb.StoreCapacity{
		Capacity:     1024 * 1024 * 1024,
		Available:    512 * 1024 * 1024,
		LogicalBytes: 512 * 1024 * 1024,
		RangeCount:   1000,
	}
	sRangesOverfull := sMean
	sRangesOverfull.RangeCount = 1500
	sRangesUnderfull := sMean
	sRangesUnderfull.RangeCount = 500

	ri := RangeInfo{}

	testCases := []struct {
		sc       roachpb.StoreCapacity
		expected float64
	}{
		{sEmpty, 1},
		{sMean, 0},
		{sRangesOverfull, -1},
		{sRangesUnderfull, 1},
	}
	for i, tc := range testCases {
		if a, e := balanceScore(storeList, tc.sc, ri, options), tc.expected; a.totalScore() != e {
			t.Errorf("%d: balanceScore(storeList, %+v) got %s; want %.2f", i, tc.sc, a, e)
		}
	}
}

func TestRebalanceConvergesOnMean(t *testing.T) {
	defer leaktest.AfterTest(t)()

	storeList := StoreList{
		candidateRanges: stat{mean: 1000},
	}

	testCases := []struct {
		rangeCount    int32
		toConverges   bool
		fromConverges bool
	}{
		{0, true, false},
		{900, true, false},
		{900, true, false},
		{999, true, false},
		{1000, false, false},
		{1001, false, true},
		{2000, false, true},
		{900, true, false},
	}

	for i, tc := range testCases {
		sc := roachpb.StoreCapacity{
			RangeCount: tc.rangeCount,
		}
		if a, e := rebalanceToConvergesOnMean(storeList, sc), tc.toConverges; a != e {
			t.Errorf("%d: rebalanceToConvergesOnMean(storeList, %+v) got %t; want %t", i, sc, a, e)
		}
		if a, e := rebalanceFromConvergesOnMean(storeList, sc), tc.fromConverges; a != e {
			t.Errorf("%d: rebalanceFromConvergesOnMean(storeList, %+v) got %t; want %t", i, sc, a, e)
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

func storeWithSpace(id, availableCapacity int64, locality string) roachpb.StoreDescriptor {
	return roachpb.StoreDescriptor{
		StoreID: roachpb.StoreID(id),
		Node: roachpb.NodeDescriptor{
			NodeID: roachpb.NodeID(id),
			Locality: roachpb.Locality{
				Tiers: testStoreTierSetup(locality, "", "", ""),
			},
		},
		Capacity: roachpb.StoreCapacity{
			Available: availableCapacity,
			Capacity:  100,
		},
	}
}

const (
	locUS = "US"
	locEU = "EU"
	locIN = "IN"
)

var (
	testCopysetStoreList = StoreList{
		stores: []roachpb.StoreDescriptor{
			storeWithSpace(1, 40, locUS), storeWithSpace(2, 30, locUS), storeWithSpace(3, 35, locUS),
			storeWithSpace(4, 40, locUS), storeWithSpace(5, 42, locUS), storeWithSpace(6, 45, locUS),
			storeWithSpace(7, 14, locUS), storeWithSpace(8, 30, locUS), storeWithSpace(9, 25, locUS),
			storeWithSpace(10, 44, locUS), storeWithSpace(11, 30, locUS), storeWithSpace(12, 35, locUS),
			storeWithSpace(13, 41, locUS), storeWithSpace(14, 41, locUS), storeWithSpace(15, 29, locUS),
		},
	}

	testCopysetStores = testCopysetStoreList.toMap()

	testCopysetsScorerRF3 = newCopysetsScorer(
		&copysets.Copysets{
			Sets: map[copysets.CopysetID]copysets.Copyset{
				1: {Ids: slToMap([]roachpb.StoreID{1, 2, 3})},
				2: {Ids: slToMap([]roachpb.StoreID{4, 5, 6})},
				3: {Ids: slToMap([]roachpb.StoreID{7, 8, 9})},
				4: {Ids: slToMap([]roachpb.StoreID{10, 11, 12})},
			},
		},
		testCopysetStores,
	)

	testCopysetDiverseStoreList = StoreList{
		stores: []roachpb.StoreDescriptor{
			storeWithSpace(1, 40, locUS), storeWithSpace(2, 30, locUS), storeWithSpace(3, 35, locUS),
			storeWithSpace(4, 40, locUS), storeWithSpace(5, 42, locUS), storeWithSpace(6, 45, locUS),
			storeWithSpace(7, 14, locUS), storeWithSpace(8, 30, locUS), storeWithSpace(9, 25, locUS),
			storeWithSpace(10, 44, locUS), storeWithSpace(11, 30, locUS), storeWithSpace(12, 35, locEU),
			storeWithSpace(13, 41, locIN), storeWithSpace(14, 41, locUS), storeWithSpace(15, 29, locUS),
		},
	}
	testCopysetsScorerRF3Imbalanced = newCopysetsScorer(
		&copysets.Copysets{
			Sets: map[copysets.CopysetID]copysets.Copyset{
				1: {Ids: slToMap([]roachpb.StoreID{1, 2, 3})},
				2: {Ids: slToMap([]roachpb.StoreID{4, 5, 6, 7, 8})},
				3: {Ids: slToMap([]roachpb.StoreID{9, 10, 11, 12, 13})},
			},
		},
		testCopysetDiverseStoreList.toMap(),
	)

	testCopysetsScorerRF5 = newCopysetsScorer(
		&copysets.Copysets{
			Sets: map[copysets.CopysetID]copysets.Copyset{
				1: {Ids: slToMap([]roachpb.StoreID{1, 2, 3, 4, 5})},
				2: {Ids: slToMap([]roachpb.StoreID{6, 7, 8, 9, 10})},
				3: {Ids: slToMap([]roachpb.StoreID{11, 12, 13, 14, 15})},
			},
		},
		testCopysetStores,
	)

	testCopysetsScatterScorer = newCopysetsScorer(
		&copysets.Copysets{
			Sets: map[copysets.CopysetID]copysets.Copyset{
				1:  {Ids: slToMap([]roachpb.StoreID{1, 2, 3})},
				2:  {Ids: slToMap([]roachpb.StoreID{4, 5, 6})},
				3:  {Ids: slToMap([]roachpb.StoreID{7, 8, 9})},
				4:  {Ids: slToMap([]roachpb.StoreID{10, 11, 12})},
				5:  {Ids: slToMap([]roachpb.StoreID{1, 4, 7})},
				6:  {Ids: slToMap([]roachpb.StoreID{10, 2, 5})},
				7:  {Ids: slToMap([]roachpb.StoreID{8, 11, 3})},
				8:  {Ids: slToMap([]roachpb.StoreID{1, 2, 5})},
				9:  {Ids: slToMap([]roachpb.StoreID{1, 8, 9})},
				11: {Ids: slToMap([]roachpb.StoreID{1, 10, 9})},
			},
		},
		testCopysetStores,
	)
)

func TestCopysetRemovalScore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name            string
		copysetsEnabled bool
		stores          []roachpb.StoreID
		scorer          *copysetsScorer
		// The last element of expectedOrder is preferred while removing a replica.
		expectedOrder []roachpb.StoreID
	}{
		{
			name:            "range within copyset",
			copysetsEnabled: true,
			scorer:          testCopysetsScorerRF3,
			stores:          []roachpb.StoreID{1, 2, 3},
			expectedOrder:   []roachpb.StoreID{1, 2, 3},
		},
		{
			name:            "remove non copyset",
			copysetsEnabled: true,
			scorer:          testCopysetsScorerRF3,
			stores:          []roachpb.StoreID{10, 12, 11, 1},
			expectedOrder:   []roachpb.StoreID{10, 11, 12, 1},
		},
		{
			name:            "remove non copyset with disabled",
			copysetsEnabled: false,
			scorer:          testCopysetsScorerRF3,
			stores:          []roachpb.StoreID{10, 12, 11, 1},
			expectedOrder:   []roachpb.StoreID{1, 10, 11, 12},
		},
		{
			name:            "range not in copyset 4",
			copysetsEnabled: true,
			scorer:          testCopysetsScorerRF3,
			stores:          []roachpb.StoreID{10, 11, 3},
			expectedOrder:   []roachpb.StoreID{10, 11, 3},
		},
		{
			name:            "range not in copyset 4 with disabled",
			copysetsEnabled: false,
			scorer:          testCopysetsScorerRF3,
			stores:          []roachpb.StoreID{10, 11, 3},
			expectedOrder:   []roachpb.StoreID{3, 10, 11},
		},
		{
			name:            "retain high idle score",
			copysetsEnabled: true,
			scorer:          testCopysetsScorerRF3,
			stores:          []roachpb.StoreID{8, 7, 10},
			expectedOrder:   []roachpb.StoreID{10, 7, 8},
		},
		{
			name:            "retain high idle score disabled",
			copysetsEnabled: false,
			scorer:          testCopysetsScorerRF3,
			stores:          []roachpb.StoreID{8, 7, 10},
			expectedOrder:   []roachpb.StoreID{7, 8, 10},
		},
		{
			name:            "range not in copyset 2",
			copysetsEnabled: true,
			scorer:          testCopysetsScorerRF3,
			stores:          []roachpb.StoreID{6, 5, 1},
			expectedOrder:   []roachpb.StoreID{5, 6, 1},
		},
		{
			name:            "range not in copyset 2 disabled",
			copysetsEnabled: false,
			scorer:          testCopysetsScorerRF3,
			stores:          []roachpb.StoreID{6, 5, 1},
			expectedOrder:   []roachpb.StoreID{1, 5, 6},
		},
		{
			name:            "scatter range within copyset 1",
			copysetsEnabled: true,
			scorer:          testCopysetsScatterScorer,
			stores:          []roachpb.StoreID{1, 2, 3},
			expectedOrder:   []roachpb.StoreID{1, 2, 3},
		},
		{
			name:            "scatter remove non copyset",
			copysetsEnabled: true,
			scorer:          testCopysetsScatterScorer,
			stores:          []roachpb.StoreID{10, 2, 5, 6},
			expectedOrder:   []roachpb.StoreID{5, 2, 10, 6},
		},
		{
			name:            "scatter remove non copyset 2",
			copysetsEnabled: true,
			scorer:          testCopysetsScatterScorer,
			stores:          []roachpb.StoreID{1, 10, 12, 9},
			expectedOrder:   []roachpb.StoreID{1, 10, 9, 12},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			options := scorerOptions{
				copysetsEnabled:                      tc.copysetsEnabled,
				copysetsIdleScoreDifferenceThreshold: testCopysetsIdleScoreDifferenceThreshold,
			}

			var scores storeScores
			rangeInfo := RangeInfo{
				Desc: &roachpb.RangeDescriptor{},
			}
			for _, storeID := range tc.stores {
				rangeInfo.Desc.Replicas = append(rangeInfo.Desc.Replicas, roachpb.ReplicaDescriptor{
					NodeID:  roachpb.NodeID(storeID),
					StoreID: storeID,
				})
			}
			for _, storeID := range tc.stores {
				s := testCopysetStores[storeID]
				var score storeScore
				score.score = copysetRemovalScore(ctx, tc.scorer, rangeInfo, testCopysetStores[storeID], options)
				score.storeID = s.StoreID
				scores = append(scores, score)
			}
			sort.Sort(sort.Reverse(scores))
			for i := 0; i < len(scores); {
				if scores[i].storeID != tc.expectedOrder[i] {
					t.Fatalf("expected the result store order to be %v, but got %v", tc.expectedOrder, scores)
				}
				i++
			}
		})
	}
}

func TestShouldRebalanceCopysets(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// In this test StoreID is same as NodeID for a node.
	// Given existingNodeIDs, a replacement is found for s where s belongs to
	// existingNodeIDs.
	// If s is not given, then a replacement is found for any of the
	// existingNodeIDs.
	testCases := []struct {
		name            string
		s               roachpb.StoreDescriptor
		sl              StoreList
		copysetsEnabled bool
		existingNodeIDs []roachpb.NodeID
		scorer          *copysetsScorer
		expected        bool
		target          roachpb.StoreID
	}{
		{
			name:            "range within copyset",
			s:               testCopysetStores[1],
			sl:              testCopysetStoreList,
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{1, 2, 3},
			expected:        false,
		},
		{
			name:            "balance copyset",
			s:               testCopysetStores[1],
			sl:              testCopysetStoreList,
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{1, 4, 5},
			expected:        true,
			target:          6,
		},
		{
			name:            "copyset disabled",
			s:               testCopysetStores[1],
			sl:              testCopysetStoreList,
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: false,
			existingNodeIDs: []roachpb.NodeID{1, 4, 5},
			expected:        false,
		},
		// A range will migrate from copy set 3 to copy set 2 since 2 has
		// a significantly higher idle score.
		{
			name:            "migrate copyset idle score",
			s:               testCopysetStores[7],
			sl:              testCopysetStoreList,
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{7, 8, 9},
			expected:        true,
			target:          4,
		},
		{
			name:            "migrate copyset idle score step 2",
			s:               testCopysetStores[8],
			sl:              testCopysetStoreList,
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{4, 8, 9},
			expected:        true,
			target:          5,
		},
		{
			name:            "migrate copyset idle score step 3",
			s:               testCopysetStores[9],
			sl:              testCopysetStoreList,
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{4, 5, 9},
			expected:        true,
			target:          6,
		},
		{
			name:            "migrate copyset idle score complete",
			sl:              testCopysetStoreList,
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{4, 5, 6},
			expected:        false,
		},
		// A range will migrate from 9, 10, 11 to 9, 12, 13 because
		// 9, 10, 11, 12, 13 all belong to the same copyset and 9, 10, 11
		// have the same locality.
		// This test verifies locality fault tolerance within a copyset.
		{
			name:            "migrate copyset to increase diversity",
			s:               testCopysetStores[10],
			sl:              testCopysetDiverseStoreList,
			scorer:          testCopysetsScorerRF3Imbalanced,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{9, 10, 11},
			expected:        true,
			target:          12,
		},
		{
			name:            "migrate copyset to increase diversity step 2",
			s:               testCopysetStores[11],
			sl:              testCopysetDiverseStoreList,
			scorer:          testCopysetsScorerRF3Imbalanced,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{9, 12, 11},
			expected:        true,
			target:          13,
		},
		{
			name:            "migrate copyset to increase diversity stabilize",
			sl:              testCopysetDiverseStoreList,
			scorer:          testCopysetsScorerRF3Imbalanced,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{9, 12, 13},
			expected:        false,
		},
		// A range will migrate from copy set 2 to copy set 1 of replication factor
		// 5 since 1 has a significantly higher idle score.
		{
			name:            "migrate rf5 copyset idle score",
			s:               testCopysetStores[7],
			sl:              testCopysetStoreList,
			scorer:          testCopysetsScorerRF5,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{6, 7, 8, 9, 10},
			expected:        true,
			target:          1,
		},
		{
			name:            "migrate rf5 copyset idle score step 2",
			s:               testCopysetStores[6],
			sl:              testCopysetStoreList,
			scorer:          testCopysetsScorerRF5,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{6, 1, 8, 9, 10},
			expected:        true,
			target:          2,
		},
		{
			name:            "migrate rf5 copyset idle score step 3",
			s:               testCopysetStores[9],
			sl:              testCopysetStoreList,
			scorer:          testCopysetsScorerRF5,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{2, 1, 8, 9, 10},
			expected:        true,
			target:          3,
		},
		{
			name:            "migrate rf5 copyset idle score step 4",
			s:               testCopysetStores[8],
			sl:              testCopysetStoreList,
			scorer:          testCopysetsScorerRF5,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{2, 1, 8, 3, 10},
			expected:        true,
			target:          4,
		},
		{
			name:            "migrate rf5 copyset idle score step 5",
			s:               testCopysetStores[10],
			sl:              testCopysetStoreList,
			scorer:          testCopysetsScorerRF5,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{2, 1, 4, 3, 10},
			expected:        true,
			target:          5,
		},
		{
			name:            "migrate rf5 copyset idle score complete",
			sl:              testCopysetStoreList,
			scorer:          testCopysetsScorerRF5,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{2, 1, 4, 3, 5},
			expected:        false,
		},
		{
			name:            "scatter range within copyset",
			s:               testCopysetStores[1],
			sl:              testCopysetStoreList,
			scorer:          testCopysetsScatterScorer,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{1, 8, 9},
			expected:        false,
		},
		{
			name:            "scatter range within copyset 2",
			s:               testCopysetStores[1],
			sl:              testCopysetStoreList,
			scorer:          testCopysetsScatterScorer,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{1, 2, 3},
			expected:        false,
		},
		// A range will migrate from copy set 3 to copy set 9 since copy set 9
		// has a significantly higher idle score.
		{
			name:            "scatter migrate copyset idle score",
			s:               testCopysetStores[7],
			sl:              testCopysetStoreList,
			scorer:          testCopysetsScatterScorer,
			copysetsEnabled: true,
			existingNodeIDs: []roachpb.NodeID{7, 8, 9},
			expected:        true,
			target:          1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			removeStore := func(sl StoreList, nodeID roachpb.NodeID) StoreList {
				for i, s := range sl.stores {
					if s.Node.NodeID == nodeID {
						return makeStoreList(append(sl.stores[:i], sl.stores[i+1:]...))
					}
				}
				return sl
			}
			filteredSL := tc.sl
			filteredSL.stores = append([]roachpb.StoreDescriptor(nil), filteredSL.stores...)
			existingNodeLocalities := make(map[roachpb.NodeID]roachpb.Locality)
			rangeInfo := RangeInfo{
				Desc: &roachpb.RangeDescriptor{},
			}
			storeMap := tc.sl.toMap()
			for _, nodeID := range tc.existingNodeIDs {
				storeID := roachpb.StoreID(nodeID)
				rangeInfo.Desc.Replicas = append(rangeInfo.Desc.Replicas, roachpb.ReplicaDescriptor{
					NodeID:  nodeID,
					StoreID: storeID,
				})
				existingNodeLocalities[nodeID] = storeMap[storeID].Node.Locality
				// For the sake of testing, remove all other existing stores from the
				// store list to only test whether we want to remove the replica on tc.s.
				if tc.s.StoreID != 0 {
					if nodeID != tc.s.Node.NodeID {
						filteredSL = removeStore(filteredSL, nodeID)
					}
				}
			}

			options := scorerOptions{
				copysetsEnabled:                      tc.copysetsEnabled,
				copysetsIdleScoreDifferenceThreshold: testCopysetsIdleScoreDifferenceThreshold,
			}
			targets := rebalanceCandidates(
				context.Background(),
				filteredSL,
				analyzedConstraints{},
				rangeInfo,
				existingNodeLocalities,
				func(nodeID roachpb.NodeID) string {
					return ""
				},
				options,
				tc.scorer,
			)
			if a.Equal(tc.expected, len(targets) > 0) {
				if tc.expected {
					a.Equal(tc.target, targets[0].candidates[0].store.StoreID)
				}
			}
		})
	}
}

func TestFractionDiskFree(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		store                    roachpb.StoreDescriptor
		expectedFractionDiskFree float64
	}{
		{
			store: roachpb.StoreDescriptor{
				Capacity: roachpb.StoreCapacity{
					Available: 31,
					Capacity:  100,
				},
			},
			expectedFractionDiskFree: 0.31,
		},
		{
			store: roachpb.StoreDescriptor{
				Capacity: roachpb.StoreCapacity{
					Available: 40,
					Capacity:  80,
				},
			},
			expectedFractionDiskFree: 0.5,
		},
		{
			store: roachpb.StoreDescriptor{
				Capacity: roachpb.StoreCapacity{
					Available: 0,
					Capacity:  0,
				},
			},
			expectedFractionDiskFree: 0,
		},
	}

	a := assert.New(t)
	for _, tc := range testCases {
		a.InDelta(tc.expectedFractionDiskFree, fractionDiskFree(tc.store), 0.001*tc.expectedFractionDiskFree)
	}
}

func TestCopysetScoreEquivalence(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name            string
		stores          []roachpb.StoreID
		addedStore      roachpb.StoreID
		removedStore    roachpb.StoreID
		scorer          *copysetsScorer
		copysetsEnabled bool
		expected        float64
	}{
		{
			name:            "nil stores",
			stores:          nil,
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: false,
			expected:        0.0,
		},
		{
			name:            "no stores",
			stores:          []roachpb.StoreID{},
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: false,
			expected:        0.0,
		},
		{
			name:            "unknown store",
			stores:          []roachpb.StoreID{1, 2, 100},
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: false,
			expected:        0.0,
		},
		{
			name:            "copy sets disabled",
			stores:          []roachpb.StoreID{1, 2, 3},
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: false,
			expected:        0.0,
		},
		{
			name:            "copy set 1 add store",
			stores:          []roachpb.StoreID{1, 2},
			addedStore:      3,
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: true,
			expected:        0.3488,
		},
		{
			name:            "copy set 1 add poor store",
			stores:          []roachpb.StoreID{1, 2},
			addedStore:      4,
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: true,
			expected:        0.3333,
		},
		{
			name:            "copy set 1",
			stores:          []roachpb.StoreID{1, 2, 3},
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: true,
			expected:        0.3488,
		},
		{
			name:            "copy set 1 remove store",
			stores:          []roachpb.StoreID{1, 2, 3, 8},
			removedStore:    8,
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: true,
			expected:        0.3488,
		},
		{
			name:            "copy set 3",
			stores:          []roachpb.StoreID{7, 8, 9},
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: true,
			expected:        0.2,
		},
		{
			name:            "copy set migration",
			stores:          []roachpb.StoreID{7, 8, 9},
			addedStore:      2,
			removedStore:    7,
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: true,
			expected:        0.2031,
		},
		{
			// Copy set 1 has a high idle score and copy set 3 has a low idle score
			name:            "copy set migrated",
			stores:          []roachpb.StoreID{2, 8, 9},
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: true,
			expected:        0.2031,
		},
		{
			name:            "copy set migration 2",
			stores:          []roachpb.StoreID{7, 8, 9},
			addedStore:      4,
			removedStore:    7,
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: true,
			expected:        0.2341,
		},
		{
			// Copy set 2 has a high idle score and copy set 3 has a low idle score
			name:            "copy set migrated 2",
			stores:          []roachpb.StoreID{4, 8, 9},
			scorer:          testCopysetsScorerRF3,
			copysetsEnabled: true,
			expected:        0.2341,
		},
		{
			name:            "scatter copy set 1 add store",
			stores:          []roachpb.StoreID{1, 2},
			addedStore:      3,
			scorer:          testCopysetsScatterScorer,
			copysetsEnabled: true,
			expected:        0.3488,
		},
		{
			name:            "scatter copy set migration",
			stores:          []roachpb.StoreID{7, 8, 9},
			scorer:          testCopysetsScatterScorer,
			copysetsEnabled: true,
			expected:        0.2837,
		},
		{
			name:            "scatter copy set migration 2",
			stores:          []roachpb.StoreID{7, 8, 9},
			scorer:          testCopysetsScatterScorer,
			removedStore:    7,
			addedStore:      1,
			copysetsEnabled: true,
			expected:        0.3333,
		},
		{
			name:            "scatter copy set migrated",
			stores:          []roachpb.StoreID{1, 8, 9},
			scorer:          testCopysetsScatterScorer,
			copysetsEnabled: true,
			expected:        0.3333,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			ctx := context.Background()
			options := scorerOptions{
				copysetsEnabled:                      tc.copysetsEnabled,
				copysetsIdleScoreDifferenceThreshold: testCopysetsIdleScoreDifferenceThreshold,
			}
			rangeInfo := RangeInfo{
				Desc: &roachpb.RangeDescriptor{},
			}
			for _, storeID := range tc.stores {
				rangeInfo.Desc.Replicas = append(rangeInfo.Desc.Replicas, roachpb.ReplicaDescriptor{
					NodeID:  roachpb.NodeID(storeID),
					StoreID: storeID,
				})
			}
			var addedStore, removedStore *roachpb.StoreDescriptor
			if tc.addedStore != 0 {
				store := testCopysetStores[tc.addedStore]
				addedStore = &store
			}
			if tc.removedStore != 0 {
				store := testCopysetStores[tc.removedStore]
				removedStore = &store
			}
			rangeScore := copysetRebalanceScore(ctx, tc.scorer, rangeInfo, addedStore, removedStore, options)
			a.InDelta(tc.expected, rangeScore, tc.expected*0.001)
		})
	}
}

func TestCopysetScorerHomogeneityScore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name                     string
		stores                   []roachpb.StoreID
		scorer                   *copysetsScorer
		expectedHomogeneityScore float64
	}{
		{
			name:                     "empty store list",
			stores:                   []roachpb.StoreID{},
			scorer:                   testCopysetsScorerRF3,
			expectedHomogeneityScore: 0,
		},
		{
			name:                     "store list with one store only",
			stores:                   []roachpb.StoreID{1},
			scorer:                   testCopysetsScorerRF3,
			expectedHomogeneityScore: 1,
		},
		{
			name:                     "same copyset 1",
			stores:                   []roachpb.StoreID{1, 2, 3},
			scorer:                   testCopysetsScorerRF3,
			expectedHomogeneityScore: 1,
		},
		{
			name:                     "same copyset 2",
			stores:                   []roachpb.StoreID{4, 5, 6},
			scorer:                   testCopysetsScorerRF3,
			expectedHomogeneityScore: 1,
		},
		{
			name:                     "different copysets",
			stores:                   []roachpb.StoreID{2, 10, 5},
			scorer:                   testCopysetsScorerRF3,
			expectedHomogeneityScore: 0,
		},
		{
			name:                     "single different copyset",
			stores:                   []roachpb.StoreID{1, 2, 9},
			scorer:                   testCopysetsScorerRF3,
			expectedHomogeneityScore: 0.33,
		},
		{
			name:                     "single different large copyset",
			stores:                   []roachpb.StoreID{1, 2, 3, 9},
			scorer:                   testCopysetsScorerRF3,
			expectedHomogeneityScore: 0.5,
		},
		{
			name:                     "unknown store",
			stores:                   []roachpb.StoreID{1, 2, 100},
			scorer:                   testCopysetsScorerRF3,
			expectedHomogeneityScore: 0,
		},
		{
			name:                     "scatter same copyset",
			stores:                   []roachpb.StoreID{2, 10, 5},
			scorer:                   testCopysetsScatterScorer,
			expectedHomogeneityScore: 1,
		},
		{
			name:                     "scatter two copysets",
			stores:                   []roachpb.StoreID{2, 3, 5, 6},
			scorer:                   testCopysetsScatterScorer,
			expectedHomogeneityScore: 0.33,
		},
		{
			name:                     "scatter two copysets 2",
			stores:                   []roachpb.StoreID{1, 10, 9, 5},
			scorer:                   testCopysetsScatterScorer,
			expectedHomogeneityScore: 0.5,
		},
		{
			name:                     "scatter unknown store",
			stores:                   []roachpb.StoreID{1, 2, 100},
			scorer:                   testCopysetsScatterScorer,
			expectedHomogeneityScore: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			ctx := context.Background()
			a.InDelta(tc.expectedHomogeneityScore, tc.scorer.homogeneityScore(ctx, tc.stores), 0.05*tc.expectedHomogeneityScore)
		})
	}
}

func TestCopysetScorerIdleScore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name              string
		store             roachpb.StoreID
		scorer            *copysetsScorer
		expectedIdleScore float64
	}{
		{
			name:              "store 1",
			store:             1,
			scorer:            testCopysetsScorerRF3,
			expectedIdleScore: 0.3,
		},
		{
			name:              "store 2",
			store:             2,
			scorer:            testCopysetsScorerRF3,
			expectedIdleScore: 0.3,
		},
		{
			name:              "store 3",
			store:             3,
			scorer:            testCopysetsScorerRF3,
			expectedIdleScore: 0.3,
		},
		{
			name:              "store 6",
			store:             6,
			scorer:            testCopysetsScorerRF3,
			expectedIdleScore: 0.4,
		},
		{
			name:              "store 8",
			store:             8,
			scorer:            testCopysetsScorerRF3,
			expectedIdleScore: 0.14,
		},
		{
			name:              "unknown store",
			store:             100,
			scorer:            testCopysetsScorerRF3,
			expectedIdleScore: 0,
		},
		// With scatter idle score is the idle score of the best copyset a store
		// belongs to
		{
			name:              "scatter store 1",
			store:             1,
			scorer:            testCopysetsScatterScorer,
			expectedIdleScore: 0.3,
		},
		{
			name:              "scatter store 8",
			store:             8,
			scorer:            testCopysetsScatterScorer,
			expectedIdleScore: 0.3,
		},
		{
			name:              "scatter unknown store",
			store:             100,
			scorer:            testCopysetsScatterScorer,
			expectedIdleScore: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			ctx := context.Background()
			a.InDelta(tc.expectedIdleScore, tc.scorer.idleScore(ctx, tc.store), 0.05*tc.expectedIdleScore)
		})
	}
}

func TestCopysetScore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	a := assert.New(t)

	testCases := []struct {
		homogeneityScore float64
		idleScore        float64
		expectedScore    float64
	}{
		{0, 0, 0},
		{0.33, 0.4, 0.395},
		{0.33, 0.57, 0.553},
		{1, 0.57, 0.6},
		{1, 1, 1},
	}
	options := scorerOptions{
		copysetsEnabled:                      true,
		copysetsIdleScoreDifferenceThreshold: testCopysetsIdleScoreDifferenceThreshold,
	}

	for _, tc := range testCases {
		a.InDelta(
			tc.expectedScore,
			copysetScore(
				options,
				tc.homogeneityScore,
				tc.idleScore,
			),
			tc.expectedScore*0.001,
		)
	}
}
