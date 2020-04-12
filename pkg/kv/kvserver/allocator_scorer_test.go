// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/constraint"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"
)

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
				// We want to test here that everything works when the deversity score
				// is not the exact value but very close to it. Nextafter will give us
				// the closest number to the diversity score in the test case,
				// that isn't equal to it and is either above or below (at random).
				diversityScore: math.Nextafter(float64(score.diversity), rand.Float64()),
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
			buffer.WriteString(fmt.Sprintf("%d:%d", int(c.diversityScore), c.rangeCount))
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
			actual := scoreTuple{int(good.diversityScore + 0.5), good.rangeCount}
			if actual != tc.good {
				t.Errorf("expected:%v actual:%v", tc.good, actual)
			}
		})
		t.Run(fmt.Sprintf("bad-%s", formatter(cl)), func(t *testing.T) {
			bad := cl.selectBad(allocRand)
			if bad == nil {
				t.Fatalf("no bad candidate found")
			}
			actual := scoreTuple{int(bad.diversityScore + 0.5), bad.rangeCount}
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
			valid:          true,
			diversityScore: 1,
			rangeCount:     0,
		},
		{
			valid:          true,
			diversityScore: 0.9999999999999999,
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

	expectedResults := []int{0, 0, 2, 2, 4, 4, 6, 6, 8, 8, 8}

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
		constraints []zonepb.ConstraintsConjunction
		expected    map[roachpb.StoreID]bool
	}{
		{
			name: "required constraint",
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Value: "b", Type: zonepb.Constraint_REQUIRED},
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
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Key: "datacenter", Value: "us", Type: zonepb.Constraint_REQUIRED},
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
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Value: "b", Type: zonepb.Constraint_PROHIBITED},
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
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Key: "datacenter", Value: "us", Type: zonepb.Constraint_PROHIBITED},
					},
				},
			},
			expected: map[roachpb.StoreID]bool{
				testStoreEurope: true,
			},
		},
		{
			name: "positive constraints are ignored",
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Value: "a", Type: zonepb.Constraint_DEPRECATED_POSITIVE},
						{Value: "b", Type: zonepb.Constraint_DEPRECATED_POSITIVE},
						{Value: "c", Type: zonepb.Constraint_DEPRECATED_POSITIVE},
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
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Key: "datacenter", Value: "eur", Type: zonepb.Constraint_DEPRECATED_POSITIVE},
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
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Key: "datacenter", Value: "eur", Type: zonepb.Constraint_REQUIRED},
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
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Key: "datacenter", Value: "eur", Type: zonepb.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
				{
					Constraints: []zonepb.Constraint{
						{Value: "b", Type: zonepb.Constraint_REQUIRED},
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
		constraints       []zonepb.ConstraintsConjunction
		zoneNumReplicas   int32
		existing          []roachpb.StoreID
		expectedValid     map[roachpb.StoreID]bool
		expectedNecessary map[roachpb.StoreID]bool
	}{
		{
			name: "prohibited constraint",
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Value: "b", Type: zonepb.Constraint_PROHIBITED},
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
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Value: "b", Type: zonepb.Constraint_REQUIRED},
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
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Value: "b", Type: zonepb.Constraint_REQUIRED},
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
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Value: "a", Type: zonepb.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
				{
					Constraints: []zonepb.Constraint{
						{Value: "b", Type: zonepb.Constraint_REQUIRED},
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
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Value: "a", Type: zonepb.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
				{
					Constraints: []zonepb.Constraint{
						{Value: "b", Type: zonepb.Constraint_REQUIRED},
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
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Value: "a", Type: zonepb.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
				{
					Constraints: []zonepb.Constraint{
						{Value: "b", Type: zonepb.Constraint_REQUIRED},
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
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Value: "a", Type: zonepb.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
				{
					Constraints: []zonepb.Constraint{
						{Value: "b", Type: zonepb.Constraint_REQUIRED},
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
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Value: "a", Type: zonepb.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
				{
					Constraints: []zonepb.Constraint{
						{Value: "b", Type: zonepb.Constraint_REQUIRED},
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
			zone := &zonepb.ZoneConfig{
				Constraints: tc.constraints,
				NumReplicas: proto.Int32(tc.zoneNumReplicas),
			}
			analyzed := constraint.AnalyzeConstraints(
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
		constraints     []zonepb.ConstraintsConjunction
		zoneNumReplicas int32
		expected        map[roachpb.StoreID]expected
	}{
		{
			name: "prohibited constraint",
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Value: "b", Type: zonepb.Constraint_PROHIBITED},
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
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Value: "b", Type: zonepb.Constraint_REQUIRED},
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
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Value: "b", Type: zonepb.Constraint_REQUIRED},
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
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Value: "a", Type: zonepb.Constraint_REQUIRED},
					},
					NumReplicas: 1,
				},
				{
					Constraints: []zonepb.Constraint{
						{Value: "b", Type: zonepb.Constraint_REQUIRED},
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
			constraints: []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Value: "b", Type: zonepb.Constraint_REQUIRED},
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
			zone := &zonepb.ZoneConfig{
				Constraints: tc.constraints,
				NumReplicas: proto.Int32(tc.zoneNumReplicas),
			}
			analyzed := constraint.AnalyzeConstraints(
				context.Background(), getTestStoreDesc, existing, zone)
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
		var replicas []roachpb.ReplicaDescriptor
		for _, nodeID := range tc.existingNodeIDs {
			replicas = append(replicas, roachpb.ReplicaDescriptor{
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
			constraint.AnalyzedConstraints{},
			replicas,
			existingNodeLocalities,
			func(nodeID roachpb.NodeID) string {
				locality := localityForNodeID(tc.sl, nodeID)
				return locality.String()
			},
			options)
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
		if a, e := balanceScore(storeList, tc.sc, options), tc.expected; a.totalScore() != e {
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
