// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo

import "testing"

type testAux struct {
	fullScanCount uint8
}

func TestCostLess(t *testing.T) {
	testCases := []struct {
		left, right Cost
		expected    bool
	}{
		{Cost{C: 0.0}, Cost{C: 1.0}, true},
		{Cost{C: 0.0}, Cost{C: 1e-20}, true},
		{Cost{C: 0.0}, Cost{C: 0.0}, false},
		{Cost{C: 1.0}, Cost{C: 0.0}, false},
		{Cost{C: 1e-20}, Cost{C: 1.0000000000001e-20}, false},
		{Cost{C: 1e-20}, Cost{C: 1.000001e-20}, true},
		{Cost{C: 1}, Cost{C: 1.00000000000001}, false},
		{Cost{C: 1}, Cost{C: 1.00000001}, true},
		{Cost{C: 1000}, Cost{C: 1000.00000000001}, false},
		{Cost{C: 1000}, Cost{C: 1000.00001}, true},
		{Cost{C: 1.0, Flags: FullScanPenalty}, Cost{C: 1.0}, false},
		{Cost{C: 1.0}, Cost{C: 1.0, Flags: HugeCostPenalty}, true},
		{Cost{C: 1.0, Flags: FullScanPenalty | HugeCostPenalty}, Cost{C: 1.0}, false},
		{Cost{C: 1.0, Flags: FullScanPenalty}, Cost{C: 1.0, Flags: HugeCostPenalty}, true},
		{MaxCost, Cost{C: 1.0}, false},
		{Cost{C: 0.0}, MaxCost, true},
		{MaxCost, MaxCost, false},
		{MaxCost, Cost{C: 1.0, Flags: FullScanPenalty}, false},
		{Cost{C: 1.0, Flags: HugeCostPenalty}, MaxCost, true},
		{Cost{C: 2.0}, Cost{C: 1.0, Flags: UnboundedCardinality}, true},
		{Cost{C: 1.0, Flags: UnboundedCardinality}, Cost{C: 2.0}, false},
		// Auxiliary information should not affect the comparison.
		{Cost{C: 1.0, aux: testAux{0}}, Cost{C: 1.0, aux: testAux{1}}, false},
	}
	for _, tc := range testCases {
		if tc.left.Less(tc.right) != tc.expected {
			t.Errorf("expected %v.Less(%v) to be %v", tc.left, tc.right, tc.expected)
		}
	}
}

func TestCostAdd(t *testing.T) {
	testCases := []struct {
		left, right, expected Cost
	}{
		{Cost{C: 1.0}, Cost{C: 2.0}, Cost{C: 3.0}},
		{Cost{C: 0.0}, Cost{C: 0.0}, Cost{C: 0.0}},
		{Cost{C: -1.0}, Cost{C: 1.0}, Cost{C: 0.0}},
		{Cost{C: 1.5}, Cost{C: 2.5}, Cost{C: 4.0}},
		{Cost{C: 1.0, Flags: FullScanPenalty}, Cost{C: 2.0}, Cost{C: 3.0, Flags: FullScanPenalty}},
		{Cost{C: 1.0}, Cost{C: 2.0, Flags: HugeCostPenalty}, Cost{C: 3.0, Flags: HugeCostPenalty}},
		{Cost{C: 1.0, Flags: UnboundedCardinality}, Cost{C: 2.0, Flags: HugeCostPenalty}, Cost{C: 3.0, Flags: HugeCostPenalty | UnboundedCardinality}},
		{Cost{C: 1.0, aux: testAux{1}}, Cost{C: 1.0, aux: testAux{2}}, Cost{C: 2.0, aux: testAux{3}}},
		{Cost{C: 1.0, aux: testAux{200}}, Cost{C: 1.0, aux: testAux{100}}, Cost{C: 2.0, aux: testAux{255}}},
	}
	for _, tc := range testCases {
		tc.left.Add(tc.right)
		if tc.left != tc.expected {
			t.Errorf("expected %v.Add(%v) to be %v, got %v", tc.left, tc.right, tc.expected, tc.left)
		}
	}
}
