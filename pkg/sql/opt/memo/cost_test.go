// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo

import "testing"

type testAux struct {
	fullScanCount        uint8
	unboundedCardinality bool
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
		{Cost{C: 1.0, Penalties: FullScanPenalty}, Cost{C: 1.0}, false},
		{Cost{C: 1.0}, Cost{C: 1.0, Penalties: HugeCostPenalty}, true},
		{Cost{C: 1.0, Penalties: FullScanPenalty | HugeCostPenalty}, Cost{C: 1.0}, false},
		{Cost{C: 1.0, Penalties: FullScanPenalty}, Cost{C: 1.0, Penalties: HugeCostPenalty}, true},
		{MaxCost, Cost{C: 1.0}, false},
		{Cost{C: 0.0}, MaxCost, true},
		{MaxCost, MaxCost, false},
		{MaxCost, Cost{C: 1.0, Penalties: FullScanPenalty}, false},
		{Cost{C: 1.0, Penalties: HugeCostPenalty}, MaxCost, true},
		{Cost{C: 2.0}, Cost{C: 1.0, Penalties: UnboundedCardinalityPenalty}, true},
		{Cost{C: 1.0, Penalties: UnboundedCardinalityPenalty}, Cost{C: 2.0}, false},
		// Auxiliary information should not affect the comparison.
		{Cost{C: 1.0, aux: testAux{0, false}}, Cost{C: 1.0, aux: testAux{1, true}}, false},
		{Cost{C: 1.0, aux: testAux{1, true}}, Cost{C: 1.0, aux: testAux{0, false}}, false},
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
		{Cost{C: 1.0, Penalties: FullScanPenalty}, Cost{C: 2.0}, Cost{C: 3.0, Penalties: FullScanPenalty}},
		{Cost{C: 1.0}, Cost{C: 2.0, Penalties: HugeCostPenalty}, Cost{C: 3.0, Penalties: HugeCostPenalty}},
		{Cost{C: 1.0, Penalties: UnboundedCardinalityPenalty}, Cost{C: 2.0, Penalties: HugeCostPenalty}, Cost{C: 3.0, Penalties: HugeCostPenalty | UnboundedCardinalityPenalty}},
		{Cost{C: 1.0, aux: testAux{1, false}}, Cost{C: 1.0, aux: testAux{2, true}}, Cost{C: 2.0, aux: testAux{3, true}}},
		{Cost{C: 1.0, aux: testAux{200, true}}, Cost{C: 1.0, aux: testAux{100, false}}, Cost{C: 2.0, aux: testAux{255, true}}},
	}
	for _, tc := range testCases {
		tc.left.Add(tc.right)
		if tc.left != tc.expected {
			t.Errorf("expected %v.Add(%v) to be %v, got %v", tc.left, tc.right, tc.expected, tc.left)
		}
	}
}

func TestCostSummary(t *testing.T) {
	testCases := []struct {
		c        Cost
		expected string
	}{
		{Cost{C: 1.0}, "1::0f"},
		{Cost{C: 1.23}, "1.23::0f"},
		{Cost{C: 1.23456}, "1.23456::0f"},
		{Cost{C: 1.23, Penalties: HugeCostPenalty}, "1.23:H:0f"},
		{Cost{C: 1.23, Penalties: FullScanPenalty}, "1.23:F:0f"},
		{Cost{C: 1.23, Penalties: UnboundedCardinalityPenalty}, "1.23:U:0f"},
		{Cost{C: 1.23, Penalties: HugeCostPenalty | FullScanPenalty | UnboundedCardinalityPenalty}, "1.23:HFU:0f"},
		{Cost{C: 1.23, Penalties: HugeCostPenalty | FullScanPenalty | UnboundedCardinalityPenalty}, "1.23:HFU:0f"},
		{Cost{C: 1.23, aux: testAux{5, false}}, "1.23::5f"},
		{Cost{C: 1.23, aux: testAux{0, true}}, "1.23::0fu"},
		{Cost{C: 1.23, aux: testAux{5, true}}, "1.23::5fu"},
		{Cost{C: 1.23, Penalties: HugeCostPenalty | FullScanPenalty, aux: testAux{5, true}}, "1.23:HF:5fu"},
	}
	for _, tc := range testCases {
		if r := tc.c.Summary(); r != tc.expected {
			t.Errorf("expected %q, got %q", tc.expected, r)
		}
	}
}
