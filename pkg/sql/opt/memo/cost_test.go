// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo

import "testing"

type testAux struct {
	fullScanCount      uint16
	unboundedReadCount uint16
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
		{Cost{C: 1.0, aux: testAux{0, 0}}, Cost{C: 1.0, aux: testAux{1, 1}}, false},
		{Cost{C: 1.0, aux: testAux{1, 1}}, Cost{C: 1.0, aux: testAux{0, 0}}, false},
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
		{Cost{C: 1.0, aux: testAux{1, 4}}, Cost{C: 1.0, aux: testAux{2, 5}}, Cost{C: 2.0, aux: testAux{3, 9}}},
		{Cost{C: 1.0, aux: testAux{65530, 65530}}, Cost{C: 1.0, aux: testAux{100, 100}}, Cost{C: 2.0, aux: testAux{65535, 65535}}},
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
		{Cost{C: 1.0}, "1::0f0u"},
		{Cost{C: 1.23}, "1.23::0f0u"},
		{Cost{C: 1.23456}, "1.23456::0f0u"},
		{Cost{C: 1.23, Penalties: HugeCostPenalty}, "1.23:H:0f0u"},
		{Cost{C: 1.23, Penalties: FullScanPenalty}, "1.23:F:0f0u"},
		{Cost{C: 1.23, Penalties: UnboundedCardinalityPenalty}, "1.23:U:0f0u"},
		{Cost{C: 1.23, Penalties: HugeCostPenalty | FullScanPenalty | UnboundedCardinalityPenalty}, "1.23:HFU:0f0u"},
		{Cost{C: 1.23, Penalties: HugeCostPenalty | FullScanPenalty | UnboundedCardinalityPenalty}, "1.23:HFU:0f0u"},
		{Cost{C: 1.23, aux: testAux{5, 0}}, "1.23::5f0u"},
		{Cost{C: 1.23, aux: testAux{0, 6}}, "1.23::0f6u"},
		{Cost{C: 1.23, aux: testAux{5, 10}}, "1.23::5f10u"},
		{Cost{C: 1.23, Penalties: HugeCostPenalty | FullScanPenalty, aux: testAux{5, 9}}, "1.23:HF:5f9u"},
	}
	for _, tc := range testCases {
		if r := tc.c.Summary(); r != tc.expected {
			t.Errorf("expected %q, got %q", tc.expected, r)
		}
	}
}
