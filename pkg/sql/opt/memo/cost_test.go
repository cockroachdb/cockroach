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
		{Cost{C: 1.0, Flags: CostFlags{FullScanPenalty: true}}, Cost{C: 1.0}, false},
		{Cost{C: 1.0}, Cost{C: 1.0, Flags: CostFlags{HugeCostPenalty: true}}, true},
		{Cost{C: 1.0, Flags: CostFlags{FullScanPenalty: true, HugeCostPenalty: true}}, Cost{C: 1.0}, false},
		{Cost{C: 1.0, Flags: CostFlags{FullScanPenalty: true}}, Cost{C: 1.0, Flags: CostFlags{HugeCostPenalty: true}}, true},
		{MaxCost, Cost{C: 1.0}, false},
		{Cost{C: 0.0}, MaxCost, true},
		{MaxCost, MaxCost, false},
		{MaxCost, Cost{C: 1.0, Flags: CostFlags{FullScanPenalty: true}}, false},
		{Cost{C: 1.0, Flags: CostFlags{HugeCostPenalty: true}}, MaxCost, true},
		{Cost{C: 2.0, Flags: CostFlags{}}, Cost{C: 1.0, Flags: CostFlags{UnboundedCardinality: true}}, true},
		{Cost{C: 1.0, Flags: CostFlags{UnboundedCardinality: true}}, Cost{C: 2.0, Flags: CostFlags{}}, false},
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
		{Cost{C: 1.0, Flags: CostFlags{FullScanPenalty: true}}, Cost{C: 2.0}, Cost{C: 3.0, Flags: CostFlags{FullScanPenalty: true}}},
		{Cost{C: 1.0}, Cost{C: 2.0, Flags: CostFlags{HugeCostPenalty: true}}, Cost{C: 3.0, Flags: CostFlags{HugeCostPenalty: true}}},
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

func TestCostFlagsLess(t *testing.T) {
	testCases := []struct {
		left, right CostFlags
		expected    bool
	}{
		{CostFlags{FullScanPenalty: false, HugeCostPenalty: false}, CostFlags{FullScanPenalty: true, HugeCostPenalty: true}, true},
		{CostFlags{FullScanPenalty: true, HugeCostPenalty: true}, CostFlags{FullScanPenalty: false, HugeCostPenalty: false}, false},
		{CostFlags{FullScanPenalty: true, HugeCostPenalty: true}, CostFlags{FullScanPenalty: true, HugeCostPenalty: true}, false},
		{CostFlags{FullScanPenalty: false}, CostFlags{FullScanPenalty: true}, true},
		{CostFlags{HugeCostPenalty: false}, CostFlags{HugeCostPenalty: true}, true},
		{CostFlags{UnboundedCardinality: false}, CostFlags{UnboundedCardinality: true}, true},
		{CostFlags{UnboundedCardinality: true}, CostFlags{UnboundedCardinality: false}, false},
	}
	for _, tc := range testCases {
		if tc.left.Less(tc.right) != tc.expected {
			t.Errorf("expected %v.Less(%v) to be %v", tc.left, tc.right, tc.expected)
		}
	}
}

func TestCostFlagsAdd(t *testing.T) {
	testCases := []struct {
		left, right, expected CostFlags
	}{
		{CostFlags{FullScanPenalty: false, HugeCostPenalty: false}, CostFlags{FullScanPenalty: true, HugeCostPenalty: true}, CostFlags{FullScanPenalty: true, HugeCostPenalty: true}},
		{CostFlags{FullScanPenalty: true, HugeCostPenalty: true}, CostFlags{FullScanPenalty: false, HugeCostPenalty: false}, CostFlags{FullScanPenalty: true, HugeCostPenalty: true}},
		{CostFlags{FullScanPenalty: false}, CostFlags{FullScanPenalty: true}, CostFlags{FullScanPenalty: true}},
		{CostFlags{HugeCostPenalty: false}, CostFlags{HugeCostPenalty: true}, CostFlags{HugeCostPenalty: true}},
		{CostFlags{FullScanPenalty: true, HugeCostPenalty: false}, CostFlags{FullScanPenalty: false, HugeCostPenalty: true}, CostFlags{FullScanPenalty: true, HugeCostPenalty: true}},
	}
	for _, tc := range testCases {
		tc.left.Add(tc.right)
		if tc.left != tc.expected {
			t.Errorf("expected %v.Add(%v) to be %v, got %v", tc.left, tc.right, tc.expected, tc.left)
		}
	}
}
