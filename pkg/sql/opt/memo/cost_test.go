// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

func TestCostLess(t *testing.T) {
	testCases := []struct {
		left, right memo.Cost
		expected    bool
	}{
		{memo.Cost{C: 0.0}, memo.Cost{C: 1.0}, true},
		{memo.Cost{C: 0.0}, memo.Cost{C: 1e-20}, true},
		{memo.Cost{C: 0.0}, memo.Cost{C: 0.0}, false},
		{memo.Cost{C: 1.0}, memo.Cost{C: 0.0}, false},
		{memo.Cost{C: 1e-20}, memo.Cost{C: 1.0000000000001e-20}, false},
		{memo.Cost{C: 1e-20}, memo.Cost{C: 1.000001e-20}, true},
		{memo.Cost{C: 1}, memo.Cost{C: 1.00000000000001}, false},
		{memo.Cost{C: 1}, memo.Cost{C: 1.00000001}, true},
		{memo.Cost{C: 1000}, memo.Cost{C: 1000.00000000001}, false},
		{memo.Cost{C: 1000}, memo.Cost{C: 1000.00001}, true},
		{memo.Cost{C: 1.0, Flags: memo.CostFlags{FullScanPenalty: true}}, memo.Cost{C: 1.0}, false},
		{memo.Cost{C: 1.0}, memo.Cost{C: 1.0, Flags: memo.CostFlags{HugeCostPenalty: true}}, true},
		{memo.Cost{C: 1.0, Flags: memo.CostFlags{FullScanPenalty: true, HugeCostPenalty: true}}, memo.Cost{C: 1.0}, false},
		{memo.Cost{C: 1.0, Flags: memo.CostFlags{FullScanPenalty: true}}, memo.Cost{C: 1.0, Flags: memo.CostFlags{HugeCostPenalty: true}}, true},
		{memo.MaxCost, memo.Cost{C: 1.0}, false},
		{memo.Cost{C: 0.0}, memo.MaxCost, true},
		{memo.MaxCost, memo.MaxCost, false},
		{memo.MaxCost, memo.Cost{C: 1.0, Flags: memo.CostFlags{FullScanPenalty: true}}, false},
		{memo.Cost{C: 1.0, Flags: memo.CostFlags{HugeCostPenalty: true}}, memo.MaxCost, true},
		{memo.Cost{C: 2.0, Flags: memo.CostFlags{}}, memo.Cost{C: 1.0, Flags: memo.CostFlags{UnboundedCardinality: true}}, true},
		{memo.Cost{C: 1.0, Flags: memo.CostFlags{UnboundedCardinality: true}}, memo.Cost{C: 2.0, Flags: memo.CostFlags{}}, false},
	}
	for _, tc := range testCases {
		if tc.left.Less(tc.right) != tc.expected {
			t.Errorf("expected %v.Less(%v) to be %v", tc.left, tc.right, tc.expected)
		}
	}
}

func TestCostAdd(t *testing.T) {
	testCases := []struct {
		left, right, expected memo.Cost
	}{
		{memo.Cost{C: 1.0}, memo.Cost{C: 2.0}, memo.Cost{C: 3.0}},
		{memo.Cost{C: 0.0}, memo.Cost{C: 0.0}, memo.Cost{C: 0.0}},
		{memo.Cost{C: -1.0}, memo.Cost{C: 1.0}, memo.Cost{C: 0.0}},
		{memo.Cost{C: 1.5}, memo.Cost{C: 2.5}, memo.Cost{C: 4.0}},
		{memo.Cost{C: 1.0, Flags: memo.CostFlags{FullScanPenalty: true}}, memo.Cost{C: 2.0}, memo.Cost{C: 3.0, Flags: memo.CostFlags{FullScanPenalty: true}}},
		{memo.Cost{C: 1.0}, memo.Cost{C: 2.0, Flags: memo.CostFlags{HugeCostPenalty: true}}, memo.Cost{C: 3.0, Flags: memo.CostFlags{HugeCostPenalty: true}}},
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
		left, right memo.CostFlags
		expected    bool
	}{
		{memo.CostFlags{FullScanPenalty: false, HugeCostPenalty: false}, memo.CostFlags{FullScanPenalty: true, HugeCostPenalty: true}, true},
		{memo.CostFlags{FullScanPenalty: true, HugeCostPenalty: true}, memo.CostFlags{FullScanPenalty: false, HugeCostPenalty: false}, false},
		{memo.CostFlags{FullScanPenalty: true, HugeCostPenalty: true}, memo.CostFlags{FullScanPenalty: true, HugeCostPenalty: true}, false},
		{memo.CostFlags{FullScanPenalty: false}, memo.CostFlags{FullScanPenalty: true}, true},
		{memo.CostFlags{HugeCostPenalty: false}, memo.CostFlags{HugeCostPenalty: true}, true},
		{memo.CostFlags{UnboundedCardinality: false}, memo.CostFlags{UnboundedCardinality: true}, true},
		{memo.CostFlags{UnboundedCardinality: true}, memo.CostFlags{UnboundedCardinality: false}, false},
	}
	for _, tc := range testCases {
		if tc.left.Less(tc.right) != tc.expected {
			t.Errorf("expected %v.Less(%v) to be %v", tc.left, tc.right, tc.expected)
		}
	}
}

func TestCostFlagsAdd(t *testing.T) {
	testCases := []struct {
		left, right, expected memo.CostFlags
	}{
		{memo.CostFlags{FullScanPenalty: false, HugeCostPenalty: false}, memo.CostFlags{FullScanPenalty: true, HugeCostPenalty: true}, memo.CostFlags{FullScanPenalty: true, HugeCostPenalty: true}},
		{memo.CostFlags{FullScanPenalty: true, HugeCostPenalty: true}, memo.CostFlags{FullScanPenalty: false, HugeCostPenalty: false}, memo.CostFlags{FullScanPenalty: true, HugeCostPenalty: true}},
		{memo.CostFlags{FullScanPenalty: false}, memo.CostFlags{FullScanPenalty: true}, memo.CostFlags{FullScanPenalty: true}},
		{memo.CostFlags{HugeCostPenalty: false}, memo.CostFlags{HugeCostPenalty: true}, memo.CostFlags{HugeCostPenalty: true}},
		{memo.CostFlags{FullScanPenalty: true, HugeCostPenalty: false}, memo.CostFlags{FullScanPenalty: false, HugeCostPenalty: true}, memo.CostFlags{FullScanPenalty: true, HugeCostPenalty: true}},
	}
	for _, tc := range testCases {
		tc.left.Add(tc.right)
		if tc.left != tc.expected {
			t.Errorf("expected %v.Add(%v) to be %v, got %v", tc.left, tc.right, tc.expected, tc.left)
		}
	}
}
