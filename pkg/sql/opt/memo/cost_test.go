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
		left, right float64
		expected    bool
	}{
		{0.0, 1.0, true},
		{0.0, 1e-20, true},
		{0.0, 0.0, false},
		{1.0, 0.0, false},
		{1e-20, 1.0000000000001e-20, false},
		{1e-20, 1.000001e-20, true},
		{1, 1.00000000000001, false},
		{1, 1.00000001, true},
		{1000, 1000.00000000001, false},
		{1000, 1000.00001, true},
	}
	flagsTestCases := []struct {
		left, right  memo.CostFlags
		overrideCost bool
		expected     bool
	}{
		{memo.CostFlags{FullScanPenalty: false}, memo.CostFlags{FullScanPenalty: false}, false, false},
		{memo.CostFlags{FullScanPenalty: false}, memo.CostFlags{FullScanPenalty: true}, true, true},
		{memo.CostFlags{FullScanPenalty: true}, memo.CostFlags{FullScanPenalty: false}, true, false},
		{memo.CostFlags{FullScanPenalty: true}, memo.CostFlags{FullScanPenalty: true}, false, false},
	}
	for _, tc := range testCases {
		left := memo.Cost{Cost: tc.left}
		right := memo.Cost{Cost: tc.right}
		if left.Less(right) != tc.expected {
			t.Errorf("expected %v.Less(%v) to be %v", left, right, tc.expected)
		}

		for _, ftc := range flagsTestCases {
			left = memo.Cost{Cost: tc.left, Flags: ftc.left}
			right = memo.Cost{Cost: tc.right, Flags: ftc.right}
			if ftc.overrideCost {
				if left.Less(right) != ftc.expected {
					t.Errorf("expected %v.Less(%v) to be %v", left, right, ftc.expected)
				}
			} else {
				if left.Less(right) != tc.expected {
					t.Errorf("expected %v.Less(%v) to be %v", left, right, tc.expected)
				}
			}
		}

	}
}
