// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	for _, tc := range testCases {
		if tc.left.Less(tc.right) != tc.expected {
			t.Errorf("expected %v.Less(%v) to be %v", tc.left, tc.right, tc.expected)
		}
	}
}

func TestCostSub(t *testing.T) {
	testSub := func(left, right memo.Cost, expected memo.Cost) {
		actual := left.Sub(right)
		if actual != expected {
			t.Errorf("expected %v.Sub(%v) to be %v, got %v", left, right, expected, actual)
		}
	}

	testSub(memo.Cost(10.0), memo.Cost(3.0), memo.Cost(7.0))
	testSub(memo.Cost(3.0), memo.Cost(10.0), memo.Cost(-7.0))
	testSub(memo.Cost(10.0), memo.Cost(10.0), memo.Cost(0.0))
}
