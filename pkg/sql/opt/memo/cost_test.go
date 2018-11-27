// Copyright 2018 The Cockroach Authors.
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
