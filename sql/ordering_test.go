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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package sql

import (
	"runtime"
	"testing"

	"github.com/cockroachdb/cockroach/util/leaktest"
)

type desiredCase struct {
	line            int
	desired         columnOrdering
	expected        int
	expectedReverse int
}

type computeOrderCase struct {
	existing orderingInfo
	cases    []desiredCase
}

func defTestCase(expected, expectedReverse int, desired columnOrdering) desiredCase {
	// The line number is used to identify testcases in error messages.
	_, _, line, _ := runtime.Caller(1)
	return desiredCase{
		line:            line,
		desired:         desired,
		expected:        expected,
		expectedReverse: expectedReverse,
	}
}

func TestComputeOrderingMatch(t *testing.T) {
	defer leaktest.AfterTest(t)

	e := struct{}{}
	testSets := []computeOrderCase{
		{
			// No existing ordering
			existing: orderingInfo{
				exactMatchCols: nil,
				ordering:       nil,
			},
			cases: []desiredCase{
				defTestCase(0, 0, columnOrdering{{1, true}, {5, false}}),
			},
		},
		{
			// Ordering with no exact-match columns
			existing: orderingInfo{
				exactMatchCols: nil,
				ordering:       []columnOrderInfo{{1, true}, {2, false}},
			},
			cases: []desiredCase{
				defTestCase(1, 0, columnOrdering{{1, true}, {5, false}}),
				defTestCase(0, 1, columnOrdering{{1, false}, {5, false}, {2, false}}),
			},
		},
		{
			// Ordering with only exact-match columns
			existing: orderingInfo{
				exactMatchCols: map[int]struct{}{1: e, 2: e},
				ordering:       nil,
			},
			cases: []desiredCase{
				defTestCase(1, 1, columnOrdering{{2, true}, {5, false}, {1, false}}),
				defTestCase(0, 0, columnOrdering{{5, false}, {2, false}}),
			},
		},
		{
			existing: orderingInfo{
				exactMatchCols: map[int]struct{}{0: e, 5: e, 6: e},
				ordering:       []columnOrderInfo{{1, true}, {2, false}},
			},
			cases: []desiredCase{
				defTestCase(2, 0, columnOrdering{{1, true}, {5, false}}),
				defTestCase(2, 2, columnOrdering{{0, true}, {5, false}}),
				defTestCase(1, 0, columnOrdering{{1, true}, {2, true}}),
				defTestCase(5, 2, columnOrdering{{0, false}, {6, true}, {1, true}, {5, true}, {2, false}}),
				defTestCase(2, 2, columnOrdering{{0, false}, {6, true}, {2, false}, {5, true}, {1, true}}),
			},
		},
	}

	for _, ts := range testSets {
		for _, tc := range ts.cases {
			res := computeOrderingMatch(tc.desired, ts.existing, false)
			resRev := computeOrderingMatch(tc.desired, ts.existing, true)
			if res != tc.expected || resRev != tc.expectedReverse {
				t.Errorf("Test defined on line %d failed: expected:%d/%d got:%d/%d",
					tc.line, tc.expected, tc.expectedReverse, res, resRev)
			}
		}
	}
}
