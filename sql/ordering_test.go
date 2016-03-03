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

	"github.com/cockroachdb/cockroach/util/encoding"
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
	defer leaktest.AfterTest(t)()

	e := struct{}{}
	asc := encoding.Ascending
	desc := encoding.Descending
	testSets := []computeOrderCase{
		{
			// No existing ordering.
			existing: orderingInfo{
				exactMatchCols: nil,
				ordering:       nil,
				unique:         false,
			},
			cases: []desiredCase{
				defTestCase(0, 0, columnOrdering{{1, desc}, {5, asc}}),
			},
		},
		{
			// Ordering with no exact-match columns.
			existing: orderingInfo{
				exactMatchCols: nil,
				ordering:       []columnOrderInfo{{1, desc}, {2, asc}},
				unique:         false,
			},
			cases: []desiredCase{
				defTestCase(1, 0, columnOrdering{{1, desc}, {5, asc}}),
				defTestCase(0, 1, columnOrdering{{1, asc}, {5, asc}, {2, asc}}),
			},
		},
		{
			// Ordering with no exact-match columns but with distinct.
			existing: orderingInfo{
				exactMatchCols: nil,
				ordering:       []columnOrderInfo{{1, desc}, {2, asc}},
				unique:         true,
			},
			cases: []desiredCase{
				defTestCase(1, 0, columnOrdering{{1, desc}, {5, asc}}),
				defTestCase(3, 0, columnOrdering{{1, desc}, {2, asc}, {5, asc}}),
				defTestCase(4, 0, columnOrdering{{1, desc}, {2, asc}, {5, asc}, {6, desc}}),
				defTestCase(0, 1, columnOrdering{{1, asc}, {5, asc}, {2, asc}}),
				defTestCase(0, 3, columnOrdering{{1, asc}, {2, desc}, {5, asc}}),
				defTestCase(0, 4, columnOrdering{{1, asc}, {2, desc}, {5, asc}, {6, asc}}),
			},
		},
		{
			// Ordering with only exact-match columns.
			existing: orderingInfo{
				exactMatchCols: map[int]struct{}{1: e, 2: e},
				ordering:       nil,
				unique:         false,
			},
			cases: []desiredCase{
				defTestCase(1, 1, columnOrdering{{2, desc}, {5, asc}, {1, asc}}),
				defTestCase(0, 0, columnOrdering{{5, asc}, {2, asc}}),
			},
		},
		{
			// Ordering with exact-match columns.
			existing: orderingInfo{
				exactMatchCols: map[int]struct{}{0: e, 5: e, 6: e},
				ordering:       []columnOrderInfo{{1, desc}, {2, asc}},
				unique:         false,
			},
			cases: []desiredCase{
				defTestCase(2, 0, columnOrdering{{1, desc}, {5, asc}}),
				defTestCase(2, 1, columnOrdering{{5, asc}, {1, desc}}),
				defTestCase(2, 2, columnOrdering{{0, desc}, {5, asc}}),
				defTestCase(1, 0, columnOrdering{{1, desc}, {2, desc}}),
				defTestCase(5, 2, columnOrdering{{0, asc}, {6, desc}, {1, desc}, {5, desc}, {2, asc}}),
				defTestCase(2, 2, columnOrdering{{0, asc}, {6, desc}, {2, asc}, {5, desc}, {1, desc}}),
			},
		},
		{
			// Ordering with exact-match columns and distinct.
			existing: orderingInfo{
				exactMatchCols: map[int]struct{}{0: e, 5: e, 6: e},
				ordering:       []columnOrderInfo{{1, desc}, {2, asc}},
				unique:         true,
			},
			cases: []desiredCase{
				defTestCase(2, 0, columnOrdering{{1, desc}, {5, asc}}),
				defTestCase(2, 1, columnOrdering{{5, asc}, {1, desc}}),
				defTestCase(4, 0, columnOrdering{{1, desc}, {5, asc}, {2, asc}, {7, desc}}),
				defTestCase(4, 1, columnOrdering{{5, asc}, {1, desc}, {2, asc}, {7, desc}}),
				defTestCase(2, 2, columnOrdering{{0, desc}, {5, asc}}),
				defTestCase(2, 1, columnOrdering{{5, asc}, {1, desc}, {2, desc}}),
				defTestCase(1, 0, columnOrdering{{1, desc}, {2, desc}}),
				defTestCase(6, 2, columnOrdering{{0, asc}, {6, desc}, {1, desc}, {5, desc}, {2, asc}, {9, asc}}),
				defTestCase(2, 2, columnOrdering{{0, asc}, {6, desc}, {2, asc}, {5, desc}, {1, desc}}),
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
