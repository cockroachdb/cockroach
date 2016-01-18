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

// Returns the line where it is called, used to indentify testcases in errors.
func getLine() int {
	_, _, line, _ := runtime.Caller(1)
	return line
}

func TestComputeOrderMatch(t *testing.T) {
	defer leaktest.AfterTest(t)

	e := struct{}{}
	testSets := []computeOrderCase{
		{
			// No existing ordering
			existing: orderingInfo{
				singleResultCols: nil,
				ordering:         nil,
			},
			cases: []desiredCase{
				{
					line:            getLine(),
					desired:         columnOrdering{{1, true}, {5, false}},
					expected:        0,
					expectedReverse: 0,
				},
			},
		},
		{
			// Ordering with no single-result columns
			existing: orderingInfo{
				singleResultCols: nil,
				ordering:         []columnOrderInfo{{1, true}, {2, false}},
			},
			cases: []desiredCase{
				{
					line:            getLine(),
					desired:         columnOrdering{{1, true}, {5, false}},
					expected:        1,
					expectedReverse: 0,
				},
				{
					line:            getLine(),
					desired:         columnOrdering{{1, false}, {5, false}, {2, false}},
					expected:        0,
					expectedReverse: 1,
				},
			},
		},
		{
			// Ordering with only single-result columns
			existing: orderingInfo{
				singleResultCols: map[int]struct{}{1: e, 2: e},
				ordering:         nil,
			},
			cases: []desiredCase{
				{
					line:            getLine(),
					desired:         columnOrdering{{2, true}, {5, false}, {1, false}},
					expected:        1,
					expectedReverse: 1,
				},
				{
					line:            getLine(),
					desired:         columnOrdering{{5, false}, {2, false}},
					expected:        0,
					expectedReverse: 0,
				},
			},
		},
		{
			existing: orderingInfo{
				singleResultCols: map[int]struct{}{0: e, 5: e, 6: e},
				ordering:         []columnOrderInfo{{1, true}, {2, false}},
			},
			cases: []desiredCase{
				{
					line:            getLine(),
					desired:         columnOrdering{{1, true}, {5, false}},
					expected:        2,
					expectedReverse: 0,
				},
				{
					line:            getLine(),
					desired:         columnOrdering{{0, true}, {5, false}},
					expected:        2,
					expectedReverse: 2,
				},
				{
					line:            getLine(),
					desired:         columnOrdering{{1, true}, {2, true}},
					expected:        1,
					expectedReverse: 0,
				},
				{
					line:            getLine(),
					desired:         columnOrdering{{0, false}, {6, true}, {1, true}, {5, true}, {2, false}},
					expected:        5,
					expectedReverse: 2,
				},
				{
					line:            getLine(),
					desired:         columnOrdering{{0, false}, {6, true}, {2, false}, {5, true}, {1, true}},
					expected:        2,
					expectedReverse: 2,
				},
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
