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

package sql

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type desiredCase struct {
	line            int
	desired         sqlbase.ColumnOrdering
	expected        int
	expectedReverse int
}

type computeOrderCase struct {
	existing orderingInfo
	cases    []desiredCase
}

const asc = encoding.Ascending
const desc = encoding.Descending

// Sample usage:
//   makeColumnGroups(1, 2, asc, 5, desc, 4, 6, asc) -> (1/2)+,5-,(4/6)+
func makeColumnGroups(args ...interface{}) []orderingColumnGroup {
	var res []orderingColumnGroup
	var curGroup util.FastIntSet
	for _, a := range args {
		switch v := a.(type) {
		case int:
			curGroup.Add(uint32(v))
		case encoding.Direction:
			res = append(res, orderingColumnGroup{
				cols: curGroup,
				dir:  v,
			})
			curGroup = util.FastIntSet{}
		default:
			panic(fmt.Sprintf("unknown type %T", a))
		}
	}
	if !curGroup.Empty() {
		panic("last arg must be a direction")
	}
	return res
}

// Sample usage:
//   makeColumnOrdering(1, asc, 2, desc, 3, asc) -> 1+,2-,3+
func makeColumnOrdering(args ...interface{}) sqlbase.ColumnOrdering {
	var res sqlbase.ColumnOrdering
	for i := 0; i < len(args); i += 2 {
		res = append(res, sqlbase.ColumnOrderInfo{
			ColIdx:    args[i].(int),
			Direction: args[i+1].(encoding.Direction),
		})
	}
	return res
}

func defTestCase(expected, expectedReverse int, desired sqlbase.ColumnOrdering) desiredCase {
	// The line number is used to identify testcases in error messages.
	_, line, _ := caller.Lookup(1)
	return desiredCase{
		line:            line,
		desired:         desired,
		expected:        expected,
		expectedReverse: expectedReverse,
	}
}

func TestComputeOrderingMatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testSets := []computeOrderCase{
		{
			// No existing ordering.
			existing: orderingInfo{
				ordering: nil,
				unique:   false,
			},
			cases: []desiredCase{
				defTestCase(0, 0, makeColumnOrdering(1, desc, 5, asc)),
			},
		},
		{
			// Ordering with no exact-match columns.
			existing: orderingInfo{
				ordering: makeColumnGroups(1, desc, 2, asc),
				unique:   false,
			},
			cases: []desiredCase{
				defTestCase(1, 0, makeColumnOrdering(1, desc, 5, asc)),
				defTestCase(0, 1, makeColumnOrdering(1, asc, 5, asc, 2, asc)),
			},
		},
		{
			// Ordering with no exact-match columns but with distinct.
			existing: orderingInfo{
				ordering: makeColumnGroups(1, desc, 2, asc),
				unique:   true,
			},
			cases: []desiredCase{
				defTestCase(1, 0, makeColumnOrdering(1, desc, 5, asc)),
				defTestCase(3, 0, makeColumnOrdering(1, desc, 2, asc, 5, asc)),
				defTestCase(4, 0, makeColumnOrdering(1, desc, 2, asc, 5, asc, 6, desc)),
				defTestCase(0, 1, makeColumnOrdering(1, asc, 5, asc, 2, asc)),
				defTestCase(0, 3, makeColumnOrdering(1, asc, 2, desc, 5, asc)),
				defTestCase(0, 4, makeColumnOrdering(1, asc, 2, desc, 5, asc, 6, asc)),
			},
		},
		{
			// Ordering with only exact-match columns.
			existing: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(1, 2),
				ordering:       nil,
				unique:         false,
			},
			cases: []desiredCase{
				defTestCase(1, 1, makeColumnOrdering(2, desc, 5, asc, 1, asc)),
				defTestCase(0, 0, makeColumnOrdering(5, asc, 2, asc)),
			},
		},
		{
			// Ordering with exact-match columns.
			existing: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(0, 5, 6),
				ordering:       makeColumnGroups(1, desc, 2, asc),
				unique:         false,
			},
			cases: []desiredCase{
				defTestCase(2, 0, makeColumnOrdering(1, desc, 5, asc)),
				defTestCase(2, 1, makeColumnOrdering(5, asc, 1, desc)),
				defTestCase(2, 2, makeColumnOrdering(0, desc, 5, asc)),
				defTestCase(1, 0, makeColumnOrdering(1, desc, 2, desc)),
				defTestCase(5, 2, makeColumnOrdering(0, asc, 6, desc, 1, desc, 5, desc, 2, asc)),
				defTestCase(2, 2, makeColumnOrdering(0, asc, 6, desc, 2, asc, 5, desc, 1, desc)),
			},
		},
		{
			// Ordering with exact-match columns and distinct.
			existing: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(0, 5, 6),
				ordering:       makeColumnGroups(1, desc, 2, asc),
				unique:         true,
			},
			cases: []desiredCase{
				defTestCase(2, 0, makeColumnOrdering(1, desc, 5, asc)),
				defTestCase(2, 1, makeColumnOrdering(5, asc, 1, desc)),
				defTestCase(4, 0, makeColumnOrdering(1, desc, 5, asc, 2, asc, 7, desc)),
				defTestCase(4, 1, makeColumnOrdering(5, asc, 1, desc, 2, asc, 7, desc)),
				defTestCase(2, 2, makeColumnOrdering(0, desc, 5, asc)),
				defTestCase(2, 1, makeColumnOrdering(5, asc, 1, desc, 2, desc)),
				defTestCase(1, 0, makeColumnOrdering(1, desc, 2, desc)),
				defTestCase(6, 2, makeColumnOrdering(0, asc, 6, desc, 1, desc, 5, desc, 2, asc, 9, asc)),
				defTestCase(2, 2, makeColumnOrdering(0, asc, 6, desc, 2, asc, 5, desc, 1, desc)),
			},
		},
	}

	for _, ts := range testSets {
		for _, tc := range ts.cases {
			res := ts.existing.computeMatch(tc.desired)
			resRev := ts.existing.reverse().computeMatch(tc.desired)
			if res != tc.expected || resRev != tc.expectedReverse {
				t.Errorf("Test defined on line %d failed: expected:%d/%d got:%d/%d",
					tc.line, tc.expected, tc.expectedReverse, res, resRev)
			}
		}
	}
}

func TestTrimOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		ord      orderingInfo
		desired  sqlbase.ColumnOrdering
		expected orderingInfo
	}{
		{
			name: "basic-prefix-1",
			ord: orderingInfo{
				ordering: makeColumnGroups(1, asc, 2, desc),
				unique:   true,
			},
			desired: makeColumnOrdering(1, asc),
			expected: orderingInfo{
				ordering: makeColumnGroups(1, asc),
				unique:   false,
			},
		},
		{
			name: "direction-mismatch",
			ord: orderingInfo{
				ordering: makeColumnGroups(1, asc, 2, desc),
				unique:   true,
			},
			desired: makeColumnOrdering(1, desc),
			expected: orderingInfo{
				ordering: makeColumnGroups(),
				unique:   false,
			},
		},
		{
			name: "exact-match-columns-1",
			ord: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(0, 5, 6),
				ordering:       makeColumnGroups(1, desc, 2, desc),
				unique:         true,
			},
			desired: makeColumnOrdering(5, asc, 1, desc),
			expected: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(0, 5, 6),
				ordering:       makeColumnGroups(1, desc),
				unique:         false,
			},
		},
		{
			name: "exact-match-columns-2",
			ord: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(0, 5, 6),
				ordering:       makeColumnGroups(1, desc, 2, desc, 3, asc),
				unique:         true,
			},
			desired: makeColumnOrdering(5, asc, 1, desc, 0, desc, 2, desc),
			expected: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(0, 5, 6),
				ordering:       makeColumnGroups(1, desc, 2, desc),
				unique:         false,
			},
		},
		{
			name: "no-match",
			ord: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(0, 5, 6),
				ordering:       makeColumnGroups(1, desc, 2, desc, 3, asc),
				unique:         true,
			},
			desired: makeColumnOrdering(5, asc, 4, asc),
			expected: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(0, 5, 6),
				ordering:       makeColumnGroups(),
				unique:         false,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.ord.trim(tc.desired)
			if !tc.ord.exactMatchCols.Equals(tc.expected.exactMatchCols) ||
				tc.ord.unique != tc.expected.unique ||
				len(tc.ord.ordering) != len(tc.expected.ordering) {
				t.Errorf("expected %v, got %v", tc.expected, tc.ord)
			}
			for i, o := range tc.ord.ordering {
				if !o.cols.Equals(tc.expected.ordering[i].cols) ||
					o.dir != tc.expected.ordering[i].dir {
					t.Errorf("expected %v, got %v", tc.expected, tc.ord)
					break
				}
			}
		})
	}
}

func TestComputeMergeJoinOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name       string
		a, b       orderingInfo
		colA, colB []int
		expected   sqlbase.ColumnOrdering
	}{
		{
			name: "basic",
			a: orderingInfo{
				ordering: makeColumnGroups(1, asc, 2, desc, 3, asc),
			},
			b: orderingInfo{
				ordering: makeColumnGroups(3, asc, 4, desc),
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},
		{
			name: "exact-match-a",
			a: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(1, 2),
			},
			b: orderingInfo{
				ordering: makeColumnGroups(3, asc, 4, desc),
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},
		{
			name: "exact-match-b",
			a: orderingInfo{
				ordering: makeColumnGroups(1, asc, 2, desc, 3, asc),
			},
			b: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(3, 4),
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},
		{
			name: "exact-match-both",
			a: orderingInfo{
				ordering:       makeColumnGroups(2, desc),
				exactMatchCols: util.MakeFastIntSet(1),
			},
			b: orderingInfo{
				ordering:       makeColumnGroups(3, asc),
				exactMatchCols: util.MakeFastIntSet(4),
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(1, desc, 0, asc),
		},
		{
			name: "unique-a",
			a: orderingInfo{
				ordering: makeColumnGroups(1, asc),
				unique:   true,
			},
			b: orderingInfo{
				ordering: makeColumnGroups(3, asc, 4, desc),
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},
		{
			name: "unique-b",
			a: orderingInfo{
				ordering: makeColumnGroups(1, asc, 2, desc),
			},
			b: orderingInfo{
				ordering: makeColumnGroups(3, asc),
				unique:   true,
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc, 1, desc),
		},
		{
			name: "partial-ordering-1",
			a: orderingInfo{
				ordering: makeColumnGroups(1, asc, 3, asc, 2, desc),
			},
			b: orderingInfo{
				ordering: makeColumnGroups(3, asc, 4, desc),
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc),
		},
		{
			name: "partial-ordering-2",
			a: orderingInfo{
				ordering: makeColumnGroups(1, asc, 2, desc, 3, asc),
			},
			b: orderingInfo{
				ordering: makeColumnGroups(3, asc, 5, desc, 4, desc),
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: makeColumnOrdering(0, asc),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := computeMergeJoinOrdering(tc.a, tc.b, tc.colA, tc.colB)
			if !reflect.DeepEqual(tc.expected, result) {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}
