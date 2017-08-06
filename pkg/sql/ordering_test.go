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

func defTestCase(expected, expectedReverse int, desired ...sqlbase.ColumnOrderInfo) desiredCase {
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

	// Helper function to create a ColumnOrderInfo. The "simple" composite
	// literal syntax causes vet to warn about unkeyed literals and the explicit
	// syntax is too verbose.
	o := func(colIdx int, direction encoding.Direction) sqlbase.ColumnOrderInfo {
		return sqlbase.ColumnOrderInfo{ColIdx: colIdx, Direction: direction}
	}

	asc := encoding.Ascending
	desc := encoding.Descending
	testSets := []computeOrderCase{
		{
			// No existing ordering.
			existing: orderingInfo{
				ordering: nil,
				unique:   false,
			},
			cases: []desiredCase{
				defTestCase(0, 0, o(1, desc), o(5, asc)),
			},
		},
		{
			// Ordering with no exact-match columns.
			existing: orderingInfo{
				ordering: sqlbase.ColumnOrdering{o(1, desc), o(2, asc)},
				unique:   false,
			},
			cases: []desiredCase{
				defTestCase(1, 0, o(1, desc), o(5, asc)),
				defTestCase(0, 1, o(1, asc), o(5, asc), o(2, asc)),
			},
		},
		{
			// Ordering with no exact-match columns but with distinct.
			existing: orderingInfo{
				ordering: sqlbase.ColumnOrdering{o(1, desc), o(2, asc)},
				unique:   true,
			},
			cases: []desiredCase{
				defTestCase(1, 0, o(1, desc), o(5, asc)),
				defTestCase(3, 0, o(1, desc), o(2, asc), o(5, asc)),
				defTestCase(4, 0, o(1, desc), o(2, asc), o(5, asc), o(6, desc)),
				defTestCase(0, 1, o(1, asc), o(5, asc), o(2, asc)),
				defTestCase(0, 3, o(1, asc), o(2, desc), o(5, asc)),
				defTestCase(0, 4, o(1, asc), o(2, desc), o(5, asc), o(6, asc)),
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
				defTestCase(1, 1, o(2, desc), o(5, asc), o(1, asc)),
				defTestCase(0, 0, o(5, asc), o(2, asc)),
			},
		},
		{
			// Ordering with exact-match columns.
			existing: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(0, 5, 6),
				ordering:       sqlbase.ColumnOrdering{o(1, desc), o(2, asc)},
				unique:         false,
			},
			cases: []desiredCase{
				defTestCase(2, 0, o(1, desc), o(5, asc)),
				defTestCase(2, 1, o(5, asc), o(1, desc)),
				defTestCase(2, 2, o(0, desc), o(5, asc)),
				defTestCase(1, 0, o(1, desc), o(2, desc)),
				defTestCase(5, 2, o(0, asc), o(6, desc), o(1, desc), o(5, desc), o(2, asc)),
				defTestCase(2, 2, o(0, asc), o(6, desc), o(2, asc), o(5, desc), o(1, desc)),
			},
		},
		{
			// Ordering with exact-match columns and distinct.
			existing: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(0, 5, 6),
				ordering:       sqlbase.ColumnOrdering{o(1, desc), o(2, asc)},
				unique:         true,
			},
			cases: []desiredCase{
				defTestCase(2, 0, o(1, desc), o(5, asc)),
				defTestCase(2, 1, o(5, asc), o(1, desc)),
				defTestCase(4, 0, o(1, desc), o(5, asc), o(2, asc), o(7, desc)),
				defTestCase(4, 1, o(5, asc), o(1, desc), o(2, asc), o(7, desc)),
				defTestCase(2, 2, o(0, desc), o(5, asc)),
				defTestCase(2, 1, o(5, asc), o(1, desc), o(2, desc)),
				defTestCase(1, 0, o(1, desc), o(2, desc)),
				defTestCase(6, 2, o(0, asc), o(6, desc), o(1, desc), o(5, desc), o(2, asc), o(9, asc)),
				defTestCase(2, 2, o(0, asc), o(6, desc), o(2, asc), o(5, desc), o(1, desc)),
			},
		},
	}

	for _, ts := range testSets {
		for _, tc := range ts.cases {
			res := ts.existing.computeMatch(tc.desired)
			ts.existing.reverse()
			resRev := ts.existing.computeMatch(tc.desired)
			ts.existing.reverse()
			if res != tc.expected || resRev != tc.expectedReverse {
				t.Errorf("Test defined on line %d failed: expected:%d/%d got:%d/%d",
					tc.line, tc.expected, tc.expectedReverse, res, resRev)
			}
		}
	}
}

func TestTrimOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Helper function to create a ColumnOrderInfo. The "simple" composite
	// literal syntax causes vet to warn about unkeyed literals and the explicit
	// syntax is too verbose.
	o := func(colIdx int, direction encoding.Direction) sqlbase.ColumnOrderInfo {
		return sqlbase.ColumnOrderInfo{ColIdx: colIdx, Direction: direction}
	}

	asc := encoding.Ascending
	desc := encoding.Descending
	testCases := []struct {
		name     string
		ord      orderingInfo
		desired  sqlbase.ColumnOrdering
		expected orderingInfo
	}{
		{
			name: "basic-prefix-1",
			ord: orderingInfo{
				ordering: sqlbase.ColumnOrdering{o(1, asc), o(2, desc)},
				unique:   true,
			},
			desired: sqlbase.ColumnOrdering{o(1, asc)},
			expected: orderingInfo{
				ordering: sqlbase.ColumnOrdering{o(1, asc)},
				unique:   false,
			},
		},
		{
			name: "direction-mismatch",
			ord: orderingInfo{
				ordering: sqlbase.ColumnOrdering{o(1, asc), o(2, desc)},
				unique:   true,
			},
			desired: sqlbase.ColumnOrdering{o(1, desc)},
			expected: orderingInfo{
				ordering: sqlbase.ColumnOrdering{},
				unique:   false,
			},
		},
		{
			name: "exact-match-columns-1",
			ord: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(0, 5, 6),
				ordering:       sqlbase.ColumnOrdering{o(1, desc), o(2, desc)},
				unique:         true,
			},
			desired: sqlbase.ColumnOrdering{o(5, asc), o(1, desc)},
			expected: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(0, 5, 6),
				ordering:       sqlbase.ColumnOrdering{o(1, desc)},
				unique:         false,
			},
		},
		{
			name: "exact-match-columns-2",
			ord: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(0, 5, 6),
				ordering:       sqlbase.ColumnOrdering{o(1, desc), o(2, desc), o(3, asc)},
				unique:         true,
			},
			desired: sqlbase.ColumnOrdering{o(5, asc), o(1, desc), o(0, desc), o(2, desc)},
			expected: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(0, 5, 6),
				ordering:       sqlbase.ColumnOrdering{o(1, desc), o(2, desc)},
				unique:         false,
			},
		},
		{
			name: "no-match",
			ord: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(0, 5, 6),
				ordering:       sqlbase.ColumnOrdering{o(1, desc), o(2, desc), o(3, asc)},
				unique:         true,
			},
			desired: sqlbase.ColumnOrdering{o(5, asc), o(4, asc)},
			expected: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(0, 5, 6),
				ordering:       sqlbase.ColumnOrdering{},
				unique:         false,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.ord.trim(tc.desired)
			if !reflect.DeepEqual(tc.ord, tc.expected) {
				t.Errorf("expected %v, got %v", tc.expected, tc.ord)
			}
		})
	}
}

func TestComputeMergeJoinOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Helper function to create a ColumnOrderInfo. The "simple" composite
	// literal syntax causes vet to warn about unkeyed literals and the explicit
	// syntax is too verbose.
	o := func(colIdx int, direction encoding.Direction) sqlbase.ColumnOrderInfo {
		return sqlbase.ColumnOrderInfo{ColIdx: colIdx, Direction: direction}
	}

	asc := encoding.Ascending
	desc := encoding.Descending
	testCases := []struct {
		name       string
		a, b       orderingInfo
		colA, colB []int
		expected   sqlbase.ColumnOrdering
	}{
		{
			name: "basic",
			a: orderingInfo{
				ordering: sqlbase.ColumnOrdering{o(1, asc), o(2, desc), o(3, asc)},
			},
			b: orderingInfo{
				ordering: sqlbase.ColumnOrdering{o(3, asc), o(4, desc)},
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: sqlbase.ColumnOrdering{o(0, asc), o(1, desc)},
		},
		{
			name: "exact-match-a",
			a: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(1, 2),
			},
			b: orderingInfo{
				ordering: sqlbase.ColumnOrdering{o(3, asc), o(4, desc)},
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: sqlbase.ColumnOrdering{o(0, asc), o(1, desc)},
		},
		{
			name: "exact-match-b",
			a: orderingInfo{
				ordering: sqlbase.ColumnOrdering{o(1, asc), o(2, desc), o(3, asc)},
			},
			b: orderingInfo{
				exactMatchCols: util.MakeFastIntSet(3, 4),
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: sqlbase.ColumnOrdering{o(0, asc), o(1, desc)},
		},
		{
			name: "exact-match-both",
			a: orderingInfo{
				ordering:       sqlbase.ColumnOrdering{o(2, desc)},
				exactMatchCols: util.MakeFastIntSet(1),
			},
			b: orderingInfo{
				ordering:       sqlbase.ColumnOrdering{o(3, asc)},
				exactMatchCols: util.MakeFastIntSet(4),
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: sqlbase.ColumnOrdering{o(1, desc), o(0, asc)},
		},
		{
			name: "unique-a",
			a: orderingInfo{
				ordering: sqlbase.ColumnOrdering{o(1, asc)},
				unique:   true,
			},
			b: orderingInfo{
				ordering: sqlbase.ColumnOrdering{o(3, asc), o(4, desc)},
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: sqlbase.ColumnOrdering{o(0, asc), o(1, desc)},
		},
		{
			name: "unique-b",
			a: orderingInfo{
				ordering: sqlbase.ColumnOrdering{o(1, asc), o(2, desc)},
			},
			b: orderingInfo{
				ordering: sqlbase.ColumnOrdering{o(3, asc)},
				unique:   true,
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: sqlbase.ColumnOrdering{o(0, asc), o(1, desc)},
		},
		{
			name: "partial-ordering-1",
			a: orderingInfo{
				ordering: sqlbase.ColumnOrdering{o(1, asc), o(3, asc), o(2, desc)},
			},
			b: orderingInfo{
				ordering: sqlbase.ColumnOrdering{o(3, asc), o(4, desc)},
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: sqlbase.ColumnOrdering{o(0, asc)},
		},
		{
			name: "partial-ordering-2",
			a: orderingInfo{
				ordering: sqlbase.ColumnOrdering{o(1, asc), o(2, desc), o(3, asc)},
			},
			b: orderingInfo{
				ordering: sqlbase.ColumnOrdering{o(3, asc), o(5, desc), o(4, desc)},
			},
			colA:     []int{1, 2},
			colB:     []int{3, 4},
			expected: sqlbase.ColumnOrdering{o(0, asc)},
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
