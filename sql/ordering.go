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
	"fmt"

	"github.com/cockroachdb/cockroach/util/encoding"
)

// columnOrdering is used to describe a desired column ordering. For example,
//     []columnOrderInfo{ {3, true}, {1, false} }
// represents an ordering first by column 3 (descending), then by column 1 (ascending).
type columnOrdering []columnOrderInfo

type columnOrderInfo struct {
	colIdx    int
	direction encoding.Direction
}

// orderingInfo describes the column ordering on a set of results.
//
// If results are known to be restricted to a single value on some columns, we call these "exact
// match" columns; these are inconsequential w.r.t ordering.
//
// For example, if an index was defined on columns (a, b, c, d) and the WHERE clause was
// "(a, c) = (1, 2)" then a and c are exact-match columns and we have an ordering by b then d.
// Such an ordering satisfies any of the following desired orderings (among many others):
//    a, c
//    c, a
//    b, a, c
//    b, c, a
//    a, b, c
//    c, b, a
//    b, d, a
//    a, b, c, d
//    b, a, c, d
//
type orderingInfo struct {
	// columns for which we know we have a single value.
	exactMatchCols map[int]struct{}

	// ordering of any other columns (the columns in exactMatchCols do not appear in this ordering).
	ordering columnOrdering

	// true if we know that all the value tuples for the columns in `ordering` are distinct.
	unique bool
}

func (ord orderingInfo) isEmpty() bool {
	return len(ord.exactMatchCols) == 0 && len(ord.ordering) == 0
}

func (ord *orderingInfo) addExactMatchColumn(colIdx int) {
	if ord.exactMatchCols == nil {
		ord.exactMatchCols = make(map[int]struct{})
	}
	ord.exactMatchCols[colIdx] = struct{}{}
}

func (ord *orderingInfo) addColumn(colIdx int, dir encoding.Direction) {
	if dir != encoding.Ascending && dir != encoding.Descending {
		panic(fmt.Sprintf("Invalid direction %d", dir))
	}
	// If unique is true, there are no "ties" to break with adding more columns.
	if !ord.unique {
		ord.ordering = append(ord.ordering, columnOrderInfo{colIdx, dir})
	}
}

// Computes how long of a prefix of a desired columnOrdering is matched by an existing ordering. If
// reverse is set, we assume the reverse of the existing ordering.
//
// Returns a value between 0 and len(desired).
func computeOrderingMatch(desired columnOrdering, existing orderingInfo, reverse bool) int {
	// position in existing.ordering
	pos := 0
	for i, col := range desired {
		if pos < len(existing.ordering) {
			ci := existing.ordering[pos]

			// Check that the next column matches. Note: "!=" acts as logical XOR.
			if ci.colIdx == col.colIdx && (ci.direction == col.direction) != reverse {
				pos++
				continue
			}
		} else if existing.unique {
			// Everything matched up to the last column and we know there are no duplicate
			// combinations of values for these columns. Any other columns we may want to "refine"
			// the ordering by don't make a difference.
			return len(desired)
		}
		// If the column did not match, check if it is one of the exact match columns.
		if _, ok := existing.exactMatchCols[col.colIdx]; !ok {
			// Everything matched up to this point.
			return i
		}
	}
	// Everything matched!
	return len(desired)
}
