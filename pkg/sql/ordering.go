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
	"bytes"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

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
	ordering sqlbase.ColumnOrdering

	// true if we know that all the value tuples for the columns in `ordering` are distinct.
	unique bool
}

// Format pretty-prints the orderingInfo to a stream.
// If columns is not nil, column names are printed instead of column indexes.
func (ord orderingInfo) Format(buf *bytes.Buffer, columns sqlbase.ResultColumns) {
	sep := ""

	// Print the exact match columns. We sort them to ensure
	// a deterministic output order.
	cols := make([]int, 0, len(ord.exactMatchCols))
	for i := range ord.exactMatchCols {
		cols = append(cols, i)
	}
	sort.Ints(cols)

	for _, i := range cols {
		buf.WriteString(sep)
		sep = ","

		buf.WriteByte('=')
		if columns == nil || i >= len(columns) {
			fmt.Fprintf(buf, "%d", i)
		} else {
			parser.FormatNode(buf, parser.FmtSimple, parser.Name(columns[i].Name))
		}
	}

	// Print the ordering columns and for each their sort order.
	for _, o := range ord.ordering {
		buf.WriteString(sep)
		sep = ","

		prefix := byte('+')
		if o.Direction == encoding.Descending {
			prefix = '-'
		}
		buf.WriteByte(prefix)
		if columns != nil && o.ColIdx < len(columns) {
			parser.FormatNode(buf, parser.FmtSimple, parser.Name(columns[o.ColIdx].Name))
		} else {
			fmt.Fprintf(buf, "@%d", o.ColIdx+1)
		}
	}

	if ord.unique {
		buf.WriteString(sep)
		buf.WriteString("unique")
	}
}

// AsString pretty-prints the orderingInfo to a string.
func (ord orderingInfo) AsString(columns sqlbase.ResultColumns) string {
	var buf bytes.Buffer
	ord.Format(&buf, columns)
	return buf.String()
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
		ord.ordering = append(ord.ordering, sqlbase.ColumnOrderInfo{ColIdx: colIdx, Direction: dir})
	}
}

// reverse returns the reversed ordering.
func (ord orderingInfo) reverse() orderingInfo {
	result := orderingInfo{unique: ord.unique}
	if len(ord.exactMatchCols) > 0 {
		result.exactMatchCols = make(map[int]struct{}, len(ord.exactMatchCols))
		for c := range ord.exactMatchCols {
			result.exactMatchCols[c] = struct{}{}
		}
	}
	if len(ord.ordering) > 0 {
		result.ordering = make(sqlbase.ColumnOrdering, len(ord.ordering))
		for i, o := range ord.ordering {
			ord.ordering[i].ColIdx = o.ColIdx
			ord.ordering[i].Direction = o.Direction.Reverse()
		}
	}
	return result
}

// computeMatch computes how long of a prefix of a desired ColumnOrdering is
// matched by the orderingInfo.
//
// Returns a value between 0 and len(desired).
func (ord orderingInfo) computeMatch(desired sqlbase.ColumnOrdering) int {
	// position in ord.ordering
	pos := 0
	for i, col := range desired {
		if pos < len(ord.ordering) {
			ci := ord.ordering[pos]

			// Check that the next column matches.
			if ci.ColIdx == col.ColIdx && ci.Direction == col.Direction {
				pos++
				continue
			}
		} else if ord.unique {
			// Everything matched up to the last column and we know there are no duplicate
			// combinations of values for these columns. Any other columns we may want to "refine"
			// the ordering by don't make a difference.
			return len(desired)
		}
		// If the column did not match, check if it is one of the exact match columns.
		if _, ok := ord.exactMatchCols[col.ColIdx]; !ok {
			// Everything matched up to this point.
			return i
		}
	}
	// Everything matched!
	return len(desired)
}

// trim simplifies ord.ordering, retaining only the columns that are needed to
// to match a desired ordering (or a prefix of it); exact match columns are left
// untouched.
//
// A trimmed ordering is guaranteed to still match the desired ordering to the
// same extent, i.e. before and after are equal in:
//   before := ord.computeMatch(desired)
//   ord.trim(desired)
//   after := ord.computeMatch(desired)
func (ord *orderingInfo) trim(desired sqlbase.ColumnOrdering) {
	// position in ord.ordering
	pos := 0
	// The code in this loop follows the one in computeMatch.
	for _, col := range desired {
		if pos == len(ord.ordering) {
			return
		}
		ci := ord.ordering[pos]
		// Check that the next column matches.
		if ci.ColIdx == col.ColIdx && ci.Direction == col.Direction {
			pos++
		} else if _, ok := ord.exactMatchCols[col.ColIdx]; !ok {
			break
		}
	}
	if pos < len(ord.ordering) {
		ord.ordering = ord.ordering[:pos]
		ord.unique = false
	}
}

// computeMergeOrdering takes the ordering of two data sources that are to be
// merged on a set of columns (specifically all values for column colA[i] for the
// first source and all values for column colB[i] for the second source end up
// in the same result column i). It computes an orderingInfo that can be
// guaranteed on the merged data (if we interleave the rows correctly).
//
// In the resulting ordering, the equality columns are referenced by their index
// in colA/colB. Specifically column i in the resulting ordering refers
// to the column that contains colA[i] from A and colB[i] from B.
func computeMergeOrdering(a, b orderingInfo, colA, colB []int) sqlbase.ColumnOrdering {
	if len(colA) != len(colB) {
		panic(fmt.Sprintf("invalid column lists %v; %v", colA, colB))
	}
	if a.isEmpty() || b.isEmpty() || len(colA) == 0 {
		return nil
	}

	var result sqlbase.ColumnOrdering

	used := make([]bool, len(colA))

	// First, find any merged columns that are exact matches in both sources. This
	// means that in each source, this column only sees one value. Note that this
	// value can be different for the two sources, so the merged column is not
	// necessarily an exact match column in the merged data.
	for i := range colA {
		if _, ok := a.exactMatchCols[colA[i]]; ok {
			if _, ok := b.exactMatchCols[colB[i]]; ok {
				used[i] = true
				result = append(result, sqlbase.ColumnOrderInfo{ColIdx: i, Direction: encoding.Ascending})
			}
		}
	}

	eqMapA := make(map[int]int, len(colA))
	for i, v := range colA {
		eqMapA[v] = i
	}
	eqMapB := make(map[int]int, len(colB))
	for i, v := range colB {
		eqMapB[v] = i
	}

	for ordA, ordB := a.ordering, b.ordering; ; {
		aDone, bDone := (len(ordA) == 0), (len(ordB) == 0)
		// See if the first column in the orderings matches.
		if !aDone && !bDone {
			i, okA := eqMapA[ordA[0].ColIdx]
			j, okB := eqMapB[ordB[0].ColIdx]
			if okA && okB && i == j {
				dir := ordA[0].Direction
				if dir != ordB[0].Direction {
					// Both orderings start with the same merged column, but the
					// ordering is different. That's all, folks.
					break
				}
				result = append(result, sqlbase.ColumnOrderInfo{ColIdx: i, Direction: dir})
				ordA, ordB = ordA[1:], ordB[1:]
				continue
			}
		}
		// See if the first column in A is an exact match in B. Or, if we consumed B
		// and it is "unique", then we are free to add any other columns in A.
		if !aDone {
			if i, ok := eqMapA[ordA[0].ColIdx]; ok {
				if _, ok := b.exactMatchCols[colB[i]]; ok || (bDone && b.unique) {
					result = append(result, sqlbase.ColumnOrderInfo{ColIdx: i, Direction: ordA[0].Direction})
					ordA = ordA[1:]
					continue
				}
			}
		}
		// See if the first column in B is an exact match in A.  Or, if we consumed
		// A and it is "unique", then we are free to add any other columns in B.
		if !bDone {
			if i, ok := eqMapB[ordB[0].ColIdx]; ok {
				if _, ok := a.exactMatchCols[colA[i]]; ok || (aDone && a.unique) {
					result = append(result, sqlbase.ColumnOrderInfo{ColIdx: i, Direction: ordB[0].Direction})
					ordB = ordB[1:]
					continue
				}
			}
		}
		break
	}
	// TODO(radu): if we consumed an ordering and it is ".unique" (i.e. at most
	// one row has the same set of values on those columns), then we can assume
	// the remainder of that ordering
	return result
}
