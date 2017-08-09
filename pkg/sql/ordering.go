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
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// orderingInfo describes the column ordering on a set of results. Typically the
// ordering information is used to see to what extent it can satisfy a "desired"
// ordering (which is just a list of columns and directions).
//
// In its simplest form, an ordering is simply a list of columns and a direction
// for each column, for example a+,b-,c+. This indicates that the rows are
// ordered by the value on column a (ascending); rows that have the same value
// on a are ordered by column b (descending); and rows that have the same values
// on a and b are ordered by column c (ascending).
//
// == Constant columns ==
//
// If results are known to be restricted to a single value on some columns, we
// call these "constant" columns; these are inconsequential w.r.t ordering.
//
// For example, if an index was defined on columns (a, b, c, d) and the WHERE
// clause was "(a, c) = (1, 2)" then a and c are constant columns and we have
// an ordering by b+ then d+. Such an ordering satisfies any of the following
// desired orderings (among many others):
//   a+,c+
//   a-,c+
//   b+,a+,c+
//   b+,c+,a-
//   a+,b+,c+
//   c-,b+,a+
//   b+,d+,a-
//   a+,b+,c+,d+
//   b+,a+,c-,d+
//
// Note that constant means all values for this column are equal, but in general
// this doesn't mean that the values are identical/interchangeable (consider
// collated strings).
//
// == Key flag ==
//
// A set of columns S forms a "key" if any two rows that are equal on S are
// necessarily equal on all columns. A special case of this property is when
// each row is "unique" when projected on S (i.e. no two rows have the same
// tuple of values on columns in S); the most important case which provides a
// uniqueness guarantee is if we have unique index on S. Note that uniqueness is
// a stronger (more restrictive) property than the key property; the key
// property in general allows multiple rows to have the same values on S as long
// as those rows are equal on all columns).
//
// The key flag for an ordering is set if the set of columns in the ordering
// form a key.
//
// For example, if we have a unique index on columns (a, b), the results have
// the ordering a+,b+ and the key flag tells us that this satisfies any desired
// ordering that has a+,b+ as a prefix (such as a+,b+,c-,d+).
//
// When we have multiple columns in a group, all possible combinations of
// columns (one from each group) must form a key.
//
// == Column groups ==
//
// In some cases we know that a set of columns are always equal (i.e. any row
// has equal values for these columns); these columns can be used
// interchangeably in orderings. One case is for inner joins that constrain
// equality on two columns: every row has equal values on these two columns, so
// using either is equivalent in an ordering. For example:
//  - table U(a,b,c,d) with primary index (a,b)
//  - table V(e,f,g) with primary index (e,g)
//  - join ON a=e AND b=g
//  - the results will be ordered by (a+/e+),(b+/g+); this satisfies any of the
//    following desired orderings:
//      a+,b+
//      a+,g+
//      e+,b+
//      e+,g+
//      a+,b+,g+
//      a+,e+,b+,g+
// As with constant columns, equality doesn't necessarily mean the values are
// identical/interchangeable (e.g. collated strings).
type orderingInfo struct {
	// columns for which we know we have a single value.
	constantCols util.FastIntSet

	// ordering of any other columns (the columns in constantCols do not appear in this ordering).
	ordering []orderingColumnGroup

	// true if the columns in the ordering form a "key" (see above).
	isKey bool
}

type orderingColumnGroup struct {
	// All columns in the group have the same direction.
	// We could be more general and allow mixed direction columns, but that
	// complicates things with little benefit.
	dir  encoding.Direction
	cols util.FastIntSet
}

// Format pretty-prints the orderingInfo to a stream.
// If columns is not nil, column names are printed instead of column indexes.
func (ord orderingInfo) Format(buf *bytes.Buffer, columns sqlbase.ResultColumns) {
	sep := ""

	// Print the constant columns. We sort them to ensure
	// deterministic output order.
	cols := ord.constantCols.Ordered()

	printCol := func(buf *bytes.Buffer, columns sqlbase.ResultColumns, colIdx int) {
		if columns == nil || colIdx >= len(columns) {
			fmt.Fprintf(buf, "@%d", colIdx+1)
		} else {
			parser.FormatNode(buf, parser.FmtSimple, parser.Name(columns[colIdx].Name))
		}
	}

	for _, c := range cols {
		buf.WriteString(sep)
		sep = ","

		buf.WriteByte('=')
		printCol(buf, columns, c)
	}

	// Print the ordering columns and for each their sort order.
	for _, o := range ord.ordering {
		buf.WriteString(sep)
		sep = ","

		// If there is a single column in the group (the common case), we just print
		// the column prefixed by + or -, e.g. "+a".
		// If there are multiple columns in the group, we enclose the group in
		// parens and separate the columns by slashes: "+(a/b)".
		prefix := byte('+')
		if o.dir == encoding.Descending {
			prefix = byte('-')
		}
		buf.WriteByte(prefix)

		cols := o.cols.Ordered()
		if len(cols) > 1 {
			buf.WriteByte('(')
		}

		for i, c := range cols {
			if i > 0 {
				buf.WriteByte('/')
			}
			printCol(buf, columns, c)
		}

		if len(cols) > 1 {
			buf.WriteByte(')')
		}
	}

	if ord.isKey {
		buf.WriteString(sep)
		buf.WriteString("key")
	}
}

// AsString pretty-prints the orderingInfo to a string. The result columns are
// used for printing column names and are optional.
func (ord orderingInfo) AsString(columns sqlbase.ResultColumns) string {
	var buf bytes.Buffer
	ord.Format(&buf, columns)
	return buf.String()
}

func (ord orderingInfo) isEmpty() bool {
	return ord.constantCols.Empty() && len(ord.ordering) == 0
}

func (ord *orderingInfo) addConstantColumn(colIdx int) {
	ord.constantCols.Add(uint32(colIdx))
}

func (ord *orderingInfo) addColumnGroup(cols util.FastIntSet, dir encoding.Direction) {
	if dir != encoding.Ascending && dir != encoding.Descending {
		panic(fmt.Sprintf("Invalid direction %d", dir))
	}
	// If isKey is true, there are no "ties" to break with adding more columns.
	if !ord.isKey {
		ord.ordering = append(ord.ordering, orderingColumnGroup{dir: dir, cols: cols})
	}
}

func (ord *orderingInfo) addColumn(colIdx int, dir encoding.Direction) {
	var s util.FastIntSet
	s.Add(uint32(colIdx))
	ord.addColumnGroup(s, dir)
}

// copy returns a copy of ord which can be modified independently.
func (ord orderingInfo) copy() orderingInfo {
	result := orderingInfo{
		isKey:        ord.isKey,
		constantCols: ord.constantCols.Copy(),
	}
	if len(ord.ordering) > 0 {
		result.ordering = make([]orderingColumnGroup, len(ord.ordering))
		for i, o := range ord.ordering {
			result.ordering[i].cols = o.cols.Copy()
			result.ordering[i].dir = o.dir
		}
	}
	return result
}

// reverse returns the reversed ordering.
func (ord orderingInfo) reverse() orderingInfo {
	result := orderingInfo{
		isKey:        ord.isKey,
		constantCols: ord.constantCols.Copy(),
	}
	if len(ord.ordering) > 0 {
		result.ordering = make([]orderingColumnGroup, len(ord.ordering))
		for i, o := range ord.ordering {
			result.ordering[i].cols = o.cols.Copy()
			result.ordering[i].dir = o.dir.Reverse()
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
Outer:
	for i, col := range desired {
		// Check if the column is one of the constant columns.
		if ord.constantCols.Contains(uint32(col.ColIdx)) {
			continue Outer
		}
		// Check if the next column group matches this column, or if any of the
		// previous column groups does - for example, if we have an ordering
		// (1/2)+,3+ and the desired ordering is 1+,3+,2+ the 2+ is redundant with
		// 1+ and can be ignored.
		for j := 0; j <= pos && j < len(ord.ordering); j++ {
			if ord.ordering[j].dir == col.Direction &&
				ord.ordering[j].cols.Contains(uint32(col.ColIdx)) {
				if j == pos {
					// The next column group matched.
					pos++
				}
				continue Outer
			}
		}
		if pos == len(ord.ordering) && ord.isKey {
			// Everything matched up to the last column and we know there are no
			// duplicate combinations of values for these columns. Any other columns
			// with which we may want to "refine" the ordering don't make a
			// difference.
			return len(desired)
		}
		// Everything matched up to this point.
		return i
	}
	// Everything matched!
	return len(desired)
}

// trim simplifies ord.ordering, retaining only the column groups that are
// needed to to match a desired ordering (or a prefix of it); constant
// columns are left untouched.
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
Outer:
	for _, col := range desired {
		if pos == len(ord.ordering) {
			// We couldn't trim anything.
			return
		}
		if ord.constantCols.Contains(uint32(col.ColIdx)) {
			continue
		}
		// Check if the next column group matches this column, or if any of the
		// previous column groups does - for example, if we have an ordering
		// (1/2)+,3+ and the desired ordering is 1+,3+,2+ the 2+ is redundant with
		// 1+ and can be ignored.
		for j := 0; j <= pos; j++ {
			if ord.ordering[j].dir == col.Direction &&
				ord.ordering[j].cols.Contains(uint32(col.ColIdx)) {
				if j == pos {
					// The next column group matched.
					pos++
				}
				continue Outer
			}
		}
		// This column didn't "match".
		break
	}
	if pos < len(ord.ordering) {
		ord.ordering = ord.ordering[:pos]
		ord.isKey = false
	}
}

// getColumnOrdering returns a ColumnOrdering that corresponds to ord.ordering
// (excludes constant columns). It is guaranteed that:
//  - ord.ComputeMatch(ord.getColumnOrdering()) == len(ord.ordering)
//  - ord.trim(ord.getColumnOrdering()) is a no-op.
//
// If column groups have more than one column, we return an arbitrary column for
// each group. In that case, the result is one of multiple possible results.
func (ord *orderingInfo) getColumnOrdering() sqlbase.ColumnOrdering {
	result := make(sqlbase.ColumnOrdering, len(ord.ordering))
	for i := range ord.ordering {
		col, ok := ord.ordering[i].cols.Next(0)
		if !ok {
			panic("empty column group")
		}
		result[i].ColIdx = int(col)
		result[i].Direction = ord.ordering[i].dir
	}
	return result
}

// computeMergeJoinOrdering determines if merge-join can be used to perform a join.
//
// It takes the orderings of the two data sources that are to be joined on a set
// of equality columns (the join condition is that the value for the column
// colA[i] equals the value for column colB[i]).
//
// If merge-join can be used, the function returns a ColumnOrdering that refers
// to the equality columns by their index in colA/ColB. Specifically column i in
// the returned ordering refers to column colA[i] for A and colB[i] for B. This
// is the ordering that must be used by the merge-join.
//
// The returned ordering can be partial, i.e. only contains a subset of the
// equality columns. This indicates that a hybrid merge/hash join can be used
// (or alternatively, an extra sorting step to complete the ordering followed by
// a merge-join). See example below.
//
// Note that this function is not intended to calculate the output orderingInfo
// of joins (this is a separate problem with other complications).
//
// Examples:
//  -  natural join between
//       table A with columns (u, v, x, y)  primary key x+,y+,u+
//       table B with columns (x, y, w)     primary key x+,y+
//     equality columns are x, y
//     a orderingInfo is 2+,3+,0+
//     b orderingInfo is 0+,1+
//     colA is {2, 3}   // column indices of x,y in table A
//     colB is {0, 1}   // column indices of x,y in table B
//
//     The function returns 0+,1+. This result maps to ordering 2+,3+ for A and
//     0+,1+ for B; this is what the merge-join will use: it will interleave
//     rows by comparing column A2 with column B0, breaking equalities by
//     comparing column A3 with column B1.
//
//  -  natural join between
//       table A with columns (u, v, x, y)  primary key x+
//       table B with columns (x, y, w)     primary key x+,y+
//     equality columns are x, y
//     a orderingInfo is 2+
//     b orderingInfo is 0+,1+
//     colA is {2, 3}   // column indices of x,y in table A
//     colB is {0, 1}   // column indices of x,y in table B
//
//     The function returns 0+. This maps to ordering 2+ for A and 0+ for B.
//     This is a partial ordering, so a hybrid merge-join can be used: groups of
//     rows that are equal on columns a2 and b0 are loaded and a hash-join is
//     performed on this group. Alternatively, an extra sorting step could be
//     used to refine the ordering (this sorting step would also use the partial
//     ordering to only sort within groups) followed by a regular merge-join.
func computeMergeJoinOrdering(a, b orderingInfo, colA, colB []int) sqlbase.ColumnOrdering {
	if len(colA) != len(colB) {
		panic(fmt.Sprintf("invalid column lists %v; %v", colA, colB))
	}
	if a.isEmpty() || b.isEmpty() || len(colA) == 0 {
		return nil
	}

	var result sqlbase.ColumnOrdering

	// First, find any merged columns that are constant in both sources. This
	// means that in each source, this column only sees one value.
	for i := range colA {
		if a.constantCols.Contains(uint32(colA[i])) && b.constantCols.Contains(uint32(colB[i])) {
			// The direction here is arbitrary - the orderings guarantee that either works.
			// TODO(radu): perhaps the correct thing would be to return an
			// orderingInfo with this as a constant column.
			result = append(result, sqlbase.ColumnOrderInfo{ColIdx: i, Direction: encoding.Ascending})
		}
	}

	// eqMapA/B maps a column index in A/B to the index of the equality column
	// (0 to len(colA)-1).
	eqMapA := make(map[int]int, len(colA))
	for i, v := range colA {
		eqMapA[v] = i
	}
	eqMapB := make(map[int]int, len(colB))
	for i, v := range colB {
		eqMapB[v] = i
	}

	// To understand what's going on, it's useful to first think of the easy
	// case: there are no equality columns or column groups, we just have simple
	// orderings. In this case we need to check that:
	//  - the first column in A's ordering is an equality column, and
	//  - the first column in B's ordering is the same equality column.
	//    If this is the case, we can check the same for the second column, and so
	//    on. If not, we stop.
	//
	// This gets more complicated because of constant columns. If the first
	// column in A's ordering is an equality column and the corresponding B column
	// is a constant, this pairing also works. This means that we will not
	// necessarily consume the orderings at the same rate. The remaining parts of
	// the orderings are maintained in ordA/ordB.
	//
	// Another complication is the "key" flag: such an ordering remains correct
	// when appending arbitrary columns to it.
	//
	// Column groups complicate things further, for example:
	//   A: (1/3)+, (2/4)+
	//   B: 1+, 2+, 3+, 4+
	// Here we match (1/3)+ with 1+, then (2/4)+ with 2+, after which column 3+
	// matches a column inside an earlier group and is thus inconsequential for
	// A's ordering (like a constant column would be). Note that the direction of
	// a redundant column can even differ, B: 1+, 2+, 3-, 4- would match with A
	// just as well. This is because within each group of rows with the same
	// values on columns 1 and 2 in A, there is a single value for columns 3 and
	// 4.
	//
	// To help handle these cases in a unified manner, we keep a list of
	// "optional" columns on each side that can be used arbitrarily to extend an
	// ordering: these are the constant columns + all columns in the groups
	// processed so far.

	optionalColsA := a.constantCols.Copy()
	optionalColsB := b.constantCols.Copy()

MainLoop:
	for ordA, ordB := a.ordering, b.ordering; ; {
		doneA, doneB := (len(ordA) == 0), (len(ordB) == 0)
		// See if the first column group in each ordering contain the same equality
		// column.
		if !doneA && !doneB {
			foundCol := -1
			for i := range colA {
				if ordA[0].cols.Contains(uint32(colA[i])) && ordB[0].cols.Contains(uint32(colB[i])) {
					// Both ordering groups contain the i-th equality column.
					foundCol = i
					break
				}
			}
			if foundCol != -1 {
				dir := ordA[0].dir
				if dir != ordB[0].dir {
					// Both orderings start with the same merged column, but the
					// ordering is different. That's all, folks.
					break MainLoop
				}
				result = append(result, sqlbase.ColumnOrderInfo{ColIdx: foundCol, Direction: dir})
				for col, ok := ordA[0].cols.Next(0); ok; col, ok = ordA[0].cols.Next(col + 1) {
					optionalColsA.Add(col)
				}
				for col, ok := ordB[0].cols.Next(0); ok; col, ok = ordB[0].cols.Next(col + 1) {
					optionalColsB.Add(col)
				}
				ordA, ordB = ordA[1:], ordB[1:]
				continue MainLoop
			}
		}
		// See if any column in the first group in A is constant in B. Or, if
		// we consumed B and it is "unique", then we are free to add any other
		// columns in A.
		if !doneA {
			for i := range colA {
				if ordA[0].cols.Contains(uint32(colA[i])) &&
					((doneB && b.isKey) || optionalColsB.Contains(uint32(colB[i]))) {
					result = append(result, sqlbase.ColumnOrderInfo{ColIdx: i, Direction: ordA[0].dir})
					for col, ok := ordA[0].cols.Next(0); ok; col, ok = ordA[0].cols.Next(col + 1) {
						optionalColsA.Add(col)
					}
					ordA = ordA[1:]
					continue MainLoop
				}
			}
		}
		// See if any column in the first group in B is constant in A. Or, if
		// we consumed A and it is a "key", then we are free to add any other
		// columns in B.
		if !doneB {
			for i := range colB {
				if ordB[0].cols.Contains(uint32(colB[i])) &&
					((doneA && a.isKey) || optionalColsA.Contains(uint32(colA[i]))) {
					result = append(result, sqlbase.ColumnOrderInfo{ColIdx: i, Direction: ordB[0].dir})
					for col, ok := ordB[0].cols.Next(0); ok; col, ok = ordB[0].cols.Next(col + 1) {
						optionalColsB.Add(col)
					}
					ordB = ordB[1:]
					continue MainLoop
				}
			}
		}
		break
	}
	return result
}
