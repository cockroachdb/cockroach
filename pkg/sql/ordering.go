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

// orderingInfo describes the physical properties of a set of results:
// equivalent columns, ordering information, etc.
//
// TODO(radu): rename this structure to "physicalProperties" or similar.
// The intention is that this will evolve towards what is described in
// "Fundamental techniques for order optimization" by Simmen et al.
//
// == Equivalency groups ==
//
// Columns which we know are always equal on any row are equivalent; this
// information can come from filter and join conditions. This information is
// stored in a disjoint sets data structure.
//
// Note that in general, equality doesn't necessarily mean the values are
// identical/interchangeable (e.g. collated strings).
//
// == Constant columns ==
//
// If results are known to be restricted to a single value on some columns, we
// call these "constant columns".
//
// TODO(radu): generalize this to Functional Dependencies as described in the
// paper (referenced above).
//
// == Key information ==
//
// A set of columns S forms a "key" if no two rows are equal when projected
// on S. Such a property arises when we are scanning a unique index.
//
// Currently, we only maintain an "isKey" flag which indicates if the columns in
// the ordering (see below) form a key.
//
// TODO(radu): generalize this to allow arbitrary sets for keys.
//
// == Ordering information ==
//
// Typically the ordering information is used to see to what extent it can
// satisfy a "desired" ordering (which is just a list of columns and
// directions).
//
// In its simplest form, an ordering is simply a list of columns and a direction
// for each column, for example a+,b-,c+. This indicates that the rows are
// ordered by the value on column a (ascending); rows that have the same value
// on a are ordered by column b (descending); and rows that have the same values
// on a and b are ordered by column c (ascending).
//
// Ordering interacts with the other properties:
//  - constant columns are inconsequential w.r.t. ordering; for example, if an
//    index was defined on columns (a, b, c, d) and the WHERE clause was
//    "(a, c) = (1, 2)" then a and c are constant columns and we have an
//    ordering by b+ then d+. Such an ordering satisfies any of the following
//    desired orderings (among many others):
//      a+,c+
//      a-,c+
//      b+,a+,c+
//      b+,c+,a-
//      a+,b+,c+
//      c-,b+,a+
//      b+,d+,a-
//      a+,b+,c+,d+
//      b+,a+,c-,d+
//
//  - equivalency groups: a column in the ordering represents the entire group
//    of equivalent columns; any column in that group can be substituted in the
//    ordering. It's illegal for an ordering to contain multiple columns from
//    the same group.
//
//  - keys: if we have a key on columns (a, b) and the results have the ordering
//    a+,b+ we can satisfy any desired ordering that has a+,b+ as a prefix (such
//    as a+,b+,c-,d+).
type orderingInfo struct {
	// column equivalency groups. This structure assigns a "representative" for
	// each group, which is the smallest column in that group (returned by Find);
	// only representatives can appear in the other fields below.
	eqGroups util.UnionFind

	// columns for which we know we have a single value. For groups of equivalent
	// columns, only the group representative can be in the set.
	constantCols util.FastIntSet

	// ordering of any other columns. This order is "reduced", meaning that there
	// the columns in constantCols do not appear in this ordering and for groups of
	// equivalent columns, only the group representative can appear.
	ordering sqlbase.ColumnOrdering

	// true if the columns in ordering form a "key" (see above).
	isKey bool
}

// check verifies the invariants of the structure.
func (ord orderingInfo) check() {
	// Only equivalency group representatives show up in constantCols.
	for c, ok := ord.constantCols.Next(0); ok; c, ok = ord.constantCols.Next(c + 1) {
		if repr := ord.eqGroups.Find(c); repr != c {
			panic(fmt.Sprintf("non-representative const column %d (representative: %d)", c, repr))
		}
	}
	var seen util.FastIntSet
	for _, o := range ord.ordering {
		// Only equivalency group representatives show up in ordering.
		if repr := ord.eqGroups.Find(o.ColIdx); repr != o.ColIdx {
			panic(fmt.Sprintf("non-representative order column %d (representative: %d)", o.ColIdx, repr))
		}
		// The ordering should not contain any constant or redundant columns.
		if ord.constantCols.Contains(o.ColIdx) {
			panic(fmt.Sprintf("const column %d appears in ordering", o.ColIdx))
		}
		if seen.Contains(o.ColIdx) {
			panic(fmt.Sprintf("duplicate column %d appears in ordering", o.ColIdx))
		}
		seen.Add(o.ColIdx)
	}
	if ord.isKey && len(ord.ordering) == 0 {
		panic("isKey with no ordering")
	}
}

// reduce rewrites an order specification, replacing columns with the
// equivalency group representative and removing any columns that are redundant
// (thanks to the physical properties). If no modifications are necessary,
// returns the same slice.
func (ord *orderingInfo) reduce(order sqlbase.ColumnOrdering) sqlbase.ColumnOrdering {
	// We only allocate the result if we need to make modifications.
	var result sqlbase.ColumnOrdering

	// Set of column groups seen so far.
	var groupsSeen util.FastIntSet
	for i, o := range order {
		group := ord.eqGroups.Find(o.ColIdx)
		redundant := groupsSeen.Contains(group) || ord.constantCols.Contains(group)
		groupsSeen.Add(group)
		if result == nil {
			if !redundant && o.ColIdx == group {
				// No modification necessary, continue.
				continue
			}
			result = make(sqlbase.ColumnOrdering, i, len(order))
			copy(result, order[:i])
		}
		if redundant {
			continue
		}
		o.ColIdx = group
		result = append(result, o)
	}
	if result == nil {
		// No modifications were necessary.
		return order
	}
	return result
}

// Format pretty-prints the orderingInfo to a stream.
// If columns is not nil, column names are printed instead of column indexes.
//
// The output is a series of information "groups" separated by semicolons; each
// group shows:
//  - an equivalency group (e.g. a=b=c)
//  - a constant column (e.g. a=CONST)
//  - ordering (e.g. a+,b-)
//
// Example:
//   a=b=c; d=e=f; g=CONST; h=CONST; b+,d-
func (ord *orderingInfo) Format(buf *bytes.Buffer, columns sqlbase.ResultColumns) {
	ord.check()
	printCol := func(buf *bytes.Buffer, columns sqlbase.ResultColumns, colIdx int) {
		if columns == nil || colIdx >= len(columns) {
			fmt.Fprintf(buf, "@%d", colIdx+1)
		} else {
			parser.FormatNode(buf, parser.FmtSimple, parser.Name(columns[colIdx].Name))
		}
	}

	// Print any equivalency groups.
	var groups util.FastIntSet
	for i := 0; i < ord.eqGroups.Len(); i++ {
		representative := ord.eqGroups.Find(i)
		if representative != i {
			// We found a multi-column group.
			groups.Add(representative)
		}
	}

	firstGroup := true
	semiColon := func() {
		if !firstGroup {
			buf.WriteString("; ")
		}
		firstGroup = false
	}
	for r, ok := groups.Next(0); ok; r, ok = groups.Next(r + 1) {
		semiColon()
		// The representative is always the first element in the group.
		printCol(buf, columns, r)
		for i := r + 1; i < ord.eqGroups.Len(); i++ {
			if ord.eqGroups.Find(i) == r {
				buf.WriteByte('=')
				printCol(buf, columns, i)
			}
		}
	}
	// Print the constant columns.
	if !ord.constantCols.Empty() {
		for _, c := range ord.constantCols.Ordered() {
			semiColon()
			printCol(buf, columns, c)
			buf.WriteString("=CONST")
		}
	}

	// Print the ordering columns and for each their sort order.
	for i, o := range ord.ordering {
		if i == 0 {
			semiColon()
		} else {
			buf.WriteByte(',')
		}

		// We print the representative column of the group.
		prefix := byte('+')
		if o.Direction == encoding.Descending {
			prefix = byte('-')
		}
		buf.WriteByte(prefix)
		printCol(buf, columns, o.ColIdx)
	}

	if ord.isKey {
		semiColon()
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

func (ord *orderingInfo) isEmpty() bool {
	return ord.constantCols.Empty() && len(ord.ordering) == 0
}

func (ord *orderingInfo) addConstantColumn(colIdx int) {
	ord.constantCols.Add(ord.eqGroups.Find(colIdx))
	ord.ordering = ord.reduce(ord.ordering)
	if len(ord.ordering) == 0 {
		ord.isKey = false
	}
}

func (ord *orderingInfo) addEquivalency(colA, colB int) {
	ord.eqGroups.Union(colA, colB)
	ord.ordering = ord.reduce(ord.ordering)
	if len(ord.ordering) == 0 {
		ord.isKey = false
	}
}

func (ord *orderingInfo) addOrderColumn(colIdx int, dir encoding.Direction) {
	// If isKey is true, there are no "ties" to break with adding more columns.
	if ord.isKey {
		return
	}
	ord.ordering = append(ord.ordering, sqlbase.ColumnOrderInfo{
		ColIdx:    ord.eqGroups.Find(colIdx),
		Direction: dir,
	})
	ord.ordering = ord.reduce(ord.ordering)
}

// copy returns a copy of ord which can be modified independently.
func (ord *orderingInfo) copy() orderingInfo {
	result := orderingInfo{
		eqGroups:     ord.eqGroups.Copy(),
		isKey:        ord.isKey,
		constantCols: ord.constantCols.Copy(),
	}
	if len(ord.ordering) > 0 {
		result.ordering = append(sqlbase.ColumnOrdering(nil), ord.ordering...)
	}
	return result
}

// reverse returns the reversed ordering.
func (ord *orderingInfo) reverse() orderingInfo {
	result := ord.copy()
	for i := range ord.ordering {
		result.ordering[i].Direction = result.ordering[i].Direction.Reverse()
	}
	return result
}

// project returns an orderingInfo for a set of columns that include a
// projection of the original columns. Each column for ord has
// an entry in colMap; colMap[i] is the index of ord's column i
// in the new column set, or -1 if the column is not part of the projection.
//
// The map allows arbitrary remapping of columns for cases where projected
// columns are intermingled with rendered expressions.
func (ord *orderingInfo) project(colMap []int) orderingInfo {
	var newOrd orderingInfo

	// For every group that has at least a column that is projected, pick one
	// such column as a representative for that group. If newRepr[i] != -1,
	// then i is a group representative in ord, and colMap[i] != -1.
	newRepr := make([]int, len(colMap))
	for i := 0; i < len(colMap); i++ {
		newRepr[i] = -1
	}

	for i := range colMap {
		if colMap[i] != -1 {
			group := ord.eqGroups.Find(i)
			if newRepr[group] == -1 {
				// Pick i as a representative for this group.
				newRepr[group] = i
			}
		}
	}

	// Rebuild equivalency groups.
	for i := range colMap {
		if colMap[i] != -1 {
			group := ord.eqGroups.Find(i)
			if r := newRepr[group]; r != -1 {
				newOrd.eqGroups.Union(colMap[i], colMap[r])
			}
		}
	}

	// Remap constant columns, ignoring column groups that have no projected
	// columns.
	for col, ok := ord.constantCols.Next(0); ok; col, ok = ord.constantCols.Next(col + 1) {
		group := ord.eqGroups.Find(col)
		if r := newRepr[group]; r != -1 {
			newOrd.constantCols.Add(newOrd.eqGroups.Find(colMap[r]))
		}
	}

	newOrd.isKey = ord.isKey
	newOrd.ordering = make(sqlbase.ColumnOrdering, 0, len(ord.ordering))

	// Remove all the columns after the first one that's not present in
	// the result columns.
	for _, o := range ord.ordering {
		r := newRepr[o.ColIdx]
		if r == -1 {
			// None of the columns in the equivalency group are present. We need to
			// break the ordering here.
			// If something is ordered by columns A, then B, then C, if I remove
			// column B I can't say it's ordered by columns A, then C.
			// Example:
			// A | B | C          A | C
			// ---------          -----
			// 1 | 1 | 2   --->   1 | 2
			// 1 | 2 | 1          1 | 1
			// 1 | 2 | 3          1 | 3
			newOrd.isKey = false
			break
		}
		newOrd.ordering = append(newOrd.ordering, sqlbase.ColumnOrderInfo{
			ColIdx: newOrd.eqGroups.Find(colMap[r]), Direction: o.Direction,
		})
	}
	return newOrd
}

// computeMatch computes how long of a prefix of a desired ColumnOrdering is
// matched by the orderingInfo.
//
// Returns a value between 0 and len(desired).
func (ord orderingInfo) computeMatch(desired sqlbase.ColumnOrdering) int {
	matchLen, _ := ord.computeMatchInternal(desired)
	return matchLen
}

// computeMatchInternal returns both the length of the match and the number of
// columns of ord.ordering necessary for the match.
func (ord orderingInfo) computeMatchInternal(
	desired sqlbase.ColumnOrdering,
) (matchLen, ordPos int) {
	ord.check()
	// position in ord.ordering
	pos := 0
	// Set of column groups seen so far.
	var groupsSeen util.FastIntSet

	for i, col := range desired {
		group := ord.eqGroups.Find(col.ColIdx)
		// Check if the column is one of the constant columns.
		if ord.constantCols.Contains(group) {
			continue
		}
		if groupsSeen.Contains(group) {
			// Redundant column; can be ignored.
			continue
		}
		groupsSeen.Add(group)
		if pos < len(ord.ordering) && ord.ordering[pos].ColIdx == group &&
			ord.ordering[pos].Direction == col.Direction {
			// The next column matches.
			pos++
			continue
		}
		if pos == len(ord.ordering) && ord.isKey {
			// Everything matched up to the last column and we know there are no
			// duplicate combinations of values for these columns. Any other columns
			// with which we may want to "refine" the ordering don't make a
			// difference.
			return len(desired), pos
		}
		// Everything matched up to this point.
		return i, pos
	}
	// Everything matched!
	return len(desired), pos
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
	_, pos := ord.computeMatchInternal(desired)
	if pos < len(ord.ordering) {
		ord.ordering = ord.ordering[:pos]
		ord.isKey = false
	}
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
		if a.constantCols.Contains(colA[i]) && b.constantCols.Contains(colB[i]) {
			// The direction here is arbitrary - the orderings guarantee that either works.
			// TODO(radu): perhaps the correct thing would be to return an
			// orderingInfo with this as a constant column.
			result = append(result, sqlbase.ColumnOrderInfo{ColIdx: i, Direction: encoding.Ascending})
		}
	}

	// To understand what's going on, it's useful to first think of the easy
	// case: there are no constant columns or equivalent column groups, we just
	// have simple orderings. In this case we need to check that:
	//  - the first column in A's ordering is an equality column, and
	//  - the first column in B's ordering is the same equality column.
	//    If this is the case, we can check the same for the second column, and so
	//    on. If not, we stop.
	//
	// This gets more complicated because of constant columns. If the first
	// column in A's ordering is an equality column and the corresponding B column
	// is a constant, this pairing also works. This means that we will not
	// necessarily consume the orderings at the same rate. The code below
	// tracks the remaining parts of the orderings in ordA/ordB.
	//
	// Another complication is the "key" flag: such an ordering remains correct
	// when appending arbitrary columns to it.
	//
	// Column groups complicate things further, for example:
	//   A: 1=3; 2=4; 1+,2+
	//   B: 1+, 2+, 3+, 4+
	// Here we match (1/3)+ with 1+, then (2/4)+ with 2+, after which column 3+
	// matches a column inside an earlier group and is thus inconsequential for
	// A's ordering (like a constant column would be). Note that the direction of
	// a redundant column can even differ, B: 1+, 2+, 3-, 4- would match with A
	// just as well. This is because within each group of rows with the same
	// values on columns 1 and 2 in A, there is a single value for columns 3 and
	// 4.
	//
	// To help handle these cases in a unified manner, we keep a list of "seen"
	// column groups on each side that can be used arbitrarily to extend an
	// ordering: these are the constant column groups and the groups processed so
	// far.

	seenGroupsA := a.constantCols.Copy()
	seenGroupsB := b.constantCols.Copy()

MainLoop:
	for ordA, ordB := a.ordering, b.ordering; ; {
		doneA, doneB := (len(ordA) == 0), (len(ordB) == 0)
		// See if the first column group in each ordering contain the same equality
		// column.
		if !doneA && !doneB {
			foundCol := -1
			groupA := a.eqGroups.Find(ordA[0].ColIdx)
			groupB := b.eqGroups.Find(ordB[0].ColIdx)
			for i := range colA {
				if a.eqGroups.Find(colA[i]) == groupA && b.eqGroups.Find(colB[i]) == groupB {
					// Both ordering groups contain the i-th equality column.
					foundCol = i
					break
				}
			}
			if foundCol != -1 {
				dir := ordA[0].Direction
				if dir != ordB[0].Direction {
					// Both orderings start with the same merged column, but the
					// ordering is different. That's all, folks.
					break MainLoop
				}
				result = append(result, sqlbase.ColumnOrderInfo{ColIdx: foundCol, Direction: dir})
				seenGroupsA.Add(groupA)
				seenGroupsB.Add(groupB)
				ordA, ordB = ordA[1:], ordB[1:]
				continue MainLoop
			}
		}
		// See if any column in the first group in A is "seen" in B. Or, if
		// we consumed B and it is a "key", then we are free to add any other
		// columns in A.
		if !doneA {
			groupA := a.eqGroups.Find(ordA[0].ColIdx)
			for i := range colA {
				if a.eqGroups.Find(colA[i]) == groupA &&
					((doneB && b.isKey) || seenGroupsB.Contains(b.eqGroups.Find(colB[i]))) {
					result = append(result, sqlbase.ColumnOrderInfo{ColIdx: i, Direction: ordA[0].Direction})
					seenGroupsA.Add(groupA)
					ordA = ordA[1:]
					continue MainLoop
				}
			}
		}
		// See if any column in the first group in B is "seen" in A. Or, if
		// we consumed A and it is a "key", then we are free to add any other
		// columns in B.
		if !doneB {
			groupB := b.eqGroups.Find(ordB[0].ColIdx)
			for i := range colB {
				if b.eqGroups.Find(colB[i]) == groupB &&
					((doneA && a.isKey) || seenGroupsA.Contains(a.eqGroups.Find(colA[i]))) {
					result = append(result, sqlbase.ColumnOrderInfo{ColIdx: i, Direction: ordB[0].Direction})
					seenGroupsB.Add(groupB)
					ordB = ordB[1:]
					continue MainLoop
				}
			}
		}
		break
	}
	return result
}
