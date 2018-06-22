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

package props

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
)

// OrderingChoice defines the set of possible row orderings that are provided or
// required by an operator. An OrderingChoice consists of two parts: an ordered
// sequence of equivalent column groups and a set of optional columns. Together,
// these parts specify a simple pattern that can match one or more candidate
// orderings. Here are some examples:
//
//   +1                  ORDER BY a
//   +1,-2               ORDER BY a,b DESC
//   +(1|2)              ORDER BY a        | ORDER BY b
//   +(1|2),+3           ORDER BY a,c      | ORDER BY b, c
//   -(3|4),+5 opt(1,2)  ORDER BY c DESC,e | ORDER BY a,d DESC,b DESC,e | ...
//
// Each column in the ordering sequence forms the corresponding column of the
// sort key, from most significant to least significant. Each column has a sort
// direction, either ascending or descending. The relation is ordered by the
// first column; rows that have the same value are then ordered by the second
// column; rows that still have the same value are ordered by the third column,
// and so on.
//
// Sometimes multiple columns in the relation have equivalent values. The
// OrderingChoiceColumn stores these columns in a group; any of the columns in
// the group can be used to form the corresponding column in the sort key. The
// equivalent group columns come from SQL expressions like:
//
//   a=b
//
// The optional column set contains columns that can appear anywhere (or
// nowhere) in the ordering. Optional columns come from SQL expressions like:
//
//   a=1
//
// In the last example, the optional columns can be interleaved anywhere in the
// sequence of ordering columns, as they have no effect on the ordering.
type OrderingChoice struct {
	// Optional is the set of columns that can appear at any position in the
	// ordering. Columns in Optional must not appear in the Columns sequence.
	// After initial construction, Optional is immutable. To update, replace
	// with a different set containing the desired columns.
	Optional opt.ColSet

	// Columns is the sequence of equivalent column groups that can be used to
	// form each column in the sort key. Columns must not appear in the Optional
	// set. The array memory is owned by this struct, and should not be copied
	// to another OrderingChoice unless both are kept immutable.
	Columns []OrderingColumnChoice
}

// OrderingColumnChoice specifies the set of columns which can form one of the
// columns in the sort key, as well as the direction of that column (ascending
// or descending).
type OrderingColumnChoice struct {
	// Group is a set of equivalent columns, any of which can be used to form a
	// column in the sort key. After initial construction, Group is immutable.
	// To update, replace with a different set containing the desired columns.
	Group opt.ColSet

	// Descending is true if the sort key column is ordered from highest to
	// lowest. Otherwise, it's ordered from lowest to highest.
	Descending bool
}

// Any is true if this instance allows any ordering (any length, any columns).
func (oc *OrderingChoice) Any() bool {
	return len(oc.Columns) == 0
}

// Ordering returns an opt.Ordering instance composed of the shortest possible
// orderings that this instance allows. If there are several, then one is chosen
// arbitrarily.
func (oc *OrderingChoice) Ordering() opt.Ordering {
	ordering := make(opt.Ordering, len(oc.Columns))
	for i := range oc.Columns {
		col := &oc.Columns[i]
		ordering[i] = opt.MakeOrderingColumn(col.AnyID(), col.Descending)
	}
	return ordering
}

// ColSet returns the set of all non-optional columns that are part of this
// instance. For example, (1,2,3) will be returned if the OrderingChoice is:
//
//   +1,(2|3) opt(4,5)
//
func (oc *OrderingChoice) ColSet() opt.ColSet {
	var cs opt.ColSet
	for i := range oc.Columns {
		cs.UnionWith(oc.Columns[i].Group)
	}
	return cs
}

// SubsetOf returns true if every ordering in this set is allowed by the given
// set. In other words, is this set of ordering choices at least as restrictive
// as the input set? Here are some examples:
//
//   +1                matches +1
//   +1,-2             matches +1             (given set is prefix)
//   +1,-2             matches +1,-2
//   +1                matches +1 opt(2)      (optional col is ignored)
//   -2,+1             matches +1 opt(2)      (optional col is ignored)
//   -2 opt(1)         matches -2 opt(1,3)
//   +1,-2             matches opt(1)         (given set is prefix + optional)
//   +1                matches +(1|2)         (subset of choice)
//   +(1|2)            matches +(1|2|3)       (subset of choice)
//   +(1|2)            matches +1 opt(2)      (minus optional col)
//
//   +1                !matches +1,-2         (prefix matching not commutative)
//   +1 opt(2)         !matches +1            (extra optional cols not allowed)
//   +(1|2|3)          !matches +(1|2)        (subset of choice not commutative)
//
func (oc *OrderingChoice) SubsetOf(other *OrderingChoice) bool {
	if !oc.Optional.SubsetOf(other.Optional) {
		return false
	}

	if len(other.Columns) == 0 {
		return true
	}

	j := 0
	for i := range oc.Columns {
		left := &oc.Columns[i]
		right := other.Columns[j]

		leftGroup := left.Group
		if leftGroup.Intersects(other.Optional) {
			if leftGroup.SubsetOf(other.Optional) {
				continue
			}
			leftGroup = leftGroup.Difference(other.Optional)
		}

		if left.Descending != right.Descending {
			break
		}

		if !leftGroup.SubsetOf(right.Group) {
			break
		}

		j++
		if j >= len(other.Columns) {
			return true
		}
	}
	return false
}

// SubsetOfCols is true if at least one column in each ordering column group is
// part of the given column set. For example, if the OrderingChoice is:
//
//   +1,-(2|3) opt(4,5)
//
// then SubsetOfCols will behave as follows for these input sets:
//
//   (1,2)   => true
//   (1,3)   => true
//   (1,2,4) => true
//   (1)     => false
//   (3,4)   => false
//
func (oc *OrderingChoice) SubsetOfCols(cs opt.ColSet) bool {
	for i := range oc.Columns {
		if !oc.Columns[i].Group.Intersects(cs) {
			return false
		}
	}
	return true
}

// MatchesAt returns true if the ordering column at the given index in this
// instance matches the given column. The column matches if its id is part of
// the equivalence group and if it has the same direction.
func (oc *OrderingChoice) MatchesAt(index int, col opt.OrderingColumn) bool {
	if oc.Optional.Contains(int(col.ID())) {
		return true
	}
	choice := &oc.Columns[index]
	if choice.Descending != col.Descending() {
		return false
	}
	if !choice.Group.Contains(int(col.ID())) {
		return false
	}
	return true
}

// AppendCol adds a new column to the end of the sequence of ordering columns
// maintained by this instance.
func (oc *OrderingChoice) AppendCol(col *OrderingColumnChoice) {
	oc.Columns = append(oc.Columns, *col)
}

// Copy returns a complete copy of this instance, with a private version of the
// ordering column array.
func (oc *OrderingChoice) Copy() OrderingChoice {
	var other OrderingChoice
	other.Optional = oc.Optional
	other.Columns = make([]OrderingColumnChoice, len(oc.Columns))
	copy(other.Columns, oc.Columns)
	return other
}

// Simplify uses the given FD set to streamline the orderings allowed by this
// instance, and to potentially increase the number of allowed orderings:
//
//   1. Constant columns add additional optional column choices.
//
//   2. Equivalent columns allow additional choices within an ordering column
//      group.
//
//   3. If the columns in a group are functionally determined by columns from
//      previous groups, the group can be dropped. This technique is described
//      in the "Reduce Order" section of this paper:
//
//        Simmen, David & Shekita, Eugene & Malkemus, Timothy. (1996).
//        Fundamental Techniques for Order Optimization.
//        Sigmod Record. Volume 25 Issue 2, June 1996. Pages 57-67.
//        https://cs.uwaterloo.ca/~gweddell/cs798/p57-simmen.pdf
//
func (oc *OrderingChoice) Simplify(fdset *FuncDepSet) {
	oc.Optional = fdset.ComputeClosure(oc.Optional)

	closure := oc.Optional
	n := 0
	for i := range oc.Columns {
		group := &oc.Columns[i]

		// Constant columns from the FD set become optional ordering columns and
		// so can be removed.
		if group.Group.Intersects(oc.Optional) {
			if group.Group.SubsetOf(oc.Optional) {
				continue
			}
			group.Group = group.Group.Difference(oc.Optional)
		}

		// Expand group with equivalent columns from FD set.
		group.Group = fdset.ComputeEquivClosure(group.Group)

		// If this group is functionally determined from previous groups, then
		// discard it.
		if group.Group.SubsetOf(closure) {
			continue
		}

		// Add this group's columns and find closure with the new columns.
		closure.UnionWith(group.Group)
		closure = fdset.ComputeClosure(closure)

		if n != i {
			oc.Columns[n] = oc.Columns[i]
		}
		n++
	}
	oc.Columns = oc.Columns[:n]
}

// Truncate removes all ordering columns beyond the given index. For example,
// +1,+(2|3),-4 opt(5,6) would be truncated to:
//
//   prefix=0  => opt(5,6)
//   prefix=1  => +1 opt(5,6)
//   prefix=2  => +1,+(2|3) opt(5,6)
//   prefix=3+ => +1,+(2|3),-4 opt(5,6)
//
func (oc *OrderingChoice) Truncate(prefix int) {
	if prefix < len(oc.Columns) {
		oc.Columns = oc.Columns[:prefix]
	}
}

// Equals returns true if the set of orderings matched by this instance is the
// same as the set matched by the given instance.
func (oc *OrderingChoice) Equals(rhs *OrderingChoice) bool {
	if len(oc.Columns) != len(rhs.Columns) {
		return false
	}
	if !oc.Optional.Equals(rhs.Optional) {
		return false
	}

	for i := range oc.Columns {
		left := &oc.Columns[i]
		right := &rhs.Columns[i]

		if left.Descending != right.Descending {
			return false
		}
		if !left.Group.Equals(right.Group) {
			return false
		}
	}
	return true
}

func (oc OrderingChoice) String() string {
	var buf bytes.Buffer
	oc.Format(&buf)
	return buf.String()
}

// Format writes the OrderingChoice to the given buffer in a human-readable
// string representation that can also be parsed by ParseOrderingChoice:
//
//   +1
//   +1,-2
//   +(1|2)
//   +(1|2),+3
//   -(3|4),+5 opt(1,2)
//
func (oc OrderingChoice) Format(buf *bytes.Buffer) {
	for g := range oc.Columns {
		group := &oc.Columns[g]
		count := group.Group.Len()

		if group.Descending {
			buf.WriteByte('-')
		} else {
			buf.WriteByte('+')
		}

		// Write equivalence group.
		if count > 1 {
			buf.WriteByte('(')
		}
		first := true
		for i, ok := group.Group.Next(0); ok; i, ok = group.Group.Next(i + 1) {
			if !first {
				buf.WriteByte('|')
			} else {
				first = false
			}
			fmt.Fprintf(buf, "%d", i)
		}
		if count > 1 {
			buf.WriteByte(')')
		}

		if g+1 != len(oc.Columns) {
			buf.WriteByte(',')
		}
	}

	// Write set of optional columns.
	if !oc.Optional.Empty() {
		if len(oc.Columns) != 0 {
			buf.WriteByte(' ')
		}
		fmt.Fprintf(buf, "opt%s", oc.Optional)
	}
}

// AnyID returns the ID of an arbitrary member of the group of equivalent
// columns.
func (oc *OrderingColumnChoice) AnyID() opt.ColumnID {
	id, ok := oc.Group.Next(0)
	if !ok {
		panic("column choice group should have at least one column id")
	}
	return opt.ColumnID(id)
}
