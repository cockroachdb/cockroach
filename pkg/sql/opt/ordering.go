// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opt

import (
	"bytes"
	"fmt"
)

// OrderingColumn is the ColumnID for a column that is part of an ordering,
// except that it can be negated to indicate a descending ordering on that
// column.
type OrderingColumn int32

// MakeOrderingColumn initializes an ordering column with a ColumnID and a flag
// indicating whether the direction is descending.
func MakeOrderingColumn(id ColumnID, descending bool) OrderingColumn {
	if descending {
		return OrderingColumn(-id)
	}
	return OrderingColumn(id)
}

// ID returns the ColumnID for this OrderingColumn.
func (c OrderingColumn) ID() ColumnID {
	if c < 0 {
		return ColumnID(-c)
	}
	return ColumnID(c)
}

// Ascending returns true if the ordering on this column is ascending.
func (c OrderingColumn) Ascending() bool {
	return c > 0
}

// Descending returns true if the ordering on this column is descending.
func (c OrderingColumn) Descending() bool {
	return c < 0
}

// RemapColumn returns a new OrderingColumn that uses a ColumnID from the 'to'
// table. The original ColumnID must be from the 'from' table.
func (c OrderingColumn) RemapColumn(from, to TableID) OrderingColumn {
	ord := from.ColumnOrdinal(c.ID())
	newColID := to.ColumnID(ord)
	return MakeOrderingColumn(newColID, c.Descending())
}

func (c OrderingColumn) String() string {
	var buf bytes.Buffer
	c.Format(&buf)
	return buf.String()
}

// Format prints a string representation to the buffer.
func (c OrderingColumn) Format(buf *bytes.Buffer) {
	if c.Descending() {
		buf.WriteByte('-')
	} else {
		buf.WriteByte('+')
	}
	fmt.Fprintf(buf, "%d", c.ID())
}

// Ordering defines the order of rows provided or required by an operator. A
// negative value indicates descending order on the column id "-(value)".
type Ordering []OrderingColumn

// Empty returns true if the ordering is empty or unset.
func (o Ordering) Empty() bool {
	return len(o) == 0
}

func (o Ordering) String() string {
	var buf bytes.Buffer
	o.Format(&buf)
	return buf.String()
}

// Format prints a string representation to the buffer.
func (o Ordering) Format(buf *bytes.Buffer) {
	for i, col := range o {
		if i > 0 {
			buf.WriteString(",")
		}
		col.Format(buf)
	}
}

// ColSet returns the set of column IDs used in the ordering.
func (o Ordering) ColSet() ColSet {
	var colSet ColSet
	for _, col := range o {
		colSet.Add(col.ID())
	}
	return colSet
}

// Provides returns true if the required ordering is a prefix of this ordering.
func (o Ordering) Provides(required Ordering) bool {
	if len(o) < len(required) {
		return false
	}

	for i := range required {
		if o[i] != required[i] {
			return false
		}
	}
	return true
}

// CommonPrefix returns the longest ordering that is a prefix of both orderings.
func (o Ordering) CommonPrefix(other Ordering) Ordering {
	for i := range o {
		if i >= len(other) || o[i] != other[i] {
			return o[:i]
		}
	}
	return o
}

// Equals returns true if the two orderings are identical.
func (o Ordering) Equals(rhs Ordering) bool {
	if len(o) != len(rhs) {
		return false
	}

	for i := range o {
		if o[i] != rhs[i] {
			return false
		}
	}
	return true
}

// restrictToCols returns an ordering that refers only to columns in the given
// set. The equivCols argument allows ordering columns that are not in the given
// set to be remapped to equivalent columns that are in the given set. A column
// is only remapped if:
//
//   1. It does not exist in cols.
//   2. And equivCols returns at least one column that:
//     A. Exists in cols.
//     B. And does not exist in any preceding columns of the ordering.
//
// Note that a new ordering is allocated if one or more of its columns are
// remapped in order to prevent mutating the original ordering. If no columns
// are remapped, the returned ordering is a slice of the original.
func (o Ordering) restrictToCols(cols ColSet, equivCols func(ColumnID) ColSet) Ordering {
	// Find the longest prefix of the ordering that contains only columns in
	// the set.
	newOrd, isCopy := o, false
	for i, c := range o {
		if cols.Contains(c.ID()) {
			// If a new ordering was created in a previous iteration of the
			// loop and it already contains the current column, do not
			// duplicate it in the ordering.
			// TODO(mgartner): If we ignore this column and shift the remaining
			// columns to the left and continue rather than returning early,
			// then longer orderings can be generated in some cases.
			if isCopy && newOrd[:i].ColSet().Contains(c.ID()) {
				return newOrd[:i]
			}

			// Otherwise, continue to the next column in the ordering.
			continue
		}

		// If the current column does not exist in cols, check if there is
		// an equivalent column in cols if equivCols was provided.
		if equivCols == nil {
			return newOrd[:i]
		}

		// Get all the equivalent columns.
		eqCols := equivCols(c.ID())
		if eqCols.Empty() {
			return newOrd[:i]
		}

		// Find the first column equivalent to c that is not already in the
		// ordering.
		currCols := newOrd[:i].ColSet()
		eqCol, ok := eqCols.Difference(currCols).Intersection(cols).Next(0)
		if !ok {
			return newOrd[:i]
		}

		// Copy the ordering to avoid mutating the original.
		if !isCopy {
			newOrd = make(Ordering, len(o))
			copy(newOrd, o)
			isCopy = true
		}

		// Replace the i-th column with the equivalent column.
		newOrd[i] = MakeOrderingColumn(eqCol, c.Descending())
	}
	return newOrd
}
