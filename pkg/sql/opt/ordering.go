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

	"github.com/cockroachdb/errors"
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

// OrderingSet is a set of orderings, with the restriction that no ordering
// is a prefix of another ordering in the set.
type OrderingSet []Ordering

// Copy returns a copy of the set which can be independently modified.
func (os OrderingSet) Copy() OrderingSet {
	res := make(OrderingSet, len(os))
	copy(res, os)
	return res
}

// Add an ordering to the list, checking whether it is a prefix of another
// ordering (or vice-versa).
func (os *OrderingSet) Add(o Ordering) {
	if len(o) == 0 {
		panic(errors.AssertionFailedf("empty ordering"))
	}
	for i := range *os {
		prefix := (*os)[i].CommonPrefix(o)
		if len(prefix) == len(o) {
			// o is equal to, or a prefix of os[i]. Do nothing.
			return
		}
		if len(prefix) == len((*os)[i]) {
			// os[i] is a prefix of o; replace it.
			(*os)[i] = o
			return
		}
	}
	*os = append(*os, o)
}

// RestrictToPrefix keeps only the orderings that have the required ordering as
// a prefix.
func (os *OrderingSet) RestrictToPrefix(required Ordering) {
	res := (*os)[:0]
	for _, o := range *os {
		if o.Provides(required) {
			res = append(res, o)
		}
	}
	*os = res
}

// RestrictToCols keeps only the orderings (or prefixes of them) that refer to
// columns in the given set.
func (os *OrderingSet) RestrictToCols(cols ColSet) {
	old := *os
	*os = old[:0]
	for _, o := range old {
		// Find the longest prefix of the ordering that contains
		// only columns in the set.
		prefix := 0
		for _, c := range o {
			if !cols.Contains(c.ID()) {
				break
			}
			prefix++
		}
		if prefix > 0 {
			// This function appends at most one element; it is ok to operate on the
			// same slice.
			os.Add(o[:prefix])
		}
	}
}

func (os OrderingSet) String() string {
	var buf bytes.Buffer
	for i, o := range os {
		if i > 0 {
			buf.WriteByte(' ')
		}
		buf.WriteByte('(')
		buf.WriteString(o.String())
		buf.WriteByte(')')
	}
	return buf.String()
}
