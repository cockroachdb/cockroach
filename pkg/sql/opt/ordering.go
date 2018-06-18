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

// ColListToSet converts a column id list to a column id set.
func ColListToSet(colList ColList) ColSet {
	var r ColSet
	for _, col := range colList {
		r.Add(int(col))
	}
	return r
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
		if col.Descending() {
			buf.WriteByte('-')
		} else {
			buf.WriteByte('+')
		}
		fmt.Fprintf(buf, "%d", col.ID())
	}
}

// ColSet returns the set of column IDs used in the ordering.
func (o Ordering) ColSet() ColSet {
	var colSet ColSet
	for _, col := range o {
		colSet.Add(int(col.ID()))
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
