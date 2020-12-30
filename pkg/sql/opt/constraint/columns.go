// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package constraint

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
)

// Columns identifies the columns which correspond to the values in a Key (and
// consequently the columns of a Span or Constraint).
//
// The columns have directions; a descending column inverts the order of the
// values on that column (in other words, inverts the result of any Datum
// comparisons on that column).
type Columns struct {
	// firstCol holds the first column id and otherCols hold any ids beyond the
	// first. These are separated in order to optimize for the common case of a
	// single-column constraint.
	firstCol  opt.OrderingColumn
	otherCols []opt.OrderingColumn
}

// Init initializes a Columns structure.
func (c *Columns) Init(cols []opt.OrderingColumn) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*c = Columns{
		firstCol:  cols[0],
		otherCols: cols[1:],
	}
}

// InitSingle is a more efficient version of Init for the common case of a
// single column.
func (c *Columns) InitSingle(col opt.OrderingColumn) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*c = Columns{firstCol: col}
}

var _ = (*Columns).InitSingle

// Count returns the number of constrained columns (always at least one).
func (c *Columns) Count() int {
	// There's always at least one column.
	return 1 + len(c.otherCols)
}

// Get returns the nth column and direction. Together with the
// Count method, Get allows iteration over the list of constrained
// columns (since there is no method to return a slice of columns).
func (c *Columns) Get(nth int) opt.OrderingColumn {
	// There's always at least one column.
	if nth == 0 {
		return c.firstCol
	}
	return c.otherCols[nth-1]
}

// Equals returns true if the two lists of columns are identical.
func (c *Columns) Equals(other *Columns) bool {
	n := c.Count()
	if n != other.Count() {
		return false
	}
	if c.firstCol != other.firstCol {
		return false
	}
	// Fast path for when the two share the same slice.
	if n == 1 || &c.otherCols[0] == &other.otherCols[0] {
		return true
	}
	// Hint for the compiler to eliminate bounds check inside the loop.
	tmp := other.otherCols[:len(c.otherCols)]
	for i, v := range c.otherCols {
		if v != tmp[i] {
			return false
		}
	}
	return true
}

// IsPrefixOf returns true if the columns in c are a prefix of the columns in
// other.
func (c *Columns) IsPrefixOf(other *Columns) bool {
	if c.firstCol != other.firstCol || len(c.otherCols) > len(other.otherCols) {
		return false
	}
	for i := range c.otherCols {
		if c.otherCols[i] != other.otherCols[i] {
			return false
		}
	}
	return true
}

// IsStrictSuffixOf returns true if the columns in c are a strict suffix of the
// columns in other.
func (c *Columns) IsStrictSuffixOf(other *Columns) bool {
	offset := other.Count() - c.Count()
	if offset <= 0 {
		return false
	}
	if c.firstCol != other.otherCols[offset-1] {
		return false
	}
	// Fast path when the slices are aliased.
	if len(c.otherCols) == 0 || &c.otherCols[0] == &other.otherCols[offset] {
		return true
	}
	cmpCols := other.otherCols[offset:]
	// Hint for the compiler to eliminate the bound check inside the loop.
	cmpCols = cmpCols[:len(c.otherCols)]
	for i, v := range c.otherCols {
		if v != cmpCols[i] {
			return false
		}
	}
	return true
}

// RemapColumns returns a new Columns object with all ColumnIDs remapped to
// ones that come from the 'to' table. The old ColumnIDs must come from the
// 'from' table.
func (c *Columns) RemapColumns(from, to opt.TableID) Columns {
	var newColumns Columns
	newColumns.firstCol = c.firstCol.RemapColumn(from, to)
	newColumns.otherCols = make([]opt.OrderingColumn, len(c.otherCols))
	for i := range c.otherCols {
		newColumns.otherCols[i] = c.otherCols[i].RemapColumn(from, to)
	}
	return newColumns
}

// ColSet returns the columns as a ColSet.
func (c *Columns) ColSet() opt.ColSet {
	var r opt.ColSet
	r.Add(c.firstCol.ID())
	for _, c := range c.otherCols {
		r.Add(c.ID())
	}
	return r
}

func (c Columns) String() string {
	var b strings.Builder

	for i := 0; i < c.Count(); i++ {
		b.WriteRune('/')
		b.WriteString(fmt.Sprintf("%d", c.Get(i)))
	}
	return b.String()
}
