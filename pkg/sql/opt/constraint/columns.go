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
	c.firstCol = cols[0]
	c.otherCols = cols[1:]
}

// InitSingle is a more efficient version of Init for the common case of a
// single column.
func (c *Columns) InitSingle(col opt.OrderingColumn) {
	c.firstCol = col
	c.otherCols = nil
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

func (c Columns) String() string {
	var b strings.Builder

	for i := 0; i < c.Count(); i++ {
		b.WriteRune('/')
		b.WriteString(fmt.Sprintf("%d", c.Get(i)))
	}
	return b.String()
}
