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

import "github.com/cockroachdb/cockroach/pkg/sql/opt"

// Columns identifies the columns which correspond to the values in a Key (and
// consequently the columns of a Span or Constraint).
//
// The columns have directions; a descending column inverts the order of the
// values on that column (in other words, inverts the result of any Datum
// comparisons on that column).
type Columns struct {
	// firstCol holds the first column index and otherCols hold any indexes
	// beyond the first. These are separated in order to optimize for the common
	// case of a single-column constraint.
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
