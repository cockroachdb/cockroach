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
	"github.com/cockroachdb/cockroach/pkg/util"
)

// ColumnIndex uniquely identifies the usage of a column within the scope of a
// query. ColumnIndex 0 is reserved to mean "unknown column". See the comment
// for Metadata for more details.
type ColumnIndex int32

// ColSet efficiently stores an unordered set of column indexes.
type ColSet = util.FastIntSet

// ColList is a list of column indexes.
//
// TODO(radu): perhaps implement a FastIntList with the same "small"
// representation as FastIntMap but with a slice for large cases.
type ColList = []ColumnIndex

// ColMap provides a 1:1 mapping from one column index to another. It is used
// by operators that need to match columns from its inputs.
type ColMap = util.FastIntMap

// LabeledColumn specifies the label and index of a column.
type LabeledColumn struct {
	Label string
	Index ColumnIndex
}

// OrderingColumn is the ColumnIndex for a column that is part of an ordering,
// except that it can be negated to indicate a descending ordering on that
// column.
type OrderingColumn int32

// MakeOrderingColumn initializes an ordering column with a ColumnIndex and a
// flag indicating whether the direction is descending.
func MakeOrderingColumn(index ColumnIndex, descending bool) OrderingColumn {
	if descending {
		return OrderingColumn(-index)
	}
	return OrderingColumn(index)
}

// Index returns the ColumnIndex for this OrderingColumn.
func (c OrderingColumn) Index() ColumnIndex {
	if c < 0 {
		return ColumnIndex(-c)
	}
	return ColumnIndex(c)
}

// Ascending returns true if the ordering on this column is ascending.
func (c OrderingColumn) Ascending() bool {
	return c > 0
}

// Descending returns true if the ordering on this column is descending.
func (c OrderingColumn) Descending() bool {
	return c < 0
}

// ColListToSet converts a column index list to a column index set.
func ColListToSet(colList ColList) ColSet {
	var r ColSet
	for _, col := range colList {
		r.Add(int(col))
	}
	return r
}
