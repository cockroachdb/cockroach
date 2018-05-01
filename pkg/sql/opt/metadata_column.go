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

// ColumnID uniquely identifies the usage of a column within the scope of a
// query. ColumnID 0 is reserved to mean "unknown column". See the comment for
// Metadata for more details.
type ColumnID int32

// ColSet efficiently stores an unordered set of column ids.
type ColSet = util.FastIntSet

// ColList is a list of column ids.
//
// TODO(radu): perhaps implement a FastIntList with the same "small"
// representation as FastIntMap but with a slice for large cases.
type ColList = []ColumnID

// ColMap provides a 1:1 mapping from one column id to another. It is used by
// operators that need to match columns from its inputs.
type ColMap = util.FastIntMap

// LabeledColumn specifies the label and id of a column.
type LabeledColumn struct {
	Label string
	ID    ColumnID
}

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

// ColSetToList converts a column id set to a column id list.
func ColSetToList(colSet ColSet) ColList {
	colList := make(ColList, 0, colSet.Len())
	colSet.ForEach(func(i int) {
		colList = append(colList, ColumnID(i))
	})
	return colList
}

// WeakKeys are combinations of columns that form a weak key. No two non-null
// rows are equal if they contain columns from a weak key. For more details, see
// LogicalProps.WeakKeys.
type WeakKeys []ColSet

// ContainsSubsetOf returns true if the weak key list contains a key that is a
// subset of the given key. In that case, there's no reason to add the key to
// the list, since it's redundant.
func (wk *WeakKeys) ContainsSubsetOf(weakKey ColSet) bool {
	for _, existing := range *wk {
		if existing.SubsetOf(weakKey) {
			return true
		}
	}
	return false
}

// Add appends a new weak key to the list of weak keys. It also ensures that no
// weak key is a superset of another, since that is a redundant weak key.
func (wk *WeakKeys) Add(new ColSet) {
	// If one weak key is a subset of another, then use that, since the
	// longer key is redundant.
	insert := 0
	for i, existing := range *wk {
		// If new key is redundant, don't add it.
		if existing.SubsetOf(new) {
			return
		}

		// If existing key is redundant, then remove it from the list. Since
		// there may be multiple redundant keys, wait until after looping to
		// remove all at once.
		if !new.SubsetOf(existing) {
			if insert != i {
				(*wk)[insert] = existing
			}
			insert++
		}
	}
	*wk = append((*wk)[:insert], new)
}
