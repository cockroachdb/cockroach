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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
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
type ColList []ColumnID

// mdColumn stores information about one of the columns stored in the metadata,
// including its label and type.
type mdColumn struct {
	// tabID is the identifier of the base table to which this column belongs.
	// If the column was synthesized (i.e. no base table), then the value is set
	// to UnknownTableID.
	tabID TableID

	// label is the best-effort name of this column. Since the same column can
	// have multiple labels (using aliasing), one of those is chosen to be used
	// for pretty-printing and debugging. This might be different than what is
	// stored in the physical properties and is presented to end users.
	label string

	// typ is the scalar SQL type of this column.
	typ types.T
}

// ColumnTableID returns the identifier of the base table to which the given
// column belongs. If the column has no base table because it was synthesized,
// ColumnTableID returns zero.
func (md *Metadata) ColumnTableID(id ColumnID) TableID {
	// ColumnID is biased so that 0 is never used (reserved to indicate the
	// unknown column).
	return md.cols[id-1].tabID
}

// ColumnLabel returns the label of the given column. It is used for pretty-
// printing and debugging.
func (md *Metadata) ColumnLabel(id ColumnID) string {
	// ColumnID is biased so that 0 is never used (reserved to indicate the
	// unknown column).
	return md.cols[id-1].label
}

// ColumnType returns the SQL scalar type of the given column.
func (md *Metadata) ColumnType(id ColumnID) types.T {
	// ColumnID is biased so that 0 is never used (reserved to indicate the
	// unknown column).
	return md.cols[id-1].typ
}

// ColumnOrdinal returns the ordinal position of the column in its base table.
// It panics if the column has no base table because it was synthesized.
func (md *Metadata) ColumnOrdinal(id ColumnID) int {
	tabID := md.cols[id-1].tabID
	if tabID == 0 {
		panic("column was synthesized and has no ordinal position")
	}
	return int(id - tabID.firstColID())
}

// QualifiedColumnLabel returns the column label, qualified with the table name
// if either of these conditions is true:
//
//   1. fullyQualify is true
//   2. there's another column in the metadata with the same column name but
//      different table name
//
// If the column label is qualified, the table is prefixed to it and separated
// by a "." character. The table name is qualified with catalog/schema only if
// fullyQualify is true.
func (md *Metadata) QualifiedColumnLabel(id ColumnID, fullyQualify bool) string {
	col := &md.cols[id-1]
	if col.tabID == 0 {
		// Column doesn't belong to a table, so no need to qualify it further.
		return col.label
	}
	tab := md.Table(col.tabID)

	// If a fully qualified label has not been requested, then only qualify it if
	// it would otherwise be ambiguous.
	var tabName string
	if !fullyQualify {
		for i := range md.cols {
			if i == int(id-1) {
				continue
			}

			// If there are two columns with same name, then column is ambiguous.
			otherCol := &md.cols[i]
			if otherCol.label == col.label {
				tabName = string(tab.Name().TableName)
				if otherCol.tabID == 0 {
					fullyQualify = true
				} else {
					// Only qualify if the qualified names are actually different.
					otherTabName := string(md.Table(otherCol.tabID).Name().TableName)
					if tabName != otherTabName {
						fullyQualify = true
					}
				}
			}
		}
	} else {
		tabName = tab.Name().FQString()
	}

	if !fullyQualify {
		return col.label
	}

	var sb strings.Builder
	sb.WriteString(tabName)
	sb.WriteRune('.')
	sb.WriteString(col.label)
	return sb.String()
}

// ToSet converts a column id list to a column id set.
func (cl ColList) ToSet() ColSet {
	var r ColSet
	for _, col := range cl {
		r.Add(int(col))
	}
	return r
}

// Find searches for a column in the list and returns its index in the list (if
// successful).
func (cl ColList) Find(col ColumnID) (idx int, ok bool) {
	for i := range cl {
		if cl[i] == col {
			return i, true
		}
	}
	return -1, false
}

// Equals returns true if this column list has the same columns as the given
// column list, in the same order.
func (cl ColList) Equals(other ColList) bool {
	if len(cl) != len(other) {
		return false
	}
	for i := range cl {
		if cl[i] != other[i] {
			return false
		}
	}
	return true
}

// ColSetToList converts a column id set to a list, in column id order.
func ColSetToList(set ColSet) ColList {
	res := make(ColList, 0, set.Len())
	set.ForEach(func(x int) {
		res = append(res, ColumnID(x))
	})
	return res
}

// ColMap provides a 1:1 mapping from one column id to another. It is used by
// operators that need to match columns from its inputs.
type ColMap = util.FastIntMap

// LabeledColumn specifies the label and id of a column.
type LabeledColumn struct {
	Label string
	ID    ColumnID
}
