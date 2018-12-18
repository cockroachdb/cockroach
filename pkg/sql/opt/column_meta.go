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

// index returns the index of the column in Metadata.cols. It's biased by 1, so
// that ColumnID 0 can be be reserved to mean "unknown column".
func (c ColumnID) index() int {
	return int(c - 1)
}

// ColSet efficiently stores an unordered set of column ids.
type ColSet = util.FastIntSet

// ColList is a list of column ids.
//
// TODO(radu): perhaps implement a FastIntList with the same "small"
// representation as FastIntMap but with a slice for large cases.
type ColList []ColumnID

// ColumnMeta stores information about one of the columns stored in the
// metadata.
type ColumnMeta struct {
	// MetaID is the identifier for this column that is unique within the query
	// metadata.
	MetaID ColumnID

	// Alias is the best-effort name of this column. Since the same column in a
	// query can have multiple names (using aliasing), one of those is chosen to
	// be used for pretty-printing and debugging. This might be different than
	// what is stored in the physical properties and is presented to end users.
	Alias string

	// Type is the scalar SQL type of this column.
	Type types.T

	// TableMeta is the metadata for the base table to which this column belongs.
	// If the column was synthesized (i.e. no base table), then is is null.
	TableMeta *TableMeta

	// md is a back-reference to the query metadata.
	md *Metadata
}

// QualifiedAlias returns the column alias, possibly qualified with the table,
// schema, or database name:
//
//   1. If fullyQualify is true, then the returned alias is prefixed by the
//      original, fully qualified name of the table: tab.Name().FQString().
//
//   2. If there's another column in the metadata with the same column alias but
//      a different table name, then prefix the column alias with the table
//      name: "tabName.columnAlias".
//
func (cm *ColumnMeta) QualifiedAlias(fullyQualify bool) string {
	if cm.TableMeta == nil {
		// Column doesn't belong to a table, so no need to qualify it further.
		return cm.Alias
	}
	tab := cm.TableMeta.Table
	md := cm.md

	// If a fully qualified name has not been requested, then only qualify it if
	// it would otherwise be ambiguous.
	var tabName string
	if !fullyQualify {
		for i := range md.cols {
			if i == int(cm.MetaID-1) {
				continue
			}

			// If there are two columns with same alias, then column is ambiguous.
			cm2 := &md.cols[i]
			if cm2.Alias == cm.Alias {
				tabName = cm.TableMeta.Name()
				if cm2.TableMeta == nil {
					fullyQualify = true
				} else {
					// Only qualify if the qualified names are actually different.
					tabName2 := cm2.TableMeta.Name()
					if tabName != tabName2 {
						fullyQualify = true
					}
				}
			}
		}
	} else {
		tabName = tab.Name().FQString()
	}

	if !fullyQualify {
		return cm.Alias
	}

	var sb strings.Builder
	sb.WriteString(tabName)
	sb.WriteRune('.')
	sb.WriteString(cm.Alias)
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

// AliasedColumn specifies the label and id of a column.
type AliasedColumn struct {
	Alias string
	ID    ColumnID
}
