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
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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

// ColList is a list of column IDs.
//
// TODO(radu): perhaps implement a FastIntList with the same "small"
// representation as FastIntMap but with a slice for large cases.
type ColList []ColumnID

// ToSet converts a column id list to a column id set.
func (cl ColList) ToSet() ColSet {
	var r ColSet
	for _, col := range cl {
		r.Add(col)
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

// OptionalColList is a list of column IDs where some of the IDs can be unset.
// It is used when the columns map 1-to-1 to a known list of objects (e.g. table
// columns).
type OptionalColList []ColumnID

// IsEmpty returns true if all columns in the list are unset.
func (ocl OptionalColList) IsEmpty() bool {
	for i := range ocl {
		if ocl[i] != 0 {
			return false
		}
	}
	return true
}

// Equals returns true if this column list is identical to another list.
func (ocl OptionalColList) Equals(other OptionalColList) bool {
	if len(ocl) != len(other) {
		return false
	}
	for i := range ocl {
		if ocl[i] != other[i] {
			return false
		}
	}
	return true
}

// Find searches for a column in the list and returns its index in the list (if
// successful).
func (ocl OptionalColList) Find(col ColumnID) (idx int, ok bool) {
	if col == 0 {
		// An OptionalColList cannot contain zero column IDs.
		return -1, false
	}
	for i := range ocl {
		if ocl[i] == col {
			return i, true
		}
	}
	return -1, false
}

// ToSet returns the set of columns that are present in the list.
func (ocl OptionalColList) ToSet() ColSet {
	var r ColSet
	for _, col := range ocl {
		if col != 0 {
			r.Add(col)
		}
	}
	return r
}

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
	Type *types.T

	// Table is the base table to which this column belongs.
	// If the column was synthesized (i.e. no base table), then it is 0.
	Table TableID
}

// ColMap provides a 1:1 mapping from one column id to another. It is used by
// operators that need to match columns from its inputs.
type ColMap = util.FastIntMap

// AliasedColumn specifies the label and id of a column.
type AliasedColumn struct {
	Alias string
	ID    ColumnID
}
