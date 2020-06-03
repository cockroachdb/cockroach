// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import "github.com/cockroachdb/cockroach/pkg/util"

// TableColSet efficiently stores an unordered set of column ids.
type TableColSet struct {
	set util.FastIntSet
}

// MakeTableColSet returns a set initialized with the given values.
func MakeTableColSet(vals ...ColumnID) TableColSet {
	var res TableColSet
	for _, v := range vals {
		res.Add(v)
	}
	return res
}

// Add adds a column to the set. No-op if the column is already in the set.
func (s *TableColSet) Add(col ColumnID) { s.set.Add(int(col)) }

// Contains returns true if the set contains the column.
func (s TableColSet) Contains(col ColumnID) bool { return s.set.Contains(int(col)) }

// Empty returns true if the set is empty.
func (s TableColSet) Empty() bool { return s.set.Empty() }

// Len returns the number of the columns in the set.
func (s TableColSet) Len() int { return s.set.Len() }

// ForEach calls a function for each column in the set (in increasing order).
func (s TableColSet) ForEach(f func(col ColumnID)) { s.set.ForEach(func(i int) { f(ColumnID(i)) }) }

// Ordered returns a slice with all the ColumnIDs in the set, in increasing
// order.
func (s TableColSet) Ordered() []ColumnID {
	if s.Empty() {
		return nil
	}
	result := make([]ColumnID, 0, s.Len())
	s.ForEach(func(i ColumnID) {
		result = append(result, i)
	})
	return result
}

// String returns a list representation of elements. Sequential runs of positive
// numbers are shown as ranges. For example, for the set {1, 2, 3  5, 6, 10},
// the output is "(1-3,5,6,10)".
func (s TableColSet) String() string { return s.set.String() }
