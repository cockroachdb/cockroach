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

// Remove removes a column from the set. No-op if the column is not in the set.
func (s *TableColSet) Remove(col ColumnID) { s.set.Remove(int(col)) }

// Contains returns true if the set contains the column.
func (s TableColSet) Contains(col ColumnID) bool { return s.set.Contains(int(col)) }

// Empty returns true if the set is empty.
func (s TableColSet) Empty() bool { return s.set.Empty() }

// Len returns the number of the columns in the set.
func (s TableColSet) Len() int { return s.set.Len() }

// Next returns the first value in the set which is >= startVal. If there is no
// such column, the second return value is false.
func (s TableColSet) Next(startVal ColumnID) (ColumnID, bool) {
	c, ok := s.set.Next(int(startVal))
	return ColumnID(c), ok
}

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

// Copy returns a copy of s which can be modified independently.
func (s TableColSet) Copy() TableColSet { return TableColSet{set: s.set.Copy()} }

// UnionWith adds all the columns from rhs to this set.
func (s *TableColSet) UnionWith(rhs TableColSet) { s.set.UnionWith(rhs.set) }

// Union returns the union of s and rhs as a new set.
func (s TableColSet) Union(rhs TableColSet) TableColSet { return TableColSet{set: s.set.Union(rhs.set)} }

// IntersectionWith removes any columns not in rhs from this set.
func (s *TableColSet) IntersectionWith(rhs TableColSet) { s.set.IntersectionWith(rhs.set) }

// Intersection returns the intersection of s and rhs as a new set.
func (s TableColSet) Intersection(rhs TableColSet) TableColSet {
	return TableColSet{set: s.set.Intersection(rhs.set)}
}

// DifferenceWith removes any elements in rhs from this set.
func (s *TableColSet) DifferenceWith(rhs TableColSet) { s.set.DifferenceWith(rhs.set) }

// Difference returns the elements of s that are not in rhs as a new set.
func (s TableColSet) Difference(rhs TableColSet) TableColSet {
	return TableColSet{set: s.set.Difference(rhs.set)}
}

// Intersects returns true if s has any elements in common with rhs.
func (s TableColSet) Intersects(rhs TableColSet) bool { return s.set.Intersects(rhs.set) }

// Equals returns true if the two sets are identical.
func (s TableColSet) Equals(rhs TableColSet) bool { return s.set.Equals(rhs.set) }

// SubsetOf returns true if rhs contains all the elements in s.
func (s TableColSet) SubsetOf(rhs TableColSet) bool { return s.set.SubsetOf(rhs.set) }

// String returns a list representation of elements. Sequential runs of positive
// numbers are shown as ranges. For example, for the set {1, 2, 3  5, 6, 10},
// the output is "(1-3,5,6,10)".
func (s TableColSet) String() string { return s.set.String() }
