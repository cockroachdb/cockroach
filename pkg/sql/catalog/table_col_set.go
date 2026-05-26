// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catalog

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
)

// TableColSet efficiently stores an unordered set of column ids.
type TableColSet struct {
	set intsets.Fast
}

// MakeTableColSet returns a set initialized with the given values.
func MakeTableColSet(vals ...descpb.ColumnID) TableColSet {
	var res TableColSet
	for _, v := range vals {
		res.Add(v)
	}
	return res
}

// Add adds a column to the set. No-op if the column is already in the set.
func (s *TableColSet) Add(col descpb.ColumnID) { s.set.Add(int(col)) }

// Remove removes a column from the set. No-op if the column is not in the set.
func (s *TableColSet) Remove(col descpb.ColumnID) { s.set.Remove(int(col)) }

// Contains returns true if the set contains the column.
func (s TableColSet) Contains(col descpb.ColumnID) bool { return s.set.Contains(int(col)) }

// Empty returns true if the set is empty.
func (s TableColSet) Empty() bool { return s.set.Empty() }

// Len returns the number of the columns in the set.
func (s TableColSet) Len() int { return s.set.Len() }

// Next returns the first value in the set which is >= startVal. If there is no
// value, the second return value is false.
func (s TableColSet) Next(startVal descpb.ColumnID) (descpb.ColumnID, bool) {
	c, ok := s.set.Next(int(startVal))
	return descpb.ColumnID(c), ok
}

// ForEach calls a function for each column in the set (in increasing order).
func (s TableColSet) ForEach(f func(col descpb.ColumnID)) {
	s.set.ForEach(func(i int) { f(descpb.ColumnID(i)) })
}

// Copy returns a copy of s which can be modified independently.
func (s TableColSet) Copy() TableColSet { return TableColSet{set: s.set.Copy()} }

// SubsetOf returns true if s is a subset of other.
func (s TableColSet) SubsetOf(other TableColSet) bool {
	return s.set.SubsetOf(other.set)
}

// Intersection returns the intersection between s and other.
func (s TableColSet) Intersection(other TableColSet) TableColSet {
	return TableColSet{set: s.set.Intersection(other.set)}
}

// Difference returns the column IDs in s which are not in other.
func (s TableColSet) Difference(other TableColSet) TableColSet {
	return TableColSet{set: s.set.Difference(other.set)}
}

// Equals returns the column IDs in s are equal to the ones in other.
func (s TableColSet) Equals(other TableColSet) bool {
	return s.set.Equals(other.set)
}

// Ordered returns a slice with all the descpb.ColumnIDs in the set, in
// increasing order.
func (s TableColSet) Ordered() []descpb.ColumnID {
	if s.Empty() {
		return nil
	}
	result := make([]descpb.ColumnID, 0, s.Len())
	s.ForEach(func(i descpb.ColumnID) {
		result = append(result, i)
	})
	return result
}

// UnionWith adds all the columns from rhs to this set.
func (s *TableColSet) UnionWith(rhs TableColSet) { s.set.UnionWith(rhs.set) }

// String returns a list representation of elements. Sequential runs of positive
// numbers are shown as ranges. For example, for the set {1, 2, 3  5, 6, 10},
// the output is "(1-3,5,6,10)".
func (s TableColSet) String() string { return s.set.String() }

// Intersects returns true if s has any elements in common with rhs.
func (s TableColSet) Intersects(rhs TableColSet) bool { return s.set.Intersects(rhs.set) }
