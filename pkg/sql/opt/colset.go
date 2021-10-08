// Copyright 2019 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// ColSet efficiently stores an unordered set of column ids.
type ColSet struct {
	set util.FastIntSet
}

// MakeColSet returns a set initialized with the given values.
func MakeColSet(vals ...ColumnID) ColSet {
	var res ColSet
	for _, v := range vals {
		res.Add(v)
	}
	return res
}

// Add adds a column to the set. No-op if the column is already in the set.
func (s *ColSet) Add(col ColumnID) { s.set.Add(int(col)) }

// Remove removes a column from the set. No-op if the column is not in the set.
func (s *ColSet) Remove(col ColumnID) { s.set.Remove(int(col)) }

// Contains returns true if the set contains the column.
func (s ColSet) Contains(col ColumnID) bool { return s.set.Contains(int(col)) }

// Empty returns true if the set is empty.
func (s ColSet) Empty() bool { return s.set.Empty() }

// Len returns the number of the columns in the set.
func (s ColSet) Len() int { return s.set.Len() }

// Next returns the first value in the set which is >= startVal. If there is no
// such column, the second return value is false.
func (s ColSet) Next(startVal ColumnID) (ColumnID, bool) {
	c, ok := s.set.Next(int(startVal))
	return ColumnID(c), ok
}

// ForEach calls a function for each column in the set (in increasing order).
func (s ColSet) ForEach(f func(col ColumnID)) { s.set.ForEach(func(i int) { f(ColumnID(i)) }) }

// Copy returns a copy of s which can be modified independently.
func (s ColSet) Copy() ColSet { return ColSet{set: s.set.Copy()} }

// UnionWith adds all the columns from rhs to this set.
func (s *ColSet) UnionWith(rhs ColSet) { s.set.UnionWith(rhs.set) }

// Union returns the union of s and rhs as a new set.
func (s ColSet) Union(rhs ColSet) ColSet { return ColSet{set: s.set.Union(rhs.set)} }

// IntersectionWith removes any columns not in rhs from this set.
func (s *ColSet) IntersectionWith(rhs ColSet) { s.set.IntersectionWith(rhs.set) }

// Intersection returns the intersection of s and rhs as a new set.
func (s ColSet) Intersection(rhs ColSet) ColSet { return ColSet{set: s.set.Intersection(rhs.set)} }

// DifferenceWith removes any elements in rhs from this set.
func (s *ColSet) DifferenceWith(rhs ColSet) { s.set.DifferenceWith(rhs.set) }

// Difference returns the elements of s that are not in rhs as a new set.
func (s ColSet) Difference(rhs ColSet) ColSet { return ColSet{set: s.set.Difference(rhs.set)} }

// Intersects returns true if s has any elements in common with rhs.
func (s ColSet) Intersects(rhs ColSet) bool { return s.set.Intersects(rhs.set) }

// Equals returns true if the two sets are identical.
func (s ColSet) Equals(rhs ColSet) bool { return s.set.Equals(rhs.set) }

// SubsetOf returns true if rhs contains all the elements in s.
func (s ColSet) SubsetOf(rhs ColSet) bool { return s.set.SubsetOf(rhs.set) }

// String returns a list representation of elements. Sequential runs of positive
// numbers are shown as ranges. For example, for the set {1, 2, 3  5, 6, 10},
// the output is "(1-3,5,6,10)".
func (s ColSet) String() string { return s.set.String() }

// SingleColumn returns the single column in s. Panics if s does not contain
// exactly one column.
func (s ColSet) SingleColumn() ColumnID {
	if s.Len() != 1 {
		panic(errors.AssertionFailedf("expected a single column but found %d columns", s.Len()))
	}
	col, _ := s.Next(0)
	return col
}

// ToList converts the set to a ColList, in column ID order.
func (s ColSet) ToList() ColList {
	res := make(ColList, 0, s.Len())
	s.ForEach(func(x ColumnID) {
		res = append(res, x)
	})
	return res
}

// TranslateColSet is used to translate a ColSet from one set of column IDs
// to an equivalent set. This is relevant for set operations such as UNION,
// INTERSECT and EXCEPT, and can be used to map a ColSet defined on the left
// relation to an equivalent ColSet on the right relation (or between any two
// relations with a defined column mapping).
//
// For example, suppose we have a UNION with the following column mapping:
//   Left:  1, 2, 3
//   Right: 4, 5, 6
//   Out:   7, 8, 9
//
// Here are some possible calls to TranslateColSet and their results:
//   TranslateColSet(ColSet{1, 2}, Left, Right) -> ColSet{4, 5}
//   TranslateColSet(ColSet{5, 6}, Right, Out)  -> ColSet{8, 9}
//   TranslateColSet(ColSet{9}, Out, Right)     -> ColSet{6}
//
// Any columns in the input set that do not appear in the from list are ignored.
//
// Even when all the columns in the input set appear in the from list, it is
// possible for the input and output sets to have different cardinality.
// Consider the following case:
//
//   SELECT x, x, y FROM xyz UNION SELECT a, b, c FROM abc
//
// TranslateColSet(ColSet{x, y}, {x, x, y}, {a, b, c}) returns ColSet{a, b, c}.
//
// Conversely, TranslateColSet(ColSet{a, b, c}, {a, b, c}, {x, x, y}) returns
// ColSet{x, y}.
func TranslateColSet(colSetIn ColSet, from ColList, to ColList) ColSet {
	var colSetOut ColSet
	for i := range from {
		if colSetIn.Contains(from[i]) {
			colSetOut.Add(to[i])
		}
	}

	return colSetOut
}

// TranslateColSetStrict is a version of TranslateColSet which requires that all
// columns in the input set appear in the from list.
func TranslateColSetStrict(colSetIn ColSet, from ColList, to ColList) ColSet {
	if util.CrdbTestBuild && !colSetIn.SubsetOf(from.ToSet()) {
		panic(errors.AssertionFailedf("input set contains unknown columns"))
	}
	return TranslateColSet(colSetIn, from, to)
}
