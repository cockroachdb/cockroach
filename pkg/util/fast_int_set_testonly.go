// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build fast_int_set_small fast_int_set_large

// This file implements two variants of FastIntSet used for testing which always
// behaves like in either the "small" or "large" case (depending on
// fastIntSetAlwaysSmall). Tests that exhibit a difference when using one of
// these variants indicates a bug.

package util

import "golang.org/x/tools/container/intsets"

// FastIntSet keeps track of a set of integers. It does not perform any
// allocations when the values are small. It is not thread-safe.
type FastIntSet struct {
	// Used to keep the size of the struct the same.
	_ uint64
	s *intsets.Sparse
}

// MakeFastIntSet returns a set initialized with the given values.
func MakeFastIntSet(vals ...int) FastIntSet {
	var res FastIntSet
	for _, v := range vals {
		res.Add(v)
	}
	return res
}

func (s *FastIntSet) prepareForMutation() {
	if s.s == nil {
		s.s = &intsets.Sparse{}
	} else if fastIntSetAlwaysSmall {
		// We always make a full copy to prevent any aliasing; this simulates the
		// semantics of the "small" regime of FastIntSet.
		*s = s.Copy()
	}
}

// Add adds a value to the set. No-op if the value is already in the set.
func (s *FastIntSet) Add(i int) {
	s.prepareForMutation()
	s.s.Insert(i)
}

// AddRange adds values 'from' up to 'to' (inclusively) to the set.
// E.g. AddRange(1,5) adds the values 1, 2, 3, 4, 5 to the set.
// 'to' must be >= 'from'.
// AddRange is always more efficient than individual Adds.
func (s *FastIntSet) AddRange(from, to int) {
	s.prepareForMutation()
	for i := from; i <= to; i++ {
		s.s.Insert(i)
	}
}

// Remove removes a value from the set. No-op if the value is not in the set.
func (s *FastIntSet) Remove(i int) {
	s.prepareForMutation()
	s.s.Remove(i)
}

// Contains returns true if the set contains the value.
func (s FastIntSet) Contains(i int) bool {
	return s.s != nil && s.s.Has(i)
}

// Empty returns true if the set is empty.
func (s FastIntSet) Empty() bool {
	return s.s == nil || s.s.IsEmpty()
}

// Len returns the number of the elements in the set.
func (s FastIntSet) Len() int {
	if s.s == nil {
		return 0
	}
	return s.s.Len()
}

// Next returns the first value in the set which is >= startVal. If there is no
// value, the second return value is false.
func (s FastIntSet) Next(startVal int) (int, bool) {
	if s.s == nil {
		return intsets.MaxInt, false
	}
	res := s.s.LowerBound(startVal)
	return res, res != intsets.MaxInt
}

// ForEach calls a function for each value in the set (in increasing order).
func (s FastIntSet) ForEach(f func(i int)) {
	if s.s == nil {
		return
	}
	for x := s.s.Min(); x != intsets.MaxInt; x = s.s.LowerBound(x + 1) {
		f(x)
	}
}

// Ordered returns a slice with all the integers in the set, in increasing order.
func (s FastIntSet) Ordered() []int {
	if s.Empty() {
		return nil
	}
	return s.s.AppendTo([]int(nil))
}

// Copy returns a copy of s which can be modified independently.
func (s FastIntSet) Copy() FastIntSet {
	n := &intsets.Sparse{}
	if s.s != nil {
		n.Copy(s.s)
	}
	return FastIntSet{s: n}
}

// CopyFrom sets the receiver to a copy of other, which can then be modified
// independently.
func (s *FastIntSet) CopyFrom(other FastIntSet) {
	*s = other.Copy()
}

// UnionWith adds all the elements from rhs to this set.
func (s *FastIntSet) UnionWith(rhs FastIntSet) {
	if rhs.s == nil {
		return
	}
	s.prepareForMutation()
	s.s.UnionWith(rhs.s)
}

// Union returns the union of s and rhs as a new set.
func (s FastIntSet) Union(rhs FastIntSet) FastIntSet {
	r := s.Copy()
	r.UnionWith(rhs)
	return r
}

// IntersectionWith removes any elements not in rhs from this set.
func (s *FastIntSet) IntersectionWith(rhs FastIntSet) {
	if rhs.s == nil {
		*s = FastIntSet{}
		return
	}
	s.prepareForMutation()
	s.s.IntersectionWith(rhs.s)
}

// Intersection returns the intersection of s and rhs as a new set.
func (s FastIntSet) Intersection(rhs FastIntSet) FastIntSet {
	r := s.Copy()
	r.IntersectionWith(rhs)
	return r
}

// Intersects returns true if s has any elements in common with rhs.
func (s FastIntSet) Intersects(rhs FastIntSet) bool {
	if s.s == nil || rhs.s == nil {
		return false
	}
	return s.s.Intersects(rhs.s)
}

// DifferenceWith removes any elements in rhs from this set.
func (s *FastIntSet) DifferenceWith(rhs FastIntSet) {
	if rhs.s == nil {
		return
	}
	s.prepareForMutation()
	s.s.DifferenceWith(rhs.s)
}

// Difference returns the elements of s that are not in rhs as a new set.
func (s FastIntSet) Difference(rhs FastIntSet) FastIntSet {
	r := s.Copy()
	r.DifferenceWith(rhs)
	return r
}

// Equals returns true if the two sets are identical.
func (s FastIntSet) Equals(rhs FastIntSet) bool {
	if s.Empty() || rhs.Empty() {
		return s.Empty() == rhs.Empty()
	}
	return s.s.Equals(rhs.s)
}

// SubsetOf returns true if rhs contains all the elements in s.
func (s FastIntSet) SubsetOf(rhs FastIntSet) bool {
	if s.Empty() {
		return true
	}
	if rhs.s == nil {
		return false
	}
	return s.s.SubsetOf(rhs.s)
}

// Shift generates a new set which contains elements i+delta for elements i in
// the original set.
func (s *FastIntSet) Shift(delta int) FastIntSet {
	n := &intsets.Sparse{}
	s.ForEach(func(i int) {
		n.Insert(i + delta)
	})
	return FastIntSet{s: n}
}

// This is defined to allow the test to build.
const smallCutoff = 10
