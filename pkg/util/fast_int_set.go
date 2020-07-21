// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !fast_int_set_small,!fast_int_set_large

package util

import (
	"math/bits"

	"golang.org/x/tools/container/intsets"
)

// FastIntSet keeps track of a set of integers. It does not perform any
// allocations when the values are small. It is not thread-safe.
type FastIntSet struct {
	// We use a uint64 as long as all elements are between 0 and 63. If we add an
	// element outside of this range, we use both the uint64 and the Sparse; the
	// Sparse then holds all elements of the set and the uint64 provides a fast
	// path in certain cases. We don't just use the latter directly because it's
	// larger and can't be passed around by value.
	small uint64
	large *intsets.Sparse
}

// MakeFastIntSet returns a set initialized with the given values.
func MakeFastIntSet(vals ...int) FastIntSet {
	var res FastIntSet
	for _, v := range vals {
		res.Add(v)
	}
	return res
}

// We store bits for values smaller than this cutoff.
// Note: this can be set to a smaller value, e.g. for testing.
const smallCutoff = 64

func (s *FastIntSet) toLarge() *intsets.Sparse {
	if s.large != nil {
		return s.large
	}
	large := new(intsets.Sparse)
	for i, ok := s.Next(0); ok; i, ok = s.Next(i + 1) {
		large.Insert(i)
	}
	return large
}

// Returns the bit encoded set from 0 to 63, and a flag that indicates whether
// there are elements outside this range.
func (s *FastIntSet) largeToSmall() (small uint64, otherValues bool) {
	if s.large == nil {
		panic("set not large")
	}
	return s.small, s.large.Min() < 0 || s.large.Max() >= smallCutoff
}

// Add adds a value to the set. No-op if the value is already in the set. If the
// large set is not nil and the value is within the range [0, 63], the value is
// added to both the large and small sets.
func (s *FastIntSet) Add(i int) {
	withinSmallBounds := i >= 0 && i < smallCutoff
	if withinSmallBounds {
		s.small |= (1 << uint64(i))
	}
	if !withinSmallBounds && s.large == nil {
		s.large = s.toLarge()
	}
	if s.large != nil {
		s.large.Insert(i)
	}
}

// AddRange adds values 'from' up to 'to' (inclusively) to the set.
// E.g. AddRange(1,5) adds the values 1, 2, 3, 4, 5 to the set.
// 'to' must be >= 'from'.
// AddRange is always more efficient than individual Adds.
func (s *FastIntSet) AddRange(from, to int) {
	if to < from {
		panic("invalid range when adding range to FastIntSet")
	}

	withinSmallBounds := from >= 0 && to < smallCutoff
	if withinSmallBounds && s.large == nil {
		nValues := to - from + 1
		s.small |= (1<<uint64(nValues) - 1) << uint64(from)
	} else {
		for i := from; i <= to; i++ {
			s.Add(i)
		}
	}
}

// Remove removes a value from the set. No-op if the value is not in the set.
func (s *FastIntSet) Remove(i int) {
	if i >= 0 && i < smallCutoff {
		s.small &= ^(1 << uint64(i))
	}
	if s.large != nil {
		s.large.Remove(i)
	}
}

// Contains returns true if the set contains the value.
func (s FastIntSet) Contains(i int) bool {
	if i >= 0 && i < smallCutoff {
		return (s.small & (1 << uint64(i))) != 0
	}
	if s.large != nil {
		return s.large.Has(i)
	}
	return false
}

// Empty returns true if the set is empty.
func (s FastIntSet) Empty() bool {
	return s.small == 0 && (s.large == nil || s.large.IsEmpty())
}

// Len returns the number of the elements in the set.
func (s FastIntSet) Len() int {
	if s.large == nil {
		return bits.OnesCount64(s.small)
	}
	return s.large.Len()
}

// Next returns the first value in the set which is >= startVal. If there is no
// value, the second return value is false.
func (s FastIntSet) Next(startVal int) (int, bool) {
	if startVal < smallCutoff {
		if startVal < 0 {
			startVal = 0
		}

		if ntz := bits.TrailingZeros64(s.small >> uint64(startVal)); ntz < 64 {
			return startVal + ntz, true
		}
	}
	if s.large != nil {
		res := s.large.LowerBound(startVal)
		return res, res != intsets.MaxInt
	}
	return intsets.MaxInt, false
}

// ForEach calls a function for each value in the set (in increasing order).
func (s FastIntSet) ForEach(f func(i int)) {
	if s.large != nil {
		for x := s.large.Min(); x != intsets.MaxInt; x = s.large.LowerBound(x + 1) {
			f(x)
		}
		return
	}
	for v := s.small; v != 0; {
		i := bits.TrailingZeros64(v)
		f(i)
		v &^= 1 << uint(i)
	}
}

// Ordered returns a slice with all the integers in the set, in increasing order.
func (s FastIntSet) Ordered() []int {
	if s.Empty() {
		return nil
	}
	if s.large != nil {
		return s.large.AppendTo([]int(nil))
	}
	result := make([]int, 0, s.Len())
	s.ForEach(func(i int) {
		result = append(result, i)
	})
	return result
}

// Copy returns a copy of s which can be modified independently.
func (s FastIntSet) Copy() FastIntSet {
	var c FastIntSet
	c.small = s.small
	if s.large != nil {
		c.large = new(intsets.Sparse)
		c.large.Copy(s.large)
	}
	return c
}

// CopyFrom sets the receiver to a copy of other, which can then be modified
// independently.
func (s *FastIntSet) CopyFrom(other FastIntSet) {
	s.small = other.small
	if other.large != nil {
		if s.large == nil {
			s.large = new(intsets.Sparse)
		}
		s.large.Copy(other.large)
	} else {
		if s.large != nil {
			s.large.Clear()
		}
	}
}

// UnionWith adds all the elements from rhs to this set.
func (s *FastIntSet) UnionWith(rhs FastIntSet) {
	s.small |= rhs.small
	if s.large == nil && rhs.large == nil {
		// Fast path.
		return
	}

	if s.large == nil {
		s.large = s.toLarge()
	}
	if rhs.large == nil {
		for i, ok := rhs.Next(0); ok; i, ok = rhs.Next(i + 1) {
			s.large.Insert(i)
		}
	} else {
		s.large.UnionWith(rhs.large)
	}
}

// Union returns the union of s and rhs as a new set.
func (s FastIntSet) Union(rhs FastIntSet) FastIntSet {
	r := s.Copy()
	r.UnionWith(rhs)
	return r
}

// IntersectionWith removes any elements not in rhs from this set.
func (s *FastIntSet) IntersectionWith(rhs FastIntSet) {
	s.small &= rhs.small
	if rhs.large == nil {
		s.large = nil
	}
	if s.large == nil {
		// Fast path.
		return
	}

	s.large.IntersectionWith(rhs.toLarge())
}

// Intersection returns the intersection of s and rhs as a new set.
func (s FastIntSet) Intersection(rhs FastIntSet) FastIntSet {
	r := s.Copy()
	r.IntersectionWith(rhs)
	return r
}

// Intersects returns true if s has any elements in common with rhs.
func (s FastIntSet) Intersects(rhs FastIntSet) bool {
	if (s.small & rhs.small) != 0 {
		return true
	}
	if s.large == nil || rhs.large == nil {
		return false
	}
	return s.large.Intersects(rhs.toLarge())
}

// DifferenceWith removes any elements in rhs from this set.
func (s *FastIntSet) DifferenceWith(rhs FastIntSet) {
	s.small &^= rhs.small
	if s.large == nil {
		// Fast path
		return
	}
	s.large.DifferenceWith(rhs.toLarge())
}

// Difference returns the elements of s that are not in rhs as a new set.
func (s FastIntSet) Difference(rhs FastIntSet) FastIntSet {
	r := s.Copy()
	r.DifferenceWith(rhs)
	return r
}

// Equals returns true if the two sets are identical.
func (s FastIntSet) Equals(rhs FastIntSet) bool {
	if s.large == nil && rhs.large == nil {
		return s.small == rhs.small
	}
	if s.large != nil && rhs.large != nil {
		return s.large.Equals(rhs.large)
	}
	// One set is "large" and one is "small". They might still be equal (the large
	// set could have had a large element added and then removed).
	var extraVals bool
	s1 := s.small
	s2 := rhs.small
	if s.large != nil {
		s1, extraVals = s.largeToSmall()
	} else {
		s2, extraVals = rhs.largeToSmall()
	}
	return !extraVals && s1 == s2
}

// SubsetOf returns true if rhs contains all the elements in s.
func (s FastIntSet) SubsetOf(rhs FastIntSet) bool {
	if s.large == nil {
		return (s.small & rhs.small) == s.small
	}
	if s.large != nil && rhs.large != nil {
		return s.large.SubsetOf(rhs.large)
	}
	// s is "large" and rhs is "small".
	_, hasExtra := s.largeToSmall()
	if hasExtra {
		return false
	}
	// s is "large", but has no elements outside of the range [0, 63]. This can
	// happen if elements outside of the range are removed.
	return (s.small & rhs.small) == s.small
}

// Shift generates a new set which contains elements i+delta for elements i in
// the original set.
func (s *FastIntSet) Shift(delta int) FastIntSet {
	if s.large == nil {
		// Fast path.
		if delta > 0 {
			if bits.LeadingZeros64(s.small)-(64-smallCutoff) >= delta {
				return FastIntSet{small: s.small << uint32(delta)}
			}
		} else {
			if bits.TrailingZeros64(s.small) >= -delta {
				return FastIntSet{small: s.small >> uint32(-delta)}
			}
		}
	}
	// Do the slow thing.
	var result FastIntSet
	s.ForEach(func(i int) {
		result.Add(i + delta)
	})
	return result
}
