// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !fast_int_set_small && !fast_int_set_large
// +build !fast_int_set_small,!fast_int_set_large

package util

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/bits"

	"github.com/cockroachdb/errors"
	"golang.org/x/tools/container/intsets"
)

// FastIntSet keeps track of a set of integers. It does not perform any
// allocations when the values are small. It is not thread-safe.
type FastIntSet struct {
	small bitmap
	large *intsets.Sparse
}

// We maintain a bitmap for small element values (specifically 0 to
// smallCutoff-1). When this bitmap is sufficient, we avoid allocating the
// `Sparse` set.  Even when we have to allocate the `Sparse` set, we still
// maintain the bitmap as it can provide a fast path for certain operations.
// Note: this can be set to a smaller value, e.g. for testing.
const smallCutoff = 128

// bitmap implements a bitmap of size smallCutoff.
type bitmap struct {
	// We don't use an array because that makes Go always keep the struct on the
	// stack (see https://github.com/golang/go/issues/24416).
	lo, hi uint64
}

// MakeFastIntSet returns a set initialized with the given values.
func MakeFastIntSet(vals ...int) FastIntSet {
	var res FastIntSet
	for _, v := range vals {
		res.Add(v)
	}
	return res
}

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

// fitsInSmall returns whether all elements in this set are between 0 and
// smallCutoff.
func (s *FastIntSet) fitsInSmall() bool {
	if s.large == nil {
		return true
	}
	// It is possible that we have a large set allocated but all elements still
	// fit the cutoff. This can happen if the set used to contain other elements.
	return s.large.Min() >= 0 && s.large.Max() < smallCutoff
}

// Add adds a value to the set. No-op if the value is already in the set. If the
// large set is not nil and the value is within the range [0, 63], the value is
// added to both the large and small sets.
func (s *FastIntSet) Add(i int) {
	withinSmallBounds := i >= 0 && i < smallCutoff
	if withinSmallBounds {
		s.small.Set(i)
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

	if s.large == nil && from >= 0 && to < smallCutoff {
		s.small.SetRange(from, to)
	} else {
		for i := from; i <= to; i++ {
			s.Add(i)
		}
	}
}

// Remove removes a value from the set. No-op if the value is not in the set.
func (s *FastIntSet) Remove(i int) {
	if i >= 0 && i < smallCutoff {
		s.small.Unset(i)
	}
	if s.large != nil {
		s.large.Remove(i)
	}
}

// Contains returns true if the set contains the value.
func (s FastIntSet) Contains(i int) bool {
	if i >= 0 && i < smallCutoff {
		return s.small.IsSet(i)
	}
	if s.large != nil {
		return s.large.Has(i)
	}
	return false
}

// Empty returns true if the set is empty.
func (s FastIntSet) Empty() bool {
	return s.small == bitmap{} && (s.large == nil || s.large.IsEmpty())
}

// Len returns the number of the elements in the set.
func (s FastIntSet) Len() int {
	if s.large == nil {
		return s.small.OnesCount()
	}
	return s.large.Len()
}

// Next returns the first value in the set which is >= startVal. If there is no
// value, the second return value is false.
func (s FastIntSet) Next(startVal int) (int, bool) {
	if startVal < 0 && s.large == nil {
		startVal = 0
	}
	if startVal >= 0 && startVal < smallCutoff {
		if nextVal, ok := s.small.Next(startVal); ok {
			return nextVal, true
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
	if !s.fitsInSmall() {
		for x := s.large.Min(); x != intsets.MaxInt; x = s.large.LowerBound(x + 1) {
			f(x)
		}
		return
	}
	for v := s.small.lo; v != 0; {
		i := bits.TrailingZeros64(v)
		f(i)
		v &^= 1 << uint(i)
	}
	for v := s.small.hi; v != 0; {
		i := bits.TrailingZeros64(v)
		f(64 + i)
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
	s.small.UnionWith(rhs.small)
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
	s.small.IntersectionWith(rhs.small)
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
	if s.small.Intersects(rhs.small) {
		return true
	}
	if s.large == nil || rhs.large == nil {
		return false
	}
	return s.large.Intersects(rhs.toLarge())
}

// DifferenceWith removes any elements in rhs from this set.
func (s *FastIntSet) DifferenceWith(rhs FastIntSet) {
	s.small.DifferenceWith(rhs.small)
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
	if s.small != rhs.small {
		return false
	}
	if s.fitsInSmall() {
		// We already know that the `small` fields are equal. We just have to make
		// sure that the other set also has no large elements.
		return rhs.fitsInSmall()
	}
	// We know that s has large elements.
	return rhs.large != nil && s.large.Equals(rhs.large)
}

// SubsetOf returns true if rhs contains all the elements in s.
func (s FastIntSet) SubsetOf(rhs FastIntSet) bool {
	if s.fitsInSmall() {
		return s.small.SubsetOf(rhs.small)
	}
	if rhs.fitsInSmall() {
		// s doesn't fit in small and rhs does.
		return false
	}
	return s.large.SubsetOf(rhs.large)
}

// Encode the set and write it to a bytes.Buffer using binary.varint byte
// encoding.
//
// This method cannot be used if the set contains negative elements.
//
// If the set has only elements in the range [0, 63], we encode a 0 followed by
// a 64-bit bitmap. Otherwise, we encode a length followed by each element.
//
// WARNING: this is used by plan gists, so if this encoding changes,
// explain.gistVersion needs to be bumped.
func (s *FastIntSet) Encode(buf *bytes.Buffer) error {
	if s.large != nil && s.large.Min() < 0 {
		return errors.AssertionFailedf("Encode used with negative elements")
	}

	// This slice should stay on stack. We only need enough bytes to encode a 0
	// and then an arbitrary 64-bit integer.
	//gcassert:noescape
	tmp := make([]byte, binary.MaxVarintLen64+1)

	if s.small.hi == 0 && s.fitsInSmall() {
		n := binary.PutUvarint(tmp, 0)
		n += binary.PutUvarint(tmp[n:], s.small.lo)
		buf.Write(tmp[:n])
	} else {
		n := binary.PutUvarint(tmp, uint64(s.Len()))
		buf.Write(tmp[:n])
		for i, ok := s.Next(0); ok; i, ok = s.Next(i + 1) {
			n := binary.PutUvarint(tmp, uint64(i))
			buf.Write(tmp[:n])
		}
	}
	return nil
}

// Decode does the opposite of Encode. The contents of the receiver are
// overwritten.
func (s *FastIntSet) Decode(br io.ByteReader) error {
	length, err := binary.ReadUvarint(br)
	if err != nil {
		return err
	}
	*s = FastIntSet{}

	if length == 0 {
		// Special case: a 64-bit bitmap is encoded directly.
		val, err := binary.ReadUvarint(br)
		if err != nil {
			return err
		}
		s.small.lo = val
	} else {
		for i := 0; i < int(length); i++ {
			elem, err := binary.ReadUvarint(br)
			if err != nil {
				*s = FastIntSet{}
				return err
			}
			s.Add(int(elem))
		}
	}
	return nil
}

func (v bitmap) IsSet(i int) bool {
	w := v.lo
	if i >= 64 {
		w = v.hi
	}
	return w&(1<<uint64(i&63)) != 0
}

func (v *bitmap) Set(i int) {
	if i < 64 {
		v.lo |= (1 << uint64(i))
	} else {
		v.hi |= (1 << uint64(i&63))
	}
}

func (v *bitmap) Unset(i int) {
	if i < 64 {
		v.lo &= ^(1 << uint64(i))
	} else {
		v.hi &= ^(1 << uint64(i&63))
	}
}

func (v *bitmap) SetRange(from, to int) {
	mask := func(from, to int) uint64 {
		return (1<<(to-from+1) - 1) << from
	}
	switch {
	case to < 64:
		v.lo |= mask(from, to)
	case from >= 64:
		v.hi |= mask(from&63, to&63)
	default:
		v.lo |= mask(from, 63)
		v.hi |= mask(0, to&63)
	}
}

func (v *bitmap) UnionWith(other bitmap) {
	v.lo |= other.lo
	v.hi |= other.hi
}

func (v *bitmap) IntersectionWith(other bitmap) {
	v.lo &= other.lo
	v.hi &= other.hi
}

func (v bitmap) Intersects(other bitmap) bool {
	return ((v.lo & other.lo) | (v.hi & other.hi)) != 0
}

func (v *bitmap) DifferenceWith(other bitmap) {
	v.lo &^= other.lo
	v.hi &^= other.hi
}

func (v bitmap) SubsetOf(other bitmap) bool {
	return (v.lo&other.lo == v.lo) && (v.hi&other.hi == v.hi)
}

func (v bitmap) OnesCount() int {
	return bits.OnesCount64(v.lo) + bits.OnesCount64(v.hi)
}

func (v bitmap) Next(startVal int) (nextVal int, ok bool) {
	if startVal < 64 {
		if ntz := bits.TrailingZeros64(v.lo >> uint64(startVal)); ntz < 64 {
			// Found next element in the low word.
			return startVal + ntz, true
		}
		startVal = 64
	}
	// Check high word.
	if ntz := bits.TrailingZeros64(v.hi >> uint64(startVal&63)); ntz < 64 {
		return startVal + ntz, true
	}
	return -1, false
}
