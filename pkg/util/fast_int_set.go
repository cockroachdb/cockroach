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
// allocations when the values are in the range [0, smallCutoff). It is not
// thread-safe.
type FastIntSet struct {
	// small is a bitmap that stores values in the range [0, smallCutoff).
	small bitmap
	// large is only allocated if values are added to the set that are not in
	// the range [0, smallCutoff).
	//
	// The implementation of intsets.Sparse is a circular, doubly-linked list of
	// blocks. Each block contains a 256-bit bitmap and an offset. A block with
	// offset=n contains a value n+i if the i-th bit of the bitmap is set. Block
	// offsets are always divisible by 256.
	//
	// For example, here is a diagram of the set {0, 1, 256, 257, 512}, where
	// each block is denoted by {offset, bitmap}:
	//
	//   ---> {0, ..011} <----> {256, ..011} <----> {512, ..001} <---
	//   |                                                          |
	//   ------------------------------------------------------------
	//
	// FastIntSet stores only values outside the range [0, smallCutoff) in
	// large. Values less than 0 are stored in large as-is. For values greater
	// than or equal to smallCutoff, we subtract by smallCutoff before storing
	// them in large. When they are retrieved from large, we add smallCutoff to
	// get the original value. For example, if 300 is added to the FastIntSet,
	// it would be added to large as the value (300 - smallCutoff).
	//
	// This scheme better utilizes the block with offset=0 compared to an
	// alternative implementation where small and large contain overlapping
	// values. For example, consider the set {0, 200, 300}. In the overlapping
	// implementation, two blocks would be allocated: a block with offset=0
	// would store 0 and 200, and a block with offset=256 would store 300. By
	// omitting values in the range [0, smallCutoff) in large, only one block is
	// allocated: a block with offset=0 that stores 200-smallCutoff and
	// 300-smallCutoff.
	large *intsets.Sparse
}

// smallCutoff is the size of the small bitmap.
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

// fitsInSmall returns whether all elements in this set are between 0 and
// smallCutoff.
func (s *FastIntSet) fitsInSmall() bool {
	return s.large == nil || s.large.IsEmpty()
}

// Add adds a value to the set. No-op if the value is already in the set. If the
// large set is not nil and the value is within the range [0, 63], the value is
// added to both the large and small sets.
func (s *FastIntSet) Add(i int) {
	if i >= 0 && i < smallCutoff {
		s.small.Set(i)
		return
	}
	if s.large == nil {
		s.large = new(intsets.Sparse)
	}
	if i >= smallCutoff {
		i -= smallCutoff
	}
	s.large.Insert(i)
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
		return
	}
	if s.large != nil {
		if i >= smallCutoff {
			i -= smallCutoff
		}
		s.large.Remove(i)
	}
}

// Contains returns true if the set contains the value.
func (s FastIntSet) Contains(i int) bool {
	if i >= 0 && i < smallCutoff {
		return s.small.IsSet(i)
	}
	if s.large != nil {
		if i >= smallCutoff {
			i -= smallCutoff
		}
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
	l := s.small.OnesCount()
	if s.large != nil {
		l += s.large.Len()
	}
	return l
}

// Next returns the first value in the set which is >= startVal. If there is no
// value, the second return value is false.
func (s FastIntSet) Next(startVal int) (int, bool) {
	if startVal < 0 && s.large != nil {
		if res := s.large.LowerBound(startVal); res < 0 {
			return res, true
		}
	}
	if startVal < 0 {
		// Negative values are must be in s.large.
		startVal = 0
	}
	if startVal < smallCutoff {
		if nextVal, ok := s.small.Next(startVal); ok {
			return nextVal, true
		}
	}
	if s.large != nil {
		startVal -= smallCutoff
		if startVal < 0 {
			// We already searched for negative values in large above.
			startVal = 0
		}
		res := s.large.LowerBound(startVal)
		return res + smallCutoff, res != intsets.MaxInt
	}
	return intsets.MaxInt, false
}

// ForEach calls a function for each value in the set (in increasing order).
func (s FastIntSet) ForEach(f func(i int)) {
	if !s.fitsInSmall() {
		for x := s.large.Min(); x < 0; x = s.large.LowerBound(x + 1) {
			f(x)
		}
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
	if !s.fitsInSmall() {
		for x := s.large.LowerBound(0); x != intsets.MaxInt; x = s.large.LowerBound(x + 1) {
			f(x + smallCutoff)
		}
	}
}

// Ordered returns a slice with all the integers in the set, in increasing order.
func (s FastIntSet) Ordered() []int {
	if s.Empty() {
		return nil
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
	if s.large != nil && !s.large.IsEmpty() {
		c.large = new(intsets.Sparse)
		c.large.Copy(s.large)
	}
	return c
}

// CopyFrom sets the receiver to a copy of other, which can then be modified
// independently.
func (s *FastIntSet) CopyFrom(other FastIntSet) {
	s.small = other.small
	if other.large != nil && !other.large.IsEmpty() {
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
	if rhs.large == nil || rhs.large.IsEmpty() {
		// Fast path.
		return
	}
	if s.large == nil {
		s.large = new(intsets.Sparse)
	}
	s.large.UnionWith(rhs.large)
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
	s.large.IntersectionWith(rhs.large)
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
	return s.large.Intersects(rhs.large)
}

// DifferenceWith removes any elements in rhs from this set.
func (s *FastIntSet) DifferenceWith(rhs FastIntSet) {
	s.small.DifferenceWith(rhs.small)
	if s.large == nil || rhs.large == nil {
		// Fast path
		return
	}
	s.large.DifferenceWith(rhs.large)
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
	return s.small.SubsetOf(rhs.small) && s.large.SubsetOf(rhs.large)
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
