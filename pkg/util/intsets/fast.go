// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !fast_int_set_small && !fast_int_set_large

package intsets

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/bits"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/base64"
	"github.com/cockroachdb/errors"
)

// Fast keeps track of a set of integers. It does not perform any
// allocations when the values are in the range [0, smallCutoff). It is not
// thread-safe.
type Fast struct {
	// small is a bitmap that stores values in the range [0, smallCutoff).
	small bitmap
	// large is only allocated if values are added to the set that are not in
	// the range [0, smallCutoff).
	large *Sparse
}

// MakeFast returns a set initialized with the given values.
func MakeFast(vals ...int) Fast {
	var res Fast
	for _, v := range vals {
		res.Add(v)
	}
	return res
}

// fitsInSmall returns whether all elements in this set are between 0 and
// smallCutoff.
//
//gcassert:inline
func (s *Fast) fitsInSmall() bool {
	return s.large == nil || s.large.Empty()
}

// Add adds a value to the set. No-op if the value is already in the set. If the
// large set is not nil and the value is within the range [0, 63], the value is
// added to both the large and small sets.
func (s *Fast) Add(i int) {
	if i >= 0 && i < smallCutoff {
		s.small.Set(i)
		return
	}
	if s.large == nil {
		s.large = new(Sparse)
	}
	s.large.Add(i)
}

// AddRange adds values 'from' up to 'to' (inclusively) to the set.
// E.g. AddRange(1,5) adds the values 1, 2, 3, 4, 5 to the set.
// 'to' must be >= 'from'.
// AddRange is always more efficient than individual Adds.
func (s *Fast) AddRange(from, to int) {
	if to < from {
		panic("invalid range when adding range to Fast")
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
func (s *Fast) Remove(i int) {
	if i >= 0 && i < smallCutoff {
		s.small.Unset(i)
		return
	}
	if s.large != nil {
		s.large.Remove(i)
	}
}

// Contains returns true if the set contains the value.
func (s Fast) Contains(i int) bool {
	if i >= 0 && i < smallCutoff {
		return s.small.IsSet(i)
	}
	if s.large != nil {
		return s.large.Contains(i)
	}
	return false
}

// Empty returns true if the set is empty.
func (s Fast) Empty() bool {
	return s.small == bitmap{} && (s.large == nil || s.large.Empty())
}

// Len returns the number of the elements in the set.
func (s Fast) Len() int {
	l := s.small.OnesCount()
	if s.large != nil {
		l += s.large.Len()
	}
	return l
}

// Next returns the first value in the set which is >= startVal. If there is no
// value, the second return value is false.
func (s Fast) Next(startVal int) (int, bool) {
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
		res := s.large.LowerBound(startVal)
		return res, res != MaxInt
	}
	return MaxInt, false
}

// ForEach calls a function for each value in the set (in increasing order).
func (s Fast) ForEach(f func(i int)) {
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
		for x := s.large.LowerBound(0); x != MaxInt; x = s.large.LowerBound(x + 1) {
			f(x)
		}
	}
}

// Ordered returns a slice with all the integers in the set, in increasing order.
func (s Fast) Ordered() []int {
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
func (s Fast) Copy() Fast {
	var c Fast
	c.small = s.small
	if s.large != nil && !s.large.Empty() {
		c.large = new(Sparse)
		c.large.Copy(s.large)
	}
	return c
}

// CopyFrom sets the receiver to a copy of other, which can then be modified
// independently.
func (s *Fast) CopyFrom(other Fast) {
	s.small = other.small
	if other.large != nil && !other.large.Empty() {
		if s.large == nil {
			s.large = new(Sparse)
		}
		s.large.Copy(other.large)
	} else {
		if s.large != nil {
			s.large.Clear()
		}
	}
}

// UnionWith adds all the elements from rhs to this set.
func (s *Fast) UnionWith(rhs Fast) {
	s.small.UnionWith(rhs.small)
	if rhs.large == nil || rhs.large.Empty() {
		// Fast path.
		return
	}
	if s.large == nil {
		s.large = new(Sparse)
	}
	s.large.UnionWith(rhs.large)
}

// Union returns the union of s and rhs as a new set.
func (s Fast) Union(rhs Fast) Fast {
	r := s.Copy()
	r.UnionWith(rhs)
	return r
}

// IntersectionWith removes any elements not in rhs from this set.
func (s *Fast) IntersectionWith(rhs Fast) {
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
func (s Fast) Intersection(rhs Fast) Fast {
	r := s.Copy()
	r.IntersectionWith(rhs)
	return r
}

// Intersects returns true if s has any elements in common with rhs.
func (s Fast) Intersects(rhs Fast) bool {
	if s.small.Intersects(rhs.small) {
		return true
	}
	if s.large == nil || rhs.large == nil {
		return false
	}
	return s.large.Intersects(rhs.large)
}

// DifferenceWith removes any elements in rhs from this set.
func (s *Fast) DifferenceWith(rhs Fast) {
	s.small.DifferenceWith(rhs.small)
	if s.large == nil || rhs.large == nil {
		// Fast path
		return
	}
	s.large.DifferenceWith(rhs.large)
}

// Difference returns the elements of s that are not in rhs as a new set.
func (s Fast) Difference(rhs Fast) Fast {
	r := s.Copy()
	r.DifferenceWith(rhs)
	return r
}

// Equals returns true if the two sets are identical.
func (s Fast) Equals(rhs Fast) bool {
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
func (s Fast) SubsetOf(rhs Fast) bool {
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
func (s *Fast) Encode(buf *bytes.Buffer) error {
	return s.encodeImpl(buf, nil, nil)
}

// EncodeBase64 is similar to Encode. It writes the encoded set to enc. It also
// adds each pre-base64-encoded byte to hash.
//
// Closures or interfaces could be used to merge both methods into one, but they
// are intentionally avoided to prevent extra allocations of temporary buffers
// used during encoding.
//
// WARNING: this is used by plan gists, so if this encoding changes,
// explain.gistVersion needs to be bumped.
func (s *Fast) EncodeBase64(enc *base64.Encoder, hash *util.FNV64) error {
	return s.encodeImpl(nil, enc, hash)
}

func (s *Fast) encodeImpl(buf *bytes.Buffer, enc *base64.Encoder, hash *util.FNV64) error {
	if s.large != nil && s.large.Min() < 0 {
		return errors.AssertionFailedf("Encode used with negative elements")
	}

	write := func(b []byte) {
		if buf != nil {
			buf.Write(b)
		} else {
			enc.Write(b)
			for i := range b {
				hash.Add(uint64(b[i]))
			}
		}
	}

	// This slice should stay on stack. We only need enough bytes to encode a 0
	// and then an arbitrary 64-bit integer.
	//gcassert:noescape
	tmp := make([]byte, binary.MaxVarintLen64+1)

	var n int
	if s.small.hi == 0 && s.fitsInSmall() {
		n = binary.PutUvarint(tmp, 0)
		n += binary.PutUvarint(tmp[n:], s.small.lo)
		write(tmp[:n])
	} else {
		n = binary.PutUvarint(tmp, uint64(s.Len()))
		write(tmp[:n])
		for i, ok := s.Next(0); ok; i, ok = s.Next(i + 1) {
			n := binary.PutUvarint(tmp, uint64(i))
			write(tmp[:n])
		}
	}
	return nil
}

// Decode does the opposite of Encode. The contents of the receiver are
// overwritten.
func (s *Fast) Decode(br io.ByteReader) error {
	length, err := binary.ReadUvarint(br)
	if err != nil {
		return err
	}
	*s = Fast{}

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
				*s = Fast{}
				return err
			}
			s.Add(int(elem))
		}
	}
	return nil
}
