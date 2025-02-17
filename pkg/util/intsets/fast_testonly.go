// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build fast_int_set_small || fast_int_set_large

// This file implements two variants of Fast used for testing which always
// behaves like in either the "small" or "large" case (depending on
// fastIntSetAlwaysSmall). Tests that exhibit a difference when using one of
// these variants indicates a bug.

package intsets

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/base64"
	"github.com/cockroachdb/errors"
)

// Fast keeps track of a set of integers. It does not perform any
// allocations when the values are small. It is not thread-safe.
type Fast struct {
	// Used to keep the size of the struct the same.
	_ struct{ lo, hi uint64 }
	s *Sparse
}

// MakeFast returns a set initialized with the given values.
func MakeFast(vals ...int) Fast {
	var res Fast
	for _, v := range vals {
		res.Add(v)
	}
	return res
}

func (s *Fast) prepareForMutation() {
	if s.s == nil {
		s.s = &Sparse{}
	} else if fastIntSetAlwaysSmall {
		// We always make a full copy to prevent any aliasing; this simulates the
		// semantics of the "small" regime of Fast.
		*s = s.Copy()
	}
}

// Add adds a value to the set. No-op if the value is already in the set.
func (s *Fast) Add(i int) {
	s.prepareForMutation()
	s.s.Add(i)
}

// AddRange adds values 'from' up to 'to' (inclusively) to the set.
// E.g. AddRange(1,5) adds the values 1, 2, 3, 4, 5 to the set.
// 'to' must be >= 'from'.
// AddRange is always more efficient than individual Adds.
func (s *Fast) AddRange(from, to int) {
	s.prepareForMutation()
	for i := from; i <= to; i++ {
		s.s.Add(i)
	}
}

// Remove removes a value from the set. No-op if the value is not in the set.
func (s *Fast) Remove(i int) {
	s.prepareForMutation()
	s.s.Remove(i)
}

// Contains returns true if the set contains the value.
func (s Fast) Contains(i int) bool {
	return s.s != nil && s.s.Contains(i)
}

// Empty returns true if the set is empty.
func (s Fast) Empty() bool {
	return s.s == nil || s.s.Empty()
}

// Len returns the number of the elements in the set.
func (s Fast) Len() int {
	if s.s == nil {
		return 0
	}
	return s.s.Len()
}

// Next returns the first value in the set which is >= startVal. If there is no
// value, the second return value is false.
func (s Fast) Next(startVal int) (int, bool) {
	if s.s == nil {
		return MaxInt, false
	}
	res := s.s.LowerBound(startVal)
	return res, res != MaxInt
}

// ForEach calls a function for each value in the set (in increasing order).
func (s Fast) ForEach(f func(i int)) {
	if s.s == nil {
		return
	}
	for x := s.s.Min(); x != MaxInt; x = s.s.LowerBound(x + 1) {
		f(x)
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
	n := &Sparse{}
	if s.s != nil {
		n.Copy(s.s)
	}
	return Fast{s: n}
}

// CopyFrom sets the receiver to a copy of other, which can then be modified
// independently.
func (s *Fast) CopyFrom(other Fast) {
	*s = other.Copy()
}

// UnionWith adds all the elements from rhs to this set.
func (s *Fast) UnionWith(rhs Fast) {
	if rhs.s == nil {
		return
	}
	s.prepareForMutation()
	s.s.UnionWith(rhs.s)
}

// Union returns the union of s and rhs as a new set.
func (s Fast) Union(rhs Fast) Fast {
	r := s.Copy()
	r.UnionWith(rhs)
	return r
}

// IntersectionWith removes any elements not in rhs from this set.
func (s *Fast) IntersectionWith(rhs Fast) {
	if rhs.s == nil {
		*s = Fast{}
		return
	}
	s.prepareForMutation()
	s.s.IntersectionWith(rhs.s)
}

// Intersection returns the intersection of s and rhs as a new set.
func (s Fast) Intersection(rhs Fast) Fast {
	r := s.Copy()
	r.IntersectionWith(rhs)
	return r
}

// Intersects returns true if s has any elements in common with rhs.
func (s Fast) Intersects(rhs Fast) bool {
	if s.s == nil || rhs.s == nil {
		return false
	}
	return s.s.Intersects(rhs.s)
}

// DifferenceWith removes any elements in rhs from this set.
func (s *Fast) DifferenceWith(rhs Fast) {
	if rhs.s == nil {
		return
	}
	s.prepareForMutation()
	s.s.DifferenceWith(rhs.s)
}

// Difference returns the elements of s that are not in rhs as a new set.
func (s Fast) Difference(rhs Fast) Fast {
	r := s.Copy()
	r.DifferenceWith(rhs)
	return r
}

// Equals returns true if the two sets are identical.
func (s Fast) Equals(rhs Fast) bool {
	if s.Empty() || rhs.Empty() {
		return s.Empty() == rhs.Empty()
	}
	return s.s.Equals(rhs.s)
}

// SubsetOf returns true if rhs contains all the elements in s.
func (s Fast) SubsetOf(rhs Fast) bool {
	if s.Empty() {
		return true
	}
	if rhs.s == nil {
		return false
	}
	return s.s.SubsetOf(rhs.s)
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
	if s.s != nil && s.s.Min() < 0 {
		return errors.AssertionFailedf("Encode used with negative elements")
	}

	// This slice should stay on stack. We only need enough bytes to encode a 0
	// and then an arbitrary 64-bit integer.
	//gcassert:noescape
	tmp := make([]byte, binary.MaxVarintLen64+1)

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

	max := MinInt
	s.ForEach(func(i int) {
		if i > max {
			max = i
		}
	})

	if s.s == nil || max < 64 {
		n := binary.PutUvarint(tmp, 0)
		var bitmap uint64
		for i, ok := s.Next(0); ok; i, ok = s.Next(i + 1) {
			bitmap |= (1 << uint64(i))
		}
		n += binary.PutUvarint(tmp[n:], bitmap)
		write(tmp[:n])
	} else {
		n := binary.PutUvarint(tmp, uint64(s.Len()))
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
	if s.s != nil {
		s.s.Clear()
	}

	if length == 0 {
		// Special case: the bitmap is encoded directly.
		val, err := binary.ReadUvarint(br)
		if err != nil {
			return err
		}
		for i := 0; i < 64; i++ {
			if val&(1<<uint64(i)) != 0 {
				s.Add(i)
			}
		}
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
