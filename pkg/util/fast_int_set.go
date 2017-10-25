// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package util

import (
	"bytes"
	"fmt"
	"math/bits"

	"golang.org/x/tools/container/intsets"
)

// FastIntSet keeps track of a set of integers. It does not perform any
// allocations when the values are small. It is not thread-safe.
type FastIntSet struct {
	// We use a uint64 as long as all elements are between 0 and 63. If we add an
	// element outside of this range, we switch to Sparse. We don't just use the
	// latter directly because it's larger and can't be passed around by value.
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
	for x := s.large.LowerBound(0); x < smallCutoff; x = s.large.LowerBound(x + 1) {
		small |= (1 << uint64(x))
	}
	return small, s.large.Min() < 0 || s.large.Max() >= smallCutoff
}

// Add adds a value to the set. No-op if the value is already in the set.
func (s *FastIntSet) Add(i int) {
	if i >= 0 && i < smallCutoff && s.large == nil {
		// Fast path.
		s.small |= (1 << uint64(i))
		return
	}
	if s.large == nil {
		s.large = s.toLarge()
		s.small = 0
	}
	s.large.Insert(i)
}

// Remove removes a value from the set. No-op if the value is not in the set.
func (s *FastIntSet) Remove(i int) {
	if s.large == nil {
		if i >= 0 && i < smallCutoff {
			s.small &= ^(1 << uint64(i))
		}
	} else {
		s.large.Remove(i)
	}
}

// Contains returns true if the set contains the value.
func (s *FastIntSet) Contains(i int) bool {
	if s.large != nil {
		return s.large.Has(i)
	}
	return i >= 0 && i < smallCutoff && (s.small&(1<<uint64(i))) != 0
}

// Empty returns true if the set is empty.
func (s *FastIntSet) Empty() bool {
	return s.small == 0 && (s.large == nil || s.large.IsEmpty())
}

// Next returns the first value in the set which is >= startVal. If there is no
// value, the second return value is false.
func (s *FastIntSet) Next(startVal int) (int, bool) {
	if s.large != nil {
		res := s.large.LowerBound(startVal)
		return res, res != intsets.MaxInt
	}
	if startVal < smallCutoff {
		if startVal < 0 {
			startVal = 0
		}

		if ntz := bits.TrailingZeros64(s.small >> uint64(startVal)); ntz < 64 {
			return startVal + ntz, true
		}
	}
	return intsets.MaxInt, false
}

// ForEach calls a function for each value in the set (in increasing order).
func (s *FastIntSet) ForEach(f func(i int)) {
	if s.large != nil {
		for x := s.large.Min(); x != intsets.MaxInt; x = s.large.LowerBound(x + 1) {
			f(x)
		}
		return
	}
	for i, v := 0, s.small; v > 0; {
		ntz := bits.TrailingZeros64(v)
		if ntz == 64 {
			return
		}
		i += ntz
		f(i)
		i++
		v >>= uint64(ntz + 1)
	}
}

// Ordered returns a slice with all the integers in the set, in increasing order.
func (s *FastIntSet) Ordered() []int {
	if s.Empty() {
		return nil
	}
	if s.large != nil {
		return s.large.AppendTo([]int(nil))
	}
	result := make([]int, 0, bits.OnesCount64(s.small))
	s.ForEach(func(i int) {
		result = append(result, i)
	})
	return result
}

// Copy makes an copy of a FastIntSet which can be modified independently.
func (s *FastIntSet) Copy() FastIntSet {
	var c FastIntSet
	if s.large != nil {
		c.large = new(intsets.Sparse)
		c.large.Copy(s.large)
	} else {
		c.small = s.small
	}
	return c
}

// UnionWith adds all the elements from rhs to this set.
func (s *FastIntSet) UnionWith(rhs FastIntSet) {
	if s.large == nil && rhs.large == nil {
		// Fast path.
		s.small |= rhs.small
		return
	}

	if s.large == nil {
		s.large = s.toLarge()
		s.small = 0
	}
	s.large.UnionWith(rhs.toLarge())
}

// IntersectionWith removes any elements not in rhs from this set.
func (s *FastIntSet) IntersectionWith(rhs FastIntSet) {
	if s.large == nil {
		// Fast path.
		other := rhs.small
		if rhs.large != nil {
			// If the other set is large, we can ignore any values outside of the
			// small range.
			other, _ = rhs.largeToSmall()
		}
		s.small &= other
		return
	}

	s.large.IntersectionWith(rhs.toLarge())
}

// Equals returns true if the two sets are identical.
func (s *FastIntSet) Equals(rhs FastIntSet) bool {
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
func (s *FastIntSet) SubsetOf(rhs FastIntSet) bool {
	if s.large == nil && rhs.large == nil {
		return (s.small & rhs.small) == s.small
	}
	if s.large != nil && rhs.large != nil {
		return s.large.SubsetOf(rhs.large)
	}
	// One set is "large" and one is "small".
	s1 := s.small
	s2 := rhs.small
	if s.large != nil {
		var extraVals bool
		s1, extraVals = s.largeToSmall()
		if extraVals {
			// s has elements that rhs (which is small) can't have.
			return false
		}
	} else {
		// We don't care if rhs has extra values.
		s2, _ = rhs.largeToSmall()
	}
	return (s1 & s2) == s1
}

func (s FastIntSet) String() string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	first := true
	s.ForEach(func(i int) {
		if first {
			first = false
		} else {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, "%d", i)
	})
	buf.WriteByte(')')
	return buf.String()
}
