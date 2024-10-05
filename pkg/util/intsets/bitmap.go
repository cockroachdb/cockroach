// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package intsets

import "math/bits"

// smallCutoff is the size of the small bitmap.
// Note: this can be set to a smaller value, e.g. for testing.
const smallCutoff = 128

// bitmap implements a bitmap of size smallCutoff.
type bitmap struct {
	// We don't use an array because that makes Go always keep the struct on the
	// stack (see https://github.com/golang/go/issues/24416).
	lo, hi uint64
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
