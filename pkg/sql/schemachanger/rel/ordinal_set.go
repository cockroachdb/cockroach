// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

import (
	"math/bits"
	"unsafe"
)

// ordinal is used to correlate attributes in a schema.
// It enables use of the ordinalSet.
type ordinal uint8

// ordinalSet represents A bitmask over ordinals.
// Note that it cannot contain attributes with ordinals greater than 30.
type ordinalSet uint32

const ordinalSetCapacity = (unsafe.Sizeof(ordinalSet(0)) * 8)
const ordinalSetMaxOrdinal = ordinal(ordinalSetCapacity - 1)

// ForEach iterates the set of attributes.
func (m ordinalSet) forEach(f func(a ordinal) (wantMore bool)) {
	rem := m
	for rem > 0 {
		ord := ordinal(bits.TrailingZeros64(uint64(rem)))
		if !f(ord) {
			return
		}
		rem = rem.remove(ord)
	}
}

// remove returns the set constructed by removing ord from m.
func (m ordinalSet) remove(ord ordinal) ordinalSet {
	return m & ^(1 << ord)
}

// contains tests if m contains ord.
func (m ordinalSet) contains(ord ordinal) bool {
	return m&(1<<ord) != 0
}

// add returns the set constructed by adding ord to m.
func (m ordinalSet) add(ord ordinal) ordinalSet {
	return m | (1 << ord)
}

// without returns the set constructed by removing the members of other from m.
func (m ordinalSet) without(other ordinalSet) ordinalSet {
	return m & ^other
}

// intersection returns the set constructing with the intersection of m and other.
func (m ordinalSet) intersection(other ordinalSet) ordinalSet {
	return m & other
}

// union returns the set constructing with the union of m and other.
func (m ordinalSet) union(other ordinalSet) ordinalSet {
	return m | other
}

// len returns the number of ordinals in the set.
func (m ordinalSet) len() int {
	return bits.OnesCount32(uint32(m))
}

// rank returns the rank in the set of the passed ordinal.
func (m ordinalSet) rank(a ordinal) int {
	return bits.OnesCount32(uint32(m & ((1 << a) - 1)))
}

// isContainedIn returns true of m contains every element of other.
func (m ordinalSet) isContainedIn(other ordinalSet) bool {
	return other.intersection(m) == m
}

func (m ordinalSet) empty() bool {
	return m == 0
}
