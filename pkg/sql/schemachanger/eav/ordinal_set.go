// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eav

import "math/bits"

// MakeOrdinalSetWithAttributes constructs an OrdinalSet with a slice of
// Attribute.
func MakeOrdinalSetWithAttributes(attrs []Attribute) (m OrdinalSet) {
	for _, a := range attrs {
		m = m.Add(a.Ordinal())
	}
	return m
}

// OrdinalSet represents a bitmask over ordinals.
// Note that it cannot contain attributes with ordinals greater than 64.
type OrdinalSet uint64

// ForEach iterates the set of attributes.
func (m OrdinalSet) ForEach(as Schema, f func(a Attribute) (wantMore bool)) {
	rem := m
	for rem > 0 {
		ord := Ordinal(bits.TrailingZeros64(uint64(rem)))
		if !f(as.At(ord)) {
			return
		}
		rem = rem.Remove(ord)
	}
}

// Remove returns the set constructed by removing ord from m.
func (m OrdinalSet) Remove(ord Ordinal) OrdinalSet {
	return m & ^(1 << ord)
}

// Contains tests if m contains ord.
func (m OrdinalSet) Contains(ord Ordinal) bool {
	return m&(1<<ord) != 0
}

// Add returns the set constructed by adding ord to m.
func (m OrdinalSet) Add(ord Ordinal) OrdinalSet {
	return m | (1 << ord)
}

// Without returns the set constructed by removing the members of other from m.
func (m OrdinalSet) Without(other OrdinalSet) OrdinalSet {
	return m & ^other
}

// Intersection returns the set constructing with the intersection of m and other.
func (m OrdinalSet) Intersection(other OrdinalSet) OrdinalSet {
	return m & other
}

// Union returns the set constructing with the union of m and other.
func (m OrdinalSet) Union(other OrdinalSet) OrdinalSet {
	return m | other
}

// Len returns the number of ordinals in the set.
func (m OrdinalSet) Len() int {
	return bits.OnesCount64(uint64(m))
}
