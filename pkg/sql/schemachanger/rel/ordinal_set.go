// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rel

import "math/bits"

// ordinal is used to correlate attributes in a schema.
// It enables use of the ordinalSet.
type ordinal uint64

// attributesToOrdinals constructs a slice of ordinals and the corresponding
// ordinalSet from a slice of Attr.
func (sc *Schema) attributesToOrdinals(attrs []Attr) ([]ordinal, ordinalSet, error) {
	var set ordinalSet
	ret := make([]ordinal, len(attrs))
	for i, a := range attrs {
		ord, err := sc.getOrdinal(a)
		if err != nil {
			return nil, 0, err
		}
		set = set.add(ord)
		ret[i] = ord
	}
	return ret, set, nil
}

// ordinalSet represents A bitmask over ordinals.
// Note that it cannot contain attributes with ordinals greater than 64.
type ordinalSet uint64

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
	return bits.OnesCount64(uint64(m))
}
