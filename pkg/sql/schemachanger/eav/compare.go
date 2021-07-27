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

import "github.com/cockroachdb/errors"

// CompareOn compares two elements on a given attribute.
// If the entities do not return the same type of value for the
// attribute, this function will panic. Note that it is fine if
// either or both do not contain this attribute. The lack of a
// value is considered the highest value; you can think of this
// library as sorting with NULLS LAST.
func CompareOn(attr Attribute, a, b Entity) (less, eq bool) {
	av, bv := a.Get(attr), b.Get(attr)
	switch {
	case av == nil && bv == nil:
		return false, true
	case av == nil:
		return false, false
	case bv == nil:
		return true, false
	default:
		var ok bool
		ok, less, eq = av.Compare(bv)
		if !ok {
			// See TestElementAttributeValueTypesMatch for why this is safe.
			panic(errors.AssertionFailedf(
				"type mismatch (%T, %T) for attribute %s", av, bv, attr))
		}
		return less, eq
	}
}

// Compare compares two elements by their attributes.
func Compare(s Schema, a, b Entity) (less, eq bool) {
	OrdinalSet.Union(
		a.Attributes(), b.Attributes(),
	).ForEach(s, func(attr Attribute) (wantMore bool) {
		less, eq = CompareOn(attr, a, b)
		return eq
	})
	return less, eq
}

// Equal returns true if the two elements have identical attributes.
func Equal(s Schema, a, b Entity) bool {
	_, eq := Compare(s, a, b)
	return eq
}
