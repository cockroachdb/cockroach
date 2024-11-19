// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package screl

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

// equalityAttrs are used to sort elements.
var equalityAttrs = func() []rel.Attr {
	s := make([]rel.Attr, 0, AttrMax)
	s = append(s, rel.Type)
	for a := Attr(1); a <= AttrMax; a++ {
		// Do not compare on slice attributes.
		if !Schema.IsSliceAttr(a) {
			s = append(s, a)
		}
	}
	return s
}()

// EqualElementKeys returns true if the two elements are equal over all of
// their scalar attributes and have the same type. Note that two elements
// which differ only in the contents of slice attributes will be considered
// equal by this function.
func EqualElementKeys(a, b scpb.Element) bool {
	return Schema.EqualOn(equalityAttrs, a, b)
}
