// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
		s = append(s, a)
	}
	return s
}()

// EqualElements returns true if the two elements are equal.
func EqualElements(a, b scpb.Element) bool {
	return Schema.EqualOn(equalityAttrs, a, b)
}

// CompareElements orders two elements.
func CompareElements(a, b scpb.Element) (less, eq bool) {
	return Schema.CompareOn(equalityAttrs, a, b)
}
