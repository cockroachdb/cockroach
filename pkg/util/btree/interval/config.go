// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package interval

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/util/btree/ordered"
	"golang.org/x/exp/constraints"
)

// WithPair is a config for pairs of ordered values. The assumption is that
// the second value is exclusive and is greater than or equal to the first
// value.
type WithPair[K constraints.Ordered] struct{}

// Compare implements ordered.Comparator.
func (WithPair[K]) Compare(a, b [2]K) int {
	if c := ordered.Compare(a[0], b[0]); c != 0 {
		return c
	}
	return ordered.Compare(a[1], b[1])
}

// Key is part of the Config interface.
func (WithPair[K]) Key(i [2]K) K { return i[0] }

// UpperBound is part of the Config interface.
func (WithPair[K]) UpperBound(i [2]K) (K, bool) { return i[1], i[0] == i[1] }

// ComparePoints is part of the Config interface.
func (WithPair[K]) ComparePoints(a, b K) int { return ordered.Compare(a, b) }

// WithBytesSpan is a config for objects which correspond to a bytes key span.
// The assumption is that the second value is exclusive and is greater than the
// first value. If an interval corresponds to a point, its EndKey should be a
// nil value.
type WithBytesSpan[Sp ordered.BytesSpan[K], K ordered.Bytes] struct {
}

// Compare implements Comparator.
func (WithBytesSpan[Sp, K]) Compare(a, b Sp) int {
	if c := bytes.Compare(a.Key(), b.Key()); c != 0 {
		return c
	}
	return bytes.Compare(a.EndKey(), b.EndKey())
}

// Key is part of the Config interface.
func (cmp WithBytesSpan[Sp, K]) Key(i Sp) K { return i.Key() }

// UpperBound is part of the Config interface.
func (cmp WithBytesSpan[Sp, K]) UpperBound(i Sp) (_ K, inclusive bool) {
	if ek := i.EndKey(); ek != nil {
		return ek, false
	}
	return i.Key(), true
}

// ComparePoints is part of the Config interface.
func (cmp WithBytesSpan[Sp, K]) ComparePoints(a, b K) int {
	return bytes.Compare(a, b)
}

// WithBytesSpanAndID is like WithBytesSpan but also allows for equal spans to
// be distinguished with an ID.
type WithBytesSpanAndID[
	Sp ordered.BytesSpanWithID[K, ID],
	K ordered.Bytes,
	ID constraints.Ordered,
] struct{}

// Key is part of the Config interface.
func (cmp WithBytesSpanAndID[Sp, K, ID]) Key(i Sp) K { return i.Key() }

// UpperBound is part of the Config interface.
func (cmp WithBytesSpanAndID[Sp, K, ID]) UpperBound(i Sp) (_ K, inclusive bool) {
	if ek := i.EndKey(); ek != nil {
		return ek, false
	}
	return i.Key(), true
}

// ComparePoints is part of the Config interface.
func (cmp WithBytesSpanAndID[Sp, K, ID]) ComparePoints(a, b K) int {
	return bytes.Compare(a, b)
}

// Compare is part of the Config interface.
func (cmp WithBytesSpanAndID[Sp, K, ID]) Compare(a, b Sp) int {
	if c := bytes.Compare(a.Key(), b.Key()); c != 0 {
		return c
	}
	if c := bytes.Compare(a.EndKey(), b.EndKey()); c != 0 {
		return c
	}
	return ordered.Compare(a.ID(), b.ID())
}
