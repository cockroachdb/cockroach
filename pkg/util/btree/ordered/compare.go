// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package ordered contains utilities for dealing with types which implement
// constraints.Ordered.
package ordered

import "golang.org/x/exp/constraints"

// Compare is a comparison function for ordered types.
func Compare[T constraints.Ordered](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// Bytes is any type which has an underlying type that is a byte slice.
type Bytes interface{ ~[]byte }

// Func is a custom comparator for any type.
type Func[K any] func(K, K) int

// Pair is a pair of ordered values.
type Pair[K constraints.Ordered] [2]K

// Span is an interface to describe an object which spans two points.
type Span[Point any] interface {
	Key() Point
	EndKey() Point
}

// Key is an interface for objects which know how to compare themselves
// against objects of the same type.
type Key[K any] interface {
	Compare(other K) int
}

// BytesSpan is an interface describing a Span where the points are
// byte slices.
type BytesSpan[K Bytes] interface {
	Span[K]
}

// BytesSpanWithID is an interface describing a Span where the points are
// byte slices and the spans can be distinguished by an ID.
type BytesSpanWithID[K Bytes, ID constraints.Ordered] interface {
	BytesSpan[K]
	ID() ID
}
