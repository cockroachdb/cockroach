// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rspb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

var (
	keyA = []byte("a")
	keyB = []byte("b")
	keyC = []byte("c")
	keyD = []byte("d")
	keyE = []byte("e")
	ts10 = hlc.Timestamp{WallTime: 10}
	ts20 = hlc.Timestamp{WallTime: 20}
	ts30 = hlc.Timestamp{WallTime: 30}
	ts40 = hlc.Timestamp{WallTime: 40}
)

func TestSegmentAddReadSpan(t *testing.T) {
	seg := Segment{LowWater: ts20}

	// Span at timestamp below low water mark is ignored.
	seg.AddReadSpan(ReadSpan{Key: keyB, EndKey: keyD, Timestamp: ts10})
	require.Len(t, seg.ReadSpans, 0)

	// Span at timestamp equal to low water mark is ignored.
	seg.AddReadSpan(ReadSpan{Key: keyB, EndKey: keyD, Timestamp: ts20})
	require.Len(t, seg.ReadSpans, 0)

	// Spans at timestamps above low water mark are included.
	seg.AddReadSpan(ReadSpan{Key: keyB, EndKey: keyD, Timestamp: ts30})
	require.Len(t, seg.ReadSpans, 1)

	// Under test builds, assertions are enabled.
	if assertEnabled {
		// ["e", "d") is inverted.
		require.Panics(t, func() {
			seg.AddReadSpan(ReadSpan{Key: keyE, EndKey: keyD, Timestamp: ts30})
		})
		// ["e", "e") is inverted.
		require.Panics(t, func() {
			seg.AddReadSpan(ReadSpan{Key: keyE, EndKey: keyE, Timestamp: ts30})
		})
		// ["a"] before ["b", "d").
		require.Panics(t, func() {
			seg.AddReadSpan(ReadSpan{Key: keyA, Timestamp: ts30})
		})
		// ["b"] overlaps ["b", "d").
		require.Panics(t, func() {
			seg.AddReadSpan(ReadSpan{Key: keyB, Timestamp: ts30})
		})
		// ["c"] overlaps ["b", "d").
		require.Panics(t, func() {
			seg.AddReadSpan(ReadSpan{Key: keyC, Timestamp: ts30})
		})
		// ["c", "e") overlaps ["b", "d").
		require.Panics(t, func() {
			seg.AddReadSpan(ReadSpan{Key: keyC, EndKey: keyE, Timestamp: ts30})
		})
		// ["d", "e") does not overlap ["b", "d").
		require.NotPanics(t, func() {
			seg.AddReadSpan(ReadSpan{Key: keyD, EndKey: keyE, Timestamp: ts30})
		})
	}
}

func TestSegmentClone(t *testing.T) {
	var orig Segment
	orig.LowWater = ts10
	orig.AddReadSpan(ReadSpan{Key: keyB, Timestamp: ts30})

	requireOrig := func(c Segment) {
		require.Equal(t, ts10, c.LowWater)
		require.Len(t, c.ReadSpans, 1)
		require.Equal(t, ReadSpan{Key: keyB, Timestamp: ts30}, c.ReadSpans[0])
	}
	requireOrig(orig)

	// Clone and assert equality.
	clone := orig.Clone()
	require.Equal(t, orig, clone)
	requireOrig(clone)

	// Mutate the original.
	orig.LowWater = ts20
	orig.AddReadSpan(ReadSpan{Key: keyE, Timestamp: ts30})

	// Assert clone has not changed.
	require.NotEqual(t, orig, clone)
	requireOrig(clone)
}
