// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rspb

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

var (
	keyA    = []byte("a")
	keyB    = []byte("b")
	keyC    = []byte("c")
	keyD    = []byte("d")
	keyE    = []byte("e")
	ts10    = hlc.Timestamp{WallTime: 10}
	ts20    = hlc.Timestamp{WallTime: 20}
	ts30    = hlc.Timestamp{WallTime: 30}
	ts40    = hlc.Timestamp{WallTime: 40}
	uuidNil = uuid.Nil
	uuid1   = uuid.NamespaceDNS
	uuid2   = uuid.NamespaceURL
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

func TestSegmentMerge(t *testing.T) {
	testCases := []struct {
		a, b Segment
		exp  Segment
	}{
		// Test empty segments.
		{
			a:   Segment{},
			b:   Segment{},
			exp: Segment{},
		},
		// Test forward low water mark.
		{
			a:   Segment{LowWater: ts20},
			b:   Segment{LowWater: ts30},
			exp: Segment{LowWater: ts30},
		},
		// Test filter spans on new low water mark.
		{
			a: Segment{
				LowWater:  ts10,
				ReadSpans: []ReadSpan{{Timestamp: ts20}, {Timestamp: ts30}, {Timestamp: ts40}},
			},
			b: Segment{LowWater: ts30},
			exp: Segment{
				LowWater:  ts30,
				ReadSpans: []ReadSpan{{Timestamp: ts40}},
			},
		},
		// Test merge spans.
		// NOTE: there's more comprehensive testing of span merging in
		// merge_spans_test.go.
		{
			a: Segment{
				LowWater: ts10,
				ReadSpans: []ReadSpan{
					{Key: keyA, Timestamp: ts30},
					{Key: keyC, Timestamp: ts30, TxnID: uuid1},
					{Key: keyD, Timestamp: ts40, TxnID: uuid1},
				},
			},
			b: Segment{
				LowWater: ts20,
				ReadSpans: []ReadSpan{
					{Key: keyB, Timestamp: ts30},
					{Key: keyC, Timestamp: ts30, TxnID: uuid2},
					{Key: keyD, Timestamp: ts30, TxnID: uuid2},
				},
			},
			exp: Segment{
				LowWater: ts20,
				ReadSpans: []ReadSpan{
					{Key: keyA, Timestamp: ts30},
					{Key: keyB, Timestamp: ts30},
					{Key: keyC, Timestamp: ts30, TxnID: uuidNil},
					{Key: keyD, Timestamp: ts40, TxnID: uuid1},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			for _, reverse := range []bool{false, true} {
				t.Run(fmt.Sprintf("reverse=%t", reverse), func(t *testing.T) {
					a, b := tc.a, tc.b
					if reverse {
						a, b = b, a
					}
					res := a.Clone()
					res.Merge(b)
					require.Equal(t, tc.exp, res)
				})
			}
		})
	}
}

func TestFilterSpans(t *testing.T) {
	orig := []ReadSpan{
		{Key: keyA, Timestamp: ts20},
		{Key: keyB, Timestamp: ts30},
		{Key: keyC, Timestamp: ts40},
		{Key: keyD, Timestamp: ts30},
		{Key: keyE, Timestamp: ts20},
	}

	// Nothing to filter.
	filter10 := filterSpans(orig, ts10)
	exp10 := orig
	require.Equal(t, exp10, filter10)

	// Filter out some.
	filter20 := filterSpans(orig, ts20)
	exp20 := []ReadSpan{
		{Key: keyB, Timestamp: ts30},
		{Key: keyC, Timestamp: ts40},
		{Key: keyD, Timestamp: ts30},
	}
	require.Equal(t, exp20, filter20)

	// Filter out some more.
	filter30 := filterSpans(orig, ts30)
	exp30 := []ReadSpan{
		{Key: keyC, Timestamp: ts40},
	}
	require.Equal(t, exp30, filter30)

	// Filter out all.
	filter40 := filterSpans(orig, ts40)
	exp40 := []ReadSpan{}
	require.Equal(t, exp40, filter40)
}

func TestSegmentCompress(t *testing.T) {
	testCases := []struct {
		seg    Segment
		budget int64
		exp    Segment
	}{
		// Test empty segment and no budget.
		{
			seg:    Segment{},
			budget: 0,
			exp:    Segment{},
		},
		// Test segment with low water and no budget.
		{
			seg:    Segment{LowWater: ts10},
			budget: 0,
			exp:    Segment{LowWater: ts10},
		},
		// Test segment with low water and spans and no budget.
		{
			seg: Segment{
				LowWater: ts10,
				ReadSpans: []ReadSpan{
					{Key: keyA, Timestamp: ts20},
					{Key: keyB, Timestamp: ts30},
					{Key: keyC, Timestamp: ts40},
				},
			},
			budget: 0,
			exp:    Segment{LowWater: ts40},
		},
		// Test segment with low water and spans and a small budget.
		{
			seg: Segment{
				LowWater: ts10,
				ReadSpans: []ReadSpan{
					{Key: keyA, Timestamp: ts20},
					{Key: keyB, Timestamp: ts30},
					{Key: keyC, Timestamp: ts40},
				},
			},
			budget: 30,
			exp: Segment{
				LowWater: ts30,
				ReadSpans: []ReadSpan{
					{Key: keyC, Timestamp: ts40},
				},
			},
		},
		// Test segment with low water and spans and a medium budget.
		{
			seg: Segment{
				LowWater: ts10,
				ReadSpans: []ReadSpan{
					{Key: keyA, Timestamp: ts20},
					{Key: keyB, Timestamp: ts30},
					{Key: keyC, Timestamp: ts40},
				},
			},
			budget: 60,
			exp: Segment{
				LowWater: ts20,
				ReadSpans: []ReadSpan{
					{Key: keyB, Timestamp: ts30},
					{Key: keyC, Timestamp: ts40},
				},
			},
		},
		// Test segment with low water and spans and a large budget.
		{
			seg: Segment{
				LowWater: ts10,
				ReadSpans: []ReadSpan{
					{Key: keyA, Timestamp: ts20},
					{Key: keyB, Timestamp: ts30},
					{Key: keyC, Timestamp: ts40},
				},
			},
			budget: 100,
			exp: Segment{
				LowWater: ts10,
				ReadSpans: []ReadSpan{
					{Key: keyA, Timestamp: ts20},
					{Key: keyB, Timestamp: ts30},
					{Key: keyC, Timestamp: ts40},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			res := tc.seg.Clone()
			res.Compress(tc.budget)
			require.Equal(t, tc.exp, res)
		})
	}
}
