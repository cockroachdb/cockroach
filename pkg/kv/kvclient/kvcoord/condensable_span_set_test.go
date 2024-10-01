// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// Test that the size of the condensableSpanSet is properly maintained when
// contiguous spans are merged.
func TestCondensableSpanSetMergeContiguousSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := condensableSpanSet{}
	s.insert(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")})
	s.insert(roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")})
	require.Equal(t, 4+2*roachpb.SpanOverhead, s.bytes)
	s.mergeAndSort()
	require.Equal(t, 2+roachpb.SpanOverhead, s.bytes)
}

func TestCondensableSpanSetEstimateSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ab := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}
	bc := roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}
	largeSpan := roachpb.Span{Key: roachpb.Key("ccccc"), EndKey: roachpb.Key("ddddd")}

	tests := []struct {
		name           string
		set            []roachpb.Span
		newSpans       []roachpb.Span
		mergeThreshold int64
		expEstimate    int64
	}{
		{
			name:           "new spans fit without merging",
			set:            []roachpb.Span{ab, bc},
			newSpans:       []roachpb.Span{ab},
			mergeThreshold: 100 + 3*roachpb.SpanOverhead,
			expEstimate:    6 + 3*roachpb.SpanOverhead,
		},
		{
			// The set gets merged, the new spans don't.
			name:           "set needs merging",
			set:            []roachpb.Span{ab, bc},
			newSpans:       []roachpb.Span{ab},
			mergeThreshold: 5 + 2*roachpb.SpanOverhead,
			expEstimate:    4 + 2*roachpb.SpanOverhead,
		},
		{
			// The set gets merged, and then it gets merged again with the newSpans.
			name:           "new spans fit without merging",
			set:            []roachpb.Span{ab, bc},
			newSpans:       []roachpb.Span{ab, bc},
			mergeThreshold: 5 + 2*roachpb.SpanOverhead,
			expEstimate:    2 + roachpb.SpanOverhead,
		},
		{
			// Everything gets merged, but it still doesn't fit.
			name:           "new spans dont fit",
			set:            []roachpb.Span{ab, bc},
			newSpans:       []roachpb.Span{ab, bc, largeSpan},
			mergeThreshold: 5 + 2*roachpb.SpanOverhead,
			expEstimate:    12 + 2*roachpb.SpanOverhead,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := condensableSpanSet{}
			s.insert(tc.set...)
			require.Equal(t, tc.expEstimate, s.estimateSize(tc.newSpans, tc.mergeThreshold))
		})
	}
}
