// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestSetupSpansAndFrontier tests that the setupSpansAndFrontier function
// correctly sets up frontier for the changefeed aggregator frontier.
func TestSetupSpansAndFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		name             string
		expectedFrontier hlc.Timestamp
		watches          []execinfrapb.ChangeAggregatorSpec_Watch
	}{
		{
			name:             "new initial scan",
			expectedFrontier: hlc.Timestamp{},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{
					Span:            roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					InitialResolved: hlc.Timestamp{},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					InitialResolved: hlc.Timestamp{},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
					InitialResolved: hlc.Timestamp{},
				},
			},
		},
		{
			name:             "incomplete initial scan with non-empty initial resolved in the middle",
			expectedFrontier: hlc.Timestamp{},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{
					Span:            roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					InitialResolved: hlc.Timestamp{WallTime: 5},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					InitialResolved: hlc.Timestamp{},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
					InitialResolved: hlc.Timestamp{WallTime: 20},
				},
			},
		},
		{
			name:             "incomplete initial scan with non-empty initial resolved in the front",
			expectedFrontier: hlc.Timestamp{},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{
					Span:            roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					InitialResolved: hlc.Timestamp{},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					InitialResolved: hlc.Timestamp{WallTime: 10},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
					InitialResolved: hlc.Timestamp{WallTime: 20},
				},
			},
		},
		{
			name:             "incomplete initial scan with empty initial resolved in the end",
			expectedFrontier: hlc.Timestamp{},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{
					Span:            roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					InitialResolved: hlc.Timestamp{WallTime: 10},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					InitialResolved: hlc.Timestamp{WallTime: 20},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
					InitialResolved: hlc.Timestamp{},
				},
			},
		},
		{
			name:             "complete initial scan",
			expectedFrontier: hlc.Timestamp{WallTime: 5},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{
					Span:            roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					InitialResolved: hlc.Timestamp{WallTime: 10},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					InitialResolved: hlc.Timestamp{WallTime: 20},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
					InitialResolved: hlc.Timestamp{WallTime: 5},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ca := &changeAggregator{
				spec: execinfrapb.ChangeAggregatorSpec{
					Watches: tc.watches,
				},
			}
			_, err := ca.setupSpansAndFrontier()
			require.NoError(t, err)
			require.Equal(t, tc.expectedFrontier, ca.frontier.Frontier())
		})
	}
}

func makeTS(wt int64) hlc.Timestamp {
	return hlc.Timestamp{WallTime: wt}
}

func makeResolvedSpan(
	start, end string, ts hlc.Timestamp, boundaryType jobspb.ResolvedSpan_BoundaryType,
) jobspb.ResolvedSpan {
	return jobspb.ResolvedSpan{
		Span:         makeSpan(start, end),
		Timestamp:    ts,
		BoundaryType: boundaryType,
	}
}

func makeSpan(start, end string) roachpb.Span {
	return roachpb.Span{
		Key:    roachpb.Key(start),
		EndKey: roachpb.Key(end),
	}
}

func TestSchemaChangeFrontier_ForwardResolvedSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a fresh frontier with no progress.
	f, err := makeSchemaChangeFrontier(
		hlc.Timestamp{},
		makeSpan("a", "f"),
	)
	require.NoError(t, err)
	require.Zero(t, f.Frontier())

	t.Run("advance frontier with no boundary", func(t *testing.T) {
		// Forwarding part of the span space to 10 should not advance the frontier.
		forwarded, err := f.ForwardResolvedSpan(
			makeResolvedSpan("a", "b", makeTS(10), jobspb.ResolvedSpan_NONE))
		require.NoError(t, err)
		require.False(t, forwarded)
		require.Zero(t, f.Frontier())

		// Forwarding the rest of the span space to 10 should advance the frontier.
		forwarded, err = f.ForwardResolvedSpan(
			makeResolvedSpan("b", "f", makeTS(10), jobspb.ResolvedSpan_NONE))
		require.NoError(t, err)
		require.True(t, forwarded)
		require.Equal(t, makeTS(10), f.Frontier())
	})

	t.Run("advance frontier with same timestamp and new boundary", func(t *testing.T) {
		// Forwarding part of the span space to 10 again with a non-NONE boundary
		// should be considered forwarding the frontier because we're learning
		// about a new boundary.
		forwarded, err := f.ForwardResolvedSpan(
			makeResolvedSpan("c", "f", makeTS(10), jobspb.ResolvedSpan_RESTART))
		require.NoError(t, err)
		require.True(t, forwarded)
		require.Equal(t, makeTS(10), f.Frontier())

		// Forwarding the rest of the span space to 10 again with a non-NONE boundary
		// should not be considered forwarding the frontier because we already
		// know about the new boundary.
		forwarded, err = f.ForwardResolvedSpan(
			makeResolvedSpan("a", "c", makeTS(10), jobspb.ResolvedSpan_RESTART))
		require.NoError(t, err)
		require.False(t, forwarded)
		require.Equal(t, makeTS(10), f.Frontier())
	})
}
