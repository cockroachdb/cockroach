// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"testing"

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
