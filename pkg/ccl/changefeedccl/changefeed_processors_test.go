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
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/stretchr/testify/require"
)

// TestSetupSpansAndFrontier tests that the setupSpansAndFrontier function
// correctly sets up frontier for the changefeed aggregator frontier.
func TestSetupSpansAndFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	statementTime := hlc.Timestamp{WallTime: 10}
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
			ca.spec.Feed.StatementTime = statementTime
			_, err := ca.setupSpansAndFrontier()
			require.NoError(t, err)
			require.Equal(t, tc.expectedFrontier, ca.frontier.Frontier())
		})
	}
}

type checkpointSpan struct {
	span roachpb.Span
	ts   hlc.Timestamp
}

type checkpointSpans []checkpointSpan

// TestSetupSpansAndFrontierWithNewSpec tests that the setupSpansAndFrontier
// function correctly sets up frontier for the changefeed aggregator frontier
// with the new ChangeAggregatorSpec_Watch where initial resolved is not set but
// initial highwater is passed in.
func TestSetupSpansAndFrontierWithNewSpec(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	statementTime := hlc.Timestamp{WallTime: 10}

	for _, tc := range []struct {
		name             string
		initialHighWater hlc.Timestamp
		watches          []execinfrapb.ChangeAggregatorSpec_Watch
		//lint:ignore SA1019 deprecated usage
		checkpoints          execinfrapb.ChangeAggregatorSpec_Checkpoint
		expectedFrontierTs   hlc.Timestamp
		expectedFrontierSpan checkpointSpans
	}{
		{
			name:             "new initial scan",
			initialHighWater: hlc.Timestamp{},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
				{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("e")}},
				{Span: roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}},
			},
			expectedFrontierTs: hlc.Timestamp{},
			expectedFrontierSpan: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: hlc.Timestamp{}},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("e")}, ts: hlc.Timestamp{}},
				{span: roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}, ts: hlc.Timestamp{}},
			},
		},
		{
			name:             "complete initial scan with empty span level checkpoints",
			initialHighWater: hlc.Timestamp{WallTime: 5},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
				{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("e")}},
				{Span: roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}},
			},
			expectedFrontierTs: hlc.Timestamp{WallTime: 5},
			expectedFrontierSpan: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: hlc.Timestamp{WallTime: 5}},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("e")}, ts: hlc.Timestamp{WallTime: 5}},
				{span: roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}, ts: hlc.Timestamp{WallTime: 5}},
			},
		},
		{
			name:             "initial scan in progress with span level checkpoints and checkpoint ts",
			initialHighWater: hlc.Timestamp{},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
				{Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}},
				{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
			},
			//lint:ignore SA1019 deprecated usage
			checkpoints: execinfrapb.ChangeAggregatorSpec_Checkpoint{
				Spans:     []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, {Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
				Timestamp: hlc.Timestamp{WallTime: 5},
			},
			expectedFrontierTs: hlc.Timestamp{},
			expectedFrontierSpan: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: hlc.Timestamp{WallTime: 5}},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: hlc.Timestamp{}},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: hlc.Timestamp{WallTime: 5}},
			},
		},
		{
			name:             "initial scan with span level checkpoints but no checkpoint ts",
			initialHighWater: hlc.Timestamp{},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
				{Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}},
				{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
			},
			//lint:ignore SA1019 deprecated usage
			checkpoints: execinfrapb.ChangeAggregatorSpec_Checkpoint{
				Spans: []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, {Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
			},
			expectedFrontierTs: hlc.Timestamp{},
			expectedFrontierSpan: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: statementTime},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: hlc.Timestamp{}},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: statementTime},
			},
		},
		{
			name:             "schema backfill with span level checkpoints and checkpoint ts",
			initialHighWater: hlc.Timestamp{WallTime: 5},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
				{Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}},
				{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
			},
			//lint:ignore SA1019 deprecated usage
			checkpoints: execinfrapb.ChangeAggregatorSpec_Checkpoint{
				Spans:     []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, {Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
				Timestamp: hlc.Timestamp{WallTime: 7},
			},
			expectedFrontierTs: hlc.Timestamp{WallTime: 5},
			expectedFrontierSpan: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: hlc.Timestamp{WallTime: 7}},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: hlc.Timestamp{WallTime: 5}},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: hlc.Timestamp{WallTime: 7}},
			},
		},
		{
			name:             "schema backfill with span level checkpoints but no checkpoint ts",
			initialHighWater: hlc.Timestamp{WallTime: 5},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
				{Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}},
				{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
			},
			//lint:ignore SA1019 deprecated usage
			checkpoints: execinfrapb.ChangeAggregatorSpec_Checkpoint{
				Spans: []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, {Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
			},
			expectedFrontierSpan: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: hlc.Timestamp{WallTime: 5}.Next()},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: hlc.Timestamp{WallTime: 5}},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: hlc.Timestamp{WallTime: 5}.Next()},
			},
			expectedFrontierTs: hlc.Timestamp{WallTime: 5},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ca := &changeAggregator{}
			ca.spec.Feed.StatementTime = statementTime
			ca.spec.Checkpoint = tc.checkpoints
			ca.spec.Watches = tc.watches
			ca.spec.InitialHighWater = &tc.initialHighWater
			_, err := ca.setupSpansAndFrontier()
			require.NoError(t, err)
			actualFrontierSpan := checkpointSpans{}
			ca.frontier.Entries(func(sp roachpb.Span, ts hlc.Timestamp) span.OpResult {
				actualFrontierSpan = append(actualFrontierSpan, checkpointSpan{span: sp, ts: ts})
				return span.ContinueMatch
			})

			require.Equal(t, tc.expectedFrontierSpan, actualFrontierSpan)
			require.Equal(t, tc.expectedFrontierTs, ca.frontier.Frontier())
		})
	}
}
