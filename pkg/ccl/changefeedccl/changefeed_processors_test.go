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

	statementTime := hlc.Timestamp{WallTime: 10}

	type checkpointSpan struct {
		span roachpb.Span
		ts   hlc.Timestamp
	}

	type checkpointSpans []checkpointSpan

	for _, tc := range []struct {
		name                 string
		initialHighWater     hlc.Timestamp
		watches              []execinfrapb.ChangeAggregatorSpec_Watch
		spanLevelCheckpoint  *jobspb.TimestampSpansMap
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
			spanLevelCheckpoint: jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
				{WallTime: 5}: []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, {Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
			}),
			expectedFrontierTs: hlc.Timestamp{},
			expectedFrontierSpan: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: hlc.Timestamp{WallTime: 5}},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: hlc.Timestamp{}},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: hlc.Timestamp{WallTime: 5}},
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
			spanLevelCheckpoint: jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
				{WallTime: 7}: []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, {Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
			}),
			expectedFrontierTs: hlc.Timestamp{WallTime: 5},
			expectedFrontierSpan: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: hlc.Timestamp{WallTime: 7}},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: hlc.Timestamp{WallTime: 5}},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: hlc.Timestamp{WallTime: 7}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ca := &changeAggregator{}
			ca.spec.Feed.StatementTime = statementTime
			ca.spec.Watches = tc.watches
			ca.spec.InitialHighWater = &tc.initialHighWater
			ca.spec.SpanLevelCheckpoint = tc.spanLevelCheckpoint
			_, err := ca.setupSpansAndFrontier()
			require.NoError(t, err)
			actualFrontierSpan := checkpointSpans{}
			for sp, ts := range ca.frontier.Entries() {
				actualFrontierSpan = append(actualFrontierSpan, checkpointSpan{span: sp, ts: ts})
			}

			require.Equal(t, tc.expectedFrontierSpan, actualFrontierSpan)
			require.Equal(t, tc.expectedFrontierTs, ca.frontier.Frontier())
		})
	}
}
