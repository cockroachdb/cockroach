// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/cockroachdb/cockroach/pkg/util/span"
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

type rspans []roachpb.Span

func (rs rspans) Len() int {
	return len(rs)
}

func (rs rspans) Less(i int, j int) bool {
	return rs[i].Key.Compare(rs[j].Key) < 0
}

func (rs rspans) Swap(i int, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

type checkpointSpan struct {
	span roachpb.Span
	ts   hlc.Timestamp
}

type checkpointSpans []checkpointSpan

func (rs checkpointSpans) Len() int {
	return len(rs)
}

func (rs checkpointSpans) Less(i int, j int) bool {
	return rs[i].span.Key.Compare(rs[j].span.Key) < 0
}

func (rs checkpointSpans) Swap(i int, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

// TestGetCheckpointSpans generates 100 random non-overlapping spans with random
// timestamps within a minute of each other and turns them into checkpoint
// spans. It then does some sanity checks. It also compares the total
// catchup time between the checkpoint timestamp and the high watermark.
// Although the test relies on internal implementation details, it is a
// good base to explore other fine-grained checkpointing algorithms.
func TestGetCheckpointSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numSpans = 100
	maxBytes := changefeedbase.FrontierCheckpointMaxBytes.Default()
	hwm := hlc.Timestamp{}
	rng, _ := randutil.NewTestRand()

	spans := make(checkpointSpans, numSpans)

	// Generate spans. They should not be overlapping.
	// Randomize the order in which spans are processed.
	for i, s := range rangefeed.GenerateRandomizedSpans(rng, numSpans) {
		ts := rangefeed.GenerateRandomizedTs(rng, time.Minute.Nanoseconds())
		if hwm.IsEmpty() || ts.Less(hwm) {
			hwm = ts
		}
		spans[i] = checkpointSpan{s.AsRawSpanWithNoLocals(), ts}
	}
	shuffle.Shuffle(spans)

	forEachSpan := func(fn span.Operation) {
		for _, s := range spans {
			fn(s.span, s.ts)
		}
	}

	// Compute the checkpoint.
	cpSpans, cpTS := getCheckpointSpans(hwm, forEachSpan, maxBytes)
	require.Less(t, len(cpSpans), numSpans)
	require.True(t, hwm.Less(cpTS))

	// Calculate the total amount of time these spans would have to "catch up"
	// using the checkpoint spans compared to starting at the frontier.
	catchup := cpTS.GoTime().Sub(hwm.GoTime())
	sort.Sort(rspans(cpSpans))
	sort.Sort(spans)
	var catchupFromCheckpoint, catchupFromHWM time.Duration
	j := 0
	for _, s := range spans {
		catchupFromHWM += s.ts.GoTime().Sub(hwm.GoTime())
		if j < len(cpSpans) && cpSpans[j].Equal(s.span) {
			catchupFromCheckpoint += s.ts.GoTime().Sub(cpTS.GoTime())
			j++
		}
	}
	t.Logf("Checkpoint time improved by %v for %d/%d spans\ntotal catchup from checkpoint: %v\ntotal catchup from high watermark: %v\nPercent improvement %f",
		catchup, len(cpSpans), numSpans, catchupFromCheckpoint, catchupFromHWM,
		100*(1-float64(catchupFromCheckpoint.Nanoseconds())/float64(catchupFromHWM.Nanoseconds())))
	require.Less(t, catchupFromCheckpoint, catchupFromHWM)
}
