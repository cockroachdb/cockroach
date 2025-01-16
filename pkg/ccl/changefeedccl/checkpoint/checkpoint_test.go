// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package checkpoint_test

import (
	"math"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/checkpoint"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/stretchr/testify/require"
)

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

func TestCheckpointMake(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(wt int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: wt}
	}

	for name, tc := range map[string]struct {
		frontier hlc.Timestamp
		spans    checkpointSpans
		maxBytes int64
		expected jobspb.ChangefeedProgress_Checkpoint
	}{
		"all spans ahead of frontier checkpointed": {
			frontier: ts(1),
			spans: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: ts(2)},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")}, ts: ts(4)},
			},
			maxBytes: 100,
			expected: jobspb.ChangefeedProgress_Checkpoint{
				Timestamp: ts(2),
				Spans: []roachpb.Span{
					{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")},
				},
			},
		},
		"only some spans ahead of frontier checkpointed because of maxBytes constraint": {
			frontier: ts(1),
			spans: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: ts(2)},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")}, ts: ts(4)},
			},
			maxBytes: 2,
			expected: jobspb.ChangefeedProgress_Checkpoint{
				Timestamp: ts(2),
				Spans:     []roachpb.Span{{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}},
			},
		},
		"no spans checkpointed because of maxBytes constraint": {
			frontier: ts(1),
			spans: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: ts(2)},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")}, ts: ts(4)},
			},
			maxBytes: 0,
			expected: jobspb.ChangefeedProgress_Checkpoint{
				Timestamp: ts(2),
			},
		},
		"no spans checkpointed because all spans are at frontier": {
			frontier: ts(1),
			spans: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")}, ts: ts(1)},
			},
			maxBytes: 100,
			expected: jobspb.ChangefeedProgress_Checkpoint{},
		},
		"adjacent spans ahead of frontier merged before being checkpointed": {
			frontier: ts(1),
			spans: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: ts(2)},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: ts(4)},
				{span: roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")}, ts: ts(1)},
			},
			maxBytes: 100,
			expected: jobspb.ChangefeedProgress_Checkpoint{
				Timestamp: ts(2),
				Spans:     []roachpb.Span{{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")}},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			aggMetrics := checkpoint.NewAggMetrics(aggmetric.MakeBuilder())
			actual := checkpoint.Make(
				tc.frontier,
				func(fn span.Operation) {
					for _, sp := range tc.spans {
						fn(sp.span, sp.ts)
					}
				},
				tc.maxBytes,
				aggMetrics.AddChild(),
			)
			require.Equal(t, tc.expected, actual)

			// Verify that metrics were set/not set based on whether a
			// checkpoint was created.
			if tc.expected.Timestamp.IsSet() {
				require.Greater(t, aggMetrics.CreateNanos.CumulativeSnapshot().Mean(), float64(0))
				require.Greater(t, aggMetrics.TotalBytes.CumulativeSnapshot().Mean(), float64(0))
				require.Equal(t, float64(len(tc.expected.Spans)), aggMetrics.SpanCount.CumulativeSnapshot().Mean())
			} else {
				require.True(t, math.IsNaN(aggMetrics.CreateNanos.CumulativeSnapshot().Mean()))
				require.True(t, math.IsNaN(aggMetrics.TotalBytes.CumulativeSnapshot().Mean()))
				require.True(t, math.IsNaN(aggMetrics.SpanCount.CumulativeSnapshot().Mean()))
			}
		})
	}
}

// TestCheckpointCatchupTime generates 100 random non-overlapping spans with random
// timestamps within a minute of each other and turns them into checkpoint
// spans. It then does some sanity checks. It also compares the total
// catchup time between the checkpoint timestamp and the high watermark.
// Although the test relies on internal implementation details, it is a
// good base to explore other fine-grained checkpointing algorithms.
func TestCheckpointCatchupTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numSpans = 100
	maxBytes := changefeedbase.SpanCheckpointMaxBytes.Default()
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
	cp := checkpoint.Make(hwm, forEachSpan, maxBytes, nil /* metrics */)
	cpSpans, cpTS := roachpb.Spans(cp.Spans), cp.Timestamp
	require.Less(t, len(cpSpans), numSpans)
	require.True(t, hwm.Less(cpTS))

	// Calculate the total amount of time these spans would have to "catch up"
	// using the checkpoint spans compared to starting at the frontier.
	catchup := cpTS.GoTime().Sub(hwm.GoTime())
	sort.Sort(cpSpans)
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
