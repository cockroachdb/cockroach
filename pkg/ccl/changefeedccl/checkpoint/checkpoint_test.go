// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package checkpoint_test

import (
	"iter"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/checkpoint"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
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

func (rs checkpointSpans) All() iter.Seq2[roachpb.Span, hlc.Timestamp] {
	return func(yield func(roachpb.Span, hlc.Timestamp) bool) {
		for _, checkpointSpan := range rs {
			if !yield(checkpointSpan.span, checkpointSpan.ts) {
				return
			}
		}
	}
}

func TestCheckpointMake(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(wt int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: wt}
	}

	for name, tc := range map[string]struct {
		frontier                        hlc.Timestamp
		spans                           checkpointSpans
		maxBytes                        int64
		expectedCheckpointPossibilities []*jobspb.TimestampSpansMap
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
			expectedCheckpointPossibilities: []*jobspb.TimestampSpansMap{
				jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
					ts(2): {{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}},
					ts(4): {{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")}},
				}),
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
			expectedCheckpointPossibilities: []*jobspb.TimestampSpansMap{
				jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
					ts(2): {{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}},
				}),
				jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
					ts(4): {{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")}},
				}),
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
			maxBytes:                        0,
			expectedCheckpointPossibilities: []*jobspb.TimestampSpansMap{nil},
		},
		"no spans checkpointed because all spans are at frontier": {
			frontier: ts(1),
			spans: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")}, ts: ts(1)},
			},
			maxBytes:                        100,
			expectedCheckpointPossibilities: []*jobspb.TimestampSpansMap{nil},
		},
		"adjacent spans ahead of frontier merged before being checkpointed": {
			frontier: ts(1),
			spans: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: ts(2)},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: ts(2)},
				{span: roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")}, ts: ts(1)},
			},
			maxBytes: 100,
			expectedCheckpointPossibilities: []*jobspb.TimestampSpansMap{
				jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
					ts(2): {{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")}},
				}),
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			aggMetrics := checkpoint.NewAggMetrics(aggmetric.MakeBuilder())

			actualCheckpoint := checkpoint.Make(
				tc.frontier,
				tc.spans.All(),
				tc.maxBytes,
				aggMetrics.AddChild(),
			)
			require.Condition(t, func() bool {
				for _, expectedCheckpoint := range tc.expectedCheckpointPossibilities {
					if expectedCheckpoint.Equal(actualCheckpoint) {
						return true
					}
				}
				return false
			})

			// Verify that metrics were set/not set based on whether a
			// checkpoint was created.
			if actualCheckpoint != nil {
				require.Greater(t, aggMetrics.CreateNanos.CumulativeSnapshot().Mean(), float64(0))
				require.Equal(t, aggMetrics.TotalBytes.CumulativeSnapshot().Mean(), float64(actualCheckpoint.Size()))
				require.Equal(t, aggMetrics.TimestampCount.CumulativeSnapshot().Mean(), float64(actualCheckpoint.TimestampCount()))
				require.Equal(t, aggMetrics.SpanCount.CumulativeSnapshot().Mean(), float64(actualCheckpoint.SpanCount()))
			} else {
				require.True(t, math.IsNaN(aggMetrics.CreateNanos.CumulativeSnapshot().Mean()))
				require.True(t, math.IsNaN(aggMetrics.TotalBytes.CumulativeSnapshot().Mean()))
				require.True(t, math.IsNaN(aggMetrics.TimestampCount.CumulativeSnapshot().Mean()))
				require.True(t, math.IsNaN(aggMetrics.SpanCount.CumulativeSnapshot().Mean()))
			}
		})
	}
}

func TestCheckpointRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(wt int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: wt}
	}

	for name, tc := range map[string]struct {
		trackedSpans              roachpb.Spans
		initialHighWater          hlc.Timestamp
		checkpointToRestore       *jobspb.TimestampSpansMap
		expectedCheckpointedSpans checkpointSpans
		expectedError             string
	}{
		"restoring checkpoint with single timestamp": {
			trackedSpans:     roachpb.Spans{{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}},
			initialHighWater: ts(1),
			checkpointToRestore: jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
				ts(2): {{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}},
			}),
			expectedCheckpointedSpans: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: ts(2)},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: ts(2)},
			},
		},
		"restoring checkpoint with multiple timestamps": {
			trackedSpans:     roachpb.Spans{{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}},
			initialHighWater: ts(0),
			checkpointToRestore: jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
				ts(2): {{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}},
				ts(1): {{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")}},
			}),
			expectedCheckpointedSpans: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: ts(2)},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: ts(2)},
				{span: roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")}, ts: ts(1)},
			},
		},
		"restoring checkpoint containing empty timestamp (developer error)": {
			trackedSpans:     roachpb.Spans{{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}},
			initialHighWater: ts(0),
			checkpointToRestore: jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
				ts(2): {{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}},
				ts(0): {{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")}},
			}),
			expectedError: "checkpoint timestamp is empty",
		},
	} {
		t.Run(name, func(t *testing.T) {
			actualFrontier, err := span.MakeFrontierAt(tc.initialHighWater, tc.trackedSpans...)
			require.NoError(t, err)
			err = checkpoint.Restore(actualFrontier, tc.checkpointToRestore)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
				return
			}
			require.NoError(t, err)

			actualFrontierSpans := checkpointSpans{}
			for sp, ts := range actualFrontier.Entries() {
				actualFrontierSpans = append(actualFrontierSpans, checkpointSpan{span: sp, ts: ts})
			}

			expectedFrontierSpans := checkpointSpans{}
			expectedFrontier, err := span.MakeFrontierAt(tc.initialHighWater, tc.trackedSpans...)
			require.NoError(t, err)
			for _, s := range tc.expectedCheckpointedSpans {
				_, err = expectedFrontier.Forward(s.span, s.ts)
				require.NoError(t, err)
			}
			for sp, ts := range expectedFrontier.Entries() {
				expectedFrontierSpans = append(expectedFrontierSpans, checkpointSpan{span: sp, ts: ts})
			}
			require.Equal(t, expectedFrontierSpans, actualFrontierSpans)
		})
	}
}

func TestCheckpointMakeRestoreRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(wt int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: wt}
	}

	for name, tc := range map[string]struct {
		trackedSpans             roachpb.Spans
		frontier                 hlc.Timestamp
		spans                    checkpointSpans
		expectedSpansIfDifferent checkpointSpans
	}{
		"some spans ahead of frontier": {
			trackedSpans: roachpb.Spans{{Key: roachpb.Key("a"), EndKey: roachpb.Key("e")}},
			frontier:     ts(1),
			spans: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: ts(2)},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")}, ts: ts(4)},
			},
		},
		"some spans ahead of frontier with some spans needing to be merged": {
			trackedSpans: roachpb.Spans{{Key: roachpb.Key("a"), EndKey: roachpb.Key("e")}},
			spans: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: ts(2)},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: ts(2)},
				{span: roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")}, ts: ts(1)},
			},
			expectedSpansIfDifferent: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: ts(1)},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")}, ts: ts(2)},
				{span: roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")}, ts: ts(1)},
			},
		},
		"no spans ahead of frontier": {
			trackedSpans: roachpb.Spans{{Key: roachpb.Key("a"), EndKey: roachpb.Key("e")}},
			frontier:     ts(1),
			spans: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("e")}, ts: ts(1)},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			cp := checkpoint.Make(
				tc.frontier,
				tc.spans.All(),
				changefeedbase.SpanCheckpointMaxBytes.Default(),
				nil, /* metrics */
			)

			restoredSpans := func() checkpointSpans {
				var spans checkpointSpans
				restoredFrontier, err := span.MakeFrontierAt(tc.frontier, tc.trackedSpans...)
				require.NoError(t, err)
				require.NoError(t, checkpoint.Restore(restoredFrontier, cp))
				for sp, ts := range restoredFrontier.Entries() {
					spans = append(spans, checkpointSpan{span: sp, ts: ts})
				}
				return spans
			}()

			if tc.expectedSpansIfDifferent == nil {
				require.ElementsMatch(t, tc.spans, restoredSpans)
			} else {
				require.ElementsMatch(t, tc.expectedSpansIfDifferent, restoredSpans)
			}
		})
	}
}
