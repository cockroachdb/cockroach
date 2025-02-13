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
				func(fn span.Operation) {
					for _, sp := range tc.spans {
						fn(sp.span, sp.ts)
					}
				},
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
				require.Equal(t, aggMetrics.SpanCount.CumulativeSnapshot().Mean(), float64(actualCheckpoint.SpanCount()))
			} else {
				require.True(t, math.IsNaN(aggMetrics.CreateNanos.CumulativeSnapshot().Mean()))
				require.True(t, math.IsNaN(aggMetrics.TotalBytes.CumulativeSnapshot().Mean()))
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
			actualFrontier.Entries(func(sp roachpb.Span, ts hlc.Timestamp) span.OpResult {
				actualFrontierSpans = append(actualFrontierSpans, checkpointSpan{span: sp, ts: ts})
				return span.ContinueMatch
			})

			expectedFrontierSpans := checkpointSpans{}
			expectedFrontier, err := span.MakeFrontierAt(tc.initialHighWater, tc.trackedSpans...)
			require.NoError(t, err)
			for _, s := range tc.expectedCheckpointedSpans {
				_, err = expectedFrontier.Forward(s.span, s.ts)
				require.NoError(t, err)
			}
			expectedFrontier.Entries(func(sp roachpb.Span, ts hlc.Timestamp) span.OpResult {
				expectedFrontierSpans = append(expectedFrontierSpans, checkpointSpan{span: sp, ts: ts})
				return span.ContinueMatch
			})
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
				func(fn span.Operation) {
					for _, sp := range tc.spans {
						fn(sp.span, sp.ts)
					}
				},
				changefeedbase.SpanCheckpointMaxBytes.Default(),
				nil, /* metrics */
			)

			restoredSpans := func() checkpointSpans {
				var spans checkpointSpans
				restoredFrontier, err := span.MakeFrontierAt(tc.frontier, tc.trackedSpans...)
				require.NoError(t, err)
				require.NoError(t, checkpoint.Restore(restoredFrontier, cp))
				restoredFrontier.Entries(func(sp roachpb.Span, ts hlc.Timestamp) (done span.OpResult) {
					spans = append(spans, checkpointSpan{span: sp, ts: ts})
					return span.ContinueMatch
				})
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

func TestConvertFromLegacyCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for name, tc := range map[string]struct {
		//lint:ignore SA1019 deprecated usage
		legacyCheckpoint *jobspb.ChangefeedProgress_Checkpoint
		statementTime    hlc.Timestamp
		initialHighWater hlc.Timestamp
		expected         *jobspb.TimestampSpansMap
	}{
		"nil legacy checkpoint": {
			legacyCheckpoint: nil,
			expected:         nil,
		},
		"zero legacy checkpoint": {
			//lint:ignore SA1019 deprecated usage
			legacyCheckpoint: &jobspb.ChangefeedProgress_Checkpoint{},
			expected:         nil,
		},
		"legacy checkpoint with empty timestamp and empty initial highwater": {
			//lint:ignore SA1019 deprecated usage
			legacyCheckpoint: &jobspb.ChangefeedProgress_Checkpoint{
				Spans: roachpb.Spans{
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
				},
			},
			statementTime: hlc.Timestamp{WallTime: 50},
			expected: jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
				{WallTime: 50}: {
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
				},
			}),
		},
		"legacy checkpoint with empty timestamp and non-empty initial highwater": {
			//lint:ignore SA1019 deprecated usage
			legacyCheckpoint: &jobspb.ChangefeedProgress_Checkpoint{
				Spans: roachpb.Spans{
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
				},
			},
			statementTime:    hlc.Timestamp{WallTime: 50},
			initialHighWater: hlc.Timestamp{WallTime: 100},
			expected: jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
				hlc.Timestamp{WallTime: 100}.Next(): {
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
				},
			}),
		},
		"legacy checkpoint with non-empty timestamp": {
			//lint:ignore SA1019 deprecated usage
			legacyCheckpoint: &jobspb.ChangefeedProgress_Checkpoint{
				Spans: roachpb.Spans{
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
				},
				Timestamp: hlc.Timestamp{WallTime: 200},
			},
			statementTime:    hlc.Timestamp{WallTime: 50},
			initialHighWater: hlc.Timestamp{WallTime: 100},
			expected: jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
				{WallTime: 200}: {
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
				},
			}),
		},
	} {
		t.Run(name, func(t *testing.T) {
			actual := checkpoint.ConvertFromLegacyCheckpoint(tc.legacyCheckpoint, tc.statementTime, tc.initialHighWater)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestConvertToLegacyCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for name, tc := range map[string]struct {
		cp *jobspb.TimestampSpansMap
		//lint:ignore SA1019 deprecated usage
		expected *jobspb.ChangefeedProgress_Checkpoint
	}{
		"nil checkpoint": {
			cp:       nil,
			expected: nil,
		},
		"checkpoint with single timestamp": {
			cp: jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
				{WallTime: 50}: {
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
				},
			}),
			//lint:ignore SA1019 deprecated usage
			expected: &jobspb.ChangefeedProgress_Checkpoint{
				Spans: roachpb.Spans{
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
				},
				Timestamp: hlc.Timestamp{WallTime: 50},
			},
		},
		"checkpoint with multiple timestamps": {
			cp: jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
				{WallTime: 50}: {
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
				},
				{WallTime: 100}: {
					roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
				},
			}),
			//lint:ignore SA1019 deprecated usage
			expected: &jobspb.ChangefeedProgress_Checkpoint{
				Spans: roachpb.Spans{
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("d")},
				},
				Timestamp: hlc.Timestamp{WallTime: 50},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			actual := checkpoint.ConvertToLegacyCheckpoint(tc.cp)
			require.Equal(t, tc.expected, actual)
		})
	}
}

// TestLegacyCheckpointCatchupTime generates 100 random non-overlapping spans with random
// timestamps within a minute of each other and turns them into checkpoint
// spans. It then does some sanity checks. It also compares the total
// catchup time between the checkpoint timestamp and the high watermark.
// Although the test relies on internal implementation details, it is a
// good base to explore other fine-grained checkpointing algorithms.
func TestLegacyCheckpointCatchupTime(t *testing.T) {
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
	cp := checkpoint.ConvertToLegacyCheckpoint(checkpoint.Make(hwm, forEachSpan, maxBytes, nil /* metrics */))
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
