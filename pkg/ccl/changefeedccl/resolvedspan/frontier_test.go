// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package resolvedspan_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/resolvedspan"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestAggregatorFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Create a fresh frontier with no progress.
	statementTime := makeTS(10)
	var initialHighwater hlc.Timestamp
	f, err := resolvedspan.NewAggregatorFrontier(
		statementTime,
		initialHighwater,
		makeSpan("a", "f"),
	)
	require.NoError(t, err)
	require.Equal(t, initialHighwater, f.Frontier())

	// Forward spans representing initial scan.
	testBackfillSpan(t, ctx, f, "a", "b", statementTime, initialHighwater)
	testBackfillSpan(t, ctx, f, "b", "f", statementTime, statementTime)

	// Forward spans signalling a backfill is required.
	backfillTS := makeTS(20)
	testBoundarySpan(t, ctx, f, "a", "b", backfillTS.Prev(), jobspb.ResolvedSpan_BACKFILL, statementTime)
	testBoundarySpan(t, ctx, f, "b", "c", backfillTS.Prev(), jobspb.ResolvedSpan_BACKFILL, statementTime)
	testBoundarySpan(t, ctx, f, "c", "d", backfillTS.Prev(), jobspb.ResolvedSpan_BACKFILL, statementTime)
	testBoundarySpan(t, ctx, f, "d", "e", backfillTS.Prev(), jobspb.ResolvedSpan_BACKFILL, statementTime)
	testBoundarySpan(t, ctx, f, "e", "f", backfillTS.Prev(), jobspb.ResolvedSpan_BACKFILL, backfillTS.Prev())

	// Confirm that attempting to signal an earlier boundary causes an assertion error.
	illegalBoundaryTS := makeTS(15)
	testIllegalBoundarySpan(t, ctx, f, "a", "f", illegalBoundaryTS, jobspb.ResolvedSpan_BACKFILL)
	testIllegalBoundarySpan(t, ctx, f, "a", "f", illegalBoundaryTS, jobspb.ResolvedSpan_RESTART)
	testIllegalBoundarySpan(t, ctx, f, "a", "f", illegalBoundaryTS, jobspb.ResolvedSpan_EXIT)

	// Forward spans representing actual backfill.
	testBackfillSpan(t, ctx, f, "d", "e", backfillTS, backfillTS.Prev())
	testBackfillSpan(t, ctx, f, "e", "f", backfillTS, backfillTS.Prev())
	testBackfillSpan(t, ctx, f, "a", "d", backfillTS, backfillTS)

	// Forward spans signalling a restart is required.
	restartTS := makeTS(30)
	testBoundarySpan(t, ctx, f, "a", "b", restartTS.Prev(), jobspb.ResolvedSpan_RESTART, backfillTS)
	testBoundarySpan(t, ctx, f, "b", "f", restartTS.Prev(), jobspb.ResolvedSpan_RESTART, restartTS.Prev())

	// Simulate restarting by creating a new frontier with the initial highwater
	// set to the previous frontier timestamp.
	initialHighwater = restartTS.Prev()
	f, err = resolvedspan.NewAggregatorFrontier(
		statementTime,
		initialHighwater,
		makeSpan("a", "f"),
	)
	require.NoError(t, err)

	// Forward spans representing post-restart backfill.
	testBackfillSpan(t, ctx, f, "a", "b", restartTS, initialHighwater)
	testBackfillSpan(t, ctx, f, "e", "f", restartTS, initialHighwater)
	testBackfillSpan(t, ctx, f, "b", "e", restartTS, restartTS)

	// Forward spans signalling an exit is required.
	exitTS := makeTS(40)
	testBoundarySpan(t, ctx, f, "a", "f", exitTS.Prev(), jobspb.ResolvedSpan_EXIT, exitTS.Prev())
}

func TestCoordinatorFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Create a fresh frontier with no progress.
	statementTime := makeTS(10)
	var initialHighwater hlc.Timestamp
	f, err := resolvedspan.NewCoordinatorFrontier(
		statementTime,
		initialHighwater,
		makeSpan("a", "f"),
	)
	require.NoError(t, err)
	require.Equal(t, initialHighwater, f.Frontier())

	// Forward spans representing initial scan.
	testBackfillSpan(t, ctx, f, "a", "b", statementTime, initialHighwater)
	testBackfillSpan(t, ctx, f, "b", "f", statementTime, statementTime)

	// Forward span signalling a backfill is required.
	backfillTS1 := makeTS(15)
	testBoundarySpan(t, ctx, f, "a", "b", backfillTS1.Prev(), jobspb.ResolvedSpan_BACKFILL, statementTime)

	// Forward span signalling another backfill is required (simulates multiple
	// aggregators progressing at different speeds).
	backfillTS2 := makeTS(20)
	testBackfillSpan(t, ctx, f, "a", "b", backfillTS1, statementTime)
	testBoundarySpan(t, ctx, f, "a", "b", backfillTS2.Prev(), jobspb.ResolvedSpan_BACKFILL, statementTime)

	// Verify that spans signalling backfills at earlier timestamp are allowed.
	testBoundarySpan(t, ctx, f, "b", "c", backfillTS1.Prev(), jobspb.ResolvedSpan_BACKFILL, statementTime)

	// Verify that no other boundary spans at earlier timestamp are allowed.
	testIllegalBoundarySpan(t, ctx, f, "a", "f", backfillTS1.Prev(), jobspb.ResolvedSpan_RESTART)
	testIllegalBoundarySpan(t, ctx, f, "a", "f", backfillTS1.Prev(), jobspb.ResolvedSpan_EXIT)

	// Forward spans completing first backfill and signalling and completing second backfill.
	testBackfillSpan(t, ctx, f, "b", "f", backfillTS1, backfillTS1)
	testBoundarySpan(t, ctx, f, "b", "f", backfillTS2.Prev(), jobspb.ResolvedSpan_BACKFILL, backfillTS2.Prev())
	testBackfillSpan(t, ctx, f, "a", "f", backfillTS2, backfillTS2)

	// Forward spans signalling a restart is required.
	restartTS := makeTS(30)
	testBoundarySpan(t, ctx, f, "a", "b", restartTS.Prev(), jobspb.ResolvedSpan_RESTART, backfillTS2)
	testBoundarySpan(t, ctx, f, "b", "f", restartTS.Prev(), jobspb.ResolvedSpan_RESTART, restartTS.Prev())

	// Simulate restarting by creating a new frontier with the initial highwater
	// set to the previous frontier timestamp.
	initialHighwater = restartTS.Prev()
	f, err = resolvedspan.NewCoordinatorFrontier(
		statementTime,
		initialHighwater,
		makeSpan("a", "f"),
	)
	require.NoError(t, err)

	// Forward spans representing post-restart backfill.
	testBackfillSpan(t, ctx, f, "a", "b", restartTS, initialHighwater)
	testBackfillSpan(t, ctx, f, "e", "f", restartTS, initialHighwater)
	testBackfillSpan(t, ctx, f, "b", "e", restartTS, restartTS)

	// Forward spans signalling an exit is required.
	exitTS := makeTS(40)
	testBoundarySpan(t, ctx, f, "a", "f", exitTS.Prev(), jobspb.ResolvedSpan_EXIT, exitTS.Prev())
}

type frontier interface {
	Frontier() hlc.Timestamp
	ForwardResolvedSpan(context.Context, jobspb.ResolvedSpan) (bool, error)
	InBackfill(jobspb.ResolvedSpan) bool
	AtBoundary() (bool, jobspb.ResolvedSpan_BoundaryType, hlc.Timestamp)
}

func testBackfillSpan(
	t *testing.T,
	ctx context.Context,
	f frontier,
	start, end string,
	ts hlc.Timestamp,
	frontierAfterSpan hlc.Timestamp,
) {
	backfillSpan := makeResolvedSpan(start, end, ts, jobspb.ResolvedSpan_NONE)
	require.True(t, f.InBackfill(backfillSpan))
	_, err := f.ForwardResolvedSpan(ctx, backfillSpan)
	require.NoError(t, err)
	require.Equal(t, frontierAfterSpan, f.Frontier())
}

func testBoundarySpan(
	t *testing.T,
	ctx context.Context,
	f frontier,
	start, end string,
	boundaryTS hlc.Timestamp,
	boundaryType jobspb.ResolvedSpan_BoundaryType,
	frontierAfterSpan hlc.Timestamp,
) {
	boundarySpan := makeResolvedSpan(start, end, boundaryTS, boundaryType)
	_, err := f.ForwardResolvedSpan(ctx, boundarySpan)
	require.NoError(t, err)

	if finalBoundarySpan := frontierAfterSpan.Equal(boundaryTS); finalBoundarySpan {
		atBoundary, bType, bTS := f.AtBoundary()
		require.True(t, atBoundary)
		require.Equal(t, boundaryType, bType)
		require.Equal(t, boundaryTS, bTS)
		if f, ok := f.(*resolvedspan.AggregatorFrontier); ok {
			f.EntriesWithBoundaryType(func(
				_ roachpb.Span, entryTS hlc.Timestamp, entryBoundaryType jobspb.ResolvedSpan_BoundaryType,
			) (done span.OpResult) {
				require.Equal(t, boundaryTS, entryTS)
				require.Equal(t, boundaryType, entryBoundaryType)
				return span.ContinueMatch
			})
		}
	} else {
		atBoundary, _, _ := f.AtBoundary()
		require.False(t, atBoundary)
		if f, ok := f.(*resolvedspan.AggregatorFrontier); ok {
			f.EntriesWithBoundaryType(func(
				s roachpb.Span, entryTS hlc.Timestamp, entryBoundaryType jobspb.ResolvedSpan_BoundaryType,
			) (done span.OpResult) {
				if s.Contains(makeSpan(start, end)) {
					require.Equal(t, boundaryTS, entryTS)
					require.Equal(t, boundaryType, entryBoundaryType)
					return span.StopMatch
				}
				return span.ContinueMatch
			})
		}
	}
}

func testIllegalBoundarySpan(
	t *testing.T,
	ctx context.Context,
	f frontier,
	start, end string,
	boundaryTS hlc.Timestamp,
	boundaryType jobspb.ResolvedSpan_BoundaryType,
) {
	boundarySpan := makeResolvedSpan(start, end, boundaryTS, boundaryType)
	_, err := f.ForwardResolvedSpan(ctx, boundarySpan)
	require.True(t, errors.HasAssertionFailure(err))
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
