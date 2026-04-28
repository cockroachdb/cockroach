// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cloud/inmemstorage"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogjob"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// twoSpans returns two non-overlapping test spans plus an iterator
// helper for the values written into each.
func twoSpans() (roachpb.Span, roachpb.Span) {
	return roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
		roachpb.Span{Key: roachpb.Key("m"), EndKey: roachpb.Key("z")}
}

func newMultiSpanDriver(
	t *testing.T,
) (*revlogjob.Driver, *inmemstorage.Storage, roachpb.Span, roachpb.Span) {
	t.Helper()
	es := inmemstorage.New()
	t.Cleanup(func() { _ = es.Close() })
	left, right := twoSpans()
	d, err := revlogjob.NewDriver(es, []roachpb.Span{left, right}, ts(100),
		testTickWidth, &seqFileIDs{}, revlogjob.ResumeState{})
	require.NoError(t, err)
	return d, es.(*inmemstorage.Storage), left, right
}

// TestDriverLaggingSpanGatesTickClose verifies that with multiple
// watched spans, a checkpoint for only one span does NOT close the
// tick — the manager's data frontier is the min across all spans, so
// a lagging contributor holds the close. Once the lagging span
// reports past the tick boundary, the tick closes.
//
// This is the core multi-producer correctness invariant: every
// per-span contributor must have advanced past `tick_end` before the
// manifest can claim "every change for the tick was flushed."
func TestDriverLaggingSpanGatesTickClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	d, es, left, right := newMultiSpanDriver(t)

	// Both spans see one event mid-tick.
	d.OnValue(ctx, roachpb.Key("b"), ts(105), []byte("L"), nil)
	d.OnValue(ctx, roachpb.Key("p"), ts(107), []byte("R"), nil)

	// Only left's checkpoint advances past the tick boundary; right
	// stays at startHLC. Tick must NOT close.
	require.NoError(t, d.OnCheckpoint(ctx, left, ts(115)))

	require.Empty(t, readBackTicks(t, ctx, es, ts(99), ts(120)),
		"tick must not close while right span is still at startHLC")

	// Now right catches up. Tick closes; both events appear.
	require.NoError(t, d.OnCheckpoint(ctx, right, ts(115)))

	ticks := readBackTicks(t, ctx, es, ts(99), ts(120))
	require.Len(t, ticks, 1)
	require.Equal(t, ts(110), ticks[0].EndTime)
	events := readBackEvents(t, ctx, es, ticks[0])
	require.Len(t, events, 2)
	keys := []string{string(events[0].Key), string(events[1].Key)}
	require.ElementsMatch(t, []string{"b", "p"}, keys)
}

// TestDriverPartialCheckpointAdvancesFrontier verifies that a
// partial checkpoint (advances one span without closing a tick) is
// still recorded — when the second span eventually catches up, the
// already-advanced span doesn't need its checkpoint re-delivered.
func TestDriverPartialCheckpointAdvancesFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	d, es, left, right := newMultiSpanDriver(t)

	// Advance left way ahead of right via two checkpoints; no events.
	require.NoError(t, d.OnCheckpoint(ctx, left, ts(115)))
	require.NoError(t, d.OnCheckpoint(ctx, left, ts(125)))

	require.Empty(t, readBackTicks(t, ctx, es, ts(99), ts(130)),
		"no tick should close while right is still at startHLC")

	// Now bring right up past both tick boundaries in one checkpoint.
	require.NoError(t, d.OnCheckpoint(ctx, right, ts(125)))

	ticks := readBackTicks(t, ctx, es, ts(99), ts(130))
	require.Len(t, ticks, 2,
		"both ticks should close once right catches up")
	require.Equal(t, ts(110), ticks[0].EndTime)
	require.Equal(t, ts(120), ticks[1].EndTime)
}

// TestDriverPerSpanFrontierIndependent verifies that events stamped
// at ts <= one span's frontier are still flushed if the other span's
// frontier hasn't advanced past them. The producer's per-span
// frontier filters arrivals from the rangefeed (the "this span has
// already reported past this ts" check) but mid-tick events for an
// already-advanced span are not retroactively dropped if they were
// buffered before the tick closed.
//
// Concretely: event for left at ts=105 arrives, left checkpoints to
// 115, right is still at startHLC. The buffered event for left
// should appear in the eventual closed tick.
func TestDriverPerSpanFrontierIndependent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	d, es, left, right := newMultiSpanDriver(t)

	d.OnValue(ctx, roachpb.Key("c"), ts(105), []byte("L"), nil)
	require.NoError(t, d.OnCheckpoint(ctx, left, ts(115)))
	// right still at startHLC; nothing closes yet.
	require.Empty(t, readBackTicks(t, ctx, es, ts(99), ts(120)))

	require.NoError(t, d.OnCheckpoint(ctx, right, ts(115)))
	ticks := readBackTicks(t, ctx, es, ts(99), ts(120))
	require.Len(t, ticks, 1)
	events := readBackEvents(t, ctx, es, ticks[0])
	require.Len(t, events, 1)
	require.Equal(t, "c", string(events[0].Key))
}
