// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogjob"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// spanA and spanB are two disjoint spans used to simulate
// multi-producer setups where each producer covers a subset.
var (
	spanA = roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")}
	spanB = roachpb.Span{Key: roachpb.Key("m"), EndKey: roachpb.Key("z")}
)

// TestTickManagerMultiProducerFlush verifies that two producers
// covering disjoint spans can both contribute files and checkpoints
// to the same tick, and the tick only closes once both spans'
// frontiers cross the boundary.
func TestTickManagerMultiProducerFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	es := newTestStorage(t)
	defer es.Close()
	ids := &seqFileIDs{}
	mgr, err := revlogjob.NewTickManager(
		es, []roachpb.Span{spanA, spanB}, ts(100), testTickWidth, ids,
	)
	require.NoError(t, err)
	mgr.DisableDescFrontier()

	// Producer A writes a file and advances spanA past tick boundary.
	prodA, err := revlogjob.NewProducer(
		es, []roachpb.Span{spanA}, ts(100), testTickWidth, ids, mgr,
		revlogjob.ResumeState{}, 0,
	)
	require.NoError(t, err)
	prodA.OnValue(ctx, roachpb.Key("c"), ts(105), []byte("vA"), nil)
	require.NoError(t, prodA.OnCheckpoint(ctx, spanA, ts(111)))

	// Tick should NOT be closed yet — spanB is still at startHLC.
	lr := revlog.NewLogReader(es)
	var ticks []revlog.Tick
	for tk, tickErr := range lr.Ticks(ctx, ts(99), ts(120)) {
		require.NoError(t, tickErr)
		ticks = append(ticks, tk)
	}
	require.Empty(t, ticks, "tick should not close until both spans advance")

	// Producer B writes a file and advances spanB past tick boundary.
	prodB, err := revlogjob.NewProducer(
		es, []roachpb.Span{spanB}, ts(100), testTickWidth, ids, mgr,
		revlogjob.ResumeState{}, 0,
	)
	require.NoError(t, err)
	prodB.OnValue(ctx, roachpb.Key("n"), ts(107), []byte("vB"), nil)
	require.NoError(t, prodB.OnCheckpoint(ctx, spanB, ts(111)))

	// Now tick should be closed — both spans past ts(110).
	ticks = nil
	for tk, tickErr := range lr.Ticks(ctx, ts(99), ts(120)) {
		require.NoError(t, tickErr)
		ticks = append(ticks, tk)
	}
	require.Len(t, ticks, 1)
	require.Equal(t, ts(110), ticks[0].EndTime)
	require.Len(t, ticks[0].Manifest.Files, 2,
		"tick should have files from both producers")
}

// TestTickManagerCoalesceMergeFromMultipleProducers verifies that
// coalesce contributions from two producers for the same tick are
// merged and appear in the tick's manifest (either as InlineTail
// or as a promoted file).
func TestTickManagerCoalesceMergeFromMultipleProducers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	es := newTestStorage(t)
	defer es.Close()
	ids := &seqFileIDs{}
	mgr, err := revlogjob.NewTickManager(
		es, []roachpb.Span{spanA, spanB}, ts(100), testTickWidth, ids,
	)
	require.NoError(t, err)
	mgr.DisableDescFrontier()

	// Both producers use the coalesce path (forwardThreshold = 1MB).
	prodA, err := revlogjob.NewProducer(
		es, []roachpb.Span{spanA}, ts(100), testTickWidth, ids, mgr,
		revlogjob.ResumeState{}, 1<<20,
	)
	require.NoError(t, err)
	prodB, err := revlogjob.NewProducer(
		es, []roachpb.Span{spanB}, ts(100), testTickWidth, ids, mgr,
		revlogjob.ResumeState{}, 1<<20,
	)
	require.NoError(t, err)

	prodA.OnValue(ctx, roachpb.Key("c"), ts(105), []byte("vA"), nil)
	prodA.OnValue(ctx, roachpb.Key("d"), ts(106), []byte("vA2"), nil)
	require.NoError(t, prodA.OnCheckpoint(ctx, spanA, ts(111)))

	prodB.OnValue(ctx, roachpb.Key("n"), ts(107), []byte("vB"), nil)
	require.NoError(t, prodB.OnCheckpoint(ctx, spanB, ts(111)))

	// Both producers contributed coalesces; tick should be closed
	// with the merged entries appearing inline.
	lr := revlog.NewLogReader(es)
	var ticks []revlog.Tick
	for tk, tickErr := range lr.Ticks(ctx, ts(99), ts(120)) {
		require.NoError(t, tickErr)
		ticks = append(ticks, tk)
	}
	require.Len(t, ticks, 1)
	require.Empty(t, ticks[0].Manifest.Files,
		"small coalesces should be inlined, not written as files")
	require.Len(t, ticks[0].Manifest.InlineTail, 3,
		"merged inline tail should have all 3 events")

	// Verify events are readable and in key order.
	events := readBackEvents(t, ctx, es, ticks[0])
	require.Len(t, events, 3)
	require.Equal(t, "c", string(events[0].Key))
	require.Equal(t, "d", string(events[1].Key))
	require.Equal(t, "n", string(events[2].Key))
}

// TestTickManagerRehydrateAndResume verifies that the TickManager
// can be rehydrated from a persisted State and that subsequent
// flushes continue from where the prior incarnation left off.
func TestTickManagerRehydrateAndResume(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	es := newTestStorage(t)
	defer es.Close()
	ids := &seqFileIDs{}

	// First incarnation: write one tick, leave another open.
	mgr1, err := revlogjob.NewTickManager(
		es, []roachpb.Span{allSpan}, ts(100), testTickWidth, ids,
	)
	require.NoError(t, err)
	mgr1.DisableDescFrontier()

	prod1, err := revlogjob.NewProducer(
		es, []roachpb.Span{allSpan}, ts(100), testTickWidth, ids, mgr1,
		revlogjob.ResumeState{}, 0,
	)
	require.NoError(t, err)
	prod1.OnValue(ctx, roachpb.Key("a"), ts(105), []byte("v1"), nil)
	prod1.OnValue(ctx, roachpb.Key("b"), ts(115), []byte("v2"), nil)
	require.NoError(t, prod1.OnCheckpoint(ctx, allSpan, ts(115)))

	// ts(110) tick should be closed; ts(120) tick has "b" buffered
	// but not closed. Verify first tick closed.
	lr := revlog.NewLogReader(es)
	var ticks []revlog.Tick
	for tk, tickErr := range lr.Ticks(ctx, ts(99), ts(120)) {
		require.NoError(t, tickErr)
		ticks = append(ticks, tk)
	}
	require.Len(t, ticks, 1)
	require.Equal(t, ts(110), ticks[0].EndTime)

	// Simulate checkpoint: snapshot the manager's state.
	resumeState := mgr1.ResumeForPartition([]roachpb.Span{allSpan})

	// Second incarnation: rehydrate and continue.
	mgr2, err := revlogjob.NewTickManager(
		es, []roachpb.Span{allSpan}, ts(100), testTickWidth, ids,
	)
	require.NoError(t, err)
	mgr2.DisableDescFrontier()

	// Rehydrate from the persisted state.
	state := revlogjob.State{
		HighWater: ts(110),
	}
	require.NoError(t, mgr2.Rehydrate(state))

	// New producer starts with resume state — its flushorder
	// should continue past any prior files.
	prod2, err := revlogjob.NewProducer(
		es, []roachpb.Span{allSpan}, ts(100), testTickWidth, ids, mgr2,
		resumeState, 0,
	)
	require.NoError(t, err)
	prod2.OnValue(ctx, roachpb.Key("c"), ts(118), []byte("v3"), nil)
	require.NoError(t, prod2.OnCheckpoint(ctx, allSpan, ts(121)))

	// Now ts(120) tick should close (from the new producer).
	// Query from ts(119) to avoid seeing the already-closed ts(110).
	ticks = nil
	for tk, tickErr := range lr.Ticks(ctx, ts(119), ts(130)) {
		require.NoError(t, tickErr)
		ticks = append(ticks, tk)
	}
	require.Len(t, ticks, 1)
	require.Equal(t, ts(120), ticks[0].EndTime)
	require.Equal(t, ts(110), ticks[0].Manifest.TickStart)
}

// TestTickManagerDescFrontierGatesClose verifies that the close
// loop respects both the data frontier AND the descriptor frontier.
func TestTickManagerDescFrontierGatesClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	es := newTestStorage(t)
	defer es.Close()
	ids := &seqFileIDs{}
	mgr, err := revlogjob.NewTickManager(
		es, []roachpb.Span{allSpan}, ts(100), testTickWidth, ids,
	)
	require.NoError(t, err)
	// Don't disable desc frontier — we want to test gating.

	prod, err := revlogjob.NewProducer(
		es, []roachpb.Span{allSpan}, ts(100), testTickWidth, ids, mgr,
		revlogjob.ResumeState{}, 0,
	)
	require.NoError(t, err)

	// Data frontier advances past ts(110), but desc frontier is
	// still at startHLC = ts(100).
	prod.OnValue(ctx, roachpb.Key("a"), ts(105), []byte("v"), nil)
	require.NoError(t, prod.OnCheckpoint(ctx, allSpan, ts(115)))

	// Tick should NOT close — desc frontier blocks it.
	lr := revlog.NewLogReader(es)
	var ticks []revlog.Tick
	for tk, tickErr := range lr.Ticks(ctx, ts(99), ts(120)) {
		require.NoError(t, tickErr)
		ticks = append(ticks, tk)
	}
	require.Empty(t, ticks, "desc frontier should gate tick close")

	// Advance desc frontier past the tick boundary.
	require.NoError(t, mgr.ForwardDescFrontier(ctx, ts(115)))

	ticks = nil
	for tk, tickErr := range lr.Ticks(ctx, ts(99), ts(120)) {
		require.NoError(t, tickErr)
		ticks = append(ticks, tk)
	}
	require.Len(t, ticks, 1)
	require.Equal(t, ts(110), ticks[0].EndTime)
}

// TestTickManagerFrontierAdvanceHook verifies that the
// afterFrontierAdvance callback fires when the aggregate frontier
// moves forward and does NOT fire on duplicate checkpoints.
func TestTickManagerFrontierAdvanceHook(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	es := newTestStorage(t)
	defer es.Close()
	ids := &seqFileIDs{}
	mgr, err := revlogjob.NewTickManager(
		es, []roachpb.Span{allSpan}, ts(100), testTickWidth, ids,
	)
	require.NoError(t, err)
	mgr.DisableDescFrontier()

	var advances []hlc.Timestamp
	mgr.SetAfterFrontierAdvance(func(_ context.Context, frontier hlc.Timestamp) {
		advances = append(advances, frontier)
	})

	// First flush — frontier moves from ts(100) to ts(105).
	require.NoError(t, mgr.Flush(ctx, &revlogpb.Flush{
		Checkpoints: checkpoints(allSpan, ts(105)),
	}))
	require.Len(t, advances, 1)
	require.Equal(t, ts(105), advances[0])

	// Same checkpoint again — no forward movement, no hook fire.
	require.NoError(t, mgr.Flush(ctx, &revlogpb.Flush{
		Checkpoints: checkpoints(allSpan, ts(105)),
	}))
	require.Len(t, advances, 1, "hook should not fire on no-op forward")

	// Frontier advances again.
	require.NoError(t, mgr.Flush(ctx, &revlogpb.Flush{
		Checkpoints: checkpoints(allSpan, ts(108)),
	}))
	require.Len(t, advances, 2)
	require.Equal(t, ts(108), advances[1])
}

// TestTickManagerAddSpansAt verifies that dynamically adding spans
// to the frontier causes the close loop to wait for those spans.
func TestTickManagerAddSpansAt(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	es := newTestStorage(t)
	defer es.Close()
	ids := &seqFileIDs{}
	mgr, err := revlogjob.NewTickManager(
		es, []roachpb.Span{spanA}, ts(100), testTickWidth, ids,
	)
	require.NoError(t, err)
	mgr.DisableDescFrontier()

	// Advance spanA past tick boundary.
	require.NoError(t, mgr.Flush(ctx, &revlogpb.Flush{
		Checkpoints: checkpoints(spanA, ts(115)),
	}))

	// Tick ts(110) should be closed since spanA is the only span.
	lr := revlog.NewLogReader(es)
	var ticks []revlog.Tick
	for tk, tickErr := range lr.Ticks(ctx, ts(99), ts(120)) {
		require.NoError(t, tickErr)
		ticks = append(ticks, tk)
	}
	require.Len(t, ticks, 1)

	// Add spanB at ts(100) (simulating span widening).
	require.NoError(t, mgr.AddSpansAt([]roachpb.Span{spanB}, ts(100)))

	// Now flush spanA further — ts(120) tick should NOT close
	// because spanB is still at ts(100).
	require.NoError(t, mgr.Flush(ctx, &revlogpb.Flush{
		Checkpoints: checkpoints(spanA, ts(125)),
	}))

	// Query from ts(119) to only see ticks past what was already closed.
	ticks = nil
	for tk, tickErr := range lr.Ticks(ctx, ts(119), ts(130)) {
		require.NoError(t, tickErr)
		ticks = append(ticks, tk)
	}
	require.Empty(t, ticks, "new span should gate tick close")

	// Advance spanB past tick boundary.
	require.NoError(t, mgr.Flush(ctx, &revlogpb.Flush{
		Checkpoints: checkpoints(spanB, ts(125)),
	}))

	ticks = nil
	for tk, tickErr := range lr.Ticks(ctx, ts(119), ts(130)) {
		require.NoError(t, tickErr)
		ticks = append(ticks, tk)
	}
	require.NotEmpty(t, ticks, "tick should close after new span advances")
}

// TestTickManagerNewTickManagerValidation checks constructor
// validation.
func TestTickManagerNewTickManagerValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	es := newTestStorage(t)
	defer es.Close()
	ids := &seqFileIDs{}

	_, err := revlogjob.NewTickManager(es, nil, ts(100), testTickWidth, ids)
	require.Error(t, err, "empty spans should fail")

	_, err = revlogjob.NewTickManager(
		es, []roachpb.Span{allSpan}, ts(100), 0, ids,
	)
	require.Error(t, err, "zero tickWidth should fail")

	_, err = revlogjob.NewTickManager(
		es, []roachpb.Span{allSpan}, ts(100), testTickWidth, nil,
	)
	require.Error(t, err, "nil fileIDs should fail")
}

// checkpoints is a helper to build a checkpoint slice for Flush.
func checkpoints(sp roachpb.Span, resolved hlc.Timestamp) []kvpb.RangeFeedCheckpoint {
	return []kvpb.RangeFeedCheckpoint{{Span: sp, ResolvedTS: resolved}}
}
