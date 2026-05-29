// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogjob"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestScopeTerminationFlushesPartialTick verifies that when the
// revlog scope dissolves (all watched descriptors dropped), events
// buffered in the still-open partial tick are flushed and their
// manifest is written before the job exits.
//
// Without this, a RESTORE to an AOST in the final partial-tick
// window would see "table existed, no data" — the schema deltas
// record the table's existence but the revlog has no events for
// that window.
//
// See the TODO in descfeed.go processBatch: the fix is to wait
// for manager.LastClosed() > resolvedTS before returning
// ErrScopeTerminated.
func TestScopeTerminationFlushesPartialTick(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, 0, "scope termination does not yet flush the partial tick")

	ctx := context.Background()

	es := newTestStorage(t)
	defer es.Close()
	ids := &seqFileIDs{}

	// Start at ts(100). Tick width = 10s, so tick boundaries are
	// at 110, 120, etc.
	startHLC := ts(100)
	mgr, err := revlogjob.NewTickManager(
		es, []roachpb.Span{allSpan}, startHLC, testTickWidth, ids,
	)
	require.NoError(t, err)
	mgr.DisableDescFrontier()

	prod, err := revlogjob.NewProducer(
		es, []roachpb.Span{allSpan}, startHLC, testTickWidth, ids, mgr,
		revlogjob.ResumeState{}, 0,
	)
	require.NoError(t, err)

	// Close one full tick so we have a baseline.
	prod.OnValue(ctx, roachpb.Key("a"), ts(105), []byte("v1"), nil)
	require.NoError(t, prod.OnCheckpoint(ctx, allSpan, ts(111)))

	lr := revlog.NewLogReader(es)
	var ticks []revlog.Tick
	for tk, tickErr := range lr.Ticks(ctx, ts(99), ts(120)) {
		require.NoError(t, tickErr)
		ticks = append(ticks, tk)
	}
	require.Len(t, ticks, 1, "first full tick should be closed")

	// Now buffer events in the NEXT tick (110, 120] but don't
	// advance the frontier past 120 — simulating a scope
	// termination at ts(115) before the tick closes.
	prod.OnValue(ctx, roachpb.Key("b"), ts(113), []byte("v2"), nil)
	prod.OnValue(ctx, roachpb.Key("c"), ts(114), []byte("v3"), nil)

	// Advance frontier to the termination point but NOT past the
	// tick boundary.
	require.NoError(t, prod.OnCheckpoint(ctx, allSpan, ts(115)))

	// At this point, scope termination would fire. The events for
	// keys "b" and "c" are buffered but the tick (110, 120] is
	// still open. The correct behavior is that the partial tick
	// is flushed with TickEnd adjusted to the termination point,
	// or the frontier is forced past the tick boundary so the
	// close loop fires.
	//
	// For now, verify the events ARE in a closed tick — this is
	// the behavior we want. When the TODO in descfeed.go is
	// implemented, this test should pass.
	ticks = nil
	for tk, tickErr := range lr.Ticks(ctx, ts(109), hlc.MaxTimestamp) {
		require.NoError(t, tickErr)
		ticks = append(ticks, tk)
	}
	require.Len(t, ticks, 1, "partial tick should be closed on termination")

	events := readBackEvents(t, ctx, es, ticks[0])
	require.Len(t, events, 2, "partial tick should contain the buffered events")
	require.Equal(t, "b", string(events[0].Key))
	require.Equal(t, "c", string(events[1].Key))
}
