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

// newTickManagerWithDescGating builds a TickManager + Producer pair
// the same way Driver does, but does NOT call DisableDescFrontier()
// — so the close loop gates on min(data, descriptor) frontier and
// the test must explicitly forward the descriptor frontier to make
// progress.
func newTickManagerWithDescGating(
	t *testing.T,
) (*revlogjob.TickManager, *revlogjob.Producer, *inmemstorage.Storage) {
	t.Helper()
	es := inmemstorage.New()
	t.Cleanup(func() { _ = es.Close() })
	mgr, err := revlogjob.NewTickManager(es, []roachpb.Span{allSpan},
		ts(100), testTickWidth)
	require.NoError(t, err)
	// Intentionally NOT disabling desc gating.
	prod, err := revlogjob.NewProducer(es, []roachpb.Span{allSpan},
		ts(100), testTickWidth, &seqFileIDs{}, mgr, revlogjob.ResumeState{})
	require.NoError(t, err)
	return mgr, prod, es.(*inmemstorage.Storage)
}

// TestDescFrontierGatesTickClose verifies that tick close requires
// BOTH the data frontier and the descriptor frontier to have advanced
// past tick_end. Without this, a late-delivered descriptor change
// inside an already-closed tick would mean the recorded coverage was
// wrong for some events the tick contained.
func TestDescFrontierGatesTickClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	mgr, prod, es := newTickManagerWithDescGating(t)

	// Data frontier reaches ts(115), past tick boundary at 110. But
	// desc frontier is still at startHLC (100). Tick must NOT close.
	prod.OnValue(ctx, roachpb.Key("a"), ts(105), []byte("v"), nil)
	require.NoError(t, prod.OnCheckpoint(ctx, allSpan, ts(115)))
	require.Empty(t, readBackTicks(t, ctx, es, ts(99), ts(120)),
		"tick must not close while desc frontier is still at startHLC")

	// Forward desc frontier just shy of tick_end — still no close.
	require.NoError(t, mgr.ForwardDescFrontier(ctx, ts(109)))
	require.Empty(t, readBackTicks(t, ctx, es, ts(99), ts(120)),
		"tick must not close while desc frontier is below tick_end")

	// Forward desc frontier past tick_end — tick closes.
	require.NoError(t, mgr.ForwardDescFrontier(ctx, ts(115)))
	ticks := readBackTicks(t, ctx, es, ts(99), ts(120))
	require.Len(t, ticks, 1)
	require.Equal(t, ts(110), ticks[0].EndTime)
}

// TestDataFrontierGatesTickCloseWhenDescAhead verifies the
// symmetric case: desc frontier ahead, data frontier still at
// startHLC. Tick must not close.
func TestDataFrontierGatesTickCloseWhenDescAhead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	mgr, _, es := newTickManagerWithDescGating(t)

	// Desc frontier jumps far ahead. Data frontier still at startHLC
	// (no Producer.OnCheckpoint calls). Tick must not close.
	require.NoError(t, mgr.ForwardDescFrontier(ctx, ts(200)))
	require.Empty(t, readBackTicks(t, ctx, es, ts(99), ts(200)),
		"tick must not close while data frontier is still at startHLC")
}

// TestForwardDescFrontierIdempotent verifies that calling
// ForwardDescFrontier with a ts <= the current value is a no-op
// (doesn't run the close loop redundantly, doesn't error).
func TestForwardDescFrontierIdempotent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	mgr, _, _ := newTickManagerWithDescGating(t)

	require.NoError(t, mgr.ForwardDescFrontier(ctx, ts(150)))
	require.NoError(t, mgr.ForwardDescFrontier(ctx, ts(150)))
	require.NoError(t, mgr.ForwardDescFrontier(ctx, ts(120))) // backwards, ignored
}
