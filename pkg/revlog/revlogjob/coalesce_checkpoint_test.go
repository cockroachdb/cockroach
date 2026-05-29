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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestCoalesceOnlyTickSurvivesCheckpointAndResume exercises the
// scenario where a producer forwards small events as coalesces
// (not as files), the tick can't close yet (another span is
// lagging), a checkpoint fires, and then a crash occurs.
//
// The danger: Checkpoint skips ticks with len(files)==0, so the
// coalesce entries are not persisted. But the checkpoint DOES
// persist the advanced frontier for the producer's span. On
// resume, the rangefeed re-subscribes at the advanced frontier
// and won't re-deliver the coalesced events. The tick closes
// without spanA's contributions — permanent data loss.
//
// This test verifies the correct behavior: after crash+resume,
// the tick should close with ALL events, including those that
// were only in coalesce entries at checkpoint time.
func TestCoalesceOnlyTickSurvivesCheckpointAndResume(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, 0,
		"Checkpoint omits coalesce-only ticks; "+
			"frontier advances past events that exist only in inlineEntries")

	ctx := context.Background()

	es := newTestStorage(t)
	defer es.Close()
	ids := &seqFileIDs{}

	// --- First incarnation ---
	// Two producers on disjoint spans. Both use coalesce path.
	mgr1, err := revlogjob.NewTickManager(
		es, []roachpb.Span{spanA, spanB}, ts(100), testTickWidth, ids,
	)
	require.NoError(t, err)
	mgr1.DisableDescFrontier()

	prodA, err := revlogjob.NewProducer(
		es, []roachpb.Span{spanA}, ts(100), testTickWidth, ids, mgr1,
		revlogjob.ResumeState{}, 1<<20, /* forwardThreshold = 1MB */
	)
	require.NoError(t, err)

	// Producer A sends events for tick (100, 110] and advances
	// its frontier past 110. These events take the coalesce path
	// (small enough to forward).
	prodA.OnValue(ctx, roachpb.Key("c"), ts(105), []byte("vA1"), nil)
	prodA.OnValue(ctx, roachpb.Key("d"), ts(106), []byte("vA2"), nil)
	require.NoError(t, prodA.OnCheckpoint(ctx, spanA, ts(112)))

	// Producer B is lagging — hasn't advanced past startHLC.
	// This means tick 110 can't close yet (aggregate frontier =
	// min(112, 100) = 100 < 110).

	// Verify tick is NOT closed.
	lr := revlog.NewLogReader(es)
	var ticks []revlog.Tick
	for tk, tickErr := range lr.Ticks(ctx, ts(99), ts(120)) {
		require.NoError(t, tickErr)
		ticks = append(ticks, tk)
	}
	require.Empty(t, ticks, "tick should not close while spanB lags")

	// Checkpoint. This persists frontier={spanA:112, spanB:100}
	// and should persist the coalesce entries for tick 110.
	p := &memPersister{}
	require.NoError(t, mgr1.Checkpoint(ctx, p))
	loaded, found, err := p.Load(ctx)
	require.NoError(t, err)
	require.True(t, found)

	// --- Simulate crash + resume ---
	mgr2, err := revlogjob.NewTickManager(
		es, []roachpb.Span{spanA, spanB}, ts(100), testTickWidth, ids,
	)
	require.NoError(t, err)
	mgr2.DisableDescFrontier()
	require.NoError(t, mgr2.Rehydrate(loaded))

	// Producer A resumes at frontier=112. It will NOT re-deliver
	// events with ts in (100, 110].
	resumeA := mgr2.ResumeForPartition([]roachpb.Span{spanA})

	// Producer B resumes at frontier=100. It will deliver its
	// events normally.
	resumeB := mgr2.ResumeForPartition([]roachpb.Span{spanB})

	prodA2, err := revlogjob.NewProducer(
		es, []roachpb.Span{spanA}, ts(100), testTickWidth, ids, mgr2,
		resumeA, 1<<20,
	)
	require.NoError(t, err)
	prodB2, err := revlogjob.NewProducer(
		es, []roachpb.Span{spanB}, ts(100), testTickWidth, ids, mgr2,
		resumeB, 1<<20,
	)
	require.NoError(t, err)

	// Producer A has nothing new for tick 110 (frontier already
	// past it). Just advance it further.
	require.NoError(t, prodA2.OnCheckpoint(ctx, spanA, ts(115)))

	// Producer B catches up and advances past tick 110.
	prodB2.OnValue(ctx, roachpb.Key("n"), ts(107), []byte("vB"), nil)
	require.NoError(t, prodB2.OnCheckpoint(ctx, spanB, ts(115)))

	// Now tick 110 should close. It MUST include:
	// - "c" and "d" from producer A's pre-crash coalesces
	// - "n" from producer B's post-resume contribution
	ticks = nil
	for tk, tickErr := range lr.Ticks(ctx, ts(99), ts(120)) {
		require.NoError(t, tickErr)
		ticks = append(ticks, tk)
	}
	require.Len(t, ticks, 1)

	events := readBackEvents(t, ctx, es, ticks[0])
	require.Len(t, events, 3,
		"tick must contain events from both producers, "+
			"including pre-crash coalesces from producer A")
	require.Equal(t, "c", string(events[0].Key))
	require.Equal(t, "d", string(events[1].Key))
	require.Equal(t, "n", string(events[2].Key))
}
