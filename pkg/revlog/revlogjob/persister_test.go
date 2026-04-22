// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/revlog/revlogjob"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/stretchr/testify/require"
)

// memPersister is an in-memory revlogjob.Persister for unit tests
// that exercise the orchestration code without spinning up a job
// registry.
type memPersister struct {
	state revlogjob.State
	found bool
	// stores counts how many times Store was called. Useful for
	// driving synctest-bubbled checkpoint-loop tests.
	stores int
}

var _ revlogjob.Persister = (*memPersister)(nil)

func (m *memPersister) Load(_ context.Context) (revlogjob.State, bool, error) {
	if !m.found {
		return revlogjob.State{}, false, nil
	}
	return m.state, true, nil
}

func (m *memPersister) Store(_ context.Context, state revlogjob.State) error {
	m.state = state
	m.found = true
	m.stores++
	return nil
}

// TestSnapshotRoundTrip verifies that a fresh TickManager rehydrated
// from another TickManager's Snapshot reproduces the same
// observable state: HighWater (LastClosed), per-tick file lists,
// and per-span frontier.
func TestSnapshotRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	d, _ := newTestDriver(t, ts(100))
	d.OnValue(ctx, roachpb.Key("a"), ts(105), []byte("v_a"), nil)
	require.NoError(t, d.OnCheckpoint(ctx, allSpan, ts(115)))
	d.OnValue(ctx, roachpb.Key("b"), ts(125), []byte("v_b"), nil)
	// Don't checkpoint past the second tick — leaves it in pending
	// state on the manager so Snapshot has both a closed tick and
	// an open tick to capture.

	snap, err := d.Manager.Snapshot()
	require.NoError(t, err)

	// HighWater = last-closed tick end.
	require.Equal(t, ts(110), snap.HighWater)
	// Frontier reached ts(115).
	require.Equal(t, ts(115), snap.Frontier.Frontier())

	// Build a fresh manager, rehydrate, and verify it reports the
	// same LastClosed and that the frontier round-trips.
	d2, _ := newTestDriver(t, ts(100))
	require.NoError(t, d2.Manager.Rehydrate(snap))
	require.Equal(t, ts(110), d2.Manager.LastClosed())

	snap2, err := d2.Manager.Snapshot()
	require.NoError(t, err)
	require.Equal(t, ts(110), snap2.HighWater)
	require.Equal(t, ts(115), snap2.Frontier.Frontier())
}

// TestRehydrateOpenTicksAreClosedWithPriorFiles verifies that a
// resumed manager preserves files PUT before the restart: when the
// new incarnation closes a tick whose pending file list was
// rehydrated, the manifest references those files (in addition to
// any new contributions from the resumed flow).
//
// The test fakes an in-flight open tick via Rehydrate (rather than
// arranging for the prior-incarnation driver to leave one) and
// then runs a fresh driver pointed at the same external storage
// to close it.
func TestRehydrateOpenTicksAreClosedWithPriorFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	es := newTestStorage(t)
	t.Cleanup(func() { es.Close() })

	openTickEnd := ts(120)
	preservedFile := revlogpb.File{FileID: 99, FlushOrder: 0}
	state := revlogjob.State{
		HighWater: ts(110),
		OpenTicks: map[hlc.Timestamp][]revlogpb.File{
			openTickEnd: {preservedFile},
		},
	}

	d, err := revlogjob.NewDriver(es, []roachpb.Span{allSpan}, ts(100), testTickWidth,
		&seqFileIDs{}, revlogjob.ResumeState{})
	require.NoError(t, err)
	require.NoError(t, d.Manager.Rehydrate(state))

	// Drive the manager's frontier past the open tick's end. The
	// manifest written for tick (110, 120] should include the
	// preserved file (from rehydrate) plus any files the resumed
	// producer PUT for the same tick.
	d.OnValue(ctx, roachpb.Key("b"), ts(115), []byte("v_b"), nil)
	require.NoError(t, d.OnCheckpoint(ctx, allSpan, ts(120)))

	ticks := readBackTicks(t, ctx, es, ts(110), ts(120))
	require.Len(t, ticks, 1)
	require.Equal(t, openTickEnd, ticks[0].EndTime)
	require.GreaterOrEqual(t, len(ticks[0].Manifest.Files), 1)
	var foundPreserved bool
	for _, f := range ticks[0].Manifest.Files {
		if f.FileID == preservedFile.FileID {
			foundPreserved = true
			break
		}
	}
	require.True(t, foundPreserved,
		"manifest %v should include the rehydrated open-tick file %v",
		ticks[0].Manifest.Files, preservedFile)
}

// TestResumeProducerBumpsFlushOrder is the end-to-end resume
// scenario: a fresh producer constructed from the prior
// incarnation's persisted state writes its first file for an open
// tick at flushorder = max(prior) + 1, so per-key revision
// ordering survives the restart.
func TestResumeProducerBumpsFlushOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	es := newTestStorage(t)
	t.Cleanup(func() { es.Close() })

	// Inject a State that pretends the prior incarnation already
	// PUT one file at flushorder=0 for an open tick. The new
	// producer's resume info derived from this State must seed
	// startingFlushOrders[200] = 1 so its file lands at
	// flushorder=1.
	priorFile := revlogpb.File{FileID: 99, FlushOrder: 0}
	openTickEnd := ts(200)
	state := revlogjob.State{
		HighWater: ts(190),
		OpenTicks: map[hlc.Timestamp][]revlogpb.File{
			openTickEnd: {priorFile},
		},
	}

	resume := revlogjob.ResumeStateForPartition(state, []roachpb.Span{allSpan})
	require.Equal(t, int32(1), resume.StartingFlushOrders[openTickEnd])

	d, err := revlogjob.NewDriver(es, []roachpb.Span{allSpan}, ts(100), testTickWidth,
		&seqFileIDs{}, resume)
	require.NoError(t, err)
	require.NoError(t, d.Manager.Rehydrate(state))

	// Drive an event into the open tick and close it.
	d.OnValue(ctx, roachpb.Key("k"), ts(195), []byte("v"), nil)
	require.NoError(t, d.OnCheckpoint(ctx, allSpan, openTickEnd))

	ticks := readBackTicks(t, ctx, es, ts(190), openTickEnd)
	require.Len(t, ticks, 1)
	require.Equal(t, openTickEnd, ticks[0].EndTime)

	files := ticks[0].Manifest.Files
	require.Len(t, files, 2)
	// Order in the manifest is contribution order. Find the prior
	// file by ID and the new file by exclusion; assert flushorders.
	var priorIdx, newIdx int = -1, -1
	for i, f := range files {
		if f.FileID == priorFile.FileID {
			priorIdx = i
		} else {
			newIdx = i
		}
	}
	require.NotEqual(t, -1, priorIdx, "prior file %v missing from %v", priorFile, files)
	require.NotEqual(t, -1, newIdx, "new file missing from %v", files)
	require.Equal(t, int32(0), files[priorIdx].FlushOrder)
	require.Equal(t, int32(1), files[newIdx].FlushOrder,
		"new file must sit at flushorder=max(prior)+1")
}

// TestPersisterRoundTrip exercises the in-memory Persister contract:
// Store followed by Load returns the same state.
func TestPersisterRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	p := &memPersister{}
	got, found, err := p.Load(ctx)
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, got.OpenTicks)

	frontier, err := span.MakeFrontierAt(ts(50), allSpan)
	require.NoError(t, err)
	want := revlogjob.State{
		HighWater: ts(40),
		Frontier:  frontier,
		OpenTicks: map[hlc.Timestamp][]revlogpb.File{
			ts(50): {{FileID: 1, FlushOrder: 0}, {FileID: 2, FlushOrder: 1}},
		},
	}
	require.NoError(t, p.Store(ctx, want))

	got, found, err = p.Load(ctx)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, want.HighWater, got.HighWater)
	require.Equal(t, want.OpenTicks, got.OpenTicks)
	require.Equal(t, want.Frontier.Frontier(), got.Frontier.Frontier())
}
