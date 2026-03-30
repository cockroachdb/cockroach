// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag/wagpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestShouldApply(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	event := func(replID roachpb.ReplicaID, index kvpb.RaftIndex, typ wagpb.EventType) wagpb.Event {
		return wagpb.Event{Addr: wagpb.Addr{RangeID: 1, ReplicaID: replID, Index: index}, Type: typ}
	}

	tests := []struct {
		name          string
		event         wagpb.Event
		state         PersistedRangeState
		shouldApply   bool
		shouldCatchUp kvpb.RaftIndex
	}{
		{
			name:        "replica destroyed, event below tombstone",
			event:       event(3, 10, wagpb.EventApply),
			state:       PersistedRangeState{TombstoneNextReplicaID: 5},
			shouldApply: false,
		},
		{
			name:        "replica destroyed, event at tombstone boundary",
			event:       event(5, 1, wagpb.EventApply),
			state:       PersistedRangeState{TombstoneNextReplicaID: 5},
			shouldApply: true, shouldCatchUp: 1,
		},
		{
			name:        "new replica not yet seen",
			event:       event(7, 1, wagpb.EventApply),
			state:       PersistedRangeState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
			shouldApply: true, shouldCatchUp: 1,
		},
		{
			name:        "same replica, apply already applied",
			event:       event(3, 49, wagpb.EventApply),
			state:       PersistedRangeState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
			shouldApply: false,
		},
		{
			name:        "same replica, apply at applied index",
			event:       event(3, 50, wagpb.EventApply),
			state:       PersistedRangeState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
			shouldApply: false,
		},
		{
			name:        "same replica, apply ahead needs raft replay",
			event:       event(3, 51, wagpb.EventApply),
			state:       PersistedRangeState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
			shouldApply: true, shouldCatchUp: 51,
		},
		{
			name:        "same replica, split ahead needs catch-up",
			event:       event(3, 100, wagpb.EventSplit),
			state:       PersistedRangeState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
			shouldApply: true, shouldCatchUp: 99,
		},
		{
			name:        "same replica, destroy needs catch-up to index",
			event:       event(3, 100, wagpb.EventDestroy),
			state:       PersistedRangeState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
			shouldApply: true, shouldCatchUp: 100,
		},
		{
			name:        "same replica, destroy at applied index still needs applying",
			event:       event(3, 50, wagpb.EventDestroy),
			state:       PersistedRangeState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
			shouldApply: true, shouldCatchUp: 50,
		},
		{
			name:        "same replica, subsume at applied index still needs applying",
			event:       event(3, 50, wagpb.EventSubsume),
			state:       PersistedRangeState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
			shouldApply: true, shouldCatchUp: 50,
		},
		{
			name:        "destroy already applied, tombstone bumped",
			event:       event(3, 50, wagpb.EventDestroy),
			state:       PersistedRangeState{TombstoneNextReplicaID: 4},
			shouldApply: false,
		},
		{
			name:        "fresh range, no state exists",
			event:       event(1, 0, wagpb.EventCreate),
			shouldApply: true,
		},
		{
			name:        "EventCreate index 0 on existing replica",
			event:       event(3, 0, wagpb.EventCreate),
			state:       PersistedRangeState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
			shouldApply: false,
		},
		{
			name:        "tombstone with no current replica, event above tombstone",
			event:       event(10, 1, wagpb.EventInit),
			state:       PersistedRangeState{TombstoneNextReplicaID: 5},
			shouldApply: true,
		},
		{
			name:        "tombstone with no current replica, event below tombstone",
			event:       event(4, 100, wagpb.EventApply),
			state:       PersistedRangeState{TombstoneNextReplicaID: 5},
			shouldApply: false,
		},
		{
			name:        "EventCreate for new replica after tombstone",
			event:       event(5, 0, wagpb.EventCreate),
			state:       PersistedRangeState{TombstoneNextReplicaID: 5},
			shouldApply: true,
		},
		{
			name:        "stale replica, tombstone lags behind current replica",
			event:       event(2, 100, wagpb.EventApply),
			state:       PersistedRangeState{ReplicaID: 5, AppliedIndex: 10},
			shouldApply: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.shouldApply, ShouldApply(tc.event, tc.state))
			// raftCatchUp is only meaningful when the event will be applied.
			if tc.shouldApply {
				require.Equal(t, tc.shouldCatchUp, raftCatchUp(tc.event))
			}
		})
	}
}

// writePersistedRangeState writes the replay-relevant state for a range to the
// state machine.
func writePersistedRangeState(
	t *testing.T, stateRW StateRW, rangeID roachpb.RangeID, state PersistedRangeState,
) {
	t.Helper()
	ctx := context.Background()
	sl := MakeStateLoader(rangeID)
	ts := kvserverpb.RangeTombstone{NextReplicaID: state.TombstoneNextReplicaID}
	as := &kvserverpb.RangeAppliedState{RaftAppliedIndex: state.AppliedIndex}
	require.NoError(t, sl.SetRaftReplicaID(ctx, stateRW, state.ReplicaID))
	require.NoError(t, sl.SetRangeTombstone(ctx, stateRW, ts))
	require.NoError(t, sl.SetRangeAppliedState(ctx, stateRW, as))
}

func TestNodeShouldApply(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	t.Run("single event, needs apply", func(t *testing.T) {
		stateEng := storage.NewDefaultInMemForTesting()
		defer stateEng.Close()
		state := PersistedRangeState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50}
		writePersistedRangeState(t, stateEng, 1, state)

		node := wagpb.Node{
			Events: []wagpb.Event{
				{Addr: wagpb.Addr{RangeID: 1, ReplicaID: 3, Index: 51}, Type: wagpb.EventApply},
			},
		}
		action, err := NodeShouldApply(ctx, node, StateRO(stateEng))
		require.NoError(t, err)
		require.True(t, action.Apply)
		require.Len(t, action.CatchUps, 1)
		require.Equal(t, RaftCatchUpTarget{RangeID: 1, ReplicaID: 3, Index: 51}, action.CatchUps[0])
	})

	t.Run("single event, already applied", func(t *testing.T) {
		stateEng := storage.NewDefaultInMemForTesting()
		defer stateEng.Close()
		state := PersistedRangeState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50}
		writePersistedRangeState(t, stateEng, 1, state)

		node := wagpb.Node{
			Events: []wagpb.Event{
				{Addr: wagpb.Addr{RangeID: 1, ReplicaID: 3, Index: 50}, Type: wagpb.EventApply},
			},
		}
		action, err := NodeShouldApply(ctx, node, StateRO(stateEng))
		require.NoError(t, err)
		require.False(t, action.Apply)
		require.Empty(t, action.CatchUps)
	})

	t.Run("multi-event split, needs apply with per-range catch-ups", func(t *testing.T) {
		stateEng := storage.NewDefaultInMemForTesting()
		defer stateEng.Close()
		lhsState := PersistedRangeState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 99}
		writePersistedRangeState(t, stateEng, 1, lhsState)
		// RHS doesn't exist yet; empty state is the zero value.

		// A split node: EventInit for the RHS (no catch-up) + EventSplit for LHS
		// (catch-up to index 99). CatchUps should contain only the LHS target.
		node := wagpb.Node{
			Events: []wagpb.Event{
				{Addr: wagpb.Addr{RangeID: 2, ReplicaID: 1, Index: 10}, Type: wagpb.EventInit},
				{Addr: wagpb.Addr{RangeID: 1, ReplicaID: 3, Index: 100}, Type: wagpb.EventSplit},
			},
		}
		action, err := NodeShouldApply(ctx, node, StateRO(stateEng))
		require.NoError(t, err)
		require.True(t, action.Apply)
		require.Len(t, action.CatchUps, 1)
		require.Equal(t, RaftCatchUpTarget{RangeID: 1, ReplicaID: 3, Index: 99}, action.CatchUps[0])
	})

	t.Run("multi-event split, already applied", func(t *testing.T) {
		stateEng := storage.NewDefaultInMemForTesting()
		defer stateEng.Close()
		lhsState := PersistedRangeState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 100}
		rhsState := PersistedRangeState{ReplicaID: 1, TombstoneNextReplicaID: 1, AppliedIndex: 10}
		writePersistedRangeState(t, stateEng, 1, lhsState)
		writePersistedRangeState(t, stateEng, 2, rhsState)

		node := wagpb.Node{
			Events: []wagpb.Event{
				{Addr: wagpb.Addr{RangeID: 2, ReplicaID: 1, Index: 10}, Type: wagpb.EventInit},
				{Addr: wagpb.Addr{RangeID: 1, ReplicaID: 3, Index: 100}, Type: wagpb.EventSplit},
			},
		}
		action, err := NodeShouldApply(ctx, node, StateRO(stateEng))
		require.NoError(t, err)
		require.False(t, action.Apply)
		require.Empty(t, action.CatchUps)
	})

	t.Run("multi-event disagreement returns error", func(t *testing.T) {
		stateEng := storage.NewDefaultInMemForTesting()
		defer stateEng.Close()
		// LHS already applied but RHS not yet — should not happen.
		lhsState := PersistedRangeState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 100}
		writePersistedRangeState(t, stateEng, 1, lhsState)
		// RHS has no state (zero value).

		node := wagpb.Node{
			Events: []wagpb.Event{
				{Addr: wagpb.Addr{RangeID: 2, ReplicaID: 1, Index: 10}, Type: wagpb.EventInit},
				{Addr: wagpb.Addr{RangeID: 1, ReplicaID: 3, Index: 100}, Type: wagpb.EventSplit},
			},
		}
		_, err := NodeShouldApply(ctx, node, StateRO(stateEng))
		require.ErrorContains(t, err, "disagree on whether to apply")
	})
}

// makeBatchRepr builds a batch with a single unversioned key-value pair. The
// engine is used only to construct the batch format; nothing is committed.
func makeBatchRepr(t *testing.T, eng storage.Engine, key string, val string) []byte {
	t.Helper()
	b := eng.NewWriteBatch()
	defer b.Close()
	require.NoError(t, b.PutUnversioned(roachpb.Key(key), []byte(val)))
	return b.Repr()
}

// testWAGWriter is a test helper that writes WAG nodes to the log engine,
// managing the sequencer and batch internally.
type testWAGWriter struct {
	t     *testing.T
	seq   wag.Seq
	batch storage.WriteBatch
}

func newTestWAGWriter(t *testing.T, logEng storage.Engine) *testWAGWriter {
	return &testWAGWriter{t: t, batch: logEng.NewWriteBatch()}
}

// writeNode writes a WAG node with the given events and batch mutation.
func (w *testWAGWriter) writeNode(events []wagpb.Event, batchRepr []byte) {
	w.t.Helper()
	require.NoError(w.t, wag.Write(w.batch, w.seq.Next(1), wagpb.Node{
		Events:   events,
		Mutation: wagpb.Mutation{Batch: batchRepr},
	}))
}

func (w *testWAGWriter) commit() {
	w.t.Helper()
	require.NoError(w.t, w.batch.Commit(false /* sync */))
}

// getKey reads a single unversioned key from the engine.
func getKey(ctx context.Context, t *testing.T, eng storage.Engine, key string) []byte {
	t.Helper()
	kvs, err := storage.Scan(ctx, eng, roachpb.Key(key), roachpb.Key(key).Next(), 1)
	require.NoError(t, err)
	if len(kvs) == 0 {
		return nil
	}
	return kvs[0].Value
}

func TestReplay(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	t.Run("empty WAG", func(t *testing.T) {
		logEng := storage.NewDefaultInMemForTesting()
		defer logEng.Close()
		stateEng := storage.NewDefaultInMemForTesting()
		defer stateEng.Close()

		require.NoError(t, Replay(ctx, RaftRO(logEng), StateRW(stateEng)))
	})

	t.Run("applied node is skipped, unapplied node is applied", func(t *testing.T) {
		logEng := storage.NewDefaultInMemForTesting()
		defer logEng.Close()
		stateEng := storage.NewDefaultInMemForTesting()
		defer stateEng.Close()

		w := newTestWAGWriter(t, logEng)
		w.writeNode(
			[]wagpb.Event{{Addr: wagpb.Addr{RangeID: 1, ReplicaID: 1, Index: 10}, Type: wagpb.EventApply}},
			makeBatchRepr(t, stateEng, "key1", "val1"),
		)
		w.writeNode(
			[]wagpb.Event{{Addr: wagpb.Addr{RangeID: 1, ReplicaID: 1, Index: 11}, Type: wagpb.EventApply}},
			makeBatchRepr(t, stateEng, "key2", "val2"),
		)
		w.commit()

		// First node is already applied; second is not.
		state := PersistedRangeState{ReplicaID: 1, TombstoneNextReplicaID: 1, AppliedIndex: 10}
		writePersistedRangeState(t, stateEng, 1, state)
		require.NoError(t, Replay(ctx, RaftRO(logEng), StateRW(stateEng)))

		require.Nil(t, getKey(ctx, t, stateEng, "key1"))
		require.Equal(t, []byte("val2"), getKey(ctx, t, stateEng, "key2"))
	})
}
