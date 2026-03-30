// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package wag

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
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

	ev := func(
		replicaID roachpb.ReplicaID, index kvpb.RaftIndex, typ wagpb.EventType,
	) wagpb.Event {
		return wagpb.Event{
			Addr: wagpb.Addr{RangeID: 1, ReplicaID: replicaID, Index: index},
			Type: typ,
		}
	}

	tests := []struct {
		name        string
		event       wagpb.Event
		state       RangeAppliedState
		shouldApply   bool
		wantCatchUp kvpb.RaftIndex
	}{
		{
			name:  "replica destroyed, event below tombstone",
			event: ev(3, 10, wagpb.EventApply),
			state: RangeAppliedState{TombstoneNextReplicaID: 5},
		},
		{
			name:      "replica destroyed, event at tombstone boundary",
			event:     ev(5, 1, wagpb.EventApply),
			state:     RangeAppliedState{TombstoneNextReplicaID: 5},
			shouldApply: true, wantCatchUp: 1,
		},
		{
			name:      "new replica not yet seen",
			event:     ev(7, 1, wagpb.EventApply),
			state:     RangeAppliedState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
			shouldApply: true, wantCatchUp: 1,
		},
		{
			name:  "same replica, apply already applied",
			event: ev(3, 49, wagpb.EventApply),
			state: RangeAppliedState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
		},
		{
			name:  "same replica, apply at applied index",
			event: ev(3, 50, wagpb.EventApply),
			state: RangeAppliedState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
		},
		{
			name:      "same replica, apply ahead needs raft replay",
			event:     ev(3, 51, wagpb.EventApply),
			state:     RangeAppliedState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
			shouldApply: true, wantCatchUp: 51,
		},
		{
			name:      "same replica, split ahead needs catch-up",
			event:     ev(3, 100, wagpb.EventSplit),
			state:     RangeAppliedState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
			shouldApply: true, wantCatchUp: 99,
		},
		{
			name:      "same replica, destroy needs catch-up to index",
			event:     ev(3, 100, wagpb.EventDestroy),
			state:     RangeAppliedState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
			shouldApply: true, wantCatchUp: 100,
		},
		{
			name:      "same replica, destroy at applied index still needs applying",
			event:     ev(3, 50, wagpb.EventDestroy),
			state:     RangeAppliedState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
			shouldApply: true, wantCatchUp: 50,
		},
		{
			name:      "same replica, subsume at applied index still needs applying",
			event:     ev(3, 50, wagpb.EventSubsume),
			state:     RangeAppliedState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
			shouldApply: true, wantCatchUp: 50,
		},
		{
			name:  "destroy already applied, tombstone bumped",
			event: ev(3, 50, wagpb.EventDestroy),
			state: RangeAppliedState{TombstoneNextReplicaID: 4},
		},
		{
			name:      "fresh range, no state exists",
			event:     ev(1, 0, wagpb.EventCreate),
			shouldApply: true,
		},
		{
			name:  "EventCreate index 0 on existing replica",
			event: ev(3, 0, wagpb.EventCreate),
			state: RangeAppliedState{ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
		},
		{
			name:      "tombstone with no current replica, event above tombstone",
			event:     ev(10, 1, wagpb.EventInit),
			state:     RangeAppliedState{TombstoneNextReplicaID: 5},
			shouldApply: true,
		},
		{
			name:  "tombstone with no current replica, event below tombstone",
			event: ev(4, 100, wagpb.EventApply),
			state: RangeAppliedState{TombstoneNextReplicaID: 5},
		},
		{
			name:      "EventCreate for new replica after tombstone",
			event:     ev(5, 0, wagpb.EventCreate),
			state:     RangeAppliedState{TombstoneNextReplicaID: 5},
			shouldApply: true,
		},
		{
			name:  "stale replica, tombstone lags behind current replica",
			event: ev(2, 100, wagpb.EventApply),
			state: RangeAppliedState{ReplicaID: 5, AppliedIndex: 10},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ShouldApply(tc.event, tc.state)
			require.Equal(t, tc.shouldApply, got,
				fmt.Sprintf("ShouldApply(%+v, %+v)", tc.event, tc.state))
			// raftCatchUp is only meaningful when the event will be applied.
			if tc.shouldApply {
				gotCatchUp := raftCatchUp(tc.event.Addr, tc.event.Type)
				require.Equal(t, tc.wantCatchUp, gotCatchUp,
					fmt.Sprintf("raftCatchUp(%s, %s)", tc.event.Addr, tc.event.Type))
			}
		})
	}

}

// makeTestLoader returns an AppliedStateLoader backed by a map. Tests populate
// the map with per-range applied state before invoking replay logic.
func makeTestLoader(states map[roachpb.RangeID]RangeAppliedState) AppliedStateLoader {
	return func(
		_ context.Context, rangeID roachpb.RangeID,
	) (RangeAppliedState, error) {
		return states[rangeID], nil
	}
}

func TestNodeShouldApply(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	t.Run("single event, needs apply", func(t *testing.T) {
		node := wagpb.Node{
			Events: []wagpb.Event{
				{Addr: wagpb.Addr{RangeID: 1, ReplicaID: 3, Index: 51}, Type: wagpb.EventApply},
			},
		}
		states := map[roachpb.RangeID]RangeAppliedState{
			1: {ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
		}
		action, err := NodeShouldApply(ctx, node, makeTestLoader(states))
		require.NoError(t, err)
		require.True(t, action.Apply)
		require.Len(t, action.CatchUps, 1)
		require.Equal(t, RaftCatchUpTarget{
			RangeID: 1, ReplicaID: 3, Index: 51,
		}, action.CatchUps[0])
	})

	t.Run("single event, already applied", func(t *testing.T) {
		node := wagpb.Node{
			Events: []wagpb.Event{
				{Addr: wagpb.Addr{RangeID: 1, ReplicaID: 3, Index: 50}, Type: wagpb.EventApply},
			},
		}
		states := map[roachpb.RangeID]RangeAppliedState{
			1: {ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 50},
		}
		action, err := NodeShouldApply(ctx, node, makeTestLoader(states))
		require.NoError(t, err)
		require.False(t, action.Apply)
		require.Empty(t, action.CatchUps)
	})

	t.Run("multi-event split, needs apply with per-range catch-ups", func(t *testing.T) {
		// A split node: EventInit for the RHS (no catch-up) + EventSplit for LHS
		// (catch-up to index 99). CatchUps should contain only the LHS target.
		node := wagpb.Node{
			Events: []wagpb.Event{
				{Addr: wagpb.Addr{RangeID: 2, ReplicaID: 1, Index: 10}, Type: wagpb.EventInit},
				{Addr: wagpb.Addr{RangeID: 1, ReplicaID: 3, Index: 100}, Type: wagpb.EventSplit},
			},
		}
		states := map[roachpb.RangeID]RangeAppliedState{
			1: {ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 99},
			2: {}, // RHS doesn't exist yet
		}
		action, err := NodeShouldApply(ctx, node, makeTestLoader(states))
		require.NoError(t, err)
		require.True(t, action.Apply)
		require.Len(t, action.CatchUps, 1)
		require.Equal(t, RaftCatchUpTarget{
			RangeID: 1, ReplicaID: 3, Index: 99,
		}, action.CatchUps[0])
	})

	t.Run("multi-event split, already applied", func(t *testing.T) {
		node := wagpb.Node{
			Events: []wagpb.Event{
				{Addr: wagpb.Addr{RangeID: 2, ReplicaID: 1, Index: 10}, Type: wagpb.EventInit},
				{Addr: wagpb.Addr{RangeID: 1, ReplicaID: 3, Index: 100}, Type: wagpb.EventSplit},
			},
		}
		states := map[roachpb.RangeID]RangeAppliedState{
			1: {ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 100},
			2: {ReplicaID: 1, TombstoneNextReplicaID: 1, AppliedIndex: 10},
		}
		action, err := NodeShouldApply(ctx, node, makeTestLoader(states))
		require.NoError(t, err)
		require.False(t, action.Apply)
		require.Empty(t, action.CatchUps)
	})

	t.Run("multi-event disagreement panics", func(t *testing.T) {
		// Simulate a corrupted state where the LHS has been applied but the RHS
		// hasn't. This should not happen and is flagged as an assertion failure.
		node := wagpb.Node{
			Events: []wagpb.Event{
				{Addr: wagpb.Addr{RangeID: 2, ReplicaID: 1, Index: 10}, Type: wagpb.EventInit},
				{Addr: wagpb.Addr{RangeID: 1, ReplicaID: 3, Index: 100}, Type: wagpb.EventSplit},
			},
		}
		states := map[roachpb.RangeID]RangeAppliedState{
			1: {ReplicaID: 3, TombstoneNextReplicaID: 3, AppliedIndex: 100}, // LHS already applied
			2: {},                                                           // RHS not yet applied
		}
		require.Panics(t, func() {
			_, _ = NodeShouldApply(ctx, node, makeTestLoader(states))
		})
	})
}

// makeBatchRepr builds a batch repr that puts a single unversioned key-value
// pair. The engine is used only to construct the batch format; nothing is
// committed.
func makeBatchRepr(t *testing.T, eng storage.Engine, key string, val string) []byte {
	t.Helper()
	b := eng.NewWriteBatch()
	require.NoError(t, b.PutUnversioned(roachpb.Key(key), []byte(val)))
	repr := b.Repr()
	b.Close()
	return repr
}

// writeWAGNode writes a WAG node to the log engine with a single EventApply.
func writeWAGNode(
	t *testing.T,
	logBatch storage.WriteBatch,
	seq *Seq,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	index kvpb.RaftIndex,
	batchRepr []byte,
) {
	t.Helper()
	require.NoError(t, Write(logBatch, seq.Next(1), wagpb.Node{
		Events: []wagpb.Event{
			{Addr: wagpb.Addr{RangeID: rangeID, ReplicaID: replicaID, Index: index}, Type: wagpb.EventApply},
		},
		Mutation: wagpb.Mutation{Batch: batchRepr},
	}))
}

// scanKey scans for a single key in the engine and returns whether it was
// found, along with its value.
func scanKey(t *testing.T, eng storage.Engine, key string) (bool, []byte) {
	t.Helper()
	ctx := context.Background()
	end := roachpb.Key(key).Next()
	kvs, err := storage.Scan(ctx, eng, roachpb.Key(key), end, 1)
	require.NoError(t, err)
	if len(kvs) == 0 {
		return false, nil
	}
	return true, kvs[0].Value
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

		states := map[roachpb.RangeID]RangeAppliedState{}
		require.NoError(t, Replay(ctx, logEng, stateEng, makeTestLoader(states)))
	})

	t.Run("unapplied node is applied", func(t *testing.T) {
		logEng := storage.NewDefaultInMemForTesting()
		defer logEng.Close()
		stateEng := storage.NewDefaultInMemForTesting()
		defer stateEng.Close()

		var seq Seq
		repr := makeBatchRepr(t, stateEng, "key1", "val1")
		logBatch := logEng.NewWriteBatch()
		writeWAGNode(t, logBatch, &seq, 1, 1, 10, repr)
		require.NoError(t, logBatch.Commit(false /* sync */))

		states := map[roachpb.RangeID]RangeAppliedState{}
		require.NoError(t, Replay(ctx, logEng, stateEng, makeTestLoader(states)))

		found, val := scanKey(t, stateEng, "key1")
		require.True(t, found)
		require.Equal(t, []byte("val1"), val)
	})

	t.Run("applied node is skipped", func(t *testing.T) {
		logEng := storage.NewDefaultInMemForTesting()
		defer logEng.Close()
		stateEng := storage.NewDefaultInMemForTesting()
		defer stateEng.Close()

		var seq Seq
		logBatch := logEng.NewWriteBatch()
		writeWAGNode(t, logBatch, &seq, 1, 1, 10, makeBatchRepr(t, stateEng, "key1", "val1"))
		writeWAGNode(t, logBatch, &seq, 1, 1, 11, makeBatchRepr(t, stateEng, "key2", "val2"))
		require.NoError(t, logBatch.Commit(false /* sync */))

		// First node is already applied; second is not.
		states := map[roachpb.RangeID]RangeAppliedState{
			1: {ReplicaID: 1, TombstoneNextReplicaID: 1, AppliedIndex: 10},
		}
		require.NoError(t, Replay(ctx, logEng, stateEng, makeTestLoader(states)))

		found, _ := scanKey(t, stateEng, "key1")
		require.False(t, found, "skipped node should not be applied")

		found, val := scanKey(t, stateEng, "key2")
		require.True(t, found)
		require.Equal(t, []byte("val2"), val)
	})
}
