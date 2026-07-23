// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"slices"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag/wagpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// replicaMark is a test helper that constructs a ReplicaMark.
func replicaMark(replicaID, nextReplicaID roachpb.ReplicaID) ReplicaMark {
	return ReplicaMark{
		RaftReplicaID:  kvserverpb.RaftReplicaID{ReplicaID: replicaID},
		RangeTombstone: kvserverpb.RangeTombstone{NextReplicaID: nextReplicaID},
	}
}

// TestCanApply exercises the per-event replay decision logic, verifying that
// canApply correctly classifies events as needing application or not, and that
// raftCatchUp returns the right catch-up index for applicable events.
func TestCanApply(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	event := func(replID roachpb.ReplicaID, index kvpb.RaftIndex, typ wagpb.EventType) wagpb.Event {
		return wagpb.Event{Addr: wagpb.Addr{RangeID: 1, ReplicaID: replID, Index: index}, Type: typ}
	}

	for _, tc := range []struct {
		name          string
		event         wagpb.Event
		state         persistedRangeState
		shouldApply   bool
		shouldCatchUp kvpb.RaftIndex
	}{
		// Old replica (destroyed or never existed on this store).
		{
			name:        "event below tombstone",
			event:       event(3, 10, wagpb.EventApply),
			state:       persistedRangeState{mark: replicaMark(0, 5)},
			shouldApply: false,
		}, {
			name:        "old replica, superseded by current replica",
			event:       event(3, 10, wagpb.EventApply),
			state:       persistedRangeState{mark: replicaMark(5, 0), appliedIndex: 10},
			shouldApply: false,
		},

		// Current replica — compare raft indices, with special handling for
		// Destroy/Subsume which always need applying.
		{
			name:        "create already applied",
			event:       event(3, 0, wagpb.EventCreate),
			state:       persistedRangeState{mark: replicaMark(3, 0)},
			shouldApply: false,
		}, {
			name:        "init on uninitialized replica",
			event:       event(3, 10, wagpb.EventInit),
			state:       persistedRangeState{mark: replicaMark(3, 0)},
			shouldApply: true,
		}, {
			name:        "destroy on uninitialized replica",
			event:       event(3, 0, wagpb.EventDestroy),
			state:       persistedRangeState{mark: replicaMark(3, 0)},
			shouldApply: true, shouldCatchUp: 0,
		}, {
			name:        "apply below applied index",
			event:       event(3, 49, wagpb.EventApply),
			state:       persistedRangeState{mark: replicaMark(3, 0), appliedIndex: 50},
			shouldApply: false,
		}, {
			name:        "apply at applied index",
			event:       event(3, 50, wagpb.EventApply),
			state:       persistedRangeState{mark: replicaMark(3, 0), appliedIndex: 50},
			shouldApply: false,
		}, {
			name:        "apply above applied index",
			event:       event(3, 51, wagpb.EventApply),
			state:       persistedRangeState{mark: replicaMark(3, 0), appliedIndex: 50},
			shouldApply: true, shouldCatchUp: 51,
		}, {
			name:        "split above applied index",
			event:       event(3, 100, wagpb.EventSplit),
			state:       persistedRangeState{mark: replicaMark(3, 0), appliedIndex: 50},
			shouldApply: true, shouldCatchUp: 99,
		}, {
			name:        "merge above applied index",
			event:       event(3, 100, wagpb.EventMerge),
			state:       persistedRangeState{mark: replicaMark(3, 0), appliedIndex: 50},
			shouldApply: true, shouldCatchUp: 99,
		}, {
			name:        "destroy above applied index",
			event:       event(3, 100, wagpb.EventDestroy),
			state:       persistedRangeState{mark: replicaMark(3, 0), appliedIndex: 50},
			shouldApply: true, shouldCatchUp: 100,
		}, {
			// Destroy/Subsume at the applied index still need applying — if they
			// had already been applied, the tombstone would have been bumped.
			name:        "destroy at applied index",
			event:       event(3, 50, wagpb.EventDestroy),
			state:       persistedRangeState{mark: replicaMark(3, 0), appliedIndex: 50},
			shouldApply: true, shouldCatchUp: 50,
		}, {
			name:        "subsume at applied index",
			event:       event(3, 50, wagpb.EventSubsume),
			state:       persistedRangeState{mark: replicaMark(3, 0), appliedIndex: 50},
			shouldApply: true, shouldCatchUp: 50,
		},

		// New replica, not yet seen on this store.
		{
			name:        "create on fresh range",
			event:       event(1, 0, wagpb.EventCreate),
			shouldApply: true,
		}, {
			name:        "create at tombstone boundary",
			event:       event(5, 0, wagpb.EventCreate),
			state:       persistedRangeState{mark: replicaMark(0, 5)},
			shouldApply: true,
		}, {
			name:        "create above tombstone",
			event:       event(10, 0, wagpb.EventCreate),
			state:       persistedRangeState{mark: replicaMark(0, 5)},
			shouldApply: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.shouldApply, tc.state.canApply(tc.event))
			// raftCatchUp is only meaningful when the event will be applied.
			if tc.shouldApply {
				require.Equal(t, tc.shouldCatchUp, raftCatchUp(tc.event))
			}
		})
	}
}

// writeRaftLogEntries writes empty raft log entries for the given range and
// index range [lo, hi] to the log engine so that raftlog.Visit can iterate
// over them during catch-up replay.
func writeRaftLogEntries(
	t *testing.T, logEng storage.Engine, rangeID roachpb.RangeID, lo, hi kvpb.RaftIndex,
) {
	t.Helper()
	ctx := context.Background()
	for idx := lo; idx <= hi; idx++ {
		ent := raftpb.Entry{Index: uint64(idx), Term: 1}
		key := keys.RaftLogKey(rangeID, idx)
		require.NoError(t, storage.MVCCPutProto(
			ctx, logEng, key, hlc.Timestamp{}, &ent, storage.MVCCWriteOptions{},
		))
	}
}

// writePersistedRangeState writes the replay-relevant state for a range to the
// state machine.
func writePersistedRangeState(
	t *testing.T, stateRW StateRW, rangeID roachpb.RangeID, state persistedRangeState,
) {
	t.Helper()
	ctx := context.Background()
	sl := MakeStateLoader(rangeID)
	as := &kvserverpb.RangeAppliedState{RaftAppliedIndex: state.appliedIndex}
	require.NoError(t, sl.SetRaftReplicaID(ctx, stateRW, state.mark.ReplicaID))
	require.NoError(t, sl.SetRangeTombstone(ctx, stateRW, state.mark.RangeTombstone))
	require.NoError(t, sl.SetRangeAppliedState(ctx, stateRW, as))
}

// TestCanApplyWAGNode exercises the node-level replay decision logic, verifying
// that multi-event nodes (e.g., splits) produce the correct apply decision and
// per-range catch-up targets.
func TestCanApplyWAGNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name        string
		states      map[roachpb.RangeID]persistedRangeState
		node        wagpb.Node
		shouldApply bool
		expCatchUps []raftCatchUpTarget
		expErr      string
	}{
		{
			name: "single event, needs apply",
			states: map[roachpb.RangeID]persistedRangeState{
				1: {mark: replicaMark(3, 0), appliedIndex: 50},
			},
			node: wagpb.Node{Events: []wagpb.Event{
				{Addr: wagpb.Addr{RangeID: 1, ReplicaID: 3, Index: 51}, Type: wagpb.EventApply},
			}},
			shouldApply: true,
			expCatchUps: []raftCatchUpTarget{{rangeID: 1, index: 51}},
		}, {
			name: "single event, already applied",
			states: map[roachpb.RangeID]persistedRangeState{
				1: {mark: replicaMark(3, 0), appliedIndex: 50},
			},
			node: wagpb.Node{Events: []wagpb.Event{
				{Addr: wagpb.Addr{RangeID: 1, ReplicaID: 3, Index: 50}, Type: wagpb.EventApply},
			}},
			shouldApply: false,
		}, {
			name: "multi-event split, needs apply with per-range catch-ups",
			states: map[roachpb.RangeID]persistedRangeState{
				1: {mark: replicaMark(3, 0), appliedIndex: 99},
				// RHS was created (EventCreate applied) but not yet initialized.
				2: {mark: replicaMark(1, 0)},
			},
			node: wagpb.Node{Events: []wagpb.Event{
				// EventSplit for LHS (catch-up to index 99).
				{Addr: wagpb.Addr{RangeID: 1, ReplicaID: 3, Index: 100}, Type: wagpb.EventSplit},
				// EventInit for the RHS (no catch-up).
				{Addr: wagpb.Addr{RangeID: 2, ReplicaID: 1, Index: 10}, Type: wagpb.EventInit},
			}},
			shouldApply: true,
			expCatchUps: []raftCatchUpTarget{{rangeID: 1, index: 99}},
		}, {
			name: "multi-event split, already applied",
			states: map[roachpb.RangeID]persistedRangeState{
				1: {mark: replicaMark(3, 0), appliedIndex: 100},
				2: {mark: replicaMark(1, 1), appliedIndex: 10},
			},
			node: wagpb.Node{Events: []wagpb.Event{
				{Addr: wagpb.Addr{RangeID: 1, ReplicaID: 3, Index: 100}, Type: wagpb.EventSplit},
				{Addr: wagpb.Addr{RangeID: 2, ReplicaID: 1, Index: 10}, Type: wagpb.EventInit},
			}},
			shouldApply: false,
		}, {
			name: "multi-event disagreement returns error",
			states: map[roachpb.RangeID]persistedRangeState{
				1: {mark: replicaMark(3, 0), appliedIndex: 100},
				// RHS was created (EventCreate applied) but not yet initialized.
				2: {mark: replicaMark(1, 0)},
			},
			// LHS already applied but RHS not yet — should not happen.
			node: wagpb.Node{Events: []wagpb.Event{
				{Addr: wagpb.Addr{RangeID: 1, ReplicaID: 3, Index: 100}, Type: wagpb.EventSplit},
				{Addr: wagpb.Addr{RangeID: 2, ReplicaID: 1, Index: 10}, Type: wagpb.EventInit},
			}},
			expErr: "partial apply",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stateEng := storage.NewDefaultInMemForTesting()
			defer stateEng.Close()
			for rangeID, state := range tc.states {
				writePersistedRangeState(t, stateEng, rangeID, state)
			}
			apply, err := canApplyWAGNode(ctx, tc.node, StateRO(stateEng))
			if tc.expErr != "" {
				require.ErrorContains(t, err, tc.expErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.shouldApply, apply)
			var catchUps []raftCatchUpTarget
			if apply {
				catchUps = slices.Collect(wagNodeCatchUps(tc.node))
			}
			require.Equal(t, tc.expCatchUps, catchUps)
		})
	}
}

// simpleReplayBatch is a test-only ReplayBatch that advances the applied index
// for each entry without decoding. It persists the updated applied state on
// Commit.
type simpleReplayBatch struct {
	sl       StateLoader
	stateEng storage.Engine
	as       *kvserverpb.RangeAppliedState
}

func (b *simpleReplayBatch) AppliedIndex() kvpb.RaftIndex {
	return b.as.RaftAppliedIndex
}

func (b *simpleReplayBatch) ApplyEntry(_ context.Context, ent raftpb.Entry) (bool, error) {
	b.as.RaftAppliedIndex = kvpb.RaftIndex(ent.Index)
	return true, nil
}

func (b *simpleReplayBatch) Commit(ctx context.Context) error {
	return b.sl.SetRangeAppliedState(ctx, b.stateEng, b.as)
}

func (b *simpleReplayBatch) Close() {}

func simpleNewBatch(stateEng storage.Engine) NewReplayBatchFn {
	return func(ctx context.Context, rangeID roachpb.RangeID, _ roachpb.RKey) (ReplayBatch, error) {
		sl := MakeStateLoader(rangeID)
		as, err := sl.LoadRangeAppliedState(ctx, stateEng)
		if err != nil {
			return nil, err
		}
		return &simpleReplayBatch{sl: sl, stateEng: stateEng, as: as}, nil
	}
}

// TestReplayWAG is a basic end-to-end test for WAG replay, verifying that applied
// nodes are skipped and unapplied nodes have their mutations applied to the
// state machine.
//
// TODO(mira): Replace this test with a comprehensive datadriven suite.
func TestReplayWAG(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// getKey reads a single unversioned key from the given reader.
	getKey := func(t *testing.T, r storage.Reader, key string) []byte {
		t.Helper()
		kvs, err := storage.Scan(ctx, r, roachpb.Key(key), roachpb.Key(key).Next(), 1)
		require.NoError(t, err)
		if len(kvs) == 0 {
			return nil
		}
		return kvs[0].Value
	}

	// writeWAGNode creates a Batch, stages the given event and key-value pair
	// on it, and commits. The commit writes a WAG node to the log engine
	// containing the state batch repr as its mutation.
	writeWAGNode := func(
		t *testing.T, bf *BatchFactory, addr wagpb.Addr, typ wagpb.EventType, key, val string,
	) {
		t.Helper()
		b := bf.NewWriteBatch()
		defer b.Close()
		b.WagWriter().AddEvent(addr, typ, nil /* startKey */)
		require.NoError(t, b.State().PutUnversioned(roachpb.Key(key), []byte(val)))
		require.NoError(t, b.Commit(false /* sync */))
	}

	t.Run("empty WAG", func(t *testing.T) {
		eng := MakeSeparatedEnginesForTesting(
			storage.NewDefaultInMemForTesting(), storage.NewDefaultInMemForTesting(),
		)
		defer eng.Close()
		raftRO := RaftRO(eng.LogEngine())
		stateRW := StateRW(eng.StateEngine())

		noReplay := func(context.Context, roachpb.RangeID, roachpb.RKey) (ReplayBatch, error) {
			t.Fatal("unexpected newBatch call")
			return nil, nil
		}
		require.NoError(t, ReplayWAG(ctx, raftRO, stateRW, noReplay))
	})

	t.Run("unapplied nodes are applied", func(t *testing.T) {
		eng := MakeSeparatedEnginesForTesting(
			storage.NewDefaultInMemForTesting(), storage.NewDefaultInMemForTesting(),
		)
		defer eng.Close()
		var seq wag.Seq
		bf := MakeBatchFactory(&eng, &seq)
		raftRO := RaftRO(eng.LogEngine())
		stateRW := StateRW(eng.StateEngine())

		// Establish the replica in the state engine as initialized at index 10.
		writePersistedRangeState(t, stateRW, 1, persistedRangeState{
			mark: replicaMark(1, 0), appliedIndex: 10,
		})
		// Write raft log entries covering the full catch-up range.
		writeRaftLogEntries(t, eng.LogEngine(), 1, 11, 25)

		// WAG node at index 15: catch-up replays entries 11-15.
		writeWAGNode(t, &bf, wagpb.Addr{RangeID: 1, ReplicaID: 1, Index: 15}, wagpb.EventApply, "key1", "val1")

		require.NoError(t, ReplayWAG(ctx, raftRO, stateRW, simpleNewBatch(eng.StateEngine())))
		require.Equal(t, []byte("val1"), getKey(t, stateRW, "key1"))

		// Second WAG node at index 25: catch-up replays entries 16-25.
		// The first node should be skipped on this replay.
		writeWAGNode(t, &bf, wagpb.Addr{RangeID: 1, ReplicaID: 1, Index: 25}, wagpb.EventApply, "key2", "val2")

		require.NoError(t, ReplayWAG(ctx, raftRO, stateRW, simpleNewBatch(eng.StateEngine())))
		require.Equal(t, []byte("val1"), getKey(t, stateRW, "key1"))
		require.Equal(t, []byte("val2"), getKey(t, stateRW, "key2"))
	})
}
