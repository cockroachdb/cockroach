// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"iter"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag/wagpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// The following diagram illustrates the replica lifecycle for each replica
// (with ID r) on a store. A WAG event can generally be replayed
// safely if it brings the replica's applied state from one valid state to
// another following the allowed transitions. The WAG application logic below
// (canApply) asserts on these transitions (assertValidTransition).
//
// For each transition from one state to another, let i be the applied index of
// the old state, and i' be the applied index of the new state. Some transitions
// include a restriction on these indices.
//
//                                                                 EventApply
//                                                                 EventSplit
//                                                                 EventMerge
//                                                                 (i' > i)
//                                                                  ┌────────┐
// ┌─────────────────┐             ┌──────────────┐ EventInit ┌─────▼──────┐ │
// │ Nonexistent     │ EventCreate │ Uninitialized│ (i' > i)  │ Initialized│ │
// │ !mark.Exists(r) ├────────────▶│ mark.Is(r)   ├──────────▶│ mark.Is(r) ├─┘
// └─────────────────┘             │ i == 0       │           │ i >= 10    │
//                                 └────┬─────────┘           └──┬─────────┘
//                         EventDestroy │                        │ EventDestroy
//                         (i' == 0)    │                        │ EventSubsume
//                                      └───────────────┬────────┘ (i' >= i)
//                                                      │
//                                            ┌─────────▼─────────┐
//                                            │ Destroyed         │
//                                            │ mark.Destroyed(r) │
//                                            └───────────────────┘
//
// TODO(mira): Clarify the snapshot transition. We may need a distinct event
// type for it.

// persistedRangeState describes the applied state of a range in the state
// machine, as needed by the WAG replay decision logic.
type persistedRangeState struct {
	mark         ReplicaMark
	appliedIndex kvpb.RaftIndex
}

// loadPersistedRangeState loads the replay-relevant state for a range from the
// state machine using StateLoader.
func loadPersistedRangeState(
	ctx context.Context, stateRO StateRO, rangeID roachpb.RangeID,
) (persistedRangeState, error) {
	sl := MakeStateLoader(rangeID)
	mark, err := sl.LoadReplicaMark(ctx, stateRO)
	if err != nil {
		return persistedRangeState{}, err
	}
	as, err := sl.LoadRangeAppliedState(ctx, stateRO)
	if err != nil {
		return persistedRangeState{}, err
	}
	return persistedRangeState{
		mark:         mark,
		appliedIndex: as.RaftAppliedIndex,
	}, nil
}

// raftCatchUpTarget identifies a range that must be caught up to a specific
// raft index via raft log replay before a WAG node can be applied. The start
// key is used to load the range descriptor via a point read.
type raftCatchUpTarget struct {
	rangeID  roachpb.RangeID
	startKey roachpb.RKey
	index    kvpb.RaftIndex
}

// ReplayBatch applies raft entries to the state machine via an internal storage
// batch. The implementation is responsible for decoding entries and staging
// their writes. The caller must call Close when done, whether or not Commit
// was called.
type ReplayBatch interface {
	// ApplyEntry applies a single raft entry. Returns whether the entry had
	// only trivial side effects (no lease change, GC threshold bump, etc.).
	ApplyEntry(context.Context, raftpb.Entry) (trivial bool, _ error)
	// AppliedIndex returns the raft applied index after the last ApplyEntry,
	// or the initial applied index if no entries have been applied yet.
	AppliedIndex() kvpb.RaftIndex
	// Commit writes the applied state and commits the batch.
	Commit(context.Context) error
	// Close releases batch resources. Safe to call after Commit.
	Close()
}

// NewReplayBatchFn creates a fresh ReplayBatch for a range by loading the
// current ReplicaState from the engine. The startKey is used to locate the
// range descriptor via a point read.
type NewReplayBatchFn func(ctx context.Context, rangeID roachpb.RangeID, startKey roachpb.RKey) (ReplayBatch, error)

func assert(cond bool, msg string, e wagpb.Event, s persistedRangeState) {
	if !cond {
		panic(errors.AssertionFailedf("%s: event %s, state %+v", msg, e, s))
	}
}

// assertValidTransition validates that an event being applied is a valid
// forward transition from the current replica state (see lifecycle diagram
// above). Events from the past of the replica's lifecycle are stale and
// skipped by canApply before reaching this function.
func (s persistedRangeState) assertValidTransition(e wagpb.Event) {
	if mark, id := s.mark, e.Addr.ReplicaID; mark.Destroyed(id) {
		assert(false, "replica is destroyed", e, s)
	} else if !mark.Is(id) {
		// Nonexistent: only EventCreate (at index 0) is valid.
		assert(e.Type == wagpb.EventCreate && e.Addr.Index == 0,
			"replica must start with EventCreate at index 0", e, s)
	} else if s.appliedIndex == 0 {
		// Uninitialized: only EventInit (at non-zero index) and EventDestroy
		// (at index 0) are valid.
		assert(
			(e.Type == wagpb.EventInit && e.Addr.Index > 0) ||
				(e.Type == wagpb.EventDestroy && e.Addr.Index == 0),
			"invalid forward transition from uninitialized replica", e, s,
		)
	} else {
		// Initialized: neither EventInit nor EventCreate are valid transitions.
		// All transitions satisfy i' >= i, with strict inequality for
		// everything except Destroy/Subsume.
		assert(e.Type != wagpb.EventInit && e.Type != wagpb.EventCreate,
			"EventInit or EventCreate on initialized replica", e, s)
		if e.Type == wagpb.EventDestroy || e.Type == wagpb.EventSubsume {
			assert(e.Addr.Index >= s.appliedIndex, "destroying below applied index", e, s)
		} else {
			assert(e.Addr.Index > s.appliedIndex, "applied index must advance", e, s)
		}
	}
}

// canApply reports whether a WAG event can be applied to the state machine,
// given the current applied state for the event's RangeID. It compares the
// event against the state machine's current position for this range.
//
// See the replica lifecycle diagram above, and canApplyWAGNode for the
// node-level wrapper.
func (s persistedRangeState) canApply(e wagpb.Event) (apply bool) {
	assert(e.Addr.ReplicaID != 0, "event with zero ReplicaID", e, s)
	switch {
	case s.mark.Destroyed(e.Addr.ReplicaID):
		// Old replica (destroyed or never existed); skip.
		apply = false
	case s.mark.Is(e.Addr.ReplicaID):
		// Current replica (initialized or not).
		//
		// Destroy/Subsume events always need applying here — if their mutation had
		// already been applied, the tombstone would have been bumped and the
		// Destroyed case would have matched.
		if e.Type == wagpb.EventDestroy || e.Type == wagpb.EventSubsume {
			apply = true
		} else {
			// For other events, compare raft indices.
			apply = e.Addr.Index > s.appliedIndex
		}
	case e.Addr.ReplicaID > s.mark.ReplicaID:
		// New replica: must enter the lifecycle via EventCreate.
		apply = true
	default:
		panic(errors.AssertionFailedf("unhandled: event %s, state %+v", e, s))
	}
	if apply {
		s.assertValidTransition(e)
	}
	return apply
}

// raftCatchUp returns the raft index the replica must be caught up to before
// this WAG event can be applied. Zero means no catch-up is needed.
func raftCatchUp(e wagpb.Event) kvpb.RaftIndex {
	switch e.Type {
	case wagpb.EventCreate, wagpb.EventInit:
		// No prior raft log; no catch-up.
		return 0
	case wagpb.EventApply, wagpb.EventSubsume, wagpb.EventDestroy:
		// Subsume, Destroy: the replica must be fully caught up before destruction.
		return e.Addr.Index
	case wagpb.EventSplit, wagpb.EventMerge:
		// The replica must be caught up to the command just before the
		// split/merge at e.Index.
		return e.Addr.Index - 1
	default:
		panic(errors.AssertionFailedf("unexpected event type %d", e.Type))
	}
}

// canApplyWAGNode determines whether a WAG node's mutation can be applied to
// the state machine. It checks each event in the node against the persisted
// range state to decide if the node still needs applying.
//
// All events in a node are expected to agree on whether they need applying,
// since they are written and applied atomically.
func canApplyWAGNode(ctx context.Context, node wagpb.Node, stateRO StateRO) (bool, error) {
	var apply bool
	for i, e := range node.Events {
		s, err := loadPersistedRangeState(ctx, stateRO, e.Addr.RangeID)
		if err != nil {
			return false, errors.Wrapf(err, "loading state for r%d", e.Addr.RangeID)
		}
		// A given RangeID appears at most once in the events list, so the decision
		// of whether an event can be applied is independent for each event.
		if evApply := s.canApply(e); i == 0 {
			apply = evApply
		} else if evApply != apply {
			return false, errors.Newf(
				"partial apply: event[0]=%s (apply=%t), event[%d]=%s (apply=%t)",
				node.Events[0], apply, i, e, evApply,
			)
		}
	}
	return apply, nil
}

// wagNodeCatchUps returns an iterator over the raft catch-up targets for a WAG
// node's events. Each target identifies a range/replica that must be caught up
// to a specific raft index before the node can be applied. The caller must have
// already determined that the node needs applying via canApplyWAGNode.
func wagNodeCatchUps(node wagpb.Node) iter.Seq[raftCatchUpTarget] {
	return func(yield func(raftCatchUpTarget) bool) {
		for _, e := range node.Events {
			if catchUp := raftCatchUp(e); catchUp > 0 && !yield(raftCatchUpTarget{
				rangeID:  e.Addr.RangeID,
				startKey: e.StartKey,
				index:    catchUp,
			}) {
				return
			}
		}
	}
}

// ReplayWAG iterates over the WAG in the log engine and applies any unapplied
// nodes to the state machine. It is called during store startup, before the
// store goes online. For each unapplied node, it first replays raft log entries
// to catch up the affected ranges, then applies the node's mutation.
//
// The newBatch callback creates ReplayBatch instances that apply raft entries
// to the state machine. It is the only injection point from kvserver.
func ReplayWAG(
	ctx context.Context, raftRO RaftRO, stateRW StateRW, newBatch NewReplayBatchFn,
) error {
	var it wag.Iterator
	for wagIdx, node := range it.Iter(ctx, raftRO) {
		if apply, err := canApplyWAGNode(ctx, node, stateRW); err != nil {
			return errors.Wrapf(err, "WAG node %d", wagIdx)
		} else if !apply {
			log.KvExec.VInfof(ctx, 2, "skipping already-applied WAG node %d: %s",
				wagIdx, node.Events)
			continue
		}
		log.KvExec.VInfof(ctx, 2, "applying WAG node %d: %s", wagIdx, node.Events)
		for target := range wagNodeCatchUps(node) {
			if err := replayRaftLog(ctx, raftRO, target, newBatch); err != nil {
				return errors.Wrapf(err, "WAG node %d: catch-up r%d", wagIdx, target.rangeID)
			}
		}
		if err := applyMutation(ctx, stateRW, node.Mutation); err != nil {
			return errors.Wrapf(err, "WAG node %d", wagIdx)
		}
	}
	return it.Error()
}

// replayRaftLog replays raft log entries for a range from its current applied
// index up to the target index. It creates a ReplayBatch, iterates entries via
// raftlog.Visit, and applies each one. Non-trivial entries (lease changes, GC
// threshold bumps, etc.) trigger a batch flush and state reload so that
// subsequent entries are checked against up-to-date state.
func replayRaftLog(
	ctx context.Context, raftRO RaftRO, target raftCatchUpTarget, newBatch NewReplayBatchFn,
) error {
	for {
		done, err := replayRaftLogBatch(ctx, raftRO, target, newBatch)
		if err != nil {
			return errors.Wrapf(err, "replaying raft log for r%d", target.rangeID)
		}
		if done {
			return nil
		}
	}
}

// replayRaftLogBatch creates a ReplayBatch and applies entries from the
// current applied index up to the target. It returns done=true when all
// entries have been applied, or done=false if a non-trivial entry was
// encountered and the caller should invoke this function again so that a
// fresh batch is created with reloaded state.
func replayRaftLogBatch(
	ctx context.Context, raftRO RaftRO, target raftCatchUpTarget, newBatch NewReplayBatchFn,
) (done bool, _ error) {
	rb, err := newBatch(ctx, target.rangeID, target.startKey)
	if err != nil {
		return false, err
	}
	defer rb.Close()

	lo, hi := rb.AppliedIndex(), target.index
	if lo > hi {
		return false, errors.AssertionFailedf(
			"target index %d is behind applied index %d for r%d",
			hi, lo, target.rangeID)
	}
	if lo == hi {
		return true, nil
	}

	// TODO(mira): Add a max batch size policy to bound memory usage. Large
	// ranges with many entries could produce an oversized batch here.
	var needsReload bool
	// NB: Visit uses [start, end) semantics.
	err = raftlog.Visit(ctx, raftRO, target.rangeID, lo+1, hi+1, func(ent raftpb.Entry) error {
		trivial, err := rb.ApplyEntry(ctx, ent)
		if err != nil {
			return err
		}
		if !trivial {
			// Non-trivial entries may change state (lease, GC threshold)
			// that affects accept/reject decisions for subsequent entries.
			// Stop iteration so the caller can flush and reload.
			needsReload = true
			return iterutil.StopIteration()
		}
		return nil
	})
	if err = iterutil.Map(err); err != nil {
		return false, err
	}
	if err := rb.Commit(ctx); err != nil {
		return false, err
	}
	return !needsReload, nil
}

// applyMutation applies a WAG node's mutation to the state machine. It handles
// both write batch and ingestion mutations. The ingestion is applied before the
// batch, because a mutation may have both: the ingestion is idempotent, and the
// batch "finalizes" the mutation (e.g. updates the applied index which inputs
// into the "is applied" decision).
func applyMutation(ctx context.Context, stateWO StateWO, m wagpb.Mutation) error {
	if m.Ingestion != nil {
		// TODO(mira): Implement ingestion replay. This requires translating the
		// Ingestion proto (SST paths, shared/external tables) into the appropriate
		// IngestAndExciseFiles call on the state machine.
		return errors.UnimplementedErrorf(
			errors.IssueLink{}, "WAG ingestion replay not yet implemented",
		)
	}
	if len(m.Batch) > 0 {
		return stateWO.ApplyBatchRepr(m.Batch, false /* sync */)
	}
	return nil
}
