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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

// raftCatchUpTarget identifies a range/replica that must be caught up to a
// specific raft index via raft log replay before a WAG node can be applied.
type raftCatchUpTarget struct {
	rangeID   roachpb.RangeID
	replicaID roachpb.ReplicaID
	index     kvpb.RaftIndex
}

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
				rangeID:   e.Addr.RangeID,
				replicaID: e.Addr.ReplicaID,
				index:     catchUp,
			}) {
				return
			}
		}
	}
}

// ReplayWAG iterates over the WAG in the log engine and applies any unapplied
// nodes to the state machine. It is called during store startup, before the
// store goes online.
func ReplayWAG(ctx context.Context, raftRO RaftRO, stateRW StateRW) error {
	var it wag.Iterator
	for wagIdx, node := range it.Iter(ctx, raftRO) {
		if apply, err := canApplyWAGNode(ctx, node, stateRW); err != nil {
			return errors.Wrapf(err, "WAG node %d", wagIdx)
		} else if !apply {
			continue
		}
		// TODO(mira): For each target in wagNodeCatchUps(node), replay raft log
		// entries for the target range/replica up to the target index before
		// applying the WAG node. The current raft log replay in
		// handleRaftReadyRaftMuLocked needs to be factored out and invoked here.
		// The catch-up code should assert that the target index is >= the
		// replica's current applied index.
		if err := applyMutation(ctx, stateRW, node.Mutation); err != nil {
			return errors.Wrapf(err, "WAG node %d", wagIdx)
		}
	}
	return it.Error()
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
