// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag/wagpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// The following diagram illustrates the replica lifecycle for each range and
// each replica (with ID r) on a store. A WAG event can generally be replayed
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

// replayAction describes what the replay loop must do for a WAG node.
type replayAction struct {
	// apply indicates whether the WAG node's mutation needs to be applied.
	apply bool
	// catchUps lists ranges that must be caught up via raft log replay before
	// the WAG node can be applied. Empty when apply is false, and may be empty
	// when apply is true (e.g. for EventCreate/EventInit nodes).
	catchUps []raftCatchUpTarget
}

// raftCatchUpTarget identifies a range/replica that must be caught up to a
// specific raft index via raft log replay before a WAG node can be applied.
type raftCatchUpTarget struct {
	rangeID   roachpb.RangeID
	replicaID roachpb.ReplicaID
	index     kvpb.RaftIndex
}

func assert(cond bool, msg string, event wagpb.Event, state persistedRangeState) {
	if !cond {
		panic(errors.AssertionFailedf("%s: event %s, state %+v", msg, event, state))
	}
}

// assertValidTransition validates that an event being applied is a valid
// forward transition from the current replica state (see lifecycle diagram
// above). Events from the past of the replica's lifecycle are stale and
// skipped by canApply before reaching this function.
func (state persistedRangeState) assertValidTransition(event wagpb.Event) {
	if !state.mark.Is(event.Addr.ReplicaID) {
		// Nonexistent: only EventCreate (at index 0) is valid.
		assert(event.Type == wagpb.EventCreate && event.Addr.Index == 0, "first event for new replica is not EventCreate at index 0", event, state)
	} else if state.appliedIndex == 0 {
		// Uninitialized: only EventInit (at non-zero index) and EventDestroy
		// (at index 0) are valid.
		assert(
			(event.Type == wagpb.EventInit && event.Addr.Index > 0) ||
				(event.Type == wagpb.EventDestroy && event.Addr.Index == 0),
			"invalid forward transition from uninitialized replica", event, state,
		)
	} else {
		// Initialized: neither EventInit nor EventCreate are valid transitions.
		// All transitions satisfy i' >= i, with strict inequality for
		// everything except Destroy/Subsume.
		assert(event.Type != wagpb.EventInit && event.Type != wagpb.EventCreate,
			"EventInit or EventCreate on initialized replica", event, state)
		if event.Type == wagpb.EventDestroy || event.Type == wagpb.EventSubsume {
			assert(event.Addr.Index >= state.appliedIndex, "destroy/subsume index below applied index", event, state)
		} else {
			assert(event.Addr.Index > state.appliedIndex, "event index not above applied index", event, state)
		}
	}
}

// canApply reports whether a WAG event can be applied to the state machine,
// given the current applied state for the event's RangeID. It compares the
// event against the state machine's current position for this range.
//
// See the replica lifecycle diagram above, and canApplyWAGNode for the
// node-level wrapper.
func (state persistedRangeState) canApply(event wagpb.Event) (apply bool) {
	assert(event.Addr.ReplicaID != 0, "event with zero ReplicaID", event, state)
	switch {
	case state.mark.Destroyed(event.Addr.ReplicaID):
		// Old replica (destroyed or never existed); skip.
		apply = false
	case state.mark.Is(event.Addr.ReplicaID):
		// Current replica (initialized or not).
		//
		// Destroy/Subsume events always need applying here — if their mutation had
		// already been applied, the tombstone would have been bumped and the
		// Destroyed case would have matched.
		if event.Type == wagpb.EventDestroy || event.Type == wagpb.EventSubsume {
			apply = true
		} else {
			// For other events, compare raft indices.
			apply = event.Addr.Index > state.appliedIndex
		}
	case event.Addr.ReplicaID > state.mark.ReplicaID:
		// New replica: must enter the lifecycle via EventCreate.
		apply = true
	default:
		panic(errors.AssertionFailedf("unhandled: event %s, state %+v", event, state))
	}
	if apply {
		state.assertValidTransition(event)
	}
	return apply
}

// raftCatchUp returns the raft index the replica must be caught up to before
// this WAG event can be applied. Zero means no catch-up is needed.
func raftCatchUp(event wagpb.Event) kvpb.RaftIndex {
	switch event.Type {
	case wagpb.EventCreate, wagpb.EventInit:
		// No prior raft log; no catch-up.
		return 0
	case wagpb.EventApply, wagpb.EventSubsume, wagpb.EventDestroy:
		// Subsume, Destroy: the replica must be fully caught up before destruction.
		return event.Addr.Index
	case wagpb.EventSplit, wagpb.EventMerge:
		// The replica must be caught up to the command just before the
		// split/merge at event.Index.
		return event.Addr.Index - 1
	default:
		panic(errors.AssertionFailedf("unexpected event type %d", event.Type))
	}
}

// canApplyWAGNode determines whether a WAG node's mutation can be applied to
// the state machine. It checks each event in the node and returns the replay
// action, which indicates whether to apply and which ranges need raft log
// catch-up first.
//
// All events in a node are expected to agree on whether they need applying,
// since they are written and applied atomically.
func canApplyWAGNode(ctx context.Context, node wagpb.Node, stateRO StateRO) (replayAction, error) {
	var result replayAction
	for i, event := range node.Events {
		state, err := loadPersistedRangeState(ctx, stateRO, event.Addr.RangeID)
		if err != nil {
			return replayAction{}, errors.Wrapf(err, "loading state for r%d", event.Addr.RangeID)
		}
		// A given RangeID appears at most once in the events list, so the decision
		// of whether an event can be applied is independent for each event.
		apply := state.canApply(event)
		if i == 0 {
			result.apply = apply
		} else if apply != result.apply {
			return replayAction{}, errors.Newf(
				"partial apply: event[0]=%s (apply=%t), event[%d]=%s (apply=%t)",
				node.Events[0], result.apply, i, event, apply,
			)
		}
		if !apply {
			continue
		}
		if catchUp := raftCatchUp(event); catchUp > 0 {
			result.catchUps = append(result.catchUps, raftCatchUpTarget{
				rangeID:   event.Addr.RangeID,
				replicaID: event.Addr.ReplicaID,
				index:     catchUp,
			})
		}
	}
	return result, nil
}

// ReplayWAG iterates over the WAG in the log engine and applies any unapplied
// nodes to the state machine. It is called during store startup, before the
// store goes online.
func ReplayWAG(ctx context.Context, raftRO RaftRO, stateRW StateRW) error {
	var iter wag.Iterator
	for wagIdx, node := range iter.Iter(ctx, raftRO) {
		action, err := canApplyWAGNode(ctx, node, stateRW)
		if err != nil {
			return errors.Wrapf(err, "WAG node %d", wagIdx)
		}
		if !action.apply {
			continue
		}
		// TODO(mira): For each entry in action.catchUps, replay raft log
		// entries for the target range/replica up to the target index before
		// applying the WAG node. The current raft log replay in
		// handleRaftReadyRaftMuLocked needs to be factored out and invoked here.
		// The catch-up code should assert that the target index is >= the
		// replica's current applied index.
		if err := applyMutation(ctx, stateRW, node.Mutation); err != nil {
			return errors.Wrapf(err, "WAG node %d", wagIdx)
		}
	}
	return iter.Error()
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
