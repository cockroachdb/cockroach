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

// persistedRangeState describes the applied state of a range in the state
// machine, as needed by the WAG replay decision logic.
type persistedRangeState struct {
	replicaID              roachpb.ReplicaID
	tombstoneNextReplicaID roachpb.ReplicaID
	appliedIndex           kvpb.RaftIndex
}

// validate checks persistedRangeState invariants.
func (s persistedRangeState) validate() error {
	// When a replica exists (ReplicaID > 0), the tombstone must not exceed the
	// replica ID. The tombstone is only bumped above a replica's ID when that
	// replica is destroyed.
	if s.replicaID > 0 && s.tombstoneNextReplicaID > s.replicaID {
		return errors.AssertionFailedf(
			"tombstone (NextReplicaID=%d) is above current ReplicaID=%d",
			s.tombstoneNextReplicaID, s.replicaID,
		)
	}
	return nil
}

// loadPersistedRangeState loads the replay-relevant state for a range from the
// state machine using StateLoader.
func loadPersistedRangeState(
	ctx context.Context, stateRO StateRO, rangeID roachpb.RangeID,
) (persistedRangeState, error) {
	sl := MakeStateLoader(rangeID)
	rid, err := sl.LoadRaftReplicaID(ctx, stateRO)
	if err != nil {
		return persistedRangeState{}, err
	}
	ts, err := sl.LoadRangeTombstone(ctx, stateRO)
	if err != nil {
		return persistedRangeState{}, err
	}
	as, err := sl.LoadRangeAppliedState(ctx, stateRO)
	if err != nil {
		return persistedRangeState{}, err
	}
	state := persistedRangeState{
		replicaID:              rid.ReplicaID,
		tombstoneNextReplicaID: ts.NextReplicaID,
		appliedIndex:           as.RaftAppliedIndex,
	}
	return state, state.validate()
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

// canApply reports whether a WAG event can be applied to the state machine,
// given the current applied state for the event's RangeID. It compares the
// event against the state machine's current position for this range. See
// canApplyWAGNode for the node-level wrapper.
//
// The decision is based on where event replica ID falls relative to
// state.replicaID on the number line, giving three regions:
//
//	[0, state.replicaID)   → old (destroyed or never existed); skip
//	state.replicaID        → current replica; compare raft indices
//	(state.replicaID, ∞)   → new replica; apply
//
// TODO(mira): Refactor to use ReplicaMark (#156696) which will encapsulate
// the replicaID/tombstone comparison logic.
//
// TODO(mira): Some of the cases below are not possible for all event types.
// E.g. For a new replica with event.Addr.ReplicaID > state.replicaID, we'd
// expect an EventCreate, not another type of event. Assert on these.
func (state persistedRangeState) canApply(event wagpb.Event) bool {
	// The WAG protocol ensures that any WAG node event has a non-zero ReplicaID.
	if event.Addr.ReplicaID == 0 {
		panic(errors.AssertionFailedf("WAG event for r%d has zero ReplicaID", event.Addr.RangeID))
	}
	switch {
	case event.Addr.ReplicaID < state.tombstoneNextReplicaID ||
		event.Addr.ReplicaID < state.replicaID:
		// Old replica (destroyed or never existed); skip. The persistedRangeState
		// validation guarantees state.tombstoneNextReplicaID <= state.replicaID
		// when a replica exists, but we can't rely on the state.replicaID
		// comparison alone because when no current replica exists
		// (state.replicaID == 0), only the tombstone can identify stale events.
		return false
	case event.Addr.ReplicaID == state.replicaID:
		// Current replica. Destroy/Subsume events always need applying here —
		// if their mutation had already been applied, the tombstone would have
		// been bumped and the first case would have matched.
		if event.Type == wagpb.EventDestroy || event.Type == wagpb.EventSubsume {
			return true
		}
		// For other events, compare raft indices.
		return event.Addr.Index > state.appliedIndex
	case event.Addr.ReplicaID > state.replicaID:
		// New replica not yet seen on this store; apply.
		return true
	default:
		panic(errors.AssertionFailedf("unhandled: event %s, state %+v", event, state))
	}
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
