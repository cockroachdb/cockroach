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

// canApply reports whether a WAG event can be applied to the state machine,
// given the current applied state for the event's RangeID. It compares the
// event against the state machine's current position for this range. See
// canApplyWAGNode for the node-level wrapper.
//
// The decision is based on where the event's replica ID falls relative to
// the current ReplicaMark, giving three cases:
//
//   - Destroyed: the replica ID is below the mark's tombstone or current
//     replica ID, meaning it can never (re-)appear. Skip.
//   - Current: the replica ID matches the mark's current replica. Compare
//     raft indices to determine whether the event has already been applied.
//   - New: the replica ID is above the mark's current replica and not
//     destroyed. Apply.
//
// TODO(mira): Some of the cases below are not possible for all event types.
// E.g. For a new replica with event.Addr.ReplicaID > state.mark.ReplicaID,
// we'd expect an EventCreate, not another type of event. Assert on these.
func (state persistedRangeState) canApply(event wagpb.Event) bool {
	// The WAG protocol ensures that any WAG node event has a non-zero ReplicaID.
	if event.Addr.ReplicaID == 0 {
		panic(errors.AssertionFailedf("WAG event for r%d has zero ReplicaID", event.Addr.RangeID))
	}
	switch {
	case state.mark.Destroyed(event.Addr.ReplicaID):
		// Old replica (destroyed or never existed); skip.
		return false
	case state.mark.Is(event.Addr.ReplicaID):
		// Current replica. Destroy/Subsume events always need applying here —
		// if their mutation had already been applied, the tombstone would have
		// been bumped and the Destroyed case would have matched.
		if event.Type == wagpb.EventDestroy || event.Type == wagpb.EventSubsume {
			return true
		}
		// For other events, compare raft indices.
		return event.Addr.Index > state.appliedIndex
	case event.Addr.ReplicaID > state.mark.ReplicaID:
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
// the state machine. It checks each event in the node against the persisted
// range state to decide if the node still needs applying.
//
// All events in a node are expected to agree on whether they need applying,
// since they are written and applied atomically.
func canApplyWAGNode(ctx context.Context, node wagpb.Node, stateRO StateRO) (bool, error) {
	var apply bool
	for i, event := range node.Events {
		state, err := loadPersistedRangeState(ctx, stateRO, event.Addr.RangeID)
		if err != nil {
			return false, errors.Wrapf(err, "loading state for r%d", event.Addr.RangeID)
		}
		// A given RangeID appears at most once in the events list, so the decision
		// of whether an event can be applied is independent for each event.
		if evApply := state.canApply(event); i == 0 {
			apply = evApply
		} else if evApply != apply {
			return false, errors.Newf(
				"partial apply: event[0]=%s (apply=%t), event[%d]=%s (apply=%t)",
				node.Events[0], apply, i, event, evApply,
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
		for _, event := range node.Events {
			if catchUp := raftCatchUp(event); catchUp > 0 && !yield(raftCatchUpTarget{
				rangeID:   event.Addr.RangeID,
				replicaID: event.Addr.ReplicaID,
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
