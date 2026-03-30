// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package wag

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag/wagpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
)

// RangeAppliedState describes the applied state of a range in the state machine,
// as needed by the WAG replay decision logic.
type RangeAppliedState struct {
	ReplicaID              roachpb.ReplicaID
	TombstoneNextReplicaID roachpb.ReplicaID
	AppliedIndex           kvpb.RaftIndex
}

// AppliedStateLoader loads the applied state for a range from the state machine.
// It is called during WAG replay to determine whether each WAG node needs to be
// applied.
type AppliedStateLoader func(
	ctx context.Context, rangeID roachpb.RangeID,
) (RangeAppliedState, error)

// ReplayAction describes what the replay loop must do for a WAG node.
type ReplayAction struct {
	// Apply indicates whether the WAG node's mutation needs to be applied.
	Apply bool
	// CatchUps lists ranges that must be caught up via raft log replay before
	// the WAG node can be applied. Empty when Apply is false, and may be empty
	// when Apply is true (e.g. for EventCreate/EventInit nodes).
	CatchUps []RaftCatchUpTarget
}

// RaftCatchUpTarget identifies a range/replica that must be caught up to a
// specific raft index via raft log replay before a WAG node can be applied.
type RaftCatchUpTarget struct {
	RangeID   roachpb.RangeID
	ReplicaID roachpb.ReplicaID
	Index     kvpb.RaftIndex
}

// checkAddrInvariants asserts preconditions for ShouldApply:
//
//  1. WAG events must reference a real replica (event.ReplicaID > 0).
//  2. A live replica's ID is always >= the tombstone (state.Tombstone <=
//     state.ReplicaID). Replicas are rejected at creation if their ID is
//     below the tombstone (see WriteUninitializedReplicaState), and the
//     tombstone is only bumped above a replica's ID when that replica is
//     destroyed (see DestroyReplica).
func checkAddrInvariants(event wagpb.Addr, state RangeAppliedState) {
	// Assertion 1.
	if event.ReplicaID == 0 {
		panic(errors.AssertionFailedf("WAG event for r%d has zero ReplicaID", event.RangeID))
	}
	// Assertion 2.
	if state.ReplicaID > 0 && state.TombstoneNextReplicaID > state.ReplicaID {
		panic(
			errors.AssertionFailedf(
				"tombstone (NextReplicaID=%d) is above current ReplicaID=%d for r%d",
				state.TombstoneNextReplicaID, state.ReplicaID, event.RangeID,
			),
		)
	}
}

// ShouldApply reports whether a WAG event needs to be applied to the state
// engine, given the current applied state for the event's RangeID. It compares
// the event against the state machine's current position for this range. See
// NodeShouldApply for the node-level wrapper.
//
// The decision is based on where event.ReplicaID falls relative to
// state.TombstoneNextReplicaID and state.ReplicaID on the number line.
// Assertion 2 (see checkAddrInvariants) guarantees state.NextReplicaID <=
// state.ReplicaID when a replica exists, giving four regions:
//
//	[0, state.NextReplicaID)             → destroyed; skip
//	[state.NextReplicaID, state.Replica) → stale; skip
//	state.Replica                    → current replica; compare raft indices
//	(state.Replica, ∞)               → new replica; apply
//
// The third case relies on assertion 1 (see checkAddrInvariants) to guarantee
// both IDs are positive — without it, two zero IDs would match and compare
// indices for a non-existent replica.
//
// TODO(mira): refactor to use ReplicaMark (#156696) which will encapsulate
// the replicaID/tombstone comparison logic.
//
// When no replica exists (state.ReplicaID == 0), the tombstone may also be
// zero (fresh range) or positive (replica was destroyed). Either way, any
// event above the tombstone is for a new replica and should be applied.
func ShouldApply(event wagpb.Event, state RangeAppliedState) bool {
	checkAddrInvariants(event.Addr, state)
	switch {
	case event.Addr.ReplicaID < state.TombstoneNextReplicaID:
		// Destroyed replica; skip.
		return false
	case event.Addr.ReplicaID < state.ReplicaID:
		// Stale replica superseded by a newer one; skip.
		return false
	case event.Addr.ReplicaID == state.ReplicaID:
		// Current replica. Destroy/Subsume events always need applying here —
		// if their mutation had already been applied, the tombstone would have
		// been bumped and the first case would have matched.
		if event.Type == wagpb.EventDestroy || event.Type == wagpb.EventSubsume {
			return true
		}
		// For other events, compare raft indices.
		return event.Addr.Index > state.AppliedIndex
	default:
		// New replica not yet seen on this store; apply.
		return true
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
		panic(errors.AssertionFailedf("unexpected event type %s", event.Type))
	}
}

// NodeShouldApply determines whether a WAG node's mutation needs to be applied
// to the state machine. It checks each event in the node and returns the replay
// action, which indicates whether to apply and which ranges need raft log
// catch-up first.
//
// All events in a node are expected to agree on whether they need applying,
// since they are written and applied atomically. If events disagree, this
// indicates a bug and triggers an assertion failure.
func NodeShouldApply(
	ctx context.Context, node wagpb.Node, loadState AppliedStateLoader,
) (ReplayAction, error) {
	var result ReplayAction
	for i, event := range node.Events {
		state, err := loadState(ctx, event.Addr.RangeID)
		if err != nil {
			return ReplayAction{}, errors.Wrapf(err, "loading state for r%d", event.Addr.RangeID)
		}
		apply := ShouldApply(event, state)
		if i == 0 {
			result.Apply = apply
		} else if apply != result.Apply {
			return ReplayAction{}, errors.Newf(
				"WAG node events disagree on whether to apply: event[0]=%s (apply=%t), event[%d]=%s (apply=%t)",
				node.Events[0], result.Apply, i, event, apply,
			)
		}
		if !apply {
			continue
		}
		if catchUp := raftCatchUp(event); catchUp > 0 {
			result.CatchUps = append(
				result.CatchUps, RaftCatchUpTarget{
					RangeID:   event.Addr.RangeID,
					ReplicaID: event.Addr.ReplicaID,
					Index:     catchUp,
				},
			)
		}
	}
	return result, nil
}

// Replay iterates over the WAG in the log engine and applies any unapplied
// nodes to the state machine. It is called during store startup, before the
// store goes online.
func Replay(
	ctx context.Context,
	logEngine storage.Reader,
	stateEngine storage.Engine,
	loadState AppliedStateLoader,
) error {
	var iter Iterator
	for node := range iter.Iter(ctx, logEngine) {
		action, err := NodeShouldApply(ctx, node, loadState)
		if err != nil {
			return err
		}
		if !action.Apply {
			continue
		}
		// TODO(mira): for each entry in action.CatchUps, replay raft log
		// entries for the target range/replica up to the target index before
		// applying the WAG node. The current raft log replay in
		// handleRaftReadyRaftMuLocked needs to be factored out and invoked here.
		// The catch-up code should assert that the target index is >= the
		// replica's current applied index.
		if err := applyMutation(ctx, stateEngine, node.Mutation); err != nil {
			return err
		}
	}
	return iter.Error()
}

// applyMutation applies a WAG node's mutation to the state machine. It handles
// both write batch and ingestion mutations.
func applyMutation(ctx context.Context, stateEngine storage.Engine, m wagpb.Mutation) error {
	if m.Ingestion != nil {
		// TODO(mira): implement ingestion replay. This requires translating the
		// Ingestion proto (SST paths, shared/external tables) into the appropriate
		// IngestAndExciseFiles call on the state machine.
		return errors.UnimplementedErrorf(
			errors.IssueLink{}, "WAG ingestion replay not yet implemented",
		)
	}
	if len(m.Batch) > 0 {
		return stateEngine.ApplyBatchRepr(m.Batch, false /* sync */)
	}
	return nil
}
