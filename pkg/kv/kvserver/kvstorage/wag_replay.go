// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"iter"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag/wagpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
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
// store goes online. For each unapplied node, it first replays raft log entries
// to catch up the affected ranges, then applies the node's mutation.
func ReplayWAG(ctx context.Context, raftRO RaftRO, stateRW StateRW) error {
	var it wag.Iterator
	for wagIdx, node := range it.Iter(ctx, raftRO) {
		if apply, err := canApplyWAGNode(ctx, node, stateRW); err != nil {
			return errors.Wrapf(err, "WAG node %d", wagIdx)
		} else if !apply {
			continue
		}
		for target := range wagNodeCatchUps(node) {
			// Load the ReplicaState for CheckForcedErr to reproduce command
			// accept/reject decisions during raft log replay.
			//
			// TODO(mira): We pass a minimal descriptor because loading it by
			// range ID requires a scan (descriptors are keyed by start key). Does
			// this make CheckForcedErr's lease request validation inaccurate?
			sl := MakeStateLoader(target.rangeID)
			desc := roachpb.RangeDescriptor{RangeID: target.rangeID}
			state, err := sl.Load(ctx, stateRW, &desc)
			if err != nil {
				return errors.Wrapf(err, "WAG node %d: loading state for r%d", wagIdx, target.rangeID)
			}
			if err := replayRaftLog(ctx, raftRO, stateRW, &state, target); err != nil {
				return errors.Wrapf(err, "WAG node %d: catch-up r%d", wagIdx, target.rangeID)
			}
			// Persist the updated applied state after catch-up.
			as := state.ToRangeAppliedState()
			if err := sl.SetRangeAppliedState(ctx, stateRW, &as); err != nil {
				return errors.Wrapf(err, "WAG node %d: persisting state for r%d", wagIdx, target.rangeID)
			}
		}
		if err := applyMutation(ctx, stateRW, node.Mutation); err != nil {
			return errors.Wrapf(err, "WAG node %d", wagIdx)
		}
	}
	return it.Error()
}

// replayRaftLog applies raft log entries for a range from its current applied
// index up to the given target index. It iterates entries from the raft log and
// applies each one using CheckForcedErr to reproduce the same accept/reject
// decisions as the original application. The caller provides the initial
// ReplicaState, which is mutated in place as entries are applied.
func replayRaftLog(
	ctx context.Context,
	raftRO RaftRO,
	stateWO StateWO,
	state *kvserverpb.ReplicaState,
	target raftCatchUpTarget,
) error {
	if target.index < state.RaftAppliedIndex {
		return errors.AssertionFailedf(
			"catch-up target index %d < applied index %d for r%d",
			target.index, state.RaftAppliedIndex, target.rangeID,
		)
	}
	if target.index == state.RaftAppliedIndex {
		return nil // already caught up
	}

	// Apply entries from appliedIndex+1 through target index.
	lo := state.RaftAppliedIndex + 1
	hi := target.index + 1 // Visit uses [lo, hi)
	var cmd raftlog.ReplicatedCmd
	if err := raftlog.Visit(ctx, raftRO, target.rangeID, lo, hi, func(ent raftpb.Entry) error {
		if err := cmd.Decode(&ent); err != nil {
			return err
		}
		return applyEntry(ctx, state, stateWO, &cmd)
	}); err != nil {
		return errors.Wrapf(err, "replaying raft log for r%d", target.rangeID)
	}

	// Verify we replayed all expected entries.
	if state.RaftAppliedIndex != target.index {
		return errors.AssertionFailedf(
			"raft log replay for r%d reached index %d, expected %d",
			target.rangeID, state.RaftAppliedIndex, target.index,
		)
	}

	return nil
}

// applyEntry validates and applies a single decoded raft entry to the state
// machine. It uses CheckForcedErr to reproduce the same accept/reject decisions
// as the online application path, applies the write batch for accepted commands,
// and updates the in-memory applied state.
//
// Each entry's write batch is applied directly to the engine, but the applied
// state (RangeAppliedState) is only updated in memory — the caller is
// responsible for persisting it. This means the writes and applied state are
// not atomic: if we crash mid-replay, entries may be re-applied on restart.
// This is safe as long as the write batches are idempotent (MVCC puts at the
// same timestamp are).
func applyEntry(
	ctx context.Context, state *kvserverpb.ReplicaState, writer StateWO, cmd *raftlog.ReplicatedCmd,
) error {
	if cmd.Index() == 0 {
		return errors.AssertionFailedf("applying an entry requires a non-zero index")
	}
	if idx, applied := cmd.Index(), state.RaftAppliedIndex; idx != applied+1 {
		return errors.AssertionFailedf("applied index jumped from %d to %d", applied, idx)
	}

	// Reproduce the CheckForcedErr logic to determine if this command should be
	// applied or rejected. Rejected commands are applied as no-ops.
	fr := kvserverbase.CheckForcedErr(ctx, cmd.ID, &cmd.Cmd, false /* isLocal */, state)
	if fr.ForcedError != nil {
		// Rejected: apply as an empty command.
		cmd.Cmd.ReplicatedEvalResult = kvserverpb.ReplicatedEvalResult{}
		cmd.Cmd.WriteBatch = nil
		cmd.Cmd.ClosedTimestamp = nil
	}

	// TODO(mira): AddSST, LinkExternalSSTable, and Excise have side effects
	// beyond the write batch. Can these commands appear in the catch-up range,
	// or are they always covered by the WAG node's mutation?
	res := cmd.ReplicatedResult()
	if res.AddSSTable != nil || res.LinkExternalSSTable != nil || res.Excise != nil {
		return errors.UnimplementedErrorf(
			errors.IssueLink{},
			"standalone log application does not yet handle AddSST/LinkExternalSSTable/Excise"+
				" at index %d", cmd.Index(),
		)
	}

	if wb := cmd.Cmd.WriteBatch; wb != nil {
		if err := writer.ApplyBatchRepr(wb.Data, false /* sync */); err != nil {
			return errors.Wrap(err, "applying write batch")
		}
	}

	// Update the applied state to reflect this entry.
	state.RaftAppliedIndex = cmd.Index()
	state.RaftAppliedIndexTerm = kvpb.RaftTerm(cmd.Term)
	if leaseAppliedIndex := fr.LeaseIndex; leaseAppliedIndex != 0 {
		state.LeaseAppliedIndex = leaseAppliedIndex
	}
	if cts := cmd.Cmd.ClosedTimestamp; cts != nil && !cts.IsEmpty() {
		state.RaftClosedTimestamp = *cts
	}
	state.Stats.Add(res.Delta.ToStats())
	return nil
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
