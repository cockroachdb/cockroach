// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag/wagpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// truncationOutcome indicates whether the truncation truncated any WAG nodes,
// some, or all.
type truncationOutcome int

const (
	// TruncatedNone means no WAG nodes existed or none were applied.
	TruncatedNone truncationOutcome = iota + 1
	// TruncatedSome means some but not all WAG nodes were deleted.
	TruncatedSome
	// TruncatedAll means all WAG nodes were deleted.
	TruncatedAll
)

// PersistedRangeState describes the applied state of a range in the state
// machine, as needed by the WAG replay decision logic.
type PersistedRangeState struct {
	ReplicaID              roachpb.ReplicaID
	TombstoneNextReplicaID roachpb.ReplicaID
	AppliedIndex           kvpb.RaftIndex
}

// loadPersistedRangeState loads the replay-relevant state for a range from the
// state machine using StateLoader.
// TODO(ibrahim): Merge these helper functions with the helper functions that
// are used during WAG replay.
func loadPersistedRangeState(
	ctx context.Context, stateRO StateRO, rangeID roachpb.RangeID,
) (PersistedRangeState, error) {
	sl := MakeStateLoader(rangeID)
	rid, err := sl.LoadRaftReplicaID(ctx, stateRO)
	if err != nil {
		return PersistedRangeState{}, err
	}
	ts, err := sl.LoadRangeTombstone(ctx, stateRO)
	if err != nil {
		return PersistedRangeState{}, err
	}
	as, err := sl.LoadRangeAppliedState(ctx, stateRO)
	if err != nil {
		return PersistedRangeState{}, err
	}
	return PersistedRangeState{
		ReplicaID:              rid.ReplicaID,
		TombstoneNextReplicaID: ts.NextReplicaID,
		AppliedIndex:           as.RaftAppliedIndex,
	}, nil
}

// isEventApplied checks whether the state for the given RangeID has progressed
// past the given Addr. Per the WAG Addr ordering, a higher ReplicaID implies
// all events for lower ReplicaIDs (including their destruction) have been
// applied, and the same ReplicaID is ordered by Index.
// TODO(Ibrahim): Refactor to use ReplicaMark (#156696) which will encapsulate
// the replicaID/tombstone comparison logic.
// TODO(ibrahim): Merge these helper functions with the helper functions that
// are used during WAG replay.
func isEventApplied(
	ctx context.Context, state PersistedRangeState, event wagpb.Event,
) (bool, error) {
	switch {
	case event.Addr.ReplicaID < state.TombstoneNextReplicaID:
		// Destroyed replica; skip.
		return true, nil
	case event.Addr.ReplicaID < state.ReplicaID:
		// Stale replica superseded by a newer one; skip.
		return true, nil
	case event.Addr.ReplicaID == state.ReplicaID:
		if event.Type == wagpb.EventDestroy || event.Type == wagpb.EventSubsume {
			return false, nil
		}
		// For other events, compare raft indices.
		return event.Addr.Index <= state.AppliedIndex, nil
	default:
		// New replica not yet seen on this store; apply.
		return false, nil
	}
}

// isNodeApplied checks whether all events in a WAG node have been applied to
// the state engine, by comparing each event's Addr against the replica state.
// TODO(ibrahim): Merge these helper functions with the helper functions that
// are used during WAG replay.
func isNodeApplied(ctx context.Context, stateRO StateRO, node wagpb.Node) (bool, error) {
	for _, event := range node.Events {
		state, err := loadPersistedRangeState(ctx, stateRO, event.Addr.RangeID)
		if err != nil {
			return false, errors.Wrapf(err, "loading state for r%d", event.Addr.RangeID)
		}

		applied, err := isEventApplied(ctx, state, event)
		if err != nil {
			return false, err
		}
		if !applied {
			return false, nil
		}
	}
	return true, nil
}

// destroyedReplica identifies a replica whose raft state needs to be cleaned
// up after its WAG destroy/subsume event has been applied and truncated.
type destroyedReplica struct {
	RangeID roachpb.RangeID
	// LastIndex is the replica's last applied raft log index before
	// destruction/subsumption (from the WAG event's Addr.Index). Only raft log
	// entries and sideloaded files at or below this index belong to the old
	// replica and should be cleared. Entries above this index were already
	// removed when the WAG node was written, or may belong to a newer replica.
	LastIndex kvpb.RaftIndex
}

// truncationResult holds the outcome of a truncateAppliedNodes call.
type truncationResult struct {
	// outcome indicates whether no, some, or all WAG nodes were deleted.
	outcome truncationOutcome
	// destroyed contains information about replicas whose raft state needs to
	// be cleaned up.
	destroyed []destroyedReplica
}

// truncateAppliedNodes deletes WAG nodes that have been applied. It iterates
// over WAG nodes in order, checks whether each node has been applied, and
// deletes it if that was the case. It stops at the first unapplied node.
//
// For nodes containing EventDestroy or EventSubsume events, the corresponding
// replica information is collected and returned, so that the caller can clear
// the raft log entries and sideloaded files for those replicas.
//
// The caller must provide a stateRO reader with GuaranteedDurability so that
// only state confirmed flushed to persistent storage is visible. This ensures
// we never delete a WAG node whose mutations aren't flushed yet.
// TODO(ibrahim): Make this function capable of stopping once it detects a gap
// in the WAG sequence.
func truncateAppliedNodes(
	ctx context.Context, raftRW RaftRW, stateRO StateRO,
) (truncationResult, error) {
	var iter wag.Iterator
	seen, deleted := 0, 0
	var destroyed []destroyedReplica
	for index, node := range iter.Iter(ctx, raftRW) {
		seen++
		applied, err := isNodeApplied(ctx, stateRO, node)
		if err != nil {
			return truncationResult{}, err
		}
		if !applied {
			break
		}
		if err := wag.Delete(raftRW, index); err != nil {
			return truncationResult{}, err
		}
		deleted++
		for _, event := range node.Events {
			if event.Type == wagpb.EventDestroy ||
				event.Type == wagpb.EventSubsume {
				destroyed = append(destroyed, destroyedReplica{
					RangeID:   event.Addr.RangeID,
					LastIndex: event.Addr.Index,
				})
			}
		}
	}
	if err := iter.Error(); err != nil {
		return truncationResult{}, err
	}
	if deleted == 0 {
		return truncationResult{outcome: TruncatedNone}, nil
	}

	log.KvExec.Infof(ctx, "truncated %d applied WAG nodes", deleted)
	res := TruncatedSome
	if deleted == seen {
		res = TruncatedAll
	}
	return truncationResult{
		outcome:   res,
		destroyed: destroyed,
	}, nil
}

// clearReplicaRaftLog clears raft log entries at or below the given index for
// a destroyed or subsumed replica.
func clearReplicaRaftLog(
	ctx context.Context, raftRW RaftRW, rangeID roachpb.RangeID, lastIndex kvpb.RaftIndex,
) error {
	prefixBuf := keys.MakeRangeIDPrefixBuf(rangeID)
	start := prefixBuf.RaftLogPrefix()
	end := prefixBuf.RaftLogKey(lastIndex).Next()
	if err := storage.ClearRangeWithHeuristic(
		ctx, raftRW, raftRW, start, end, ClearRangeThresholdPointKeys(),
	); err != nil {
		return err
	}
	log.KvExec.Infof(ctx,
		"cleared raft log entries up to index %d for destroyed/subsumed r%d",
		lastIndex, rangeID,
	)
	return nil
}

// SideloadClearer truncates sideloaded files for a given range up to and
// including the specified raft log index. Files beyond this index may belong
// to a newer replica and must be preserved.
type SideloadClearer func(ctx context.Context, rangeID roachpb.RangeID, upToIndex kvpb.RaftIndex) error

// TruncateWagNodesAndClearRaftState truncates applied WAG nodes and clears the
// raft log entries and sideloaded files for any destroyed or subsumed replicas
// found during truncation.
//
// After a replica is destroyed or subsumed, its state machine mutation has
// already been applied (clearing replicated state, writing the tombstone,
// etc.). This function garbage-collects the leftover raft log entries and
// sideloaded files that are no longer needed. Only entries at or below the
// destroyed replica's last applied index are cleared — entries above that were
// already removed when the WAG node was written, or may belong to a newer
// replica. HardState and RaftTruncatedState are not cleared because they are
// keyed by RangeID and may already belong to a newer replica.
func TruncateWagNodesAndClearRaftState(
	ctx context.Context, raftRW RaftRW, stateRO StateRO, clearSideloaded SideloadClearer,
) (truncationResult, error) {
	tr, err := truncateAppliedNodes(ctx, raftRW, stateRO)
	if err != nil {
		return truncationResult{}, err
	}
	for _, repl := range tr.destroyed {
		if err := clearReplicaRaftLog(ctx, raftRW, repl.RangeID, repl.LastIndex); err != nil {
			return truncationResult{}, errors.Wrapf(err, "clearing raft log for r%d", repl.RangeID)
		}
		if clearSideloaded != nil {
			// In general, we shouldn't delete sideloaded files before commiting the
			// batch that deletes the Raft log entries. However, in this case, we know
			// that the destroy/subsume event has already been flushed to disk, and
			// the replica is destroyed. We will not need to reference the sideloaded
			// files anymore.
			if err := clearSideloaded(ctx, repl.RangeID, repl.LastIndex); err != nil {
				return truncationResult{}, errors.Wrapf(err, "clearing sideloaded files for r%d", repl.RangeID)
			}
		}
	}
	return tr, nil
}
