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

// truncateAppliedNodes deletes WAG nodes that have been applied. It iterates
// over WAG nodes in order, checks whether each node has been applied, and
// deletes it if that was the case. It stops at the first unapplied node.
// The result indicates whether no nodes, some nodes, or all nodes were deleted.
//
// The caller must provide a stateRO reader with GuaranteedDurability so that
// only state confirmed flushed to persistent storage is visible. This ensures
// we never delete a WAG node whose mutations aren't flushed yet.
// TODO(Ibrahim): Add logic to clear sideloaded files for destroyed/subsumed
// replicas.
func truncateAppliedNodes(
	ctx context.Context, raftRW RaftRW, stateRO StateRO,
) (truncationOutcome, error) {
	var iter wag.Iterator
	seen, deleted := 0, 0
	for index, node := range iter.Iter(ctx, raftRW) {
		seen++
		applied, err := isNodeApplied(ctx, stateRO, node)
		if err != nil {
			return 0, err
		}
		if !applied {
			break
		}
		if err := wag.Delete(raftRW, index); err != nil {
			return 0, err
		}
		deleted++
		// TODO(Ibrahim): Add logic to clear raft state (log entries, HardState,
		// TruncatedState) for destroyed/subsumed replicas.
	}
	if err := iter.Error(); err != nil {
		return 0, err
	}
	if deleted == 0 {
		return TruncatedNone, nil
	}

	log.KvExec.Infof(ctx, "truncated %d applied WAG nodes", deleted)
	if deleted == seen {
		return TruncatedAll, nil
	}
	return TruncatedSome, nil
}
