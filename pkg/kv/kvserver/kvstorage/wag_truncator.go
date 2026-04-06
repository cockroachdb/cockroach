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
	"github.com/cockroachdb/errors"
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

// SideloadClearer truncates sideloaded files for a given range up to and
// including the specified raft log index. Files beyond this index may belong
// to a newer replica and must be preserved.
type SideloadClearer func(ctx context.Context, rangeID roachpb.RangeID, upToIndex kvpb.RaftIndex) error

// clearReplicaRaftLog clears raft log entries at or below the given index for
// a destroyed or subsumed replica.
func clearReplicaRaftLog(
	ctx context.Context, raft Raft, rangeID roachpb.RangeID, lastIndex kvpb.RaftIndex,
) error {
	prefixBuf := keys.MakeRangeIDPrefixBuf(rangeID)
	start := prefixBuf.RaftLogPrefix()
	end := prefixBuf.RaftLogKey(lastIndex).Next()
	if err := storage.ClearRangeWithHeuristic(
		ctx, raft.RO, raft.WO, start, end, ClearRangeThresholdPointKeys(),
	); err != nil {
		return err
	}
	return nil
}

// TruncateAppliedWAGNodeAndClearRaftState checks the first WAG node and
// deletes it if all of its events have been applied to the state engine. For
// nodes containing EventDestroy or EventSubsume events, it also clears the
// raft log entries and sideloaded files for those replicas.
//
// The caller must provide a stateRO reader with GuaranteedDurability so that
// only state confirmed flushed to persistent storage is visible. This ensures
// we never delete a WAG node whose mutations aren't flushed yet.
// TODO(ibrahim): Support deleting multiple WAG nodes within the same batch.
func TruncateAppliedWAGNodeAndClearRaftState(
	ctx context.Context, raft Raft, stateRO StateRO, clearSideloaded SideloadClearer,
) (bool, error) {
	var iter wag.Iterator
	for index, node := range iter.Iter(ctx, raft.RO) {
		applied, err := isNodeApplied(ctx, stateRO, node)
		if err != nil || !applied {
			return false, err
		}
		if err := wag.Delete(raft.WO, index); err != nil {
			return false, err
		}

		// Clean up the Raft state of a destroyed/subsumed replica.
		for _, event := range node.Events {
			if event.Type != wagpb.EventDestroy && event.Type != wagpb.EventSubsume {
				continue
			}
			if err := clearReplicaRaftLog(ctx, raft, event.Addr.RangeID, event.Addr.Index); err != nil {
				return false, errors.Wrapf(err, "clearing raft log for r%d", event.Addr.RangeID)
			}
			if clearSideloaded != nil {
				// In general, we shouldn't delete sideloaded files before commiting the
				// batch that deletes the Raft log entries. However, in this case, we
				// know that the destroy/subsume event has already been flushed to disk,
				// and the replica is destroyed. We will not need to reference the
				// sideloaded files anymore.
				if err := clearSideloaded(ctx, event.Addr.RangeID, event.Addr.Index); err != nil {
					return false, errors.Wrapf(err, "clearing sideloaded files for r%d", event.Addr.RangeID)
				}
			}
		}
		return true, nil
	}
	return false, iter.Error()
}
