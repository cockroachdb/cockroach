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

// SideloadClearer truncates sideloaded files for a given range up to and
// including the specified raft log index. Files beyond this index may belong
// to a newer replica and must be preserved.
// The SideloadClearer must also sync the sideloaded storage after truncation to
// avoid leaking the files in case of a node crash.
type SideloadClearer func(ctx context.Context, rangeID roachpb.RangeID, lastIndex kvpb.RaftIndex) error

// clearReplicaRaftLog clears raft log entries at or below the given index for
// a destroyed or subsumed replica.
func clearReplicaRaftLog(
	ctx context.Context, raft Raft, rangeID roachpb.RangeID, lastIndex kvpb.RaftIndex,
) error {
	return storage.ClearRangeWithHeuristic(
		ctx, raft.RO, raft.WO,
		keys.RaftLogPrefix(rangeID),           /* start */
		keys.RaftLogKey(rangeID, lastIndex+1), /* end */
		ClearRangeThresholdPointKeys(),
	)
}

// truncateAppliedWAGNodeAndClearRaftState checks the first WAG node and
// deletes it if all of its events have been applied to the state engine. For
// nodes containing EventDestroy or EventSubsume events, it also clears the
// corresponding raft log prefix from the engine and the sideloaded entries
// storage.
//
// The caller must provide a stateRO reader with GuaranteedDurability so that
// only state confirmed flushed to persistent storage is visible. This ensures
// we never delete a WAG node whose mutations aren't flushed yet.
//
// Returns a boolean indicating whether a node was successfully truncated or
// not. If the return value is false, it means that either there are no WAG
// nodes left, or that the WAG node has not been applied to the state engine.
// Also, an error is returned if the WAG node could not be fetched or deleted.
// TODO(ibrahim): Support deleting multiple WAG nodes within the same batch.
func truncateAppliedWAGNodeAndClearRaftState(
	ctx context.Context, raft Raft, stateRO StateRO, clearSideloaded SideloadClearer,
) (bool, error) {
	var iter wag.Iterator
	for index, node := range iter.Iter(ctx, raft.RO) {
		// TODO(ibrahim): Right now, the canApplyWAGNode function returns a list of
		// raftCatchUpTargets that are not needed for the purposes of truncation,
		// consider refactoring the function to return only the needed info.
		replayAction, err := canApplyWAGNode(ctx, node, stateRO)
		if err != nil {
			return false, err
		}
		if replayAction.apply {
			// If an event needs to be applied, the WAG node cannot be deleted yet.
			return false, nil
		}
		if err := wag.Delete(raft.WO, index); err != nil {
			return false, err
		}

		// Clean up the raft log prefix of a destroyed/subsumed replica.
		for _, event := range node.Events {
			if event.Type != wagpb.EventDestroy && event.Type != wagpb.EventSubsume {
				continue
			}
			if err := clearReplicaRaftLog(ctx, raft, event.Addr.RangeID, event.Addr.Index); err != nil {
				return false, errors.Wrapf(err, "clearing raft log for r%d", event.Addr.RangeID)
			}
			if clearSideloaded != nil {
				// In general, we shouldn't delete sideloaded files before committing the
				// batch that deletes the Raft log entries. However, in this case, we
				// know that the destroy/subsume event has already been flushed to disk,
				// and the replica is destroyed. We will not need to reference the
				// sideloaded files anymore. If we were to delay deleting them till
				// after the batch is committed, we risk leaking the sideloaded files
				// if a node crash happens after the batch is committed (and synced) and
				// before the sideloaded files are deleted.
				if err := clearSideloaded(ctx, event.Addr.RangeID, event.Addr.Index); err != nil {
					return false, errors.Wrapf(err, "clearing sideloaded files for r%d", event.Addr.RangeID)
				}
			}
		}
		return true, nil
	}
	return false, iter.Error()
}
