// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag/wagpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"
)

// WAGTruncator truncates applied WAG nodes and clears their associated raft
// state (log entries and sideloaded files). It supports both offline and online
// mode of operation:
//   - [offline] during the store startup, truncate the WAG after it is
//     replayed and made durable in the StateEngine.
//   - [online] during the normal operation, truncate the WAG nodes that were
//     durably applied to the state machine.
//
// TODO(ibrahim): Add the periodic truncation logic.
type WAGTruncator struct {
	st  *cluster.Settings
	eng Engines
}

// NewWAGTruncator creates a WAGTruncator.
func NewWAGTruncator(st *cluster.Settings, eng Engines) *WAGTruncator {
	return &WAGTruncator{st: st, eng: eng}
}

// TruncateAll truncates all applied WAG nodes. It's meant to be used at engine
// startup right after we replay the WAG nodes and sync the state engine.
func (t *WAGTruncator) TruncateAll(ctx context.Context) error {
	stateReader := t.eng.StateEngine().NewReader(storage.GuaranteedDurability)
	defer stateReader.Close()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b := t.eng.LogEngine().NewWriteBatch()
		truncated, err := t.truncateAppliedWAGNodeAndClearRaftState(
			ctx, Raft{RO: t.eng.LogEngine(), WO: b}, stateReader, 0, /* index */
		)
		if err == nil && truncated {
			err = b.Commit(false /* sync */)
		}
		b.Close()
		if err != nil {
			return err
		}
		if !truncated {
			break
		}
	}
	return nil
}

// truncateAppliedWAGNodeAndClearRaftState deletes a WAG node if all of its
// events have been applied to the state engine. For nodes containing
// EventDestroy or EventSubsume events, it also clears the corresponding raft
// log prefix from the engine and the sideloaded entries storage.
// If truncateIndex is 0, the function deletes the first WAG node regardless of
// its index. Otherwise, it only deletes the node matching truncateIndex if it
// exists.
//
// Returns a boolean indicating whether a node was successfully truncated or
// not. If the return value is false, it means that either there are no WAG
// nodes left, or that the WAG node has not been applied to the state engine.
// Also, an error is returned if the WAG node could not be fetched or deleted.
//
// The caller must provide a stateRO reader with GuaranteedDurability so that
// only state confirmed flushed to persistent storage is visible. This ensures
// we never delete a WAG node whose mutations aren't flushed yet. The caller is
// also responsible for creating and committing/closing the write batch in
// raft.WO.
// TODO(ibrahim): Support deleting multiple WAG nodes within the same batch.
func (t *WAGTruncator) truncateAppliedWAGNodeAndClearRaftState(
	ctx context.Context, raft Raft, stateRO StateRO, truncateIndex uint64,
) (bool, error) {
	var iter wag.Iterator
	var iterStartKey roachpb.Key
	if truncateIndex == 0 {
		// Delete the first WAG node that exists, regardless of its index.
		iterStartKey = keys.StoreWAGPrefix()
	} else {
		// Only delete the WAG node with the expected index.
		iterStartKey = keys.StoreWAGNodeKey(truncateIndex)
	}

	for index, node := range iter.IterFrom(ctx, raft.RO, iterStartKey) {
		if truncateIndex != 0 && truncateIndex != index {
			return false, nil
		}

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
			if err := t.clearReplicaRaftLog(ctx, raft, event.Addr.RangeID, event.Addr.Index); err != nil {
				return false, errors.Wrapf(err, "clearing raft log for r%d", event.Addr.RangeID)
			}
			// In general, we shouldn't delete sideloaded files before committing the
			// batch that deletes the Raft log entries. However, in this case, we
			// know that the destroy/subsume event has already been flushed to disk,
			// and the replica is destroyed. We will not need to reference the
			// sideloaded files anymore. If we were to delay deleting them till
			// after the batch is committed, we risk leaking the sideloaded files
			// if a node crash happens after the batch is committed (and synced) and
			// before the sideloaded files are deleted.
			if err := t.clearSideloaded(ctx, event.Addr.RangeID, event.Addr.Index); err != nil {
				return false, errors.Wrapf(err, "clearing sideloaded files for r%d", event.Addr.RangeID)
			}
		}
		return true, nil
	}
	return false, iter.Error()
}

// clearReplicaRaftLog clears raft log entries at or below the given index for
// a destroyed or subsumed replica.
func (t *WAGTruncator) clearReplicaRaftLog(
	ctx context.Context, raft Raft, rangeID roachpb.RangeID, lastIndex kvpb.RaftIndex,
) error {
	return storage.ClearRangeWithHeuristic(
		ctx, raft.RO, raft.WO,
		keys.RaftLogPrefix(rangeID),           /* start */
		keys.RaftLogKey(rangeID, lastIndex+1), /* end */
		ClearRangeThresholdPointKeys(),
	)
}

// clearSideloaded clears sideloaded files belonging to raft log entries at or
// below the given index for a destroyed or subsumed replica.
func (t *WAGTruncator) clearSideloaded(
	ctx context.Context, rangeID roachpb.RangeID, lastIndex kvpb.RaftIndex,
) error {
	// TODO(ibrahim): Consider doing some refactoring here to avoid creating a new
	//  DiskSideloadStorage every time we want to clear some sideloaded files.
	ss := logstore.NewDiskSideloadStorage(t.st, rangeID, t.eng.StateEngine().GetAuxiliaryDir(),
		rate.NewLimiter(rate.Inf, math.MaxInt64), t.eng.StateEngine().Env())
	if err := ss.TruncateTo(ctx, lastIndex); err != nil {
		return err
	}
	// We must sync the sideloaded storage after truncation to avoid leaking the
	// files in case of a node crash.
	return ss.Sync()
}
