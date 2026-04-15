// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"math"
	"sync/atomic"

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

// WAGTruncatorTestingKnobs contains testing knobs for the WAGTruncator.
type WAGTruncatorTestingKnobs struct{}

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
	seq *wag.Seq
	// initIndex is the WAG index at which the sequencer and truncator were
	// initialized. New WAG nodes are guaranteed to be written at > initIndex.
	// Any gaps at <= initIndex are thus never filled.
	//
	// Due to concurrent WAG writes in normal operation, indices > initIndex may
	// temporarily have gaps, guaranteed to be filled eventually. Only an abrupt
	// shutdown/crash can cause these gaps to become permanent.
	//
	// We don't truncate ahead of gaps at > initIndex, and wait for them to be
	// filled. This makes an invariant: only an entire prefix is truncated which
	// allows more efficient Pebble scans that skip over tombstones.
	initIndex uint64
	// truncIndex is the index of the last WAG node that was successfully
	// truncated.
	truncIndex atomic.Uint64
	knobs      WAGTruncatorTestingKnobs
}

// NewWAGTruncator creates a WAGTruncator.
func NewWAGTruncator(st *cluster.Settings, eng Engines, seq *wag.Seq) *WAGTruncator {
	return &WAGTruncator{st: st, eng: eng, seq: seq, initIndex: seq.Load()}
}

// truncateAppliedNodes deletes the longest fully applied prefix of the WAG.
// Does so iteratively, splitting its work into batches.
func (t *WAGTruncator) truncateAppliedNodes(ctx context.Context) error {
	stateReader := t.eng.StateEngine().NewReader(storage.GuaranteedDurability)
	defer stateReader.Close()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b := t.eng.LogEngine().NewWriteBatch()
		truncatedIdx, err := t.truncateAppliedWAGNodeAndClearRaftState(
			ctx, Raft{RO: t.eng.LogEngine(), WO: b}, stateReader,
		)
		if err == nil && truncatedIdx != 0 {
			err = b.Commit(false /* sync */)
		}
		b.Close()
		if err != nil {
			return err
		}
		if truncatedIdx == 0 {
			return nil
		}
		// At this point we know that the last truncation succeeded and the batch
		// was committed, and we can move on to the next node.
		t.truncIndex.Store(truncatedIdx)
	}
}

// truncateAppliedWAGNodeAndClearRaftState deletes the first WAG node if all of
// its events have been applied to the state engine. For nodes containing
// EventDestroy or EventSubsume events, it also clears the corresponding raft
// log prefix from the engine and the sideloaded entries storage.
//
// Returns the index of the last WAG node that was deleted, or 0 if no nodes
// were deleted.
//
// The caller must provide a stateRO reader with GuaranteedDurability so that
// only state confirmed flushed to persistent storage is visible. This ensures
// we never delete a WAG node whose mutations aren't flushed yet. The caller is
// also responsible for creating and committing/closing the write batch in
// raft.WO.
// TODO(ibrahim): Support deleting multiple WAG nodes within the same batch.
func (t *WAGTruncator) truncateAppliedWAGNodeAndClearRaftState(
	ctx context.Context, raft Raft, stateRO StateRO,
) (uint64, error) {
	var iter wag.Iterator
	truncateIndex := t.truncIndex.Load() + 1
	iterStartKey := keys.StoreWAGNodeKey(truncateIndex)
	for index, node := range iter.IterFrom(ctx, raft.RO, iterStartKey) {
		if index != truncateIndex && index > t.initIndex {
			// We cannot ignore gaps for WAG indices > initIndex.
			return 0, nil
		}
		// TODO(ibrahim): Right now, the canApplyWAGNode function returns a list of
		// raftCatchUpTargets that are not needed for the purposes of truncation,
		// consider refactoring the function to return only the needed info.
		action, err := canApplyWAGNode(ctx, node, stateRO)
		if err != nil {
			return 0, err
		}
		if action.apply {
			// If an event needs to be applied, the WAG node cannot be deleted yet.
			return 0, nil
		}
		if err := wag.Delete(raft.WO, index); err != nil {
			return 0, err
		}

		// Clean up the raft log prefix of a destroyed/subsumed replica.
		for _, event := range node.Events {
			if event.Type != wagpb.EventDestroy && event.Type != wagpb.EventSubsume {
				continue
			}
			if err := t.clearReplicaRaftLogAndSideloaded(ctx, raft, event.Addr.RangeID, event.Addr.Index); err != nil {
				return 0, err
			}
		}
		return index, nil
	}
	return 0, iter.Error()
}

// clearReplicaRaftLogAndSideloaded clears raft log entries at or below the given index for
// a destroyed or subsumed replica, and it also deletes the sideloaded files associated with the
// deleted entries.
func (t *WAGTruncator) clearReplicaRaftLogAndSideloaded(
	ctx context.Context, raft Raft, rangeID roachpb.RangeID, lastIndex kvpb.RaftIndex,
) error {
	if err := storage.ClearRangeWithHeuristic(
		ctx, raft.RO, raft.WO,
		keys.RaftLogPrefix(rangeID),           /* start */
		keys.RaftLogKey(rangeID, lastIndex+1), /* end */
		ClearRangeThresholdPointKeys(),
	); err != nil {
		return errors.Wrapf(err, "clearing raft log entries for r%d", rangeID)
	}

	// In general, we shouldn't delete sideloaded files before committing the
	// batch that deletes the Raft log entries. However, in this case, we
	// know that the destroy/subsume event has already been flushed to disk,
	// and the replica is destroyed. We will not need to reference the
	// sideloaded files anymore. If we were to delay deleting them till
	// after the batch is committed, we risk leaking the sideloaded files
	// if a node crash happens after the batch is committed (and synced) and
	// before the sideloaded files are deleted.
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
