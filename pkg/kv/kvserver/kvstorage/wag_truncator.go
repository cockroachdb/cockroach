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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
type WAGTruncator struct {
	st  *cluster.Settings
	eng Engines
	seq *wag.Seq
	// wakeCh is signaled when there are potential WAG nodes to truncate.
	wakeCh chan struct{}
	// lastTruncatedWAGIndex is the index of the last WAG node that was
	// successfully truncated. This is to be used by online WAG truncation to
	// quickly seek into the potential WAG nodes to truncate.
	lastTruncatedWAGIndex atomic.Uint64
}

// NewWAGTruncator creates a WAGTruncator.
func NewWAGTruncator(st *cluster.Settings, eng Engines, seq *wag.Seq) *WAGTruncator {
	return &WAGTruncator{
		st:     st,
		eng:    eng,
		seq:    seq,
		wakeCh: make(chan struct{}, 1),
	}
}

// Start launches the background goroutine that performs live WAG truncation.
// It must be called before starting the engine. It sets lastTruncatedWAGIndex,
// and we need to set it before any WAG node could have been created so that we
// guarantee that the periodic WAG truncation will start from the first WAG node.
//
// TODO(ibrahim): Add a TestKnob for keeping a suffix of the WAG for debugging.
func (t *WAGTruncator) Start(ctx context.Context, stopper *stop.Stopper) error {
	t.lastTruncatedWAGIndex.Store(t.seq.Load())
	return stopper.RunAsyncTask(ctx, "wag-truncation", func(ctx context.Context) {
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()
		for {
			select {
			case <-t.wakeCh:
				t.truncateAppliedNodesLive(ctx)
			case <-ctx.Done():
				return
			}
		}
	})
}

// DurabilityAdvancedCallback is invoked whenever the state engine completes a
// flush. It checks whether there could possibly be WAG nodes to truncate by
// comparing lastTruncatedWAGIndex against seq.Load(). If there are potential
// truncation opportunities, it sends a non-blocking signal to wake the
// background goroutine. It must return quickly and must not call into the
// engine to avoid deadlock (see storage.Engine.RegisterFlushCompletedCallback).
func (t *WAGTruncator) DurabilityAdvancedCallback() {
	// The key point is the point in time where we call "t.seq.Load()". If the
	// sequence number is greater than the last truncated WAG sequence number, we
	// will attempt to truncate if there are any WAG nodes that are ready for
	// truncation. If it found no WAG nodes that are ready to be truncated, it
	// will attempt to truncate on every state engine flush until
	// sequence number == lastTruncatedWAGIndex.
	seq, lastTruncated := t.seq.Load(), t.lastTruncatedWAGIndex.Load()
	if seq == lastTruncated {
		return
	}
	if seq < lastTruncated {
		log.KvExec.Fatalf(context.Background(),
			"WAG seq %d < lastTruncatedWAGIndex %d", seq, lastTruncated)
	}
	select {
	case t.wakeCh <- struct{}{}:
	default:
	}
}

// TruncateAll truncates all applied WAG nodes. It's meant to be used at engine
// startup right after we replay the WAG nodes and sync the state engine.
func (t *WAGTruncator) TruncateAll(ctx context.Context) error {
	_, err := t.truncateAppliedNodes(ctx, 0)
	return err
}

// truncateAppliedNodesLive is called by the background goroutine to truncate
// applied WAG nodes. Unlike TruncateAll, it only truncates nodes with index
// after lastTruncatedWAGIndex. This makes it avoid jumping over gaps in WAG
// node indices.
//
// Note that jumping over gaps during WAG truncation should be safe because a
// WAG node is only truncated if it has been applied to the state engine, and it
// by definition has all the required events applied. However, we avoid jumping
// over gaps so that we know the exact index to truncate next, which facilitates
// seeking past previously deleted WAG garbage.
func (t *WAGTruncator) truncateAppliedNodesLive(ctx context.Context) {
	startIndex := t.lastTruncatedWAGIndex.Load() + 1
	lastTruncated, err := t.truncateAppliedNodes(ctx, startIndex)
	if err != nil {
		// TODO(ibrahim): We need to decide if we need to retry truncating this
		//  WAG node now, or rely on the next state engine flush to do it.
		log.KvExec.Errorf(ctx, "truncating WAG node: %+v", err)
	}
	if lastTruncated >= startIndex {
		t.lastTruncatedWAGIndex.Store(lastTruncated)
	}
}

// truncateAppliedNodes is a helper function used by both offline and online WAG
// truncation functions. It repeatedly tries to delete a WAG node in a batch and
// commit that batch. When startIndex is 0, it repeatedly truncates the first
// available node (used at startup to drain the WAG). When startIndex is greater
// than 0, it truncates nodes sequentially starting from that index (used during
// online operation to avoid jumping over gaps in WAG indices).
//
// Returns the index of the last successfully truncated node, or 0 if no nodes
// were truncated.
func (t *WAGTruncator) truncateAppliedNodes(
	ctx context.Context, startIndex uint64,
) (uint64, error) {
	stateReader := t.eng.StateEngine().NewReader(storage.GuaranteedDurability)
	defer stateReader.Close()
	nextIndex := startIndex
	var lastTruncated uint64
	for {
		if err := ctx.Err(); err != nil {
			return lastTruncated, err
		}
		b := t.eng.LogEngine().NewWriteBatch()
		truncated, err := t.truncateAppliedWAGNodeAndClearRaftState(
			ctx, Raft{RO: t.eng.LogEngine(), WO: b}, stateReader, nextIndex,
		)
		if err == nil && truncated {
			err = b.Commit(false /* sync */)
		}
		b.Close()
		if err != nil {
			return lastTruncated, err
		}
		if !truncated {
			return lastTruncated, nil
		}
		if nextIndex > 0 {
			lastTruncated = nextIndex
			nextIndex++
		}
	}
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
			if err := t.clearReplicaRaftLogAndSideloaded(ctx, raft, event.Addr.RangeID, event.Addr.Index); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	return false, iter.Error()
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
