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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"
)

// wagTruncationBatchSize controls the number of WAG nodes to delete per batch.
var wagTruncationBatchSize = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.wag.truncation_batch_size",
	"number of WAG nodes to delete per write batch during truncation",
	8,
	settings.IntInRange(1, 1024),
)

// WAGTruncatorTestingKnobs contains testing knobs for the WAGTruncator.
type WAGTruncatorTestingKnobs struct {
	// AfterTruncationCallback, if set, is called after each invocation of
	// truncateAppliedNodesLive completes. Can be used to synchronize tests
	// with the background truncation goroutine.
	AfterTruncationCallback func()
}

// WAGTruncator truncates applied WAG nodes and clears their associated raft
// state (log entries and sideloaded files).
type WAGTruncator struct {
	st  *cluster.Settings
	eng Engines
	seq *wag.Seq
	// lastWAGIndexBeforeStartup is the last WAG node index that existed before
	// startup. Truncating WAG nodes with indices <= to this index can ignore
	// gaps.
	lastWAGIndexBeforeStartup uint64
	// lastTruncatedWAGIndex is the index of the last WAG node that was
	// successfully truncated. This is to quickly seek into the potential WAG
	// nodes to truncate.
	lastTruncatedWAGIndex atomic.Uint64
	// wakeCh is signaled when there are potential WAG nodes to truncate.
	wakeCh chan struct{}
	knobs  WAGTruncatorTestingKnobs
}

// NewWAGTruncator creates a WAGTruncator.
func NewWAGTruncator(
	st *cluster.Settings,
	eng Engines,
	seq *wag.Seq,
	lastIndexAfterStartup uint64,
	knobs WAGTruncatorTestingKnobs,
) *WAGTruncator {
	return &WAGTruncator{
		st:                        st,
		eng:                       eng,
		seq:                       seq,
		lastWAGIndexBeforeStartup: lastIndexAfterStartup,
		wakeCh:                    make(chan struct{}, 1),
		knobs:                     knobs,
	}
}

// Start launches the background goroutine that performs WAG truncation.
// TODO(ibrahim): Add a setting for keeping a suffix of the WAG for debugging.
// For example, setting a maximum number of WAG nodes to retain for debugging
// purposes. We could also pair it with some time threshold after which all WAG
// nodes are automatically truncated to maintain a manageable size.
func (t *WAGTruncator) Start(ctx context.Context, stopper *stop.Stopper) error {
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
	lastTruncated, seq := t.lastTruncatedWAGIndex.Load(), t.seq.Load()
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

// truncateAppliedNodesLive is called by the background goroutine to truncate
// applied WAG nodes.
func (t *WAGTruncator) truncateAppliedNodesLive(ctx context.Context) {
	defer func() {
		if t.knobs.AfterTruncationCallback != nil {
			t.knobs.AfterTruncationCallback()
		}
	}()
	startIndex := t.lastTruncatedWAGIndex.Load() + 1
	if startIndex <= t.lastWAGIndexBeforeStartup {
		// There are WAG nodes that we can potentially truncate with
		// ignoreGaps=true. These are the WAG nodes that existed at engine startup,
		// and gaps are expected to be present.
		lastTruncated, err := t.truncateAppliedNodes(ctx, startIndex, t.lastWAGIndexBeforeStartup, true /* ignoreGaps */)
		if err != nil {
			log.KvExec.Errorf(ctx, "truncating WAG node before lastWAGIndexBeforeStartup: %+v", err)
			return
		}
		if lastTruncated < t.lastWAGIndexBeforeStartup {
			// There are still WAG nodes that we can potentially truncate. We will
			// try again on the next state engine flush.
			return
		}
		// At this point, we have truncated all the WAG nodes that existed at engine
		// startup. From now on, we will only truncate WAG nodes created after
		// startup, and we will not ignore gaps.
		startIndex = lastTruncated + 1 // update startIndex for the next step below
	}

	_, err := t.truncateAppliedNodes(ctx, startIndex, math.MaxUint64, false /* ignoreGaps */)
	if err != nil {
		// TODO(ibrahim): We need to decide if we need to retry truncating this
		//  WAG node now, or rely on the next state engine flush to do it.
		log.KvExec.Errorf(ctx, "truncating WAG node: %+v", err)
	}
}

// truncateAppliedNodes is a helper function that repeatedly tries to delete
// WAG nodes in a batch and commit that batch. It iterates from startIndex to
// lastIndex (inclusive) and attempts to delete the WAG nodes it sees. It
// stops when it encounters a WAG node that cannot be deleted yet, when it
// reaches lastIndex, when it encounters an error, or when it encounters a gap
// and ignoreGaps is false.
//
// Returns the index of the last successfully truncated node or 0 when no nodes
// were truncated. It also returns an error if any occurred during truncation.
func (t *WAGTruncator) truncateAppliedNodes(
	ctx context.Context, startIndex uint64, lastIndex uint64, ignoreGaps bool,
) (uint64, error) {
	stateReader := t.eng.StateEngine().NewReader(storage.GuaranteedDurability)
	defer stateReader.Close()
	batchSize := int(wagTruncationBatchSize.Get(&t.st.SV))
	nextIndex := startIndex
	var lastTruncated uint64
	for {
		if nextIndex > lastIndex {
			// We have reached the last index requested, return early.
			return lastTruncated, nil
		}
		b := t.eng.LogEngine().NewWriteBatch()
		batchTruncatedIndex, err := t.truncateBatch(
			ctx, Raft{RO: t.eng.LogEngine(), WO: b}, stateReader,
			nextIndex, lastIndex, batchSize, ignoreGaps,
		)
		if err != nil || batchTruncatedIndex == 0 || batchTruncatedIndex == lastTruncated {
			// If there was an error in the batch or no nodes were truncated, close
			// the batch and return.
			b.Close()
			return lastTruncated, err
		}

		if batchTruncatedIndex > lastTruncated {
			// Try to commit the batch.
			if err := b.Commit(false /* sync */); err != nil {
				b.Close()
				return lastTruncated, err
			}
			b.Close()

			// Prepare for the next batch.
			lastTruncated = batchTruncatedIndex
			nextIndex = lastTruncated + 1
			t.lastTruncatedWAGIndex.Store(lastTruncated)
		} else {
			// We should never have a regression in the last truncated index.
			log.KvExec.Fatalf(ctx, "batchTruncatedIndex %d < lastTruncated %d", batchTruncatedIndex, lastTruncated)
		}
	}
}

// truncateBatch iterates WAG nodes starting from truncateIndex up to
// maxTruncateIndex (inclusive) and deletes up to batchSize nodes whose events
// have all been applied.
//
// It stops when it has deleted batchSize nodes, reaches maxTruncateIndex,
// encounters a node that cannot be deleted yet, hits a gap when ignoreGaps is
// false, or encounters an error.
//
// Returns the index of the last successfully truncated node or 0 when no nodes
// were truncated.
//
// The caller must provide a stateRO reader with GuaranteedDurability so that
// only state confirmed flushed to persistent storage is visible. This ensures
// we never delete a WAG node whose mutations aren't flushed yet. The caller is
// also responsible for creating and committing/closing the write batch in
// raft.WO.
func (t *WAGTruncator) truncateBatch(
	ctx context.Context,
	raft Raft,
	stateRO StateRO,
	truncateIndex uint64,
	maxTruncateIndex uint64,
	batchSize int,
	ignoreGaps bool,
) (uint64, error) {
	var iter wag.Iterator
	iterStartKey := keys.StoreWAGNodeKey(truncateIndex)
	var lastTruncated uint64
	var count int

	for index, node := range iter.IterFrom(ctx, raft.RO, iterStartKey) {
		if err := ctx.Err(); err != nil {
			return lastTruncated, err
		}
		if count >= batchSize || index > maxTruncateIndex || (!ignoreGaps && truncateIndex != index) {
			// Stop iterating if we have reached the batch size, reached the max
			// truncate index, or encountered a gap when we're not ignoring gaps.
			break
		}

		action, err := canApplyWAGNode(ctx, node, stateRO)
		if err != nil {
			return lastTruncated, errors.Wrapf(err, "checking if we can truncate WAG node %d", index)
		}
		if action.apply {
			// The WAG node cannot be deleted yet.
			break
		}
		if err := wag.Delete(raft.WO, index); err != nil {
			return lastTruncated, errors.Wrapf(err, "deleting WAG node %d", index)
		}

		// Clean up the raft log prefix of a destroyed/subsumed replica.
		for _, event := range node.Events {
			if event.Type != wagpb.EventDestroy && event.Type != wagpb.EventSubsume {
				continue
			}
			if err := t.clearReplicaRaftLogAndSideloaded(
				ctx, raft, event.Addr.RangeID, event.Addr.Index,
			); err != nil {
				return lastTruncated, errors.Wrapf(err, "clearing raft state for WAG node %d", index)
			}
		}

		// At this point the batch includes a WAG node deletion and the Raft prefix
		// cleanup. Move on to the next WAG node.
		lastTruncated = index
		truncateIndex = index + 1
		count++
	}
	return lastTruncated, iter.Error()
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
