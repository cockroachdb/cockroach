// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"math"
	"sync/atomic"
	"time"

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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"
)

var wagTruncatorBatchSize = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.wag.truncator_batch_size",
	"number of WAG nodes to delete per write batch during truncation",
	16,
	settings.IntInRange(1, 1024),
)

var wagSuffixRetentionCount = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.wag.retention.nodes",
	"number of trailing WAG nodes to preserve from truncation. (0 disables retention)",
	32,
	settings.NonNegativeInt,
)

var wagRetentionThreshold = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.wag.retention.duration",
	"duration a WAG node can be retained for before it is eligible for truncation. "+
		"It is only meaningful if `kv.wag.retention.nodes` is > 0. "+
		"(0 disables age-based retention threshold)",
	6*time.Hour,
)

// WAGTruncatorTestingKnobs contains testing knobs for the WAGTruncator.
type WAGTruncatorTestingKnobs struct {
	// AfterTruncationCallback is called after each truncation attempt.
	AfterTruncationCallback func()
	// AfterAgeTickCallback is called after each tick of the age-retention timer.
	AfterAgeTickCallback func()
}

// WAGTruncator truncates applied WAG nodes and clears their associated raft
// state (log entries and sideloaded files).
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
	// allowedIndex is the highest WAG index that the truncator is permitted to
	// truncate. It is advanced based on the suffix retention settings. It is safe
	// that it isn't an atomic variable because it is only accessed by the
	// truncation goroutine.
	// INVARIANT: truncIndex ≤ allowedIndex ≤ seq.Load().
	allowedIndex uint64
	// wakeCh is signaled when there are potential WAG nodes to truncate.
	wakeCh chan struct{}
	knobs  WAGTruncatorTestingKnobs
}

// NewWAGTruncator creates a WAGTruncator.
func NewWAGTruncator(st *cluster.Settings, eng Engines, seq *wag.Seq) *WAGTruncator {
	return &WAGTruncator{
		st:        st,
		eng:       eng,
		seq:       seq,
		initIndex: seq.Load(),
		wakeCh:    make(chan struct{}, 1),
	}
}

// Start launches the background goroutine that performs WAG truncation.
//
// TODO(ibrahim): React to runtime changes to kv.wag.retention.duration via
// SetOnChange so users don't have to wait for the next tick.
func (t *WAGTruncator) Start(ctx context.Context, stopper *stop.Stopper) error {
	return stopper.RunAsyncTask(ctx, "wag-truncation", func(ctx context.Context) {
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		// prevTickIndex is the last WAG index observed at the previous tick of
		// the age-retention timer. On each subsequent tick we advance allowedIndex
		// up to this value, ensuring that if nodes were retained for longer than
		// the age threshold, they can get truncated.
		var prevTickIndex uint64
		var ageTimer timeutil.Timer
		defer ageTimer.Stop()
		if d := wagRetentionThreshold.Get(&t.st.SV); d > 0 {
			ageTimer.Reset(d)
		}

		for {
			select {
			case <-t.wakeCh:
				if err := t.truncateAppliedNodes(ctx); err != nil {
					log.KvExec.Errorf(ctx, "truncating WAG: %v", err)
				}
				if cb := t.knobs.AfterTruncationCallback; cb != nil {
					cb()
				}

			case <-ageTimer.C:
				t.allowedIndex = max(t.allowedIndex, prevTickIndex)
				prevTickIndex = t.seq.Load()
				if cb := t.knobs.AfterAgeTickCallback; cb != nil {
					cb()
				}

				// Now that we might have advanced allowedIndex, signal to issue a new
				// truncation attempt.
				select {
				case t.wakeCh <- struct{}{}:
				default:
				}
				if d := wagRetentionThreshold.Get(&t.st.SV); d > 0 {
					ageTimer.Reset(d)
				}
			case <-ctx.Done():
				return
			}
		}
	})
}

// DurabilityAdvancedCallback is invoked whenever the state engine completes a
// flush. It checks whether there could possibly be WAG nodes to truncate by
// comparing truncIndex against seq.Load(). If there are potential
// truncation opportunities, it sends a non-blocking signal to wake the
// background goroutine. It must return quickly and must not call into the
// engine to avoid deadlock (see storage.Engine.RegisterFlushCompletedCallback).
func (t *WAGTruncator) DurabilityAdvancedCallback() {
	// NB: logically, truncIndex > seq isn't possible because the
	// sequencer index is incremented before the corresponding WAG node is
	// written. Physically, the inversion is possible, depending on which
	// variable is loaded first. Allow that, and treat as a no-op.
	if t.truncIndex.Load() >= t.seq.Load() {
		return
	}
	select {
	case t.wakeCh <- struct{}{}:
	default:
	}
}

// truncateAppliedNodes deletes the longest fully applied prefix of the WAG.
// Does so iteratively, splitting its work into batches.
func (t *WAGTruncator) truncateAppliedNodes(ctx context.Context) error {
	stateReader := t.eng.StateEngine().NewReader(storage.GuaranteedDurability)
	defer stateReader.Close()
	for {
		truncated, err := t.truncateBatch(ctx, stateReader)
		if err != nil || !truncated {
			return err
		}
	}
}

// truncateBatch deletes up to a batch-sized prefix of WAG nodes if all of
// their events have been applied to the state engine. For nodes containing
// EventDestroy or EventSubsume events, it also clears the corresponding raft
// log prefix from the engine and the sideloaded entries storage.
//
// Returns a bool indicating whether some WAG nodes were deleted or not.
//
// The caller must provide a stateRO reader with GuaranteedDurability so that
// only state confirmed flushed to persistent storage is visible. This ensures
// we never delete a WAG node whose mutations aren't flushed yet. The caller is
// also responsible for creating and committing/closing the write batch in
// raft.WO.
func (t *WAGTruncator) truncateBatch(ctx context.Context, stateRO StateRO) (bool, error) {
	batchSize := wagTruncatorBatchSize.Get(&t.st.SV)
	var count int64
	var iter wag.Iterator
	truncated := t.truncIndex.Load()
	t.maybeAdvanceAllowedIndex()

	b := t.eng.LogEngine().NewWriteBatch()
	defer b.Close()
	// TODO(ibrahim): Set the Iter upperbound to be allowedIndex+1.
	for index, node := range iter.IterFrom(
		ctx, t.eng.LogEngine(), keys.StoreWAGNodeKey(truncated+1),
	) {
		if index > t.allowedIndex {
			// We've reached a WAG node that retention policy requires us to keep.
			break
		}
		if index != truncated+1 && index > t.initIndex {
			// We cannot ignore gaps for WAG indices > initIndex.
			break
		}
		// TODO(ibrahim): Right now, the canApplyWAGNode function returns a list of
		// raftCatchUpTargets that are not needed for the purposes of truncation,
		// consider refactoring the function to return only the needed info.
		action, err := canApplyWAGNode(ctx, node, stateRO)
		if err != nil {
			return false, err
		}
		if action.apply {
			// If an event needs to be applied, the WAG node cannot be deleted yet.
			break
		}
		if err := wag.Delete(b, index); err != nil {
			return false, err
		}

		// Clean up the raft log prefix of a destroyed/subsumed replica.
		for _, event := range node.Events {
			if event.Type != wagpb.EventDestroy && event.Type != wagpb.EventSubsume {
				continue
			}
			if err = t.clearReplicaRaftLogAndSideloaded(ctx,
				Raft{RO: t.eng.LogEngine(), WO: b}, event.Addr.RangeID, event.Addr.Index,
			); err != nil {
				return false, err
			}
		}

		truncated = index
		count++
		if count >= batchSize {
			break
		}
	}
	if err := iter.Error(); err != nil || count == 0 {
		return false, err
	}
	if err := b.Commit(false /* sync */); err != nil {
		return false, err
	}
	t.truncIndex.Store(truncated)
	return true, nil
}

// maybeAdvanceAllowedIndex tries to advance allowedIndex to reflect the suffix
// retention setting.
func (t *WAGTruncator) maybeAdvanceAllowedIndex() {
	retain := uint64(wagSuffixRetentionCount.Get(&t.st.SV))
	if last := t.seq.Load(); last > retain {
		// The allowedIndex could get advanced by the age-timer, and we want to make
		// sure that we do not regress it.
		t.allowedIndex = max(t.allowedIndex, last-retain)
	}
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
