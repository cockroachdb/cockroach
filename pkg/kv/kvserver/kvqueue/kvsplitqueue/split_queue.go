// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvsplitqueue

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvqueue"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storemetrics"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// splitQueueTimerDuration is the duration between splits of queued ranges.
	splitQueueTimerDuration = 0 // zero duration to process splits greedily.

	// splitQueuePurgatoryCheckInterval is the interval at which replicas in
	// purgatory make split attempts. Purgatory is used by the SplitQueue to
	// store ranges that are large enough to require a split but are
	// unsplittable because they do not contain a suitable split key. Purgatory
	// prevents them from repeatedly attempting to split at an unbounded rate.
	splitQueuePurgatoryCheckInterval = 1 * time.Minute

	// splits should be relatively isolated, other than requiring expensive
	// RocksDB scans over part of the splitting range to recompute stats. We
	// allow a limitted number of splits to be processed at once.
	splitQueueConcurrency = 4
)

// SplitQueue manages a queue of ranges slated to be split due to size
// or along intersecting zone config boundaries.
type SplitQueue struct {
	*kvqueue.BaseQueue
	db       *kv.DB
	purgChan <-chan time.Time

	// loadBasedCount counts the load-based splits performed by the queue.
	loadBasedCount telemetry.Counter
}

// NewSplitQueue returns a new instance of SplitQueue.
func NewSplitQueue(
	store kvqueue.Store, db *kv.DB, metrics *storemetrics.StoreMetrics, knobs *kvqueue.TestingKnobs,
) *SplitQueue {
	var purgChan <-chan time.Time
	if knobs != nil {
		knobs = &kvqueue.TestingKnobs{}
	}
	if c := knobs.SplitQueuePurgatoryChan; c != nil {
		purgChan = c
	} else {
		purgTicker := time.NewTicker(splitQueuePurgatoryCheckInterval)
		purgChan = purgTicker.C
	}

	sq := &SplitQueue{
		db:             db,
		purgChan:       purgChan,
		loadBasedCount: telemetry.GetCounter("kv.split.load"),
	}
	sq.BaseQueue = kvqueue.NewBaseQueue(
		"split", sq, store,
		kvqueue.Config{
			MaxSize:              kvqueue.DefaultQueueMaxSize,
			MaxConcurrency:       splitQueueConcurrency,
			NeedsLease:           true,
			NeedsSystemConfig:    true,
			AcceptsUnsplitRanges: true,
			Successes:            metrics.SplitQueueSuccesses,
			Failures:             metrics.SplitQueueFailures,
			Pending:              metrics.SplitQueuePending,
			ProcessingNanos:      metrics.SplitQueueProcessingNanos,
			Purgatory:            metrics.SplitQueuePurgatory,
		}, knobs,
	)
	return sq
}

func ShouldSplitRange(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	ms enginepb.MVCCStats,
	maxBytes int64,
	shouldBackpressureWrites bool,
	confReader spanconfig.StoreReader,
) (shouldQ bool, priority float64) {
	if confReader.NeedsSplit(ctx, desc.StartKey, desc.EndKey) {
		// Set priority to 1 in the event the range is split by zone configs.
		priority = 1
		shouldQ = true
	}

	// Add priority based on the size of range compared to the max
	// size for the zone it's in.
	if ratio := float64(ms.Total()) / float64(maxBytes); ratio > 1 {
		priority += ratio
		shouldQ = true
	}

	// additionalPriorityDueToBackpressure is a mechanism to prioritize splitting
	// ranges which will actively backpressure writes.
	//
	// NB: This additional weight is totally arbitrary. The priority in the split
	// queue is usually 1 plus the ratio of the current size over the max size.
	// When a range is much larger than it is allowed to be given the
	// backpressureRangeSizeMultiplier and the zone config, backpressure is
	// not going to be applied because of the backpressureByteTolerance (see the
	// comment there for more details). However, when the range size is close to
	// the limit, we will backpressure. We strongly prefer to split over
	// backpressure.
	const additionalPriorityDueToBackpressure = 50
	if shouldQ && shouldBackpressureWrites {
		priority += additionalPriorityDueToBackpressure
	}

	return shouldQ, priority
}

// ShouldQueue determines whether a range should be queued for
// splitting. This is true if the range is intersected by a zone config
// prefix or if the range's size in bytes exceeds the limit for the zone,
// or if the range has too much load on it.
func (sq *SplitQueue) ShouldQueue(
	ctx context.Context,
	now hlc.ClockTimestamp,
	repl kvqueue.Replica,
	confReader spanconfig.StoreReader,
) (shouldQ bool, priority float64) {
	shouldQ, priority = ShouldSplitRange(ctx, repl.Desc(), repl.GetMVCCStats(),
		repl.GetMaxBytes(), repl.ShouldBackpressureWrites(), confReader)

	if !shouldQ && repl.SplitByLoadEnabled() {
		if splitKey := repl.LoadBasedSplitter().MaybeSplitKey(timeutil.Now()); splitKey != nil {
			shouldQ, priority = true, 1.0 // default priority
		}
	}

	return shouldQ, priority
}

// UnsplittableRangeError indicates that a split attempt failed because a no
// suitable split key could be found.
type UnsplittableRangeError struct{}

func (UnsplittableRangeError) Error() string         { return "could not find valid split key" }
func (UnsplittableRangeError) PurgatoryErrorMarker() {}

var _ kvqueue.PurgatoryError = UnsplittableRangeError{}

// Process synchronously invokes admin split for each proposed split key.
func (sq *SplitQueue) Process(
	ctx context.Context, r kvqueue.Replica, confReader spanconfig.StoreReader,
) (processed bool, err error) {
	processed, err = sq.processAttempt(ctx, r, confReader)
	if errors.HasType(err, (*roachpb.ConditionFailedError)(nil)) {
		// ConditionFailedErrors are an expected outcome for range split
		// attempts because splits can race with other descriptor modifications.
		// On seeing a ConditionFailedError, don't return an error and enqueue
		// this replica again in case it still needs to be split.
		log.Infof(ctx, "split saw concurrent descriptor modification; maybe retrying")
		sq.MaybeAddAsync(ctx, r, sq.Clock.NowAsClockTimestamp())
		return false, nil
	}

	return processed, err
}

func (sq *SplitQueue) processAttempt(
	ctx context.Context, r kvqueue.Replica, confReader spanconfig.StoreReader,
) (processed bool, err error) {
	desc := r.Desc()
	// First handle the case of splitting due to span config maps.
	if splitKey := confReader.ComputeSplitKey(ctx, desc.StartKey, desc.EndKey); splitKey != nil {
		if _, err := r.AdminSplitWithDescriptor(
			ctx,
			roachpb.AdminSplitRequest{
				RequestHeader: roachpb.RequestHeader{
					Key: splitKey.AsRawKey(),
				},
				SplitKey:       splitKey.AsRawKey(),
				ExpirationTime: hlc.Timestamp{},
			},
			desc,
			false, /* delayable */
			"span config",
		); err != nil {
			return false, errors.Wrapf(err, "unable to split %s at key %q", r, splitKey)
		}
		return true, nil
	}

	// Next handle case of splitting due to size. Note that we don't perform
	// size-based splitting if maxBytes is 0 (happens in certain test
	// situations).
	size := r.GetMVCCStats().Total()
	maxBytes := r.GetMaxBytes()
	if maxBytes > 0 && float64(size)/float64(maxBytes) > 1 {
		_, err := r.AdminSplitWithDescriptor(
			ctx,
			roachpb.AdminSplitRequest{},
			desc,
			false, /* delayable */
			fmt.Sprintf("%s above threshold size %s", humanizeutil.IBytes(size), humanizeutil.IBytes(maxBytes)),
		)

		return err == nil, err
	}

	now := timeutil.Now()
	if splitByLoadKey := r.LoadBasedSplitter().MaybeSplitKey(now); splitByLoadKey != nil {
		batchHandledQPS, _ := r.QueriesPerSecond()
		raftAppliedQPS := r.WritesPerSecond()
		splitQPS := r.LoadBasedSplitter().LastQPS(now)
		reason := fmt.Sprintf(
			"load at key %s (%.2f splitQPS, %.2f batches/sec, %.2f raft mutations/sec)",
			splitByLoadKey,
			splitQPS,
			batchHandledQPS,
			raftAppliedQPS,
		)
		// Add a small delay (default of 5m) to any subsequent attempt to merge
		// this range split away. While the merge queue does takes into account
		// load to avoids merging ranges that would be immediately re-split due
		// to load-based splitting, it did not used to take into account historical
		// load. This has since been fixed by #64201, but we keep this small manual
		// delay for compatibility reasons.
		// TODO(nvanbenschoten): remove this entirely in v22.1 when it is no longer
		// needed.
		var expTime hlc.Timestamp
		if expDelay := kvserverbase.SplitByLoadMergeDelay.Get(&sq.Settings.SV); expDelay > 0 {
			expTime = sq.Clock.Now().Add(expDelay.Nanoseconds(), 0)
		}
		if _, pErr := r.AdminSplitWithDescriptor(
			ctx,
			roachpb.AdminSplitRequest{
				RequestHeader: roachpb.RequestHeader{
					Key: splitByLoadKey,
				},
				SplitKey:       splitByLoadKey,
				ExpirationTime: expTime,
			},
			desc,
			false, /* delayable */
			reason,
		); pErr != nil {
			return false, errors.Wrapf(pErr, "unable to split %s at key %q", r, splitByLoadKey)
		}

		telemetry.Inc(sq.loadBasedCount)

		// Reset the splitter now that the bounds of the range changed.
		r.LoadBasedSplitter().Reset(sq.Clock.PhysicalTime())
		return true, nil
	}
	return false, nil
}

// Timer returns interval between processing successive queued splits.
func (*SplitQueue) Timer(_ time.Duration) time.Duration {
	return splitQueueTimerDuration
}

// PurgatoryChan returns the split queue's purgatory channel.
func (sq *SplitQueue) PurgatoryChan() <-chan time.Time {
	return sq.purgChan
}
