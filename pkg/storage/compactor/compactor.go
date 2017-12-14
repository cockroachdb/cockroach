// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package compactor

import (
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

const (
	// defaultCompactionMinInterval indicates the minimium period of
	// time to wait before any compaction activity is considered, after
	// suggestions are made. The intent is to allow sufficient time for
	// all ranges to be cleared when a big table is dropped, so the
	// compactor can determine contiguous stretches and efficient delete
	// sstable files.
	defaultCompactionMinInterval = 2 * time.Minute

	// defaultThresholdBytes is the threshold in bytes of suggested
	// reclamation, after which the compactor will begin processing
	// (taking compactor min interval into account).
	defaultThresholdBytes = 128 << 20 // more than 128MiB will trigger

	// defaultThresholdBytesFraction is the fraction of total logical bytes
	// used which are up for suggested reclamation, after which the
	// compactor will begin processing (taking compactor min interval into account).
	defaultThresholdBytesFraction = 0.10 // more than 10% of space will trigger

	// defaultMaxGapBytesThreshold is the maximum estimated gap in bytes
	// between two suggested compaction spans, before which two consecutive
	// suggested compactions will be merged and compacted together.
	defaultMaxGapBytesThreshold = 64 << 20 // will aggregate if gap <= 64MiB

	// defaultMaxSuggestedCompactionRecordAge is the maximum age of a
	// suggested compaction record. If not processed within this time
	// interval since the compaction was suggested, it will be deleted.
	defaultMaxSuggestedCompactionRecordAge = 24 * time.Hour
)

// compactorOptions specify knobs to tune for compactor behavior.
// These are intended for testing.
type compactorOptions struct {
	CompactionMinInterval           time.Duration
	ThresholdBytes                  int64
	ThresholdBytesFraction          float64
	MaxGapBytesThreshold            uint64
	MaxSuggestedCompactionRecordAge time.Duration
}

var defaultCompactorOptions = compactorOptions{
	CompactionMinInterval:           defaultCompactionMinInterval,
	ThresholdBytes:                  defaultThresholdBytes,
	ThresholdBytesFraction:          defaultThresholdBytesFraction,
	MaxGapBytesThreshold:            defaultMaxGapBytesThreshold,
	MaxSuggestedCompactionRecordAge: defaultMaxSuggestedCompactionRecordAge,
}

// A Compactor records suggested compactions and periodically
// makes requests to the engine to reclaim storage space.
type Compactor struct {
	eng     engine.Engine
	ch      chan struct{}
	opts    compactorOptions
	Metrics Metrics
}

// NewCompactor returns a compactor for the specified storage engine.
func NewCompactor(eng engine.Engine) *Compactor {
	return &Compactor{
		eng:     eng,
		ch:      make(chan struct{}, 1),
		opts:    defaultCompactorOptions,
		Metrics: makeMetrics(),
	}
}

// Start launches a compaction processing goroutine and exits when the
// provided stopper indicates. Processing is done with a periodicity of
// compactionMinInterval, but only if there are compactions pending.
func (c *Compactor) Start(ctx context.Context, tracer opentracing.Tracer, stopper *stop.Stopper) {
	if empty, err := c.isSpanEmpty(
		ctx, keys.LocalStoreSuggestedCompactionsMin, keys.LocalStoreSuggestedCompactionsMax,
	); err != nil {
		log.Warningf(ctx, "failed check whether compaction suggestions exist: %s", err)
	} else if !empty {
		c.ch <- struct{}{} // wake up the goroutine immediately
	}

	stopper.RunWorker(ctx, func(ctx context.Context) {
		var timer timeutil.Timer
		var timerSet bool
		for {
			select {
			case <-c.ch:
				// Set the wait timer if not already set.
				if !timerSet {
					timer.Reset(c.opts.CompactionMinInterval)
					timerSet = true
				}

			case <-timer.C:
				timer.Read = true
				timer.Stop()
				ctx, cleanup := tracing.EnsureContext(ctx, tracer, "process suggested compactions")
				ok, err := c.processSuggestions(ctx)
				cleanup()
				if err != nil {
					log.Warningf(ctx, "failed processing suggested compactions: %s", err)
				} else if ok {
					// Everything has been processed. Wait for the next
					// suggested compaction before resetting the timer.
					timerSet = false
					break
				}
				// Reset the timer to re-attempt processing after the minimum
				// compaction interval.
				timer.Reset(c.opts.CompactionMinInterval)
				timerSet = true

			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// processSuggestions considers all suggested compactions and
// processes contiguous or nearly contiguous aggregations if they
// exceed the absolute or fractional size thresholds. If suggested
// compactions don't meet thresholds, they're discarded if they're
// older than maxSuggestedCompactionRecordAge. Returns a boolean
// indicating whether the processing occurred.
func (c *Compactor) processSuggestions(ctx context.Context) (bool, error) {
	// Collect all suggestions.
	var suggestions []storagebase.SuggestedCompaction
	var totalBytes int64
	if err := c.eng.Iterate(
		engine.MVCCKey{Key: keys.LocalStoreSuggestedCompactionsMin},
		engine.MVCCKey{Key: keys.LocalStoreSuggestedCompactionsMax},
		func(kv engine.MVCCKeyValue) (bool, error) {
			var sc storagebase.SuggestedCompaction
			var err error
			if sc.StartKey, sc.EndKey, err = keys.DecodeStoreSuggestedCompactionKey(kv.Key.Key); err != nil {
				return false, errors.Wrapf(err, "failed to decode suggested compaction key")
			}
			if err := protoutil.Unmarshal(kv.Value, &sc.Compaction); err != nil {
				return false, err
			}
			suggestions = append(suggestions, sc)
			totalBytes += sc.Compaction.Bytes
			return true, nil // continue iteration
		},
	); err != nil {
		return false, err
	}
	defer func() {
		c.Metrics.BytesQueued.Update(totalBytes)
	}()

	if len(suggestions) == 0 {
		return false, nil
	}

	// Determine whether to attempt a compaction to reclaim space during
	// this processing. The decision is based on total bytes to free up
	// and the time since the last processing.
	capacity, err := c.eng.Capacity()
	if err != nil {
		return false, err
	}

	// Iterate through suggestions, merging them into a running
	// aggregation. Aggregates which exceed size thresholds are
	// compacted. Small, isolated suggestions will be ignored until
	// becoming too old, at which point they are discarded without
	// compaction.
	delBatch := c.eng.NewWriteOnlyBatch()
	defer func() {
		if err := delBatch.Commit(true); err != nil {
			log.Warningf(ctx, "unable to delete suggested compaction records: %s", err)
		}
	}()
	suggestCount := len(suggestions)
	aggr := suggestions[0]
	var startIdx int
	for i, sc := range suggestions[1:] {
		// Aggregate current suggestion with running aggregate if possible. If
		// the current suggestion cannot be merged with the aggregate, process
		// it if it meets compaction thresholds.
		if done := c.aggregateCompaction(ctx, &aggr, sc); done {
			processedBytes, err := c.processCompaction(ctx, aggr, capacity, delBatch, startIdx, i, suggestCount)
			if err != nil {
				return false, err
			}
			totalBytes -= processedBytes
		} else {
			aggr = sc
		}
	}
	// Process remaining aggregated compaction.
	processedBytes, err := c.processCompaction(ctx, aggr, capacity, delBatch, startIdx, suggestCount, suggestCount)
	if err != nil {
		return false, err
	}
	totalBytes -= processedBytes

	return true, nil
}

// processCompaction sends CompactRange requests to the storage engine
// if the aggregated suggestion exceeds size threshold(s). Otherwise,
// it either skips the compaction or skips the compaction *and* deletes
// the suggested compaction records if they're too old. Returns the
// number of bytes processed (either compacted or skipped and deleted
// due to age).
func (c *Compactor) processCompaction(
	ctx context.Context,
	aggr storagebase.SuggestedCompaction,
	capacity roachpb.StoreCapacity,
	delBatch engine.Batch,
	startIdx, curIdx, length int,
) (int64, error) {
	shouldDelete := false
	shouldProcess := aggr.Compaction.Bytes >= c.opts.ThresholdBytes ||
		aggr.Compaction.Bytes >= int64(float64(capacity.LogicalBytes)*c.opts.ThresholdBytesFraction)

	if shouldProcess {
		startTime := timeutil.Now()
		if err := c.eng.CompactRange(
			engine.MVCCKey{Key: roachpb.Key(aggr.StartKey)},
			engine.MVCCKey{Key: roachpb.Key(aggr.EndKey)},
		); err != nil {
			return 0, errors.Wrapf(err, "unable to delete files in suggested compaction %+v", aggr)
		}
		log.Eventf(ctx, "processed suggested compaction #%d/%d (%s-%s) for %s bytes in %s",
			startIdx, curIdx, length, aggr.StartKey, aggr.EndKey, humanizeutil.IBytes(aggr.Compaction.Bytes), timeutil.Since(startTime))
		c.Metrics.BytesCompacted.Inc(aggr.Compaction.Bytes)
		c.Metrics.Compactions.Inc(1)
		// Since we compacted, definitely delete all the suggested
		// compaction records which make up this aggregation.
		shouldDelete = true
	} else {
		// If the newest suggested compaction in the aggregation is
		// older than the maxSuggestedCompactionRecordAge, delete
		// all of the suggested compaction records.
		if timeutil.Since(timeutil.Unix(0, aggr.Compaction.SuggestedAtNanos)) >= c.opts.MaxSuggestedCompactionRecordAge {
			shouldDelete = true
			c.Metrics.BytesSkipped.Inc(aggr.Compaction.Bytes)
		}
		log.VEventf(ctx, 2, "skipping suggested compaction(s) %d-%d/%d (%s-%s) for %s bytes (deleting=%t)",
			startIdx, curIdx, length, aggr.StartKey, aggr.EndKey, humanizeutil.IBytes(aggr.Compaction.Bytes), shouldDelete)
	}
	if !shouldDelete {
		return 0, nil
	}

	// Add this suggested compaction record to the batch.
	clearStart := engine.MVCCKey{Key: keys.StoreSuggestedCompactionKey(aggr.StartKey, nil)}
	clearEnd := engine.MVCCKey{Key: keys.StoreSuggestedCompactionKey(aggr.EndKey, nil)}
	if err := delBatch.ClearRange(clearStart, clearEnd); err != nil {
		log.Fatal(ctx, err) // should never happen on a batch
	}
	return aggr.Compaction.Bytes, nil
}

// aggregateCompaction merges sc into aggr, to create a new suggested
// compaction, if the key spans are overlapping or near-contiguous.
// Note that because suggested compactions are stored sorted by their
// start key, sc.StartKey >= aggr.StartKey. Returns whether the
// compaction was aggregated. If false, the supplied aggregation is
// complete and should be processed.
func (c *Compactor) aggregateCompaction(
	ctx context.Context, aggr *storagebase.SuggestedCompaction, sc storagebase.SuggestedCompaction,
) bool {
	// If the key spans don't overlap, then check whether they're
	// "nearly" contiguous.
	if aggr.EndKey.Less(sc.StartKey) {
		// Ask for an estimate of disk space utilized by the engine in the
		// gap between the existing aggregation and the new suggested compaction.
		gapSize, err := c.eng.ApproximateDiskBytes(roachpb.Key(aggr.EndKey), roachpb.Key(sc.StartKey))
		if err != nil {
			log.Warningf(ctx, "could not determine approximate size in bytes in range %s-%s: %s",
				aggr.EndKey, sc.StartKey, err)
			return true
		}
		// If the estimated gap exceeds the aggregation threshold, skip aggregation.
		if gapSize > c.opts.MaxGapBytesThreshold {
			return true // suggested compaction could not be aggregated
		}
	}

	// We can aggregate, so merge sc into aggr.
	if aggr.EndKey.Less(sc.EndKey) {
		aggr.EndKey = sc.EndKey
	}
	aggr.Compaction.Bytes += sc.Compaction.Bytes
	// Use the newest timestamp.
	if aggr.Compaction.SuggestedAtNanos < sc.Compaction.SuggestedAtNanos {
		aggr.Compaction.SuggestedAtNanos = sc.Compaction.SuggestedAtNanos
	}
	return false // aggregated successfully
}

// isSpanEmpty returns whether the specified key span is empty (true)
// or contains keys (false).
func (c *Compactor) isSpanEmpty(ctx context.Context, start, end roachpb.Key) (bool, error) {
	// If there are any suggested compactions, start the compaction timer.
	var empty = true
	if err := c.eng.Iterate(
		engine.MVCCKey{Key: start},
		engine.MVCCKey{Key: end},
		func(_ engine.MVCCKeyValue) (bool, error) {
			empty = false
			return true, nil // don't continue iteration
		},
	); err != nil {
		return false, err
	}
	return empty, nil
}

// SuggestCompaction writes the specified compaction to persistent
// storage and pings the processing goroutine.
func (c *Compactor) SuggestCompaction(
	ctx context.Context, start, end roachpb.RKey, sc enginepb.Compaction,
) {
	log.VEventf(ctx, 2, "suggested compaction from %s - %s: %+v", start, end, sc)

	// Check whether a suggested compaction already exists for this key span.
	key := keys.StoreSuggestedCompactionKey(start, end)
	var existing enginepb.Compaction
	ok, _, _, err := c.eng.GetProto(engine.MVCCKey{Key: key}, &existing)
	if err != nil {
		log.ErrEventf(ctx, "unable to record suggested compaction: %s", err)
		return
	}

	// If there's already a suggested compaction, merge them. Note that
	// this method is only called after clearing keys from the underlying
	// storage engine. All such actions really do result in successively
	// more bytes being made available for compaction, so there is no
	// double-counting if the same range were cleared twice.
	if ok {
		sc.Bytes += existing.Bytes
	}

	// Store the new compaction.
	if _, _, err = engine.PutProto(c.eng, engine.MVCCKey{Key: key}, &sc); err != nil {
		log.Warningf(ctx, "unable to record suggested compaction: %s", err)
	}

	// Poke the compactor goroutine to reconsider compaction in light of
	// this new suggested compaction.
	select {
	case c.ch <- struct{}{}:
	default:
	}
}
