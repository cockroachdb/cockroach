// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package compactor

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

type storeCapacityFunc func() (roachpb.StoreCapacity, error)

type doneCompactingFunc func(ctx context.Context)

// A Compactor records suggested compactions and periodically
// makes requests to the engine to reclaim storage space.
type Compactor struct {
	st      *cluster.Settings
	eng     storage.Engine
	capFn   storeCapacityFunc
	doneFn  doneCompactingFunc
	ch      chan struct{}
	Metrics Metrics
}

// NewCompactor returns a compactor for the specified storage engine.
func NewCompactor(
	st *cluster.Settings, eng storage.Engine, capFn storeCapacityFunc, doneFn doneCompactingFunc,
) *Compactor {
	return &Compactor{
		st:      st,
		eng:     eng,
		capFn:   capFn,
		doneFn:  doneFn,
		ch:      make(chan struct{}, 1),
		Metrics: makeMetrics(),
	}
}

func (c *Compactor) enabled() bool {
	return enabled.Get(&c.st.SV)
}

func (c *Compactor) minInterval() time.Duration {
	return minInterval.Get(&c.st.SV)
}

func (c *Compactor) thresholdBytes() int64 {
	return thresholdBytes.Get(&c.st.SV)
}

func (c *Compactor) thresholdBytesUsedFraction() float64 {
	return thresholdBytesUsedFraction.Get(&c.st.SV)
}

func (c *Compactor) thresholdBytesAvailableFraction() float64 {
	return thresholdBytesAvailableFraction.Get(&c.st.SV)
}

func (c *Compactor) maxAge() time.Duration {
	return maxSuggestedCompactionRecordAge.Get(&c.st.SV)
}

// poke instructs the compactor's main loop to react to new suggestions in a
// timely manner.
func (c *Compactor) poke() {
	select {
	case c.ch <- struct{}{}:
	default:
	}
}

// Start launches a compaction processing goroutine and exits when the
// provided stopper indicates. Processing is done with a periodicity of
// compactionMinInterval, but only if there are compactions pending.
func (c *Compactor) Start(ctx context.Context, stopper *stop.Stopper) {
	ctx = logtags.AddTag(ctx, "compactor", "")

	// Wake up immediately to examine the queue and set the bytes queued metric.
	// Note that the compactor may have received suggestions before having been
	// started (this isn't great, but it's how it is right now).
	c.poke()

	// Run the Worker in a Task because the worker holds on to the engine and
	// may still access it even though the stopper has allowed it to close.
	_ = stopper.RunTask(ctx, "compactor", func(ctx context.Context) {
		stopper.RunWorker(ctx, func(ctx context.Context) {
			var timer timeutil.Timer
			defer timer.Stop()

			// The above timer will either be on c.minInterval() or c.maxAge(). The
			// former applies if we know there are new suggestions waiting to be
			// inspected: we want to look at them soon, but also want to make sure
			// "related" suggestions arrive before we start compacting. When no new
			// suggestions have been made since the last inspection, the expectation
			// is that all we have to do is clean up any previously skipped ones (at
			// least after sufficient time has passed), and so we wait out the max age.
			var isFast bool

			for {
				select {
				case <-stopper.ShouldStop():
					return

				case <-c.ch:
					// A new suggestion was made. Examine the compaction queue,
					// which returns the number of bytes queued.
					if bytesQueued, err := c.examineQueue(ctx); err != nil {
						log.Warningf(ctx, "failed check whether compaction suggestions exist: %+v", err)
					} else if bytesQueued > 0 {
						log.VEventf(ctx, 3, "compactor starting in %s as there are suggested compactions pending", c.minInterval())
					} else {
						// Queue is empty, don't set the timer. This can happen only at startup.
						break
					}
					// Set the wait timer if not already set.
					if !isFast {
						isFast = true
						timer.Reset(c.minInterval())
					}

				case <-timer.C:
					timer.Read = true
					ok, err := c.processSuggestions(ctx)
					if err != nil {
						log.Warningf(ctx, "failed processing suggested compactions: %+v", err)
					}
					if ok {
						// The queue was processed, so either it's empty or contains suggestions
						// that were skipped for now. Revisit when they are certainly expired.
						isFast = false
						timer.Reset(c.maxAge())
						break
					}
					// More work to do, revisit after minInterval. Note that basically
					// `ok == (err == nil)` but this refactor is left for a future commit.
					isFast = true
					timer.Reset(c.minInterval())
				}
			}
		})
	})
}

// aggregatedCompaction is a utility struct that holds information
// about aggregated suggested compactions.
type aggregatedCompaction struct {
	kvserverpb.SuggestedCompaction
	suggestions []kvserverpb.SuggestedCompaction
	startIdx    int
	total       int
}

func initAggregatedCompaction(
	startIdx, total int, sc kvserverpb.SuggestedCompaction,
) aggregatedCompaction {
	return aggregatedCompaction{
		SuggestedCompaction: sc,
		suggestions:         []kvserverpb.SuggestedCompaction{sc},
		startIdx:            startIdx,
		total:               total,
	}
}

func (aggr aggregatedCompaction) String() string {
	var seqFmt string
	if len(aggr.suggestions) == 1 {
		seqFmt = fmt.Sprintf("#%d/%d", aggr.startIdx+1, aggr.total)
	} else {
		seqFmt = fmt.Sprintf("#%d-%d/%d", aggr.startIdx+1, aggr.startIdx+len(aggr.suggestions), aggr.total)
	}
	return fmt.Sprintf("%s (%s-%s) for %s", seqFmt, aggr.StartKey, aggr.EndKey, humanizeutil.IBytes(aggr.Bytes))
}

// processSuggestions considers all suggested compactions and
// processes contiguous or nearly contiguous aggregations if they
// exceed the absolute or fractional size thresholds. If suggested
// compactions don't meet thresholds, they're discarded if they're
// older than maxSuggestedCompactionRecordAge. Returns a boolean
// indicating whether the queue was successfully processed.
func (c *Compactor) processSuggestions(ctx context.Context) (bool, error) {
	ctx, cleanup := tracing.EnsureContext(ctx, c.st.Tracer, "process suggested compactions")
	defer cleanup()

	suggestions, totalBytes, err := c.fetchSuggestions(ctx)
	if err != nil {
		return false, err
	}

	// Update at start of processing. Note that totalBytes is decremented and
	// updated after any compactions which are processed.
	c.Metrics.BytesQueued.Update(totalBytes)

	if len(suggestions) == 0 {
		return false, nil
	}

	log.Eventf(ctx, "considering %d suggested compaction(s)", len(suggestions))

	// Determine whether to attempt a compaction to reclaim space during
	// this processing. The decision is based on total bytes to free up
	// and the time since the last processing.
	capacity, err := c.capFn()
	if err != nil {
		return false, err
	}

	// Get information about SSTables in the underlying RocksDB instance.
	ssti := storage.NewSSTableInfosByLevel(c.eng.GetSSTables())

	// Update the bytes queued metric based, periodically querying the persisted
	// suggestions so that we pick up newly added suggestions in the case where
	// we're processing a large number of suggestions.
	lastUpdate := timeutil.Now()
	updateBytesQueued := func(delta int64) error {
		totalBytes -= delta
		if timeutil.Since(lastUpdate) >= 10*time.Second {
			lastUpdate = timeutil.Now()
			bytes, err := c.examineQueue(ctx)
			if err != nil {
				return err
			}
			totalBytes = bytes
			// NB: examineQueue updates the BytesQueued metric.
		} else {
			c.Metrics.BytesQueued.Update(totalBytes)
		}
		return nil
	}

	// Iterate through suggestions, merging them into a running
	// aggregation. Aggregates which exceed size thresholds are compacted. Small,
	// isolated suggestions will be ignored until becoming too old, at which
	// point they are discarded without compaction.
	aggr := initAggregatedCompaction(0, len(suggestions), suggestions[0])
	for i, sc := range suggestions[1:] {
		// Aggregate current suggestion with running aggregate if possible. If
		// the current suggestion cannot be merged with the aggregate, process
		// it if it meets compaction thresholds.
		if done := c.aggregateCompaction(ctx, ssti, &aggr, sc); done {
			processedBytes, err := c.processCompaction(ctx, aggr, capacity)
			if err != nil {
				log.Errorf(ctx, "failed processing suggested compactions %+v: %+v", aggr, err)
			} else if err := updateBytesQueued(processedBytes); err != nil {
				log.Errorf(ctx, "failed updating bytes queued metric %+v", err)
			}
			// Reset aggregation to the last, un-aggregated, suggested compaction.
			aggr = initAggregatedCompaction(i, len(suggestions), sc)
		}
	}
	// Process remaining aggregated compaction.
	processedBytes, err := c.processCompaction(ctx, aggr, capacity)
	if err != nil {
		return false, err
	}
	if err := updateBytesQueued(processedBytes); err != nil {
		log.Errorf(ctx, "failed updating bytes queued metric %+v", err)
	}

	return true, nil
}

// fetchSuggestions loads the persisted suggested compactions from the store.
func (c *Compactor) fetchSuggestions(
	ctx context.Context,
) (suggestions []kvserverpb.SuggestedCompaction, totalBytes int64, err error) {
	dataIter := c.eng.NewIterator(storage.IterOptions{
		UpperBound: roachpb.KeyMax, // refined before every seek
	})
	defer dataIter.Close()

	delBatch := c.eng.NewBatch()
	defer delBatch.Close()

	err = c.eng.Iterate(
		keys.LocalStoreSuggestedCompactionsMin,
		keys.LocalStoreSuggestedCompactionsMax,
		func(kv storage.MVCCKeyValue) (bool, error) {
			var sc kvserverpb.SuggestedCompaction
			var err error
			sc.StartKey, sc.EndKey, err = keys.DecodeStoreSuggestedCompactionKey(kv.Key.Key)
			if err != nil {
				return false, errors.Wrapf(err, "failed to decode suggested compaction key")
			}
			if err := protoutil.Unmarshal(kv.Value, &sc.Compaction); err != nil {
				return false, err
			}

			dataIter.SetUpperBound(sc.EndKey)
			dataIter.SeekGE(storage.MakeMVCCMetadataKey(sc.StartKey))
			if ok, err := dataIter.Valid(); err != nil {
				return false, err
			} else if ok && dataIter.UnsafeKey().Less(storage.MakeMVCCMetadataKey(sc.EndKey)) {
				// The suggested compaction span has live keys remaining. This is a
				// strong indicator that compacting this range will be significantly
				// more expensive than we expected when the compaction was suggested, as
				// compactions are only suggested when a ClearRange request has removed
				// all the keys in the span. Perhaps a replica was rebalanced away then
				// back?
				//
				// Since we can't guarantee that this compaction will be an easy win,
				// purge it to avoid bogging down the compaction queue.
				log.Infof(ctx, "purging suggested compaction for range %s - %s that contains live data",
					sc.StartKey, sc.EndKey)
				if err := delBatch.Clear(kv.Key); err != nil {
					log.Fatalf(ctx, "%v", err) // should never happen on a batch
				}
				c.Metrics.BytesSkipped.Inc(sc.Bytes)
			} else {
				suggestions = append(suggestions, sc)
				totalBytes += sc.Bytes
			}

			return false, nil // continue iteration
		},
	)
	if err != nil {
		return nil, 0, err
	}
	if err := delBatch.Commit(true); err != nil {
		log.Warningf(ctx, "unable to delete suggested compaction records: %+v", err)
	}
	return suggestions, totalBytes, nil
}

// processCompaction sends CompactRange requests to the storage engine if the
// aggregated suggestion exceeds size threshold(s). Otherwise, it either skips
// the compaction or skips the compaction *and* deletes the suggested compaction
// records if they're too old (and in particular, if the compactor is disabled,
// deletes any suggestions handed to it). Returns the number of bytes processed
// (either compacted or skipped and deleted due to age).
func (c *Compactor) processCompaction(
	ctx context.Context, aggr aggregatedCompaction, capacity roachpb.StoreCapacity,
) (int64, error) {
	aboveSizeThresh := aggr.Bytes >= c.thresholdBytes()
	aboveUsedFracThresh := func() bool {
		thresh := c.thresholdBytesUsedFraction()
		return thresh > 0 && aggr.Bytes >= int64(float64(capacity.LogicalBytes)*thresh)
	}()
	aboveAvailFracThresh := func() bool {
		thresh := c.thresholdBytesAvailableFraction()
		return thresh > 0 && aggr.Bytes >= int64(float64(capacity.Available)*thresh)
	}()

	shouldProcess := c.enabled() && (aboveSizeThresh || aboveUsedFracThresh || aboveAvailFracThresh)
	if shouldProcess {
		startTime := timeutil.Now()
		log.Infof(ctx,
			"processing compaction %s (reasons: size=%t used=%t avail=%t)",
			aggr, aboveSizeThresh, aboveUsedFracThresh, aboveAvailFracThresh,
		)

		if err := c.eng.CompactRange(aggr.StartKey, aggr.EndKey, false /* forceBottommost */); err != nil {
			c.Metrics.CompactionFailures.Inc(1)
			return 0, errors.Wrapf(err, "unable to compact range %+v", aggr)
		}
		c.Metrics.BytesCompacted.Inc(aggr.Bytes)
		c.Metrics.CompactionSuccesses.Inc(1)
		duration := timeutil.Since(startTime)
		c.Metrics.CompactingNanos.Inc(int64(duration))
		if c.doneFn != nil {
			c.doneFn(ctx)
		}
		log.Infof(ctx, "processed compaction %s in %.1fs", aggr, duration.Seconds())
	} else {
		log.VEventf(ctx, 2, "skipping compaction(s) %s", aggr)
	}

	delBatch := c.eng.NewWriteOnlyBatch()

	// Delete suggested compaction records if appropriate.
	for _, sc := range aggr.suggestions {
		age := timeutil.Since(timeutil.Unix(0, sc.SuggestedAtNanos))
		tooOld := age >= c.maxAge() || !c.enabled()
		// Delete unless we didn't process and the record isn't too old.
		if !shouldProcess && !tooOld {
			continue
		}
		if tooOld {
			c.Metrics.BytesSkipped.Inc(sc.Bytes)
		}
		key := keys.StoreSuggestedCompactionKey(sc.StartKey, sc.EndKey)
		if err := delBatch.Clear(storage.MVCCKey{Key: key}); err != nil {
			log.Fatalf(ctx, "%v", err) // should never happen on a batch
		}
	}

	if err := delBatch.Commit(true); err != nil {
		log.Warningf(ctx, "unable to delete suggested compaction records: %+v", err)
	}
	delBatch.Close()

	if shouldProcess {
		return aggr.Bytes, nil
	}
	return 0, nil
}

// aggregateCompaction merges sc into aggr, to create a new suggested
// compaction, if the key spans are overlapping or near-contiguous.  Note that
// because suggested compactions are stored sorted by their start key,
// sc.StartKey >= aggr.StartKey. Returns true if we couldn't add the new
// suggested compaction to the aggregation and are therefore done building the
// current aggregation and should process it. Returns false if we should
// continue aggregating suggested compactions.
func (c *Compactor) aggregateCompaction(
	ctx context.Context,
	ssti storage.SSTableInfosByLevel,
	aggr *aggregatedCompaction,
	sc kvserverpb.SuggestedCompaction,
) (done bool) {
	// Don't bother aggregating more once we reach threshold bytes.
	if aggr.Bytes >= c.thresholdBytes() {
		return true // suggested compation could not be aggregated
	}

	// If the key spans don't overlap, then check whether they're
	// "nearly" contiguous.
	if aggr.EndKey.Compare(sc.StartKey) < 0 {
		// Aggregate if the gap between current aggregate and proposed
		// compaction span overlaps (at most) two contiguous SSTables at
		// the bottommost level.
		span := roachpb.Span{Key: aggr.EndKey, EndKey: sc.StartKey}
		maxLevel := ssti.MaxLevelSpanOverlapsContiguousSSTables(span)
		if maxLevel < ssti.MaxLevel() {
			return true // suggested compaction could not be aggregated
		}
	}

	// We can aggregate, so merge sc into aggr.
	if aggr.EndKey.Compare(sc.EndKey) < 0 {
		aggr.EndKey = sc.EndKey
	}
	aggr.Bytes += sc.Bytes
	aggr.suggestions = append(aggr.suggestions, sc)
	return false // aggregated successfully
}

// examineQueue returns the total number of bytes queued and updates the
// BytesQueued gauge.
func (c *Compactor) examineQueue(ctx context.Context) (int64, error) {
	var totalBytes int64
	if err := c.eng.Iterate(
		keys.LocalStoreSuggestedCompactionsMin,
		keys.LocalStoreSuggestedCompactionsMax,
		func(kv storage.MVCCKeyValue) (bool, error) {
			var c kvserverpb.Compaction
			if err := protoutil.Unmarshal(kv.Value, &c); err != nil {
				return false, err
			}
			totalBytes += c.Bytes
			return false, nil // continue iteration
		},
	); err != nil {
		return 0, err
	}
	c.Metrics.BytesQueued.Update(totalBytes)
	return totalBytes, nil
}

// Suggest writes the specified compaction to persistent storage and
// pings the processing goroutine.
func (c *Compactor) Suggest(ctx context.Context, sc kvserverpb.SuggestedCompaction) {
	log.VEventf(ctx, 2, "suggested compaction from %s - %s: %+v", sc.StartKey, sc.EndKey, sc.Compaction)

	// Check whether a suggested compaction already exists for this key span.
	key := keys.StoreSuggestedCompactionKey(sc.StartKey, sc.EndKey)
	var existing kvserverpb.Compaction
	//lint:ignore SA1019 historical usage of deprecated c.eng.GetProto is OK
	ok, _, _, err := c.eng.GetProto(storage.MVCCKey{Key: key}, &existing)
	if err != nil {
		log.VErrEventf(ctx, 2, "unable to record suggested compaction: %s", err)
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
	//lint:ignore SA1019 historical usage of deprecated engine.PutProto is OK
	if _, _, err = storage.PutProto(c.eng, storage.MVCCKey{Key: key}, &sc.Compaction); err != nil {
		log.Warningf(ctx, "unable to record suggested compaction: %+v", err)
	}

	// Poke the compactor goroutine to reconsider compaction in light of
	// this new suggested compaction.
	c.poke()
}
