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

package engine

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	// compactionMinInterval indicates the period of time to wait before
	// any compaction activity is considered, after suggestions are
	// made. The intent is to allow sufficient time for all ranges to be
	// cleared when a big table is dropped, so the compactor can
	// determine contiguous stretches and efficient delete sstable files.
	compactionMinInterval = time.Second * 120 // 2 minutes

	// thresholdBytes is the threshold in bytes of suggested reclamation,
	// after which the compactor will begin processing.
	thresholdBytes = 128 << 20 // more than 128MiB will trigger

	// thresholdBytesFraction is the fraction of total bytes used which
	// are up for suggested reclamation, after which the compactor will
	// begin processing. Note that because data is prefix- and snappy-
	// compressed, we set this threshold lower than might otherwise make
	// sense.
	thresholdBytesFraction = 0.05 // more than 5% of space will trigger

	// thresholdTimeSinceLastProcess is the maximum amount of time since
	// we last processed compactions, after which the compactor will begin
	// processing outstanding suggestions.
	thresholdTimeSinceLastProcess = time.Hour * 2 // more than 2h will trigger
)

// A Compactor records suggested compactions and periodically
// makes requests to the engine to reclaim storage space.
type Compactor struct {
	eng Engine
	ch  chan struct{}
}

// NewCompactor returns a compactor for the specified storage engine.
func NewCompactor(eng Engine) *Compactor {
	return &Compactor{
		eng: eng,
		ch:  make(chan struct{}, 1),
	}
}

// Start launches a compaction processing goroutine and exits when the
// provided stopper indicates. Processing is done with a periodicity of
// compactionMinInterval, but only if there are compactions pending.
func (c *Compactor) Start(ctx context.Context, stopper *stop.Stopper) {
	if empty, err := c.isSpanEmpty(
		ctx, keys.LocalStoreSuggestedCompactionsMin, keys.LocalStoreSuggestedCompactionsMax,
	); err != nil {
		log.ErrEventf(ctx, "failed check whether compaction suggestions exist: %s", err)
	} else if !empty {
		c.ch <- struct{}{} // wake up the goroutine immediately
	}

	stopper.RunWorker(ctx, func(ctx context.Context) {
		lastProcessed := timeutil.Now()
		var timer timeutil.Timer
		var timerSet bool
		for {
			select {
			case <-c.ch:
				// Set the wait timer if not already set.
				log.Infof(ctx, "woken up to process suggestions")
				if !timerSet {
					timer.Reset(compactionMinInterval)
				}

			case <-timer.C:
				timer.Read = true
				log.Infof(ctx, "processing suggestions...")
				ok, err := c.processSuggestions(ctx, lastProcessed)
				if !ok || err != nil {
					log.ErrEventf(ctx, "failed processing suggested compactions: %s", err)
					timer.Reset(compactionMinInterval)
					break
				}
				// No need for the timer; everything has been processed. Wait
				// for the next suggestion before resetting the timer.
				lastProcessed = timeutil.Now()
				timer.Stop()
				timerSet = false

			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// processSuggestions considers all suggested compactions and
// processes them if in aggregate they have sufficient size or it's
// been sufficiently long since the last successful processing.
// Returns a boolean indicating whether the processing occurred.
func (c *Compactor) processSuggestions(
	ctx context.Context, lastProcessed time.Time,
) (bool, error) {
	// Verify we're using RocksDB engine.
	rocksdb, ok := c.eng.(*RocksDB)
	if !ok {
		return false, fmt.Errorf("cannot process suggestions on engine of type %T", c.eng)
	}

	// Collect all suggestions.
	var suggestions []enginepb.Compaction
	var totalBytes int64
	if err := c.eng.Iterate(
		MVCCKey{Key: keys.LocalStoreSuggestedCompactionsMin},
		MVCCKey{Key: keys.LocalStoreSuggestedCompactionsMax},
		func(kv MVCCKeyValue) (bool, error) {
			var sc enginepb.Compaction
			if err := protoutil.Unmarshal(kv.Value, &sc); err != nil {
				return false, err
			}
			suggestions = append(suggestions, sc)
			totalBytes += sc.Bytes
			return true, nil // continue iteration
		},
	); err != nil {
		return false, err
	}

	if len(suggestions) == 0 {
		return false, nil
	}

	// Determine whether to attempt to compaction to reclaim space during
	// this processing. The decision is based on total bytes to free up
	// and the time since the last processing.
	capacity, err := rocksdb.Capacity()
	if err != nil {
		return false, err
	}
	if totalBytes < thresholdBytes &&
		totalBytes < int64(float64(capacity.Used)*thresholdBytesFraction) &&
		timeutil.Since(lastProcessed) < thresholdTimeSinceLastProcess {
		log.Eventf(ctx, "skipping compaction with %d suggestions, total=%db, used=%db, last processed=%s",
			len(suggestions), totalBytes, capacity.Used, lastProcessed)
		log.Infof(ctx, "skipping compaction with %d suggestions, total=%db, used=%db, last processed=%s",
			len(suggestions), totalBytes, capacity.Used, lastProcessed)
		return false, nil
	}

	// Process each suggestion in turn.
	for _, sc := range suggestions {
		log.VEventf(ctx, 2, "processing suggested compaction %+v", sc)
		log.Infof(ctx, "processing suggested compaction %+v", sc)
		// If the cleared flag is set, then we can delete covered sstables.
		// Before we do so, we must verify that there is no visible data in
		// the suggested key span. This has the effect of delaying any
		// compaction on ranges composing a dropped table until after all of
		// the ranges on this store have received a ClearRange command.
		if sc.Cleared {
			if empty, err := c.isSpanEmpty(ctx, sc.StartKey, sc.EndKey); err != nil {
				return false, errors.Wrapf(err, "could not verify that span is empty in suggested compaction %+v", sc)
			} else if empty {
				if err := rocksdb.DeleteFilesInRange(
					MVCCKey{Key: sc.StartKey}, MVCCKey{Key: sc.EndKey},
				); err != nil {
					return false, errors.Wrapf(err, "unable to delete files in suggested compaction %+v", sc)
				}
			}
		}
		// Compact the range, even if we've deleted the sstable files, in
		// order to GC keys in files that couldn't be dropped.
		if err := rocksdb.CompactRange(MVCCKey{Key: sc.StartKey}, MVCCKey{Key: sc.EndKey}); err != nil {
			return false, errors.Wrapf(err, "unable to delete files in suggested compaction %+v", sc)
		}
		// Delete the suggestion.
		key := keys.StoreSuggestedCompactionKey(sc.StartKey, sc.EndKey)
		if err := c.eng.Clear(MVCCKey{Key: key}); err != nil {
			return false, errors.Wrapf(err, "unable to delete files in suggested compaction %+v", sc)
		}
	}

	log.Eventf(ctx, "processed all %d compaction(s) successfully", len(suggestions))
	log.Infof(ctx, "processed all %d compaction(s) successfully", len(suggestions))
	return true, nil
}

// isSpanEmpty returns whether the specified key span is empty (true)
// or contains keys (false).
func (c *Compactor) isSpanEmpty(ctx context.Context, start, end roachpb.Key) (bool, error) {
	// If there are any suggested compactions, start the compaction timer.
	var empty = true
	if err := c.eng.Iterate(
		MVCCKey{Key: start},
		MVCCKey{Key: end},
		func(_ MVCCKeyValue) (bool, error) {
			empty = false
			return false, nil // don't continue iteration
		},
	); err != nil {
		return false, err
	}
	return empty, nil
}

// SuggestCompaction writes the specified compaction to persistent
// storage and pings the processing goroutine.
func (c *Compactor) SuggestCompaction(ctx context.Context, sc enginepb.Compaction) {
	start, end := roachpb.RKey(sc.StartKey), roachpb.RKey(sc.EndKey)
	log.VEventf(ctx, 2, "suggested compaction from %s - %s: %+v", start, end, sc)

	// Check whether a suggested compaction already exists for this key span.
	key := keys.StoreSuggestedCompactionKey(start, end)
	var existing enginepb.Compaction
	ok, _, _, err := c.eng.GetProto(MVCCKey{Key: key}, &existing)
	if err != nil {
		log.ErrEventf(ctx, "unable to record suggested compaction: %s", err)
		return
	}

	// If there's already a suggested compaction, merge them.
	if ok {
		if existing.Cleared && !sc.Cleared {
			panic("existing suggested compaction was cleared, but new is not cleared")
		}
		sc.Bytes += existing.Bytes
	}

	// Store the new compaction.
	if _, _, err = PutProto(c.eng, MVCCKey{Key: key}, &sc); err != nil {
		log.ErrEventf(ctx, "unable to record suggested compaction: %s", err)
	}

	// Poke the compactor goroutine to reconsider compaction in light of
	// this new suggested compaction.
	select {
	case c.ch <- struct{}{}:
	default:
	}
}
