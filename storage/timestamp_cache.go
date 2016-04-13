// Copyright 2014 The Cockroach Authors.
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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/cache"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/interval"
	"github.com/cockroachdb/cockroach/util/uuid"
)

const (
	// MinTSCacheWindow specifies the minimum duration to hold entries in
	// the cache before allowing eviction. After this window expires,
	// transactions writing to this node with timestamps lagging by more
	// than minCacheWindow will necessarily have to advance their commit
	// timestamp.
	MinTSCacheWindow = 10 * time.Second

	defaultEvictionSizeThreshold = 512
)

// A TimestampCache maintains an interval tree FIFO cache of keys or
// key ranges and the timestamps at which they were most recently read
// or written. If a timestamp was read or written by a transaction,
// the txn ID is stored with the timestamp to avoid advancing
// timestamps on successive requests from the same transaction.
//
// The cache also maintains a low-water mark which is the most
// recently evicted entry's timestamp. This value always ratchets
// with monotonic increases. The low water mark is initialized to
// the current system time plus the maximum clock offset.
type TimestampCache struct {
	rCache, wCache   *cache.IntervalCache
	lowWater, latest roachpb.Timestamp

	// evictionSizeThreshold allows old entries to stay in the TimestampCache
	// indefinitely as long as the number of intervals in the cache doesn't
	// exceed this value. Once the cache grows beyond it, intervals are
	// evicted according to the time window. This threshold is intended to
	// permit transactions to take longer than the eviction window duration
	// if the size of the cache is not a concern, such as when a user
	// is using an interactive SQL shell.
	evictionSizeThreshold int
}

// A cacheValue combines the timestamp with an optional txn ID.
type cacheValue struct {
	timestamp roachpb.Timestamp
	txnID     *uuid.UUID // Nil for no transaction
}

func makeCacheEntry(key cache.IntervalKey, value cacheValue) *cache.Entry {
	alloc := struct {
		key   cache.IntervalKey
		value cacheValue
		entry cache.Entry
	}{
		key:   key,
		value: value,
	}
	alloc.entry.Key = &alloc.key
	alloc.entry.Value = &alloc.value
	return &alloc.entry
}

// NewTimestampCache returns a new timestamp cache with supplied
// hybrid clock.
func NewTimestampCache(clock *hlc.Clock) *TimestampCache {
	tc := &TimestampCache{
		rCache:                cache.NewIntervalCache(cache.Config{Policy: cache.CacheFIFO}),
		wCache:                cache.NewIntervalCache(cache.Config{Policy: cache.CacheFIFO}),
		evictionSizeThreshold: defaultEvictionSizeThreshold,
	}
	tc.Clear(clock)
	tc.rCache.Config.ShouldEvict = tc.shouldEvict
	tc.wCache.Config.ShouldEvict = tc.shouldEvict
	return tc
}

// Clear clears the cache and resets the low water mark to the
// current time plus the maximum clock offset.
func (tc *TimestampCache) Clear(clock *hlc.Clock) {
	tc.rCache.Clear()
	tc.wCache.Clear()
	tc.lowWater = clock.Now()
	// TODO(tschottdorf): It's dangerous to inject timestamps (which will make
	// it into the HLC) like that.
	tc.lowWater.WallTime += clock.MaxOffset().Nanoseconds()
	tc.latest = tc.lowWater
}

// Len returns the total number of read and write intervals in the
// TimestampCache.
func (tc *TimestampCache) Len() int {
	return tc.rCache.Len() + tc.wCache.Len()
}

// SetLowWater sets the cache's low water mark, which is the minimum
// value the cache will return from calls to GetMax().
func (tc *TimestampCache) SetLowWater(lowWater roachpb.Timestamp) {
	if tc.lowWater.Less(lowWater) {
		tc.lowWater = lowWater
	}
}

// Add the specified timestamp to the cache as covering the range of
// keys from start to end. If end is nil, the range covers the start
// key only. txnID is nil for no transaction. readTSCache specifies
// whether the command adding this timestamp should update the read
// timestamp; false to update the write timestamp cache.
func (tc *TimestampCache) Add(start, end roachpb.Key, timestamp roachpb.Timestamp, txnID *uuid.UUID, readTSCache bool) {
	// This gives us a memory-efficient end key if end is empty.
	if len(end) == 0 {
		end = start.Next()
		start = end[:len(start)]
	}
	if tc.latest.Less(timestamp) {
		tc.latest = timestamp
	}
	// Only add to the cache if the timestamp is more recent than the
	// low water mark.
	if tc.lowWater.Less(timestamp) {
		cache := tc.wCache
		if readTSCache {
			cache = tc.rCache
		}

		addRange := func(r interval.Range) {
			value := cacheValue{timestamp: timestamp, txnID: txnID}
			key := cache.MakeKey(r.Start, r.End)
			entry := makeCacheEntry(key, value)
			cache.AddEntry(entry)
		}
		r := interval.Range{
			Start: interval.Comparable(start),
			End:   interval.Comparable(end),
		}

		// Check existing, overlapping entries and truncate/split/remove if
		// superseded and in the past. If existing entries are in the future,
		// subtract from the range/ranges that need to be added to cache.
		for _, o := range cache.GetOverlaps(r.Start, r.End) {
			cv := o.Value.(*cacheValue)
			sCmp := r.Start.Compare(o.Key.Start)
			eCmp := r.End.Compare(o.Key.End)
			if !timestamp.Less(cv.timestamp) {
				// The existing interval has a timestamp less than or equal to the new interval.
				// Compare interval ranges to determine how to modify existing interval.
				switch {
				case sCmp == 0 && eCmp == 0:
					// New and old are equal; replace old with new and avoid the need to insert new.
					//
					// New: ------------
					// Old: ------------
					//
					// New: ------------
					*cv = cacheValue{timestamp: timestamp, txnID: txnID}
					cache.MoveToEnd(o.Entry)
					return
				case sCmp <= 0 && eCmp >= 0:
					// New contains or is equal to old; delete old.
					//
					// New: ------------      ------------      ------------
					// Old:   --------    or    ----------  or  ----------
					//
					// Old:
					cache.DelEntry(o.Entry)
				case sCmp > 0 && eCmp < 0:
					// Old contains new; split up old into two.
					//
					// New:     ----
					// Old: ------------
					//
					// Old: ----    ----
					oldEnd := o.Key.End
					o.Key.End = r.Start

					key := cache.MakeKey(r.End, oldEnd)
					entry := makeCacheEntry(key, *cv)
					cache.AddEntryAfter(entry, o.Entry)
				case eCmp >= 0:
					// Left partial overlap; truncate old end.
					//
					// New:     --------          --------
					// Old: --------      or  ------------
					//
					// Old: ----              ----
					o.Key.End = r.Start
				case sCmp <= 0:
					// Right partial overlap; truncate old start.
					//
					// New: --------          --------
					// Old:     --------  or  ------------
					//
					// Old:         ----              ----
					o.Key.Start = r.End
				default:
					panic(fmt.Sprintf("no overlap between %v and %v", o.Key.Range, r))
				}
			} else {
				// The existing interval has a timestamp greater than the new interval.
				// Compare interval ranges to determine how to modify new interval before
				// adding it to the timestamp cache.
				switch {
				case sCmp >= 0 && eCmp <= 0:
					// Old contains or is equal to new; no need to add.
					//
					// Old: -----------      -----------      -----------      -----------
					// New:    -----     or  -----------  or  --------     or     --------
					//
					// New:
					return
				case sCmp < 0 && eCmp > 0:
					// New contains old; split up old into two. We can add the left piece
					// immediately because it is guaranteed to be before the rest of the
					// overlaps.
					//
					// Old:    ------
					// New: ------------
					//
					// New: ---      ---
					lr := interval.Range{Start: r.Start, End: o.Key.Start}
					addRange(lr)

					r.Start = o.Key.End
				case eCmp > 0:
					// Left partial overlap; truncate new start.
					//
					// Old: --------          --------
					// New:     --------  or  ------------
					//
					// New:         ----              ----
					r.Start = o.Key.End
				case sCmp < 0:
					// Right partial overlap; truncate new end.
					//
					// Old:     --------          --------
					// New: --------      or  ------------
					//
					// New: ----              ----
					r.End = o.Key.Start
				default:
					panic(fmt.Sprintf("no overlap between %v and %v", o.Key.Range, r))
				}
			}
		}
		addRange(r)
	}
}

// GetMaxRead returns the maximum read timestamp which overlaps the
// interval spanning from start to end. Cached timestamps matching the
// specified txnID are not considered. If no part of the specified
// range is overlapped by timestamps from different transactions in
// the cache, the low water timestamp is returned for the read
// timestamps. Also returns an "ok" bool, indicating whether an
// explicit match of the interval was found in the cache.
func (tc *TimestampCache) GetMaxRead(start, end roachpb.Key, txnID *uuid.UUID) (roachpb.Timestamp, bool) {
	return tc.getMax(start, end, txnID, true)
}

// GetMaxWrite returns the maximum write timestamp which overlaps the
// interval spanning from start to end. Cached timestamps matching the
// specified txnID are not considered. If no part of the specified
// range is overlapped by timestamps from different transactions in
// the cache, the low water timestamp is returned for the write
// timestamps. Also returns an "ok" bool, indicating whether an
// explicit match of the interval was found in the cache.
//
// The txn ID prevents restarts with a pattern like: read("a"),
// write("a"). The read adds a timestamp for "a". Then the write (for
// the same transaction) would get that as the max timestamp and be
// forced to increment it. This allows timestamps from the same txn
// to be ignored because the write would instead get the low water
// timestamp.
func (tc *TimestampCache) GetMaxWrite(start, end roachpb.Key, txnID *uuid.UUID) (roachpb.Timestamp, bool) {
	return tc.getMax(start, end, txnID, false)
}

func (tc *TimestampCache) getMax(start, end roachpb.Key, txnID *uuid.UUID, readTSCache bool) (roachpb.Timestamp, bool) {
	if len(end) == 0 {
		end = start.Next()
	}
	var ok bool
	max := tc.lowWater
	cache := tc.wCache
	if readTSCache {
		cache = tc.rCache
	}
	for _, o := range cache.GetOverlaps(start, end) {
		ce := o.Value.(*cacheValue)
		if ce.txnID == nil || txnID == nil || !roachpb.TxnIDEqual(txnID, ce.txnID) {
			if max.Less(ce.timestamp) {
				ok = true
				max = ce.timestamp
			}
		}
	}
	return max, ok
}

// MergeInto merges all entries from this timestamp cache into the
// dest timestamp cache. The clear parameter, if true, copies the
// values of lowWater and latest and clears the destination cache
// before merging in the source.
func (tc *TimestampCache) MergeInto(dest *TimestampCache, clear bool) {
	if clear {
		dest.rCache.Clear()
		dest.wCache.Clear()
		dest.lowWater = tc.lowWater
		dest.latest = tc.latest

		// Because we just cleared the destination cache, we can directly
		// insert entries from this cache.
		hardMerge := func(srcCache, destCache *cache.IntervalCache) {
			srcCache.Do(func(k, v interface{}) {
				// Cache entries are mutable (see Add), so we give each cache its own
				// unique copy.
				entry := makeCacheEntry(*k.(*cache.IntervalKey), *v.(*cacheValue))
				destCache.AddEntry(entry)
			})
		}
		hardMerge(tc.rCache, dest.rCache)
		hardMerge(tc.wCache, dest.wCache)
	} else {
		dest.lowWater.Forward(tc.lowWater)
		dest.latest.Forward(tc.latest)

		// The cache was not cleared before, so we can't just insert entries because
		// intervals may need to be adjusted or removed to maintain the non-overlapping
		// guarantee.
		softMerge := func(srcCache *cache.IntervalCache, readTSCache bool) {
			srcCache.Do(func(k, v interface{}) {
				key, val := *k.(*cache.IntervalKey), *v.(*cacheValue)
				dest.Add(roachpb.Key(key.Start), roachpb.Key(key.End), val.timestamp, val.txnID, readTSCache)
			})
		}
		softMerge(tc.rCache, true)
		softMerge(tc.wCache, false)
	}
}

// shouldEvict returns true if the cache entry's timestamp is no
// longer within the MinTSCacheWindow.
func (tc *TimestampCache) shouldEvict(size int, key, value interface{}) bool {
	if tc.Len() <= tc.evictionSizeThreshold {
		return false
	}
	ce := value.(*cacheValue)
	// In case low water mark was set higher, evict any entries
	// which occurred before it.
	if ce.timestamp.Less(tc.lowWater) {
		return true
	}
	// Compute the edge of the cache window.
	edge := tc.latest
	edge.WallTime -= MinTSCacheWindow.Nanoseconds()
	// We evict and update the low water mark if the proposed evictee's
	// timestamp is <= than the edge of the window.
	if !edge.Less(ce.timestamp) {
		tc.lowWater = ce.timestamp
		return true
	}
	return false
}
