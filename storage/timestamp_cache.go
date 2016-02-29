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
	"sort"
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
	cache            *cache.IntervalCache
	lowWater, latest roachpb.Timestamp
	rg               interval.RangeGroup // avoids allocating in Add.
}

// A cacheValue combines the timestamp with an optional txn ID.
type cacheValue struct {
	timestamp roachpb.Timestamp
	txnID     *uuid.UUID // Nil for no transaction
	readOnly  bool       // Command is read-only
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
		cache: cache.NewIntervalCache(cache.Config{Policy: cache.CacheFIFO}),
		rg:    interval.NewRangeTree(),
	}
	tc.Clear(clock)
	tc.cache.Config.ShouldEvict = tc.shouldEvict
	return tc
}

// Clear clears the cache and resets the low water mark to the
// current time plus the maximum clock offset.
func (tc *TimestampCache) Clear(clock *hlc.Clock) {
	tc.cache.Clear()
	tc.lowWater = clock.Now()
	tc.lowWater.WallTime += clock.MaxOffset().Nanoseconds()
	tc.latest = tc.lowWater
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
// key only. txnID is nil for no transaction. readOnly specifies
// whether the command adding this timestamp was read-only or not.
func (tc *TimestampCache) Add(start, end roachpb.Key, timestamp roachpb.Timestamp, txnID *uuid.UUID, readOnly bool) {
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
		defer tc.rg.Clear()

		key := tc.cache.MakeKey(start, end)
		overlaps := tc.cache.GetOverlaps(key.Start, key.End)
		overlaps = filterReadOrWrite(overlaps, readOnly)

		// Check existing, overlapping entries. Remove/replace superseded
		// entries or return without adding this entry if necessary.
		introduced := false
		sort.Sort(tsValSorter(overlaps))
		for i := len(overlaps) - 1; i >= 0; i-- {
			c := overlaps[i]
			cv := c.Value.(*cacheValue)

			// Because there is no guarantee the new key will have a greater timestamp
			// than other keys, we must wait to insert it into the RangeGroup until we
			// find the correct position in the sorting.
			if !introduced && cv.timestamp.Less(timestamp) {
				introduced = true
				if !tc.rg.Add(key.Range) {
					// Don't add this key; there's already a union of cache entries with
					// >= timestamps. Note that in this case, no entries will be added to
					// the cache such that no keys would need to be deleted because they
					// are no longer the most recent, so we can just return.
					return
				}
			}

			// If the entry is no longer the most recent for any part of its range, we
			// can remove it. However, if the entry's range adds to the RangeGroup, that
			// means that it is the most recent for at least some part of the range and
			// shouldn't be removed.
			if !tc.rg.Add(c.Key.Range) {
				if introduced && key.Range.Equal(c.Key.Range) {
					// The ranges are equal, update the existing cache entry instead
					// of deleting and reinserting the same range.
					*cv = cacheValue{timestamp: timestamp, txnID: txnID, readOnly: readOnly}
					return
				}
				// Delete existing key; a set of cache entries with greater timestamps
				// that are already in the RangeGroup fully supersede.
				tc.cache.Del(c.Key)
			}
		}
		// Handle the case where the new key is the oldest overlapping key.
		if !introduced && !tc.rg.Add(key.Range) {
			// Don't add this key; there's already union of cache entries with >= timestamp.
			return
		}

		value := cacheValue{timestamp: timestamp, txnID: txnID, readOnly: readOnly}
		entry := makeCacheEntry(key, value)
		tc.cache.AddEntry(entry)
	}
}

// filterReadOrWrite filters out either the read-only or the read-write
// values from the provided slice, depending on the second provided argument.
func filterReadOrWrite(ts []cache.Overlap, keepReadOnly bool) []cache.Overlap {
	rmIdx := len(ts)
	for i := 0; i < rmIdx; {
		cv := ts[i].Value.(*cacheValue)
		if cv.readOnly == keepReadOnly {
			i++
		} else {
			ts[i], ts[rmIdx-1] = ts[rmIdx-1], ts[i]
			rmIdx--
		}
	}
	return ts[:rmIdx]
}

// tsValSorter attaches the methods of sort.Interface to []cache.Overlap, sorting
// in increasing timestamp order.
type tsValSorter []cache.Overlap

func (t tsValSorter) Len() int { return len(t) }
func (t tsValSorter) Less(i, j int) bool {
	return t[i].Value.(*cacheValue).timestamp.Less(t[j].Value.(*cacheValue).timestamp)
}
func (t tsValSorter) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

// GetMax returns the maximum read and write timestamps which overlap
// the interval spanning from start to end. Cached timestamps matching
// the specified txnID are not considered. If no part of the specified
// range is overlapped by timestamps in the cache, the low water
// timestamp is returned for both read and write timestamps.
//
// The txn ID prevents restarts with a pattern like: read("a"),
// write("a"). The read adds a timestamp for "a". Then the write (for
// the same transaction) would get that as the max timestamp and be
// forced to increment it. This allows timestamps from the same txn
// to be ignored.
func (tc *TimestampCache) GetMax(start, end roachpb.Key, txnID *uuid.UUID) (roachpb.Timestamp, roachpb.Timestamp) {
	if len(end) == 0 {
		end = start.Next()
	}
	maxR := tc.lowWater
	maxW := tc.lowWater
	for _, o := range tc.cache.GetOverlaps(start, end) {
		ce := o.Value.(*cacheValue)
		if ce.txnID == nil || txnID == nil || !roachpb.TxnIDEqual(txnID, ce.txnID) {
			if ce.readOnly && maxR.Less(ce.timestamp) {
				maxR = ce.timestamp
			} else if !ce.readOnly && maxW.Less(ce.timestamp) {
				maxW = ce.timestamp
			}
		}
	}
	return maxR, maxW
}

// MergeInto merges all entries from this timestamp cache into the
// dest timestamp cache. The clear parameter, if true, copies the
// values of lowWater and latest and clears the destination cache
// before merging in the source.
func (tc *TimestampCache) MergeInto(dest *TimestampCache, clear bool) {
	if clear {
		dest.cache.Clear()
		dest.lowWater = tc.lowWater
		dest.latest = tc.latest
	} else {
		dest.lowWater.Forward(tc.lowWater)
		dest.latest.Forward(tc.latest)
	}
	tc.cache.Do(func(k, v interface{}) {
		// Cache entries are mutable (see Add), so we give each cache its own
		// unique copy.
		entry := makeCacheEntry(*k.(*cache.IntervalKey), *v.(*cacheValue))
		dest.cache.AddEntry(entry)
	})
}

// shouldEvict returns true if the cache entry's timestamp is no
// longer within the MinTSCacheWindow.
func (tc *TimestampCache) shouldEvict(size int, key, value interface{}) bool {
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
