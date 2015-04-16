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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"time"

	"crypto/md5"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
)

const (
	// minCacheWindow specifies the minimum duration to hold entries in
	// the cache before allowing eviction. After this window expires,
	// transactions writing to this node with timestamps lagging by more
	// than minCacheWindow will necessarily have to advance their commit
	// timestamp.
	minCacheWindow = 10 * time.Second
)

// A TimestampCache maintains an interval tree FIFO cache of keys or
// key ranges and the timestamps at which they were most recently read
// or written. If a timestamp was read or written by a transaction, an
// MD5 of the txn ID is stored with the timestamp to avoid advancing
// timestamps on successive requests from the same transaction. We use
// the MD5 of the txn ID to conserve memory as txn IDs are expected to
// be fairly large ~100 bytes.
//
// The cache also maintains a low-water mark which is the most
// recently evicted entry's timestamp. This value always ratchets
// with monotonic increases. The low water mark is initialized to
// the current system time plus the maximum clock offset.
type TimestampCache struct {
	cache            *util.IntervalCache
	lowWater, latest proto.Timestamp
}

// A cacheEntry combines the timestamp with an optional MD5 of the
// transaction ID.
type cacheEntry struct {
	timestamp proto.Timestamp
	txnMD5    [md5.Size]byte // Empty for no transaction
	readOnly  bool           // Command is read-only
}

// NewTimestampCache returns a new timestamp cache with supplied
// hybrid clock.
func NewTimestampCache(clock *hlc.Clock) *TimestampCache {
	tc := &TimestampCache{
		cache: util.NewIntervalCache(util.CacheConfig{Policy: util.CacheFIFO}),
	}
	tc.Clear(clock)
	tc.cache.CacheConfig.ShouldEvict = tc.shouldEvict
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

// Add the specified timestamp to the cache as covering the range of
// keys from start to end. If end is nil, the range covers the start
// key only. txnMD5 is empty for no transaction. readOnly specifies
// whether the command adding this timestamp was read-only or not.
func (tc *TimestampCache) Add(start, end proto.Key, timestamp proto.Timestamp, txnMD5 [md5.Size]byte, readOnly bool) {
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
		// Check existing, overlapping entries. Remove superseded
		// entries or return without adding this entry if necessary.
		key := tc.cache.NewKey(start, end)
		for _, o := range tc.cache.GetOverlaps(start, end) {
			ce := o.Value.(cacheEntry)
			if ce.readOnly != readOnly {
				continue
			}
			if o.Key.Contains(key) && !ce.timestamp.Less(timestamp) {
				return // don't add this key; there's already a cache entry with >= timestamp.
			} else if key.Contains(o.Key) && !timestamp.Less(ce.timestamp) {
				tc.cache.Del(o.Key) // delete existing key; this cache entry supersedes.
			}
		}
		ce := cacheEntry{timestamp: timestamp, txnMD5: txnMD5, readOnly: readOnly}
		tc.cache.Add(key, ce)
	}
}

// GetMax returns the maximum read and write timestamps which overlap
// the interval spanning from start to end. Cached timestamps matching
// the specified txnID are not considered. If no part of the specified
// range is overlapped by timestamps in the cache, the low water
// timestamp is returned for both read and write timestamps.
//
// The txnMD5 is an MD5 of the transaction ID. It prevents restarts
// with a pattern like: read("a"), write("a"). The read adds a
// timestamp for "a". Then the write (for the same transaction) would
// get that as the max timestamp and be forced to increment it. The MD5
// allows timestamps from the same txn to be ignored.
func (tc *TimestampCache) GetMax(start, end proto.Key, txnMD5 [md5.Size]byte) (proto.Timestamp, proto.Timestamp) {
	if len(end) == 0 {
		end = start.Next()
	}
	maxR := tc.lowWater
	maxW := tc.lowWater
	for _, o := range tc.cache.GetOverlaps(start, end) {
		ce := o.Value.(cacheEntry)
		if proto.MD5Equal(ce.txnMD5, proto.NoTxnMD5) || proto.MD5Equal(txnMD5, proto.NoTxnMD5) || !proto.MD5Equal(txnMD5, ce.txnMD5) {
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
// dest timestamp cache. The copy parameter, if true, copies the
// values of lowWater and latest.
func (tc *TimestampCache) MergeInto(dest *TimestampCache, copy bool) {
	if copy {
		dest.cache.Clear()
		dest.lowWater = tc.lowWater
		dest.latest = tc.latest
	} else {
		if dest.lowWater.Less(tc.lowWater) {
			dest.lowWater = tc.lowWater
		}
		if dest.latest.Less(tc.latest) {
			tc.latest = dest.latest
		}
	}
	tc.cache.Do(func(k, v interface{}) {
		dest.cache.Add(k, v)
	})
}

// shouldEvict returns true if the cache entry's timestamp is no
// longer within the minCacheWindow.
func (tc *TimestampCache) shouldEvict(size int, key, value interface{}) bool {
	ce := value.(cacheEntry)
	// Compute the edge of the cache window.
	edge := tc.latest
	edge.WallTime -= minCacheWindow.Nanoseconds()
	// We evict and update the low water mark if the proposed evictee's
	// timestamp is <= than the edge of the window.
	if !edge.Less(ce.timestamp) {
		tc.lowWater = ce.timestamp
		return true
	}
	return false
}
