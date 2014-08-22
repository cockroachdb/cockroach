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
	"bytes"
	"time"

	"code.google.com/p/biogo.store/interval"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
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

// rangeKey implements interval.Comparable.
type rangeKey engine.Key

// Compare implements the llrb.Comparable interface for tree nodes.
func (rk rangeKey) Compare(b interval.Comparable) int {
	return bytes.Compare(rk, b.(rangeKey))
}

// A ReadTimestampCache maintains an interval tree FIFO cache of keys
// or key ranges and the timestamps at which they were most recently
// read.
//
// The cache also maintains a high-water mark which is the most
// recently evicted entry's timestamp. This value always ratchets
// with monotonic increases. The high water mark is initialized to
// the current system time plus the maximum clock skew.
type ReadTimestampCache struct {
	cache     *util.IntervalCache
	clock     *hlc.Clock
	highWater proto.Timestamp
}

// NewReadTimestampCache returns a new read timestamp cache with
// supplied hybrid clock.
func NewReadTimestampCache(clock *hlc.Clock) *ReadTimestampCache {
	rtc := &ReadTimestampCache{
		cache: util.NewIntervalCache(util.CacheConfig{Policy: util.CacheFIFO}),
		clock: clock,
	}
	rtc.Clear()
	rtc.cache.CacheConfig.ShouldEvict = rtc.shouldEvict
	return rtc
}

// Clear clears the cache and resets the high water mark to the
// current time plus the maximum clock skew.
func (rtc *ReadTimestampCache) Clear() {
	rtc.cache.Clear()
	rtc.highWater = rtc.clock.Now()
	rtc.highWater.WallTime += int64(rtc.clock.MaxDrift())
}

// Add the specified read timestamp to the cache as covering the range of
// keys from start to end. If end is nil, the range covers the start
// key only.
func (rtc *ReadTimestampCache) Add(start, end engine.Key, timestamp proto.Timestamp) {
	if end == nil {
		end = engine.NextKey(start)
	}
	rtc.cache.Add(rtc.cache.NewKey(rangeKey(start), rangeKey(end)), timestamp)
}

// GetMax returns the maximum read timestamp covering any part of the
// interval spanning from start to end keys. If no part of the
// specified range is overlapped by read timestamps in the cache, the
// high water timestamp is returned.
func (rtc *ReadTimestampCache) GetMax(start, end engine.Key) proto.Timestamp {
	if end == nil {
		end = engine.NextKey(start)
	}
	max := rtc.highWater
	for _, v := range rtc.cache.GetOverlaps(rangeKey(start), rangeKey(end)) {
		ts := v.(proto.Timestamp)
		if max.Less(ts) {
			max = ts
		}
	}
	return max
}

// shouldEvict returns true if the cache entry's timestamp is no
// longer within the minCacheWindow.
func (rtc *ReadTimestampCache) shouldEvict(size int, key, value interface{}) bool {
	ts := value.(proto.Timestamp)
	// Compute the edge of the cache window.
	edge := rtc.clock.Now()
	edge.WallTime -= minCacheWindow.Nanoseconds()
	// We evict and update the high water mark if the proposed evictee's
	// timestamp is less than the edge of the window.
	if ts.Less(edge) {
		rtc.highWater = ts
		return true
	}
	return false
}
