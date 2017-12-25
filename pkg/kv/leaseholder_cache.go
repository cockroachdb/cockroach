// Copyright 2015 The Cockroach Authors.
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

package kv

import (
	"context"
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

var (
	defaultShards = 2 * runtime.NumCPU()
)

// A LeaseHolderCache is a cache of replica descriptors keyed by range ID.
type LeaseHolderCache struct {
	shards []LeaseHolderCacheShard
}

// A LeaseHolderCacheShard is a cache of replica descriptors keyed by range ID.
type LeaseHolderCacheShard struct {
	// NB: This can't be a RWMutex for lookup because UnorderedCache.Get
	// manipulates an internal LRU list.
	mu    syncutil.Mutex
	cache *cache.UnorderedCache
}

// NewLeaseHolderCache creates a new leaseHolderCache of the given size.
// The underlying cache internally uses a hash map, so lookups
// are cheap.
func NewLeaseHolderCache(size func() int64) *LeaseHolderCache {
	leaseholderCache := &LeaseHolderCache{}
	leaseholderCache.shards = make([]LeaseHolderCacheShard, defaultShards)
	for i := range leaseholderCache.shards {
		val := &leaseholderCache.shards[i]
		val.cache = cache.NewUnorderedCache(cache.Config{
			Policy: cache.CacheLRU,
			ShouldEvict: func(s int, key, value interface{}) bool {
				return int64(s) > size()/int64(defaultShards)
			},
		})
	}
	return leaseholderCache
}

// Lookup returns the cached leader of the given range ID.
func (lc *LeaseHolderCache) Lookup(
	ctx context.Context, rangeID roachpb.RangeID,
) (roachpb.StoreID, bool) {
	ld := &lc.shards[int(rangeID)%len(lc.shards)]
	ld.mu.Lock()
	defer ld.mu.Unlock()
	if v, ok := ld.cache.Get(rangeID); ok {
		if log.V(2) {
			log.Infof(ctx, "r%d: lookup leaseholder: %s", rangeID, v)
		}
		return v.(roachpb.StoreID), true
	}
	if log.V(2) {
		log.Infof(ctx, "r%d: lookup leaseholder: not found", rangeID)
	}
	return 0, false
}

// Update invalidates the cached leader for the given range ID. If an empty
// replica descriptor is passed, the cached leader is evicted. Otherwise, the
// passed-in replica descriptor is cached.
func (lc *LeaseHolderCache) Update(
	ctx context.Context, rangeID roachpb.RangeID, storeID roachpb.StoreID,
) {
	ld := &lc.shards[int(rangeID)%len(lc.shards)]
	ld.mu.Lock()
	defer ld.mu.Unlock()
	if storeID == 0 {
		if log.V(2) {
			log.Infof(ctx, "r%d: evicting leaseholder", rangeID)
		}
		ld.cache.Del(rangeID)
	} else {
		if log.V(2) {
			log.Infof(ctx, "r%d: updating leaseholder: %d", rangeID, storeID)
		}
		ld.cache.Add(rangeID, storeID)
	}
}
