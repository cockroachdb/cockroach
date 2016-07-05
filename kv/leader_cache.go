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
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package kv

import (
	"sync"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/cache"
)

// A leaderCache is a cache of replica descriptors keyed by range iD.
type leaderCache struct {
	mu    sync.Mutex
	cache *cache.UnorderedCache
}

// newLeaderCache creates a new leaderCache of the given size.
// The underlying cache internally uses a hash map, so lookups
// are cheap.
func newLeaderCache(size int) *leaderCache {
	return &leaderCache{
		cache: cache.NewUnorderedCache(cache.Config{
			Policy: cache.CacheLRU,
			ShouldEvict: func(s int, key, value interface{}) bool {
				return s > size
			},
		}),
	}
}

// Lookup returns the cached leader of the given range ID.
func (lc *leaderCache) Lookup(rangeID roachpb.RangeID) (roachpb.ReplicaDescriptor, bool) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if v, ok := lc.cache.Get(rangeID); ok {
		return v.(roachpb.ReplicaDescriptor), true
	}
	return roachpb.ReplicaDescriptor{}, false
}

// Update invalidates the cached leader for the given range ID. If an empty
// replica descriptor is passed, the cached leader is evicted. Otherwise, the
// passed-in replica descriptor is cached.
func (lc *leaderCache) Update(rangeID roachpb.RangeID, repDesc roachpb.ReplicaDescriptor) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if (repDesc == roachpb.ReplicaDescriptor{}) {
		lc.cache.Del(rangeID)
	} else {
		lc.cache.Add(rangeID, repDesc)
	}
}
