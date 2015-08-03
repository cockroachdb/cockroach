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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package kv

import (
	"sync"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/cache"
)

// A leaderCache is a cache used to keep track of the leader
// replica of Raft consensus groups.
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

// Lookup consults the cache for the replica cached as the leader of
// the given Raft consensus group.
func (lc *leaderCache) Lookup(group proto.RangeID) proto.Replica {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	v, ok := lc.cache.Get(group)
	if !ok || v == nil {
		return proto.Replica{}
	}
	return *(v.(*proto.Replica))
}

// Update invalidates the cached leader for the given Raft group.
// If a replica is passed in, it is inserted into the cache.
// A StoreID of 0 (empty replica) means evict.
func (lc *leaderCache) Update(group proto.RangeID, r proto.Replica) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.cache.Del(group)
	if r.StoreID != 0 {
		lc.cache.Add(group, &r)
	}
}
