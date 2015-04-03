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
	"github.com/cockroachdb/cockroach/util"
)

// A LeaderCache is a cache used to keep track of the leader
// replica of Raft consensus groups.
type LeaderCache struct {
	mu    sync.RWMutex
	cache *util.UnorderedCache
}

// NewLeaderCache creates a new LeaderCache of the given size.
// The underlying cache internally uses a hash map, so lookups
// are cheap.
func NewLeaderCache(size int) *LeaderCache {
	return &LeaderCache{
		cache: util.NewUnorderedCache(util.CacheConfig{
			Policy: util.CacheLRU,
			ShouldEvict: func(s int, key, value interface{}) bool {
				return s > size
			},
		}),
	}
}

// Lookup consults the cache for the replica cached as the leader of
// the given Raft consensus group.
func (lc *LeaderCache) Lookup(group proto.RaftID) *proto.Replica {
	lc.mu.RLock()
	v, ok := lc.cache.Get(group)
	lc.mu.RUnlock()
	if !ok {
		return nil
	}
	return v.(*proto.Replica)
}

// EvictOrUpdate invalidates the cached leader for the given Raft group.
// If a new replica is passed in, and that replica does not match the
// freshly evicted one's StoreID, it is inserted into the cache.
func (lc *LeaderCache) EvictOrUpdate(group proto.RaftID, r *proto.Replica) {
	old := lc.Lookup(group)
	lc.mu.Lock()
	lc.cache.Del(group)
	if r != nil && (old == nil || old.StoreID != r.StoreID) {
		lc.cache.Add(group, r)
	}
	lc.mu.Unlock()
}
