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
// Author: Kevin GuanJian (keynovo@gmail.com)

package storage

import (
	"sync"

	"github.com/biogo/store/llrb"
	"github.com/cockroachdb/cockroach/util/cache"
	"github.com/coreos/etcd/raft/raftpb"
)

type entryCacheKey uint64

// Compare implements the llrb.Comparable interface for entryCacheKey, so that
// it can be used as a key for util.OrderedCache.
func (a entryCacheKey) Compare(b llrb.Comparable) int {
	if a > b.(entryCacheKey) {
		return 1
	} else if a < b.(entryCacheKey) {
		return -1
	}
	return 0
}

// A raftEntryCache maintains the recent entries
type raftEntryCache struct {
	// entryCache caches recent entries, the Index as the key.
	entryCache *cache.OrderedCache
	// the minimum Index in the cache, evicted when cache is filled.
	firstIndex uint64
	// protects Cache for concurrent access.
	sync.Mutex
}

// newRaftEntryCache returns a new RaftEntryCache with the given size.
func newRaftEntryCache(size int) *raftEntryCache {
	return &raftEntryCache{
		entryCache: cache.NewOrderedCache(cache.Config{
			Policy: cache.CacheLRU,
			ShouldEvict: func(n int, k, v interface{}) bool {
				return n > size
			},
		}),
	}
}

// addEntry will add entries, the Index as the key.
func (rec *raftEntryCache) addEntry(ents []raftpb.Entry) {
	rec.Lock()
	defer rec.Unlock()
	if len(ents) == 0 {
		return
	}

	for _, e := range ents {
		rec.entryCache.Add(entryCacheKey(e.Index), e)
	}

	if rec.firstIndex == uint64(0) ||
		rec.firstIndex > ents[0].Index {
		rec.firstIndex = ents[0].Index
	}
}

// getEntry will return entries between [lo, hi).
func (rec *raftEntryCache) getEntry(lo, hi, maxBytes uint64) ([]raftpb.Entry, uint64 /* size */, uint64 /* max Index */) {
	rec.Lock()
	defer rec.Unlock()
	var entries []raftpb.Entry
	var ent raftpb.Entry
	size := uint64(0)

	for i := lo; i < hi; i++ {
		v, ok := rec.entryCache.Get(entryCacheKey(i))
		if ok {
			ent = v.(raftpb.Entry)
			entries = append(entries, ent)
			size = size + uint64(ent.Size())
			if maxBytes > 0 && size > maxBytes {
				return entries, size, i + 1
			}
		} else {
			return entries, size, i
		}
	}
	return entries, size, hi
}

// delEntry will delete any entries between [lo, hi).
func (rec *raftEntryCache) delEntry(lo, hi uint64) {
	rec.Lock()
	defer rec.Unlock()
	if lo > hi {
		return
	}
	for i := lo; i < hi; i++ {
		rec.entryCache.Del(entryCacheKey(i))
	}

	if rec.firstIndex >= lo && rec.firstIndex < hi {
		rec.firstIndex = hi
	}
}

// clearAll will clears all the entries in the cache.
func (rec *raftEntryCache) clearAll() {
	rec.Lock()
	defer rec.Unlock()
	rec.entryCache.Clear()
	rec.firstIndex = uint64(0)
	return
}
