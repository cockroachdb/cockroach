// Copyright 2016 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer@cockroachlabs.com)

package storage

import (
	"github.com/biogo/store/llrb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/coreos/etcd/raft/raftpb"
)

type entryCacheKey struct {
	RangeID roachpb.RangeID
	Index   uint64
}

// Compare implements the llrb.Comparable interface for entryCacheKey, so that
// it can be used as a key for util.OrderedCache.
func (a *entryCacheKey) Compare(b llrb.Comparable) int {
	bk := b.(*entryCacheKey)
	switch {
	case a.RangeID < bk.RangeID:
		return -1
	case a.RangeID > bk.RangeID:
		return 1
	case a.Index < bk.Index:
		return -1
	case a.Index > bk.Index:
		return 1
	default:
		return 0
	}
}

type raftEntryCacheShard struct {
	syncutil.RWMutex                     // protects Cache for concurrent access.
	bytes            uint64              // total size of the cache in bytes
	cache            *cache.OrderedCache // LRU cache of log entries, keyed by rangeID / log index
}

// A raftEntryCache maintains a global cache of Raft group log
// entries. The cache mostly prevents unnecessary reads from disk of
// recently-written log entries between log append and application
// to the FSM.
type raftEntryCache struct {
	shards []raftEntryCacheShard
}

// newRaftEntryCache returns a new RaftEntryCache with the given
// maximum size in bytes.
func newRaftEntryCache(maxBytes uint64, numShards int) *raftEntryCache {
	maxBytes /= uint64(numShards)

	rec := &raftEntryCache{
		shards: make([]raftEntryCacheShard, numShards),
	}

	for i := range rec.shards {
		s := &rec.shards[i]
		cache := cache.NewOrderedCache(cache.Config{
			Policy: cache.CacheLRU,
			// The raft entry cache shard mutex will be held when the ShouldEvict and
			// OnEvicted callbacks are invoked.
			//
			// On ShouldEvict, compare the total size of the cache in bytes to the
			// configured maxBytes. We also insist that at least one entry remains
			// in the cache to prevent the case where a very large entry isn't able
			// to be cached at all.
			ShouldEvict: func(n int, k, v interface{}) bool {
				return s.bytes > maxBytes && n >= 1
			},
			OnEvicted: func(k, v interface{}) {
				ent := v.(*raftpb.Entry)
				s.bytes -= uint64(ent.Size())
			},
		})
		s.cache = cache
	}

	return rec
}

func (rec *raftEntryCache) makeCacheEntry(key entryCacheKey, value raftpb.Entry) *cache.Entry {
	alloc := struct {
		key   entryCacheKey
		value raftpb.Entry
		entry cache.Entry
	}{
		key:   key,
		value: value,
	}
	alloc.entry.Key = &alloc.key
	alloc.entry.Value = &alloc.value
	return &alloc.entry
}

func (rec *raftEntryCache) getShard(rangeID roachpb.RangeID) *raftEntryCacheShard {
	return &rec.shards[rangeID%roachpb.RangeID(len(rec.shards))]
}

// addEntries adds the slice of raft entries, using the range ID and the
// entry indexes as each cached entry's key.
func (rec *raftEntryCache) addEntries(rangeID roachpb.RangeID, ents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	s := rec.getShard(rangeID)
	s.Lock()
	defer s.Unlock()

	for _, e := range ents {
		s.bytes += uint64(e.Size())
		entry := rec.makeCacheEntry(entryCacheKey{RangeID: rangeID, Index: e.Index}, e)
		s.cache.AddEntry(entry)
	}
}

// getEntries returns entries between [lo, hi) for specified range.
// If any entries are returned for the specified indexes, they will
// start with index lo and proceed sequentially without gaps until
// 1) all entries exclusive of hi are fetched, 2) > maxBytes of
// entries data is fetched, or 3) a cache miss occurs.
func (rec *raftEntryCache) getEntries(
	ents []raftpb.Entry, rangeID roachpb.RangeID, lo, hi, maxBytes uint64,
) ([]raftpb.Entry, uint64, uint64) {
	s := rec.getShard(rangeID)
	s.RLock()
	defer s.RUnlock()
	var bytes uint64
	nextIndex := lo

	fromKey := &entryCacheKey{RangeID: rangeID, Index: lo}
	toKey := &entryCacheKey{RangeID: rangeID, Index: hi}
	s.cache.DoRange(func(k, v interface{}) bool {
		ecKey := k.(*entryCacheKey)
		if ecKey.Index != nextIndex {
			return true
		}
		ent := v.(*raftpb.Entry)
		ents = append(ents, *ent)
		bytes += uint64(ent.Size())
		nextIndex++
		if maxBytes > 0 && bytes > maxBytes {
			return true
		}
		return false
	}, fromKey, toKey)

	return ents, bytes, nextIndex
}

// delEntries deletes entries between [lo, hi) for specified range.
func (rec *raftEntryCache) delEntries(rangeID roachpb.RangeID, lo, hi uint64) {
	s := rec.getShard(rangeID)
	s.Lock()
	defer s.Unlock()
	if lo >= hi {
		return
	}
	var keys []*entryCacheKey
	fromKey := &entryCacheKey{RangeID: rangeID, Index: lo}
	toKey := &entryCacheKey{RangeID: rangeID, Index: hi}
	s.cache.DoRange(func(k, v interface{}) bool {
		keys = append(keys, k.(*entryCacheKey))
		return false
	}, fromKey, toKey)

	for _, k := range keys {
		s.cache.Del(k)
	}
}

// clearTo clears the entries in the cache for specified range up to,
// but not including the specified index.
func (rec *raftEntryCache) clearTo(rangeID roachpb.RangeID, index uint64) {
	rec.delEntries(rangeID, 0, index)
}
