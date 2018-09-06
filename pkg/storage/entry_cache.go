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

package storage

import (
	"github.com/biogo/store/llrb"
	"go.etcd.io/etcd/raft/raftpb"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

const defaultEntryCacheFreeListSize = 16

// entryCacheFreeList represents a free list of raftEntryCache cache.Entry
// objects. The free list is initially empty and is populated as entries are
// freed.
//
// A freelist is used here instead of a sync.Pool because all access to the
// freelist is protected by the raftEntryCache lock so it doesn't need its
// own locking. This allows us to avoid the added synchronization cost of
// a sync.Pool.
type entryCacheFreeList []*cache.Entry

func makeEntryCacheFreeList(size int) entryCacheFreeList {
	return make(entryCacheFreeList, 0, size)
}

func (f *entryCacheFreeList) newEntry(key entryCacheKey, value raftpb.Entry) *cache.Entry {
	i := len(*f) - 1
	if i < 0 {
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
	e := (*f)[i]
	(*f)[i] = nil
	(*f) = (*f)[:i]

	*e.Key.(*entryCacheKey) = key
	*e.Value.(*raftpb.Entry) = value
	return e
}

func (f *entryCacheFreeList) freeEntry(e *cache.Entry) {
	if len(*f) < cap(*f) {
		*e.Key.(*entryCacheKey) = entryCacheKey{}
		*e.Value.(*raftpb.Entry) = raftpb.Entry{}
		*f = append(*f, e)
	}
}

// A raftEntryCache maintains a global cache of Raft group log entries. The
// cache mostly prevents unnecessary reads from disk of recently-written log
// entries between log append and application to the FSM.
//
// This cache stores entries with sideloaded proposals inlined (i.e. ready to
// be sent to followers).
type raftEntryCache struct {
	syncutil.RWMutex                     // protects Cache for concurrent access
	bytes            uint64              // total size of the cache in bytes
	cache            *cache.OrderedCache // LRU cache of log entries, keyed by rangeID / log index
	freeList         entryCacheFreeList  // used to avoid allocations on insertion
}

// newRaftEntryCache returns a new RaftEntryCache with the given
// maximum size in bytes.
func newRaftEntryCache(maxBytes uint64) *raftEntryCache {
	rec := &raftEntryCache{
		cache:    cache.NewOrderedCache(cache.Config{Policy: cache.CacheLRU}),
		freeList: makeEntryCacheFreeList(defaultEntryCacheFreeListSize),
	}
	// The raft entry cache mutex will be held when the ShouldEvict
	// and OnEvictedEntry callbacks are invoked.
	//
	// On ShouldEvict, compare the total size of the cache in bytes to the
	// configured maxBytes. We also insist that at least one entry remains
	// in the cache to prevent the case where a very large entry isn't able
	// to be cached at all.
	rec.cache.Config.ShouldEvict = func(n int, k, v interface{}) bool {
		return rec.bytes > maxBytes && n >= 1
	}
	rec.cache.Config.OnEvictedEntry = func(e *cache.Entry) {
		ent := e.Value.(*raftpb.Entry)
		rec.bytes -= uint64(ent.Size())
		rec.freeList.freeEntry(e)
	}

	return rec
}

// addEntries adds the slice of raft entries, using the range ID and the
// entry indexes as each cached entry's key.
func (rec *raftEntryCache) addEntries(rangeID roachpb.RangeID, ents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	rec.Lock()
	defer rec.Unlock()

	for _, e := range ents {
		key := entryCacheKey{RangeID: rangeID, Index: e.Index}
		entry := rec.freeList.newEntry(key, e)
		rec.cache.AddEntry(entry)
		rec.bytes += uint64(e.Size())
	}
}

// getTerm returns the term for the specified index and true for the second
// return value. If the index is not present in the cache, false is returned.
func (rec *raftEntryCache) getTerm(rangeID roachpb.RangeID, index uint64) (uint64, bool) {
	rec.RLock()
	defer rec.RUnlock()

	fromKey := entryCacheKey{RangeID: rangeID, Index: index}
	k, v, ok := rec.cache.Ceil(&fromKey)
	if !ok {
		return 0, false
	}
	ecKey := k.(*entryCacheKey)
	if ecKey.RangeID != rangeID || ecKey.Index != index {
		return 0, false
	}
	ent := v.(*raftpb.Entry)
	return ent.Term, true
}

// getEntries returns entries between [lo, hi) for specified range.
// If any entries are returned for the specified indexes, they will
// start with index lo and proceed sequentially without gaps until
// 1) all entries exclusive of hi are fetched, 2) fetching another entry
// would add up to more than maxBytes of data, or 3) a cache miss occurs.
// The returned size reflects the size of the returned entries.
func (rec *raftEntryCache) getEntries(
	ents []raftpb.Entry, rangeID roachpb.RangeID, lo, hi, maxBytes uint64,
) (_ []raftpb.Entry, size uint64, nextIndex uint64, exceededMaxBytes bool) {
	rec.RLock()
	defer rec.RUnlock()
	var bytes uint64
	nextIndex = lo

	fromKey := entryCacheKey{RangeID: rangeID, Index: lo}
	toKey := entryCacheKey{RangeID: rangeID, Index: hi}
	rec.cache.DoRange(func(k, v interface{}) bool {
		ecKey := k.(*entryCacheKey)
		if ecKey.Index != nextIndex {
			return true
		}
		ent := v.(*raftpb.Entry)
		size := uint64(ent.Size())
		if bytes+size > maxBytes {
			exceededMaxBytes = true
			if len(ents) > 0 {
				return true
			}
		}
		nextIndex++
		bytes += size
		ents = append(ents, *ent)
		return exceededMaxBytes
	}, &fromKey, &toKey)

	return ents, bytes, nextIndex, exceededMaxBytes
}

// delEntries deletes entries between [lo, hi) for specified range.
func (rec *raftEntryCache) delEntries(rangeID roachpb.RangeID, lo, hi uint64) {
	rec.Lock()
	defer rec.Unlock()
	if lo >= hi {
		return
	}
	var cacheEnts []*cache.Entry
	fromKey := entryCacheKey{RangeID: rangeID, Index: lo}
	toKey := entryCacheKey{RangeID: rangeID, Index: hi}
	rec.cache.DoRangeEntry(func(e *cache.Entry) bool {
		cacheEnts = append(cacheEnts, e)
		return false
	}, &fromKey, &toKey)

	for _, e := range cacheEnts {
		rec.cache.DelEntry(e)
	}
}

// clearTo clears the entries in the cache for specified range up to,
// but not including the specified index.
func (rec *raftEntryCache) clearTo(rangeID roachpb.RangeID, index uint64) {
	rec.delEntries(rangeID, 0, index)
}
