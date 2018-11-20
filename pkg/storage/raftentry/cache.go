// Copyright 2018 The Cockroach Authors.
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

// Package raftentry provides a cache for entries to avoid extra
// deserializations.
package raftentry

import (
	"container/list"
	"math"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
)

// Cache is a specialized data structure for storing deserialized raftpb.Entry
// values tailored to the access patterns of the storage package.
// Cache is safe for concurrent access.
type Cache struct {
	metrics  Metrics
	maxBytes int32
	bytes    int32
	entries  int32

	mu    syncutil.Mutex
	lru   list.List
	parts map[roachpb.RangeID]*partition
}

type partition struct {
	ringBuf          // implements rangeCache, embedded to avoid interface and allocation
	syncutil.RWMutex // must be acquired while holding Cache.mu
	id               roachpb.RangeID
	el               *list.Element
	size             cacheSize
}

// rangeCache represents the interface that the partition uses.
// It is never explicitly used but a new implementation to replace ringBuf must
// implement the below interface.
type rangeCache interface {
	add(ent []raftpb.Entry) (bytesAdded, entriesAdded int32)
	clear(hi uint64) (bytesRemoved, entriesRemoved int32)
	get(index uint64) (raftpb.Entry, bool)
	scan(ents []raftpb.Entry, lo, hi, maxBytes uint64) (
		_ []raftpb.Entry, bytes uint64, nextIdx uint64, exceededMaxBytes bool)
	length() int
}

// ringBuf implements rangeCache.
var _ rangeCache = (*ringBuf)(nil)

// NewCache creates a cache with a max size.
// Size must be less than math.MaxInt32.
func NewCache(maxBytes uint64) *Cache {
	if maxBytes > math.MaxInt32 {
		maxBytes = math.MaxInt32
	}
	return &Cache{
		maxBytes: int32(maxBytes),
		metrics:  makeMetrics(),
		parts:    map[roachpb.RangeID]*partition{},
	}
}

// Metrics returns a struct which contains metrics for the raft entry cache.
func (c *Cache) Metrics() Metrics {
	return c.metrics
}

// Add inserts ents into the cache.
func (c *Cache) Add(id roachpb.RangeID, ents []raftpb.Entry) {
	bytesGuessed := analyzeEntries(ents)
	if bytesGuessed == 0 || bytesGuessed > c.maxBytes {
		return
	}
	c.mu.Lock()
	// move this partition up in the LRU
	_ = c.getPartLocked(id, false /* create */, true /* recordUse */)
	c.evictLocked(bytesGuessed)
	// get p again in case we just evicted everything
	p := c.getPartLocked(id, true /* create */, false /* recordUse */)
	p.size = p.size.add(bytesGuessed, 0)
	orig := p.size
	p.Lock()
	defer p.Unlock()
	c.mu.Unlock()

	bytesAdded, entriesAdded := p.add(ents)
	c.recordUpdate(p, orig, bytesAdded, bytesGuessed, entriesAdded)
}

// Clear removes all entries on the given range with index less than hi.
func (c *Cache) Clear(id roachpb.RangeID, hi uint64) {
	c.mu.Lock()
	p := c.getPartLocked(id, false /* create */, false /* recordUse */)
	if p == nil {
		c.mu.Unlock()
		return
	}
	p.Lock()
	defer p.Unlock()
	orig := p.size
	c.mu.Unlock()

	bytesRemoved, entriesRemoved := p.clear(hi)
	c.recordUpdate(p, orig, -1*bytesRemoved, 0, -1*entriesRemoved)
}

// Get returns the entry for the specified index and true for the second return
// value. If the index is not present in the cache, false is returned.
func (c *Cache) Get(id roachpb.RangeID, idx uint64) (e raftpb.Entry, ok bool) {
	c.metrics.Accesses.Inc(1)
	c.mu.Lock()
	p := c.getPartLocked(id, false /* create */, true /* recordUse */)
	if p == nil {
		c.mu.Unlock()
		return e, false
	}
	p.RLock()
	defer p.RUnlock()
	c.mu.Unlock()

	if e, ok = p.get(idx); ok {
		c.metrics.Hits.Inc(1)
	}
	return
}

// Scan returns entries between [lo, hi) for specified range. If any entries are
// returned for the specified indexes, they will start with index lo and proceed
// sequentially without gaps until 1) all entries exclusive of hi are fetched,
// 2) fetching another entry would add up to more than maxBytes of data, or 3) a
// cache miss occurs. The returned size reflects the size of the returned
// entries.
func (c *Cache) Scan(
	ents []raftpb.Entry, id roachpb.RangeID, lo, hi, maxBytes uint64,
) (_ []raftpb.Entry, bytes uint64, nextIdx uint64, exceededMaxBytes bool) {
	c.metrics.Accesses.Inc(1)
	c.mu.Lock()
	p := c.getPartLocked(id, false /* create */, true /* recordUse */)
	if p == nil {
		c.mu.Unlock()
		return ents, 0, lo, false
	}
	p.RLock()
	defer p.RUnlock()
	c.mu.Unlock()

	ents, bytes, nextIdx, exceededMaxBytes = p.scan(ents, lo, hi, maxBytes)
	if nextIdx == hi || exceededMaxBytes {
		// We only consider an access a "hit" if it returns all requested
		// entries or stops short because of a maximum bytes limit.
		c.metrics.Hits.Inc(1)
	}
	return ents, bytes, nextIdx, exceededMaxBytes
}

func (c *Cache) getPartLocked(id roachpb.RangeID, create, recordUse bool) *partition {
	part := c.parts[id]
	if create {
		if part == nil {
			newPart := &partition{id: id}
			newPart.el = c.lru.PushFront(newPart)
			c.parts[id] = newPart
			part = newPart
		}
	}
	if recordUse && part != nil {
		c.lru.MoveToFront(part.el)
	}
	return part
}

// evictLocked adds toAdd to the current cache byte size and evicts partitions
// until the cache is below the maxBytes threshold. toAdd must be smaller than
// c.maxBytes.
func (c *Cache) evictLocked(toAdd int32) {
	bytes := c.addBytes(toAdd)
	for bytes > c.maxBytes {
		p := c.lru.Remove(c.lru.Back()).(*partition)
		pBytes, pEntries := p.evict()
		c.lru.Remove(p.el)
		delete(c.parts, p.id)
		c.addEntries(-1 * pEntries)
		bytes = c.addBytes(-1 * pBytes)
	}
}

func (c *Cache) recordUpdate(
	p *partition, orig cacheSize, bytesAdded, bytesGuessed, entriesAdded int32,
) {
	// the only way that the stats here could change is if we were evicted so
	// we'll atomically try to update the partition and if it turns out it's been
	// evicted then don't update the cache.
	delta := bytesAdded - bytesGuessed
	new := orig.add(delta, entriesAdded)
	if p.setSize(orig, new) {
		c.updateGauges(c.addBytes(delta), c.addEntries(entriesAdded))
	}
}

func (c *Cache) addBytes(toAdd int32) int32 {
	return atomic.AddInt32(&c.bytes, toAdd)
}

func (c *Cache) addEntries(toAdd int32) int32 {
	return atomic.AddInt32(&c.entries, toAdd)
}

func (c *Cache) updateGauges(bytes, entries int32) {
	c.metrics.Bytes.Update(int64(bytes))
	c.metrics.Size.Update(int64(entries))
}

func (p *partition) evict() (bytes, entries int32) {
	const evicted = 0
	cs := p.loadSize()
	for cs != evicted && !p.setSize(cs, evicted) {
		cs = p.loadSize()
	}
	return cs.bytes(), cs.entries()
}

func (p *partition) loadSize() cacheSize {
	return cacheSize(atomic.LoadUint64((*uint64)(&p.size)))
}

func (p *partition) setSize(orig, new cacheSize) bool {
	return atomic.CompareAndSwapUint64((*uint64)(&p.size), uint64(orig), uint64(new))
}

// analyzeEntries calculates the size in bytes of ents as well and ensures that
// the entries in ents have contiguous indices.
func analyzeEntries(ents []raftpb.Entry) (size int32) {
	var prevIndex uint64
	for i, e := range ents {
		if i != 0 && e.Index != prevIndex+1 {
			panic(errors.Errorf("invalid non-contiguous set of entries %d and %d", prevIndex, e.Index))
		}
		prevIndex = e.Index
		size += int32(e.Size())
	}
	return
}

// cacheSize stores int32 counters for numbers of bytes and entries in a single
// 64-bit word.
type cacheSize uint64

func newCacheSize(bytes, entries int32) cacheSize {
	return cacheSize((uint64(entries) << 32) | uint64(bytes))
}

func (cs cacheSize) entries() int32 {
	return int32(cs >> 32)
}

func (cs cacheSize) bytes() int32 {
	return int32(cs & math.MaxUint32)
}

// signed addition to entries and bytes.
// creates a new cacheSize values.
// It is illegal to use values that will make cs negative.
func (cs cacheSize) add(bytes, entries int32) cacheSize {
	return newCacheSize(cs.bytes()+bytes, cs.entries()+entries)
}
