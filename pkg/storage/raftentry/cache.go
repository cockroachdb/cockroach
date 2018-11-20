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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
)

// Cache is a specialized data structure for storing deserialized raftpb.Entry
// values tailored to the access pattersn of the storage package.
// Cache is safe for concurrent read acess to individual ranges.
// Concurrent writes to different ranges are permitted but writes concurrent
// with other accesses to the same range are not.
// Writes which are not contiguous with existing entries in the cache for a
// given range will be dropped. Overlapping entries which are added will
// replace existing values.
type Cache struct {
	metrics  Metrics
	maxBytes int

	mu    sync.RWMutex
	lru   list.List
	parts map[roachpb.RangeID]*partition
	size  int
	bytes int
}

type partition struct {
	ringBuf // implements rangeCache, embedded to avoid interface and allocation
	id      roachpb.RangeID
	el      *list.Element
	bytes   int // protected by Cache.mu to prevent races during evictions
}

// rangeCache represents the interface that the partition uses
type rangeCache interface {
	add(ent []raftpb.Entry) (bytesAdded, entriesAdded int)
	clear(hi uint64) (bytesRemoved, entriesRemoved int)
	get(index uint64) (raftpb.Entry, bool)
	scan(ents []raftpb.Entry, id roachpb.RangeID, lo, hi, maxBytes uint64) (
		_ []raftpb.Entry, bytes uint64, nextIdx uint64, exceededMaxBytes bool)
	length() int
}

// ringBuf implements rangeCache
var _ rangeCache = (*ringBuf)(nil)

// NewCache creates a cache with a max size.
func NewCache(maxBytes uint64) *Cache {
	return &Cache{
		maxBytes: int(maxBytes),
		metrics:  makeMetrics(),
		parts:    map[roachpb.RangeID]*partition{},
	}
}

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
	c.bytes += bytesGuessed
	// get p again in case we just evicted everything
	p := c.getPartLocked(id, true /* create */, true /* recordUse */)
	c.mu.Unlock()

	bytesAdded, entriesAdded := p.add(ents)
	c.recordUpdate(p, bytesAdded, bytesGuessed, entriesAdded)
}

// Clear removes all entries on the given range with index less than hi.
func (c *Cache) Clear(id roachpb.RangeID, hi uint64) {
	c.mu.Lock()
	p := c.getPartLocked(id, false /* create */, false /* recordUse */)
	c.mu.Unlock()

	if p == nil {
		return
	}
	bytesRemoved, entriesRemoved := p.clear(hi)
	c.recordUpdate(p, -1*bytesRemoved, 0, -1*entriesRemoved)
}

// Get returns the entry for the specified index and true for the second return
// value. If the index is not present in the cache, false is returned.
func (c *Cache) Get(id roachpb.RangeID, idx uint64) (e raftpb.Entry, ok bool) {
	c.metrics.Accesses.Inc(1)
	c.mu.Lock()
	p := c.getPartLocked(id, false /* create */, true /* recordUse */)
	c.mu.Unlock()

	if p == nil {
		return e, false
	}
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
	c.mu.Unlock()

	if p == nil {
		return ents, 0, lo, false
	}
	ents, bytes, nextIdx, exceededMaxBytes = p.scan(ents, id, lo, hi, maxBytes)
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
			newPart := &partition{
				id: id,
			}
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

func (c *Cache) evictLocked(toAdd int) {
	for c.bytes+toAdd > c.maxBytes {
		p := c.lru.Remove(c.lru.Back()).(*partition)
		c.bytes -= p.bytes
		c.size -= p.length()
		delete(c.parts, p.id)
	}
}

func (c *Cache) recordUpdate(p *partition, bytesAdded, bytesGuessed, entriesAdded int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delta := int(bytesAdded) - int(bytesGuessed)
	c.bytes += delta        // delta should always be negative so no need to evict
	if c.parts[p.id] == p { // wasn't evicted
		c.size += entriesAdded
		p.bytes += bytesAdded
	}
	if p.length() == 0 {
		delete(c.parts, p.id)
	}
	c.updateGauges()
}

func (c *Cache) updateGauges() {
	c.metrics.Bytes.Update(int64(c.bytes))
	c.metrics.Size.Update(int64(c.size))
}

// analyzeEntries calculates the size in bytes of ents as well and ensures that
// the entries in ents have contiguous indices.
func analyzeEntries(ents []raftpb.Entry) (size int) {
	var prevIndex uint64
	for i, e := range ents {
		if i != 0 && e.Index != prevIndex+1 {
			panic(errors.Errorf("invalid non-contiguous set of entries %d and %d", prevIndex, e.Index))
		}
		prevIndex = e.Index
		size += e.Size()
	}
	return
}
