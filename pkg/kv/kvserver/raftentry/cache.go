// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package raftentry provides a cache for entries to avoid extra
// deserializations.
package raftentry

import (
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Cache is a specialized data structure for storing deserialized raftpb.Entry
// values tailored to the access patterns of the storage package.
// Cache is safe for concurrent access.
type Cache struct {
	metrics  Metrics
	maxBytes int32

	// accessed with atomics
	bytes   int32
	entries int32

	mu    syncutil.Mutex
	lru   partitionList
	parts map[roachpb.RangeID]*partition
}

// Design
//
// Cache is designed to be a shared store-wide object which incurs low
// contention for operations on different ranges while maintaining a global
// memory policy. This is achieved through the use of a two-level locking scheme.
// Cache.mu is acquired to access any data in the cache (Add, Clear, Get, or
// Scan) in order to locate the partition for the operation and update the LRU
// state. In the case of Add operations, partitions are lazily constructed
// under the lock. In addition to partition location, Add operations record the
// maximal amount of space that the write may add to the cache, accepting that
// in certain cases, less space may actually be consumed leading to unnecessary
// evictions. Once a partition has been located (or not found) and LRU state has
// been appropriately modified, operations release Cache.mu and proceed by
// operating on the partition under its RWMutex.
//
// This disjoint, two-level locking pattern permits the "anomaly" whereby a
// partition may be accessed and evicted concurrently. This condition is made
// safe in the implementation by using atomics to update the cache bookkeeping
// and by taking care to not mutate the partition's cache state upon eviction.
// As noted above, the Cache and partition's bookkeeping is updated with an
// initial estimate of the byte size of an addition while holding Cache.mu.
// Because empty additions are elided, this initial bookkeeping guarantees that
// the cacheSize of partition is non-zero while an Add operation proceeds unless
// the partition has been evicted. The updated value of partition.size is
// recorded before releasing Cache.mu. When a partition mutation operation
// concludes the Cache's stats need to be updated such that they reflect the new
// reality. This update (Cache.recordUpdate) is mediated through the use of an
// atomic compare and swap operation on partition.size. If the operation
// succeeds, then we know that future evictions of this partition will see the
// new updated partition.size and so any delta from what was optimistically
// recorded in the Cache stats should be updated (using atomics, see
// add(Bytes|Entries)). If the operation fails, then we know that any change
// just made to the partition are no longer stored in the cache and thus the
// Cache stats shall not change.
//
// This approach admits several undesirable conditions, fortunately they aren't
// practical concerns.
//
//   1) Evicted partitions are reclaimed asynchronously only after operations
//      concurrent with evictions complete.
//   2) Memory reuse with object pools is difficult.

type partition struct {
	id roachpb.RangeID

	mu      syncutil.RWMutex
	ringBuf // implements rangeCache, embedded to avoid interface and allocation

	size cacheSize // accessed with atomics

	next, prev *partition // accessed under Cache.mu
}

const partitionSize = int32(unsafe.Sizeof(partition{}))

// rangeCache represents the interface that the partition uses.
// It is never explicitly used but a new implementation to replace ringBuf must
// implement the below interface.
type rangeCache interface {
	add(ent []raftpb.Entry) (bytesAdded, entriesAdded int32)
	truncateFrom(lo uint64) (bytesRemoved, entriesRemoved int32)
	clearTo(hi uint64) (bytesRemoved, entriesRemoved int32)
	get(index uint64) (raftpb.Entry, bool)
	scan(ents []raftpb.Entry, lo, hi, maxBytes uint64) (
		_ []raftpb.Entry, bytes uint64, nextIdx uint64, exceededMaxBytes bool)
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

// Drop drops all cached entries associated with the specified range.
func (c *Cache) Drop(id roachpb.RangeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.getPartLocked(id, false /* create */, false /* recordUse */)
	if p != nil {
		c.updateGauges(c.evictPartitionLocked(p))
	}
}

// Add inserts ents into the cache. If truncate is true, the method also removes
// all entries with indices equal to or greater than the indices of the entries
// provided. ents is expected to consist of entries with a contiguous sequence
// of indices.
func (c *Cache) Add(id roachpb.RangeID, ents []raftpb.Entry, truncate bool) {
	if len(ents) == 0 {
		return
	}
	bytesGuessed := analyzeEntries(ents)
	add := bytesGuessed <= c.maxBytes
	if !add {
		bytesGuessed = 0
	}

	c.mu.Lock()
	// Get p and move the partition to the front of the LRU.
	p := c.getPartLocked(id, add /* create */, true /* recordUse */)
	if bytesGuessed > 0 {
		c.evictLocked(bytesGuessed)
		if len(c.parts) == 0 { // Get p again if we evicted everything.
			p = c.getPartLocked(id, true /* create */, false /* recordUse */)
		}
		// Use the atomic (load|set)Size partition methods to avoid a race condition
		// on p.size and to ensure that p.size.bytes() reflects the number of bytes
		// in c.bytes associated with p in the face of concurrent updates due to calls
		// to c.recordUpdate.
		for {
			prev := p.loadSize()
			if p.setSize(prev, prev.add(bytesGuessed, 0)) {
				break
			}
		}
	}
	c.mu.Unlock()
	if p == nil {
		// The partition did not exist and we did not create it.
		// Only possible if !add.
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	var bytesAdded, entriesAdded, bytesRemoved, entriesRemoved int32
	// We truncate (if requested) before adding. This can lead to "wasted"
	// work where we're zeroing out entries we would repopulate anyway, but
	// past experience shows that it's best to keep this code as simple as
	// possible (see #61990).
	if truncate {
		// Note that ents[0].Index may not even be in the cache
		// at this point. `truncateFrom` will still remove any entries
		// it may have at indexes >= truncIdx, as instructed.
		truncIdx := ents[0].Index
		bytesRemoved, entriesRemoved = p.truncateFrom(truncIdx)
	}
	if add {
		bytesAdded, entriesAdded = p.add(ents)
	}
	c.recordUpdate(p, bytesAdded-bytesRemoved, bytesGuessed, entriesAdded-entriesRemoved)
}

// Clear removes all entries on the given range with index less than hi.
func (c *Cache) Clear(id roachpb.RangeID, hi uint64) {
	c.mu.Lock()
	p := c.getPartLocked(id, false /* create */, false /* recordUse */)
	if p == nil {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	bytesRemoved, entriesRemoved := p.clearTo(hi)
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
	p.mu.RLock()
	defer p.mu.RUnlock()
	e, ok = p.get(idx)
	if ok {
		c.metrics.Hits.Inc(1)
	}
	return e, ok
}

// Scan returns entries between [lo, hi) for specified range. If any entries are
// returned for the specified indices, they will start with index lo and proceed
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
	p.mu.RLock()
	defer p.mu.RUnlock()

	ents, bytes, nextIdx, exceededMaxBytes = p.scan(ents, lo, hi, maxBytes)
	if nextIdx == hi || exceededMaxBytes {
		// Only consider an access a "hit" if it returns all requested entries or
		// stops short because of a maximum bytes limit.
		c.metrics.Hits.Inc(1)
	}
	return ents, bytes, nextIdx, exceededMaxBytes
}

func (c *Cache) getPartLocked(id roachpb.RangeID, create, recordUse bool) *partition {
	part := c.parts[id]
	if create && part == nil {
		part = c.lru.pushFront(id)
		c.parts[id] = part
		c.addBytes(partitionSize)
	}
	if recordUse && part != nil {
		c.lru.moveToFront(part)
	}
	return part
}

// evictLocked adds toAdd to the current cache byte size and evicts partitions
// until the cache is below the maxBytes threshold. toAdd must be smaller than
// c.maxBytes.
func (c *Cache) evictLocked(toAdd int32) {
	bytes := c.addBytes(toAdd)
	for bytes > c.maxBytes && len(c.parts) > 0 {
		bytes, _ = c.evictPartitionLocked(c.lru.back())
	}
}

func (c *Cache) evictPartitionLocked(p *partition) (updatedBytes, updatedEntries int32) {
	delete(c.parts, p.id)
	c.lru.remove(p)
	pBytes, pEntries := p.evict()
	return c.addBytes(-1 * pBytes), c.addEntries(-1 * pEntries)
}

// recordUpdate adjusts the partition and cache bookkeeping to account for the
// changes which actually occurred in an update relative to the guess made
// before the update.
func (c *Cache) recordUpdate(p *partition, bytesAdded, bytesGuessed, entriesAdded int32) {
	// This method is always called while p.mu is held.
	// The below code takes care to ensure that all bytes in c due to p are
	// updated appropriately.

	// NB: The loop and atomics are used because p.size can be modified
	// concurrently to calls to recordUpdate. In all cases where p.size is updated
	// outside of this function occur while c.mu is held inside of c.Add. These
	// occur when either:
	//
	//   1) a new write adds its guessed write size to p
	//   2) p is evicted to make room for a write
	//
	// Thus p.size is either increasing or becomes evicted while we attempt to
	// record the update to p. Once p is evicted it stays evicted forever.
	// These facts combine to ensure that p.size never becomes negative from the
	// below call to add.

	delta := bytesAdded - bytesGuessed
	for {
		curSize := p.loadSize()
		if curSize == evicted {
			return
		}
		newSize := curSize.add(delta, entriesAdded)
		if updated := p.setSize(curSize, newSize); updated {
			c.updateGauges(c.addBytes(delta), c.addEntries(entriesAdded))
			return
		}
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

var initialSize = newCacheSize(partitionSize, 0)

func newPartition(id roachpb.RangeID) *partition {
	return &partition{
		id:   id,
		size: initialSize,
	}
}

const evicted cacheSize = 0

func (p *partition) evict() (bytes, entries int32) {
	// Atomically setting size to evicted signals that the partition has been
	// evicted. Changes to p which happen concurrently with the eviction should
	// not be reflected in the Cache. The loop in recordUpdate detects the action
	// of this call.
	cs := p.loadSize()
	for !p.setSize(cs, evicted) {
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

// analyzeEntries calculates the size in bytes of ents and ensures that the
// entries in ents have contiguous indices.
func analyzeEntries(ents []raftpb.Entry) (size int32) {
	var prevIndex uint64
	var prevTerm uint64
	for i, e := range ents {
		if i != 0 && e.Index != prevIndex+1 {
			panic(errors.AssertionFailedf("invalid non-contiguous set of entries %d and %d", prevIndex, e.Index))
		}
		if i != 0 && e.Term < prevTerm {
			err := errors.AssertionFailedf("term regression idx %d: %d -> %d", prevIndex, prevTerm, e.Term)
			panic(err)
		}
		prevIndex = e.Index
		prevTerm = e.Term
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

// add constructs a new cacheSize with signed additions to entries and bytes.
// It is illegal to use values that will make cs negative.
func (cs cacheSize) add(bytes, entries int32) cacheSize {
	return newCacheSize(cs.bytes()+bytes, cs.entries()+entries)
}

// entryList is a double-linked circular list of *partition elements. The code
// is derived from the stdlib container/list but customized to partition in
// order to avoid a separate allocation for every element.
type partitionList struct {
	root partition
}

func (l *partitionList) lazyInit() {
	if l.root.next == nil {
		l.root.next = &l.root
		l.root.prev = &l.root
	}
}

func (l *partitionList) pushFront(id roachpb.RangeID) *partition {
	l.lazyInit()
	return l.insert(newPartition(id), &l.root)
}

func (l *partitionList) moveToFront(p *partition) {
	l.insert(l.remove(p), &l.root)
}

func (l *partitionList) insert(e, at *partition) *partition {
	n := at.next
	at.next = e
	e.prev = at
	e.next = n
	n.prev = e
	return e
}

func (l *partitionList) back() *partition {
	if l.root.prev == nil || l.root.prev == &l.root {
		return nil
	}
	return l.root.prev
}

func (l *partitionList) remove(e *partition) *partition {
	if e == &l.root {
		panic("cannot remove root list node")
	}
	if e.next != nil {
		e.prev.next = e.next
		e.next.prev = e.prev
		e.next = nil // avoid memory leaks
		e.prev = nil // avoid memory leaks
	}
	return e
}
