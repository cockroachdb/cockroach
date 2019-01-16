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

package querycache

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// C is a query cache, keyed on SQL statement strings (which can contain
// placeholders).
type C struct {
	totalMem int64

	mu struct {
		syncutil.Mutex

		availableMem int64

		// Sentinel list entries. All entries are part of either the used or the
		// free circular list. Any entry in the used list has a corresponding entry
		// in the map. The used list is in MRU order.
		used, free entry

		// Map with an entry for each used entry.
		m map[string]*entry
	}
}

// avgCachedSize is used to preallocate the number of "slots" in the cache.
// Specifically, the cache will be able to store at most
// (<size> / avgCachedSize) queries, even if their memory usage is small.
const avgCachedSize = 1024

// We disallow very large queries from being added to the cache.
const maxCachedSize = 128 * 1024

// CachedData is the data associated with a cache entry.
type CachedData struct {
	SQL  string
	Memo *memo.Memo
	// PrepareMetadata is set for prepare queries. In this case the memo contains
	// unassigned placeholders. For non-prepared queries, it is nil.
	PrepareMetadata *sqlbase.PrepareMetadata
}

func (cd *CachedData) memoryEstimate() int64 {
	res := int64(len(cd.SQL)) + cd.Memo.MemoryEstimate()
	if cd.PrepareMetadata != nil {
		res += cd.PrepareMetadata.MemoryEstimate()
	}
	return res
}

// entry in a circular linked list.
type entry struct {
	CachedData

	// Linked list pointers.
	prev, next *entry
}

// clear resets the CachedData in the entry.
func (e *entry) clear() {
	e.CachedData = CachedData{}
}

// remove removes the entry from the linked list it is part of.
func (e *entry) remove() {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.prev = nil
	e.next = nil
}

func (e *entry) insertAfter(a *entry) {
	b := a.next

	e.prev = a
	e.next = b

	a.next = e
	b.prev = e
}

// New creates a query cache of the given size.
func New(memorySize int64) *C {
	if memorySize < avgCachedSize {
		memorySize = avgCachedSize
	}
	numEntries := memorySize / avgCachedSize
	c := &C{totalMem: memorySize}
	c.mu.availableMem = memorySize
	c.mu.m = make(map[string]*entry, numEntries)
	entries := make([]entry, numEntries)
	// The used list is empty.
	c.mu.used.next = &c.mu.used
	c.mu.used.prev = &c.mu.used
	// Make a linked list of entries, starting with the sentinel.
	c.mu.free.next = &entries[0]
	c.mu.free.prev = &entries[numEntries-1]
	for i := range entries {
		if i > 0 {
			entries[i].prev = &entries[i-1]
		} else {
			entries[i].prev = &c.mu.free
		}
		if i+1 < len(entries) {
			entries[i].next = &entries[i+1]
		} else {
			entries[i].next = &c.mu.free
		}
	}
	return c
}

// Find returns the entry for the given query, if it is in the cache.
//
// If any cached data needs to be updated, it must be done via Add. In
// particular, PrepareMetadata in the returned CachedData must not be modified.
func (c *C) Find(sql string) (_ CachedData, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e := c.mu.m[sql]
	if e == nil {
		return CachedData{}, false
	}
	// Move the entry to the front of the used list.
	e.remove()
	e.insertAfter(&c.mu.used)
	return e.CachedData, true
}

// Add adds an entry to the cache (possibly evicting some other entry). If the
// cache already has a corresponding entry for d.SQL, it is updated.
// Note: d.PrepareMetadata cannot be modified once this method is called.
func (c *C) Add(d *CachedData) {
	mem := d.memoryEstimate()
	if d.SQL == "" || mem > maxCachedSize || mem > c.totalMem {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.mu.m[d.SQL]
	if ok {
		// The query already exists in the cache.
		e.remove()
		c.mu.availableMem += e.memoryEstimate()
	} else {
		// Get an entry to use for this query.
		e = c.getEntry()
		c.mu.m[d.SQL] = e
	}

	e.CachedData = *d

	// Evict more entries if necessary.
	c.makeSpace(mem)
	c.mu.availableMem -= mem

	// Insert the entry at the front of the used list.
	e.insertAfter(&c.mu.used)
}

// makeSpace evicts entries from the used list until we have enough free space.
func (c *C) makeSpace(needed int64) {
	for c.mu.availableMem < needed {
		// Evict entries as necessary, putting them in the free list.
		c.evict().insertAfter(&c.mu.free)
	}
}

// Evicts the last item in the used list.
func (c *C) evict() *entry {
	e := c.mu.used.prev
	if e == &c.mu.used {
		panic("no more used entries")
	}
	e.remove()
	c.mu.availableMem += e.memoryEstimate()
	delete(c.mu.m, e.SQL)
	e.clear()

	return e
}

// getEntry returns an entry that can be used for adding a new query to the
// cache. If there are free entries, one is returned; otherwise, a used entry is
// evicted.
func (c *C) getEntry() *entry {
	if e := c.mu.free.next; e != &c.mu.free {
		e.remove()
		return e
	}
	// No free entries, we must evict an entry.
	return c.evict()
}

// Clear removes all the entries from the cache.
func (c *C) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear the map.
	for sql, e := range c.mu.m {

		c.mu.availableMem += e.memoryEstimate()
		delete(c.mu.m, sql)
		e.remove()
		e.clear()
		e.insertAfter(&c.mu.free)
	}
}

// Purge removes the entry for the given query, if it exists.
func (c *C) Purge(sql string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if e := c.mu.m[sql]; e != nil {
		c.mu.availableMem += e.memoryEstimate()
		delete(c.mu.m, sql)
		e.clear()
		e.remove()
		e.insertAfter(&c.mu.free)
	}
}

// check performs various assertions on the internal consistency of the cache
// structures. Used by testing code.
func (c *C) check() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Verify that all entries in the used list have a corresponding entry in the
	// map, and that the memory accounting adds up.
	numUsed := 0
	memUsed := int64(0)
	for e := c.mu.used.next; e != &c.mu.used; e = e.next {
		numUsed++
		memUsed += e.memoryEstimate()
		if e.SQL == "" {
			panic(fmt.Sprintf("used entry with empty SQL"))
		}
		if me, ok := c.mu.m[e.SQL]; !ok {
			panic(fmt.Sprintf("used entry %s not in map", e.SQL))
		} else if e != me {
			panic(fmt.Sprintf("map entry for %s doesn't match used entry", e.SQL))
		}
	}

	if numUsed != len(c.mu.m) {
		panic(fmt.Sprintf("map length %d doesn't match used list size %d", len(c.mu.m), numUsed))
	}

	if memUsed+c.mu.availableMem != c.totalMem {
		panic(fmt.Sprintf(
			"memory usage doesn't add up: used=%d available=%d total=%d",
			memUsed, c.mu.availableMem, c.totalMem,
		))
	}
}
