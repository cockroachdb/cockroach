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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// C is a query cache, keyed on SQL statement strings (which can contain
// placeholders).
type C struct {
	mu struct {
		syncutil.Mutex

		// Sentinel list entry. Cache entries form a circular linked list, with the
		// most recently used entry at the front (i.e. sentinel.next). All available
		// entries are always part of the list; unused entries have SQL="".
		sentinel entry

		// Map with an entry for each used entry.
		m map[string]*entry
	}
}

// Currently we use a very crude memory management scheme: we never put in the
// cache plans that use more memory than maxCachedSize and we only allocate
// (totalCacheSize / maxCachedSize) cache entries.
// TODO(radu): improve this.
const maxCachedSize = 8192

// CachedData is the data associated with a cache entry.
type CachedData struct {
	SQL  string
	Memo *memo.Memo
}

func (cd *CachedData) memoryEstimate() int64 {
	return int64(len(cd.SQL)) + cd.Memo.MemoryEstimate()
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

// remove removes the entry from the linked list.
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
func New(memorySize int) *C {
	numEntries := memorySize / maxCachedSize
	if numEntries == 0 {
		numEntries = 1
	}
	c := &C{}
	c.mu.m = make(map[string]*entry, numEntries)
	entries := make([]entry, numEntries)
	// Make a linked list of entries, starting with the sentinel.
	c.mu.sentinel.next = &entries[0]
	c.mu.sentinel.prev = &entries[numEntries-1]
	for i := range entries {
		if i > 0 {
			entries[i].prev = &entries[i-1]
		} else {
			entries[i].prev = &c.mu.sentinel
		}
		if i+1 < len(entries) {
			entries[i].next = &entries[i+1]
		} else {
			entries[i].next = &c.mu.sentinel
		}
	}
	return c
}

// Find returns the entry for the given query, if it is in the cache.
func (c *C) Find(sql string) (_ CachedData, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	res := c.mu.m[sql]
	if res == nil {
		return CachedData{}, false
	}
	c.moveToFrontLocked(res)
	return res.CachedData, true
}

// Add adds an entry to the cache (possibly evicting some other entry). If the
// cache already has a corresponding entry for d.SQL, it is updated.
func (c *C) Add(d *CachedData) {
	if d.memoryEstimate() > maxCachedSize {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.mu.m[d.SQL]; ok {
		c.moveToFrontLocked(e)
		e.CachedData = *d
		return
	}

	// Evict the least recently used entry.
	e := c.mu.sentinel.prev
	if e.SQL != "" {
		delete(c.mu.m, e.SQL)
	}
	c.moveToFrontLocked(e)
	e.CachedData = *d
	c.mu.m[d.SQL] = e
}

// moveToFrontLocked moves the given entry to the front of the list (right after
// the sentinel). Assumes c.mu is locked.
func (c *C) moveToFrontLocked(e *entry) {
	e.remove()
	e.insertAfter(&c.mu.sentinel)
}

// Clear removes all the entries from the cache.
func (c *C) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear the map.
	for sql, e := range c.mu.m {
		e.clear()
		delete(c.mu.m, sql)
	}
}

// Purge removes the entry for the given query, if it exists.
func (c *C) Purge(sql string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e := c.mu.m[sql]
	if e == nil {
		return
	}
	delete(c.mu.m, sql)
	e.clear()
	e.remove()
	// Insert at the back of the list.
	e.insertAfter(c.mu.sentinel.prev)
}
