// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// This code largely duplicates that of UnorderedCache and baseCache, but with
// generic type parameters to avoid allocations incurred during interface{}
// conversions.

package cache

// typedCachePoolSize is the fixed size of the pre-allocated entry pool
// used by TypedUnorderedCache.
//
// TODO(ssd): Make this configurable? This is likely much smaller than will be
// useful for most users.
const typedCachePoolSize = 8

// typedEntry holds the key and value and a pointer to the linked list
// which defines the eviction ordering. This is a generic version of Entry
// that avoids interface{} boxing allocations.
type typedEntry[K comparable, V any] struct {
	Key        K
	Value      V
	next, prev *typedEntry[K, V]
	poolIdx    int // index in pool, for returning to free stack
}

// typedEntryList is a double-linked circular list of *TypedEntry elements.
// The code is derived from entryList but customized for generics.
type typedEntryList[K comparable, V any] struct {
	root typedEntry[K, V]
}

func (l *typedEntryList[K, V]) init() {
	l.root.next = &l.root
	l.root.prev = &l.root
}

func (l *typedEntryList[K, V]) back() *typedEntry[K, V] {
	return l.root.prev
}

func (l *typedEntryList[K, V]) insertAfter(e, at *typedEntry[K, V]) {
	n := at.next
	at.next = e
	e.prev = at
	e.next = n
	n.prev = e
}

func (l *typedEntryList[K, V]) remove(e *typedEntry[K, V]) *typedEntry[K, V] {
	if e == &l.root {
		panic("cannot remove root list node")
	}
	if e.next != nil {
		e.prev.next = e.next
		e.next.prev = e.prev
		e.next = nil
		e.prev = nil
	}
	return e
}

func (l *typedEntryList[K, V]) pushFront(e *typedEntry[K, V]) {
	l.insertAfter(e, &l.root)
}

func (l *typedEntryList[K, V]) moveToFront(e *typedEntry[K, V]) {
	if l.root.next == e {
		return
	}
	l.insertAfter(l.remove(e), &l.root)
}

// TypedConfig specifies the eviction policy, eviction trigger callback,
// and eviction listener callback for a TypedUnorderedCache.
type TypedConfig[K comparable, V any] struct {
	// Policy is one of the consts listed for EvictionPolicy.
	Policy EvictionPolicy

	// ShouldEvict is a callback function executed each time a new entry
	// is added to the cache. It supplies cache size, and potential
	// evictee's key and value. The function should return true if the
	// entry may be evicted; false otherwise.
	ShouldEvict func(size int, key K, value V) bool

	// OnEvicted optionally specifies a callback function to be
	// executed when an entry is purged from the cache.
	OnEvicted func(key K, value V)
}

// TypedUnorderedCache is a generic cache which supports custom eviction
// triggers and two eviction policies: LRU and FIFO. This cache uses a
// hashmap for storing elements, making it performant. Only exact lookups
// are supported.
//
// TypedUnorderedCache is not safe for concurrent access.
type TypedUnorderedCache[K comparable, V any] struct {
	config TypedConfig[K, V]
	hmap   map[K]*typedEntry[K, V]
	ll     typedEntryList[K, V]

	// TODO(ssd): Think about this more. This is a small pool of pre-allocated
	// TypedEntries, to avoid allocations. But, perhaps the user should pass us a
	// sync.Pool or something instead.
	pool    [typedCachePoolSize]typedEntry[K, V] // pre-allocated entry storage
	free    [typedCachePoolSize]int              // stack of free indices into pool
	freeLen int                                  // number of entries in free stack
}

// NewTypedUnorderedCache creates a new TypedUnorderedCache backed by a hash map.
func NewTypedUnorderedCache[K comparable, V any](
	config TypedConfig[K, V],
) *TypedUnorderedCache[K, V] {
	c := &TypedUnorderedCache[K, V]{
		config:  config,
		hmap:    make(map[K]*typedEntry[K, V], typedCachePoolSize),
		freeLen: typedCachePoolSize,
	}
	c.ll.init()
	for i := range c.pool {
		c.pool[i].poolIdx = i
		c.free[i] = i
	}
	return c
}

// Get looks up a key's value from the cache.
func (c *TypedUnorderedCache[K, V]) Get(key K) (V, bool) {
	if e, ok := c.hmap[key]; ok {
		c.access(e)
		return e.Value, true
	}
	var zero V
	return zero, false
}

// Add adds a value to the cache.
func (c *TypedUnorderedCache[K, V]) Add(key K, value V) {
	if e, ok := c.hmap[key]; ok {
		c.access(e)
		e.Value = value
		return
	}

	var e *typedEntry[K, V]
	if c.freeLen > 0 {
		// Pop from free stack.
		c.freeLen--
		idx := c.free[c.freeLen]
		e = &c.pool[idx]
	} else {
		// Pool exhausted, allocate on heap. This entry will be discarded on eviction.
		e = &typedEntry[K, V]{poolIdx: -1}
	}

	e.Key = key
	e.Value = value
	c.ll.pushFront(e)
	c.hmap[e.Key] = e

	// Evict as many elements as we can.
	for c.evict() {
	}
}

// Del removes the provided key from the cache.
func (c *TypedUnorderedCache[K, V]) Del(key K) {
	if e, ok := c.hmap[key]; ok {
		c.removeElement(e)
	}
}

// Len returns the number of items in the cache.
func (c *TypedUnorderedCache[K, V]) Len() int {
	return len(c.hmap)
}

// Clear clears all entries from the cache.
func (c *TypedUnorderedCache[K, V]) Clear() {
	if c.config.OnEvicted != nil {
		for e := c.ll.back(); e != &c.ll.root; e = e.prev {
			c.config.OnEvicted(e.Key, e.Value)
		}
	}
	c.ll.init()
	c.hmap = make(map[K]*typedEntry[K, V], typedCachePoolSize)
	// Reset pool entries and free stack.
	var zeroEntry typedEntry[K, V]
	for i := range c.pool {
		c.pool[i] = zeroEntry
		c.pool[i].poolIdx = i
		c.free[i] = i
	}
	c.freeLen = typedCachePoolSize
}

func (c *TypedUnorderedCache[K, V]) access(e *typedEntry[K, V]) {
	if c.config.Policy == CacheLRU {
		c.ll.moveToFront(e)
	}
}

func (c *TypedUnorderedCache[K, V]) removeElement(e *typedEntry[K, V]) {
	c.ll.remove(e)
	delete(c.hmap, e.Key)
	if c.config.OnEvicted != nil {
		c.config.OnEvicted(e.Key, e.Value)
	}

	// Return to free stack if it came from our pool.
	if e.poolIdx >= 0 {
		var zeroK K
		var zeroV V
		e.Key = zeroK
		e.Value = zeroV
		c.free[c.freeLen] = e.poolIdx
		c.freeLen++
	}
}

// evict removes the oldest item from the cache for FIFO and
// the least recently used item for LRU. Returns true if an
// entry was evicted, false otherwise.
func (c *TypedUnorderedCache[K, V]) evict() bool {
	if c.config.ShouldEvict == nil || c.config.Policy == CacheNone {
		return false
	}
	l := len(c.hmap)
	if l > 0 {
		e := c.ll.back()
		if c.config.ShouldEvict(l, e.Key, e.Value) {
			c.removeElement(e)
			return true
		}
	}
	return false
}
