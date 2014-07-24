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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// This code is based on: https://github.com/golang/groupcache/
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package util

import (
	"container/list"

	"code.google.com/p/biogo.store/interval"
	"code.google.com/p/biogo.store/llrb"
)

// EvictionPolicy is the cache eviction policy enum.
type EvictionPolicy int

// Constants describing LRU and FIFO cache eviction policies respectively.
const (
	CacheLRU EvictionPolicy = iota
	CacheFIFO
)

// A CacheConfig specifies the eviction policy, eviction
// trigger callback, and eviction listener callback.
type CacheConfig struct {
	// Policy is one of the consts listed for EvictionPolicy.
	Policy EvictionPolicy

	// ShouldEvict is a callback function executed each time a new entry
	// is added to the cache. It supplies cache size, and potential
	// evictee's key and value. The function should return true if the
	// entry may be evicted; false otherwise. For example, to support a
	// maximum size for the cache, use a method like:
	//
	//   func(size int, key Key, value interface{}) { return size > maxSize }
	//
	// To support max TTL in the cache, use something like:
	//
	//   func(size int, key Key, value interface{}) {
	//     return time.Now().UnixNano() - value.(int64) > maxTTLNanos
	//   }
	ShouldEvict func(size int, key Key, value interface{}) bool

	// OnEvicted optionally specifies a callback function to be
	// executed when an entry is purged from the cache.
	OnEvicted func(key Key, value interface{})
}

// Key is a generic key for use with all cache types, but must be:
//  - Comparable for use with UnorderedCache (http://golang.org/ref/spec#Comparison_operators)
//  - Type assertable to llrb.Comparable for use with OrderedCache
//  - Constructed from IntervalCache.NewKey() for use with IntervalCache
type Key interface{}

// entry holds the key and value and a pointer to the linked list
// which defines the eviction ordering.
type entry struct {
	key   Key
	value interface{}
	le    *list.Element
}

// Compare implements the llrb.Comparable interface for cache entries.
// This facility is used by the OrderedCache, and crucially requires
// that keys used with that cache implement llrb.Comparable.
func (e *entry) Compare(b llrb.Comparable) int {
	return e.key.(llrb.Comparable).Compare(b.(*entry).key.(llrb.Comparable))
}

// The following methods implement the interval.Interface for entry by
// casting the entry key to an interval key and calling the
// appropriate accessers.
func (e *entry) ID() uintptr {
	return e.key.(*intervalKey).id
}
func (e *entry) Start() interval.Comparable {
	return e.key.(*intervalKey).Start()
}
func (e *entry) End() interval.Comparable {
	return e.key.(*intervalKey).End()
}
func (e *entry) NewMutable() interval.Mutable {
	ik := e.key.(*intervalKey)
	return &intervalKey{id: 0, start: ik.start, end: ik.end}
}
func (e *entry) Overlap(r interval.Range) bool {
	return e.key.(*intervalKey).Overlap(r)
}

// cacheStore is an interface for the backing store used for the cache.
type cacheStore interface {
	// get returns the entry by key.
	get(key Key) *entry
	// add stores an entry.
	add(e *entry)
	// del removes an entry.
	del(key Key)
}

// baseCache contains the config, cacheStore interface, and the linked
// list for eviction order.
type baseCache struct {
	CacheConfig
	store cacheStore
	ll    *list.List
}

func newBaseCache(config CacheConfig) *baseCache {
	return &baseCache{
		CacheConfig: config,
		ll:          list.New(),
	}
}

// Add adds a value to the cache.
func (bc *baseCache) Add(key Key, value interface{}) {
	if e := bc.store.get(key); e != nil {
		if bc.Policy == CacheLRU {
			bc.ll.MoveToFront(e.le)
		}
		e.value = value
		return
	}
	e := &entry{key: key, value: value}
	e.le = bc.ll.PushFront(e)
	bc.store.add(e)
	// Evict as many elements as we can.
	for bc.evict() {
	}
}

// Get looks up a key's value from the cache.
func (bc *baseCache) Get(key Key) (value interface{}, ok bool) {
	if e := bc.store.get(key); e != nil {
		if bc.Policy == CacheLRU {
			bc.ll.MoveToFront(e.le)
		}
		return e.value, true
	}
	return
}

// Del removes the provided key from the cache.
func (bc *baseCache) Del(key Key) {
	if e := bc.store.get(key); e != nil {
		bc.removeElement(e)
	}
}

// Len returns the number of items in the cache.
func (bc *baseCache) Len() int {
	return bc.ll.Len()
}

func (bc *baseCache) removeElement(e *entry) {
	bc.ll.Remove(e.le)
	bc.store.del(e.key)
	if bc.OnEvicted != nil {
		bc.OnEvicted(e.key, e.value)
	}
}

// evict removes the oldest item from the cache for FIFO and
// the least recently used item for LRU. Returns true if an
// entry was evicted, false otherwise.
func (bc *baseCache) evict() bool {
	if bc.ShouldEvict == nil {
		return false
	}
	l := bc.ll.Len()
	if l > 0 {
		ele := bc.ll.Back()
		e := ele.Value.(*entry)
		if bc.ShouldEvict(l, e.key, e.value) {
			bc.removeElement(e)
			return true
		}
	}
	return false
}

// UnorderedCache is a cache which supports custom eviction triggers and two
// eviction policies: LRU and FIFO. A listener pattern is available
// for eviction events. This cache uses a hashmap for storing elements,
// making it the most performant. Only exact lookups are supported.
//
// UnorderedCache is not safe for concurrent access.
type UnorderedCache struct {
	*baseCache
	hmap map[Key]interface{}
}

// NewUnorderedCache creates a new UnorderedCache backed by a hash map.
func NewUnorderedCache(config CacheConfig) *UnorderedCache {
	mc := &UnorderedCache{
		baseCache: newBaseCache(config),
		hmap:      make(map[Key]interface{}),
	}
	mc.baseCache.store = mc
	return mc
}

// Implementation of cacheStore interface.
func (mc *UnorderedCache) get(key Key) *entry {
	if e, ok := mc.hmap[key].(*entry); ok {
		return e
	}
	return nil
}
func (mc *UnorderedCache) add(e *entry) {
	mc.hmap[e.key] = e
}
func (mc *UnorderedCache) del(key Key) {
	delete(mc.hmap, key)
}

// OrderedCache is a cache which supports binary searches using Ceil
// and Floor methods. It is backed by a left-leaning red black tree.
// See comments in UnorderedCache for more details on cache functionality.
//
// UnorderedCache is not safe for concurrent access.
type OrderedCache struct {
	*baseCache
	llrb llrb.Tree
}

// NewOrderedCache creates a new Cache backed by a left-leaning red
// black binary tree which supports binary searches via the Ceil() and
// Floor() methods. See NewUnorderedCache() for details on parameters.
func NewOrderedCache(config CacheConfig) *OrderedCache {
	oc := &OrderedCache{
		baseCache: newBaseCache(config),
	}
	oc.baseCache.store = oc
	return oc
}

// Implementation of cacheStore interface.
func (oc *OrderedCache) get(key Key) *entry {
	if e, ok := oc.llrb.Get(&entry{key: key}).(*entry); ok {
		return e
	}
	return nil
}
func (oc *OrderedCache) add(e *entry) {
	oc.llrb.Insert(e)
}
func (oc *OrderedCache) del(key Key) {
	oc.llrb.Delete(&entry{key: key})
}

// Ceil returns the smallest cache entry greater than or equal to key.
func (oc *OrderedCache) Ceil(key Key) (k Key, v interface{}, ok bool) {
	if e, ok := oc.llrb.Ceil(&entry{key: key}).(*entry); ok {
		return e.key, e.value, true
	}
	return
}

// Floor returns the greatest cache entry less than or equal to key.
func (oc *OrderedCache) Floor(key Key) (k Key, v interface{}, ok bool) {
	if e, ok := oc.llrb.Floor(&entry{key: key}).(*entry); ok {
		return e.key, e.value, true
	}
	return
}

// IntervalCache is a cache which supports querying of intervals which
// match a key or range of keys. It is backed by an interval tree. See
// comments in UnorderedCache for more details on cache functionality.
//
// Note that this implementation disregards the interval tree's ability
// to store multiple identical segments. Segments are treated as unique
// only by virtue of identical interval specifications.
//
// IntervalCache is not safe for concurrent access.
type IntervalCache struct {
	*baseCache
	tree  interval.Tree
	alloc int64
}

type intervalKey struct {
	id         uintptr
	start, end interval.Comparable
}

// Implementation of the interval.Range & interval.Mutable interfaces.
func (ik *intervalKey) Start() interval.Comparable     { return ik.start }
func (ik *intervalKey) End() interval.Comparable       { return ik.end }
func (ik *intervalKey) SetStart(c interval.Comparable) { ik.start = c }
func (ik *intervalKey) SetEnd(c interval.Comparable)   { ik.end = c }
func (ik *intervalKey) Overlap(r interval.Range) bool {
	return ik.end.Compare(r.Start()) > 0 && ik.start.Compare(r.End()) < 0
}

// NewIntervalCache creates a new Cache backed by an interval tree.
// See NewCache() for details on parameters.
func NewIntervalCache(config CacheConfig) *IntervalCache {
	ic := &IntervalCache{
		baseCache: newBaseCache(config),
	}
	ic.baseCache.store = ic
	return ic
}

// NewKey creates a new interval key defined by start and end values.
func (ic *IntervalCache) NewKey(start, end interval.Comparable) interface{} {
	ic.alloc++
	return &intervalKey{id: uintptr(ic.alloc), start: start, end: end}
}

// Implementation of cacheStore interface.
func (ic *IntervalCache) get(key Key) *entry {
	ik := key.(*intervalKey)
	if es := ic.tree.Get(ik); len(es) > 0 {
		// Search interval slice for any exact match and return it.
		for _, e := range es {
			e := e.(*entry)
			if e.Start() == ik.start && e.End() == ik.end {
				return e
			}
		}
		return nil
	}
	return nil
}
func (ic *IntervalCache) add(e *entry) {
	ic.tree.Insert(e, false)
}
func (ic *IntervalCache) del(key Key) {
	ic.tree.Delete(&entry{key: key}, false)
}

// GetOverlaps returns a slice of values which overlap the specified
// interval.
func (ic *IntervalCache) GetOverlaps(start, end interval.Comparable) []interface{} {
	es := ic.tree.Get(&intervalKey{start: start, end: end})
	values := make([]interface{}, len(es))
	for i, e := range es {
		values[i] = e.(*entry).value
	}
	return values
}
