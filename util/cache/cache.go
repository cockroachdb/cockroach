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
// permissions and limitations under the License.
//
// This code is based on: https://github.com/golang/groupcache/
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package cache

import (
	"bytes"
	"container/list"
	"fmt"
	"sync/atomic"

	"github.com/biogo/store/llrb"

	"github.com/cockroachdb/cockroach/util/interval"
	"github.com/cockroachdb/cockroach/util/log"
)

// EvictionPolicy is the cache eviction policy enum.
type EvictionPolicy int

// Constants describing LRU and FIFO, and None cache eviction policies
// respectively.
const (
	CacheLRU  EvictionPolicy = iota // Least recently used
	CacheFIFO                       // First in, first out
	CacheNone                       // No evictions; don't maintain ordering list
)

// A Config specifies the eviction policy, eviction
// trigger callback, and eviction listener callback.
type Config struct {
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
	ShouldEvict func(size int, key, value interface{}) bool

	// OnEvicted optionally specifies a callback function to be
	// executed when an entry is purged from the cache.
	OnEvicted func(key, value interface{})
}

// entry holds the key and value and a pointer to the linked list
// which defines the eviction ordering.
type entry struct {
	key, value interface{}
	le         *list.Element
}

func (e entry) String() string {
	return fmt.Sprintf("%s", e.key)
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
	return e.key.(*IntervalKey).id
}
func (e *entry) Range() interval.Range {
	return e.key.(*IntervalKey).Range
}
func (e *entry) Overlap(r interval.Range) bool {
	return e.key.(*IntervalKey).Overlap(r)
}

// cacheStore is an interface for the backing store used for the cache.
type cacheStore interface {
	// get returns the entry by key.
	get(key interface{}) *entry
	// add stores an entry.
	add(e *entry)
	// del removes an entry.
	del(key interface{})
	// clear clears all entries.
	clear()
	// len is number of items in store.
	length() int
}

// baseCache contains the config, cacheStore interface, and the linked
// list for eviction order.
type baseCache struct {
	Config
	store cacheStore
	ll    *list.List
}

func newBaseCache(config Config) *baseCache {
	return &baseCache{
		Config: config,
		ll:     list.New(),
	}
}

// Add adds a value to the cache.
func (bc *baseCache) Add(key, value interface{}) {
	if e := bc.store.get(key); e != nil {
		bc.access(e)
		e.value = value
		return
	}
	e := &entry{key: key, value: value}
	if bc.Policy != CacheNone {
		e.le = bc.ll.PushFront(e)
	}
	bc.store.add(e)
	// Evict as many elements as we can.
	for bc.evict() {
	}
}

// Get looks up a key's value from the cache.
func (bc *baseCache) Get(key interface{}) (value interface{}, ok bool) {
	if e := bc.store.get(key); e != nil {
		bc.access(e)
		return e.value, true
	}
	return
}

// Del removes the provided key from the cache.
func (bc *baseCache) Del(key interface{}) {
	if e := bc.store.get(key); e != nil {
		bc.removeElement(e)
	}
}

// Clear clears all entries from the cache.
func (bc *baseCache) Clear() {
	bc.store.clear()
}

// Len returns the number of items in the cache.
func (bc *baseCache) Len() int {
	return bc.store.length()
}

func (bc *baseCache) access(e *entry) {
	if bc.Policy == CacheLRU {
		bc.ll.MoveToFront(e.le)
	}
}

func (bc *baseCache) removeElement(e *entry) {
	if bc.Policy != CacheNone {
		bc.ll.Remove(e.le)
	}
	bc.store.del(e.key)
	if bc.OnEvicted != nil {
		bc.OnEvicted(e.key, e.value)
	}
}

// evict removes the oldest item from the cache for FIFO and
// the least recently used item for LRU. Returns true if an
// entry was evicted, false otherwise.
func (bc *baseCache) evict() bool {
	if bc.ShouldEvict == nil || bc.Policy == CacheNone {
		return false
	}
	l := bc.store.length()
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
// UnorderedCache requires that keys are comparable, according to the go
// specification (http://golang.org/ref/spec#Comparison_operators).
//
// UnorderedCache is not safe for concurrent access.
type UnorderedCache struct {
	*baseCache
	hmap map[interface{}]interface{}
}

// NewUnorderedCache creates a new UnorderedCache backed by a hash map.
func NewUnorderedCache(config Config) *UnorderedCache {
	mc := &UnorderedCache{
		baseCache: newBaseCache(config),
		hmap:      make(map[interface{}]interface{}),
	}
	mc.baseCache.store = mc
	return mc
}

// Implementation of cacheStore interface.
func (mc *UnorderedCache) get(key interface{}) *entry {
	if e, ok := mc.hmap[key].(*entry); ok {
		return e
	}
	return nil
}
func (mc *UnorderedCache) add(e *entry) {
	mc.hmap[e.key] = e
}
func (mc *UnorderedCache) del(key interface{}) {
	delete(mc.hmap, key)
}
func (mc *UnorderedCache) clear() {
	for key := range mc.hmap {
		mc.Del(key)
	}
}
func (mc *UnorderedCache) length() int {
	return len(mc.hmap)
}

// OrderedCache is a cache which supports binary searches using Ceil
// and Floor methods. It is backed by a left-leaning red black tree.
// See comments in UnorderedCache for more details on cache functionality.
//
// OrderedCache requires that keys implement llrb.Comparable.
//
// OrderedCache is not safe for concurrent access.
type OrderedCache struct {
	*baseCache
	llrb llrb.Tree
}

// NewOrderedCache creates a new Cache backed by a left-leaning red
// black binary tree which supports binary searches via the Ceil() and
// Floor() methods. See NewUnorderedCache() for details on parameters.
func NewOrderedCache(config Config) *OrderedCache {
	oc := &OrderedCache{
		baseCache: newBaseCache(config),
	}
	oc.baseCache.store = oc
	return oc
}

// Implementation of cacheStore interface.
func (oc *OrderedCache) get(key interface{}) *entry {
	if e, ok := oc.llrb.Get(&entry{key: key}).(*entry); ok {
		return e
	}
	return nil
}
func (oc *OrderedCache) add(e *entry) {
	oc.llrb.Insert(e)
}
func (oc *OrderedCache) del(key interface{}) {
	oc.llrb.Delete(&entry{key: key})
}
func (oc *OrderedCache) clear() {
	oc.llrb.Do(func(e llrb.Comparable) (done bool) {
		oc.Del(e.(*entry).key)
		return
	})
}
func (oc *OrderedCache) length() int {
	return oc.llrb.Len()
}

// Ceil returns the smallest cache entry greater than or equal to key.
func (oc *OrderedCache) Ceil(key interface{}) (interface{}, interface{}, bool) {
	if e, ok := oc.llrb.Ceil(&entry{key: key}).(*entry); ok {
		return e.key, e.value, true
	}
	return nil, nil, false
}

// Floor returns the greatest cache entry less than or equal to key.
func (oc *OrderedCache) Floor(key interface{}) (interface{}, interface{}, bool) {
	if e, ok := oc.llrb.Floor(&entry{key: key}).(*entry); ok {
		return e.key, e.value, true
	}
	return nil, nil, false
}

// Do invokes f on all of the entries in the cache.
func (oc *OrderedCache) Do(f func(k, v interface{})) {
	oc.llrb.Do(func(e llrb.Comparable) (done bool) {
		f(e.(*entry).key, e.(*entry).value)
		return
	})
}

// DoRange invokes f on all cache entries in the range of from -> to.
func (oc *OrderedCache) DoRange(f func(k, v interface{}), from, to interface{}) {
	oc.llrb.DoRange(func(e llrb.Comparable) (done bool) {
		f(e.(*entry).key, e.(*entry).value)
		return
	}, &entry{key: from}, &entry{key: to})
}

// IntervalCache is a cache which supports querying of intervals which
// match a key or range of keys. It is backed by an interval tree. See
// comments in UnorderedCache for more details on cache functionality.
//
// Note that the IntervalCache allow multiple identical segments, as
// specified by start and end keys.
//
// Keys supplied to the IntervalCache's Get, Add & Del methods must be
// constructed from IntervalCache.NewKey().
//
// IntervalCache is not safe for concurrent access.
type IntervalCache struct {
	*baseCache
	tree *interval.Tree

	// The fields below are used to avoid allocations during get, del and
	// GetOverlaps.
	getID      uintptr
	getEntry   *entry
	tmpEntry   entry
	overlapKey IntervalKey
	overlaps   []Overlap
}

// IntervalKey provides uniqueness as well as key interval.
type IntervalKey struct {
	interval.Range
	id uintptr
}

var intervalAlloc int64

// Implementation of the interval.Range & interval.Mutable interfaces.

// Overlap .
func (ik *IntervalKey) Overlap(r interval.Range) bool {
	return ik.End.Compare(r.Start) > 0 && ik.Start.Compare(r.End) < 0
}

func (ik IntervalKey) String() string {
	return fmt.Sprintf("%d: %q-%q", ik.id, ik.Start, ik.End)
}

// Contains returns true if the specified IntervalKey is contained
// within this IntervalKey.
func (ik IntervalKey) Contains(lk IntervalKey) bool {
	return lk.Start.Compare(ik.Start) >= 0 && ik.End.Compare(lk.End) >= 0
}

// NewIntervalCache creates a new Cache backed by an interval tree.
// See NewCache() for details on parameters.
func NewIntervalCache(config Config) *IntervalCache {
	ic := &IntervalCache{
		baseCache: newBaseCache(config),
		tree:      &interval.Tree{},
	}
	ic.baseCache.store = ic
	return ic
}

// NewKey creates a new interval key defined by start and end values.
func (ic *IntervalCache) NewKey(start, end []byte) *IntervalKey {
	k := ic.MakeKey(start, end)
	return &k
}

// MakeKey creates a new interval key defined by start and end values.
func (ic *IntervalCache) MakeKey(start, end []byte) IntervalKey {
	if bytes.Compare(start, end) >= 0 {
		panic(fmt.Sprintf("start key greater than or equal to end key %s >= %s", start, end))
	}
	return IntervalKey{
		Range: interval.Range{
			Start: interval.Comparable(start),
			End:   interval.Comparable(end),
		},
		id: uintptr(atomic.AddInt64(&intervalAlloc, 1)),
	}
}

// Implementation of cacheStore interface.
func (ic *IntervalCache) get(key interface{}) *entry {
	ik := key.(*IntervalKey)
	ic.getID = ik.id
	ic.tree.DoMatching(ic.doGet, ik)
	e := ic.getEntry
	ic.getEntry = nil
	return e
}

func (ic *IntervalCache) doGet(i interval.Interface) bool {
	e := i.(*entry)
	if e.ID() == ic.getID {
		ic.getEntry = e
		return true
	}
	return false
}

func (ic *IntervalCache) add(e *entry) {
	if err := ic.tree.Insert(e, false); err != nil {
		log.Error(err)
	}
}

func (ic *IntervalCache) del(key interface{}) {
	ic.tmpEntry.key = key
	if err := ic.tree.Delete(&ic.tmpEntry, false); err != nil {
		log.Error(err)
	}
}

func (ic *IntervalCache) clear() {
	ic.tree.Do(func(e interval.Interface) (done bool) {
		ic.Del(e.(*entry).key.(*IntervalKey))
		return
	})
}

func (ic *IntervalCache) length() int {
	return ic.tree.Len()
}

// Overlap contains the key/value pair for one overlap instance.
type Overlap struct {
	Key   *IntervalKey
	Value interface{}
}

// GetOverlaps returns a slice of values which overlap the specified
// interval. The slice is only valid until the next call to GetOverlaps.
func (ic *IntervalCache) GetOverlaps(start, end []byte) []Overlap {
	ic.overlapKey.Range = interval.Range{
		Start: interval.Comparable(start),
		End:   interval.Comparable(end),
	}
	ic.tree.DoMatching(ic.doOverlaps, &ic.overlapKey)
	overlaps := ic.overlaps
	ic.overlaps = ic.overlaps[:0]
	return overlaps
}

func (ic *IntervalCache) doOverlaps(i interval.Interface) bool {
	e := i.(*entry)
	ic.access(e) // maintain cache eviction ordering
	ic.overlaps = append(ic.overlaps, Overlap{
		Key:   e.key.(*IntervalKey),
		Value: e.value,
	})
	return false
}

// Do invokes f on all of the entries in the cache.
func (ic *IntervalCache) Do(f func(k, v interface{})) {
	ic.tree.Do(func(e interval.Interface) (done bool) {
		f(e.(*entry).key, e.(*entry).value)
		return
	})
}
