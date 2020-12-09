// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// This code is based on: https://github.com/golang/groupcache/

package cache

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"

	"github.com/biogo/store/llrb"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	//     return timeutil.Now().UnixNano() - value.(int64) > maxTTLNanos
	//   }
	ShouldEvict func(size int, key, value interface{}) bool

	// OnEvicted optionally specifies a callback function to be
	// executed when an entry is purged from the cache.
	OnEvicted func(key, value interface{})

	// OnEvictedEntry optionally specifies a callback function to
	// be executed when an entry is purged from the cache.
	OnEvictedEntry func(entry *Entry)
}

// Entry holds the key and value and a pointer to the linked list
// which defines the eviction ordering.
type Entry struct {
	Key, Value interface{}
	next, prev *Entry
}

func (e Entry) String() string {
	return fmt.Sprintf("%s", e.Key)
}

// Compare implements the llrb.Comparable interface for cache entries.
// This facility is used by the OrderedCache, and crucially requires
// that keys used with that cache implement llrb.Comparable.
func (e *Entry) Compare(b llrb.Comparable) int {
	return e.Key.(llrb.Comparable).Compare(b.(*Entry).Key.(llrb.Comparable))
}

// The following methods implement the interval.Interface for entry by casting
// the entry key to an interval key and calling the appropriate accessers.

// ID implements interval.Interface
func (e *Entry) ID() uintptr {
	return e.Key.(*IntervalKey).id
}

// Range implements interval.Interface
func (e *Entry) Range() interval.Range {
	return e.Key.(*IntervalKey).Range
}

// entryList is a double-linked circular list of *Entry elements. The code is
// derived from the stdlib container/list but customized to Entry in order to
// avoid a separate allocation for every element.
type entryList struct {
	root Entry
}

func (l *entryList) init() {
	l.root.next = &l.root
	l.root.prev = &l.root
}

func (l *entryList) back() *Entry {
	return l.root.prev
}

func (l *entryList) insertAfter(e, at *Entry) {
	n := at.next
	at.next = e
	e.prev = at
	e.next = n
	n.prev = e
}

func (l *entryList) insertBefore(e, mark *Entry) {
	l.insertAfter(e, mark.prev)
}

func (l *entryList) remove(e *Entry) *Entry {
	if e == &l.root {
		panic("cannot remove root list node")
	}
	// TODO(peter): Revert this protection against removing a non-existent entry
	// from the list when the cause of
	// https://github.com/cockroachdb/cockroach/issues/6190 is determined. Should
	// be replaced with an explicit panic instead of the implicit one of a
	// nil-pointer dereference.
	if e.next != nil {
		e.prev.next = e.next
		e.next.prev = e.prev
		e.next = nil // avoid memory leaks
		e.prev = nil // avoid memory leaks
	}
	return e
}

func (l *entryList) pushFront(e *Entry) {
	l.insertAfter(e, &l.root)
}

func (l *entryList) moveToFront(e *Entry) {
	if l.root.next == e {
		return
	}
	l.insertAfter(l.remove(e), &l.root)
}

// cacheStore is an interface for the backing store used for the cache.
type cacheStore interface {
	// init initializes or clears all entries.
	init()
	// get returns the entry by key.
	get(key interface{}) *Entry
	// add stores an entry.
	add(e *Entry)
	// del removes an entry.
	del(e *Entry)
	// len is number of items in store.
	length() int
}

// baseCache contains the config, cacheStore interface, and the linked
// list for eviction order.
type baseCache struct {
	Config
	store cacheStore
	ll    entryList
}

func newBaseCache(config Config) baseCache {
	return baseCache{
		Config: config,
	}
}

// init initializes the baseCache with the provided cacheStore. It must be
// called with a non-nil cacheStore before use of the cache.
func (bc *baseCache) init(store cacheStore) {
	bc.ll.init()
	bc.store = store
	bc.store.init()
}

// Add adds a value to the cache.
func (bc *baseCache) Add(key, value interface{}) {
	bc.add(key, value, nil, nil)
}

// AddEntry adds a value to the cache. It provides an alternative interface to
// Add which the caller can use to reduce allocations by bundling the Entry
// structure with the key and value to be stored.
func (bc *baseCache) AddEntry(entry *Entry) {
	bc.add(entry.Key, entry.Value, entry, nil)
}

// AddEntryAfter adds a value to the cache, making sure that it is placed after
// the second entry in the eviction queue. It provides an alternative interface to
// Add which the caller can use to reduce allocations by bundling the Entry
// structure with the key and value to be stored.
func (bc *baseCache) AddEntryAfter(entry, after *Entry) {
	bc.add(entry.Key, entry.Value, entry, after)
}

// MoveToEnd moves the entry to the end of the eviction queue.
func (bc *baseCache) MoveToEnd(entry *Entry) {
	bc.ll.moveToFront(entry)
}

func (bc *baseCache) add(key, value interface{}, entry, after *Entry) {
	if e := bc.store.get(key); e != nil {
		bc.access(e)
		e.Value = value
		return
	}
	e := entry
	if e == nil {
		e = &Entry{Key: key, Value: value}
	}
	if after != nil {
		bc.ll.insertBefore(e, after)
	} else {
		bc.ll.pushFront(e)
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
		return e.Value, true
	}
	return nil, false
}

// StealthyGet looks up a key's value from the cache but does not consider it an
// "access" (with respect to the policy).
func (bc *baseCache) StealthyGet(key interface{}) (value interface{}, ok bool) {
	if e := bc.store.get(key); e != nil {
		return e.Value, true
	}
	return nil, false
}

// Del removes the provided key from the cache.
func (bc *baseCache) Del(key interface{}) {
	e := bc.store.get(key)
	bc.DelEntry(e)
}

// DelEntry removes the provided entry from the cache.
func (bc *baseCache) DelEntry(entry *Entry) {
	if entry != nil {
		bc.removeElement(entry)
	}
}

// Clear clears all entries from the cache.
func (bc *baseCache) Clear() {
	if bc.OnEvicted != nil || bc.OnEvictedEntry != nil {
		for e := bc.ll.back(); e != &bc.ll.root; e = e.prev {
			if bc.OnEvicted != nil {
				bc.OnEvicted(e.Key, e.Value)
			}
			if bc.OnEvictedEntry != nil {
				bc.OnEvictedEntry(e)
			}
		}
	}
	bc.ll.init()
	bc.store.init()
}

// Len returns the number of items in the cache.
func (bc *baseCache) Len() int {
	return bc.store.length()
}

// Do iterates over all entries in the cache and calls fn with each entry.
func (bc *baseCache) Do(fn func(e *Entry)) {
	for e := bc.ll.root.next; e != &bc.ll.root; e = e.next {
		fn(e)
	}
}

func (bc *baseCache) access(e *Entry) {
	if bc.Policy == CacheLRU {
		bc.ll.moveToFront(e)
	}
}

func (bc *baseCache) removeElement(e *Entry) {
	bc.ll.remove(e)
	bc.store.del(e)
	if bc.OnEvicted != nil {
		bc.OnEvicted(e.Key, e.Value)
	}
	if bc.OnEvictedEntry != nil {
		bc.OnEvictedEntry(e)
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
		e := bc.ll.back()
		if bc.ShouldEvict(l, e.Key, e.Value) {
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
	baseCache
	hmap map[interface{}]interface{}
}

// NewUnorderedCache creates a new UnorderedCache backed by a hash map.
func NewUnorderedCache(config Config) *UnorderedCache {
	mc := &UnorderedCache{
		baseCache: newBaseCache(config),
	}
	mc.baseCache.init(mc)
	return mc
}

// Implementation of cacheStore interface.
func (mc *UnorderedCache) init() {
	mc.hmap = make(map[interface{}]interface{})
}
func (mc *UnorderedCache) get(key interface{}) *Entry {
	if e, ok := mc.hmap[key].(*Entry); ok {
		return e
	}
	return nil
}
func (mc *UnorderedCache) add(e *Entry) {
	mc.hmap[e.Key] = e
}
func (mc *UnorderedCache) del(e *Entry) {
	delete(mc.hmap, e.Key)
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
	baseCache
	llrb llrb.Tree
}

// NewOrderedCache creates a new Cache backed by a left-leaning red
// black binary tree which supports binary searches via the Ceil() and
// Floor() methods. See NewUnorderedCache() for details on parameters.
func NewOrderedCache(config Config) *OrderedCache {
	oc := &OrderedCache{
		baseCache: newBaseCache(config),
	}
	oc.baseCache.init(oc)
	return oc
}

// Implementation of cacheStore interface.
func (oc *OrderedCache) init() {
	oc.llrb = llrb.Tree{}
}
func (oc *OrderedCache) get(key interface{}) *Entry {
	if e, ok := oc.llrb.Get(&Entry{Key: key}).(*Entry); ok {
		return e
	}
	return nil
}
func (oc *OrderedCache) add(e *Entry) {
	oc.llrb.Insert(e)
}
func (oc *OrderedCache) del(e *Entry) {
	oc.llrb.Delete(e)
}
func (oc *OrderedCache) length() int {
	return oc.llrb.Len()
}

// CeilEntry returns the smallest cache entry greater than or equal to key.
func (oc *OrderedCache) CeilEntry(key interface{}) (*Entry, bool) {
	if e, ok := oc.llrb.Ceil(&Entry{Key: key}).(*Entry); ok {
		return e, true
	}
	return nil, false
}

// Ceil returns the smallest key-value pair greater than or equal to key.
func (oc *OrderedCache) Ceil(key interface{}) (interface{}, interface{}, bool) {
	if e, ok := oc.CeilEntry(key); ok {
		return e.Key, e.Value, true
	}
	return nil, nil, false
}

// FloorEntry returns the greatest cache entry less than or equal to key.
func (oc *OrderedCache) FloorEntry(key interface{}) (*Entry, bool) {
	if e, ok := oc.llrb.Floor(&Entry{Key: key}).(*Entry); ok {
		return e, true
	}
	return nil, false
}

// Floor returns the greatest key-value pair less than or equal to key.
func (oc *OrderedCache) Floor(key interface{}) (interface{}, interface{}, bool) {
	if e, ok := oc.FloorEntry(key); ok {
		return e.Key, e.Value, true
	}
	return nil, nil, false
}

// DoEntry invokes f on all cache entries in the cache. f returns a boolean
// indicating the traversal is done. If f returns true, the DoEntry loop will
// exit; false, it will continue. DoEntry returns whether the iteration exited
// early.
func (oc *OrderedCache) DoEntry(f func(e *Entry) bool) bool {
	return oc.llrb.Do(func(e llrb.Comparable) bool {
		return f(e.(*Entry))
	})
}

// Do invokes f on all key-value pairs in the cache. f returns a boolean
// indicating the traversal is done. If f returns true, the Do loop will exit;
// false, it will continue. Do returns whether the iteration exited early.
func (oc *OrderedCache) Do(f func(k, v interface{}) bool) bool {
	return oc.DoEntry(func(e *Entry) bool {
		return f(e.Key, e.Value)
	})
}

// DoRangeEntry invokes f on all cache entries in the range of from -> to. f
// returns a boolean indicating the traversal is done. If f returns true, the
// DoRangeEntry loop will exit; false, it will continue. DoRangeEntry returns
// whether the iteration exited early.
func (oc *OrderedCache) DoRangeEntry(f func(e *Entry) bool, from, to interface{}) bool {
	return oc.llrb.DoRange(func(e llrb.Comparable) bool {
		return f(e.(*Entry))
	}, &Entry{Key: from}, &Entry{Key: to})
}

// DoRangeReverseEntry invokes f on all cache entries in the range (to, from]. from
// should be higher than to.
// f returns a boolean indicating the traversal is done. If f returns true, the
// DoRangeReverseEntry loop will exit; false, it will continue.
// DoRangeReverseEntry returns whether the iteration exited early.
func (oc *OrderedCache) DoRangeReverseEntry(f func(e *Entry) bool, from, to interface{}) bool {
	return oc.llrb.DoRangeReverse(func(e llrb.Comparable) bool {
		return f(e.(*Entry))
	}, &Entry{Key: from}, &Entry{Key: to})
}

// DoRange invokes f on all key-value pairs in the range of from -> to. f
// returns a boolean indicating the traversal is done. If f returns true, the
// DoRange loop will exit; false, it will continue. DoRange returns whether the
// iteration exited early.
func (oc *OrderedCache) DoRange(f func(k, v interface{}) bool, from, to interface{}) bool {
	return oc.DoRangeEntry(func(e *Entry) bool {
		return f(e.Key, e.Value)
	}, from, to)
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
	baseCache
	tree interval.Tree

	// The fields below are used to avoid allocations during get, del and
	// GetOverlaps.
	getID      uintptr
	getEntry   *Entry
	overlapKey IntervalKey
	overlaps   []*Entry
}

// IntervalKey provides uniqueness as well as key interval.
type IntervalKey struct {
	interval.Range
	id uintptr
}

var intervalAlloc int64

func (ik IntervalKey) String() string {
	return fmt.Sprintf("%d: %q-%q", ik.id, ik.Start, ik.End)
}

// NewIntervalCache creates a new Cache backed by an interval tree.
// See NewCache() for details on parameters.
func NewIntervalCache(config Config) *IntervalCache {
	ic := &IntervalCache{
		baseCache: newBaseCache(config),
	}
	ic.baseCache.init(ic)
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
		panic(fmt.Sprintf("start key greater than or equal to end key %q >= %q", start, end))
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
func (ic *IntervalCache) init() {
	ic.tree = interval.NewTree(interval.ExclusiveOverlapper)
}

func (ic *IntervalCache) get(key interface{}) *Entry {
	ik := key.(*IntervalKey)
	ic.getID = ik.id
	ic.tree.DoMatching(ic.doGet, ik.Range)
	e := ic.getEntry
	ic.getEntry = nil
	return e
}

func (ic *IntervalCache) doGet(i interval.Interface) bool {
	e := i.(*Entry)
	if e.ID() == ic.getID {
		ic.getEntry = e
		return true
	}
	return false
}

func (ic *IntervalCache) add(e *Entry) {
	if err := ic.tree.Insert(e, false); err != nil {
		log.Errorf(context.TODO(), "%v", err)
	}
}

func (ic *IntervalCache) del(e *Entry) {
	if err := ic.tree.Delete(e, false); err != nil {
		log.Errorf(context.TODO(), "%v", err)
	}
}

func (ic *IntervalCache) length() int {
	return ic.tree.Len()
}

// GetOverlaps returns a slice of values which overlap the specified
// interval. The slice is only valid until the next call to GetOverlaps.
func (ic *IntervalCache) GetOverlaps(start, end []byte) []*Entry {
	ic.overlapKey.Range = interval.Range{
		Start: interval.Comparable(start),
		End:   interval.Comparable(end),
	}
	ic.tree.DoMatching(ic.doOverlaps, ic.overlapKey.Range)
	overlaps := ic.overlaps
	ic.overlaps = ic.overlaps[:0]
	return overlaps
}

func (ic *IntervalCache) doOverlaps(i interval.Interface) bool {
	e := i.(*Entry)
	ic.access(e) // maintain cache eviction ordering
	ic.overlaps = append(ic.overlaps, e)
	return false
}
