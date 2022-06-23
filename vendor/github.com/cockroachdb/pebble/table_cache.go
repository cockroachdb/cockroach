// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

var emptyIter = &errorIter{err: nil}
var emptyKeyspanIter = &errorKeyspanIter{err: nil}

// filteredAll is a singleton internalIterator implementation used when an
// sstable does contain point keys, but all the keys are filtered by the active
// PointKeyFilters set in the iterator's IterOptions.
//
// filteredAll implements filteredIter, ensuring the level iterator recognizes
// when it may need to return file boundaries to keep the rangeDelIter open
// during mergingIter operation.
var filteredAll = &filteredAllKeysIter{errorIter: errorIter{err: nil}}

var _ filteredIter = filteredAll

type filteredAllKeysIter struct {
	errorIter
}

func (s *filteredAllKeysIter) MaybeFilteredKeys() bool {
	return true
}

var tableCacheLabels = pprof.Labels("pebble", "table-cache")

// tableCacheOpts contains the db specific fields
// of a table cache. This is stored in the tableCacheContainer
// along with the table cache.
// NB: It is important to make sure that the fields in this
// struct are read-only. Since the fields here are shared
// by every single tableCacheShard, if non read-only fields
// are updated, we could have unnecessary evictions of those
// fields, and the surrounding fields from the CPU caches.
type tableCacheOpts struct {
	atomic struct {
		// iterCount in the tableCacheOpts keeps track of iterators
		// opened or closed by a DB. It's used to keep track of
		// leaked iterators on a per-db level.
		iterCount *int32
	}

	logger        Logger
	cacheID       uint64
	dirname       string
	fs            vfs.FS
	opts          sstable.ReaderOptions
	filterMetrics *FilterMetrics
}

// tableCacheContainer contains the table cache and
// fields which are unique to the DB.
type tableCacheContainer struct {
	tableCache *TableCache

	// dbOpts contains fields relevant to the table cache
	// which are unique to each DB.
	dbOpts tableCacheOpts
}

// newTableCacheContainer will panic if the underlying cache in the table cache
// doesn't match Options.Cache.
func newTableCacheContainer(
	tc *TableCache, cacheID uint64, dirname string, fs vfs.FS, opts *Options, size int,
) *tableCacheContainer {
	// We will release a ref to table cache acquired here when tableCacheContainer.close is called.
	if tc != nil {
		if tc.cache != opts.Cache {
			panic("pebble: underlying cache for the table cache and db are different")
		}
		tc.Ref()
	} else {
		// NewTableCache should create a ref to tc which the container should
		// drop whenever it is closed.
		tc = NewTableCache(opts.Cache, opts.Experimental.TableCacheShards, size)
	}

	t := &tableCacheContainer{}
	t.tableCache = tc
	t.dbOpts.logger = opts.Logger
	t.dbOpts.cacheID = cacheID
	t.dbOpts.dirname = dirname
	t.dbOpts.fs = fs
	t.dbOpts.opts = opts.MakeReaderOptions()
	t.dbOpts.filterMetrics = &FilterMetrics{}
	t.dbOpts.atomic.iterCount = new(int32)
	return t
}

// Before calling close, make sure that there will be no further need
// to access any of the files associated with the store.
func (c *tableCacheContainer) close() error {
	// We want to do some cleanup work here. Check for leaked iterators
	// by the DB using this container. Note that we'll still perform cleanup
	// below in the case that there are leaked iterators.
	var err error
	if v := atomic.LoadInt32(c.dbOpts.atomic.iterCount); v > 0 {
		err = errors.Errorf("leaked iterators: %d", errors.Safe(v))
	}

	// Release nodes here.
	for _, shard := range c.tableCache.shards {
		if shard != nil {
			shard.removeDB(&c.dbOpts)
		}
	}
	return firstError(err, c.tableCache.Unref())
}

func (c *tableCacheContainer) newIters(
	file *manifest.FileMetadata, opts *IterOptions, internalOpts internalIterOpts,
) (internalIterator, keyspan.FragmentIterator, error) {
	return c.tableCache.getShard(file.FileNum).newIters(file, opts, internalOpts, &c.dbOpts)
}

func (c *tableCacheContainer) newRangeKeyIter(
	file *manifest.FileMetadata, opts *keyspan.SpanIterOptions,
) (keyspan.FragmentIterator, error) {
	return c.tableCache.getShard(file.FileNum).newRangeKeyIter(file, opts, &c.dbOpts)
}

func (c *tableCacheContainer) getTableProperties(file *fileMetadata) (*sstable.Properties, error) {
	return c.tableCache.getShard(file.FileNum).getTableProperties(file, &c.dbOpts)
}

func (c *tableCacheContainer) evict(fileNum FileNum) {
	c.tableCache.getShard(fileNum).evict(fileNum, &c.dbOpts, false)
}

func (c *tableCacheContainer) metrics() (CacheMetrics, FilterMetrics) {
	var m CacheMetrics
	for i := range c.tableCache.shards {
		s := c.tableCache.shards[i]
		s.mu.RLock()
		m.Count += int64(len(s.mu.nodes))
		s.mu.RUnlock()
		m.Hits += atomic.LoadInt64(&s.atomic.hits)
		m.Misses += atomic.LoadInt64(&s.atomic.misses)
	}
	m.Size = m.Count * int64(unsafe.Sizeof(sstable.Reader{}))
	f := FilterMetrics{
		Hits:   atomic.LoadInt64(&c.dbOpts.filterMetrics.Hits),
		Misses: atomic.LoadInt64(&c.dbOpts.filterMetrics.Misses),
	}
	return m, f
}

func (c *tableCacheContainer) withReader(meta *fileMetadata, fn func(*sstable.Reader) error) error {
	s := c.tableCache.getShard(meta.FileNum)
	v := s.findNode(meta, &c.dbOpts)
	defer s.unrefValue(v)
	if v.err != nil {
		base.MustExist(c.dbOpts.fs, v.filename, c.dbOpts.logger, v.err)
		return v.err
	}
	return fn(v.reader)
}

func (c *tableCacheContainer) iterCount() int64 {
	return int64(atomic.LoadInt32(c.dbOpts.atomic.iterCount))
}

// TableCache is a shareable cache for open sstables.
type TableCache struct {
	// atomic contains fields which are accessed atomically. Go allocations
	// are guaranteed to be 64-bit aligned which we take advantage of by
	// placing the 64-bit fields which we access atomically at the beginning
	// of the TableCache struct. For more information, see
	// https://golang.org/pkg/sync/atomic/#pkg-note-BUG.
	atomic struct {
		refs int64
	}

	cache  *Cache
	shards []*tableCacheShard
}

// Ref adds a reference to the table cache. Once tableCache.init returns,
// the table cache only remains valid if there is at least one reference
// to it.
func (c *TableCache) Ref() {
	v := atomic.AddInt64(&c.atomic.refs, 1)
	// We don't want the reference count to ever go from 0 -> 1,
	// cause a reference count of 0 implies that we've closed the cache.
	if v <= 1 {
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	}
}

// Unref removes a reference to the table cache.
func (c *TableCache) Unref() error {
	v := atomic.AddInt64(&c.atomic.refs, -1)
	switch {
	case v < 0:
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	case v == 0:
		var err error
		for i := range c.shards {
			// The cache shard is not allocated yet, nothing to close
			if c.shards[i] == nil {
				continue
			}
			err = firstError(err, c.shards[i].Close())
		}

		// Unref the cache which we create a reference to when the tableCache
		// is first instantiated.
		c.cache.Unref()
		return err
	}
	return nil
}

// NewTableCache will create a reference to the table cache. It is the callers responsibility
// to call tableCache.Unref if they will no longer hold a reference to the table cache.
func NewTableCache(cache *Cache, numShards int, size int) *TableCache {
	if size == 0 {
		panic("pebble: cannot create a table cache of size 0")
	} else if numShards == 0 {
		panic("pebble: cannot create a table cache with 0 shards")
	}

	c := &TableCache{}
	c.cache = cache
	c.cache.Ref()

	c.shards = make([]*tableCacheShard, numShards)
	for i := range c.shards {
		c.shards[i] = &tableCacheShard{}
		c.shards[i].init(size / len(c.shards))
	}

	// Hold a ref to the cache here.
	c.atomic.refs = 1

	return c
}

func (c *TableCache) getShard(fileNum FileNum) *tableCacheShard {
	return c.shards[uint64(fileNum)%uint64(len(c.shards))]
}

type tableCacheKey struct {
	cacheID uint64
	fileNum FileNum
}

type tableCacheShard struct {
	// WARNING: The following struct `atomic` contains fields are accessed atomically.
	//
	// Go allocations are guaranteed to be 64-bit aligned which we take advantage
	// of by placing the 64-bit fields which we access atomically at the beginning
	// of the DB struct. For more information, see https://golang.org/pkg/sync/atomic/#pkg-note-BUG.
	atomic struct {
		hits      int64
		misses    int64
		iterCount int32
	}

	size int

	mu struct {
		sync.RWMutex
		nodes map[tableCacheKey]*tableCacheNode
		// The iters map is only created and populated in race builds.
		iters map[io.Closer][]byte

		handHot  *tableCacheNode
		handCold *tableCacheNode
		handTest *tableCacheNode

		coldTarget int
		sizeHot    int
		sizeCold   int
		sizeTest   int
	}
	releasing       sync.WaitGroup
	releasingCh     chan *tableCacheValue
	releaseLoopExit sync.WaitGroup
}

func (c *tableCacheShard) init(size int) {
	c.size = size

	c.mu.nodes = make(map[tableCacheKey]*tableCacheNode)
	c.mu.coldTarget = size
	c.releasingCh = make(chan *tableCacheValue, 100)
	c.releaseLoopExit.Add(1)
	go c.releaseLoop()

	if invariants.RaceEnabled {
		c.mu.iters = make(map[io.Closer][]byte)
	}
}

func (c *tableCacheShard) releaseLoop() {
	pprof.Do(context.Background(), tableCacheLabels, func(context.Context) {
		defer c.releaseLoopExit.Done()
		for v := range c.releasingCh {
			v.release(c)
		}
	})
}

// checkAndIntersectFilters checks the specific table and block property filters
// for intersection with any available table and block-level properties. Returns
// true for ok if this table should be read by this iterator.
func (c *tableCacheShard) checkAndIntersectFilters(
	v *tableCacheValue,
	tableFilter func(userProps map[string]string) bool,
	blockPropertyFilters []BlockPropertyFilter,
	boundLimitedFilter sstable.BoundLimitedBlockPropertyFilter,
) (ok bool, filterer *sstable.BlockPropertiesFilterer, err error) {
	if tableFilter != nil &&
		!tableFilter(v.reader.Properties.UserProperties) {
		return false, nil, nil
	}

	if boundLimitedFilter != nil || len(blockPropertyFilters) > 0 {
		filterer = sstable.NewBlockPropertiesFilterer(blockPropertyFilters, boundLimitedFilter)
		intersects, err :=
			filterer.IntersectsUserPropsAndFinishInit(v.reader.Properties.UserProperties)
		if err != nil {
			return false, nil, err
		}
		if !intersects {
			return false, nil, nil
		}
	}
	return true, filterer, nil
}

func (c *tableCacheShard) newIters(
	file *manifest.FileMetadata,
	opts *IterOptions,
	internalOpts internalIterOpts,
	dbOpts *tableCacheOpts,
) (internalIterator, keyspan.FragmentIterator, error) {
	// Calling findNode gives us the responsibility of decrementing v's
	// refCount. If opening the underlying table resulted in error, then we
	// decrement this straight away. Otherwise, we pass that responsibility to
	// the sstable iterator, which decrements when it is closed.
	v := c.findNode(file, dbOpts)
	if v.err != nil {
		defer c.unrefValue(v)
		base.MustExist(dbOpts.fs, v.filename, dbOpts.logger, v.err)
		return nil, nil, v.err
	}

	ok := true
	var filterer *sstable.BlockPropertiesFilterer
	var err error
	if opts != nil {
		ok, filterer, err = c.checkAndIntersectFilters(v, opts.TableFilter,
			opts.PointKeyFilters, internalOpts.boundLimitedFilter)
	}
	if err != nil {
		c.unrefValue(v)
		return nil, nil, err
	}

	// NB: range-del iterator does not maintain a reference to the table, nor
	// does it need to read from it after creation.
	rangeDelIter, err := v.reader.NewRawRangeDelIter()
	if err != nil {
		c.unrefValue(v)
		return nil, nil, err
	}

	if !ok {
		c.unrefValue(v)
		// Return an empty iterator. This iterator has no mutable state, so
		// using a singleton is fine.
		// NB: We still return the potentially non-empty rangeDelIter. This
		// ensures the iterator observes the file's range deletions even if the
		// block property filters exclude all the file's point keys. The range
		// deletions may still delete keys lower in the LSM in files that DO
		// match the active filters.
		//
		// The point iterator returned must implement the filteredIter
		// interface, so that the level iterator surfaces file boundaries when
		// range deletions are present.
		return filteredAll, rangeDelIter, err
	}

	var iter sstable.Iterator
	useFilter := true
	if opts != nil {
		useFilter = manifest.LevelToInt(opts.level) != 6 || opts.UseL6Filters
	}
	if internalOpts.bytesIterated != nil {
		iter, err = v.reader.NewCompactionIter(internalOpts.bytesIterated)
	} else {
		iter, err = v.reader.NewIterWithBlockPropertyFilters(
			opts.GetLowerBound(), opts.GetUpperBound(), filterer, useFilter, internalOpts.stats)
	}
	if err != nil {
		if rangeDelIter != nil {
			_ = rangeDelIter.Close()
		}
		c.unrefValue(v)
		return nil, nil, err
	}
	// NB: v.closeHook takes responsibility for calling unrefValue(v) here. Take
	// care to avoid introduceingan allocation here by adding a closure.
	iter.SetCloseHook(v.closeHook)

	atomic.AddInt32(&c.atomic.iterCount, 1)
	atomic.AddInt32(dbOpts.atomic.iterCount, 1)
	if invariants.RaceEnabled {
		c.mu.Lock()
		c.mu.iters[iter] = debug.Stack()
		c.mu.Unlock()
	}
	return iter, rangeDelIter, nil
}

func (c *tableCacheShard) newRangeKeyIter(
	file *manifest.FileMetadata, opts *keyspan.SpanIterOptions, dbOpts *tableCacheOpts,
) (keyspan.FragmentIterator, error) {
	// Calling findNode gives us the responsibility of decrementing v's
	// refCount. If opening the underlying table resulted in error, then we
	// decrement this straight away. Otherwise, we pass that responsibility to
	// the sstable iterator, which decrements when it is closed.
	v := c.findNode(file, dbOpts)
	if v.err != nil {
		defer c.unrefValue(v)
		base.MustExist(dbOpts.fs, v.filename, dbOpts.logger, v.err)
		return nil, v.err
	}

	ok := true
	var err error
	// Don't filter a table's range keys if the file contains RANGEKEYDELs.
	// The RANGEKEYDELs may delete range keys in other levels. Skipping the
	// file's range key blocks may surface deleted range keys below. This is
	// done here, rather than deferring to the block-property collector in order
	// to maintain parity with point keys and the treatment of RANGEDELs.
	if opts != nil && v.reader.Properties.NumRangeKeyDels == 0 {
		ok, _, err = c.checkAndIntersectFilters(v, nil, opts.RangeKeyFilters, nil)
	}
	if err != nil {
		c.unrefValue(v)
		return nil, err
	}
	if !ok {
		c.unrefValue(v)
		// Return the empty iterator. This iterator has no mutable state, so
		// using a singleton is fine.
		return emptyKeyspanIter, err
	}

	var iter keyspan.FragmentIterator
	iter, err = v.reader.NewRawRangeKeyIter()
	// iter is a block iter that holds the entire value of the block in memory.
	// No need to hold onto a ref of the cache value.
	c.unrefValue(v)

	if err != nil || iter == nil {
		return nil, err
	}

	return iter, nil
}

// getTableProperties return sst table properties for target file
func (c *tableCacheShard) getTableProperties(
	file *fileMetadata, dbOpts *tableCacheOpts,
) (*sstable.Properties, error) {
	// Calling findNode gives us the responsibility of decrementing v's refCount here
	v := c.findNode(file, dbOpts)
	defer c.unrefValue(v)

	if v.err != nil {
		return nil, v.err
	}
	return &v.reader.Properties, nil
}

// releaseNode releases a node from the tableCacheShard.
//
// c.mu must be held when calling this.
func (c *tableCacheShard) releaseNode(n *tableCacheNode) {
	c.unlinkNode(n)
	c.clearNode(n)
}

// unlinkNode removes a node from the tableCacheShard, leaving the shard
// reference in place.
//
// c.mu must be held when calling this.
func (c *tableCacheShard) unlinkNode(n *tableCacheNode) {
	key := tableCacheKey{n.cacheID, n.meta.FileNum}
	delete(c.mu.nodes, key)

	switch n.ptype {
	case tableCacheNodeHot:
		c.mu.sizeHot--
	case tableCacheNodeCold:
		c.mu.sizeCold--
	case tableCacheNodeTest:
		c.mu.sizeTest--
	}

	if n == c.mu.handHot {
		c.mu.handHot = c.mu.handHot.prev()
	}
	if n == c.mu.handCold {
		c.mu.handCold = c.mu.handCold.prev()
	}
	if n == c.mu.handTest {
		c.mu.handTest = c.mu.handTest.prev()
	}

	if n.unlink() == n {
		// This was the last entry in the cache.
		c.mu.handHot = nil
		c.mu.handCold = nil
		c.mu.handTest = nil
	}

	n.links.prev = nil
	n.links.next = nil
}

func (c *tableCacheShard) clearNode(n *tableCacheNode) {
	if v := n.value; v != nil {
		n.value = nil
		c.unrefValue(v)
	}
}

// unrefValue decrements the reference count for the specified value, releasing
// it if the reference count fell to 0. Note that the value has a reference if
// it is present in tableCacheShard.mu.nodes, so a reference count of 0 means
// the node has already been removed from that map.
func (c *tableCacheShard) unrefValue(v *tableCacheValue) {
	if atomic.AddInt32(&v.refCount, -1) == 0 {
		c.releasing.Add(1)
		c.releasingCh <- v
	}
}

// findNode returns the node for the table with the given file number, creating
// that node if it didn't already exist. The caller is responsible for
// decrementing the returned node's refCount.
func (c *tableCacheShard) findNode(meta *fileMetadata, dbOpts *tableCacheOpts) *tableCacheValue {
	// Fast-path for a hit in the cache.
	c.mu.RLock()
	key := tableCacheKey{dbOpts.cacheID, meta.FileNum}
	if n := c.mu.nodes[key]; n != nil && n.value != nil {
		// Fast-path hit.
		//
		// The caller is responsible for decrementing the refCount.
		v := n.value
		atomic.AddInt32(&v.refCount, 1)
		c.mu.RUnlock()
		atomic.StoreInt32(&n.referenced, 1)
		atomic.AddInt64(&c.atomic.hits, 1)
		<-v.loaded
		return v
	}
	c.mu.RUnlock()

	c.mu.Lock()

	n := c.mu.nodes[key]
	switch {
	case n == nil:
		// Slow-path miss of a non-existent node.
		n = &tableCacheNode{
			meta:  meta,
			ptype: tableCacheNodeCold,
		}
		c.addNode(n, dbOpts)
		c.mu.sizeCold++

	case n.value != nil:
		// Slow-path hit of a hot or cold node.
		//
		// The caller is responsible for decrementing the refCount.
		v := n.value
		atomic.AddInt32(&v.refCount, 1)
		atomic.StoreInt32(&n.referenced, 1)
		atomic.AddInt64(&c.atomic.hits, 1)
		c.mu.Unlock()
		<-v.loaded
		return v

	default:
		// Slow-path miss of a test node.
		c.unlinkNode(n)
		c.mu.coldTarget++
		if c.mu.coldTarget > c.size {
			c.mu.coldTarget = c.size
		}

		atomic.StoreInt32(&n.referenced, 0)
		n.ptype = tableCacheNodeHot
		c.addNode(n, dbOpts)
		c.mu.sizeHot++
	}

	atomic.AddInt64(&c.atomic.misses, 1)

	v := &tableCacheValue{
		loaded:   make(chan struct{}),
		refCount: 2,
	}
	// Cache the closure invoked when an iterator is closed. This avoids an
	// allocation on every call to newIters.
	v.closeHook = func(i sstable.Iterator) error {
		if invariants.RaceEnabled {
			c.mu.Lock()
			delete(c.mu.iters, i)
			c.mu.Unlock()
		}
		c.unrefValue(v)
		atomic.AddInt32(&c.atomic.iterCount, -1)
		atomic.AddInt32(dbOpts.atomic.iterCount, -1)
		return nil
	}
	n.value = v

	c.mu.Unlock()

	// Note adding to the cache lists must complete before we begin loading the
	// table as a failure during load will result in the node being unlinked.
	pprof.Do(context.Background(), tableCacheLabels, func(context.Context) {
		v.load(meta, c, dbOpts)
	})
	return v
}

func (c *tableCacheShard) addNode(n *tableCacheNode, dbOpts *tableCacheOpts) {
	c.evictNodes()
	n.cacheID = dbOpts.cacheID
	key := tableCacheKey{n.cacheID, n.meta.FileNum}
	c.mu.nodes[key] = n

	n.links.next = n
	n.links.prev = n
	if c.mu.handHot == nil {
		// First element.
		c.mu.handHot = n
		c.mu.handCold = n
		c.mu.handTest = n
	} else {
		c.mu.handHot.link(n)
	}

	if c.mu.handCold == c.mu.handHot {
		c.mu.handCold = c.mu.handCold.prev()
	}
}

func (c *tableCacheShard) evictNodes() {
	for c.size <= c.mu.sizeHot+c.mu.sizeCold && c.mu.handCold != nil {
		c.runHandCold()
	}
}

func (c *tableCacheShard) runHandCold() {
	n := c.mu.handCold
	if n.ptype == tableCacheNodeCold {
		if atomic.LoadInt32(&n.referenced) == 1 {
			atomic.StoreInt32(&n.referenced, 0)
			n.ptype = tableCacheNodeHot
			c.mu.sizeCold--
			c.mu.sizeHot++
		} else {
			c.clearNode(n)
			n.ptype = tableCacheNodeTest
			c.mu.sizeCold--
			c.mu.sizeTest++
			for c.size < c.mu.sizeTest && c.mu.handTest != nil {
				c.runHandTest()
			}
		}
	}

	c.mu.handCold = c.mu.handCold.next()

	for c.size-c.mu.coldTarget <= c.mu.sizeHot && c.mu.handHot != nil {
		c.runHandHot()
	}
}

func (c *tableCacheShard) runHandHot() {
	if c.mu.handHot == c.mu.handTest && c.mu.handTest != nil {
		c.runHandTest()
		if c.mu.handHot == nil {
			return
		}
	}

	n := c.mu.handHot
	if n.ptype == tableCacheNodeHot {
		if atomic.LoadInt32(&n.referenced) == 1 {
			atomic.StoreInt32(&n.referenced, 0)
		} else {
			n.ptype = tableCacheNodeCold
			c.mu.sizeHot--
			c.mu.sizeCold++
		}
	}

	c.mu.handHot = c.mu.handHot.next()
}

func (c *tableCacheShard) runHandTest() {
	if c.mu.sizeCold > 0 && c.mu.handTest == c.mu.handCold && c.mu.handCold != nil {
		c.runHandCold()
		if c.mu.handTest == nil {
			return
		}
	}

	n := c.mu.handTest
	if n.ptype == tableCacheNodeTest {
		c.mu.coldTarget--
		if c.mu.coldTarget < 0 {
			c.mu.coldTarget = 0
		}
		c.unlinkNode(n)
		c.clearNode(n)
	}

	c.mu.handTest = c.mu.handTest.next()
}

func (c *tableCacheShard) evict(fileNum FileNum, dbOpts *tableCacheOpts, allowLeak bool) {
	c.mu.Lock()
	key := tableCacheKey{dbOpts.cacheID, fileNum}
	n := c.mu.nodes[key]
	var v *tableCacheValue
	if n != nil {
		// NB: This is equivalent to tableCacheShard.releaseNode(), but we perform
		// the tableCacheNode.release() call synchronously below to ensure the
		// sstable file descriptor is closed before returning. Note that
		// tableCacheShard.releasing needs to be incremented while holding
		// tableCacheShard.mu in order to avoid a race with Close()
		c.unlinkNode(n)
		v = n.value
		if v != nil {
			if !allowLeak {
				if t := atomic.AddInt32(&v.refCount, -1); t != 0 {
					dbOpts.logger.Fatalf("sstable %s: refcount is not zero: %d\n%s", fileNum, t, debug.Stack())
				}
			}
			c.releasing.Add(1)
		}
	}

	c.mu.Unlock()

	if v != nil {
		v.release(c)
	}

	dbOpts.opts.Cache.EvictFile(dbOpts.cacheID, fileNum)
}

// removeDB evicts any nodes which have a reference to the DB
// associated with dbOpts.cacheID. Make sure that there will
// be no more accesses to the files associated with the DB.
func (c *tableCacheShard) removeDB(dbOpts *tableCacheOpts) {
	var fileNums []base.FileNum

	c.mu.RLock()
	// Collect the fileNums which need to be cleaned.
	var firstNode *tableCacheNode
	node := c.mu.handHot
	for node != firstNode {
		if firstNode == nil {
			firstNode = node
		}

		if node.cacheID == dbOpts.cacheID {
			fileNums = append(fileNums, node.meta.FileNum)
		}
		node = node.next()
	}
	c.mu.RUnlock()

	// Evict all the nodes associated with the DB.
	// This should synchronously close all the files
	// associated with the DB.
	for _, fNum := range fileNums {
		c.evict(fNum, dbOpts, true)
	}
}

func (c *tableCacheShard) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check for leaked iterators. Note that we'll still perform cleanup below in
	// the case that there are leaked iterators.
	var err error
	if v := atomic.LoadInt32(&c.atomic.iterCount); v > 0 {
		if !invariants.RaceEnabled {
			err = errors.Errorf("leaked iterators: %d", errors.Safe(v))
		} else {
			var buf bytes.Buffer
			for _, stack := range c.mu.iters {
				fmt.Fprintf(&buf, "%s\n", stack)
			}
			err = errors.Errorf("leaked iterators: %d\n%s", errors.Safe(v), buf.String())
		}
	}

	for c.mu.handHot != nil {
		n := c.mu.handHot
		if n.value != nil {
			if atomic.AddInt32(&n.value.refCount, -1) == 0 {
				c.releasing.Add(1)
				c.releasingCh <- n.value
			}
		}
		c.unlinkNode(n)
	}
	c.mu.nodes = nil
	c.mu.handHot = nil
	c.mu.handCold = nil
	c.mu.handTest = nil

	// Only shutdown the releasing goroutine if there were no leaked
	// iterators. If there were leaked iterators, we leave the goroutine running
	// and the releasingCh open so that a subsequent iterator close can
	// complete. This behavior is used by iterator leak tests. Leaking the
	// goroutine for these tests is less bad not closing the iterator which
	// triggers other warnings about block cache handles not being released.
	if err != nil {
		c.releasing.Wait()
		return err
	}

	close(c.releasingCh)
	c.releasing.Wait()
	c.releaseLoopExit.Wait()
	return err
}

type tableCacheValue struct {
	closeHook func(i sstable.Iterator) error
	reader    *sstable.Reader
	filename  string
	err       error
	loaded    chan struct{}
	// Reference count for the value. The reader is closed when the reference
	// count drops to zero.
	refCount int32
}

func (v *tableCacheValue) load(meta *fileMetadata, c *tableCacheShard, dbOpts *tableCacheOpts) {
	// Try opening the fileTypeTable first.
	var f vfs.File
	v.filename = base.MakeFilepath(dbOpts.fs, dbOpts.dirname, fileTypeTable, meta.FileNum)
	f, v.err = dbOpts.fs.Open(v.filename, vfs.RandomReadsOption)
	if v.err == nil {
		cacheOpts := private.SSTableCacheOpts(dbOpts.cacheID, meta.FileNum).(sstable.ReaderOption)
		reopenOpt := sstable.FileReopenOpt{FS: dbOpts.fs, Filename: v.filename}
		v.reader, v.err = sstable.NewReader(f, dbOpts.opts, cacheOpts, dbOpts.filterMetrics, reopenOpt)
	}
	if v.err == nil {
		if meta.SmallestSeqNum == meta.LargestSeqNum {
			v.reader.Properties.GlobalSeqNum = meta.LargestSeqNum
		}
	}
	if v.err != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		// Lookup the node in the cache again as it might have already been
		// removed.
		key := tableCacheKey{dbOpts.cacheID, meta.FileNum}
		n := c.mu.nodes[key]
		if n != nil && n.value == v {
			c.releaseNode(n)
		}
	}
	close(v.loaded)
}

func (v *tableCacheValue) release(c *tableCacheShard) {
	<-v.loaded
	// Nothing to be done about an error at this point. Close the reader if it is
	// open.
	if v.reader != nil {
		_ = v.reader.Close()
	}
	c.releasing.Done()
}

type tableCacheNodeType int8

const (
	tableCacheNodeTest tableCacheNodeType = iota
	tableCacheNodeCold
	tableCacheNodeHot
)

func (p tableCacheNodeType) String() string {
	switch p {
	case tableCacheNodeTest:
		return "test"
	case tableCacheNodeCold:
		return "cold"
	case tableCacheNodeHot:
		return "hot"
	}
	return "unknown"
}

type tableCacheNode struct {
	meta  *fileMetadata
	value *tableCacheValue

	links struct {
		next *tableCacheNode
		prev *tableCacheNode
	}
	ptype tableCacheNodeType
	// referenced is atomically set to indicate that this entry has been accessed
	// since the last time one of the clock hands swept it.
	referenced int32

	// Storing the cache id associated with the DB instance here
	// avoids the need to thread the dbOpts struct through many functions.
	cacheID uint64
}

func (n *tableCacheNode) next() *tableCacheNode {
	if n == nil {
		return nil
	}
	return n.links.next
}

func (n *tableCacheNode) prev() *tableCacheNode {
	if n == nil {
		return nil
	}
	return n.links.prev
}

func (n *tableCacheNode) link(s *tableCacheNode) {
	s.links.prev = n.links.prev
	s.links.prev.links.next = s
	s.links.next = n
	s.links.next.links.prev = s
}

func (n *tableCacheNode) unlink() *tableCacheNode {
	next := n.links.next
	n.links.prev.links.next = n.links.next
	n.links.next.links.prev = n.links.prev
	n.links.prev = n
	n.links.next = n
	return next
}
