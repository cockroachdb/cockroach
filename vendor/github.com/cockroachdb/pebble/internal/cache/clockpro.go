// Copyright 2018. All rights reserved. Use of this source code is governed by
// an MIT-style license that can be found in the LICENSE file.

// Package cache implements the CLOCK-Pro caching algorithm.
//
// CLOCK-Pro is a patent-free alternative to the Adaptive Replacement Cache,
// https://en.wikipedia.org/wiki/Adaptive_replacement_cache.
// It is an approximation of LIRS ( https://en.wikipedia.org/wiki/LIRS_caching_algorithm ),
// much like the CLOCK page replacement algorithm is an approximation of LRU.
//
// This implementation is based on the python code from https://bitbucket.org/SamiLehtinen/pyclockpro .
//
// Slides describing the algorithm: http://fr.slideshare.net/huliang64/clockpro
//
// The original paper: http://static.usenix.org/event/usenix05/tech/general/full_papers/jiang/jiang_html/html.html
//
// It is MIT licensed, like the original.
package cache // import "github.com/cockroachdb/pebble/internal/cache"

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
)

type fileKey struct {
	// id is the namespace for fileNums.
	id      uint64
	fileNum base.FileNum
}

type key struct {
	fileKey
	offset uint64
}

// file returns the "file key" for the receiver. This is the key used for the
// shard.files map.
func (k key) file() key {
	k.offset = 0
	return k
}

func (k key) String() string {
	return fmt.Sprintf("%d/%d/%d", k.id, k.fileNum, k.offset)
}

// Handle provides a strong reference to a value in the cache. The reference
// does not pin the value in the cache, but it does prevent the underlying byte
// slice from being reused.
type Handle struct {
	value *Value
}

// Get returns the value stored in handle.
func (h Handle) Get() []byte {
	if h.value != nil {
		// NB: We don't increment shard.hits in this code path because we only want
		// to record a hit when the handle is retrieved from the cache.
		return h.value.buf
	}
	return nil
}

// Release releases the reference to the cache entry.
func (h Handle) Release() {
	if h.value != nil {
		h.value.release()
	}
}

type shard struct {
	hits   int64
	misses int64

	mu sync.RWMutex

	reservedSize int64
	maxSize      int64
	coldTarget   int64
	blocks       robinHoodMap // fileNum+offset -> block
	files        robinHoodMap // fileNum -> list of blocks

	// The blocks and files maps store values in manually managed memory that is
	// invisible to the Go GC. This is fine for Value and entry objects that are
	// stored in manually managed memory, but when the "invariants" build tag is
	// set, all Value and entry objects are Go allocated and the entries map will
	// contain a reference to every entry.
	entries map[*entry]struct{}

	handHot  *entry
	handCold *entry
	handTest *entry

	sizeHot  int64
	sizeCold int64
	sizeTest int64

	// The count fields are used exclusively for asserting expectations.
	// We've seen infinite looping (cockroachdb/cockroach#70154) that
	// could be explained by a corrupted sizeCold. Through asserting on
	// these fields, we hope to gain more insight from any future
	// reproductions.
	countHot  int64
	countCold int64
	countTest int64
}

func (c *shard) Get(id uint64, fileNum base.FileNum, offset uint64) Handle {
	c.mu.RLock()
	var value *Value
	if e := c.blocks.Get(key{fileKey{id, fileNum}, offset}); e != nil {
		value = e.acquireValue()
		if value != nil {
			atomic.StoreInt32(&e.referenced, 1)
		}
	}
	c.mu.RUnlock()
	if value == nil {
		atomic.AddInt64(&c.misses, 1)
		return Handle{}
	}
	atomic.AddInt64(&c.hits, 1)
	return Handle{value: value}
}

func (c *shard) Set(id uint64, fileNum base.FileNum, offset uint64, value *Value) Handle {
	if n := value.refs(); n != 1 {
		panic(fmt.Sprintf("pebble: Value has already been added to the cache: refs=%d", n))
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	k := key{fileKey{id, fileNum}, offset}
	e := c.blocks.Get(k)

	switch {
	case e == nil:
		// no cache entry? add it
		e = newEntry(c, k, int64(len(value.buf)))
		e.setValue(value)
		if c.metaAdd(k, e) {
			value.ref.trace("add-cold")
			c.sizeCold += e.size
			c.countCold++
		} else {
			value.ref.trace("skip-cold")
			e.free()
			e = nil
		}

	case e.peekValue() != nil:
		// cache entry was a hot or cold page
		e.setValue(value)
		atomic.StoreInt32(&e.referenced, 1)
		delta := int64(len(value.buf)) - e.size
		e.size = int64(len(value.buf))
		if e.ptype == etHot {
			value.ref.trace("add-hot")
			c.sizeHot += delta
		} else {
			value.ref.trace("add-cold")
			c.sizeCold += delta
		}
		c.evict()

	default:
		// cache entry was a test page
		c.sizeTest -= e.size
		c.countTest--
		c.metaDel(e)
		c.metaCheck(e)

		e.size = int64(len(value.buf))
		c.coldTarget += e.size
		if c.coldTarget > c.targetSize() {
			c.coldTarget = c.targetSize()
		}

		atomic.StoreInt32(&e.referenced, 0)
		e.setValue(value)
		e.ptype = etHot
		if c.metaAdd(k, e) {
			value.ref.trace("add-hot")
			c.sizeHot += e.size
			c.countHot++
		} else {
			value.ref.trace("skip-hot")
			e.free()
			e = nil
		}
	}

	c.checkConsistency()

	// Values are initialized with a reference count of 1. That reference count
	// is being transferred to the returned Handle.
	return Handle{value: value}
}

func (c *shard) checkConsistency() {
	// See the comment above the count{Hot,Cold,Test} fields.
	switch {
	case c.sizeHot < 0 || c.sizeCold < 0 || c.sizeTest < 0 || c.countHot < 0 || c.countCold < 0 || c.countTest < 0:
		panic(fmt.Sprintf("pebble: unexpected negative: %d (%d bytes) hot, %d (%d bytes) cold, %d (%d bytes) test",
			c.countHot, c.sizeHot, c.countCold, c.sizeCold, c.countTest, c.sizeTest))
	case c.sizeHot > 0 && c.countHot == 0:
		panic(fmt.Sprintf("pebble: mismatch %d hot size, %d hot count", c.sizeHot, c.countHot))
	case c.sizeCold > 0 && c.countCold == 0:
		panic(fmt.Sprintf("pebble: mismatch %d cold size, %d cold count", c.sizeCold, c.countCold))
	case c.sizeTest > 0 && c.countTest == 0:
		panic(fmt.Sprintf("pebble: mismatch %d test size, %d test count", c.sizeTest, c.countTest))
	}
}

// Delete deletes the cached value for the specified file and offset.
func (c *shard) Delete(id uint64, fileNum base.FileNum, offset uint64) {
	// The common case is there is nothing to delete, so do a quick check with
	// shared lock.
	k := key{fileKey{id, fileNum}, offset}
	c.mu.RLock()
	exists := c.blocks.Get(k) != nil
	c.mu.RUnlock()
	if !exists {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	e := c.blocks.Get(k)
	if e == nil {
		return
	}
	c.metaEvict(e)

	c.checkConsistency()
}

// EvictFile evicts all of the cache values for the specified file.
func (c *shard) EvictFile(id uint64, fileNum base.FileNum) {
	c.mu.Lock()
	defer c.mu.Unlock()

	fkey := key{fileKey{id, fileNum}, 0}
	blocks := c.files.Get(fkey)
	if blocks == nil {
		return
	}
	for b, n := blocks, (*entry)(nil); ; b = n {
		n = b.fileLink.next
		c.metaEvict(b)
		if b == n {
			break
		}
	}

	c.checkConsistency()
}

func (c *shard) Free() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// NB: we use metaDel rather than metaEvict in order to avoid the expensive
	// metaCheck call when the "invariants" build tag is specified.
	for c.handHot != nil {
		e := c.handHot
		c.metaDel(c.handHot)
		e.free()
	}

	c.blocks.free()
	c.files.free()
}

func (c *shard) Reserve(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reservedSize += int64(n)

	// Changing c.reservedSize will either increase or decrease
	// the targetSize. But we want coldTarget to be in the range
	// [0, targetSize]. So, if c.targetSize decreases, make sure
	// that the coldTarget fits within the limits.
	targetSize := c.targetSize()
	if c.coldTarget > targetSize {
		c.coldTarget = targetSize
	}

	c.evict()
	c.checkConsistency()
}

// Size returns the current space used by the cache.
func (c *shard) Size() int64 {
	c.mu.RLock()
	size := c.sizeHot + c.sizeCold
	c.mu.RUnlock()
	return size
}

func (c *shard) targetSize() int64 {
	target := c.maxSize - c.reservedSize
	// Always return a positive integer for targetSize. This is so that we don't
	// end up in an infinite loop in evict(), in cases where reservedSize is
	// greater than or equal to maxSize.
	if target < 1 {
		return 1
	}
	return target
}

// Add the entry to the cache, returning true if the entry was added and false
// if it would not fit in the cache.
func (c *shard) metaAdd(key key, e *entry) bool {
	c.evict()
	if e.size > c.targetSize() {
		// The entry is larger than the target cache size.
		return false
	}

	c.blocks.Put(key, e)
	if entriesGoAllocated {
		// Go allocated entries need to be referenced from Go memory. The entries
		// map provides that reference.
		c.entries[e] = struct{}{}
	}

	if c.handHot == nil {
		// first element
		c.handHot = e
		c.handCold = e
		c.handTest = e
	} else {
		c.handHot.link(e)
	}

	if c.handCold == c.handHot {
		c.handCold = c.handCold.prev()
	}

	fkey := key.file()
	if fileBlocks := c.files.Get(fkey); fileBlocks == nil {
		c.files.Put(fkey, e)
	} else {
		fileBlocks.linkFile(e)
	}
	return true
}

// Remove the entry from the cache. This removes the entry from the blocks map,
// the files map, and ensures that hand{Hot,Cold,Test} are not pointing at the
// entry.
func (c *shard) metaDel(e *entry) {
	if value := e.peekValue(); value != nil {
		value.ref.trace("metaDel")
	}
	e.setValue(nil)

	c.blocks.Delete(e.key)
	if entriesGoAllocated {
		// Go allocated entries need to be referenced from Go memory. The entries
		// map provides that reference.
		delete(c.entries, e)
	}

	if e == c.handHot {
		c.handHot = c.handHot.prev()
	}
	if e == c.handCold {
		c.handCold = c.handCold.prev()
	}
	if e == c.handTest {
		c.handTest = c.handTest.prev()
	}

	if e.unlink() == e {
		// This was the last entry in the cache.
		c.handHot = nil
		c.handCold = nil
		c.handTest = nil
	}

	fkey := e.key.file()
	if next := e.unlinkFile(); e == next {
		c.files.Delete(fkey)
	} else {
		c.files.Put(fkey, next)
	}
}

// Check that the specified entry is not referenced by the cache.
func (c *shard) metaCheck(e *entry) {
	if invariants.Enabled {
		if _, ok := c.entries[e]; ok {
			fmt.Fprintf(os.Stderr, "%p: %s unexpectedly found in entries map\n%s",
				e, e.key, debug.Stack())
			os.Exit(1)
		}
		if c.blocks.findByValue(e) != nil {
			fmt.Fprintf(os.Stderr, "%p: %s unexpectedly found in blocks map\n%s\n%s",
				e, e.key, &c.blocks, debug.Stack())
			os.Exit(1)
		}
		if c.files.findByValue(e) != nil {
			fmt.Fprintf(os.Stderr, "%p: %s unexpectedly found in files map\n%s\n%s",
				e, e.key, &c.files, debug.Stack())
			os.Exit(1)
		}
		// NB: c.hand{Hot,Cold,Test} are pointers into a single linked list. We
		// only have to traverse one of them to check all of them.
		var countHot, countCold, countTest int64
		var sizeHot, sizeCold, sizeTest int64
		for t := c.handHot.next(); t != nil; t = t.next() {
			// Recompute count{Hot,Cold,Test} and size{Hot,Cold,Test}.
			switch t.ptype {
			case etHot:
				countHot++
				sizeHot += t.size
			case etCold:
				countCold++
				sizeCold += t.size
			case etTest:
				countTest++
				sizeTest += t.size
			}
			if e == t {
				fmt.Fprintf(os.Stderr, "%p: %s unexpectedly found in blocks list\n%s",
					e, e.key, debug.Stack())
				os.Exit(1)
			}
			if t == c.handHot {
				break
			}
		}
		if countHot != c.countHot || countCold != c.countCold || countTest != c.countTest ||
			sizeHot != c.sizeHot || sizeCold != c.sizeCold || sizeTest != c.sizeTest {
			fmt.Fprintf(os.Stderr, `divergence of Hot,Cold,Test statistics
				cache's statistics: hot %d, %d, cold %d, %d, test %d, %d
				recalculated statistics: hot %d, %d, cold %d, %d, test %d, %d\n%s`,
				c.countHot, c.sizeHot, c.countCold, c.sizeCold, c.countTest, c.sizeTest,
				countHot, sizeHot, countCold, sizeCold, countTest, sizeTest,
				debug.Stack())
			os.Exit(1)
		}
	}
}

func (c *shard) metaEvict(e *entry) {
	switch e.ptype {
	case etHot:
		c.sizeHot -= e.size
		c.countHot--
	case etCold:
		c.sizeCold -= e.size
		c.countCold--
	case etTest:
		c.sizeTest -= e.size
		c.countTest--
	}
	c.metaDel(e)
	c.metaCheck(e)
	e.free()
}

func (c *shard) evict() {
	for c.targetSize() <= c.sizeHot+c.sizeCold && c.handCold != nil {
		c.runHandCold(c.countCold, c.sizeCold)
	}
}

func (c *shard) runHandCold(countColdDebug, sizeColdDebug int64) {
	// countColdDebug and sizeColdDebug should equal c.countCold and
	// c.sizeCold. They're parameters only to aid in debugging of
	// cockroachdb/cockroach#70154. Since they're parameters, their
	// arguments will appear within stack traces should we encounter
	// a reproduction.
	if c.countCold != countColdDebug || c.sizeCold != sizeColdDebug {
		panic(fmt.Sprintf("runHandCold: cold count and size are %d, %d, arguments are %d and %d",
			c.countCold, c.sizeCold, countColdDebug, sizeColdDebug))
	}

	e := c.handCold
	if e.ptype == etCold {
		if atomic.LoadInt32(&e.referenced) == 1 {
			atomic.StoreInt32(&e.referenced, 0)
			e.ptype = etHot
			c.sizeCold -= e.size
			c.countCold--
			c.sizeHot += e.size
			c.countHot++
		} else {
			e.setValue(nil)
			e.ptype = etTest
			c.sizeCold -= e.size
			c.countCold--
			c.sizeTest += e.size
			c.countTest++
			for c.targetSize() < c.sizeTest && c.handTest != nil {
				c.runHandTest()
			}
		}
	}

	c.handCold = c.handCold.next()

	for c.targetSize()-c.coldTarget <= c.sizeHot && c.handHot != nil {
		c.runHandHot()
	}
}

func (c *shard) runHandHot() {
	if c.handHot == c.handTest && c.handTest != nil {
		c.runHandTest()
		if c.handHot == nil {
			return
		}
	}

	e := c.handHot
	if e.ptype == etHot {
		if atomic.LoadInt32(&e.referenced) == 1 {
			atomic.StoreInt32(&e.referenced, 0)
		} else {
			e.ptype = etCold
			c.sizeHot -= e.size
			c.countHot--
			c.sizeCold += e.size
			c.countCold++
		}
	}

	c.handHot = c.handHot.next()
}

func (c *shard) runHandTest() {
	if c.sizeCold > 0 && c.handTest == c.handCold && c.handCold != nil {
		// sizeCold is > 0, so assert that countCold == 0. See the
		// comment above count{Hot,Cold,Test}.
		if c.countCold == 0 {
			panic(fmt.Sprintf("pebble: mismatch %d cold size, %d cold count", c.sizeCold, c.countCold))
		}

		c.runHandCold(c.countCold, c.sizeCold)
		if c.handTest == nil {
			return
		}
	}

	e := c.handTest
	if e.ptype == etTest {
		c.sizeTest -= e.size
		c.countTest--
		c.coldTarget -= e.size
		if c.coldTarget < 0 {
			c.coldTarget = 0
		}
		c.metaDel(e)
		c.metaCheck(e)
		e.free()
	}

	c.handTest = c.handTest.next()
}

// Metrics holds metrics for the cache.
type Metrics struct {
	// The number of bytes inuse by the cache.
	Size int64
	// The count of objects (blocks or tables) in the cache.
	Count int64
	// The number of cache hits.
	Hits int64
	// The number of cache misses.
	Misses int64
}

// Cache implements Pebble's sharded block cache. The Clock-PRO algorithm is
// used for page replacement
// (http://static.usenix.org/event/usenix05/tech/general/full_papers/jiang/jiang_html/html.html). In
// order to provide better concurrency, 2 x NumCPUs shards are created, with
// each shard being given 1/n of the target cache size. The Clock-PRO algorithm
// is run independently on each shard.
//
// Blocks are keyed by an (id, fileNum, offset) triple. The ID is a namespace
// for file numbers and allows a single Cache to be shared between multiple
// Pebble instances. The fileNum and offset refer to an sstable file number and
// the offset of the block within the file. Because sstables are immutable and
// file numbers are never reused, (fileNum,offset) are unique for the lifetime
// of a Pebble instance.
//
// In addition to maintaining a map from (fileNum,offset) to data, each shard
// maintains a map of the cached blocks for a particular fileNum. This allows
// efficient eviction of all of the blocks for a file which is used when an
// sstable is deleted from disk.
//
// Memory Management
//
// In order to reduce pressure on the Go GC, manual memory management is
// performed for the data stored in the cache. Manual memory management is
// performed by calling into C.{malloc,free} to allocate memory. Cache.Values
// are reference counted and the memory backing a manual value is freed when
// the reference count drops to 0.
//
// Manual memory management brings the possibility of memory leaks. It is
// imperative that every Handle returned by Cache.{Get,Set} is eventually
// released. The "invariants" build tag enables a leak detection facility that
// places a GC finalizer on cache.Value. When the cache.Value finalizer is run,
// if the underlying buffer is still present a leak has occurred. The "tracing"
// build tag enables tracing of cache.Value reference count manipulation and
// eases finding where a leak has occurred. These two facilities are usually
// used in combination by specifying `-tags invariants,tracing`. Note that
// "tracing" produces a significant slowdown, while "invariants" does not.
type Cache struct {
	refs    int64
	maxSize int64
	idAlloc uint64
	shards  []shard

	// Traces recorded by Cache.trace. Used for debugging.
	tr struct {
		sync.Mutex
		msgs []string
	}
}

// New creates a new cache of the specified size. Memory for the cache is
// allocated on demand, not during initialization. The cache is created with a
// reference count of 1. Each DB it is associated with adds a reference, so the
// creator of the cache should usually release their reference after the DB is
// created.
//
//   c := cache.New(...)
//   defer c.Unref()
//   d, err := pebble.Open(pebble.Options{Cache: c})
func New(size int64) *Cache {
	return newShards(size, 2*runtime.GOMAXPROCS(0))
}

func newShards(size int64, shards int) *Cache {
	c := &Cache{
		refs:    1,
		maxSize: size,
		idAlloc: 1,
		shards:  make([]shard, shards),
	}
	c.trace("alloc", c.refs)
	for i := range c.shards {
		c.shards[i] = shard{
			maxSize:    size / int64(len(c.shards)),
			coldTarget: size / int64(len(c.shards)),
		}
		if entriesGoAllocated {
			c.shards[i].entries = make(map[*entry]struct{})
		}
		c.shards[i].blocks.init(16)
		c.shards[i].files.init(16)
	}

	// Note: this is a no-op if invariants are disabled or race is enabled.
	invariants.SetFinalizer(c, func(obj interface{}) {
		c := obj.(*Cache)
		if v := atomic.LoadInt64(&c.refs); v != 0 {
			c.tr.Lock()
			fmt.Fprintf(os.Stderr,
				"pebble: cache (%p) has non-zero reference count: %d\n", c, v)
			if len(c.tr.msgs) > 0 {
				fmt.Fprintf(os.Stderr, "%s\n", strings.Join(c.tr.msgs, "\n"))
			}
			c.tr.Unlock()
			os.Exit(1)
		}
	})
	return c
}

func (c *Cache) getShard(id uint64, fileNum base.FileNum, offset uint64) *shard {
	if id == 0 {
		panic("pebble: 0 cache ID is invalid")
	}

	// Inlined version of fnv.New64 + Write.
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211

	h := uint64(offset64)
	for i := 0; i < 8; i++ {
		h *= prime64
		h ^= uint64(id & 0xff)
		id >>= 8
	}
	for i := 0; i < 8; i++ {
		h *= prime64
		h ^= uint64(fileNum & 0xff)
		fileNum >>= 8
	}
	for i := 0; i < 8; i++ {
		h *= prime64
		h ^= uint64(offset & 0xff)
		offset >>= 8
	}

	return &c.shards[h%uint64(len(c.shards))]
}

// Ref adds a reference to the cache. The cache only remains valid as long a
// reference is maintained to it.
func (c *Cache) Ref() {
	v := atomic.AddInt64(&c.refs, 1)
	if v <= 1 {
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	}
	c.trace("ref", v)
}

// Unref releases a reference on the cache.
func (c *Cache) Unref() {
	v := atomic.AddInt64(&c.refs, -1)
	c.trace("unref", v)
	switch {
	case v < 0:
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	case v == 0:
		for i := range c.shards {
			c.shards[i].Free()
		}
	}
}

// Get retrieves the cache value for the specified file and offset, returning
// nil if no value is present.
func (c *Cache) Get(id uint64, fileNum base.FileNum, offset uint64) Handle {
	return c.getShard(id, fileNum, offset).Get(id, fileNum, offset)
}

// Set sets the cache value for the specified file and offset, overwriting an
// existing value if present. A Handle is returned which provides faster
// retrieval of the cached value than Get (lock-free and avoidance of the map
// lookup). The value must have been allocated by Cache.Alloc.
func (c *Cache) Set(id uint64, fileNum base.FileNum, offset uint64, value *Value) Handle {
	return c.getShard(id, fileNum, offset).Set(id, fileNum, offset, value)
}

// Delete deletes the cached value for the specified file and offset.
func (c *Cache) Delete(id uint64, fileNum base.FileNum, offset uint64) {
	c.getShard(id, fileNum, offset).Delete(id, fileNum, offset)
}

// EvictFile evicts all of the cache values for the specified file.
func (c *Cache) EvictFile(id uint64, fileNum base.FileNum) {
	if id == 0 {
		panic("pebble: 0 cache ID is invalid")
	}
	for i := range c.shards {
		c.shards[i].EvictFile(id, fileNum)
	}
}

// MaxSize returns the max size of the cache.
func (c *Cache) MaxSize() int64 {
	return c.maxSize
}

// Size returns the current space used by the cache.
func (c *Cache) Size() int64 {
	var size int64
	for i := range c.shards {
		size += c.shards[i].Size()
	}
	return size
}

// Alloc allocates a byte slice of the specified size, possibly reusing
// previously allocated but unused memory. The memory backing the value is
// manually managed. The caller MUST either add the value to the cache (via
// Cache.Set), or release the value (via Cache.Free). Failure to do so will
// result in a memory leak.
func (c *Cache) Alloc(n int) *Value {
	return newValue(n)
}

// Free frees the specified value. The buffer associated with the value will
// possibly be reused, making it invalid to use the buffer after calling
// Free. Do not call Free on a value that has been added to the cache.
func (c *Cache) Free(v *Value) {
	if n := v.refs(); n > 1 {
		panic(fmt.Sprintf("pebble: Value has been added to the cache: refs=%d", n))
	}
	v.release()
}

// Reserve N bytes in the cache. This effectively shrinks the size of the cache
// by N bytes, without actually consuming any memory. The returned closure
// should be invoked to release the reservation.
func (c *Cache) Reserve(n int) func() {
	// Round-up the per-shard reservation. Most reservations should be large, so
	// this probably doesn't matter in practice.
	shardN := (n + len(c.shards) - 1) / len(c.shards)
	for i := range c.shards {
		c.shards[i].Reserve(shardN)
	}
	return func() {
		if shardN == -1 {
			panic("pebble: cache reservation already released")
		}
		for i := range c.shards {
			c.shards[i].Reserve(-shardN)
		}
		shardN = -1
	}
}

// Metrics returns the metrics for the cache.
func (c *Cache) Metrics() Metrics {
	var m Metrics
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.RLock()
		m.Count += int64(s.blocks.Count())
		m.Size += s.sizeHot + s.sizeCold
		s.mu.RUnlock()
		m.Hits += atomic.LoadInt64(&s.hits)
		m.Misses += atomic.LoadInt64(&s.misses)
	}
	return m
}

// NewID returns a new ID to be used as a namespace for cached file
// blocks.
func (c *Cache) NewID() uint64 {
	return atomic.AddUint64(&c.idAlloc, 1)
}
