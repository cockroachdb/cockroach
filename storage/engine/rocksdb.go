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
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
// Author: Jiang-Ming Yang (jiangming.yang@gmail.com)

package engine

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"unsafe"

	"golang.org/x/net/context"

	"github.com/dustin/go-humanize"
	"github.com/elastic/gosigar"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/storage/engine/rocksdb"
	"github.com/cockroachdb/cockroach/util/envutil"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/humanizeutil"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
// #cgo linux LDFLAGS: -lrt
//
// #include <stdlib.h>
// #include "rocksdb/db.h"
import "C"

const (
	minMemtableBudget = 1 << 20  // 1 MB
	defaultBlockSize  = 32 << 10 // 32KB (rocksdb default is 4KB)

	// DefaultMaxOpenFiles is the default value for rocksDB's max_open_files
	// option.
	DefaultMaxOpenFiles = 5000
	// MinimumMaxOpenFiles is The minimum value that rocksDB's max_open_files
	// option can be set to.
	MinimumMaxOpenFiles = 256
)

func init() {
	rocksdb.Logger = func(format string, args ...interface{}) { log.Infof(context.TODO(), format, args...) }
}

// SSTableInfo contains metadata about a single RocksDB sstable. This mirrors
// the C.DBSSTable struct contents.
type SSTableInfo struct {
	Level int
	Size  int64
	Start MVCCKey
	End   MVCCKey
}

// SSTableInfos is a slice of SSTableInfo structures.
type SSTableInfos []SSTableInfo

func (s SSTableInfos) Len() int {
	return len(s)
}

func (s SSTableInfos) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SSTableInfos) Less(i, j int) bool {
	switch {
	case s[i].Level < s[j].Level:
		return true
	case s[i].Level > s[j].Level:
		return false
	case s[i].Size > s[j].Size:
		return true
	case s[i].Size < s[j].Size:
		return false
	default:
		return s[i].Start.Less(s[j].Start)
	}
}

func (s SSTableInfos) String() string {
	const (
		KB = 1 << 10
		MB = 1 << 20
		GB = 1 << 30
		TB = 1 << 40
	)

	roundTo := func(val, to int64) int64 {
		return (val + to/2) / to
	}

	// We're intentionally not using humanizeutil here as we want a slightly more
	// compact representation.
	humanize := func(size int64) string {
		switch {
		case size < MB:
			return fmt.Sprintf("%dK", roundTo(size, KB))
		case size < GB:
			return fmt.Sprintf("%dM", roundTo(size, MB))
		case size < TB:
			return fmt.Sprintf("%dG", roundTo(size, GB))
		default:
			return fmt.Sprintf("%dT", roundTo(size, TB))
		}
	}

	type levelInfo struct {
		size  int64
		count int
	}

	var levels []*levelInfo
	for _, t := range s {
		for i := len(levels); i <= t.Level; i++ {
			levels = append(levels, &levelInfo{})
		}
		info := levels[t.Level]
		info.size += t.Size
		info.count++
	}

	var maxSize int
	var maxLevelCount int
	for _, info := range levels {
		size := len(humanize(info.size))
		if maxSize < size {
			maxSize = size
		}
		count := 1 + int(math.Log10(float64(info.count)))
		if maxLevelCount < count {
			maxLevelCount = count
		}
	}
	levelFormat := fmt.Sprintf("%%d [ %%%ds %%%dd ]:", maxSize, maxLevelCount)

	level := -1
	var buf bytes.Buffer
	var lastSize string
	var lastSizeCount int

	flushLastSize := func() {
		if lastSizeCount > 0 {
			fmt.Fprintf(&buf, " %s", lastSize)
			if lastSizeCount > 1 {
				fmt.Fprintf(&buf, "[%d]", lastSizeCount)
			}
			lastSizeCount = 0
		}
	}

	maybeFlush := func(newLevel, i int) {
		if level == newLevel {
			return
		}
		flushLastSize()
		if buf.Len() > 0 {
			buf.WriteString("\n")
		}
		level = newLevel
		if level >= 0 {
			info := levels[level]
			fmt.Fprintf(&buf, levelFormat, level, humanize(info.size), info.count)
		}
	}

	for i, t := range s {
		maybeFlush(t.Level, i)
		size := humanize(t.Size)
		if size == lastSize {
			lastSizeCount++
		} else {
			flushLastSize()
			lastSize = size
			lastSizeCount = 1
		}
	}

	maybeFlush(-1, 0)
	return buf.String()
}

// ReadAmplification returns RocksDB's read amplification, which is the number
// of level-0 sstables plus the number of levels, other than level 0, with at
// least one sstable.
//
// This definition comes from here:
// https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#level-style-compaction
func (s SSTableInfos) ReadAmplification() int {
	var readAmp int
	seenLevel := make(map[int]bool)
	for _, t := range s {
		if t.Level == 0 {
			readAmp++
		} else if !seenLevel[t.Level] {
			readAmp++
			seenLevel[t.Level] = true
		}
	}
	return readAmp
}

// RocksDBCache is a wrapper around C.DBCache
type RocksDBCache struct {
	cache *C.DBCache
}

// NewRocksDBCache creates a new cache of the specified size. Note that the
// cache is refcounted internally and starts out with a refcount of one (i.e.
// Release() should be called after having used the cache).
func NewRocksDBCache(cacheSize int64) RocksDBCache {
	return RocksDBCache{cache: C.DBNewCache(C.uint64_t(cacheSize))}
}

func (c RocksDBCache) ref() RocksDBCache {
	if c.cache != nil {
		c.cache = C.DBRefCache(c.cache)
	}
	return c
}

// Release releases the cache. Note that the cache will continue to be used
// until all of the RocksDB engines it was attached to have been closed, and
// that RocksDB engines which use it auto-release when they close.
func (c RocksDBCache) Release() {
	if c.cache != nil {
		C.DBReleaseCache(c.cache)
	}
}

// RocksDB is a wrapper around a RocksDB database instance.
type RocksDB struct {
	rdb            *C.DBEngine
	attrs          roachpb.Attributes // Attributes for this engine
	dir            string             // The data directory
	cache          RocksDBCache       // Shared cache.
	memtableBudget int64              // Memory to use for the memory table.
	maxSize        int64              // Used for calculating rebalancing and free space.
	maxOpenFiles   int                // The maximum number of open files this instance will use.
	stopper        *stop.Stopper
	deallocated    chan struct{} // Closed when the underlying handle is deallocated.
}

var _ Engine = &RocksDB{}

// NewRocksDB allocates and returns a new RocksDB object.
func NewRocksDB(
	attrs roachpb.Attributes,
	dir string,
	cache RocksDBCache,
	memtableBudget, maxSize int64,
	maxOpenFiles int,
	stopper *stop.Stopper,
) *RocksDB {
	if dir == "" {
		panic("dir must be non-empty")
	}
	return &RocksDB{
		attrs:          attrs,
		dir:            dir,
		cache:          cache.ref(),
		memtableBudget: memtableBudget,
		maxSize:        maxSize,
		maxOpenFiles:   maxOpenFiles,
		stopper:        stopper,
		deallocated:    make(chan struct{}),
	}
}

func newMemRocksDB(
	attrs roachpb.Attributes,
	cache RocksDBCache,
	memtableBudget int64,
	stopper *stop.Stopper,
) *RocksDB {
	return &RocksDB{
		attrs: attrs,
		// dir: empty dir == "mem" RocksDB instance.
		cache:          cache.ref(),
		memtableBudget: memtableBudget,
		maxSize:        memtableBudget,
		stopper:        stopper,
		deallocated:    make(chan struct{}),
	}
}

// String formatter.
func (r *RocksDB) String() string {
	return fmt.Sprintf("%s=%s", r.attrs.Attrs, r.dir)
}

// Open creates options and opens the database. If the database
// doesn't yet exist at the specified directory, one is initialized
// from scratch. The RocksDB Open and Close methods are reference
// counted such that subsequent Open calls to an already opened
// RocksDB instance only bump the reference count. The RocksDB is only
// closed when a sufficient number of Close calls are performed to
// bring the reference count down to 0.
func (r *RocksDB) Open() error {
	if r.rdb != nil {
		return nil
	}

	if r.memtableBudget < minMemtableBudget {
		return errors.Errorf("memtable budget must be at least %s: %s",
			humanize.IBytes(minMemtableBudget), humanizeutil.IBytes(r.memtableBudget))
	}

	var ver storageVersion
	if len(r.dir) != 0 {
		log.Infof(context.TODO(), "opening rocksdb instance at %q", r.dir)

		// Check the version number.
		var err error
		if ver, err = getVersion(r.dir); err != nil {
			return err
		}
		if ver < versionMinimum || ver > versionCurrent {
			// Instead of an error, we should call a migration if possible when
			// one is needed immediately following the DBOpen call.
			return fmt.Errorf("incompatible rocksdb data version, current:%d, on disk:%d, minimum:%d",
				versionCurrent, ver, versionMinimum)
		}
	} else {
		log.Infof(context.TODO(), "opening in memory rocksdb instance")

		// In memory dbs are always current.
		ver = versionCurrent
	}

	status := C.DBOpen(&r.rdb, goToCSlice([]byte(r.dir)),
		C.DBOptions{
			cache:           r.cache.cache,
			memtable_budget: C.uint64_t(r.memtableBudget),
			block_size:      C.uint64_t(envutil.EnvOrDefaultBytes("rocksdb_block_size", defaultBlockSize)),
			wal_ttl_seconds: C.uint64_t(envutil.EnvOrDefaultDuration("rocksdb_wal_ttl", 0).Seconds()),
			allow_os_buffer: C.bool(true),
			logging_enabled: C.bool(log.V(3)),
			num_cpu:         C.int(runtime.NumCPU()),
			max_open_files:  C.int(r.maxOpenFiles),
		})
	if err := statusToError(status); err != nil {
		return errors.Errorf("could not open rocksdb instance: %s", err)
	}

	// Update or add the version file if needed.
	if ver < versionCurrent {
		if err := writeVersionFile(r.dir); err != nil {
			return err
		}
	}

	// Start a goroutine that will finish when the underlying handle
	// is deallocated. This is used to check a leak in tests.
	go func() {
		<-r.deallocated
	}()
	r.stopper.AddCloser(r)
	return nil
}

// Close closes the database by deallocating the underlying handle.
func (r *RocksDB) Close() {
	if r.rdb == nil {
		log.Errorf(context.TODO(), "closing unopened rocksdb instance")
		return
	}
	if len(r.dir) == 0 {
		if log.V(1) {
			log.Infof(context.TODO(), "closing in-memory rocksdb instance")
		}
	} else {
		log.Infof(context.TODO(), "closing rocksdb instance at %q", r.dir)
	}
	if r.rdb != nil {
		C.DBClose(r.rdb)
		r.rdb = nil
	}
	r.cache.Release()
	close(r.deallocated)
}

// closed returns true if the engine is closed.
func (r *RocksDB) closed() bool {
	return r.rdb == nil
}

// Attrs returns the list of attributes describing this engine. This
// may include a specification of disk type (e.g. hdd, ssd, fio, etc.)
// and potentially other labels to identify important attributes of
// the engine.
func (r *RocksDB) Attrs() roachpb.Attributes {
	return r.attrs
}

// Put sets the given key to the value provided.
//
// The key and value byte slices may be reused safely. put takes a copy of
// them before returning.
func (r *RocksDB) Put(key MVCCKey, value []byte) error {
	return dbPut(r.rdb, key, value)
}

// Merge implements the RocksDB merge operator using the function goMergeInit
// to initialize missing values and goMerge to merge the old and the given
// value into a new value, which is then stored under key.
// Currently 64-bit counter logic is implemented. See the documentation of
// goMerge and goMergeInit for details.
//
// The key and value byte slices may be reused safely. merge takes a copy
// of them before returning.
func (r *RocksDB) Merge(key MVCCKey, value []byte) error {
	return dbMerge(r.rdb, key, value)
}

// ApplyBatchRepr atomically applies a set of batched updates. Created by
// calling Repr() on a batch. Using this method is equivalent to constructing
// and committing a batch whose Repr() equals repr.
func (r *RocksDB) ApplyBatchRepr(repr []byte) error {
	return dbApplyBatchRepr(r.rdb, repr)
}

// Get returns the value for the given key.
func (r *RocksDB) Get(key MVCCKey) ([]byte, error) {
	return dbGet(r.rdb, key)
}

// GetProto fetches the value at the specified key and unmarshals it.
func (r *RocksDB) GetProto(key MVCCKey, msg proto.Message) (
	ok bool, keyBytes, valBytes int64, err error) {
	return dbGetProto(r.rdb, key, msg)
}

// Clear removes the item from the db with the given key.
func (r *RocksDB) Clear(key MVCCKey) error {
	return dbClear(r.rdb, key)
}

// Iterate iterates from start to end keys, invoking f on each
// key/value pair. See engine.Iterate for details.
func (r *RocksDB) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (bool, error)) error {
	return dbIterate(r.rdb, r, start, end, f)
}

// Capacity queries the underlying file system for disk capacity information.
func (r *RocksDB) Capacity() (roachpb.StoreCapacity, error) {
	fileSystemUsage := gosigar.FileSystemUsage{}
	dir := r.dir
	if dir == "" {
		// This is an in-memory instance. Pretend we're empty since we
		// don't know better and only use this for testing. Using any
		// part of the actual file system here can throw off allocator
		// rebalancing in a hard-to-trace manner. See #7050.
		return roachpb.StoreCapacity{
			Capacity:  r.maxSize,
			Available: r.maxSize,
		}, nil
	}
	if err := fileSystemUsage.Get(dir); err != nil {
		return roachpb.StoreCapacity{}, err
	}

	if fileSystemUsage.Total > math.MaxInt64 {
		return roachpb.StoreCapacity{}, fmt.Errorf("unsupported disk size %s, max supported size is %s",
			humanize.IBytes(fileSystemUsage.Total), humanizeutil.IBytes(math.MaxInt64))
	}
	if fileSystemUsage.Avail > math.MaxInt64 {
		return roachpb.StoreCapacity{}, fmt.Errorf("unsupported disk size %s, max supported size is %s",
			humanize.IBytes(fileSystemUsage.Avail), humanizeutil.IBytes(math.MaxInt64))
	}
	fsuTotal := int64(fileSystemUsage.Total)
	fsuAvail := int64(fileSystemUsage.Avail)

	// If no size limitation have been placed on the store size or if the
	// limitation is greater than what's available, just return the actual
	// totals.
	if r.maxSize == 0 || r.maxSize >= fsuTotal || r.dir == "" {
		return roachpb.StoreCapacity{
			Capacity:  fsuTotal,
			Available: fsuAvail,
		}, nil
	}

	// Find the total size of all the files in the r.dir and all its
	// subdirectories.
	var totalUsedBytes int64
	if errOuter := filepath.Walk(r.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.Mode().IsRegular() {
			totalUsedBytes += info.Size()
		}
		return nil
	}); errOuter != nil {
		return roachpb.StoreCapacity{}, errOuter
	}

	available := r.maxSize - totalUsedBytes
	if available > fsuAvail {
		available = fsuAvail
	}
	if available < 0 {
		available = 0
	}

	return roachpb.StoreCapacity{
		Capacity:  r.maxSize,
		Available: available,
	}, nil
}

// Compact forces compaction on the database.
func (r *RocksDB) Compact() error {
	return statusToError(C.DBCompact(r.rdb))
}

// Destroy destroys the underlying filesystem data associated with the database.
func (r *RocksDB) Destroy() error {
	return statusToError(C.DBDestroy(goToCSlice([]byte(r.dir))))
}

// Flush causes RocksDB to write all in-memory data to disk immediately.
func (r *RocksDB) Flush() error {
	return statusToError(C.DBFlush(r.rdb))
}

// Checkpoint creates a point-in-time snapshot of the on-disk state.
func (r *RocksDB) Checkpoint(dir string) error {
	if len(r.dir) == 0 {
		return errors.Errorf("unable to checkpoint in-memory rocksdb instance")
	}
	if len(dir) == 0 {
		return errors.Errorf("empty checkpoint directory")
	}
	if !filepath.IsAbs(dir) {
		dir = filepath.Join(r.dir, dir)
	}
	return statusToError(C.DBCheckpoint(r.rdb, goToCSlice([]byte(dir))))
}

// NewIterator returns an iterator over this rocksdb engine.
func (r *RocksDB) NewIterator(prefix bool) Iterator {
	return newRocksDBIterator(r.rdb, prefix, r)
}

// NewSnapshot creates a snapshot handle from engine and returns a
// read-only rocksDBSnapshot engine.
func (r *RocksDB) NewSnapshot() Reader {
	if r.rdb == nil {
		panic("RocksDB is not initialized yet")
	}
	return &rocksDBSnapshot{
		parent: r,
		handle: C.DBNewSnapshot(r.rdb),
	}
}

// NewBatch returns a new batch wrapping this rocksdb engine.
func (r *RocksDB) NewBatch() Batch {
	return newRocksDBBatch(r)
}

// GetSSTables retrieves metadata about this engine's live sstables.
func (r *RocksDB) GetSSTables() SSTableInfos {
	var n C.int
	tables := C.DBGetSSTables(r.rdb, &n)
	// We can't index into tables because it is a pointer, not a slice. The
	// hackery below treats the pointer as an array and then constructs a slice
	// from it.
	const maxLen = 0x7fffffff
	tableSlice := (*[maxLen]C.DBSSTable)(unsafe.Pointer(tables))[:n:n]

	res := make(SSTableInfos, n)
	for i := 0; i < int(n); i++ {
		res[i].Level = int(tableSlice[i].level)
		res[i].Size = int64(tableSlice[i].size)
		res[i].Start = cToGoKey(tableSlice[i].start_key)
		res[i].End = cToGoKey(tableSlice[i].end_key)
		if ptr := tableSlice[i].start_key.key.data; ptr != nil {
			C.free(unsafe.Pointer(ptr))
		}
		if ptr := tableSlice[i].end_key.key.data; ptr != nil {
			C.free(unsafe.Pointer(ptr))
		}
	}
	C.free(unsafe.Pointer(tables))

	sort.Sort(res)
	return res
}

// GetStats retrieves stats from this engine's RocksDB instance and
// returns it in a new instance of Stats.
func (r *RocksDB) GetStats() (*Stats, error) {
	var s C.DBStatsResult
	if err := statusToError(C.DBGetStats(r.rdb, &s)); err != nil {
		return nil, err
	}
	return &Stats{
		BlockCacheHits:           int64(s.block_cache_hits),
		BlockCacheMisses:         int64(s.block_cache_misses),
		BlockCacheUsage:          int64(s.block_cache_usage),
		BlockCachePinnedUsage:    int64(s.block_cache_pinned_usage),
		BloomFilterPrefixChecked: int64(s.bloom_filter_prefix_checked),
		BloomFilterPrefixUseful:  int64(s.bloom_filter_prefix_useful),
		MemtableHits:             int64(s.memtable_hits),
		MemtableMisses:           int64(s.memtable_misses),
		MemtableTotalSize:        int64(s.memtable_total_size),
		Flushes:                  int64(s.flushes),
		Compactions:              int64(s.compactions),
		TableReadersMemEstimate:  int64(s.table_readers_mem_estimate),
	}, nil
}

type rocksDBSnapshot struct {
	parent *RocksDB
	handle *C.DBEngine
}

// Close releases the snapshot handle.
func (r *rocksDBSnapshot) Close() {
	C.DBClose(r.handle)
	r.handle = nil
}

// closed returns true if the engine is closed.
func (r *rocksDBSnapshot) closed() bool {
	return r.handle == nil
}

// Get returns the value for the given key, nil otherwise using
// the snapshot handle.
func (r *rocksDBSnapshot) Get(key MVCCKey) ([]byte, error) {
	return dbGet(r.handle, key)
}

func (r *rocksDBSnapshot) GetProto(key MVCCKey, msg proto.Message) (
	ok bool, keyBytes, valBytes int64, err error) {
	return dbGetProto(r.handle, key, msg)
}

// Iterate iterates over the keys between start inclusive and end
// exclusive, invoking f() on each key/value pair using the snapshot
// handle.
func (r *rocksDBSnapshot) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (bool, error)) error {
	return dbIterate(r.handle, r, start, end, f)
}

// NewIterator returns a new instance of an Iterator over the
// engine using the snapshot handle.
func (r *rocksDBSnapshot) NewIterator(prefix bool) Iterator {
	return newRocksDBIterator(r.handle, prefix, r)
}

// reusableIterator wraps rocksDBIterator and allows reuse of an iterator
// for the lifetime of a batch.
type reusableIterator struct {
	rocksDBIterator
	inuse bool
}

func (r *reusableIterator) Close() {
	// reusableIterator.Close() leaves the underlying rocksdb iterator open until
	// the associated batch is closed.
	if !r.inuse {
		panic("closing idle iterator")
	}
	r.inuse = false
}

type distinctBatch struct {
	*rocksDBBatch
	prefixIter reusableIterator
	normalIter reusableIterator
}

func (r *distinctBatch) Close() {
	if !r.distinctOpen {
		panic("distinct batch not open")
	}
	r.distinctOpen = false
}

// NewIterator returns an iterator over the batch and underlying engine. Note
// that the returned iterator is cached and re-used for the lifetime of the
// batch. A panic will be thrown if multiple prefix or normal (non-prefix)
// iterators are used simultaneously on the same batch.
func (r *distinctBatch) NewIterator(prefix bool) Iterator {
	// Used the cached iterator, creating it on first access.
	iter := &r.normalIter
	if prefix {
		iter = &r.prefixIter
	}
	if iter.rocksDBIterator.iter == nil {
		iter.rocksDBIterator.init(r.batch, prefix, r)
	}
	if iter.inuse {
		panic("iterator already in use")
	}
	iter.inuse = true
	return iter
}

func (r *distinctBatch) Get(key MVCCKey) ([]byte, error) {
	return dbGet(r.batch, key)
}

func (r *distinctBatch) GetProto(key MVCCKey, msg proto.Message) (
	ok bool, keyBytes, valBytes int64, err error) {
	return dbGetProto(r.batch, key, msg)
}

func (r *distinctBatch) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (bool, error)) error {
	return dbIterate(r.batch, r, start, end, f)
}

func (r *distinctBatch) Put(key MVCCKey, value []byte) error {
	r.builder.Put(key, value)
	return nil
}

func (r *distinctBatch) Merge(key MVCCKey, value []byte) error {
	r.builder.Merge(key, value)
	return nil
}

func (r *distinctBatch) Clear(key MVCCKey) error {
	r.builder.Clear(key)
	return nil
}

func (r *distinctBatch) close() {
	if i := &r.prefixIter.rocksDBIterator; i.iter != nil {
		i.destroy()
	}
	if i := &r.normalIter.rocksDBIterator; i.iter != nil {
		i.destroy()
	}
}

// rocksDBBatchIterator wraps rocksDBIterator and allows reuse of an iterator
// for the lifetime of a batch.
type rocksDBBatchIterator struct {
	iter  rocksDBIterator
	batch *rocksDBBatch
}

func (r *rocksDBBatchIterator) Close() {
	// rocksDBBatchIterator.Close() leaves the underlying rocksdb iterator open
	// until the associated batch is closed.
	if r.batch == nil {
		panic("closing idle iterator")
	}
	r.batch = nil
}

func (r *rocksDBBatchIterator) Seek(key MVCCKey) {
	r.batch.flushMutations()
	r.iter.Seek(key)
}

func (r *rocksDBBatchIterator) SeekReverse(key MVCCKey) {
	r.batch.flushMutations()
	r.iter.SeekReverse(key)
}

func (r *rocksDBBatchIterator) Valid() bool {
	return r.iter.Valid()
}

func (r *rocksDBBatchIterator) Next() {
	r.batch.flushMutations()
	r.iter.Next()
}

func (r *rocksDBBatchIterator) Prev() {
	r.batch.flushMutations()
	r.iter.Prev()
}

func (r *rocksDBBatchIterator) NextKey() {
	r.batch.flushMutations()
	r.iter.NextKey()
}

func (r *rocksDBBatchIterator) PrevKey() {
	r.batch.flushMutations()
	r.iter.PrevKey()
}

func (r *rocksDBBatchIterator) ComputeStats(start, end MVCCKey, nowNanos int64) (enginepb.MVCCStats, error) {
	r.batch.flushMutations()
	return r.iter.ComputeStats(start, end, nowNanos)
}

func (r *rocksDBBatchIterator) Key() MVCCKey {
	return r.iter.Key()
}

func (r *rocksDBBatchIterator) Value() []byte {
	return r.iter.Value()
}

func (r *rocksDBBatchIterator) ValueProto(msg proto.Message) error {
	return r.iter.ValueProto(msg)
}

func (r *rocksDBBatchIterator) unsafeKey() MVCCKey {
	return r.iter.unsafeKey()
}

func (r *rocksDBBatchIterator) unsafeValue() []byte {
	return r.iter.unsafeValue()
}

func (r *rocksDBBatchIterator) Error() error {
	return r.iter.Error()
}

func (r *rocksDBBatchIterator) Less(key MVCCKey) bool {
	return r.iter.Less(key)
}

type rocksDBBatch struct {
	parent             *RocksDB
	batch              *C.DBEngine
	flushes            int
	prefixIter         rocksDBBatchIterator
	normalIter         rocksDBBatchIterator
	builder            rocksDBBatchBuilder
	distinct           distinctBatch
	distinctOpen       bool
	distinctNeedsFlush bool
}

func newRocksDBBatch(parent *RocksDB) *rocksDBBatch {
	r := &rocksDBBatch{
		parent: parent,
		batch:  C.DBNewBatch(parent.rdb),
	}
	r.distinct.rocksDBBatch = r
	return r
}

func (r *rocksDBBatch) Close() {
	r.distinct.close()
	if i := &r.prefixIter.iter; i.iter != nil {
		i.destroy()
	}
	if i := &r.normalIter.iter; i.iter != nil {
		i.destroy()
	}
	if r.batch != nil {
		C.DBClose(r.batch)
	}
}

// closed returns true if the engine is closed.
func (r *rocksDBBatch) closed() bool {
	return r.batch == nil
}

func (r *rocksDBBatch) Put(key MVCCKey, value []byte) error {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.distinctNeedsFlush = true
	r.builder.Put(key, value)
	return nil
}

func (r *rocksDBBatch) Merge(key MVCCKey, value []byte) error {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.distinctNeedsFlush = true
	r.builder.Merge(key, value)
	return nil
}

// ApplyBatchRepr atomically applies a set of batched updates to the current
// batch (the receiver).
func (r *rocksDBBatch) ApplyBatchRepr(repr []byte) error {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	return dbApplyBatchRepr(r.batch, repr)
}

func (r *rocksDBBatch) Get(key MVCCKey) ([]byte, error) {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.flushMutations()
	return dbGet(r.batch, key)
}

func (r *rocksDBBatch) GetProto(key MVCCKey, msg proto.Message) (
	ok bool, keyBytes, valBytes int64, err error) {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.flushMutations()
	return dbGetProto(r.batch, key, msg)
}

func (r *rocksDBBatch) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (bool, error)) error {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.flushMutations()
	return dbIterate(r.batch, r, start, end, f)
}

func (r *rocksDBBatch) Clear(key MVCCKey) error {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.distinctNeedsFlush = true
	r.builder.Clear(key)
	return nil
}

// NewIterator returns an iterator over the batch and underlying engine. Note
// that the returned iterator is cached and re-used for the lifetime of the
// batch. A panic will be thrown if multiple prefix or normal (non-prefix)
// iterators are used simultaneously on the same batch.
func (r *rocksDBBatch) NewIterator(prefix bool) Iterator {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	// Used the cached iterator, creating it on first access.
	iter := &r.normalIter
	if prefix {
		iter = &r.prefixIter
	}
	if iter.iter.iter == nil {
		iter.iter.init(r.batch, prefix, r)
	}
	if iter.batch != nil {
		panic("iterator already in use")
	}
	iter.batch = r
	return iter
}

func (r *rocksDBBatch) Commit() error {
	if r.batch == nil {
		panic("this batch was already committed")
	}

	r.distinctOpen = false
	if r.flushes > 0 {
		// We've previously flushed mutations to the C++ batch, so we have to flush
		// any remaining mutations as well and then commit the batch.
		r.flushMutations()
		if err := statusToError(C.DBCommitBatch(r.batch)); err != nil {
			return err
		}
	} else if r.builder.count > 0 {
		// Fast-path which avoids flushing mutations to the C++ batch. Instead, we
		// directly apply the mutations to the database.
		if err := r.parent.ApplyBatchRepr(r.builder.Finish()); err != nil {
			return err
		}
	}

	C.DBClose(r.batch)
	r.batch = nil

	return nil
}

func (r *rocksDBBatch) Repr() []byte {
	r.flushMutations()
	return cSliceToGoBytes(C.DBBatchRepr(r.batch))
}

func (r *rocksDBBatch) Distinct() ReadWriter {
	if r.distinctNeedsFlush {
		r.flushMutations()
	}
	if r.distinctOpen {
		panic("distinct batch already open")
	}
	r.distinctOpen = true
	return &r.distinct
}

func (r *rocksDBBatch) flushMutations() {
	if r.builder.count == 0 {
		return
	}
	r.distinctNeedsFlush = false
	r.flushes++
	if err := r.ApplyBatchRepr(r.builder.Finish()); err != nil {
		panic(err)
	}
	// Force a seek of the underlying iterator on the next Seek/ReverseSeek.
	r.prefixIter.iter.reseek = true
	r.normalIter.iter.reseek = true
}

type rocksDBIterator struct {
	engine Reader
	iter   *C.DBIterator
	valid  bool
	reseek bool
	key    C.DBKey
	value  C.DBSlice
}

// TODO(peter): Is this pool useful now that rocksDBBatch.NewIterator doesn't
// allocate by returning internal pointers?
var iterPool = sync.Pool{
	New: func() interface{} {
		return &rocksDBIterator{}
	},
}

// newRocksDBIterator returns a new iterator over the supplied RocksDB
// instance. If snapshotHandle is not nil, uses the indicated snapshot.
// The caller must call rocksDBIterator.Close() when finished with the
// iterator to free up resources.
func newRocksDBIterator(rdb *C.DBEngine, prefix bool, engine Reader) Iterator {
	// In order to prevent content displacement, caching is disabled
	// when performing scans. Any options set within the shared read
	// options field that should be carried over needs to be set here
	// as well.
	r := iterPool.Get().(*rocksDBIterator)
	r.init(rdb, prefix, engine)
	return r
}

func (r *rocksDBIterator) init(rdb *C.DBEngine, prefix bool, engine Reader) {
	r.iter = C.DBNewIter(rdb, C.bool(prefix))
	r.engine = engine
}

func (r *rocksDBIterator) checkEngineOpen() {
	if r.engine.closed() {
		panic("iterator used after backing engine closed")
	}
}

func (r *rocksDBIterator) destroy() {
	C.DBIterDestroy(r.iter)
	*r = rocksDBIterator{}
}

// The following methods implement the Iterator interface.
func (r *rocksDBIterator) Close() {
	r.destroy()
	iterPool.Put(r)
}

func (r *rocksDBIterator) Seek(key MVCCKey) {
	r.checkEngineOpen()
	if len(key.Key) == 0 {
		// start=Key("") needs special treatment since we need
		// to access start[0] in an explicit seek.
		r.setState(C.DBIterSeekToFirst(r.iter))
	} else {
		// We can avoid seeking if we're already at the key we seek.
		if r.valid && !r.reseek && key.Equal(r.unsafeKey()) {
			return
		}
		r.setState(C.DBIterSeek(r.iter, goToCKey(key)))
	}
}

func (r *rocksDBIterator) SeekReverse(key MVCCKey) {
	r.checkEngineOpen()
	if len(key.Key) == 0 {
		r.setState(C.DBIterSeekToLast(r.iter))
	} else {
		// We can avoid seeking if we're already at the key we seek.
		if r.valid && !r.reseek && key.Equal(r.unsafeKey()) {
			return
		}
		r.setState(C.DBIterSeek(r.iter, goToCKey(key)))
		// Maybe the key sorts after the last key in RocksDB.
		if !r.Valid() {
			r.setState(C.DBIterSeekToLast(r.iter))
		}
		if !r.Valid() {
			return
		}
		// Make sure the current key is <= the provided key.
		if key.Less(r.unsafeKey()) {
			r.Prev()
		}
	}
}

func (r *rocksDBIterator) Valid() bool {
	return r.valid
}

func (r *rocksDBIterator) Next() {
	r.checkEngineOpen()
	r.setState(C.DBIterNext(r.iter, false /* !skip_current_key_versions */))
}

func (r *rocksDBIterator) Prev() {
	r.checkEngineOpen()
	r.setState(C.DBIterPrev(r.iter, false /* !skip_current_key_versions */))
}

func (r *rocksDBIterator) NextKey() {
	r.checkEngineOpen()
	r.setState(C.DBIterNext(r.iter, true /* skip_current_key_versions */))
}

func (r *rocksDBIterator) PrevKey() {
	r.checkEngineOpen()
	r.setState(C.DBIterPrev(r.iter, true /* skip_current_key_versions */))
}

func (r *rocksDBIterator) Key() MVCCKey {
	// The data returned by rocksdb_iter_{key,value} is not meant to be
	// freed by the client. It is a direct reference to the data managed
	// by the iterator, so it is copied instead of freed.
	return cToGoKey(r.key)
}

func (r *rocksDBIterator) Value() []byte {
	return cSliceToGoBytes(r.value)
}

func (r *rocksDBIterator) ValueProto(msg proto.Message) error {
	if r.value.len <= 0 {
		return nil
	}
	return proto.Unmarshal(r.unsafeValue(), msg)
}

func (r *rocksDBIterator) unsafeKey() MVCCKey {
	return cToUnsafeGoKey(r.key)
}

func (r *rocksDBIterator) unsafeValue() []byte {
	return cSliceToUnsafeGoBytes(r.value)
}

func (r *rocksDBIterator) Error() error {
	return statusToError(C.DBIterError(r.iter))
}

func (r *rocksDBIterator) Less(key MVCCKey) bool {
	return r.unsafeKey().Less(key)
}

func (r *rocksDBIterator) setState(state C.DBIterState) {
	r.valid = bool(state.valid)
	r.reseek = false
	r.key = state.key
	r.value = state.value
}

func (r *rocksDBIterator) ComputeStats(start, end MVCCKey, nowNanos int64) (enginepb.MVCCStats, error) {
	result := C.MVCCComputeStats(r.iter, goToCKey(start), goToCKey(end), C.int64_t(nowNanos))
	ms := enginepb.MVCCStats{}
	if err := statusToError(result.status); err != nil {
		return ms, err
	}
	ms.ContainsEstimates = false
	ms.LiveBytes = int64(result.live_bytes)
	ms.KeyBytes = int64(result.key_bytes)
	ms.ValBytes = int64(result.val_bytes)
	ms.IntentBytes = int64(result.intent_bytes)
	ms.LiveCount = int64(result.live_count)
	ms.KeyCount = int64(result.key_count)
	ms.ValCount = int64(result.val_count)
	ms.IntentCount = int64(result.intent_count)
	ms.IntentAge = int64(result.intent_age)
	ms.GCBytesAge = int64(result.gc_bytes_age)
	ms.SysBytes = int64(result.sys_bytes)
	ms.SysCount = int64(result.sys_count)
	ms.LastUpdateNanos = nowNanos
	return ms, nil
}

// goToCSlice converts a go byte slice to a DBSlice. Note that this is
// potentially dangerous as the DBSlice holds a reference to the go
// byte slice memory that the Go GC does not know about. This method
// is only intended for use in converting arguments to C
// functions. The C function must copy any data that it wishes to
// retain once the function returns.
func goToCSlice(b []byte) C.DBSlice {
	if len(b) == 0 {
		return C.DBSlice{data: nil, len: 0}
	}
	return C.DBSlice{
		data: (*C.char)(unsafe.Pointer(&b[0])),
		len:  C.int(len(b)),
	}
}

func goToCKey(key MVCCKey) C.DBKey {
	return C.DBKey{
		key:       goToCSlice(key.Key),
		wall_time: C.int64_t(key.Timestamp.WallTime),
		logical:   C.int32_t(key.Timestamp.Logical),
	}
}

func cToGoKey(key C.DBKey) MVCCKey {
	// When converting a C.DBKey to an MVCCKey, give the underlying slice an
	// extra byte of capacity in anticipation of roachpb.Key.Next() being
	// called. The extra byte is trivial extra space, but allows callers to avoid
	// an allocation and copy when calling roachpb.Key.Next(). Note that it is
	// important that the extra byte contain the value 0 in order for the
	// roachpb.Key.Next() fast-path to be invoked. This is true for the code
	// below because make() zero initializes all of the bytes.
	unsafeKey := cSliceToUnsafeGoBytes(key.key)
	safeKey := make([]byte, len(unsafeKey), len(unsafeKey)+1)
	copy(safeKey, unsafeKey)

	return MVCCKey{
		Key: safeKey,
		Timestamp: hlc.Timestamp{
			WallTime: int64(key.wall_time),
			Logical:  int32(key.logical),
		},
	}
}

func cToUnsafeGoKey(key C.DBKey) MVCCKey {
	return MVCCKey{
		Key: cSliceToUnsafeGoBytes(key.key),
		Timestamp: hlc.Timestamp{
			WallTime: int64(key.wall_time),
			Logical:  int32(key.logical),
		},
	}
}

func cStringToGoString(s C.DBString) string {
	if s.data == nil {
		return ""
	}
	result := C.GoStringN(s.data, s.len)
	C.free(unsafe.Pointer(s.data))
	return result
}

func cStringToGoBytes(s C.DBString) []byte {
	if s.data == nil {
		return nil
	}
	result := C.GoBytes(unsafe.Pointer(s.data), s.len)
	C.free(unsafe.Pointer(s.data))
	return result
}

func cSliceToGoBytes(s C.DBSlice) []byte {
	if s.data == nil {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(s.data), s.len)
}

func cSliceToUnsafeGoBytes(s C.DBSlice) []byte {
	if s.data == nil {
		return nil
	}
	// Go limits arrays to a length that will fit in a (signed) 32-bit
	// integer. Fall back to using cSliceToGoBytes if our slice is
	// larger.
	const maxLen = 0x7fffffff
	if s.len > maxLen {
		return cSliceToGoBytes(s)
	}
	return (*[maxLen]byte)(unsafe.Pointer(s.data))[:s.len:s.len]
}

func statusToError(s C.DBStatus) error {
	if s.data == nil {
		return nil
	}
	return errors.New(cStringToGoString(s))
}

// goMerge takes existing and update byte slices that are expected to
// be marshalled roachpb.Values and merges the two values returning a
// marshalled roachpb.Value or an error.
func goMerge(existing, update []byte) ([]byte, error) {
	var result C.DBString
	status := C.DBMergeOne(goToCSlice(existing), goToCSlice(update), &result)
	if status.data != nil {
		return nil, errors.Errorf("%s: existing=%q, update=%q",
			cStringToGoString(status), existing, update)
	}
	return cStringToGoBytes(result), nil
}

func emptyKeyError() error {
	return errors.Errorf("attempted access to empty key")
}

func dbPut(rdb *C.DBEngine, key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	// *Put, *Get, and *Delete call memcpy() (by way of MemTable::Add)
	// when called, so we do not need to worry about these byte slices
	// being reclaimed by the GC.
	return statusToError(C.DBPut(rdb, goToCKey(key), goToCSlice(value)))
}

func dbMerge(rdb *C.DBEngine, key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	// DBMerge calls memcpy() (by way of MemTable::Add)
	// when called, so we do not need to worry about these byte slices being
	// reclaimed by the GC.
	return statusToError(C.DBMerge(rdb, goToCKey(key), goToCSlice(value)))
}

func dbApplyBatchRepr(rdb *C.DBEngine, repr []byte) error {
	return statusToError(C.DBApplyBatchRepr(rdb, goToCSlice(repr)))
}

// dbGet returns the value for the given key.
func dbGet(rdb *C.DBEngine, key MVCCKey) ([]byte, error) {
	if len(key.Key) == 0 {
		return nil, emptyKeyError()
	}
	var result C.DBString
	err := statusToError(C.DBGet(rdb, goToCKey(key), &result))
	if err != nil {
		return nil, err
	}
	return cStringToGoBytes(result), nil
}

func dbGetProto(rdb *C.DBEngine, key MVCCKey,
	msg proto.Message) (ok bool, keyBytes, valBytes int64, err error) {
	if len(key.Key) == 0 {
		err = emptyKeyError()
		return
	}
	var result C.DBString
	if err = statusToError(C.DBGet(rdb, goToCKey(key), &result)); err != nil {
		return
	}
	if result.len <= 0 {
		msg.Reset()
		return
	}
	ok = true
	if msg != nil {
		// Make a byte slice that is backed by result.data. This slice
		// cannot live past the lifetime of this method, but we're only
		// using it to unmarshal the roachpb.
		data := cSliceToUnsafeGoBytes(C.DBSlice(result))
		err = proto.Unmarshal(data, msg)
	}
	C.free(unsafe.Pointer(result.data))
	keyBytes = int64(key.EncodedSize())
	valBytes = int64(result.len)
	return
}

func dbClear(rdb *C.DBEngine, key MVCCKey) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return statusToError(C.DBDelete(rdb, goToCKey(key)))
}

func dbIterate(rdb *C.DBEngine, engine Reader, start, end MVCCKey,
	f func(MVCCKeyValue) (bool, error)) error {
	if !start.Less(end) {
		return nil
	}
	it := newRocksDBIterator(rdb, false, engine)
	defer it.Close()

	it.Seek(start)
	for ; it.Valid(); it.Next() {
		k := it.Key()
		if !k.Less(end) {
			break
		}
		if done, err := f(MVCCKeyValue{Key: k, Value: it.Value()}); done || err != nil {
			return err
		}
	}
	// Check for any errors during iteration.
	return it.Error()
}
