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
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	"github.com/dustin/go-humanize"
	"github.com/elastic/gosigar"
	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine/rocksdb"
	"github.com/cockroachdb/cockroach/util"
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

const minMemtableBudget = 1 << 20 // 1 MB

func init() {
	rocksdb.Logger = log.Infof
}

// RocksDB is a wrapper around a RocksDB database instance.
type RocksDB struct {
	rdb            *C.DBEngine
	attrs          roachpb.Attributes // Attributes for this engine
	dir            string             // The data directory
	cacheSize      int64              // Memory to use to cache values.
	memtableBudget int64              // Memory to use for the memory table.
	maxSize        int64              // Used for calculating rebalancing and free space.
	stopper        *stop.Stopper
	deallocated    chan struct{} // Closed when the underlying handle is deallocated.
}

var _ Engine = &RocksDB{}

// NewRocksDB allocates and returns a new RocksDB object.
func NewRocksDB(attrs roachpb.Attributes, dir string, cacheSize, memtableBudget, maxSize int64,
	stopper *stop.Stopper) Engine {
	if dir == "" {
		panic("dir must be non-empty")
	}
	return &RocksDB{
		attrs:          attrs,
		dir:            dir,
		cacheSize:      cacheSize,
		memtableBudget: memtableBudget,
		maxSize:        maxSize,
		stopper:        stopper,
		deallocated:    make(chan struct{}),
	}
}

func newMemRocksDB(attrs roachpb.Attributes, cacheSize, memtableBudget int64, stopper *stop.Stopper) *RocksDB {
	return &RocksDB{
		attrs: attrs,
		// dir: empty dir == "mem" RocksDB instance.
		cacheSize:      cacheSize,
		memtableBudget: memtableBudget,
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
		return util.Errorf("memtable budget must be at least %s: %s",
			humanize.IBytes(minMemtableBudget), humanizeutil.IBytes(r.memtableBudget))
	}

	var ver storageVersion
	if len(r.dir) != 0 {
		log.Infof("opening rocksdb instance at %q", r.dir)

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
		log.Infof("opening in memory rocksdb instance")

		// In memory dbs are always current.
		ver = versionCurrent
	}

	status := C.DBOpen(&r.rdb, goToCSlice([]byte(r.dir)),
		C.DBOptions{
			cache_size:      C.uint64_t(r.cacheSize),
			memtable_budget: C.uint64_t(r.memtableBudget),
			allow_os_buffer: C.bool(true),
			logging_enabled: C.bool(log.V(3)),
		})
	if err := statusToError(status); err != nil {
		return util.Errorf("could not open rocksdb instance: %s", err)
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
		log.Errorf("closing unopened rocksdb instance")
		return
	}
	if len(r.dir) == 0 {
		if log.V(1) {
			log.Infof("closing in-memory rocksdb instance")
		}
	} else {
		log.Infof("closing rocksdb instance at %q", r.dir)
	}
	if r.rdb != nil {
		C.DBClose(r.rdb)
		r.rdb = nil
	}
	close(r.deallocated)
}

// Closed returns true if the engine is closed.
func (r *RocksDB) Closed() bool {
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
		dir = "/tmp"
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

// Compact forces compaction on the database. This is currently used only to
// force partial merges to occur in unit tests.
func (r *RocksDB) Compact() {
	err := statusToError(C.DBCompact(r.rdb))
	if err != nil {
		log.Warningf("compact: %s", err)
	}
}

// Destroy destroys the underlying filesystem data associated with the database.
func (r *RocksDB) Destroy() error {
	return statusToError(C.DBDestroy(goToCSlice([]byte(r.dir))))
}

// ApproximateSize returns the approximate number of bytes on disk that RocksDB
// is using to store data for the given range of keys.
func (r *RocksDB) ApproximateSize(start, end MVCCKey) (uint64, error) {
	return uint64(C.DBApproximateSize(r.rdb, goToCKey(start), goToCKey(end))), nil
}

// Flush causes RocksDB to write all in-memory data to disk immediately.
func (r *RocksDB) Flush() error {
	return statusToError(C.DBFlush(r.rdb))
}

// NewIterator returns an iterator over this rocksdb engine.
func (r *RocksDB) NewIterator(prefix roachpb.Key) Iterator {
	return newRocksDBIterator(r.rdb, prefix, r)
}

// NewSnapshot creates a snapshot handle from engine and returns a
// read-only rocksDBSnapshot engine.
func (r *RocksDB) NewSnapshot() Engine {
	if r.rdb == nil {
		panic("RocksDB is not initialized yet")
	}
	return &rocksDBSnapshot{
		parent: r,
		handle: C.DBNewSnapshot(r.rdb),
	}
}

// NewBatch returns a new batch wrapping this rocksdb engine.
func (r *RocksDB) NewBatch() Engine {
	return newRocksDBBatch(r)
}

// Commit is a noop for RocksDB engine.
func (r *RocksDB) Commit() error {
	return nil
}

// Defer is not implemented for RocksDB engine.
func (r *RocksDB) Defer(func()) {
	panic("only implemented for rocksDBBatch")
}

// GetStats retrieves stats from this Engine's RocksDB instance and
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

// Open is a noop.
func (r *rocksDBSnapshot) Open() error {
	return nil
}

// Close releases the snapshot handle.
func (r *rocksDBSnapshot) Close() {
	C.DBClose(r.handle)
	r.handle = nil
}

// Closed returns true if the engine is closed.
func (r *rocksDBSnapshot) Closed() bool {
	return r.handle == nil
}

// Attrs returns the engine/store attributes.
func (r *rocksDBSnapshot) Attrs() roachpb.Attributes {
	return r.parent.Attrs()
}

// Put is illegal for snapshot and returns an error.
func (r *rocksDBSnapshot) Put(key MVCCKey, value []byte) error {
	return util.Errorf("cannot Put to a snapshot")
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

// Clear is illegal for snapshot and returns an error.
func (r *rocksDBSnapshot) Clear(key MVCCKey) error {
	return util.Errorf("cannot Clear from a snapshot")
}

// Merge is illegal for snapshot and returns an error.
func (r *rocksDBSnapshot) Merge(key MVCCKey, value []byte) error {
	return util.Errorf("cannot Merge to a snapshot")
}

// Capacity returns capacity details for the engine's available storage.
func (r *rocksDBSnapshot) Capacity() (roachpb.StoreCapacity, error) {
	return r.parent.Capacity()
}

// ApproximateSize returns the approximate number of bytes the engine is
// using to store data for the given range of keys.
func (r *rocksDBSnapshot) ApproximateSize(start, end MVCCKey) (uint64, error) {
	return r.parent.ApproximateSize(start, end)
}

// Flush is a no-op for snapshots.
func (r *rocksDBSnapshot) Flush() error {
	return nil
}

// NewIterator returns a new instance of an Iterator over the
// engine using the snapshot handle.
func (r *rocksDBSnapshot) NewIterator(prefix roachpb.Key) Iterator {
	return newRocksDBIterator(r.handle, prefix, r)
}

// NewSnapshot is illegal for snapshot.
func (r *rocksDBSnapshot) NewSnapshot() Engine {
	panic("cannot create a NewSnapshot from a snapshot")
}

// NewBatch is illegal for snapshot.
func (r *rocksDBSnapshot) NewBatch() Engine {
	panic("cannot create a NewBatch from a snapshot")
}

// Commit is illegal for snapshot and returns an error.
func (r *rocksDBSnapshot) Commit() error {
	return util.Errorf("cannot Commit to a snapshot")
}

// Defer is not implemented for rocksDBSnapshot.
func (r *rocksDBSnapshot) Defer(func()) {
	panic("only implemented for rocksDBBatch")
}

// GetStats is not implemented for rocksDBSnapshot.
func (r *rocksDBSnapshot) GetStats() (*Stats, error) {
	return nil, util.Errorf("GetStats is not implemented for %T", r)
}

type rocksDBBatch struct {
	parent *RocksDB
	batch  *C.DBEngine
	defers []func()
}

func newRocksDBBatch(r *RocksDB) *rocksDBBatch {
	return &rocksDBBatch{
		parent: r,
		batch:  C.DBNewBatch(r.rdb),
	}
}

func (r *rocksDBBatch) Open() error {
	return util.Errorf("cannot open a batch")
}

func (r *rocksDBBatch) Close() {
	if r.batch != nil {
		C.DBClose(r.batch)
	}
}

// Closed returns true if the engine is closed.
func (r *rocksDBBatch) Closed() bool {
	return r.batch == nil
}

// Attrs returns the engine/store attributes.
func (r *rocksDBBatch) Attrs() roachpb.Attributes {
	return r.parent.Attrs()
}

func (r *rocksDBBatch) Put(key MVCCKey, value []byte) error {
	return dbPut(r.batch, key, value)
}

func (r *rocksDBBatch) Merge(key MVCCKey, value []byte) error {
	return dbMerge(r.batch, key, value)
}

func (r *rocksDBBatch) Get(key MVCCKey) ([]byte, error) {
	return dbGet(r.batch, key)
}

func (r *rocksDBBatch) GetProto(key MVCCKey, msg proto.Message) (
	ok bool, keyBytes, valBytes int64, err error) {
	return dbGetProto(r.batch, key, msg)
}

func (r *rocksDBBatch) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (bool, error)) error {
	return dbIterate(r.batch, r, start, end, f)
}

func (r *rocksDBBatch) Clear(key MVCCKey) error {
	return dbClear(r.batch, key)
}

func (r *rocksDBBatch) Capacity() (roachpb.StoreCapacity, error) {
	return r.parent.Capacity()
}

func (r *rocksDBBatch) ApproximateSize(start, end MVCCKey) (uint64, error) {
	return r.parent.ApproximateSize(start, end)
}

func (r *rocksDBBatch) Flush() error {
	return util.Errorf("cannot flush a batch")
}

func (r *rocksDBBatch) NewIterator(prefix roachpb.Key) Iterator {
	return newRocksDBIterator(r.batch, prefix, r)
}

func (r *rocksDBBatch) NewSnapshot() Engine {
	panic("cannot create a NewSnapshot from a batch")
}

func (r *rocksDBBatch) NewBatch() Engine {
	return newRocksDBBatch(r.parent)
}

func (r *rocksDBBatch) Commit() error {
	if r.batch == nil {
		panic("this batch was already committed")
	}
	if err := statusToError(C.DBWriteBatch(r.batch)); err != nil {
		return err
	}
	C.DBClose(r.batch)
	r.batch = nil

	// On success, run the deferred functions in reverse order.
	for i := len(r.defers) - 1; i >= 0; i-- {
		r.defers[i]()
	}
	r.defers = nil

	return nil
}

func (r *rocksDBBatch) Defer(fn func()) {
	r.defers = append(r.defers, fn)
}

// GetStats is not implemented for rocksDBBatch.
func (r *rocksDBBatch) GetStats() (*Stats, error) {
	return nil, util.Errorf("GetStats is not implemented for %T", r)
}

type rocksDBIterator struct {
	engine Engine
	iter   *C.DBIterator
	valid  bool
	key    C.DBKey
	value  C.DBSlice
}

var iterPool = sync.Pool{
	New: func() interface{} {
		return &rocksDBIterator{}
	},
}

// newRocksDBIterator returns a new iterator over the supplied RocksDB
// instance. If snapshotHandle is not nil, uses the indicated snapshot.
// The caller must call rocksDBIterator.Close() when finished with the
// iterator to free up resources.
func newRocksDBIterator(rdb *C.DBEngine, prefix roachpb.Key, engine Engine) Iterator {
	// In order to prevent content displacement, caching is disabled
	// when performing scans. Any options set within the shared read
	// options field that should be carried over needs to be set here
	// as well.
	r := iterPool.Get().(*rocksDBIterator)
	r.iter = C.DBNewIter(rdb, goToCSlice(prefix))
	r.engine = engine
	return r
}

func (r *rocksDBIterator) checkEngineOpen() {
	if r.engine.Closed() {
		panic("iterator used after backing engine closed")
	}
}

// The following methods implement the Iterator interface.
func (r *rocksDBIterator) Close() {
	C.DBIterDestroy(r.iter)
	*r = rocksDBIterator{}
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
		if r.valid && key.Equal(r.unsafeKey()) {
			return
		}
		r.setState(C.DBIterSeek(r.iter, goToCKey(key)))
	}
}

func (r *rocksDBIterator) Valid() bool {
	return r.valid
}

func (r *rocksDBIterator) Next() {
	r.checkEngineOpen()
	r.setState(C.DBIterNext(r.iter))
}

func (r *rocksDBIterator) SeekReverse(key MVCCKey) {
	r.checkEngineOpen()
	if len(key.Key) == 0 {
		r.setState(C.DBIterSeekToLast(r.iter))
	} else {
		r.setState(C.DBIterSeek(r.iter, goToCKey(key)))
		// Maybe the key sorts after the last key in RocksDB.
		if !r.Valid() {
			r.setState(C.DBIterSeekToLast(r.iter))
		}
		if !r.Valid() {
			return
		}
		// Make sure the current key is <= the provided key.
		if key.Less(r.Key()) {
			r.Prev()
		}
	}
}

func (r *rocksDBIterator) Prev() {
	r.checkEngineOpen()
	r.setState(C.DBIterPrev(r.iter))
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

func (r *rocksDBIterator) setState(state C.DBIterState) {
	r.valid = bool(state.valid)
	r.key = state.key
	r.value = state.value
}

func (r *rocksDBIterator) ComputeStats(start, end MVCCKey, nowNanos int64) (MVCCStats, error) {
	result := C.MVCCComputeStats(r.iter, goToCKey(start), goToCKey(end), C.int64_t(nowNanos))
	ms := MVCCStats{}
	if err := statusToError(result.status); err != nil {
		return ms, err
	}
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
	return MVCCKey{
		Key: cSliceToGoBytes(key.key),
		Timestamp: roachpb.Timestamp{
			WallTime: int64(key.wall_time),
			Logical:  int32(key.logical),
		},
	}
}

func cToUnsafeGoKey(key C.DBKey) MVCCKey {
	return MVCCKey{
		Key: cSliceToUnsafeGoBytes(key.key),
		Timestamp: roachpb.Timestamp{
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
		return nil, util.Errorf("%s: existing=%q, update=%q",
			cStringToGoString(status), existing, update)
	}
	return cStringToGoBytes(result), nil
}

func emptyKeyError() error {
	return util.ErrorfSkipFrames(1, "attempted access to empty key")
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

func dbIterate(rdb *C.DBEngine, engine Engine, start, end MVCCKey,
	f func(MVCCKeyValue) (bool, error)) error {
	if !start.Less(end) {
		return nil
	}
	it := newRocksDBIterator(rdb, nil, engine)
	defer it.Close()

	it.Seek(start)
	for ; it.Valid(); it.Next() {
		k := it.Key()
		if !it.Key().Less(end) {
			break
		}
		if done, err := f(MVCCKeyValue{Key: k, Value: it.Value()}); done || err != nil {
			return err
		}
	}
	// Check for any errors during iteration.
	return it.Error()
}
