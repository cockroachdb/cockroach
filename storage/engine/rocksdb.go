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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
// Author: Jiang-Ming Yang (jiangming.yang@gmail.com)

package engine

/*
#cgo pkg-config: ./engine.pc
#include <stdlib.h>
#include "rocksdb/c.h"
#include "rocksdb_merge.h"
#include "rocksdb_compaction.h"
*/
import "C"
import (
	"bytes"
	"flag"
	"fmt"
	"syscall"
	"unsafe"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// defaultCacheSize is the default value for the cacheSize command line flag.
const defaultCacheSize = 1 << 30 // GB

// cacheSize is the amount of memory in bytes to use for caching data.
// The value is split evenly between the stores if there are more than one.
var cacheSize = flag.Int64("cache_size", defaultCacheSize, "total size in bytes for "+
	"caches, shared evenly if there are multiple storage devices")

// RocksDB is a wrapper around a RocksDB database instance.
type RocksDB struct {
	rdb           *C.rocksdb_t                     // The DB handle
	opts          *C.rocksdb_options_t             // Options used when creating or destroying
	rOpts         *C.rocksdb_readoptions_t         // The default read options
	wOpts         *C.rocksdb_writeoptions_t        // The default write options
	mergeOperator *C.rocksdb_mergeoperator_t       // Custom RocksDB merge operator
	snapshots     map[string]*C.rocksdb_snapshot_t // Map of snapshot handles by snapshot ID

	// Custom RocksDB compaction filter.
	compactionFilterFactory *C.rocksdb_compactionfilterfactory_t

	attrs      proto.Attributes // Attributes for this engine
	dir        string           // The data directory
	gcTimeouts func() (minTxnTS, minRCacheTS int64)
}

// NewRocksDB allocates and returns a new RocksDB object.
func NewRocksDB(attrs proto.Attributes, dir string) *RocksDB {
	return &RocksDB{
		snapshots: map[string]*C.rocksdb_snapshot_t{},
		attrs:     attrs,
		dir:       dir,
	}
}

//export getGCTimeouts
// getGCTimeouts returns timestamp values (in unix nanos) for garbage
// collecting transaction rows and response cache rows respectively.
func getGCTimeouts(rocksdbPtr unsafe.Pointer) (minTxnTS, minRCacheTS int64) {
	rocksdb := (*RocksDB)(rocksdbPtr)
	return rocksdb.gcTimeouts()
}

//export getGCPrefixes
// getGCPrefixes returns key prefixes for transaction and response
// cache rows, in that order. Each prefix is encoded to match the raw
// keys in the underlying storage engine. Ownership for the returned
// strings is assumed by the caller. They should be deallocated using
// free().
func getGCPrefixes() (*C.char, *C.char) {
	// Since we're converting to a C-string, the prefixes which we'll
	// match against will end at the first null character, or just after
	// the encoded transaction prefix, up to but not including the
	// terminating null character.
	// TODO(spencer): it's fragile to rely on this behavior. Should
	// consider changing.
	txnPrefix := KeyLocalTransactionPrefix.Encode(nil)
	rcachePrefix := KeyLocalResponseCachePrefix.Encode(nil)
	return C.CString(string(txnPrefix)), C.CString(string(rcachePrefix))
}

//export reportGCError
// reportGCError is a callback for the rocksdb custom compaction filter
// to log errors encountered while trying to parse response cache entries
// and transaction entires for garbage collection.
func reportGCError(errMsg *C.char, key *C.char, keyLen C.int, value *C.char, valueLen C.int) {
	log.Errorf("%s: key=%q, value=%q", C.GoString(errMsg), C.GoStringN(key, keyLen), C.GoStringN(value, valueLen))
}

// createOptions sets the default options for creating, reading, and writing
// from the db. destroyOptions should be called when the options aren't needed
// anymore.
func (r *RocksDB) createOptions() {
	// TODO(andybons): Set the cache size.
	r.opts = C.rocksdb_options_create()
	C.rocksdb_options_set_create_if_missing(r.opts, 1)
	// This enables us to use rocksdb_merge with counter semantics.
	// See rocksdb.{c,h} for the C implementation.
	r.mergeOperator = C.make_merge_operator()
	C.rocksdb_options_set_merge_operator(r.opts, r.mergeOperator)
	// This enables garbage collection of transaction and response cache rows.
	r.compactionFilterFactory = C.make_gc_compaction_filter_factory(unsafe.Pointer(r))
	C.rocksdb_options_set_compaction_filter_factory(r.opts, r.compactionFilterFactory)

	r.wOpts = C.rocksdb_writeoptions_create()
	r.rOpts = C.rocksdb_readoptions_create()
}

// destroyOptions destroys the options used for creating, reading, and writing
// from the db. It is meant to be used in conjunction with createOptions.
func (r *RocksDB) destroyOptions() {
	// The merge operator and compaction filter are stored inside of
	// r.opts using std::shared_ptrs, so they'll be freed
	// automatically. Calling the *_destroy methods directly would
	// ignore the shared_ptrs, and a subsequent rocksdb_options_destroy
	// would segfault. The following lines zero the shared_ptrs instead,
	// which deletes the underlying values.
	C.rocksdb_options_set_merge_operator(r.opts, nil)
	C.rocksdb_options_set_compaction_filter_factory(r.opts, nil)
	C.rocksdb_options_destroy(r.opts)
	C.rocksdb_readoptions_destroy(r.rOpts)
	C.rocksdb_writeoptions_destroy(r.wOpts)
	r.mergeOperator = nil
	r.compactionFilterFactory = nil
	r.opts = nil
	r.rOpts = nil
	r.wOpts = nil
}

// String formatter.
func (r *RocksDB) String() string {
	return fmt.Sprintf("%s=%s", r.attrs, r.dir)
}

// Start creates options and opens the database. If the database
// doesn't yet exist at the specified directory, one is initialized
// from scratch. Subsequent calls to this method on an open DB are no-ops.
func (r *RocksDB) Start() error {
	if r.rdb != nil {
		return nil
	}
	r.createOptions()

	cDir := C.CString(r.dir)
	defer C.free(unsafe.Pointer(cDir))

	var cErr *C.char
	if r.rdb = C.rocksdb_open(r.opts, cDir, &cErr); cErr != nil {
		r.rdb = nil
		r.destroyOptions()
		return charToErr(cErr)
	}
	if _, err := r.Capacity(); err != nil {
		if err := r.Destroy(); err != nil {
			log.Warningf("could not destroy db at %s", r.dir)
		}
		return err
	}
	return nil
}

// Stop closes the database by deallocating the underlying handle.
func (r *RocksDB) Stop() {
	C.rocksdb_close(r.rdb)
	r.rdb = nil
}

// CreateSnapshot creates a snapshot handle from engine.
func (r *RocksDB) CreateSnapshot(snapshotID string) error {
	if r.rdb == nil {
		return util.Errorf("RocksDB is not initialized yet")
	}
	_, ok := r.snapshots[snapshotID]
	if ok {
		return util.Errorf("snapshotID %s already exists", snapshotID)
	}
	snapshotHandle := C.rocksdb_create_snapshot(r.rdb)
	r.snapshots[snapshotID] = snapshotHandle
	return nil
}

// ReleaseSnapshot releases the existing snapshot handle for the
// given snapshotID.
func (r *RocksDB) ReleaseSnapshot(snapshotID string) error {
	if r.rdb == nil {
		return util.Errorf("RocksDB is not initialized yet")
	}
	snapshotHandle, ok := r.snapshots[snapshotID]
	if !ok {
		return util.Errorf("snapshotID %s does not exist", snapshotID)
	}
	C.rocksdb_release_snapshot(r.rdb, snapshotHandle)
	delete(r.snapshots, snapshotID)
	return nil
}

// Attrs returns the list of attributes describing this engine. This
// may include a specification of disk type (e.g. hdd, ssd, fio, etc.)
// and potentially other labels to identify important attributes of
// the engine.
func (r *RocksDB) Attrs() proto.Attributes {
	return r.attrs
}

// charToErr converts a *C.char to an error, freeing the given
// C string in the process.
func charToErr(c *C.char) error {
	s := C.GoString(c)
	C.free(unsafe.Pointer(c))
	return util.ErrorSkipFrames(1, s)
}

func emptyKeyError() error {
	return util.ErrorSkipFrames(1, "attempted access to empty key")
}

// bytesPointer returns a pointer to the first byte of the slice or
// nil if the byte slice is empty.
func bytesPointer(bytes []byte) *C.char {
	if len(bytes) > 0 {
		return (*C.char)(unsafe.Pointer(&bytes[0]))
	}
	// Empty values correspond to a null pointer.
	return (*C.char)(nil)
}

// Put sets the given key to the value provided.
//
// The key and value byte slices may be reused safely. put takes a copy of
// them before returning.
func (r *RocksDB) Put(key Key, value []byte) error {
	if len(key) == 0 {
		return emptyKeyError()
	}

	// rocksdb_put, _get, and _delete call memcpy() (by way of MemTable::Add)
	// when called, so we do not need to worry about these byte slices being
	// reclaimed by the GC.
	var cErr *C.char
	C.rocksdb_put(
		r.rdb,
		r.wOpts,
		bytesPointer(key),
		C.size_t(len(key)),
		bytesPointer(value),
		C.size_t(len(value)),
		&cErr)

	if cErr != nil {
		return charToErr(cErr)
	}
	return nil
}

// Merge implements the RocksDB merge operator using the function goMergeInit
// to initialize missing values and goMerge to merge the old and the given
// value into a new value, which is then stored under key.
// Currently 64-bit counter logic is implemented. See the documentation of
// goMerge and goMergeInit for details.
//
// The key and value byte slices may be reused safely. merge takes a copy
// of them before returning.
func (r *RocksDB) Merge(key Key, value []byte) error {
	if len(key) == 0 {
		return emptyKeyError()
	}

	// rocksdb_merge calls memcpy() (by way of MemTable::Add)
	// when called, so we do not need to worry about these byte slices being
	// reclaimed by the GC.
	var cErr *C.char
	C.rocksdb_merge(
		r.rdb,
		r.wOpts,
		bytesPointer(key),
		C.size_t(len(key)),
		bytesPointer(value),
		C.size_t(len(value)),
		&cErr)

	if cErr != nil {
		return charToErr(cErr)
	}
	return nil
}

// Get returns the value for the given key.
func (r *RocksDB) Get(key Key) ([]byte, error) {
	return r.getInternal(key, r.rOpts)
}

// GetSnapshot returns the value for the given key from the given
// snapshotID, nil otherwise.
func (r *RocksDB) GetSnapshot(key Key, snapshotID string) ([]byte, error) {
	snapshotHandle, ok := r.snapshots[snapshotID]
	if !ok {
		return nil, util.Errorf("snapshotID %s does not exist", snapshotID)
	}

	opts := C.rocksdb_readoptions_create()
	C.rocksdb_readoptions_set_snapshot(opts, snapshotHandle)
	defer C.rocksdb_readoptions_destroy(opts)
	return r.getInternal(key, opts)
}

// Get returns the value for the given key.
func (r *RocksDB) getInternal(key Key, rOpts *C.rocksdb_readoptions_t) ([]byte, error) {
	if len(key) == 0 {
		return nil, emptyKeyError()
	}
	var (
		cValLen C.size_t
		cErr    *C.char
	)

	cVal := C.rocksdb_get(
		r.rdb,
		rOpts,
		bytesPointer(key),
		C.size_t(len(key)),
		&cValLen,
		&cErr)

	if cErr != nil {
		return nil, charToErr(cErr)
	}
	if cVal == nil {
		return nil, nil
	}
	defer C.free(unsafe.Pointer(cVal))
	return C.GoBytes(unsafe.Pointer(cVal), C.int(cValLen)), nil
}

// Clear removes the item from the db with the given key.
func (r *RocksDB) Clear(key Key) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	var cErr *C.char
	C.rocksdb_delete(
		r.rdb,
		r.wOpts,
		bytesPointer(key),
		C.size_t(len(key)),
		&cErr)

	if cErr != nil {
		return charToErr(cErr)
	}
	return nil
}

// Scan returns up to max key/value objects starting from
// start (inclusive) and ending at end (non-inclusive).
// If max is zero then the number of key/values returned is unbounded.
func (r *RocksDB) Scan(start, end Key, max int64) ([]proto.RawKeyValue, error) {
	opts := C.rocksdb_readoptions_create()
	C.rocksdb_readoptions_set_fill_cache(opts, 0)
	defer C.rocksdb_readoptions_destroy(opts)
	return r.scanInternal(start, end, max, opts)
}

// ScanSnapshot returns up to max key/value objects starting from
// start (inclusive) and ending at end (non-inclusive) from the
// given snapshotID.
// Specify max=0 for unbounded scans.
func (r *RocksDB) ScanSnapshot(start, end Key, max int64, snapshotID string) ([]proto.RawKeyValue, error) {
	snapshotHandle, ok := r.snapshots[snapshotID]
	if !ok {
		return nil, util.Errorf("snapshotID %s does not exist", snapshotID)
	}

	opts := C.rocksdb_readoptions_create()
	C.rocksdb_readoptions_set_fill_cache(opts, 0)
	C.rocksdb_readoptions_set_snapshot(opts, snapshotHandle)
	defer C.rocksdb_readoptions_destroy(opts)
	return r.scanInternal(start, end, max, opts)
}

// scanInternal returns up to max key/value objects starting from
// start (inclusive) and ending at end (non-inclusive).
// If max is zero then the number of key/values returned is unbounded.
func (r *RocksDB) scanInternal(start, end Key, max int64, opts *C.rocksdb_readoptions_t) ([]proto.RawKeyValue, error) {
	var keyVals []proto.RawKeyValue
	if bytes.Compare(start, end) >= 0 {
		return keyVals, nil
	}
	// In order to prevent content displacement, caching is disabled
	// when performing scans. Any options set within the shared read
	// options field that should be carried over needs to be set here
	// as well.
	it := C.rocksdb_create_iterator(r.rdb, opts)
	defer C.rocksdb_iter_destroy(it)

	byteCount := len(start)
	if byteCount == 0 {
		// start=Key("") needs special treatment since we need
		// to access start[0] in an explicit seek.
		C.rocksdb_iter_seek_to_first(it)
	} else {
		C.rocksdb_iter_seek(it, bytesPointer(start), C.size_t(byteCount))
	}
	for i := int64(1); C.rocksdb_iter_valid(it) == 1; C.rocksdb_iter_next(it) {
		if max > 0 && i > max {
			break
		}
		var l C.size_t
		// The data returned by rocksdb_iter_{key,value} is not meant to be
		// freed by the client. It is a direct reference to the data managed
		// by the iterator, so it is copied instead of freed.
		data := C.rocksdb_iter_key(it, &l)
		k := C.GoBytes(unsafe.Pointer(data), C.int(l))
		if bytes.Compare(k, end) >= 0 {
			break
		}
		data = C.rocksdb_iter_value(it, &l)
		v := C.GoBytes(unsafe.Pointer(data), C.int(l))
		keyVals = append(keyVals, proto.RawKeyValue{
			Key:   k,
			Value: v,
		})
		i++
	}
	// Check for any errors during iteration.
	var cErr *C.char
	C.rocksdb_iter_get_error(it, &cErr)
	if cErr != nil {
		return nil, charToErr(cErr)
	}
	return keyVals, nil
}

// WriteBatch applies the puts, merges and deletes atomically via
// the RocksDB write batch facility. The list must only contain
// elements of type Batch{Put,Merge,Delete}.
func (r *RocksDB) WriteBatch(cmds []interface{}) error {
	if len(cmds) == 0 {
		return nil
	}
	batch := C.rocksdb_writebatch_create()
	defer C.rocksdb_writebatch_destroy(batch)
	for i, e := range cmds {
		switch v := e.(type) {
		case BatchDelete:
			if len(v.Key) == 0 {
				return emptyKeyError()
			}
			C.rocksdb_writebatch_delete(
				batch,
				bytesPointer(v.Key),
				C.size_t(len(v.Key)))
		case BatchPut:
			key, value := v.Key, v.Value
			// We write the batch before returning from this method, so we
			// don't need to worry about the GC reclaiming the data stored.
			C.rocksdb_writebatch_put(
				batch,
				bytesPointer(key),
				C.size_t(len(key)),
				bytesPointer(value),
				C.size_t(len(value)))
		case BatchMerge:
			key, value := v.Key, v.Value
			C.rocksdb_writebatch_merge(
				batch,
				bytesPointer(key),
				C.size_t(len(key)),
				bytesPointer(value),
				C.size_t(len(value)))
		default:
			panic(fmt.Sprintf("illegal operation #%d passed to writeBatch: %T", i, v))
		}
	}

	var cErr *C.char
	C.rocksdb_write(r.rdb, r.wOpts, batch, &cErr)
	if cErr != nil {
		return charToErr(cErr)
	}
	return nil
}

// Capacity queries the underlying file system for disk capacity
// information.
func (r *RocksDB) Capacity() (StoreCapacity, error) {
	var fs syscall.Statfs_t
	var capacity StoreCapacity
	if err := syscall.Statfs(r.dir, &fs); err != nil {
		return capacity, err
	}
	capacity.Capacity = int64(fs.Bsize) * int64(fs.Blocks)
	capacity.Available = int64(fs.Bsize) * int64(fs.Bavail)
	return capacity, nil
}

// SetGCTimeouts sets the garbage collector timeouts function.
func (r *RocksDB) SetGCTimeouts(gcTimeouts func() (minTxnTS, minRCacheTS int64)) {
	r.gcTimeouts = gcTimeouts
}

// CompactRange compacts the specified key range. Specifying nil for
// the start key starts the compaction from the start of the database.
// Similarly, specifying nil for the end key will compact through the
// last key. Note that the use of the word "Range" here does not refer
// to Cockroach ranges, just to a generalized key range.
func (r *RocksDB) CompactRange(start, end Key) {
	C.rocksdb_compact_range(r.rdb, bytesPointer(start), (C.size_t)(len(start)),
		bytesPointer(end), (C.size_t)(len(end)))
}

// Destroy destroys the underlying filesystem data associated with the database.
func (r *RocksDB) Destroy() error {
	cDir := C.CString(r.dir)
	defer C.free(unsafe.Pointer(cDir))

	defer r.destroyOptions()

	var cErr *C.char
	C.rocksdb_destroy_db(r.opts, cDir, &cErr)
	if cErr != nil {
		return charToErr(cErr)
	}
	return nil
}

// ApproximateSize returns the approximate number of bytes on disk that RocksDB
// is using to store data for the given range of keys.
func (r *RocksDB) ApproximateSize(start, end Key) (uint64, error) {
	// RocksDB's ApproximateSizes function operates on a set of ranges, the
	// various values of which are passed as parallel arrays. We are only
	// operating on a single range, but the call is still structured as if there
	// are multiple ranges.
	var (
		rngStarts   = []*C.char{bytesPointer(start)}
		rngLimits   = []*C.char{bytesPointer(end)}
		startSizes  = []C.size_t{(C.size_t)(len(start))}
		limitSizes  = []C.size_t{(C.size_t)(len(end))}
		returnSizes = make([]uint64, 1)
	)
	C.rocksdb_approximate_sizes(
		r.rdb,
		C.int(1),
		&rngStarts[0],
		&startSizes[0],
		&rngLimits[0],
		&limitSizes[0],
		(*C.uint64_t)(&returnSizes[0]),
	)
	return returnSizes[0], nil
}

// Flush causes RocksDB to write all in-memory data to disk immediately.
func (r *RocksDB) Flush() error {
	flushopts := C.rocksdb_flushoptions_create()
	defer C.rocksdb_flushoptions_destroy(flushopts)
	C.rocksdb_flushoptions_set_wait(flushopts, 1)

	var cErr *C.char
	C.rocksdb_flush(r.rdb,
		flushopts,
		&cErr)
	if cErr != nil {
		return charToErr(cErr)
	}
	return nil
}
