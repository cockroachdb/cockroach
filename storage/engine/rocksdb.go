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

import (
	"bytes"
	"errors"
	"fmt"
	"syscall"
	"unsafe"

	"github.com/cockroachdb/cockroach/storage/engine/rocksdb"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	gogoproto "github.com/gogo/protobuf/proto"
)

// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
// #cgo linux LDFLAGS: -lrt
//
// #include <stdlib.h>
// #include "rocksdb/db.h"
import "C"

func init() {
	rocksdb.Logger = log.Infof
}

// RocksDB is a wrapper around a RocksDB database instance.
type RocksDB struct {
	rdb         *C.DBEngine
	attrs       proto.Attributes // Attributes for this engine
	dir         string           // The data directory
	cacheSize   int64            // Memory to use to cache values.
	stopper     *stop.Stopper
	deallocated chan struct{} // Closed when the underlying handle is deallocated.
}

// NewRocksDB allocates and returns a new RocksDB object.
func NewRocksDB(attrs proto.Attributes, dir string, cacheSize int64, stopper *stop.Stopper) *RocksDB {
	if dir == "" {
		panic(util.Errorf("dir must be non-empty"))
	}
	return &RocksDB{
		attrs:       attrs,
		dir:         dir,
		cacheSize:   cacheSize,
		stopper:     stopper,
		deallocated: make(chan struct{}),
	}
}

func newMemRocksDB(attrs proto.Attributes, cacheSize int64, stopper *stop.Stopper) *RocksDB {
	return &RocksDB{
		attrs: attrs,
		// dir: empty dir == "mem" RocksDB instance.
		cacheSize:   cacheSize,
		stopper:     stopper,
		deallocated: make(chan struct{}),
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

	if len(r.dir) != 0 {
		log.Infof("opening rocksdb instance at %q", r.dir)
	}
	status := C.DBOpen(&r.rdb, goToCSlice([]byte(r.dir)),
		C.DBOptions{
			cache_size:      C.int64_t(r.cacheSize),
			allow_os_buffer: C.bool(true),
			logging_enabled: C.bool(log.V(3)),
		})
	err := statusToError(status)
	if err != nil {
		return util.Errorf("could not open rocksdb instance: %s", err)
	}

	// Start a gorountine that will finish when the underlying handle
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
		log.Infof("closing in-memory rocksdb instance")
	} else {
		log.Infof("closing rocksdb instance at %q", r.dir)
	}
	if r.rdb != nil {
		C.DBClose(r.rdb)
		r.rdb = nil
	}
	close(r.deallocated)
}

// Attrs returns the list of attributes describing this engine. This
// may include a specification of disk type (e.g. hdd, ssd, fio, etc.)
// and potentially other labels to identify important attributes of
// the engine.
func (r *RocksDB) Attrs() proto.Attributes {
	return r.attrs
}

func emptyKeyError() error {
	return util.ErrorfSkipFrames(1, "attempted access to empty key")
}

// Put sets the given key to the value provided.
//
// The key and value byte slices may be reused safely. put takes a copy of
// them before returning.
func (r *RocksDB) Put(key proto.EncodedKey, value []byte) error {
	if len(key) == 0 {
		return emptyKeyError()
	}

	// *Put, *Get, and *Delete call memcpy() (by way of MemTable::Add)
	// when called, so we do not need to worry about these byte slices
	// being reclaimed by the GC.
	return statusToError(C.DBPut(r.rdb, goToCSlice(key), goToCSlice(value)))
}

// Merge implements the RocksDB merge operator using the function goMergeInit
// to initialize missing values and goMerge to merge the old and the given
// value into a new value, which is then stored under key.
// Currently 64-bit counter logic is implemented. See the documentation of
// goMerge and goMergeInit for details.
//
// The key and value byte slices may be reused safely. merge takes a copy
// of them before returning.
func (r *RocksDB) Merge(key proto.EncodedKey, value []byte) error {
	if len(key) == 0 {
		return emptyKeyError()
	}

	// DBMerge calls memcpy() (by way of MemTable::Add)
	// when called, so we do not need to worry about these byte slices being
	// reclaimed by the GC.
	return statusToError(C.DBMerge(r.rdb, goToCSlice(key), goToCSlice(value)))
}

// Get returns the value for the given key.
func (r *RocksDB) Get(key proto.EncodedKey) ([]byte, error) {
	return r.getInternal(key, nil)
}

// getInternal returns the value for the given key.
func (r *RocksDB) getInternal(key proto.EncodedKey, snapshotHandle *C.DBSnapshot) ([]byte, error) {
	if len(key) == 0 {
		return nil, emptyKeyError()
	}
	var result C.DBString
	err := statusToError(C.DBGet(r.rdb, snapshotHandle, goToCSlice(key), &result))
	if err != nil {
		return nil, err
	}
	return cStringToGoBytes(result), nil
}

// GetProto fetches the value at the specified key and unmarshals it.
func (r *RocksDB) GetProto(key proto.EncodedKey, msg gogoproto.Message) (
	ok bool, keyBytes, valBytes int64, err error) {
	return r.getProtoInternal(key, msg, nil)
}

func (r *RocksDB) getProtoInternal(key proto.EncodedKey, msg gogoproto.Message,
	snapshotHandle *C.DBSnapshot) (ok bool, keyBytes, valBytes int64, err error) {
	if len(key) == 0 {
		err = emptyKeyError()
		return
	}
	var result C.DBString
	if err = statusToError(C.DBGet(r.rdb, snapshotHandle, goToCSlice(key), &result)); err != nil {
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
		// using it to unmarshal the proto.
		data := cSliceToUnsafeGoBytes(C.DBSlice(result))
		err = gogoproto.Unmarshal(data, msg)
	}
	C.free(unsafe.Pointer(result.data))
	keyBytes = int64(len(key))
	valBytes = int64(result.len)
	return
}

// Clear removes the item from the db with the given key.
func (r *RocksDB) Clear(key proto.EncodedKey) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	return statusToError(C.DBDelete(r.rdb, goToCSlice(key)))
}

// Iterate iterates from start to end keys, invoking f on each
// key/value pair. See engine.Iterate for details.
func (r *RocksDB) Iterate(start, end proto.EncodedKey, f func(proto.RawKeyValue) (bool, error)) error {
	return r.iterateInternal(start, end, f, nil)
}

func (r *RocksDB) iterateInternal(start, end proto.EncodedKey, f func(proto.RawKeyValue) (bool, error),
	snapshotHandle *C.DBSnapshot) error {
	if bytes.Compare(start, end) >= 0 {
		return nil
	}
	it := newRocksDBIterator(r.rdb, snapshotHandle)
	defer it.Close()

	it.Seek(start)
	for ; it.Valid(); it.Next() {
		k := it.Key()
		if !it.Key().Less(end) {
			break
		}
		if done, err := f(proto.RawKeyValue{Key: k, Value: it.Value()}); done || err != nil {
			return err
		}
	}
	// Check for any errors during iteration.
	return it.Error()
}

// Capacity queries the underlying file system for disk capacity information.
func (r *RocksDB) Capacity() (proto.StoreCapacity, error) {
	var fs syscall.Statfs_t
	var capacity proto.StoreCapacity
	dir := r.dir
	if dir == "" {
		dir = "/tmp"
	}
	if err := syscall.Statfs(dir, &fs); err != nil {
		return capacity, err
	}
	capacity.Capacity = int64(fs.Bsize) * int64(fs.Blocks)
	capacity.Available = int64(fs.Bsize) * int64(fs.Bavail)
	return capacity, nil
}

// SetGCTimeouts calls through to the DBEngine's SetGCTimeouts method.
func (r *RocksDB) SetGCTimeouts(minTxnTS, minRCacheTS int64) {
	C.DBSetGCTimeouts(r.rdb, C.int64_t(minTxnTS), C.int64_t(minRCacheTS))
}

// CompactRange compacts the specified key range. Specifying nil for
// the start key starts the compaction from the start of the database.
// Similarly, specifying nil for the end key will compact through the
// last key. Note that the use of the word "Range" here does not refer
// to Cockroach ranges, just to a generalized key range.
func (r *RocksDB) CompactRange(start, end proto.EncodedKey) {
	var (
		s, e       C.DBSlice
		sPtr, ePtr *C.DBSlice
	)
	if start != nil {
		sPtr = &s
		s = goToCSlice(start)
	}
	if end != nil {
		ePtr = &e
		e = goToCSlice(end)
	}
	err := statusToError(C.DBCompactRange(r.rdb, sPtr, ePtr))
	if err != nil {
		log.Warningf("compact range: %s", err)
	}
}

// Destroy destroys the underlying filesystem data associated with the database.
func (r *RocksDB) Destroy() error {
	return statusToError(C.DBDestroy(goToCSlice([]byte(r.dir))))
}

// ApproximateSize returns the approximate number of bytes on disk that RocksDB
// is using to store data for the given range of keys.
func (r *RocksDB) ApproximateSize(start, end proto.EncodedKey) (uint64, error) {
	return uint64(C.DBApproximateSize(r.rdb, goToCSlice(start), goToCSlice(end))), nil
}

// Flush causes RocksDB to write all in-memory data to disk immediately.
func (r *RocksDB) Flush() error {
	return statusToError(C.DBFlush(r.rdb))
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
// be marshalled proto.Values and merges the two values returning a
// marshalled proto.Value or an error.
func goMerge(existing, update []byte) ([]byte, error) {
	var result C.DBString
	status := C.DBMergeOne(goToCSlice(existing), goToCSlice(update), &result)
	if status.data != nil {
		return nil, util.Errorf("%s: existing=%q, update=%q",
			cStringToGoString(status), existing, update)
	}
	return cStringToGoBytes(result), nil
}

// NewIterator returns an iterator over this rocksdb engine.
func (r *RocksDB) NewIterator() Iterator {
	return newRocksDBIterator(r.rdb, nil)
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

type rocksDBSnapshot struct {
	parent *RocksDB
	handle *C.DBSnapshot
}

// Open is a noop.
func (r *rocksDBSnapshot) Open() error {
	return nil
}

// Close releases the snapshot handle.
func (r *rocksDBSnapshot) Close() {
	C.DBSnapshotRelease(r.handle)
}

// Attrs returns the engine/store attributes.
func (r *rocksDBSnapshot) Attrs() proto.Attributes {
	return r.parent.Attrs()
}

// Put is illegal for snapshot and returns an error.
func (r *rocksDBSnapshot) Put(key proto.EncodedKey, value []byte) error {
	return util.Errorf("cannot Put to a snapshot")
}

// Get returns the value for the given key, nil otherwise using
// the snapshot handle.
func (r *rocksDBSnapshot) Get(key proto.EncodedKey) ([]byte, error) {
	return r.parent.getInternal(key, r.handle)
}

func (r *rocksDBSnapshot) GetProto(key proto.EncodedKey, msg gogoproto.Message) (
	ok bool, keyBytes, valBytes int64, err error) {
	return r.parent.getProtoInternal(key, msg, r.handle)
}

// Iterate iterates over the keys between start inclusive and end
// exclusive, invoking f() on each key/value pair using the snapshot
// handle.
func (r *rocksDBSnapshot) Iterate(start, end proto.EncodedKey, f func(proto.RawKeyValue) (bool, error)) error {
	return r.parent.iterateInternal(start, end, f, r.handle)
}

// Clear is illegal for snapshot and returns an error.
func (r *rocksDBSnapshot) Clear(key proto.EncodedKey) error {
	return util.Errorf("cannot Clear from a snapshot")
}

// Merge is illegal for snapshot and returns an error.
func (r *rocksDBSnapshot) Merge(key proto.EncodedKey, value []byte) error {
	return util.Errorf("cannot Merge to a snapshot")
}

// Capacity returns capacity details for the engine's available storage.
func (r *rocksDBSnapshot) Capacity() (proto.StoreCapacity, error) {
	return r.parent.Capacity()
}

// SetGCTimeouts is a noop for a snapshot.
func (r *rocksDBSnapshot) SetGCTimeouts(minTxnTS, minRCacheTS int64) {
}

// ApproximateSize returns the approximate number of bytes the engine is
// using to store data for the given range of keys.
func (r *rocksDBSnapshot) ApproximateSize(start, end proto.EncodedKey) (uint64, error) {
	return r.parent.ApproximateSize(start, end)
}

// Flush is a no-op for snapshots.
func (r *rocksDBSnapshot) Flush() error {
	return nil
}

// NewIterator returns a new instance of an Iterator over the
// engine using the snapshot handle.
func (r *rocksDBSnapshot) NewIterator() Iterator {
	return newRocksDBIterator(r.parent.rdb, r.handle)
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

type rocksDBBatch struct {
	parent *RocksDB
	batch  *C.DBBatch
	defers []func()
}

func newRocksDBBatch(r *RocksDB) *rocksDBBatch {
	return &rocksDBBatch{
		parent: r,
		batch:  C.DBNewBatch(),
	}
}

func (r *rocksDBBatch) Open() error {
	return util.Errorf("cannot open a batch")
}

func (r *rocksDBBatch) Close() {
	if r.batch != nil {
		C.DBBatchDestroy(r.batch)
	}
}

// Attrs returns the engine/store attributes.
func (r *rocksDBBatch) Attrs() proto.Attributes {
	return r.parent.Attrs()
}

func (r *rocksDBBatch) Put(key proto.EncodedKey, value []byte) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	C.DBBatchPut(r.batch, goToCSlice(key), goToCSlice(value))
	return nil
}

func (r *rocksDBBatch) Merge(key proto.EncodedKey, value []byte) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	C.DBBatchMerge(r.batch, goToCSlice(key), goToCSlice(value))
	return nil
}

func (r *rocksDBBatch) Get(key proto.EncodedKey) ([]byte, error) {
	if len(key) == 0 {
		return nil, emptyKeyError()
	}
	var result C.DBString
	err := statusToError(C.DBBatchGet(r.parent.rdb, r.batch, goToCSlice(key), &result))
	if err != nil {
		return nil, err
	}
	return cStringToGoBytes(result), nil
}

func (r *rocksDBBatch) GetProto(key proto.EncodedKey, msg gogoproto.Message) (
	ok bool, keyBytes, valBytes int64, err error) {
	if len(key) == 0 {
		err = emptyKeyError()
		return
	}
	var result C.DBString
	if err = statusToError(C.DBBatchGet(r.parent.rdb, r.batch, goToCSlice(key), &result)); err != nil {
		return
	}
	if result.len <= 0 {
		return
	}
	ok = true
	if msg != nil {
		// Make a byte slice that is backed by result.data. This slice
		// cannot live past the lifetime of this method, but we're only
		// using it to unmarshal the proto.
		data := cSliceToUnsafeGoBytes(C.DBSlice(result))
		err = gogoproto.Unmarshal(data, msg)
	}
	C.free(unsafe.Pointer(result.data))
	keyBytes = int64(len(key))
	valBytes = int64(result.len)
	return
}

func (r *rocksDBBatch) Iterate(start, end proto.EncodedKey, f func(proto.RawKeyValue) (bool, error)) error {
	if bytes.Compare(start, end) >= 0 {
		return nil
	}
	it := &rocksDBIterator{
		iter: C.DBBatchNewIter(r.parent.rdb, r.batch),
	}
	defer it.Close()

	it.Seek(start)
	for ; it.Valid(); it.Next() {
		k := it.Key()
		if !it.Key().Less(end) {
			break
		}
		if done, err := f(proto.RawKeyValue{Key: k, Value: it.Value()}); done || err != nil {
			return err
		}
	}
	// Check for any errors during iteration.
	return it.Error()
}

func (r *rocksDBBatch) Clear(key proto.EncodedKey) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	C.DBBatchDelete(r.batch, goToCSlice(key))
	return nil
}

func (r *rocksDBBatch) Capacity() (proto.StoreCapacity, error) {
	return r.parent.Capacity()
}

func (r *rocksDBBatch) SetGCTimeouts(minTxnTS, minRCacheTS int64) {
	// no-op
}

func (r *rocksDBBatch) ApproximateSize(start, end proto.EncodedKey) (uint64, error) {
	return r.parent.ApproximateSize(start, end)
}

func (r *rocksDBBatch) Flush() error {
	return util.Errorf("cannot flush a batch")
}

func (r *rocksDBBatch) NewIterator() Iterator {
	return &rocksDBIterator{
		iter: C.DBBatchNewIter(r.parent.rdb, r.batch),
	}
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
	if err := statusToError(C.DBWrite(r.parent.rdb, r.batch)); err != nil {
		return err
	}
	C.DBBatchDestroy(r.batch)
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

type rocksDBIterator struct {
	iter *C.DBIterator
}

// newRocksDBIterator returns a new iterator over the supplied RocksDB
// instance. If snapshotHandle is not nil, uses the indicated snapshot.
// The caller must call rocksDBIterator.Close() when finished with the
// iterator to free up resources.
func newRocksDBIterator(rdb *C.DBEngine, snapshotHandle *C.DBSnapshot) *rocksDBIterator {
	// In order to prevent content displacement, caching is disabled
	// when performing scans. Any options set within the shared read
	// options field that should be carried over needs to be set here
	// as well.
	return &rocksDBIterator{
		iter: C.DBNewIter(rdb, snapshotHandle),
	}
}

// The following methods implement the Iterator interface.
func (r *rocksDBIterator) Close() {
	C.DBIterDestroy(r.iter)
}

func (r *rocksDBIterator) Seek(key []byte) {
	if len(key) == 0 {
		// start=Key("") needs special treatment since we need
		// to access start[0] in an explicit seek.
		C.DBIterSeekToFirst(r.iter)
	} else {
		C.DBIterSeek(r.iter, goToCSlice(key))
	}
}

func (r *rocksDBIterator) Valid() bool {
	return C.DBIterValid(r.iter) == 1
}

func (r *rocksDBIterator) Next() {
	C.DBIterNext(r.iter)
}

func (r *rocksDBIterator) SeekReverse(key []byte) {
	if len(key) == 0 {
		C.DBIterSeekToLast(r.iter)
	} else {
		C.DBIterSeek(r.iter, goToCSlice(key))
		// Maybe the key sorts after the last key in RocksDB.
		if !r.Valid() {
			C.DBIterSeekToLast(r.iter)
		}
		if !r.Valid() {
			return
		}
		// Make sure the current key is <= the provided key.
		curKey := r.Key()
		if proto.EncodedKey(key).Less(curKey) {
			r.Prev()
		}
	}
}

func (r *rocksDBIterator) Prev() {
	C.DBIterPrev(r.iter)
}

func (r *rocksDBIterator) Key() proto.EncodedKey {
	// The data returned by rocksdb_iter_{key,value} is not meant to be
	// freed by the client. It is a direct reference to the data managed
	// by the iterator, so it is copied instead of freed.
	data := C.DBIterKey(r.iter)
	return cSliceToGoBytes(data)
}

func (r *rocksDBIterator) Value() []byte {
	data := C.DBIterValue(r.iter)
	return cSliceToGoBytes(data)
}

func (r *rocksDBIterator) ValueProto(msg gogoproto.Message) error {
	result := C.DBIterValue(r.iter)
	if result.len <= 0 {
		return nil
	}
	// Make a byte slice that is backed by result.data. This slice
	// cannot live past the lifetime of this method, but we're only
	// using it to unmarshal the proto.
	data := cSliceToUnsafeGoBytes(result)
	return gogoproto.Unmarshal(data, msg)
}

func (r *rocksDBIterator) Error() error {
	return statusToError(C.DBIterError(r.iter))
}
