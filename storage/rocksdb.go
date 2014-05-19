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
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Andrew Bonventre (andybons@gmail.com)

package storage

// #cgo LDFLAGS: -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy
// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"

import (
	"errors"
	"flag"
	"syscall"
	"unsafe"

	"github.com/cockroachdb/cockroach/util"
	"github.com/golang/glog"
)

// defaultCacheSize is the default value for the cacheSize command line flag.
const defaultCacheSize = 1 << 30 // GB

// cacheSize is the amount of memory in bytes to use for caching data.
// The value is split evenly between the stores if there are more than one.
var cacheSize = flag.Int64("cache_size", defaultCacheSize, "total size in bytes for "+
	"caches, shared evenly if there are multiple storage devices")

// RocksDB is a wrapper around a RocksDB database instance.
type RocksDB struct {
	rdb *C.rocksdb_t // The DB handle
	typ DiskType     // HDD or SSD
	dir string       // The data directory
}

// NewRocksDB allocates and returns a new RocksDB object.
func NewRocksDB(typ DiskType, dir string) (*RocksDB, error) {
	r := &RocksDB{typ: typ, dir: dir}

	cDir := C.CString(dir)
	defer C.free(unsafe.Pointer(cDir))

	cOpts := C.rocksdb_options_create()
	defer C.rocksdb_options_destroy(cOpts)
	C.rocksdb_options_set_create_if_missing(cOpts, 1)
	// TODO(andybons): Set the cache size.

	var cErr *C.char
	if r.rdb = C.rocksdb_open(cOpts, cDir, &cErr); cErr != nil {
		r.rdb = nil
		s := C.GoString(cErr)
		C.free(unsafe.Pointer(cErr))
		return nil, errors.New(s)
	}
	if _, err := r.capacity(); err != nil {
		return nil, err
	}
	return r, nil
}

// Type returns either HDD or SSD depending on how engine
// was configured.
func (r *RocksDB) Type() DiskType {
	return r.typ
}

// charToErr converts a *C.char to an error, freeing the given
// C string in the process.
func charToErr(c *C.char) error {
	s := C.GoString(c)
	C.free(unsafe.Pointer(c))
	return errors.New(s)
}

// put sets the given key to the value provided.
//
// The key and value byte slices may be reused safely. put takes a copy of
// them before returning.
func (r *RocksDB) put(key Key, value Value) error {
	// rocksdb_put, _get, and _delete call memcpy() (by way of Memtable::Add)
	// when called, so we do not need to worry about these byte slices being
	// reclaimed by the GC.
	var cKey, cVal *C.char
	keyLen, valLen := len(key), len(value.Bytes)
	if keyLen > 0 {
		cKey = (*C.char)(unsafe.Pointer(&key[0]))
	}
	if valLen > 0 {
		cVal = (*C.char)(unsafe.Pointer(&value.Bytes[0]))
	}
	wOpts := C.rocksdb_writeoptions_create()
	defer C.rocksdb_writeoptions_destroy(wOpts)
	var cErr *C.char
	C.rocksdb_put(r.rdb, wOpts, cKey, C.size_t(keyLen), cVal, C.size_t(valLen), &cErr)
	if cErr != nil {
		return charToErr(cErr)
	}
	return nil
}

// get returns the value for the given key.
func (r *RocksDB) get(key Key) (Value, error) {
	keyLen := len(key)
	var cKey *C.char
	if keyLen > 0 {
		cKey = (*C.char)(unsafe.Pointer(&key[0]))
	}
	rOpts := C.rocksdb_readoptions_create()
	defer C.rocksdb_readoptions_destroy(rOpts)
	var (
		cValLen C.size_t
		cErr    *C.char
	)
	cVal := C.rocksdb_get(r.rdb, rOpts, cKey, C.size_t(keyLen), &cValLen, &cErr)
	if cErr != nil {
		return Value{}, charToErr(cErr)
	}
	if cVal == nil {
		return Value{}, nil
	}
	defer C.free(unsafe.Pointer(cVal))
	return Value{Bytes: C.GoBytes(unsafe.Pointer(cVal), C.int(cValLen))}, nil
}

// del removes the item from the db with the given key.
func (r *RocksDB) del(key Key) error {
	keyLen := len(key)
	var cKey *C.char
	if keyLen > 0 {
		cKey = (*C.char)(unsafe.Pointer(&key[0]))
	}
	wOpts := C.rocksdb_writeoptions_create()
	defer C.rocksdb_writeoptions_destroy(wOpts)
	var cErr *C.char
	C.rocksdb_delete(r.rdb, wOpts, cKey, C.size_t(keyLen), &cErr)
	if cErr != nil {
		return charToErr(cErr)
	}
	return nil
}

// scan returns up to max key/value objects starting from
// start (inclusive) and ending at end (non-inclusive).
func (r *RocksDB) scan(start, end Key, max int64) ([]KeyValue, error) {
	return []KeyValue{}, util.Error("scan unimplemented")
}

// capacity queries the underlying file system for disk capacity
// information.
func (r *RocksDB) capacity() (StoreCapacity, error) {
	var fs syscall.Statfs_t
	var capacity StoreCapacity
	if err := syscall.Statfs(r.dir, &fs); err != nil {
		return capacity, err
	}
	glog.Infof("stat filesystem: %v", fs)
	capacity.Capacity = int64(fs.Bsize) * int64(fs.Blocks)
	capacity.Available = int64(fs.Bsize) * int64(fs.Bavail)
	capacity.DiskType = r.typ
	return capacity, nil
}

// close closes the database by deallocating the underlying handle.
func (r *RocksDB) close() {
	C.rocksdb_close(r.rdb)
	r.rdb = nil
}
