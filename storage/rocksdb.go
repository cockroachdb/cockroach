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
// #cgo linux LDFLAGS: -lrt
// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"

import (
	"bytes"
	"errors"
	"flag"
	"syscall"
	"unsafe"

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
	rdb   *C.rocksdb_t              // The DB handle
	opts  *C.rocksdb_options_t      // Options used when creating or destroying
	rOpts *C.rocksdb_readoptions_t  // The default read options
	wOpts *C.rocksdb_writeoptions_t // The default write options

	typ DiskType // HDD or SSD
	dir string   // The data directory
}

// NewRocksDB allocates and returns a new RocksDB object.
func NewRocksDB(typ DiskType, dir string) (*RocksDB, error) {
	r := &RocksDB{typ: typ, dir: dir}
	r.createOptions()

	cDir := C.CString(dir)
	defer C.free(unsafe.Pointer(cDir))

	var cErr *C.char
	if r.rdb = C.rocksdb_open(r.opts, cDir, &cErr); cErr != nil {
		r.rdb = nil
		r.destroyOptions()
		s := C.GoString(cErr)
		C.free(unsafe.Pointer(cErr))
		// TODO(andybons): use util.Error.
		return nil, errors.New(s)
	}
	if _, err := r.capacity(); err != nil {
		if err := r.destroy(); err != nil {
			glog.Warningf("could not destroy db at %s", dir)
		}
		return nil, err
	}
	return r, nil
}

// destroy destroys the underlying filesystem data associated with the database.
func (r *RocksDB) destroy() error {
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

// createOptions sets the default options for creating, reading, and writing
// from the db. destroyOptions should be called when the options aren't needed
// anymore.
func (r *RocksDB) createOptions() {
	// TODO(andybons): Set the cache size.
	r.opts = C.rocksdb_options_create()
	C.rocksdb_options_set_create_if_missing(r.opts, 1)

	r.wOpts = C.rocksdb_writeoptions_create()
	r.rOpts = C.rocksdb_readoptions_create()
}

// destroyOptions destroys the options used for creating, reading, and writing
// from the db. It is meant to be used in conjunction with createOptions.
func (r *RocksDB) destroyOptions() {
	C.rocksdb_options_destroy(r.opts)
	C.rocksdb_readoptions_destroy(r.rOpts)
	C.rocksdb_writeoptions_destroy(r.wOpts)
	r.opts = nil
	r.rOpts = nil
	r.wOpts = nil
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
	// TODO(andybons): use util.Error.
	return errors.New(s)
}

// put sets the given key to the value provided.
//
// The key and value byte slices may be reused safely. put takes a copy of
// them before returning.
func (r *RocksDB) put(key Key, value Value) error {
	// rocksdb_put, _get, and _delete call memcpy() (by way of MemTable::Add)
	// when called, so we do not need to worry about these byte slices being
	// reclaimed by the GC.
	var cErr *C.char
	C.rocksdb_put(
		r.rdb,
		r.wOpts,
		(*C.char)(unsafe.Pointer(&key[0])),
		C.size_t(len(key)),
		(*C.char)(unsafe.Pointer(&value.Bytes[0])),
		C.size_t(len(value.Bytes)),
		&cErr)

	if cErr != nil {
		return charToErr(cErr)
	}
	return nil
}

// get returns the value for the given key.
func (r *RocksDB) get(key Key) (Value, error) {
	var (
		cValLen C.size_t
		cErr    *C.char
	)
	cVal := C.rocksdb_get(
		r.rdb,
		r.rOpts,
		(*C.char)(unsafe.Pointer(&key[0])),
		C.size_t(len(key)),
		&cValLen,
		&cErr)

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
	var cErr *C.char
	C.rocksdb_delete(
		r.rdb,
		r.wOpts,
		(*C.char)(unsafe.Pointer(&key[0])),
		C.size_t(len(key)),
		&cErr)

	if cErr != nil {
		return charToErr(cErr)
	}
	return nil
}

// scan returns up to max key/value objects starting from
// start (inclusive) and ending at end (non-inclusive).
// If max is zero then the number of key/values returned is unbounded.
func (r *RocksDB) scan(start, end Key, max int64) ([]KeyValue, error) {
	// In order to prevent content displacement, caching is disabled
	// when performing scans. Any options set within the shared read
	// options field that should be carried over needs to be set here
	// as well.
	opts := C.rocksdb_readoptions_create()
	C.rocksdb_readoptions_set_fill_cache(opts, 0)
	defer C.rocksdb_readoptions_destroy(opts)
	it := C.rocksdb_create_iterator(r.rdb, opts)
	defer C.rocksdb_iter_destroy(it)

	keyVals := []KeyValue{}
	C.rocksdb_iter_seek(it, (*C.char)(unsafe.Pointer(&start)), C.size_t(len(start)))
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
		if bytes.Equal(k, end) {
			break
		}
		data = C.rocksdb_iter_value(it, &l)
		v := C.GoBytes(unsafe.Pointer(data), C.int(l))
		keyVals = append(keyVals, KeyValue{
			Key:   k,
			Value: Value{Bytes: v},
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
