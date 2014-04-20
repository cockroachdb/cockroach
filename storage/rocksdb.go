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

package storage

import (
	"flag"
	"syscall"

	"github.com/cockroachdb/cockroach/util"
	"github.com/golang/glog"
)

const (
	// defaultCacheSize is the default value for the cacheSize command line flag.
	defaultCacheSize = 1 << 30 // GB
)

var (
	// cacheSize is the amount of memory in bytes to use for caching data.
	// The value is split evenly between the stores if there are more than one.
	cacheSize = flag.Int64("cache_size", defaultCacheSize, "total size in bytes for "+
		"caches, shared evenly if there are multiple storage devices")
)

// RocksDB is a wrapper around a RocksDB database instance.
type RocksDB struct {
	typ DiskType // HDD or SSD
	dir string   // The data directory
}

// NewRocksDB allocates and returns a new InMem object.
func NewRocksDB(typ DiskType, dir string) (*RocksDB, error) {
	r := &RocksDB{
		typ: typ,
		dir: dir,
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

// put sets the given key to the value provided.
func (r *RocksDB) put(key Key, value Value) error {
	return util.Error("put unimplemented")
}

// get returns the value for the given key, nil otherwise.
func (r *RocksDB) get(key Key) (Value, error) {
	return Value{}, util.Error("get unimplemented")
}

// scan returns up to max key/value objects starting from
// start (inclusive) and ending at end (non-inclusive).
func (r *RocksDB) scan(start, end Key, max int64) ([]KeyValue, error) {
	return []KeyValue{}, util.Error("scan unimplemented")
}

// del removes the item from the db with the given key.
func (r *RocksDB) del(key Key) error {
	return util.Error("del unimplemented")
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
