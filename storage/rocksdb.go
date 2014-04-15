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
	"log"
	"syscall"
)

// RocksDB is a wrapper around a RocksDB database instance.
type RocksDB struct {
	dir  string // The data directory
	name string // The device name
}

// NewRocksDb allocates and returns a new InMem object.
func NewRocksDB(dir string) (*RocksDB, error) {
	r := &RocksDB{
		dir: dir,
	}
	if _, err := r.capacity(); err != nil {
		return nil, err
	}
	return r, nil
}

// put sets the given key to the value provided.
func (r *RocksDB) put(key Key, value Value) error {
	return nil
}

// get returns the value for the given key, nil otherwise.
func (r *RocksDB) get(key Key) (Value, error) {
	return Value{}, nil
}

// del removes the item from the db with the given key.
func (r *RocksDB) del(key Key) error {
	return nil
}

// capacity queries the underlying file system for disk capacity
// information.
func (r *RocksDB) capacity() (*DiskCapacity, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(r.dir, &fs); err != nil {
		return nil, err
	}
	log.Println("stat filesystem: %v", fs)
	if r.name == "" {
		// TODO(spencer): set name.
	}
	// TODO(spencer): set values in DiskCapacity struct.
	// capacity = fs.Bsize * fs.Blocks
	// available = fs.Bsize * fs.Bavail
	// percentAvail = float64(available) / float64(capacity)
	capacity := &DiskCapacity{}
	return capacity, nil
}
