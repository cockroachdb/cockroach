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
// Author: Andrew Bonventre (andybons@gmail.com)

package storage

import "sync"

// InMem a simple, in-memory key-value store.
type InMem struct {
	sync.RWMutex
	maxSize int64
	data    map[string]string
}

// NewInMem allocates and returns a new InMem object.
func NewInMem(maxSize int64) *InMem {
	return &InMem{
		maxSize: maxSize,
		data:    make(map[string]string),
	}
}

// put sets the given key to the value provided.
func (in *InMem) put(key Key, value Value) error {
	in.Lock()
	defer in.Unlock()
	in.data[string(key)] = string(value.Bytes)
	return nil
}

// get returns the value for the given key, nil otherwise.
func (in *InMem) get(key Key) (Value, error) {
	in.RLock()
	defer in.RUnlock()
	return Value{
		Bytes: []byte(in.data[string(key)]),
	}, nil
}

// del removes the item from the db with the given key.
func (in *InMem) del(key Key) error {
	in.Lock()
	defer in.Unlock()
	delete(in.data, string(key))
	return nil
}

// capacity formulates available space based on cache size and
// computed size of cached keys and values. The actual free space may
// not be entirely accurate due to object storage costs and other
// internal glue.
func (in *InMem) capacity() (StoreCapacity, error) {
	return StoreCapacity{
		Capacity:  in.maxSize,
		Available: in.maxSize, // TODO(spencer): fix this.
		DiskType:  MEM,
	}, nil
}
