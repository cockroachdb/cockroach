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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"bytes"
	"sync"
	"unsafe"

	"code.google.com/p/biogo.store/llrb"
	"github.com/cockroachdb/cockroach/util"
)

var (
	llrbNodeSize = int64(unsafe.Sizeof(llrb.Node{}))
	keyValueSize = int64(unsafe.Sizeof(KeyValue{}))
)

// computeSize returns the approximate size in bytes that the keyVal
// object took while stored in the underlying LLRB.
func computeSize(kv KeyValue) int64 {
	return int64(len(kv.Key)) + int64(len(kv.Value.Bytes)) + llrbNodeSize + keyValueSize
}

// Implement the llrb.Comparable interface for tree nodes.
func (kv KeyValue) Compare(b llrb.Comparable) int {
	return bytes.Compare(kv.Key, b.(KeyValue).Key)
}

// InMem a simple, in-memory key-value store.
type InMem struct {
	sync.RWMutex
	maxBytes  int64
	usedBytes int64
	data      llrb.Tree
}

// NewInMem allocates and returns a new InMem object.
func NewInMem(maxBytes int64) *InMem {
	return &InMem{
		maxBytes: maxBytes,
	}
}

// put sets the given key to the value provided.
func (in *InMem) put(key Key, value Value) error {
	in.Lock()
	defer in.Unlock()
	kv := KeyValue{Key: key, Value: value}
	size := computeSize(kv)
	if size+in.usedBytes > in.maxBytes {
		return util.Errorf("in mem store at capacity %d + %d > %d", in.usedBytes, size, in.maxBytes)
	}
	in.usedBytes += size
	in.data.Insert(kv)
	return nil
}

// get returns the value for the given key, nil otherwise.
func (in *InMem) get(key Key) (Value, error) {
	in.RLock()
	defer in.RUnlock()
	val := in.data.Get(KeyValue{Key: key})
	if val == nil {
		return Value{}, nil
	}
	return val.(KeyValue).Value, nil
}

// scan returns up to max key/value objects starting from
// start (inclusive) and ending at end (non-inclusive).
func (in *InMem) scan(start, end Key, max int) ([]KeyValue, error) {
	in.RLock()
	defer in.RUnlock()

	var scanned []KeyValue = nil
	in.data.DoRange(func(kv llrb.Comparable) (done bool) {
		if len(scanned) >= max {
			done = true
			return
		}
		scanned = append(scanned, kv.(KeyValue))
		return
	}, KeyValue{Key: start}, KeyValue{Key: end})

	return scanned, nil
}

// del removes the item from the db with the given key.
func (in *InMem) del(key Key) error {
	in.Lock()
	defer in.Unlock()
	// Note: this is approximate. There is likely something missing.
	// The storage/in_mem_test.go benchmarks this and the measurement
	// being made seems close enough for government work (tm).
	if val := in.data.Get(KeyValue{Key: key}); val != nil {
		in.usedBytes -= computeSize(val.(KeyValue))
	}
	in.data.Delete(KeyValue{Key: key})
	return nil
}

// capacity formulates available space based on cache size and
// computed size of cached keys and values. The actual free space may
// not be entirely accurate due to object storage costs and other
// internal glue.
func (in *InMem) capacity() (StoreCapacity, error) {
	return StoreCapacity{
		Capacity:  in.maxBytes,
		Available: in.maxBytes - in.usedBytes,
		DiskType:  MEM,
	}, nil
}
