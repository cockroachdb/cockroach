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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package storage

import (
	"bytes"
	"fmt"
	"reflect"
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

// Compare implements the llrb.Comparable interface for tree nodes.
func (kv KeyValue) Compare(b llrb.Comparable) int {
	return bytes.Compare(kv.Key, b.(KeyValue).Key)
}

// InMem a simple, in-memory key-value store.
type InMem struct {
	sync.RWMutex
	attrs     Attributes
	maxBytes  int64
	usedBytes int64
	data      llrb.Tree
}

// NewInMem allocates and returns a new InMem object.
func NewInMem(attrs Attributes, maxBytes int64) *InMem {
	return &InMem{
		attrs:    attrs,
		maxBytes: maxBytes,
	}
}

// String formatter.
func (in *InMem) String() string {
	return fmt.Sprintf("%s=%d", in.attrs, in.maxBytes)
}

// Attrs returns the list of attributes describing this engine.  This
// includes the disk type (always "mem") and potentially other labels
// to identify important attributes of the engine.
func (in *InMem) Attrs() Attributes {
	return in.attrs
}

// put sets the given key to the value provided.
func (in *InMem) put(key Key, value Value) error {
	in.Lock()
	defer in.Unlock()
	return in.putLocked(key, value)
}

// putLocked assumes mutex is already held by caller. See put().
func (in *InMem) putLocked(key Key, value Value) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	kv := KeyValue{Key: key, Value: value}
	size := computeSize(kv)
	// If the key already exists, compute the size change of the
	// replacement with the new value.
	if val := in.data.Get(KeyValue{Key: key}); val != nil {
		size -= computeSize(val.(KeyValue))
	}

	if size > in.maxBytes-in.usedBytes {
		return util.Errorf("in mem store at capacity %d + %d > %d", in.usedBytes, size, in.maxBytes)
	}
	in.usedBytes += size
	in.data.Insert(kv)
	return nil
}

// merge implements a merge operation which updates the existing value stored
// under key based on the value passed.
// See the documentation of goMerge and goMergeInit for details.
func (in *InMem) merge(key Key, value Value) error {
	in.Lock()
	defer in.Unlock()
	return in.mergeLocked(key, value)
}

// mergeLocked assumes the mutex is already held by the caller. See merge().
func (in *InMem) mergeLocked(key Key, value Value) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	existingVal, err := in.getLocked(key)
	if err != nil {
		return err
	}
	// Emulate RocksDB errors by... not having errors.
	newValue, _ := goMerge(existingVal.Bytes, value.Bytes)
	return in.putLocked(key, Value{Bytes: newValue})
}

// get returns the value for the given key, nil otherwise.
func (in *InMem) get(key Key) (Value, error) {
	in.RLock()
	defer in.RUnlock()
	return in.getLocked(key)
}

// getLocked performs a get operation assuming that the caller
// is already holding the mutex.
func (in *InMem) getLocked(key Key) (Value, error) {
	if len(key) == 0 {
		return Value{}, emptyKeyError()
	}
	val := in.data.Get(KeyValue{Key: key})
	if val == nil {
		return Value{}, nil
	}
	return val.(KeyValue).Value, nil
}

// scan returns up to max key/value objects starting from
// start (inclusive) and ending at end (non-inclusive).
func (in *InMem) scan(start, end Key, max int64) ([]KeyValue, error) {
	in.RLock()
	defer in.RUnlock()
	return in.scanLocked(start, end, max)
}

// scanLocked is intended to be called within at least a read lock.
func (in *InMem) scanLocked(start, end Key, max int64) ([]KeyValue, error) {
	var scanned []KeyValue
	in.data.DoRange(func(kv llrb.Comparable) (done bool) {
		if max != 0 && int64(len(scanned)) >= max {
			done = true
			return
		}
		scanned = append(scanned, kv.(KeyValue))
		return
	}, KeyValue{Key: start}, KeyValue{Key: end})

	return scanned, nil
}

// clear removes the item from the db with the given key.
func (in *InMem) clear(key Key) error {
	in.Lock()
	defer in.Unlock()
	return in.clearLocked(key)
}

// clearLocked assumes mutex is already held by caller. See clear().
func (in *InMem) clearLocked(key Key) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	// Note: this is approximate. There is likely something missing.
	// The storage/in_mem_test.go benchmarks this and the measurement
	// being made seems close enough for government work (tm).
	if val := in.data.Get(KeyValue{Key: key}); val != nil {
		in.usedBytes -= computeSize(val.(KeyValue))
	}
	in.data.Delete(KeyValue{Key: key})
	return nil
}

// writeBatch atomically applies the specified writes, merges and
// deletions by holding the mutex. The list must only contain
// elements of type Batch{Put,Merge,Delete}.
func (in *InMem) writeBatch(cmds []interface{}) error {
	if len(cmds) == 0 {
		return nil
	}
	in.Lock()
	defer in.Unlock()
	// TODO(Tobias): pre-execution loop to check that we will
	// not run out of space. Make sure merge() bookkeeps correctly.
	for i, e := range cmds {
		switch v := e.(type) {
		case BatchDelete:
			if err := in.clearLocked(Key(v)); err != nil {
				return err
			}
		case BatchPut:
			if err := in.putLocked(v.Key, v.Value); err != nil {
				return err
			}
		case BatchMerge:
			if err := in.mergeLocked(v.Key, v.Value); err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("illegal operation #%d passed to writeBatch: %v", i, reflect.TypeOf(v)))
		}
	}
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
	}, nil
}
