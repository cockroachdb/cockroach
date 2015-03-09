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
// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package engine

import (
	"bytes"
	"fmt"
	"sync"
	"unsafe"

	"code.google.com/p/biogo.store/llrb"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
)

// TODO(petermattis): Remove this file.

var (
	llrbNodeSize = int64(unsafe.Sizeof(llrb.Node{}))
	keyValueSize = int64(unsafe.Sizeof(proto.RawKeyValue{}))
)

// computeSize returns the approximate size in bytes that the keyVal
// object took while stored in the underlying LLRB.
func computeSize(kv proto.RawKeyValue) int64 {
	return computeKeyValueSize(kv) + llrbNodeSize + keyValueSize
}

// computeKeyValueSize returns the approximate size in bytes that the key and
// value take, excluding the overhead of the underlying LLRB.
func computeKeyValueSize(kv proto.RawKeyValue) int64 {
	return int64(len(kv.Key)) + int64(len(kv.Value))
}

// InMem a simple, in-memory key-value store.
type InMem struct {
	sync.RWMutex
	attrs     proto.Attributes
	maxBytes  int64
	usedBytes int64
	data      llrb.Tree
}

// NewInMem allocates and returns a new InMem object.
func NewInMem(attrs proto.Attributes, maxBytes int64) *InMem {
	return &InMem{
		attrs:    attrs,
		maxBytes: maxBytes,
	}
}

// String formatter.
func (in *InMem) String() string {
	return fmt.Sprintf("%s=%d", in.attrs, in.maxBytes)
}

// Start is a noop for the InMem engine.
func (in *InMem) Start() error {
	return nil
}

// Stop is a noop for the InMem engine.
func (in *InMem) Stop() {
}

// Attrs returns the list of attributes describing this engine. This
// includes the disk type (always "mem") and potentially other labels
// to identify important attributes of the engine.
func (in *InMem) Attrs() proto.Attributes {
	return in.attrs
}

// Put sets the given key to the value provided.
func (in *InMem) Put(key proto.EncodedKey, value []byte) error {
	in.Lock()
	defer in.Unlock()
	return in.putLocked(key, value)
}

// putLocked assumes mutex is already held by caller. See Put().
func (in *InMem) putLocked(key proto.EncodedKey, value []byte) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	kv := proto.RawKeyValue{Key: key, Value: value}
	size := computeSize(kv)
	// If the key already exists, compute the size change of the
	// replacement with the new value.
	if val := in.data.Get(proto.RawKeyValue{Key: key}); val != nil {
		size -= computeSize(val.(proto.RawKeyValue))
	}

	if size > in.maxBytes-in.usedBytes {
		return util.Errorf("in mem store at capacity %d + %d > %d", in.usedBytes, size, in.maxBytes)
	}
	in.usedBytes += size
	in.data.Insert(kv)
	return nil
}

// Merge implements a merge operation which updates the existing value stored
// under key based on the value passed.
// See the documentation of goMerge and goMergeInit for details.
func (in *InMem) Merge(key proto.EncodedKey, value []byte) error {
	in.Lock()
	defer in.Unlock()
	return in.mergeLocked(key, value)
}

// mergeLocked assumes the mutex is already held by the caller. See merge().
func (in *InMem) mergeLocked(key proto.EncodedKey, value []byte) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	existingVal, err := in.getLocked(key, in.data)
	if err != nil {
		return err
	}
	// Emulate RocksDB errors by... not having errors.
	newValue, _ := goMerge(existingVal, value)
	return in.putLocked(key, newValue)
}

// Get returns the value for the given key, nil otherwise.
func (in *InMem) Get(key proto.EncodedKey) ([]byte, error) {
	in.RLock()
	defer in.RUnlock()
	return in.getLocked(key, in.data)
}

// getLocked performs a get operation assuming that the caller
// is already holding the mutex.
func (in *InMem) getLocked(key proto.EncodedKey, data llrb.Tree) ([]byte, error) {
	if len(key) == 0 {
		return nil, emptyKeyError()
	}

	val := data.Get(proto.RawKeyValue{Key: key})
	if val == nil {
		return nil, nil
	}
	return val.(proto.RawKeyValue).Value, nil
}

// Iterate iterates from start to end keys, invoking f on each
// key/value pair. See engine.Iterate for details.
func (in *InMem) Iterate(start, end proto.EncodedKey, f func(proto.RawKeyValue) (bool, error)) error {
	in.RLock()
	defer in.RUnlock()
	return in.iterateLocked(start, end, f, in.data)
}

func (in *InMem) iterateLocked(start, end proto.EncodedKey, f func(proto.RawKeyValue) (bool, error), data llrb.Tree) error {
	if bytes.Compare(start, end) >= 0 {
		return nil
	}
	var err error
	data.DoRange(func(kv llrb.Comparable) (done bool) {
		if done, err = f(kv.(proto.RawKeyValue)); done || err != nil {
			done = true
			return
		}
		return
	}, proto.RawKeyValue{Key: start}, proto.RawKeyValue{Key: end})

	return err
}

// Clear removes the item from the db with the given key.
func (in *InMem) Clear(key proto.EncodedKey) error {
	in.Lock()
	defer in.Unlock()
	return in.clearLocked(key)
}

// clearLocked assumes mutex is already held by caller. See clear().
func (in *InMem) clearLocked(key proto.EncodedKey) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	// Note: this is approximate. There is likely something missing.
	// The storage/in_mem_test.go benchmarks this and the measurement
	// being made seems close enough for government work (tm).
	if val := in.data.Get(proto.RawKeyValue{Key: key}); val != nil {
		in.usedBytes -= computeSize(val.(proto.RawKeyValue))
	}
	in.data.Delete(proto.RawKeyValue{Key: key})
	return nil
}

// WriteBatch atomically applies the specified writes, merges and
// deletions by holding the mutex. The list must only contain
// elements of type Batch{Put,Merge,Delete}.
func (in *InMem) WriteBatch(cmds []interface{}) error {
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
			if err := in.clearLocked(v.Key); err != nil {
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
			panic(fmt.Sprintf("illegal operation #%d passed to writeBatch: %T", i, v))
		}
	}
	return nil
}

// Capacity formulates available space based on cache size and
// computed size of cached keys and values. The actual free space may
// not be entirely accurate due to object storage costs and other
// internal glue.
func (in *InMem) Capacity() (StoreCapacity, error) {
	return StoreCapacity{
		Capacity:  in.maxBytes,
		Available: in.maxBytes - in.usedBytes,
	}, nil
}

// SetGCTimeouts is a noop for the InMem engine.
func (in *InMem) SetGCTimeouts(minTxnTS, minRCacheTS int64) {}

// ApproximateSize computes the size of data used to store all keys in the given
// key range.
func (in *InMem) ApproximateSize(start, end proto.EncodedKey) (uint64, error) {
	var size uint64
	in.RLock()
	defer in.RUnlock()
	in.data.DoRange(func(node llrb.Comparable) bool {
		size += uint64(computeKeyValueSize(node.(proto.RawKeyValue)))
		return false
	}, proto.RawKeyValue{Key: start}, proto.RawKeyValue{Key: end})
	return size, nil
}

// NewIterator returns an iterator over this in-memory engine.
func (in *InMem) NewIterator() Iterator {
	in.RLock()
	defer in.RUnlock()
	return &inMemIterator{
		data: in.data,
		mu:   &in.RWMutex,
	}
}

// NewSnapshot creates a read-only snapshot engine from this engine.
func (in *InMem) NewSnapshot() Engine {
	in.Lock()
	defer in.Unlock()
	return &inMemSnapshot{
		InMem: InMem{
			attrs:     in.attrs,
			maxBytes:  in.maxBytes,
			usedBytes: in.usedBytes,
			data:      cloneTree(in.data),
		},
	}
}

func cloneTree(a llrb.Tree) llrb.Tree {
	var newTree = llrb.Tree{Count: a.Count}
	newTree.Root = cloneNode(a.Root)
	return newTree
}

func cloneNode(a *llrb.Node) *llrb.Node {
	if a == nil {
		return nil
	}
	var newNode = &llrb.Node{Elem: a.Elem, Color: a.Color}
	newNode.Left = cloneNode(a.Left)
	newNode.Right = cloneNode(a.Right)
	return newNode
}

// NewBatch returns a new Batch wrapping this in-memory engine.
func (in *InMem) NewBatch() Engine {
	return &Batch{engine: in}
}

// Commit is a noop for in-memory engine.
func (in *InMem) Commit() error {
	return nil
}

// This implementation restricts operation of the underlying in memory
// engine to read-only commands.
type inMemSnapshot struct {
	InMem
}

// Put is illegal for snapshot and returns an error.
func (in *inMemSnapshot) Put(key proto.EncodedKey, value []byte) error {
	return util.Errorf("cannot Put to a snapshot")
}

// Clear is illegal for snapshot and returns an error.
func (in *inMemSnapshot) Clear(key proto.EncodedKey) error {
	return util.Errorf("cannot Clear from a snapshot")
}

// WriteBatch is illegal for snapshot and returns an error.
func (in *inMemSnapshot) WriteBatch([]interface{}) error {
	return util.Errorf("cannot WriteBatch to a snapshot")
}

// Merge is illegal for snapshot and returns an error.
func (in *inMemSnapshot) Merge(key proto.EncodedKey, value []byte) error {
	return util.Errorf("cannot Merge to a snapshot")
}

// SetGCTimeouts is a noop for a snapshot.
func (in *inMemSnapshot) SetGCTimeouts(minTxnTS, minRCacheTS int64) {
}

// NewSnapshot is illegal for snapshot and returns nil.
func (in *inMemSnapshot) NewSnapshot() Engine {
	panic("cannot create a NewSnapshot from a snapshot")
}

// NewBatch is illegal for snapshot and returns nil.
func (in *inMemSnapshot) NewBatch() Engine {
	panic("cannot create a NewBatch from a snapshot")
}

// Commit is illegal for snapshot and returns an error.
func (in *inMemSnapshot) Commit() error {
	return util.Errorf("cannot Commit to a snapshot")
}

// This implementation is not very efficient because the biogo LLRB
// API supports iterations, not iterators. Every call to Next() is
// O(logN). But since the in-memory engine is really only good for
// unittesting, this is not worth fixing.
type inMemIterator struct {
	data llrb.Tree
	mu   *sync.RWMutex
	cur  *proto.RawKeyValue
	err  error
}

// The following methods implement the Iterator interface.
func (in *inMemIterator) Close() {
}

func (in *inMemIterator) Seek(key []byte) {
	in.cur = nil
	in.err = nil
	if len(key) == 0 {
		key = KeyMin
	}
	in.mu.RLock()
	defer in.mu.RUnlock()

	in.data.DoRange(func(c llrb.Comparable) (done bool) {
		kv := c.(proto.RawKeyValue)
		in.cur = &kv
		return true
	}, proto.RawKeyValue{Key: key}, proto.RawKeyValue{Key: proto.EncodedKey(MVCCKeyMax)})
}

func (in *inMemIterator) Valid() bool {
	return in.err == nil && in.cur != nil
}

func (in *inMemIterator) Next() {
	if !in.Valid() {
		in.err = util.Errorf("next called with invalid iterator")
		return
	}
	start := in.cur.Key.Next()
	in.cur = nil
	in.mu.RLock()
	defer in.mu.RUnlock()
	in.data.DoRange(func(c llrb.Comparable) (done bool) {
		kv := c.(proto.RawKeyValue)
		in.cur = &kv
		return true
	}, proto.RawKeyValue{Key: start}, proto.RawKeyValue{Key: proto.EncodedKey(MVCCKeyMax)})
}

func (in *inMemIterator) Key() proto.EncodedKey {
	if !in.Valid() {
		in.err = util.Errorf("invalid iterator")
		return nil
	}
	return in.cur.Key
}

func (in *inMemIterator) Value() []byte {
	if !in.Valid() {
		in.err = util.Errorf("invalid iterator")
		return nil
	}
	return in.cur.Value
}

func (in *inMemIterator) Error() error {
	return in.err
}
