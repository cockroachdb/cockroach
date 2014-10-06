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
	snapshots map[string]llrb.Tree
}

// NewInMem allocates and returns a new InMem object.
func NewInMem(attrs proto.Attributes, maxBytes int64) *InMem {
	return &InMem{
		snapshots: map[string]llrb.Tree{},
		attrs:     attrs,
		maxBytes:  maxBytes,
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

// CreateSnapshot creates a snapshot handle from engine.
func (in *InMem) CreateSnapshot(snapshotID string) error {
	_, ok := in.snapshots[snapshotID]
	if ok {
		return util.Errorf("snapshotID %s already exists", snapshotID)
	}
	snapshotHandle := cloneTree(in.data)
	in.snapshots[snapshotID] = snapshotHandle
	return nil
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

// ReleaseSnapshot releases the existing snapshot handle for the
// given snapshotID.
func (in *InMem) ReleaseSnapshot(snapshotID string) error {
	_, ok := in.snapshots[snapshotID]
	if !ok {
		return util.Errorf("snapshotID %s does not exist", snapshotID)
	}
	delete(in.snapshots, snapshotID)
	return nil
}

// Attrs returns the list of attributes describing this engine. This
// includes the disk type (always "mem") and potentially other labels
// to identify important attributes of the engine.
func (in *InMem) Attrs() proto.Attributes {
	return in.attrs
}

// Put sets the given key to the value provided.
func (in *InMem) Put(key Key, value []byte) error {
	in.Lock()
	defer in.Unlock()
	return in.putLocked(key, value)
}

// putLocked assumes mutex is already held by caller. See Put().
func (in *InMem) putLocked(key Key, value []byte) error {
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
func (in *InMem) Merge(key Key, value []byte) error {
	in.Lock()
	defer in.Unlock()
	return in.mergeLocked(key, value)
}

// mergeLocked assumes the mutex is already held by the caller. See merge().
func (in *InMem) mergeLocked(key Key, value []byte) error {
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
func (in *InMem) Get(key Key) ([]byte, error) {
	in.RLock()
	defer in.RUnlock()
	return in.getLocked(key, in.data)
}

// GetSnapshot returns the value for the given key from the given
// snapshotID, nil otherwise.
func (in *InMem) GetSnapshot(key Key, snapshotID string) ([]byte, error) {
	snapshotHandle, ok := in.snapshots[snapshotID]
	if !ok {
		return nil, util.Errorf("snapshotID %s does not exist", snapshotID)
	}
	return in.getLocked(key, snapshotHandle)
}

// getLocked performs a get operation assuming that the caller
// is already holding the mutex.
func (in *InMem) getLocked(key Key, data llrb.Tree) ([]byte, error) {
	if len(key) == 0 {
		return nil, emptyKeyError()
	}

	val := data.Get(proto.RawKeyValue{Key: key})
	if val == nil {
		return nil, nil
	}
	return val.(proto.RawKeyValue).Value, nil
}

// Scan returns up to max key/value objects starting from
// start (inclusive) and ending at end (non-inclusive).
func (in *InMem) Scan(start, end Key, max int64) ([]proto.RawKeyValue, error) {
	in.RLock()
	defer in.RUnlock()
	return in.scanLocked(start, end, max, in.data)
}

// ScanSnapshot returns up to max key/value objects starting from
// start (inclusive) and ending at end (non-inclusive) from the
// given snapshotID.
// Specify max=0 for unbounded scans.
func (in *InMem) ScanSnapshot(start, end Key, max int64, snapshotID string) ([]proto.RawKeyValue, error) {
	snapshotHandle, ok := in.snapshots[snapshotID]
	if !ok {
		return nil, util.Errorf("snapshotID %s does not exist", snapshotID)
	}
	return in.scanLocked(start, end, max, snapshotHandle)
}

// scanLocked is intended to be called within at least a read lock.
func (in *InMem) scanLocked(start, end Key, max int64, data llrb.Tree) ([]proto.RawKeyValue, error) {
	var scanned []proto.RawKeyValue
	if bytes.Compare(start, end) >= 0 {
		return scanned, nil
	}
	data.DoRange(func(kv llrb.Comparable) (done bool) {
		if max != 0 && int64(len(scanned)) >= max {
			done = true
			return
		}
		scanned = append(scanned, kv.(proto.RawKeyValue))
		return
	}, proto.RawKeyValue{Key: start}, proto.RawKeyValue{Key: end})

	return scanned, nil
}

// Clear removes the item from the db with the given key.
func (in *InMem) Clear(key Key) error {
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
func (in *InMem) SetGCTimeouts(gcTimeouts func() (minTxnTS, minRCacheTS int64)) {}

// ApproximateSize computes the size of data used to store all keys in the given
// key range.
func (in *InMem) ApproximateSize(start, end Key) (uint64, error) {
	var size uint64
	in.RLock()
	defer in.RUnlock()
	in.data.DoRange(func(node llrb.Comparable) bool {
		size += uint64(computeKeyValueSize(node.(proto.RawKeyValue)))
		return false
	}, proto.RawKeyValue{Key: start}, proto.RawKeyValue{Key: end})
	return size, nil
}
