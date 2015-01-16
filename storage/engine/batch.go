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

package engine

import (
	"bytes"
	"runtime/debug"

	"code.google.com/p/biogo.store/llrb"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
)

// Batch wrap an instance of Engine and provides a limited subset of
// Engine functionality. Mutations are added to a write batch
// transparently and only applied to the wrapped engine on invocation
// of Commit(). Reads are passed through to the wrapped engine. In the
// event that reads access keys for which there are already-batched
// updates, reads from the wrapped engine are combined on the fly with
// pending write, delete, and merge updates.
//
// This struct is not thread safe.
type Batch struct {
	engine    Engine
	updates   llrb.Tree
	committed bool
}

// NewBatch returns a new instance of Batch which wraps engine.
func NewBatch(engine Engine) *Batch {
	return &Batch{engine: engine}
}

// Put stores the key / value as a BatchPut in the updates tree.
func (b *Batch) Put(key proto.EncodedKey, value []byte) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	b.updates.Insert(BatchPut{proto.RawKeyValue{Key: key, Value: value}})
	return nil
}

// Get reads first from the updates tree. If the key is found there
// and is deleted, then a nil value is returned. If the key is found,
// and is a Put, returns the value from the tree. If a merge, then
// merge is performed on the fly to combine with the value from the
// underlying engine. Otherwise, the Get is simply passed through to
// the wrapped engine.
func (b *Batch) Get(key proto.EncodedKey) ([]byte, error) {
	if len(key) == 0 {
		return nil, emptyKeyError()
	}
	val := b.updates.Get(proto.RawKeyValue{Key: key})
	if val != nil {
		switch t := val.(type) {
		case BatchDelete:
			return nil, nil
		case BatchPut:
			return t.Value, nil
		case BatchMerge:
			existingVal, err := b.engine.Get(key)
			if err != nil {
				return nil, err
			}
			return goMerge(existingVal, t.Value)
		}
	}
	return b.engine.Get(key)
}

// Iterate invokes f on key/value pairs merged from the underlying
// engine and pending batch updates. If f returns done or an error,
// the iteration ends and propagates the error.
func (b *Batch) Iterate(start, end proto.EncodedKey, f func(proto.RawKeyValue) (bool, error)) error {
	it := b.NewIterator()
	defer it.Close()

	it.Seek(start)
	for ; it.Valid(); it.Next() {
		k := it.Key()
		if bytes.Compare(it.Key(), end) >= 0 {
			break
		}
		if done, err := f(proto.RawKeyValue{Key: k, Value: it.Value()}); done || err != nil {
			return err
		}
	}
	// Check for any errors during iteration.
	return it.Error()
}

// Scan scans from both the updates tree and the underlying engine
// and combines the results, up to max.
func (b *Batch) Scan(start, end proto.EncodedKey, max int64) ([]proto.RawKeyValue, error) {
	var kvs []proto.RawKeyValue
	err := b.Iterate(start, end, func(kv proto.RawKeyValue) (bool, error) {
		if max != 0 && int64(len(kvs)) >= max {
			return true, nil
		}
		kvs = append(kvs, kv)
		return false, nil
	})
	return kvs, err
}

// Clear stores the key as a BatchDelete in the updates tree.
func (b *Batch) Clear(key proto.EncodedKey) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	b.updates.Insert(BatchDelete{proto.RawKeyValue{Key: key}})
	return nil
}

// Merge stores the key / value as a BatchMerge in the updates tree.
// If the updates map already contains a BatchPut, then this value is
// merged with the Put and kept as a BatchPut. If the updates map
// already contains a BatchMerge, then this value is merged with the
// existing BatchMerge and kept as a BatchMerge. If the updates map
// contains a BatchDelete, then this value is merged with a nil byte
// slice and stored as a BatchPut.
func (b *Batch) Merge(key proto.EncodedKey, value []byte) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	val := b.updates.Get(proto.RawKeyValue{Key: key})
	if val != nil {
		switch t := val.(type) {
		case BatchDelete:
			mergedBytes, err := goMerge(nil, value)
			if err != nil {
				return err
			}
			b.updates.Insert(BatchPut{proto.RawKeyValue{Key: key, Value: mergedBytes}})
		case BatchPut:
			mergedBytes, err := goMerge(t.Value, value)
			if err != nil {
				return err
			}
			b.updates.Insert(BatchPut{proto.RawKeyValue{Key: key, Value: mergedBytes}})
		case BatchMerge:
			mergedBytes, err := goMerge(t.Value, value)
			if err != nil {
				return err
			}
			b.updates.Insert(BatchMerge{proto.RawKeyValue{Key: key, Value: mergedBytes}})
		}
	} else {
		b.updates.Insert(BatchMerge{proto.RawKeyValue{Key: key, Value: value}})
	}
	return nil
}

// Commit writes all pending updates to the underlying engine in
// an atomic write batch.
func (b *Batch) Commit() error {
	if b.committed {
		panic("this batch was already committed")
	}
	var batch []interface{}
	b.updates.DoRange(func(n llrb.Comparable) (done bool) {
		batch = append(batch, n)
		return false
	}, proto.RawKeyValue{Key: proto.EncodedKey(KeyMin)}, proto.RawKeyValue{Key: proto.EncodedKey(KeyMax)})
	b.committed = true
	return b.engine.WriteBatch(batch)
}

// Start returns an error if called on a Batch.
func (b *Batch) Start() error {
	return util.Errorf("cannot start a batch")
}

// Stop is a noop for Batch.
func (b *Batch) Stop() {
}

// Attrs is a noop for Batch.
func (b *Batch) Attrs() proto.Attributes {
	return proto.Attributes{}
}

// WriteBatch returns an error if called on a Batch.
func (b *Batch) WriteBatch([]interface{}) error {
	return util.Errorf("cannot write batch from a Batch")
}

// Capacity returns an error if called on a Batch.
func (b *Batch) Capacity() (StoreCapacity, error) {
	return StoreCapacity{}, util.Errorf("cannot report capacity from a Batch")
}

// SetGCTimeouts is a noop for Batch.
func (b *Batch) SetGCTimeouts(minTxnTS, minRCacheTS int64) {
}

// ApproximateSize returns an error if called on a Batch.
func (b *Batch) ApproximateSize(start, end proto.EncodedKey) (uint64, error) {
	return 0, util.Errorf("cannot get approximate size from a Batch")
}

// NewIterator returns an iterator over Batch. Batch iterators are
// not thread safe.
func (b *Batch) NewIterator() Iterator {
	return newBatchIterator(b.engine, &b.updates)
}

// NewSnapshot returns nil if called on a Batch.
func (b *Batch) NewSnapshot() Engine {
	return nil
}

// NewBatch returns a new Batch instance wrapping same underlying engine.
func (b *Batch) NewBatch() Engine {
	return &Batch{engine: b.engine}
}

type batchIterator struct {
	iter    Iterator
	updates *llrb.Tree
	pending []proto.RawKeyValue
	err     error
}

// newBatchIterator returns a new iterator over the supplied Batch instance.
func newBatchIterator(engine Engine, updates *llrb.Tree) *batchIterator {
	return &batchIterator{
		iter:    engine.NewIterator(),
		updates: updates,
	}
}

// The following methods implement the Iterator interface.
func (bi *batchIterator) Close() {
	bi.iter.Close()
}

func (bi *batchIterator) Seek(key []byte) {
	bi.pending = []proto.RawKeyValue{}
	bi.err = nil
	bi.iter.Seek(key)
	bi.mergeUpdates(key)
}

func (bi *batchIterator) Valid() bool {
	return bi.err == nil && len(bi.pending) > 0
}

func (bi *batchIterator) Next() {
	if !bi.Valid() {
		bi.err = util.Errorf("next called with invalid iterator")
		return
	}
	last := bi.pending[0].Key.Next()
	if len(bi.pending) > 0 {
		bi.pending = bi.pending[1:]
	}
	if len(bi.pending) == 0 {
		bi.mergeUpdates(last)
	}
}

func (bi *batchIterator) Key() proto.EncodedKey {
	if !bi.Valid() {
		debug.PrintStack()
		bi.err = util.Errorf("access to invalid key")
		return nil
	}
	return bi.pending[0].Key
}

func (bi *batchIterator) Value() []byte {
	if !bi.Valid() {
		bi.err = util.Errorf("access to invalid value")
		return nil
	}
	return bi.pending[0].Value
}

func (bi *batchIterator) Error() error {
	return bi.err
}

// mergeUpdates combines the next key/value from the engine iterator
// with all batch updates which preceed it. The final batch update
// which might overlap the next key/value is merged. The start
// parameter indicates the first possible key to merge from either
// iterator.
func (bi *batchIterator) mergeUpdates(start proto.EncodedKey) {
	// Use a for-loop because deleted entries might cause nothing
	// to be added to bi.pending; in this case, we loop to next key.
	for len(bi.pending) == 0 && bi.iter.Valid() {
		kv := proto.RawKeyValue{Key: bi.iter.Key(), Value: bi.iter.Value()}
		bi.iter.Next()

		// Get updates up to the engine iterator's current key.
		bi.getUpdates(start, kv.Key)

		// Possibly merge an update with engine iterator's current key.
		if val := bi.updates.Get(kv); val != nil {
			switch t := val.(type) {
			case BatchDelete:
			case BatchPut:
				bi.pending = append(bi.pending, t.RawKeyValue)
			case BatchMerge:
				mergedKV := proto.RawKeyValue{Key: t.Key}
				mergedKV.Value, bi.err = goMerge(kv.Value, t.Value)
				if bi.err == nil {
					bi.pending = append(bi.pending, mergedKV)
				}
			}
		} else {
			bi.pending = append(bi.pending, kv)
		}
		start = kv.Key.Next()
	}

	if len(bi.pending) == 0 {
		bi.getUpdates(start, proto.EncodedKey(KeyMax))
	}
}

// getUpdates scans the updates tree from start to end, adding
// each value to bi.pending.
func (bi *batchIterator) getUpdates(start, end proto.EncodedKey) {
	// Scan the updates tree for the key range, merging as we go.
	bi.updates.DoRange(func(n llrb.Comparable) bool {
		switch t := n.(type) {
		case BatchDelete: // On delete, skip.
		case BatchPut: // On put, override the corresponding engine entry.
			bi.pending = append(bi.pending, t.RawKeyValue)
		case BatchMerge: // On merge, merge with empty value.
			kv := proto.RawKeyValue{Key: t.Key}
			kv.Value, bi.err = goMerge([]byte(nil), t.Value)
			if bi.err == nil {
				bi.pending = append(bi.pending, kv)
			}
		}
		return bi.err != nil
	}, proto.RawKeyValue{Key: start}, proto.RawKeyValue{Key: end})
}
