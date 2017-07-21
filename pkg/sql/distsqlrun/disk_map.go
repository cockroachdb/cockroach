// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// Author: Alfonso Subiotto Marqués (alfonso@cockroachlabs.com)

package distsqlrun

import (
	"bytes"
	"math"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// SortedDiskMapIterator is a simple iterator used to iterate over keys and/or
// values.
// Example use of iterating over all keys:
// 	var i SortedDiskMapIterator
// 	for i.Rewind(); ; i.Next() {
// 		if ok, err := i.Valid(); err != nil {
//			// Handle error.
// 		} else if !ok {
//			break
// 		}
// 		key := i.Key()
//		// Do something.
// 	}
type SortedDiskMapIterator interface {
	// Seek sets the iterator's position to the first key greater than or equal
	// to the provided key.
	Seek(key []byte)
	// Rewind seeks to the start key.
	Rewind()
	// Valid must be called after any call to Seek(), Rewind(), or Next(). It
	// returns (true, nil) if the iterator points to a valid key and
	// (false, nil) if the iterator has moved past the end of the valid range.
	// If an error has occurred, the returned bool is invalid.
	Valid() (bool, error)
	// Next advances the iterator to the next key in the iteration.
	Next()
	// Key returns the current key. The resulting byte slice is still valid
	// after the next call to Seek(), Rewind(), or Next().
	Key() []byte
	// Value returns the current value. The resulting byte slice is still valid
	// after the next call to Seek(), Rewind(), or Next().
	Value() []byte

	// Close frees up resources held by the iterator.
	Close()
}

// SortedDiskMapBatchWriter batches writes to a SortedDiskMap.
type SortedDiskMapBatchWriter interface {
	// Put writes the given key/value pair to the batch. The write to the
	// underlying store happens on Flush(), Close(), or when the batch writer
	// reaches its capacity.
	Put(k []byte, v []byte) error
	// Flush flushes all writes to the underlying store. The batch can be reused
	// after a call to Flush().
	Flush() error

	// Close flushes all writes to the underlying store and frees up resources
	// held by the batch writer.
	Close(context.Context) error
}

// SortedDiskMap is an on-disk map. Keys are iterated over in sorted order.
type SortedDiskMap interface {
	// Put writes the given key/value pair.
	Put(k []byte, v []byte) error
	// Get reads the value for the given key.
	Get(k []byte) ([]byte, error)

	// NewIterator returns a SortedDiskMapIterator that can be used to iterate
	// over key/value pairs in sorted order.
	NewIterator() SortedDiskMapIterator
	// NewBatchWriter returns a SortedDiskMapBatchWriter that can be used to
	// batch writes to this map for performance improvements.
	NewBatchWriter() SortedDiskMapBatchWriter
	// NewBatchWriterCapacity is identical to NewBatchWriter, but overrides the
	// SortedDiskMapBatchWriter's default capacity with capacityBytes.
	NewBatchWriterCapacity(capacityBytes int) SortedDiskMapBatchWriter

	// Close frees up resources held by the map.
	Close(context.Context)
}

// defaultBatchCapacityBytes is the default capacity for a
// SortedDiskMapBatchWriter.
const defaultBatchCapacityBytes = 4096

// RocksDBMapBatchWriter batches writes to a RocksDBMap.
type RocksDBMapBatchWriter struct {
	// capacity is the number of bytes to write before a Flush() is triggered.
	capacity int

	// makeKey is a function that transforms a key into an MVCCKey with a prefix
	// to be written to the underlying store.
	makeKey func(k []byte) engine.MVCCKey
	batch   engine.Batch
	store   engine.Engine
}

// RocksDBMapIterator iterates over the keys of a RocksDBMap in sorted order.
type RocksDBMapIterator struct {
	iter engine.Iterator
	// makeKey is a function that transforms a key into an MVCCKey with a prefix
	// used to Seek() the underlying iterator.
	makeKey func(k []byte) engine.MVCCKey
	// prefix is the prefix of keys that this iterator iterates over.
	prefix []byte
}

// RocksDBMap is a SortedDiskMap that uses RocksDB as its underlying storage
// engine.
type RocksDBMap struct {
	// TODO(asubiotto): Add memory accounting.
	prefix []byte
	store  engine.Engine
}

var _ SortedDiskMapBatchWriter = &RocksDBMapBatchWriter{}
var _ SortedDiskMapIterator = &RocksDBMapIterator{}
var _ SortedDiskMap = &RocksDBMap{}

// NewRocksDBMap creates a new RocksDBMap with the passed in engine.Engine as
// the underlying store. The RocksDBMap instance will have a keyspace prefixed
// by prefix.
func NewRocksDBMap(prefix uint64, e engine.Engine) (*RocksDBMap, error) {
	// When we close this instance, we also delete the associated keyspace. If
	// we accepted math.MaxUint64 as a prefix, our prefixBytes would be
	// []byte{0xff, ..., 0xff} for which there is no end key, thus deleting
	// nothing.
	if prefix == math.MaxUint64 {
		return nil, errors.New("invalid prefix")
	}

	return &RocksDBMap{prefix: encoding.EncodeUvarintAscending([]byte(nil), prefix), store: e}, nil
}

// makeKey appends k to the RocksDBMap's prefix to keep the key local to this
// instance and creates an MVCCKey, which is what the underlying storage engine
// expects. The returned key is only valid until the next call to makeKey().
func (r *RocksDBMap) makeKey(k []byte) engine.MVCCKey {
	// TODO(asubiotto): We can make this more performant by bypassing MVCCKey
	// creation (have to generalize storage API). See
	// https://github.com/cockroachdb/cockroach/issues/16718#issuecomment-311493414
	prefixLen := len(r.prefix)
	r.prefix = append(r.prefix, k...)
	mvccKey := engine.MVCCKey{Key: r.prefix}
	r.prefix = r.prefix[:prefixLen]
	return mvccKey
}

// Put implements the SortedDiskMap interface.
func (r *RocksDBMap) Put(k []byte, v []byte) error {
	return r.store.Put(r.makeKey(k), v)
}

// Get implements the SortedDiskMap interface.
func (r *RocksDBMap) Get(k []byte) ([]byte, error) {
	return r.store.Get(r.makeKey(k))
}

// NewIterator implements the SortedDiskMap interface.
func (r *RocksDBMap) NewIterator() SortedDiskMapIterator {
	// NOTE: prefix is only false because we can't use the normal prefix
	// extractor. This iterator still only does prefix iteration. See
	// RocksDBMapIterator.Valid().
	return &RocksDBMapIterator{iter: r.store.NewIterator(false /* prefix */), makeKey: r.makeKey, prefix: r.prefix}
}

// NewBatchWriter implements the SortedDiskMap interface.
func (r *RocksDBMap) NewBatchWriter() SortedDiskMapBatchWriter {
	return r.NewBatchWriterCapacity(defaultBatchCapacityBytes)
}

// NewBatchWriterCapacity implements the SortedDiskMap interface.
func (r *RocksDBMap) NewBatchWriterCapacity(capacityBytes int) SortedDiskMapBatchWriter {
	return &RocksDBMapBatchWriter{
		capacity: capacityBytes,
		makeKey:  r.makeKey,
		batch:    r.store.NewWriteOnlyBatch(),
		store:    r.store,
	}
}

// Close implements the SortedDiskMap interface.
func (r *RocksDBMap) Close(ctx context.Context) {
	if err := r.store.ClearRange(
		engine.MVCCKey{Key: r.prefix},
		engine.MVCCKey{Key: roachpb.Key(r.prefix).PrefixEnd()},
	); err != nil {
		log.Error(ctx, errors.Wrapf(err, "unable to clear range with prefix %v", r.prefix))
	}
}

// Seek implements the SortedDiskMapIterator interface.
func (i *RocksDBMapIterator) Seek(k []byte) {
	i.iter.Seek(i.makeKey(k))
}

// Rewind implements the SortedDiskMapIterator interface.
func (i *RocksDBMapIterator) Rewind() {
	i.iter.Seek(i.makeKey(nil))
}

// Valid implements the SortedDiskMapIterator interface.
func (i *RocksDBMapIterator) Valid() (bool, error) {
	ok, err := i.iter.Valid()
	if err != nil {
		return false, err
	}
	if ok && !bytes.HasPrefix(i.iter.UnsafeKey().Key, i.prefix) {
		return false, nil
	}

	return ok, nil
}

// Next implements the SortedDiskMapIterator interface.
func (i *RocksDBMapIterator) Next() {
	i.iter.Next()
}

// Key implements the SortedDiskMapIterator interface.
func (i *RocksDBMapIterator) Key() []byte {
	return i.iter.Key().Key[len(i.prefix):]
}

// Value implements the SortedDiskMapIterator interface.
func (i *RocksDBMapIterator) Value() []byte {
	return i.iter.Value()
}

// Close implements the SortedDiskMapIterator interface.
func (i *RocksDBMapIterator) Close() {
	i.iter.Close()
}

// Put implements the SortedDiskMapBatchWriter interface.
func (b *RocksDBMapBatchWriter) Put(k []byte, v []byte) error {
	if err := b.batch.Put(b.makeKey(k), v); err != nil {
		return err
	}
	if len(b.batch.Repr()) >= b.capacity {
		return b.Flush()
	}
	return nil
}

// Flush implements the SortedDiskMapBatchWriter interface.
func (b *RocksDBMapBatchWriter) Flush() error {
	if len(b.batch.Repr()) < 1 {
		return nil
	}
	if err := b.batch.Commit(false /* syncCommit */); err != nil {
		return err
	}
	b.batch = b.store.NewWriteOnlyBatch()
	return nil
}

// Close implements the SortedDiskMapBatchWriter interface.
func (b *RocksDBMapBatchWriter) Close(ctx context.Context) error {
	err := b.Flush()
	b.batch.Close()
	return err
}
