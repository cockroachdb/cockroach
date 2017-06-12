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
	"encoding/binary"
	"math"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// SortedDiskMapIterator is a simple iterator used to iterate over keys and/or
// values.
// Example use of iterating over all keys:
// 	for SortedDiskMapIterator.Rewind(); SortedDiskMapIterator.Valid(); SortedDiskMapIterator.Next() {
// 		key := SortedDiskMapIterator.Key()
//		// Do something.
// 	}
type SortedDiskMapIterator interface {
	Seek(key []byte)
	// Rewind seeks to the start key.
	Rewind()
	Valid() bool
	Next()
	Key() []byte
	Value() []byte

	Close()
}

// SortedDiskMapWriteBatch batches writes to a SortedDiskMap.
type SortedDiskMapWriteBatch interface {
	Put(k []byte, v []byte) error
	// Flush flushes all writes to the underlying store. The batch can be
	// reused after a call to Flush().
	Flush() error

	Close()
}

// SortedDiskMap is an on-disk map. Keys are iterated over in sorted order.
type SortedDiskMap interface {
	Put(k []byte, v []byte) error
	Get(k []byte) ([]byte, error)

	NewIterator() SortedDiskMapIterator
	NewWriteBatch() SortedDiskMapWriteBatch
	NewWriteBatchSize(size int) SortedDiskMapWriteBatch

	Close()
}

const defaultBatchSize = 4096

type RocksDBMapWriteBatch struct {
	// capacity is the number of bytes to write before a Flush() is triggered.
	capacity         int
	numBytesBuffered int

	makeKey func(k []byte) engine.MVCCKey
	batch   engine.Batch
	store   engine.Engine
}

type RocksDBMapIterator struct {
	iter    engine.Iterator
	makeKey func(k []byte) engine.MVCCKey
	// NOTE: This will not be a member once we take care of our prefix
	// iteration. See RocksDBMapIterator.Valid().
	prefix []byte
}

type RocksDBMap struct {
	// TODO(asubiotto): Add memory accounting.
	prefix []byte
	store  engine.Engine
}

var _ SortedDiskMapWriteBatch = &RocksDBMapWriteBatch{}
var _ SortedDiskMapIterator = RocksDBMapIterator{}
var _ SortedDiskMap = RocksDBMap{}

// NewRocksDBMap creates a new RocksDBMap with the passed in engine.RocksDB as
// the underlying storage engine. The RocksDBMap instance will have its own
// keyspace.
func NewRocksDBMap(prefix uint64, e engine.Engine) (RocksDBMap, error) {
	// When we close this instance, we also delete the associated keyspace. If
	// we accepted math.MaxUint64 as a prefix, our prefixBytes would be
	// []byte{0xff, ..., 0xff} for which there is no end key, thus deleting
	// nothing.
	if prefix == math.MaxUint64 {
		return RocksDBMap{}, errors.New("invalid prefix")
	}

	prefixBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(prefixBytes, prefix)
	return RocksDBMap{prefix: prefixBytes, store: e}, nil
}

// makeKey appends k to the RocksDBMap's prefix to keep the key local to this
// instance and creates an MVCCKey, which is what the underlying storage engine
// expects.
func (r RocksDBMap) makeKey(k []byte) engine.MVCCKey {
	// TODO(asubiotto): We can make this more performant by bypassing MVCCKey
	// creation (have to generalize storage API) and there probably is
	// something to do to with regards to appending the given key to the prefix.
	return engine.MVCCKey{Key: append(r.prefix, k...)}
}

func (r RocksDBMap) Put(k []byte, v []byte) error {
	return r.store.Put(r.makeKey(k), v)
}

func (r RocksDBMap) Get(k []byte) ([]byte, error) {
	return r.store.Get(r.makeKey(k))
}

func (r RocksDBMap) NewIterator() SortedDiskMapIterator {
	// NOTE: prefix is only false because we can't use the normal prefix
	// extractor. This iterator still only does prefix iteration. See
	// RocksDBMapIterator.Valid().
	return RocksDBMapIterator{iter: r.store.NewIterator(false /* prefix */), makeKey: r.makeKey, prefix: r.prefix}
}

func (r RocksDBMap) NewWriteBatch() SortedDiskMapWriteBatch {
	return r.NewWriteBatchSize(defaultBatchSize)
}

func (r RocksDBMap) NewWriteBatchSize(size int) SortedDiskMapWriteBatch {
	return &RocksDBMapWriteBatch{capacity: size, makeKey: r.makeKey, batch: r.store.NewWriteOnlyBatch(), store: r.store}
}

func (r RocksDBMap) Close() {
	if err := r.store.ClearRange(
		engine.MVCCKey{Key: r.prefix},
		engine.MVCCKey{Key: roachpb.Key(r.prefix).PrefixEnd()},
	); err != nil {
		log.Error(context.TODO(), errors.Wrapf(err, "unable to clear range with prefix %v", r.prefix))
	}
}

func (i RocksDBMapIterator) Seek(k []byte) {
	i.iter.Seek(i.makeKey(k))
}

func (i RocksDBMapIterator) Rewind() {
	i.iter.Seek(i.makeKey(nil))
}

func (i RocksDBMapIterator) Valid() bool {
	ok, err := i.iter.Valid()
	// TODO(asubiotto): Another way is to write our own prefix extractor.
	// See pkg/storage/engine/db.cc::DBPrefixExtractor and
	// https://github.com/facebook/rocksdb/wiki/Prefix-Seek-API-Changes for
	// reference. Would it be better?
	if ok && !bytes.HasPrefix(i.iter.Key().Key, i.prefix) {
		return false
	}

	// TODO(asubiotto): When does this return errors? From what I gather from
	// the underlying iterator, these errors would be in relation to seeking
	// or calling Next/Prev.
	if err != nil {
		panic(err)
	}
	return ok
}

func (i RocksDBMapIterator) Next() {
	i.iter.Next()
}

func (i RocksDBMapIterator) Key() []byte {
	return i.iter.Key().Key[len(i.prefix):]
}

func (i RocksDBMapIterator) Value() []byte {
	return i.iter.Value()
}

func (i RocksDBMapIterator) Close() {
	i.iter.Close()
}

func (b *RocksDBMapWriteBatch) Put(k []byte, v []byte) error {
	if err := b.batch.Put(b.makeKey(k), v); err != nil {
		return err
	}
	if b.numBytesBuffered += len(k) + len(v); b.numBytesBuffered >= b.capacity {
		return b.Flush()
	}
	return nil
}

func (b *RocksDBMapWriteBatch) Flush() error {
	if b.numBytesBuffered < 1 {
		return nil
	}
	if err := b.batch.Commit(false /* syncCommit */); err != nil {
		return err
	}
	b.batch = b.store.NewWriteOnlyBatch()
	b.numBytesBuffered = 0
	return nil
}

func (b *RocksDBMapWriteBatch) Close() {
	if err := b.Flush(); err != nil {
		log.Fatal(context.TODO(), errors.Wrapf(err, "map write batch could not be flushed"))
	}
	b.batch.Close()
}
