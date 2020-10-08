// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/diskmap"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// defaultBatchCapacityBytes is the default capacity for a
// SortedDiskMapBatchWriter.
const defaultBatchCapacityBytes = 4096

// rocksDBMapBatchWriter batches writes to a RocksDBMap.
type rocksDBMapBatchWriter struct {
	// capacity is the number of bytes to write before a Flush() is triggered.
	capacity int

	// makeKey is a function that transforms a key into an MVCCKey with a prefix
	// to be written to the underlying store.
	makeKey           func(k []byte) MVCCKey
	batch             Batch
	numPutsSinceFlush int
	store             Engine
}

// rocksDBMapIterator iterates over the keys of a RocksDBMap in sorted order.
type rocksDBMapIterator struct {
	iter Iterator
	// makeKey is a function that transforms a key into an MVCCKey with a prefix
	// used to SeekGE() the underlying iterator.
	makeKey func(k []byte) MVCCKey
	// prefix is the prefix of keys that this iterator iterates over.
	prefix []byte
}

// rocksDBMap is a SortedDiskMap that uses RocksDB as its underlying storage
// engine.
type rocksDBMap struct {
	// TODO(asubiotto): Add memory accounting.
	prefix          []byte
	store           Engine
	allowDuplicates bool
	keyID           int64
}

var _ diskmap.SortedDiskMapBatchWriter = &rocksDBMapBatchWriter{}
var _ diskmap.SortedDiskMapIterator = &rocksDBMapIterator{}
var _ diskmap.SortedDiskMap = &rocksDBMap{}

// tempStorageID is the temp ID generator for a node. It generates unique
// prefixes for NewRocksDBMap. It is a global because newRocksDBMap needs to
// prefix its writes uniquely, and using a global prevents users from having to
// specify the prefix themselves and correctly guarantee that it is unique.
var tempStorageID uint64

func generateTempStorageID() uint64 {
	return atomic.AddUint64(&tempStorageID, 1)
}

// newRocksDBMap creates a new rocksDBMap with the passed in Engine as the
// underlying store. The rocksDBMap instance will have a keyspace prefixed by a
// unique prefix. The allowDuplicates parameter controls whether Puts with
// identical keys will write multiple entries or overwrite previous entries.
func newRocksDBMap(e Engine, allowDuplicates bool) *rocksDBMap {
	prefix := generateTempStorageID()
	return &rocksDBMap{
		prefix:          encoding.EncodeUvarintAscending([]byte(nil), prefix),
		store:           e,
		allowDuplicates: allowDuplicates,
	}
}

// makeKey appends k to the rocksDBMap's prefix to keep the key local to this
// instance and creates an MVCCKey, which is what the underlying storage engine
// expects. The returned key is only valid until the next call to makeKey().
func (r *rocksDBMap) makeKey(k []byte) MVCCKey {
	// TODO(asubiotto): We can make this more performant by bypassing MVCCKey
	// creation (have to generalize storage API). See
	// https://github.com/cockroachdb/cockroach/issues/16718#issuecomment-311493414
	prefixLen := len(r.prefix)
	r.prefix = append(r.prefix, k...)
	mvccKey := MVCCKey{Key: r.prefix}
	r.prefix = r.prefix[:prefixLen]
	return mvccKey
}

// makeKeyWithTimestamp makes a key appropriate for a Put operation. It is like
// makeKey except it respects allowDuplicates, which uses the MVCC timestamp
// field to assign a unique keyID so duplicate keys don't overwrite each other.
func (r *rocksDBMap) makeKeyWithTimestamp(k []byte) MVCCKey {
	mvccKey := r.makeKey(k)
	if r.allowDuplicates {
		r.keyID++
		mvccKey.Timestamp.WallTime = r.keyID
	}
	return mvccKey
}

// NewIterator implements the SortedDiskMap interface.
func (r *rocksDBMap) NewIterator() diskmap.SortedDiskMapIterator {
	// NOTE: prefix is only false because we can't use the normal prefix
	// extractor. This iterator still only does prefix iteration. See
	// rocksDBMapIterator.Valid().
	return &rocksDBMapIterator{
		iter: r.store.NewIterator(IterOptions{
			UpperBound: roachpb.Key(r.prefix).PrefixEnd(),
		}),
		makeKey: r.makeKey,
		prefix:  r.prefix,
	}
}

// NewBatchWriter implements the SortedDiskMap interface.
func (r *rocksDBMap) NewBatchWriter() diskmap.SortedDiskMapBatchWriter {
	return r.NewBatchWriterCapacity(defaultBatchCapacityBytes)
}

// NewBatchWriterCapacity implements the SortedDiskMap interface.
func (r *rocksDBMap) NewBatchWriterCapacity(capacityBytes int) diskmap.SortedDiskMapBatchWriter {
	makeKey := r.makeKey
	if r.allowDuplicates {
		makeKey = r.makeKeyWithTimestamp
	}
	return &rocksDBMapBatchWriter{
		capacity: capacityBytes,
		makeKey:  makeKey,
		batch:    r.store.NewWriteOnlyBatch(),
		store:    r.store,
	}
}

// Clear implements the SortedDiskMap interface.
func (r *rocksDBMap) Clear() error {
	if err := r.store.ClearRange(
		MVCCKey{Key: r.prefix},
		MVCCKey{Key: roachpb.Key(r.prefix).PrefixEnd()},
	); err != nil {
		return errors.Wrapf(err, "unable to clear range with prefix %v", r.prefix)
	}
	// NB: we manually flush after performing the clear range to ensure that the
	// range tombstone is pushed to disk which will kick off compactions that
	// will eventually free up the deleted space.
	return r.store.Flush()
}

// Close implements the SortedDiskMap interface.
func (r *rocksDBMap) Close(ctx context.Context) {
	if err := r.Clear(); err != nil {
		log.Errorf(ctx, "%v", err)
	}
}

// SeekGE implements the SortedDiskMapIterator interface.
func (i *rocksDBMapIterator) SeekGE(k []byte) {
	i.iter.SeekGE(i.makeKey(k))
}

// Rewind implements the SortedDiskMapIterator interface.
func (i *rocksDBMapIterator) Rewind() {
	i.iter.SeekGE(i.makeKey(nil))
}

// Valid implements the SortedDiskMapIterator interface.
func (i *rocksDBMapIterator) Valid() (bool, error) {
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
func (i *rocksDBMapIterator) Next() {
	i.iter.Next()
}

// UnsafeKey implements the SortedDiskMapIterator interface.
func (i *rocksDBMapIterator) UnsafeKey() []byte {
	return i.iter.UnsafeKey().Key[len(i.prefix):]
}

// UnsafeValue implements the SortedDiskMapIterator interface.
func (i *rocksDBMapIterator) UnsafeValue() []byte {
	return i.iter.UnsafeValue()
}

// Close implements the SortedDiskMapIterator interface.
func (i *rocksDBMapIterator) Close() {
	i.iter.Close()
}

// Put implements the SortedDiskMapBatchWriter interface.
func (b *rocksDBMapBatchWriter) Put(k []byte, v []byte) error {
	if err := b.batch.Put(b.makeKey(k), v); err != nil {
		return err
	}
	b.numPutsSinceFlush++
	if b.batch.Len() >= b.capacity {
		return b.Flush()
	}
	return nil
}

// Flush implements the SortedDiskMapBatchWriter interface.
func (b *rocksDBMapBatchWriter) Flush() error {
	if b.batch.Empty() {
		return nil
	}
	if err := b.batch.Commit(false /* syncCommit */); err != nil {
		return err
	}
	b.numPutsSinceFlush = 0
	b.batch = b.store.NewWriteOnlyBatch()
	return nil
}

// NumPutsSinceFlush implements the SortedDiskMapBatchWriter interface.
func (b *rocksDBMapBatchWriter) NumPutsSinceFlush() int {
	return b.numPutsSinceFlush
}

// Close implements the SortedDiskMapBatchWriter interface.
func (b *rocksDBMapBatchWriter) Close(ctx context.Context) error {
	err := b.Flush()
	b.batch.Close()
	return err
}

// pebbleMapBatchWriter batches writes to a pebbleMap.
type pebbleMapBatchWriter struct {
	// capacity is the number of bytes to write before a Flush() is triggered.
	capacity int

	// makeKey is a function that transforms a key into a byte slice with a prefix
	// to be written to the underlying store.
	makeKey           func(k []byte) []byte
	batch             *pebble.Batch
	numPutsSinceFlush int
	store             *pebble.DB
}

// pebbleMapIterator iterates over the keys of a pebbleMap in sorted order.
type pebbleMapIterator struct {
	allowDuplicates bool
	iter            *pebble.Iterator
	// makeKey is a function that transforms a key into a byte slice with a prefix
	// used to SeekGE() the underlying iterator.
	makeKey func(k []byte) []byte
	// prefix is the prefix of keys that this iterator iterates over.
	prefix []byte
}

// pebbleMap is a SortedDiskMap, similar to rocksDBMap, that uses pebble as its
// underlying storage engine.
type pebbleMap struct {
	prefix          []byte
	store           *pebble.DB
	allowDuplicates bool
	keyID           int64
}

var _ diskmap.SortedDiskMapBatchWriter = &pebbleMapBatchWriter{}
var _ diskmap.SortedDiskMapIterator = &pebbleMapIterator{}
var _ diskmap.SortedDiskMap = &pebbleMap{}

// newPebbleMap creates a new pebbleMap with the passed in Engine as the
// underlying store. The pebbleMap instance will have a keyspace prefixed by a
// unique prefix. The allowDuplicates parameter controls whether Puts with
// identical keys will write multiple entries or overwrite previous entries.
func newPebbleMap(e *pebble.DB, allowDuplicates bool) *pebbleMap {
	prefix := generateTempStorageID()
	return &pebbleMap{
		prefix:          encoding.EncodeUvarintAscending([]byte(nil), prefix),
		store:           e,
		allowDuplicates: allowDuplicates,
	}
}

// makeKey appends k to the pebbleMap's prefix to keep the key local to this
// instance and returns a byte slice containing the user-provided key and the
// prefix. Pebble's operations can take this byte slice as a key. This key is
// only valid until the next call to makeKey.
func (r *pebbleMap) makeKey(k []byte) []byte {
	prefixLen := len(r.prefix)
	r.prefix = append(r.prefix, k...)
	key := r.prefix
	r.prefix = r.prefix[:prefixLen]
	return key
}

// makeKeyWithSequence makes a key appropriate for a Put operation. It is like
// makeKey except it respects allowDuplicates, by appending a sequence number to
// the user-provided key.
func (r *pebbleMap) makeKeyWithSequence(k []byte) []byte {
	byteKey := r.makeKey(k)
	if r.allowDuplicates {
		r.keyID++
		byteKey = encoding.EncodeUint64Ascending(byteKey, uint64(r.keyID))
	}
	return byteKey
}

// NewIterator implements the SortedDiskMap interface.
func (r *pebbleMap) NewIterator() diskmap.SortedDiskMapIterator {
	return &pebbleMapIterator{
		allowDuplicates: r.allowDuplicates,
		iter: r.store.NewIter(&pebble.IterOptions{
			UpperBound: roachpb.Key(r.prefix).PrefixEnd(),
		}),
		makeKey: r.makeKey,
		prefix:  r.prefix,
	}
}

// NewBatchWriter implements the SortedDiskMap interface.
func (r *pebbleMap) NewBatchWriter() diskmap.SortedDiskMapBatchWriter {
	return r.NewBatchWriterCapacity(defaultBatchCapacityBytes)
}

// NewBatchWriterCapacity implements the SortedDiskMap interface.
func (r *pebbleMap) NewBatchWriterCapacity(capacityBytes int) diskmap.SortedDiskMapBatchWriter {
	makeKey := r.makeKey
	if r.allowDuplicates {
		makeKey = r.makeKeyWithSequence
	}
	return &pebbleMapBatchWriter{
		capacity: capacityBytes,
		makeKey:  makeKey,
		batch:    r.store.NewBatch(),
		store:    r.store,
	}
}

// Clear implements the SortedDiskMap interface.
func (r *pebbleMap) Clear() error {
	if err := r.store.DeleteRange(
		r.prefix,
		roachpb.Key(r.prefix).PrefixEnd(),
		pebble.NoSync,
	); err != nil {
		return errors.Wrapf(err, "unable to clear range with prefix %v", r.prefix)
	}
	// NB: we manually flush after performing the clear range to ensure that the
	// range tombstone is pushed to disk which will kick off compactions that
	// will eventually free up the deleted space.
	_, err := r.store.AsyncFlush()
	return err
}

// Close implements the SortedDiskMap interface.
func (r *pebbleMap) Close(ctx context.Context) {
	if err := r.Clear(); err != nil {
		log.Errorf(ctx, "%v", err)
	}
}

// SeekGE implements the SortedDiskMapIterator interface.
func (i *pebbleMapIterator) SeekGE(k []byte) {
	i.iter.SeekGE(i.makeKey(k))
}

// Rewind implements the SortedDiskMapIterator interface.
func (i *pebbleMapIterator) Rewind() {
	i.iter.SeekGE(i.makeKey(nil))
}

// Valid implements the SortedDiskMapIterator interface.
func (i *pebbleMapIterator) Valid() (bool, error) {
	return i.iter.Valid(), nil
}

// Next implements the SortedDiskMapIterator interface.
func (i *pebbleMapIterator) Next() {
	i.iter.Next()
}

// UnsafeKey implements the SortedDiskMapIterator interface.
func (i *pebbleMapIterator) UnsafeKey() []byte {
	unsafeKey := i.iter.Key()
	end := len(unsafeKey)
	if i.allowDuplicates {
		// There are 8 bytes of sequence number at the end of the key, remove them.
		end -= 8
	}
	return unsafeKey[len(i.prefix):end]
}

// UnsafeValue implements the SortedDiskMapIterator interface.
func (i *pebbleMapIterator) UnsafeValue() []byte {
	return i.iter.Value()
}

// Close implements the SortedDiskMapIterator interface.
func (i *pebbleMapIterator) Close() {
	_ = i.iter.Close()
}

// Put implements the SortedDiskMapBatchWriter interface.
func (b *pebbleMapBatchWriter) Put(k []byte, v []byte) error {
	key := b.makeKey(k)
	if err := b.batch.Set(key, v, nil); err != nil {
		return err
	}
	b.numPutsSinceFlush++
	if len(b.batch.Repr()) >= b.capacity {
		return b.Flush()
	}
	return nil
}

// Flush implements the SortedDiskMapBatchWriter interface.
func (b *pebbleMapBatchWriter) Flush() error {
	if err := b.batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	b.numPutsSinceFlush = 0
	b.batch = b.store.NewBatch()
	return nil
}

// NumPutsSinceFlush implements the SortedDiskMapBatchWriter interface.
func (b *pebbleMapBatchWriter) NumPutsSinceFlush() int {
	return b.numPutsSinceFlush
}

// Close implements the SortedDiskMapBatchWriter interface.
func (b *pebbleMapBatchWriter) Close(ctx context.Context) error {
	err := b.Flush()
	if err != nil {
		return err
	}
	return b.batch.Close()
}
