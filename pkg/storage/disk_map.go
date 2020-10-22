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

// tempStorageID is the temp ID generator for a node. It generates unique
// prefixes for NewPebbleMap. It is a global because NewPebbleMap needs to
// prefix its writes uniquely, and using a global prevents users from having to
// specify the prefix themselves and correctly guarantee that it is unique.
var tempStorageID uint64

func generateTempStorageID() uint64 {
	return atomic.AddUint64(&tempStorageID, 1)
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
