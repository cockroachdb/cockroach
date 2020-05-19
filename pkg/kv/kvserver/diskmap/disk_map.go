// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package diskmap

import "context"

// Factory is an interface that can produce SortedDiskMaps.
type Factory interface {
	// Close the factory, freeing up associated resources.
	Close()
	// NewSortedDiskMap returns a fresh SortedDiskMap with no contents.
	NewSortedDiskMap() SortedDiskMap
	// NewSortedDiskMultiMap returns a fresh SortedDiskMap with no contents that permits
	// duplicate keys.
	NewSortedDiskMultiMap() SortedDiskMap
}

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
// 		key := i.UnsafeKey()
//		// Do something.
// 	}
type SortedDiskMapIterator interface {
	// SeekGE sets the iterator's position to the first key greater than or equal
	// to the provided key.
	SeekGE(key []byte)
	// Rewind seeks to the start key.
	Rewind()
	// Valid must be called after any call to Seek(), Rewind(), or Next(). It
	// returns (true, nil) if the iterator points to a valid key and
	// (false, nil) if the iterator has moved past the end of the valid range.
	// If an error has occurred, the returned bool is invalid.
	Valid() (bool, error)
	// Next advances the iterator to the next key in the iteration.
	Next()

	// UnsafeKey returns the same value as Key, but the memory is invalidated on
	// the next call to {Next,Rewind,Seek,Close}.
	UnsafeKey() []byte
	// UnsafeValue returns the same value as Value, but the memory is
	// invalidated on the next call to {Next,Rewind,Seek,Close}.
	UnsafeValue() []byte

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
	// The number of put calls since the last time the writer was flushed.
	NumPutsSinceFlush() int
	// Close flushes all writes to the underlying store and frees up resources
	// held by the batch writer.
	Close(context.Context) error
}

// SortedDiskMap is an on-disk map. Keys are iterated over in sorted order.
type SortedDiskMap interface {
	// NewIterator returns a SortedDiskMapIterator that can be used to iterate
	// over key/value pairs in sorted order.
	NewIterator() SortedDiskMapIterator
	// NewBatchWriter returns a SortedDiskMapBatchWriter that can be used to
	// batch writes to this map for performance improvements.
	NewBatchWriter() SortedDiskMapBatchWriter
	// NewBatchWriterCapacity is identical to NewBatchWriter, but overrides the
	// SortedDiskMapBatchWriter's default capacity with capacityBytes.
	NewBatchWriterCapacity(capacityBytes int) SortedDiskMapBatchWriter

	// Clear clears the map's data for reuse.
	Clear() error

	// Close frees up resources held by the map.
	Close(context.Context)
}
