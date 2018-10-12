// Copyright 2018 The Cockroach Authors.
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

package diskmap

import "context"

// Factory is an interface that can produce SortedDiskMaps.
type Factory interface {
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

	// Clear clears the map's data for reuse.
	Clear() error

	// Close frees up resources held by the map.
	Close(context.Context)
}
