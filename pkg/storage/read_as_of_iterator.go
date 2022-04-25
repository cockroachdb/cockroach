// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import "github.com/cockroachdb/cockroach/pkg/util/hlc"


// ReadAsOfIterator wraps an iterator that implements the SimpleMVCCIterator
// such that all MVCC keys returned are not delete tombstones, shadowed by a
// delete tombstone, and will have timestamps less than the asOf timestamp,
// unless that key is invalid.
type ReadAsOfIterator struct {
	iter SimpleMVCCIterator

	// asOf is the latest timestamp an MVCC key will have that the
	// ReadAsOfIterator will return
	asOf hlc.Timestamp
}

var _ SimpleMVCCIterator = &ReadAsOfIterator{}

// Close no-ops. The caller is responsible for closing the underlying iterator.
func (f *ReadAsOfIterator) Close() {
}

// SeekGE advances the iterator to the first key in the engine which is >= the
// provided key, less than f.asOf, and isn't a tombstone or shadowed by one.
//
// If Valid() is false after SeekGE() returns, the iterator may be at an invalid
// key with a timestamp greater than or equal to f.asOf.
func (f *ReadAsOfIterator) SeekGE(key MVCCKey) {
	// To ensure SeekGE seeks to a key that isn't shadowed by a tombstone,
	// seek to the key with the newest possible timestamp, advance to the next valid key,
	// and only re-seek if this valid key is less (at a newer timestamp,
	// and same key) than the original key.
	synthetic := MVCCKey{Key: key.Key, Timestamp: hlc.MaxTimestamp}
	f.iter.SeekGE(synthetic)
	f.advance()

	if f.UnsafeKey().Less(key) {
		f.iter.SeekGE(key)
		f.advance()
	}
}

// Valid implements the simpleMVCCIterator
func (f *ReadAsOfIterator) Valid() (bool, error) {
	return f.iter.Valid()
}

// Next advances the iterator to the next valid key/value in the iteration that
// has a timestamp less than f.asOf. If Valid() is false after NextKey()
// returns, the iterator may be at an invalid key with a timestamp greater than
// or equal to f.asOf.
func (f *ReadAsOfIterator) Next() {
	f.iter.Next()
	if ok, _ := f.Valid(); !ok {
		return
	}
	f.advance()
}

// NextKey advances the iterator to the next valid MVCC key that has a timestamp
// less than f.asOf. If Valid() is false after NextKey() returns, the
// iterator may be at an invalid key with a timestamp greater than or equal to
// f.asOf.
func (f *ReadAsOfIterator) NextKey() {
	f.iter.NextKey()
	if ok, _ := f.Valid(); !ok {
		return
	}
	f.advance()
}

// UnsafeKey returns the current key, but the memory is invalidated on the next
// call to {NextKey,Seek}.
func (f *ReadAsOfIterator) UnsafeKey() MVCCKey {
	return f.iter.UnsafeKey()
}

// UnsafeValue returns the current value as a byte slice, but the memory is
// invalidated on the next call to {NextKey,Seek}.
func (f *ReadAsOfIterator) UnsafeValue() []byte {
	return f.iter.UnsafeValue()
}

// advance moves past keys with timestamps later than the f.asOf and MVCC keys whose
// value has been deleted.
func (f *ReadAsOfIterator) advance() {
	var ok bool

	// If neither of the internal functions move the iterator, then the current
	// key, if valid, is guaranteed to have a timestamp earlier than the asOf time and is
	// not a deleted key. If either function moves the iterator, the timestamp and
	// tombstone presence must be checked again.
	moved := true
	for {
		ok, moved = f.advanceDel()
		if !ok {
			return
		}
		if moved {
			// if a tombstone is found, we must re-check for tombstones before checking the timestamp
			continue
		}
		ok, moved = f.advanceAsOf()
		if !ok {
			return
		}
		if !moved {
			break
		}
	}
}

// advanceDel moves the iterator to the next key if the current key is a delete tombstone
func (f *ReadAsOfIterator) advanceDel() (bool, bool) {
	var moved bool
	if len(f.iter.UnsafeValue()) == 0 {
		// Implies the latest version of the given key is a tombstone.
		f.iter.NextKey()
		moved = true
		if ok, _ := f.Valid(); !ok {
			return ok, moved
		}
	}
	return true, moved
}

// advanceAsOf moves the iterator to the next key if the current key is later than the asOf
// timestamp.
func (f *ReadAsOfIterator) advanceAsOf() (bool, bool) {
	var moved bool
	if !f.asOf.IsEmpty() && f.asOf.Less(f.iter.UnsafeKey().Timestamp) {
		f.iter.Next()
		moved = true
		if ok, _ := f.Valid(); !ok {
			return ok, moved
		}
	}
	return true, moved
}

// NewReadAsOfIterator constructs a ReadAsOfIterator which wraps a
// SimpleMVCCIterator such that all MVCC keys returned will not be tombstones
// and will have timestamps less than the asOf timestamp, unless that key is
// invalid. The caller is responsible for closing the underlying
// SimpleMVCCIterator.
func NewReadAsOfIterator(iter SimpleMVCCIterator, asOf hlc.Timestamp) *ReadAsOfIterator {
	return &ReadAsOfIterator{iter: iter, asOf: asOf}
}
