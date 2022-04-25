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
// such that any valid MVCC key returned will have a timestamp less than the
// asOf timestamp, is neither a delete tombstone, nor shadowed by a delete
// tombstone less than the asOf timestamp.
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
// provided key that obeys the ReadAsOfIterator key constraints.
func (f *ReadAsOfIterator) SeekGE(originalKey MVCCKey) {
	// To ensure SeekGE seeks to a key that isn't shadowed by a tombstone that the
	// ReadAsOfIterator would have skipped (i.e. at a time earlier than the AOST),
	// seek to the key with the latest possible timestamp, advance to the next
	// valid key only if this valid key is less (at a newer timestamp, and same
	// key) than the original key.
	synthetic := MVCCKey{Key: originalKey.Key, Timestamp: hlc.MaxTimestamp}
	f.iter.SeekGE(synthetic)
	if ok := f.advance(); !ok {
		return
	}
	for f.UnsafeKey().Less(originalKey) {
		// The following is true:
		// originalKey.Key == syntheticKey.key &&
		// f.asOf timestamp >= current timestamp > originalKey timestamp
		// move to the next key and advance to a key that obeys the asOf constraints.
		f.iter.Next()
		if ok := f.advance(); !ok {
			return
		}
	}
}

// Valid implements the simpleMVCCIterator.
func (f *ReadAsOfIterator) Valid() (bool, error) {
	return f.iter.Valid()
}

// Next advances the iterator to the next valid key/value in the iteration that
// has a timestamp less than f.asOf.
func (f *ReadAsOfIterator) Next() {
	f.iter.Next()
	f.advance()
}

// NextKey advances the iterator to the next valid MVCC key that has a timestamp
// less than f.asOf.
func (f *ReadAsOfIterator) NextKey() {
	f.iter.NextKey()
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
func (f *ReadAsOfIterator) advance() bool {
	if ok, _ := f.Valid(); !ok {
		return ok
	}

	// If neither of the internal functions move the iterator, then the current
	// key, if valid, is guaranteed to have a timestamp earlier than the asOf time and is
	// not a deleted key. If either function moves the iterator, the timestamp and
	// tombstone presence must be checked again.
	var ok bool
	moved := true
	for {
		ok, moved = f.advanceAsOf()
		if !ok {
			return ok
		}
		if moved {
			continue
		}
		ok, moved = f.advanceDel()
		if !ok {
			return ok
		}
		if !moved {
			break
		}
	}
	return true
}

// advanceDel moves the iterator to the next key if the current key is a delete
// tombstone.
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

// advanceAsOf moves the iterator to the next key if the current key is later
// than the asOf timestamp.
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
// SimpleMVCCIterator such that any valid MVCC key returned will have a
// timestamp less than the asOf timestamp, is neither a delete tombstone, nor
// shadowed by a delete tombstone less than the asOf timestamp. The caller is
// responsible for closing the underlying SimpleMVCCIterator.
func NewReadAsOfIterator(iter SimpleMVCCIterator, asOf hlc.Timestamp) *ReadAsOfIterator {
	return &ReadAsOfIterator{iter: iter, asOf: asOf}
}
