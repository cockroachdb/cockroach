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

// ReadAsOfIterator wraps a SimpleMVCCIterator and only surfaces the latest
// valid key of a given MVCC key that is also below the asOf timestamp, if set.
// Further, the iterator does not surface delete tombstones, nor any MVCC keys
// shadowed by delete tombstones below the asOf timestamp, if set. The iterator
// assumes that it will not encounter any write intents.
type ReadAsOfIterator struct {
	iter SimpleMVCCIterator

	// asOf is the latest timestamp of a key surfaced by the iterator.
	asOf hlc.Timestamp
}

var _ SimpleMVCCIterator = &ReadAsOfIterator{}

// Close closes the underlying iterator.
func (f *ReadAsOfIterator) Close() {
	f.iter.Close()
}

// SeekGE advances the iterator to the first key in the engine which is >= the
// provided key that obeys the ReadAsOfIterator key constraints.
func (f *ReadAsOfIterator) SeekGE(originalKey MVCCKey) {
	// To ensure SeekGE seeks to a key that isn't shadowed by a tombstone that the
	// ReadAsOfIterator would have skipped (i.e. a tombstone below asOf), seek to
	// the key with the latest possible timestamp that the iterator could surface
	// (i.e. asOf, if set) and iterate to the next valid key at or below the caller's
	// key that also obeys the iterator's constraints.
	maxTime := f.asOf
	if f.asOf.IsEmpty() {
		maxTime = hlc.MaxTimestamp
	}
	synthetic := MVCCKey{Key: originalKey.Key, Timestamp: maxTime}
	f.iter.SeekGE(synthetic)

	if ok := f.advance(); ok && f.UnsafeKey().Less(originalKey) {
		// The following is true:
		// originalKey.Key == f.UnsafeKey &&
		// f.asOf timestamp (if set) >= current timestamp > originalKey timestamp.
		//
		// This implies the caller is seeking to a key that is shadowed by a valid
		// key that obeys the iterator 's constraints. The caller's key is NOT the
		// latest key of the given MVCC key; therefore, skip to the next MVCC key.
		f.NextKey()
	}
}

// Valid implements the simpleMVCCIterator.
func (f *ReadAsOfIterator) Valid() (bool, error) {
	return f.iter.Valid()
}

// Next advances the iterator to the next valid MVCC key obeying the iterator's
// constraints. Note that Next and NextKey have the same implementation because
// the iterator only surfaces the latest valid key of a given MVCC key below the
// asOf timestamp.
func (f *ReadAsOfIterator) Next() {
	f.NextKey()
}

// NextKey advances the iterator to the next valid MVCC key obeying the
// iterator's constraints. NextKey() is only guaranteed to surface a key that
// obeys the iterator's constraints if the iterator was already on a key that
// obeys the constraints. To ensure this, initialize the iterator with a SeekGE
// call before any calls to NextKey().
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

// advance moves past keys with timestamps later than f.asOf and skips MVCC keys
// whose latest value (subject to f.asOF) has been deleted. Note that advance
// moves past keys above asOF before evaluating tombstones, implying the
// iterator will never call f.iter.NextKey() on a tombstone with a timestamp
// later than f.asOF.
func (f *ReadAsOfIterator) advance() bool {
	for {
		if ok, err := f.Valid(); err != nil || !ok {
			// No valid keys found.
			return false
		} else if !f.asOf.IsEmpty() && f.asOf.Less(f.iter.UnsafeKey().Timestamp) {
			// Skip keys above the asOf timestamp.
			f.iter.Next()
		} else if len(f.iter.UnsafeValue()) == 0 {
			// Skip to the next MVCC key if we find a tombstone.
			f.iter.NextKey()
		} else {
			// On a valid key.
			return true
		}
	}
}

// NewReadAsOfIterator constructs a ReadAsOfIterator.
func NewReadAsOfIterator(iter SimpleMVCCIterator, asOf hlc.Timestamp) *ReadAsOfIterator {
	return &ReadAsOfIterator{iter: iter, asOf: asOf}
}
