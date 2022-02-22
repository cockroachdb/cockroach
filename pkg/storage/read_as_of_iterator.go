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
// such that all MVCC keys returned are not delete tombstones and will have
// timestamps less than the endtime timestamp, unless that key is invalid.
type ReadAsOfIterator struct {
	iter SimpleMVCCIterator

	// endtime is the latest timestamp an MVCC key will have that the
	// ReadAsOfIterator will return
	endTime hlc.Timestamp

	// valid is the latest boolean value returned by Valid()
	valid bool
}

var _ SimpleMVCCIterator = &ReadAsOfIterator{}

// Close no-ops. The caller is responsible for closing the underlying iterator.
func (f *ReadAsOfIterator) Close() {
}

// SeekGE advances the iterator to the first key in the engine which is >= the
// provided key and less than f.endtime.
//
// If Valid() is false after SeekGE() returns, the iterator may be at an invalid
// key with a timestamp greater than or equal to f.endtime.
func (f *ReadAsOfIterator) SeekGE(key MVCCKey) {
	f.iter.SeekGE(key)
	f.advance()
}

// Valid must be called after any call to Seek(), Next(), or similar methods. It
// returns (true, nil) if the iterator points to a valid key (it is undefined to
// call UnsafeKey(), UnsafeValue(), or similar methods unless Valid() has
// returned (true, nil)). It returns (false, nil) if the iterator has moved past
// the end of the valid range, or (false, err) if an error has occurred. Valid()
// will never return true with a non-nil error.
func (f *ReadAsOfIterator) Valid() (bool, error) {
	var err error
	f.valid, err = f.iter.Valid()
	return f.valid, err
}

// Next advances the iterator to the next valid key/value in the iteration that
// has a timestamp less than f.endtime. If Valid() is false after NextKey()
// returns, the iterator may be at an invalid key with a timestamp greater than
// or equal to f.endtime.
func (f *ReadAsOfIterator) Next() {
	f.iter.Next()
	f.advance()
}

// NextKey advances the iterator to the next valid MVCC key that has a timestamp
// less than f.endtime. If Valid() is false after NextKey() returns, the
// iterator may be at an invalid key with a timestamp greater than or equal to
// f.endtime.
//
// This operation is distinct from Next which advances to the next version of
// the current key or the next key if the iterator is currently located at the
// last version for a key.
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

// advance checks that the current key is valid, and if it is, advances past
// keys with timestamps later than the f.endTime and keys whose value has
// been deleted.
func (f *ReadAsOfIterator) advance() {
	if _, _ = f.Valid(); !f.valid {
		return
	}
	if !f.endTime.IsEmpty() {
		for f.endTime.Less(f.iter.UnsafeKey().Timestamp) {
			f.iter.Next()
			if _, _ = f.Valid(); !f.valid {
				return
			}
		}
	}
	for len(f.iter.UnsafeValue()) == 0 {
		f.iter.NextKey()
		if _, _ = f.Valid(); !f.valid {
			return
		}
	}
}

// MakeReadAsOfIterator constructs a ReadAsOfIterator which wraps a
// SimpleMVCCIterator such that all MVCC keys returned will not be tombstones
// and will have timestamps less than the endtime timestamp, unless that key is
// invalid. The caller is responsible for closing the underlying
// SimpleMVCCIterator.
func MakeReadAsOfIterator(iter SimpleMVCCIterator, endtime hlc.Timestamp) *ReadAsOfIterator {
	return &ReadAsOfIterator{iter: iter, endTime: endtime}
}
