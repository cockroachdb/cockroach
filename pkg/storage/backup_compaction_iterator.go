// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// BackupCompactionIterator wraps a SimpleMVCCIterator and only surfaces the
// latest valid key of a given MVCC key, including point tombstones, at or below the
// asOfTimestamp, if set.
//
// The iterator assumes that it will not encounter any write intents and that the
// wrapped SimpleMVCCIterator *only* surfaces point keys.
type BackupCompactionIterator struct {
	iter SimpleMVCCIterator

	// asOf is the latest timestamp of a key surfaced by the iterator.
	asOf hlc.Timestamp

	// valid tracks if the current key is valid.
	valid bool

	// err tracks if iterating to the current key returned an error.
	err error
}

var _ SimpleMVCCIterator = &BackupCompactionIterator{}

// NewBackupCompactionIterator creates a new BackupCompactionIterator. The asOf timestamp cannot be empty.
func NewBackupCompactionIterator(
	iter SimpleMVCCIterator, asOf hlc.Timestamp,
) (*BackupCompactionIterator, error) {
	if asOf.IsEmpty() {
		return nil, errors.New("asOf timestamp cannot be empty")
	}
	return &BackupCompactionIterator{
		iter: iter,
		asOf: asOf,
	}, nil
}

func (f *BackupCompactionIterator) Close() {
	f.iter.Close()
}

// Next is identical to NextKey, as BackupCompactionIterator only surfaces live keys.
func (f *BackupCompactionIterator) Next() {
	f.NextKey()
}

func (f *BackupCompactionIterator) NextKey() {
	f.iter.NextKey()
	f.advance()
}

func (f *BackupCompactionIterator) SeekGE(originalKey MVCCKey) {
	// See ReadAsOfIterator comment for explanation of this.
	synthetic := MVCCKey{Key: originalKey.Key, Timestamp: f.asOf}
	f.iter.SeekGE(synthetic)
	if f.advance(); f.valid && f.UnsafeKey().Less(originalKey) {
		f.NextKey()
	}
}

func (f *BackupCompactionIterator) updateValid() bool {
	f.valid, f.err = f.iter.Valid()
	return f.valid
}

// advance moves past keys with timestamps later than f.asOf.
func (f *BackupCompactionIterator) advance() {
	for {
		if ok := f.updateValid(); !ok {
			return
		}
		if key := f.iter.UnsafeKey(); f.asOf.Less(key.Timestamp) {
			f.iter.Next()
			continue
		}
		return
	}
}

func (f *BackupCompactionIterator) UnsafeKey() MVCCKey {
	return f.iter.UnsafeKey()
}

func (f *BackupCompactionIterator) UnsafeValue() ([]byte, error) {
	return f.iter.UnsafeValue()
}

func (f *BackupCompactionIterator) Valid() (bool, error) {
	if util.RaceEnabled && f.valid {
		if err := f.assertInvariants(); err != nil {
			return false, err
		}
	}
	return f.valid, f.err
}

func (f *BackupCompactionIterator) MVCCValueLenAndIsTombstone() (int, bool, error) {
	return f.iter.MVCCValueLenAndIsTombstone()
}

func (f *BackupCompactionIterator) ValueLen() int {
	return f.iter.ValueLen()
}

func (f *BackupCompactionIterator) HasPointAndRange() (bool, bool) {
	hasPoint, hasRange := f.iter.HasPointAndRange()
	if hasRange {
		panic("unexpected range tombstone")
	}
	return hasPoint, hasRange
}

func (f *BackupCompactionIterator) RangeBounds() roachpb.Span {
	return roachpb.Span{}
}

func (f *BackupCompactionIterator) RangeKeys() MVCCRangeKeyStack {
	return MVCCRangeKeyStack{}
}

func (f *BackupCompactionIterator) RangeKeyChanged() bool {
	return false
}

// assertInvariants checks that the iterator is in a valid state, but first assumes that the underlying iterator
// has already been validated and is in a valid state.
func (f *BackupCompactionIterator) assertInvariants() error {
	if err := assertSimpleMVCCIteratorInvariants(f); err != nil {
		return err
	}

	if ok, err := f.iter.Valid(); !ok || err != nil {
		errMsg := err.Error()
		return errors.AssertionFailedf("invalid underlying iter with err=%s", errMsg)
	}

	key := f.UnsafeKey()
	if key.Timestamp.IsEmpty() {
		return errors.AssertionFailedf("emitted key %s has no timestamp", key)
	}
	if f.asOf.Less(key.Timestamp) {
		return errors.AssertionFailedf("emitted key %s above asOf timestamp %s", key, f.asOf)
	}

	return nil
}
