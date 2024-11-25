// Copyright 2024 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// ReadCompleteAsOfIterator wraps a SimpleMVCCIterator and only surfaces the latest valid key
// of a given MVCC key that is also below the asOfTimestamp, if set. The iterator does
// surface point/range tombstones, but not any MVCC keys shadowed by tombstones below the asOfTimestamp, if set.
// The iterator assumes that it will not encounter any write intents.
type ReadCompleteAsOfIterator struct {
	iter SimpleMVCCIterator

	// asOf is the latest timestamp of a key surfaced by the iterator.
	asOf hlc.Timestamp

	// valid tracks if the current key is valid
	valid bool

	// err tracks if iterating to the current key returned an error
	err error

	// newestRangeTombstone contains the timestamp of the latest range
	// tombstone below asOf at the current position, if any.
	newestRangeTombstone hlc.Timestamp
}

var _ SimpleMVCCIterator = &ReadCompleteAsOfIterator{}

func NewReadCompleteAsOfIterator(
	iter SimpleMVCCIterator, asOf hlc.Timestamp,
) *ReadCompleteAsOfIterator {
	if asOf.IsEmpty() {
		asOf = hlc.MaxTimestamp
	}
	return &ReadCompleteAsOfIterator{
		iter: iter,
		asOf: asOf,
	}
}

func (f *ReadCompleteAsOfIterator) Close() {
	f.iter.Close()
}

func (f *ReadCompleteAsOfIterator) Next() {
	f.NextKey()
}

func (f *ReadCompleteAsOfIterator) NextKey() {
	f.iter.NextKey()
	f.advance()
}

func (f *ReadCompleteAsOfIterator) SeekGE(originalKey MVCCKey) {
	synthetic := MVCCKey{Key: originalKey.Key, Timestamp: f.asOf}
	f.iter.SeekGE(synthetic)
	if f.advance(); f.valid && f.UnsafeKey().Less(originalKey) {
		f.NextKey()
	}
}

func (f *ReadCompleteAsOfIterator) updateValid() bool {
	f.valid, f.err = f.iter.Valid()
	return f.valid
}

func (f *ReadCompleteAsOfIterator) advance() {
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

func (f *ReadCompleteAsOfIterator) UnsafeKey() MVCCKey {
	return f.iter.UnsafeKey()
}

func (f *ReadCompleteAsOfIterator) UnsafeValue() ([]byte, error) {
	return f.iter.UnsafeValue()
}

func (f *ReadCompleteAsOfIterator) Valid() (bool, error) {
	if util.RaceEnabled && f.valid {
		if err := f.assertInvariants(); err != nil {
			return false, err
		}
	}
	return f.valid, f.err
}

func (f *ReadCompleteAsOfIterator) MVCCValueLenAndIsTombstone() (int, bool, error) {
	return f.iter.MVCCValueLenAndIsTombstone()
}

func (f *ReadCompleteAsOfIterator) ValueLen() int {
	return f.iter.ValueLen()
}

func (f *ReadCompleteAsOfIterator) HasPointAndRange() (bool, bool) {
	return true, false
}

func (f *ReadCompleteAsOfIterator) RangeBounds() roachpb.Span {
	return roachpb.Span{}
}

func (f *ReadCompleteAsOfIterator) RangeKeys() MVCCRangeKeyStack {
	return MVCCRangeKeyStack{}
}

func (f *ReadCompleteAsOfIterator) RangeKeyChanged() bool {
	return false
}

func (f *ReadCompleteAsOfIterator) assertInvariants() error {
	if err := assertSimpleMVCCIteratorInvariants(f); err != nil {
		return err
	}

	if f.asOf.IsEmpty() {
		return errors.AssertionFailedf("f.asOf is empty")
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
