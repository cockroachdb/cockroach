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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// NewPebbleSSTIterator returns an `MVCCIterator` for the provided SST files.
// The SSTs are merged during iteration. The file slice must be sorted in
// reverse chronological order: a key in a file at a lower index will shadow the
// same key contained within a file at a higher index.
//
// TODO(erikgrinaker): This currently has significant performance overhead
// compared with sstIterator. This must be optimized, and then replace (or be
// used in) NewSSTIterator and NewMemSSTIterator. It should also replace
// MultiIterator.
func NewPebbleSSTIterator(files []sstable.ReadableFile, opts IterOptions) (MVCCIterator, error) {
	return newPebbleSSTIterator(files, opts)
}

// NewPebbleMemSSTIterator returns an `MVCCIterator` for the provided SST data,
// similarly to NewPebbleSSTIterator().
func NewPebbleMemSSTIterator(sst []byte, verify bool, opts IterOptions) (MVCCIterator, error) {
	return NewPebbleMultiMemSSTIterator([][]byte{sst}, verify, opts)
}

// NewPebbleMultiMemSSTIterator returns an `MVCCIterator` for the provided SST
// data, similarly to NewPebbleSSTIterator().
func NewPebbleMultiMemSSTIterator(
	ssts [][]byte, verify bool, opts IterOptions,
) (MVCCIterator, error) {
	files := make([]sstable.ReadableFile, 0, len(ssts))
	for _, sst := range ssts {
		files = append(files, vfs.NewMemFile(sst))
	}
	iter, err := NewPebbleSSTIterator(files, opts)
	if err != nil {
		return nil, err
	}
	if verify {
		iter = NewVerifyingMVCCIterator(iter)
	}
	return iter, nil
}

type sstIterator struct {
	sst  *sstable.Reader
	iter sstable.Iterator

	mvccKey   MVCCKey
	value     []byte
	iterValid bool
	err       error

	// For allocation avoidance in SeekGE and NextKey.
	keyBuf []byte

	// roachpb.Verify k/v pairs on each call to Next.
	verify bool

	// For determining whether to trySeekUsingNext=true in SeekGE.
	prevSeekKey  MVCCKey
	seekGELastOp bool
}

// NewSSTIterator returns a `SimpleMVCCIterator` for the provided file, which it
// assumes was written by pebble `sstable.Writer`and contains keys which use
// Cockroach's MVCC format.
//
// NewSSTIterator will be deprecated. Callers should use NewPebbleSSTIterator
// instead. It is worthwhile to run a benchmark after the refactor, to ensure
// the PebbleSSTIterator doesn't cause a significant regression.
func NewSSTIterator(file sstable.ReadableFile) (SimpleMVCCIterator, error) {
	sst, err := sstable.NewReader(file, sstable.ReaderOptions{
		Comparer: EngineComparer,
	})
	if err != nil {
		return nil, err
	}
	return &sstIterator{sst: sst}, nil
}

// NewMemSSTIterator returns a `SimpleMVCCIterator` for an in-memory sstable.
// It's compatible with sstables written by `RocksDBSstFileWriter` and
// Pebble's `sstable.Writer`, and assumes the keys use Cockroach's MVCC
// format.
//
// NewMemSSTIterator will be deprecated. Callers should use
// NewPebbleMemSSTIterator instead. It is worthwhile to run a benchmark after
// the refactor, to ensure the PebbleSSTIterator doesn't cause a significant
// regression.
func NewMemSSTIterator(data []byte, verify bool) (SimpleMVCCIterator, error) {
	sst, err := sstable.NewReader(vfs.NewMemFile(data), sstable.ReaderOptions{
		Comparer: EngineComparer,
	})
	if err != nil {
		return nil, err
	}
	return &sstIterator{sst: sst, verify: verify}, nil
}

// Close implements the SimpleMVCCIterator interface.
func (r *sstIterator) Close() {
	if r.iter != nil {
		r.err = errors.Wrap(r.iter.Close(), "closing sstable iterator")
	}
	if err := r.sst.Close(); err != nil && r.err == nil {
		r.err = errors.Wrap(err, "closing sstable")
	}
}

// SeekGE implements the SimpleMVCCIterator interface.
func (r *sstIterator) SeekGE(key MVCCKey) {
	if r.err != nil {
		return
	}
	if r.iter == nil {
		// MVCCIterator creation happens on the first Seek as it involves I/O.
		r.iter, r.err = r.sst.NewIter(nil /* lower */, nil /* upper */)
		if r.err != nil {
			return
		}
	}
	r.keyBuf = EncodeMVCCKeyToBuf(r.keyBuf, key)
	var iKey *sstable.InternalKey
	var flags sstable.SeekGEFlags
	if r.seekGELastOp && !key.Less(r.prevSeekKey) {
		// trySeekUsingNext = r.prevSeekKey <= key
		flags = flags.EnableTrySeekUsingNext()
	}
	// NB: seekGELastOp may still be true, and we haven't updated prevSeekKey.
	// So be careful not to return before the end of the function that sets these
	// fields up for the next SeekGE.
	iKey, r.value = r.iter.SeekGE(r.keyBuf, flags)
	if iKey != nil {
		r.iterValid = true
		r.mvccKey, r.err = DecodeMVCCKey(iKey.UserKey)
	} else {
		r.iterValid = false
		r.err = r.iter.Error()
	}
	if r.iterValid && r.err == nil && r.verify && r.mvccKey.IsValue() {
		r.verifyValue()
	}
	r.prevSeekKey.Key = append(r.prevSeekKey.Key[:0], r.mvccKey.Key...)
	r.prevSeekKey.Timestamp = r.mvccKey.Timestamp
	r.seekGELastOp = true
}

// Valid implements the SimpleMVCCIterator interface.
func (r *sstIterator) Valid() (bool, error) {
	return r.iterValid && r.err == nil, r.err
}

// Next implements the SimpleMVCCIterator interface.
func (r *sstIterator) Next() {
	r.seekGELastOp = false
	if !r.iterValid || r.err != nil {
		return
	}
	var iKey *sstable.InternalKey
	iKey, r.value = r.iter.Next()
	if iKey != nil {
		r.mvccKey, r.err = DecodeMVCCKey(iKey.UserKey)
	} else {
		r.iterValid = false
		r.err = r.iter.Error()
	}
	if r.iterValid && r.err == nil && r.verify && r.mvccKey.IsValue() {
		r.verifyValue()
	}
}

// NextKey implements the SimpleMVCCIterator interface.
func (r *sstIterator) NextKey() {
	r.seekGELastOp = false
	if !r.iterValid || r.err != nil {
		return
	}
	r.keyBuf = append(r.keyBuf[:0], r.mvccKey.Key...)
	for r.Next(); r.iterValid && r.err == nil && bytes.Equal(r.keyBuf, r.mvccKey.Key); r.Next() {
	}
}

// UnsafeKey implements the SimpleMVCCIterator interface.
func (r *sstIterator) UnsafeKey() MVCCKey {
	return r.mvccKey
}

// UnsafeValue implements the SimpleMVCCIterator interface.
func (r *sstIterator) UnsafeValue() []byte {
	return r.value
}

// verifyValue verifies the checksum of the current value.
func (r *sstIterator) verifyValue() {
	mvccValue, ok, err := tryDecodeSimpleMVCCValue(r.value)
	if !ok && err == nil {
		mvccValue, err = decodeExtendedMVCCValue(r.value)
	}
	if err != nil {
		r.err = err
	} else {
		r.err = mvccValue.Value.Verify(r.mvccKey.Key)
	}
}

// HasPointAndRange implements SimpleMVCCIterator.
//
// TODO(erikgrinaker): implement range key support.
func (r *sstIterator) HasPointAndRange() (bool, bool) {
	return true, false
}

// RangeBounds implements SimpleMVCCIterator.
func (r *sstIterator) RangeBounds() roachpb.Span {
	return roachpb.Span{}
}

// RangeKeys implements SimpleMVCCIterator.
func (r *sstIterator) RangeKeys() MVCCRangeKeyStack {
	return MVCCRangeKeyStack{}
}
