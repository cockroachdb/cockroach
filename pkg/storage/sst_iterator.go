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
}

// NewSSTIterator returns a `SimpleMVCCIterator` for the provided file, which it
// assumes was written by pebble `sstable.Writer`and contains keys which use
// Cockroach's MVCC format.
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
	r.keyBuf = EncodeKeyToBuf(r.keyBuf, key)
	var iKey *sstable.InternalKey
	iKey, r.value = r.iter.SeekGE(r.keyBuf)
	if iKey != nil {
		r.iterValid = true
		r.mvccKey, r.err = DecodeMVCCKey(iKey.UserKey)
	} else {
		r.iterValid = false
		r.err = r.iter.Error()
	}
	if r.iterValid && r.err == nil && r.verify {
		r.err = roachpb.Value{RawBytes: r.value}.Verify(r.mvccKey.Key)
	}
}

// Valid implements the SimpleMVCCIterator interface.
func (r *sstIterator) Valid() (bool, error) {
	return r.iterValid && r.err == nil, r.err
}

// Next implements the SimpleMVCCIterator interface.
func (r *sstIterator) Next() {
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
	if r.iterValid && r.err == nil && r.verify {
		r.err = roachpb.Value{RawBytes: r.value}.Verify(r.mvccKey.Key)
	}
}

// NextKey implements the SimpleMVCCIterator interface.
func (r *sstIterator) NextKey() {
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
