// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pkg/errors"
)

type sstIterator struct {
	sst  *sstable.Reader
	iter sstable.Iterator

	mvccKey   MVCCKey
	value     []byte
	iterValid bool
	err       error

	// For allocation avoidance in NextKey.
	nextKeyStart []byte

	// roachpb.Verify k/v pairs on each call to Next()
	verify bool
}

// NewSSTIterator returns a `SimpleIterator` for an in-memory sstable.
// It's compatible with sstables written by `RocksDBSstFileWriter` and
// Pebble's `sstable.Writer`, and assumes the keys use Cockroach's MVCC
// format.
func NewSSTIterator(path string) (SimpleIterator, error) {
	file, err := vfs.Default.Open(path)
	if err != nil {
		return nil, err
	}
	sst, err := sstable.NewReader(file, sstable.ReaderOptions{
		Comparer: MVCCComparer,
	})
	if err != nil {
		return nil, err
	}
	return &sstIterator{sst: sst}, nil
}

// NewMemSSTIterator returns a `SimpleIterator` for an in-memory sstable.
// It's compatible with sstables written by `RocksDBSstFileWriter` and
// Pebble's `sstable.Writer`, and assumes the keys use Cockroach's MVCC
// format.
func NewMemSSTIterator(data []byte, verify bool) (SimpleIterator, error) {
	sst, err := sstable.NewReader(vfs.NewMemFile(data), sstable.ReaderOptions{
		Comparer: MVCCComparer,
	})
	if err != nil {
		return nil, err
	}
	return &sstIterator{sst: sst, verify: verify}, nil
}

// Close implements the SimpleIterator interface.
func (r *sstIterator) Close() {
	if r.iter != nil {
		r.err = errors.Wrap(r.iter.Close(), "closing sstable iterator")
	}
	if err := r.sst.Close(); err != nil && r.err == nil {
		r.err = errors.Wrap(err, "closing sstable")
	}
}

// SeekGE implements the SimpleIterator interface.
func (r *sstIterator) SeekGE(key MVCCKey) {
	if r.err != nil {
		return
	}
	if r.iter == nil {
		// Iterator creation happens on the first Seek as it involves I/O.
		r.iter = r.sst.NewIter(nil /* lower */, nil /* upper */)
	}
	var iKey *sstable.InternalKey
	iKey, r.value = r.iter.SeekGE(EncodeKey(key))
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

// Valid implements the SimpleIterator interface.
func (r *sstIterator) Valid() (bool, error) {
	return r.iterValid && r.err == nil, r.err
}

// Next implements the SimpleIterator interface.
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

// NextKey implements the SimpleIterator interface.
func (r *sstIterator) NextKey() {
	if !r.iterValid || r.err != nil {
		return
	}
	r.nextKeyStart = append(r.nextKeyStart[:0], r.mvccKey.Key...)
	for r.Next(); r.iterValid && r.err == nil && bytes.Equal(r.nextKeyStart, r.mvccKey.Key); r.Next() {
	}
}

// UnsafeKey implements the SimpleIterator interface.
func (r *sstIterator) UnsafeKey() MVCCKey {
	return r.mvccKey
}

// UnsafeValue implements the SimpleIterator interface.
func (r *sstIterator) UnsafeValue() []byte {
	return r.value
}

// MVCCKeyCompare compares cockroach keys, including the MVCC timestamps.
// This assumes these are the keys cockroach usually works with i.e. "user" keys
// from the point of view of rocksdb.
func MVCCKeyCompare(a, b []byte) int {
	keyA, tsA, okA := enginepb.SplitMVCCKey(a)
	keyB, tsB, okB := enginepb.SplitMVCCKey(b)
	if !okA || !okB {
		// This should never happen unless there is some sort of corruption of
		// the keys. This is a little bizarre, but the behavior exactly matches
		// engine/db.cc:DBComparator.
		return bytes.Compare(a, b)
	}

	if c := bytes.Compare(keyA, keyB); c != 0 {
		return c
	}
	if len(tsA) == 0 {
		if len(tsB) == 0 {
			return 0
		}
		return -1
	} else if len(tsB) == 0 {
		return 1
	}
	if tsCmp := bytes.Compare(tsB, tsA); tsCmp != 0 {
		return tsCmp
	}
	// If decoded MVCC keys are the same, fallback to comparing raw internal keys
	// in case the internal suffix differentiates them.
	return bytes.Compare(a, b)
}
