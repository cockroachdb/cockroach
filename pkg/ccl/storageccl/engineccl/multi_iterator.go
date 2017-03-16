// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package engineccl

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

// MultiIterator multiplexes iteration over a number of engine.Iterators.
type MultiIterator struct {
	iters          []engine.Iterator
	itersToAdvance []int

	valid bool
	err   error
	key   engine.MVCCKey
	value []byte
}

// TODO(dan): This is intentionally a subset of the engine.Iterator interface.
// Consider making an interface that's a subset (SimpleIterator?) and unexport
// MultiIterator.

// MakeMultiIterator creates an iterator that multiplexes engine.Iterators.
func MakeMultiIterator(iters []engine.Iterator) *MultiIterator {
	// TODO(dan): Support Next().
	return &MultiIterator{
		iters:          iters,
		itersToAdvance: make([]int, 0, len(iters)),
	}
}

// Seek advances the iterator to the first key in the engine which is >= the
// provided key.
func (f *MultiIterator) Seek(key engine.MVCCKey) {
	for _, iter := range f.iters {
		iter.Seek(key)
	}
	f.NextKey()
}

// Valid returns true if the iterator is currently valid. An iterator that
// hasn't had Seek called on it or has gone past the end of the key range
// is invalid.
func (f *MultiIterator) Valid() bool {
	return f.valid && f.err == nil
}

// Error returns the error, if any, which the iterator encountered.
func (f *MultiIterator) Error() error {
	return f.err
}

// UnsafeKey returns the same value as Key, but the memory is invalidated on the
// next call to {NextKey,Seek}.
func (f *MultiIterator) UnsafeKey() engine.MVCCKey {
	return f.key
}

// UnsafeValue returns the same value as Value, but the memory is invalidated on
// the next call to {NextKey,Seek}.
func (f *MultiIterator) UnsafeValue() []byte {
	return f.value
}

// NextKey advances the iterator to the next MVCC key.
func (f *MultiIterator) NextKey() {
	for {
		f.key.Key = append(f.key.Key[:0], keys.MaxKey...)
		f.key.Timestamp = hlc.Timestamp{}
		f.itersToAdvance = f.itersToAdvance[:0]

		empty := true
		for iterIdx, iter := range f.iters {
			if !iter.Valid() {
				if err := iter.Error(); err != nil {
					f.valid = false
					f.err = err
					return
				}
				continue
			}
			empty = false
			key := iter.UnsafeKey()
			cmp := bytes.Compare(key.Key, f.key.Key)
			if cmp < 0 {
				f.itersToAdvance = f.itersToAdvance[:0]
				f.itersToAdvance = append(f.itersToAdvance, iterIdx)
				f.key.Key = append(f.key.Key[:0], key.Key...)
				f.key.Timestamp = key.Timestamp
				f.value = append(f.value[:0], iter.UnsafeValue()...)
			} else if cmp == 0 {
				f.itersToAdvance = append(f.itersToAdvance, iterIdx)
				if f.key.Timestamp.Less(key.Timestamp) {
					f.key.Key = append(f.key.Key[:0], key.Key...)
					f.key.Timestamp = key.Timestamp
					f.value = append(f.value[:0], iter.UnsafeValue()...)
				} else if f.key.Timestamp == key.Timestamp {
					f.valid = false
					f.err = errors.Errorf(
						"got two entries for the same key and timestamp %s: %x vs %x",
						f.key, f.value, iter.UnsafeValue(),
					)
				}
			}
		}
		if empty {
			f.valid = false
			return
		}
		for _, iterIdx := range f.itersToAdvance {
			f.iters[iterIdx].NextKey()
		}
		if len(f.value) == 0 {
			continue
		}

		f.valid = true
		return
	}
}
