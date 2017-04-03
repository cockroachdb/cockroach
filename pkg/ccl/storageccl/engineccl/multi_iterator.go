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
	"github.com/pkg/errors"
)

const invalidIdxSentinel = -1

// MultiIterator multiplexes iteration over a number of engine.Iterators.
type MultiIterator struct {
	iters []engine.Iterator
	// The index into `iters` of the iterator currently being pointed at.
	currentIdx int
	// The indexes of every iterator with the same key as the one in currentIdx.
	itersWithCurrentKey []int

	// err, if non-nil, is an error encountered by one of the underlying
	// Iterators or in some incompatibility between them. If err is non-nil,
	// Valid must return false.
	err error
}

// TODO(dan): The methods on MultiIterator intentionally have the same
// signatures as the ones on engine.Iterator, though not all of them are
// present. Consider making an interface that's a subset (SimpleIterator?) and
// unexport MultiIterator.

// MakeMultiIterator creates an iterator that multiplexes engine.Iterators.
func MakeMultiIterator(iters []engine.Iterator) *MultiIterator {
	return &MultiIterator{
		iters:               iters,
		currentIdx:          invalidIdxSentinel,
		itersWithCurrentKey: make([]int, 0, len(iters)),
	}
}

// Seek advances the iterator to the first key in the engine which is >= the
// provided key.
func (f *MultiIterator) Seek(key engine.MVCCKey) {
	for _, iter := range f.iters {
		iter.Seek(key)
	}
	// Each of the iterators now points at the first key >= key. Set currentIdx
	// and itersWithCurrentKey but don't advance any of the underlying
	// iterators.
	f.advance()
}

// Valid returns true if the iterator is currently valid. An iterator that
// hasn't had Seek called on it or has gone past the end of the key range is
// invalid.
func (f *MultiIterator) Valid() bool {
	return f.currentIdx != invalidIdxSentinel && f.err == nil
}

// Error returns the error, if any, which the iterator encountered.
func (f *MultiIterator) Error() error {
	return f.err
}

// UnsafeKey returns the current key, but the memory is invalidated on the next
// call to {NextKey,Seek}.
func (f *MultiIterator) UnsafeKey() engine.MVCCKey {
	return f.iters[f.currentIdx].UnsafeKey()
}

// UnsafeValue returns the current value as a byte slice, but the memory is
// invalidated on the next call to {NextKey,Seek}.
func (f *MultiIterator) UnsafeValue() []byte {
	return f.iters[f.currentIdx].UnsafeValue()
}

// Next advances the iterator to the next key/value in the iteration. After this
// call, Valid() will be true if the iterator was not positioned at the last
// key.
func (f *MultiIterator) Next() {
	// Advance the current iterator one and recompute currentIdx.
	f.iters[f.currentIdx].Next()
	f.advance()
}

// NextKey advances the iterator to the next MVCC key. This operation is
// distinct from Next which advances to the next version of the current key or
// the next key if the iterator is currently located at the last version for a
// key.
func (f *MultiIterator) NextKey() {
	// Advance each iterator at the current key to its next key, then recompute
	// currentIdx.
	for _, iterIdx := range f.itersWithCurrentKey {
		f.iters[iterIdx].NextKey()
	}
	f.advance()
}

func (f *MultiIterator) advance() {
	// Loop through every iterator, storing the best next value for currentIdx
	// in proposedNextIdx as we go. If it's still invalidIdxSentinel at the end,
	// then all the underlying iterators are exhausted and so is this one.
	proposedNextIdx := invalidIdxSentinel
	for iterIdx, iter := range f.iters {
		// If this iterator is exhausted skip it (or error if it's errored).
		// TODO(dan): Iterators that are exhausted could be removed to save
		// having to check them on all future calls to advance.
		if ok, err := iter.Valid(); err != nil {
			f.err = err
			return
		} else if !ok {
			continue
		}

		// Fill proposedMVCCKey with the mvcc key of the current best for the
		// next value for currentIdx (or a sentinel that sorts after everything
		// if this is the first non-exhausted iterator).
		proposedMVCCKey := engine.MVCCKey{Key: keys.MaxKey}
		if proposedNextIdx != invalidIdxSentinel {
			proposedMVCCKey = f.iters[proposedNextIdx].UnsafeKey()
		}

		iterMVCCKey := iter.UnsafeKey()
		if cmp := bytes.Compare(iterMVCCKey.Key, proposedMVCCKey.Key); cmp < 0 {
			// The iterator at iterIdx has a lower key than any seen so far.
			// Update proposedNextIdx with it and reset itersWithCurrentKey
			// (because everything seen so for must have had a higher key).
			f.itersWithCurrentKey = f.itersWithCurrentKey[:0]
			f.itersWithCurrentKey = append(f.itersWithCurrentKey, iterIdx)
			proposedNextIdx = iterIdx
		} else if cmp == 0 {
			// The iterator at iterIdx has the same key as the current best, add
			// it to itersWithCurrentKey and check how the timestamps compare.
			f.itersWithCurrentKey = append(f.itersWithCurrentKey, iterIdx)
			if proposedMVCCKey.Timestamp.Less(iterMVCCKey.Timestamp) {
				// This iterator has a greater timestamp and so should sort
				// first, update the current best.
				proposedNextIdx = iterIdx
			} else if proposedMVCCKey.Timestamp == iterMVCCKey.Timestamp {
				// We have two exactly equal mvcc keys (both key and timestamps
				// match). It's ambiguous which should sort first, so error.
				f.err = errors.Errorf(
					"got two entries for the same key and timestamp %s: %x vs %x",
					iterMVCCKey, f.iters[proposedNextIdx].UnsafeValue(), f.iters[proposedNextIdx].UnsafeValue(),
				)
				return
			}
		}
	}

	// NB: proposedNextIdx will still be invalidIdxSentinel here if this
	// iterator is exhausted.
	f.currentIdx = proposedNextIdx
}
