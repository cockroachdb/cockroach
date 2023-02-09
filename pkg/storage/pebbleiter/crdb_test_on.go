// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build crdb_test && !crdb_test_off
// +build crdb_test,!crdb_test_off

package pebbleiter

import (
	"math/rand"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// Iterator wraps the *pebble.Iterator in crdb_test builds with an assertionIter
// that detects when Close is called on the iterator twice.  Double closes are
// problematic because they can result in an iterator being added to a sync pool
// twice, allowing concurrent use of the same iterator struct.
type Iterator = *assertionIter

// MaybeWrap returns the provided Pebble iterator, wrapped with double close
// detection.
func MaybeWrap(iter *pebble.Iterator) Iterator {
	return &assertionIter{Iterator: iter}
}

// assertionIter wraps a *pebble.Iterator with assertion checking.
type assertionIter struct {
	*pebble.Iterator
	closed bool
	// unsafeBufs hold buffers used for returning values with short lifetimes to
	// the caller. To assert that the client is respecting the lifetimes,
	// assertionIter mangles the buffers as soon as the associated lifetime
	// expires. This is the same technique applied by the unsafeMVCCIterator in
	// pkg/storage, but this time applied at the API boundary between
	// pkg/storage and Pebble.
	//
	// unsafeBufs holds two buffers per-key type and an index indicating which
	// are currently in use. This is used to randomly switch to a different
	// buffer, ensuring that the buffer(s) returned to the caller for the
	// previous iterator position are garbage (as opposed to just state
	// corresponding to the current iterator position).
	unsafeBufs struct {
		idx int
		key [2][]byte
		val [2][]byte
	}
	rangeKeyBufs struct {
		idx   int
		start [2][]byte
		end   [2][]byte
		keys  [2][]pebble.RangeKeyData
	}
}

func (i *assertionIter) Clone(cloneOpts pebble.CloneOptions) (Iterator, error) {
	iter, err := i.Iterator.Clone(cloneOpts)
	if err != nil {
		return nil, err
	}
	return MaybeWrap(iter), nil
}

func (i *assertionIter) Close() error {
	if i.closed {
		panic(errors.AssertionFailedf("pebble.Iterator already closed"))
	}
	i.closed = true
	return i.Iterator.Close()
}

func (i *assertionIter) Key() []byte {
	if !i.Valid() {
		panic(errors.AssertionFailedf("Key() called on !Valid() pebble.Iterator"))
	}
	idx := i.unsafeBufs.idx
	i.unsafeBufs.key[idx] = append(i.unsafeBufs.key[idx][:0], i.Iterator.Key()...)
	return i.unsafeBufs.key[idx]
}

func (i *assertionIter) Value() []byte {
	if !i.Valid() {
		panic(errors.AssertionFailedf("Value() called on !Valid() pebble.Iterator"))
	}
	idx := i.unsafeBufs.idx
	i.unsafeBufs.val[idx] = append(i.unsafeBufs.val[idx][:0], i.Iterator.Value()...)
	return i.unsafeBufs.val[idx]
}

func (i *assertionIter) LazyValue() pebble.LazyValue {
	if !i.Valid() {
		panic(errors.AssertionFailedf("LazyValue() called on !Valid() pebble.Iterator"))
	}
	return i.Iterator.LazyValue()
}

func (i *assertionIter) RangeBounds() ([]byte, []byte) {
	if !i.Valid() {
		panic(errors.AssertionFailedf("RangeBounds() called on !Valid() pebble.Iterator"))
	}
	if _, hasRange := i.Iterator.HasPointAndRange(); !hasRange {
		panic(errors.AssertionFailedf("RangeBounds() called on pebble.Iterator without range keys"))
	}
	// See maybeSaveAndMangleRangeKeyBufs for where these are saved.
	j := i.rangeKeyBufs.idx
	return i.rangeKeyBufs.start[j], i.rangeKeyBufs.end[j]
}

func (i *assertionIter) RangeKeys() []pebble.RangeKeyData {
	if !i.Valid() {
		panic(errors.AssertionFailedf("RangeKeys() called on !Valid() pebble.Iterator"))
	}
	if _, hasRange := i.Iterator.HasPointAndRange(); !hasRange {
		panic(errors.AssertionFailedf("RangeKeys() called on pebble.Iterator without range keys"))
	}
	// See maybeSaveAndMangleRangeKeyBufs for where these are saved.
	return i.rangeKeyBufs.keys[i.rangeKeyBufs.idx]
}

func (i *assertionIter) First() bool {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.First()
}

func (i *assertionIter) SeekGE(key []byte) bool {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.SeekGE(key)
}

func (i *assertionIter) SeekGEWithLimit(key []byte, limit []byte) pebble.IterValidityState {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.SeekGEWithLimit(key, limit)
}

func (i *assertionIter) SeekPrefixGE(key []byte) bool {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.SeekPrefixGE(key)
}

func (i *assertionIter) Next() bool {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.Next()
}

func (i *assertionIter) NextWithLimit(limit []byte) pebble.IterValidityState {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.NextWithLimit(limit)
}

func (i *assertionIter) NextPrefix() bool {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.NextPrefix()
}

func (i *assertionIter) Last() bool {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.Last()
}

func (i *assertionIter) SeekLT(key []byte) bool {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.SeekLT(key)
}

func (i *assertionIter) SeekLTWithLimit(key []byte, limit []byte) pebble.IterValidityState {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.SeekLTWithLimit(key, limit)
}

func (i *assertionIter) Prev() bool {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.Prev()
}

func (i *assertionIter) PrevWithLimit(limit []byte) pebble.IterValidityState {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.PrevWithLimit(limit)
}

// maybeMangleBufs trashes the contents of buffers used to return unsafe values
// to the caller. This is used to ensure that the client respects the Pebble
// iterator interface and the lifetimes of buffers it returns.
func (i *assertionIter) maybeMangleBufs() {
	if rand.Intn(2) == 0 {
		idx := i.unsafeBufs.idx
		for _, b := range [...][]byte{i.unsafeBufs.key[idx], i.unsafeBufs.val[idx]} {
			for i := range b {
				b[i] = 0
			}
		}
		if rand.Intn(2) == 0 {
			// Switch to a new buffer for the next iterator position.
			i.unsafeBufs.idx = (i.unsafeBufs.idx + 1) % 2
		}
	}
}

// maybeSaveAndMangleRangeKeyBufs is invoked at the end of every iterator
// operation. It saves the range keys to buffers owned by `assertionIter` and
// with random probability mangles any buffers previously returned to the user.
func (i *assertionIter) maybeSaveAndMangleRangeKeyBufs() {
	// If RangeKeyChanged()=false, the pebble.Iterator contract guarantees that
	// any buffers previously returned through RangeBounds() and RangeKeys() are
	// still valid.
	//
	// NB: Only permitted to call RangeKeyChanged() if Valid().
	valid := i.Iterator.Valid()
	if valid && !i.Iterator.RangeKeyChanged() {
		return
	}
	// INVARIANT: !Valid() || RangeKeyChanged()

	// The previous range key buffers are no longer guaranteed to be stable.
	// Randomly zero them to ensure we catch bugs where they're reused.
	if rand.Intn(2) == 0 {
		idx := i.rangeKeyBufs.idx
		for _, b := range [...][]byte{i.rangeKeyBufs.start[idx], i.rangeKeyBufs.end[idx]} {
			for j := range b {
				b[j] = 0
			}
		}
		for _, k := range i.rangeKeyBufs.keys[idx] {
			for _, b := range [...][]byte{k.Suffix, k.Value} {
				for j := range b {
					b[j] = 0
				}
			}
		}
	}
	// If the new iterator position has range keys, copy them to our buffers.
	if !valid {
		return
	}
	if _, hasRange := i.Iterator.HasPointAndRange(); !hasRange {
		return
	}
	if rand.Intn(2) == 0 {
		// Switch to a new buffer for the new range key state.
		i.rangeKeyBufs.idx = (i.rangeKeyBufs.idx + 1) % 2
	}
	start, end := i.Iterator.RangeBounds()
	rangeKeys := i.Iterator.RangeKeys()

	idx := i.rangeKeyBufs.idx
	i.rangeKeyBufs.start[idx] = append(i.rangeKeyBufs.start[idx][:0], start...)
	i.rangeKeyBufs.end[idx] = append(i.rangeKeyBufs.end[idx][:0], end...)
	if len(rangeKeys) > cap(i.rangeKeyBufs.keys[idx]) {
		i.rangeKeyBufs.keys[idx] = make([]pebble.RangeKeyData, len(rangeKeys))
	} else {
		i.rangeKeyBufs.keys[idx] = i.rangeKeyBufs.keys[idx][:len(rangeKeys)]
	}
	for k := range rangeKeys {
		bufKey := &i.rangeKeyBufs.keys[idx][k]
		// Preserve nil-ness returned by Pebble to ensure we're testing exactly
		// what Pebble will return in production.
		if rangeKeys[k].Suffix == nil {
			bufKey.Suffix = nil
		} else {
			if len(rangeKeys[k].Suffix) > 0 {
				bufKey.Suffix = append(bufKey.Suffix[:0], rangeKeys[k].Suffix...)
			} else if bufKey.Suffix == nil {
				bufKey.Suffix = []byte{}
			}
		}
		if rangeKeys[k].Value == nil {
			bufKey.Value = nil
		} else {
			if len(rangeKeys[k].Value) > 0 {
				bufKey.Value = append(bufKey.Value[:0], rangeKeys[k].Value...)
			} else if bufKey.Value == nil {
				bufKey.Value = []byte{}
			}
		}
	}
}
