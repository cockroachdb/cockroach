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
		panic(errors.AssertionFailedf("Value() called on !Valid() pebble.Iterator"))
	}
	return i.Iterator.LazyValue()
}

func (i *assertionIter) First() bool {
	i.mangleBufs()
	return i.Iterator.First()
}

func (i *assertionIter) SeekGE(key []byte) bool {
	i.mangleBufs()
	return i.Iterator.SeekGE(key)
}

func (i *assertionIter) SeekGEWithLimit(key []byte, limit []byte) pebble.IterValidityState {
	i.mangleBufs()
	return i.Iterator.SeekGEWithLimit(key, limit)
}

func (i *assertionIter) SeekPrefixGE(key []byte) bool {
	i.mangleBufs()
	return i.Iterator.SeekPrefixGE(key)
}

func (i *assertionIter) Next() bool {
	i.mangleBufs()
	return i.Iterator.Next()
}

func (i *assertionIter) NextWithLimit(limit []byte) pebble.IterValidityState {
	i.mangleBufs()
	return i.Iterator.NextWithLimit(limit)
}

func (i *assertionIter) NextPrefix() bool {
	i.mangleBufs()
	return i.Iterator.NextPrefix()
}

func (i *assertionIter) Last() bool {
	i.mangleBufs()
	return i.Iterator.Last()
}

func (i *assertionIter) SeekLT(key []byte) bool {
	i.mangleBufs()
	return i.Iterator.SeekLT(key)
}

func (i *assertionIter) SeekLTWithLimit(key []byte, limit []byte) pebble.IterValidityState {
	i.mangleBufs()
	return i.Iterator.SeekLTWithLimit(key, limit)
}

func (i *assertionIter) Prev() bool {
	i.mangleBufs()
	return i.Iterator.Prev()
}

func (i *assertionIter) PrevWithLimit(limit []byte) pebble.IterValidityState {
	i.mangleBufs()
	return i.Iterator.PrevWithLimit(limit)
}

// mangleBufs trashes the contents of buffers used to return unsafe values to
// the caller. This is used to ensure that the client respects the Pebble
// iterator interface and the lifetimes of buffers it returns.
func (i *assertionIter) mangleBufs() {
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
