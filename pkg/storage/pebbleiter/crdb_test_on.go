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

import "github.com/cockroachdb/pebble"

// Iterator wraps the *pebble.Iterator in crdb_test builds with an AssertionIter
// that detects when Close is called on the iterator twice.  Double closes are
// problematic because they can result in an iterator being added to a sync pool
// twice, allowing concurrent use of the same iterator struct.
type Iterator = *AssertionIter

// MaybeWrap returns the provided Pebble iterator, wrapped with double close
// detection.
func MaybeWrap(iter *pebble.Iterator) Iterator {
	return &AssertionIter{Iterator: iter}
}

// AssertionIter wraps a *pebble.Iterator with assertion checking.
type AssertionIter struct {
	*pebble.Iterator
	closed bool
}

func (i *AssertionIter) Clone(cloneOpts pebble.CloneOptions) (Iterator, error) {
	iter, err := i.Iterator.Clone(cloneOpts)
	if err != nil {
		return nil, err
	}
	return MaybeWrap(iter), nil
}

func (i *AssertionIter) Close() error {
	if i.closed {
		panic("pebble.Iterator already closed")
	}
	i.closed = true
	return i.Iterator.Close()
}
