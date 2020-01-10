// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
)

// gcIterator wraps an rditer.ReplicaDataIterator which it reverse iterates for
// the purpose of discovering gc-able replicated data.
type gcIterator struct {
	it   *rditer.ReplicaDataIterator
	done bool
	err  error
	buf  gcIteratorRingBuf
}

func makeGCIterator(desc *roachpb.RangeDescriptor, snap engine.Reader) gcIterator {
	return gcIterator{
		it: rditer.NewReplicaDataIterator(desc, snap,
			true /* replicatedOnly */, true /* seekEnd */),
	}
}

// state returns the current state of the iterator. It will return a non-nil
// next if another version of the same key exists. If there is an intent on this
// key then it will be the case that isNewest is true and next is non-nil. In
// this case, next is the value of an intent.
func (it *gcIterator) state() (
	cur, next *engine.MVCCKeyValue,
	isNewest, isIntent, ok bool,
	err error,
) {
	// The current key is the newest if the key which comes next is different or
	// the key which comes after the current key is an intent or this is the first
	// key in the range.
	cur, ok = it.peekAt(0)
	if !ok {
		return nil, nil, false, false, false, it.err
	}
	next, ok = it.peekAt(1)
	if !ok {
		if it.done { // cur is the first key in the range
			return cur, nil, true, false, true, nil
		}
		return nil, nil, false, false, false, it.err
	}
	// The next MVCCKey is for a different key, the current key is the newest.
	if !next.Key.Key.Equal(cur.Key.Key) {
		return cur, nil, true, false, true, nil
	}
	// The next MVCCKey is metadata, the current key is an intent.
	if !next.Key.IsValue() {
		return cur, next, false, true, true, nil
	}
	// We can't read the MVCCKey afterNext, if we're done then we know that the
	// next key is the newest and the current key is not. Otherwise, we hit an
	// error.
	afterNext, ok := it.peekAt(2)
	if !ok {
		if it.done {
			return cur, next, false, false, true, nil
		}
		return nil, nil, false, false, false, it.err
	}
	// If afterNext is metadata, then next is an intent and the current key is
	// the newest.
	if afterNext.Key.Key.Equal(cur.Key.Key) && !afterNext.Key.IsValue() {
		return cur, next, true, false, true, nil
	}
	return cur, next, false, false, true, nil
}

func (it *gcIterator) step() {
	it.buf.removeFront()
}

func (it *gcIterator) peekAt(i int) (*engine.MVCCKeyValue, bool) {
	if it.buf.len <= i {
		if !it.fillTo(i + 1) {
			return nil, false
		}
	}
	return it.buf.at(i), true
}

func (it *gcIterator) fillTo(targetLen int) (ok bool) {
	for it.buf.len < targetLen {
		if ok, err := it.it.Valid(); !ok {
			it.err, it.done = err, err == nil
			return false
		}
		it.buf.pushBack(it.it.KeyValue())
		it.it.ResetAllocator()
		it.it.Prev()
	}
	return true
}

func (it *gcIterator) close() {
	it.it.Close()
	it.it = nil
}

// gcIteratorRingBufSize is 3 because the gcIterator.state method at most needs
// to look forward two keys ahead of the current key.
const gcIteratorRingBufSize = 3

type gcIteratorRingBuf struct {
	buf  [gcIteratorRingBufSize]engine.MVCCKeyValue
	len  int
	head int
}

func (b *gcIteratorRingBuf) at(i int) *engine.MVCCKeyValue {
	if i >= b.len {
		panic("index out of range")
	}
	return &b.buf[(b.head+i)%gcIteratorRingBufSize]
}

func (b *gcIteratorRingBuf) removeFront() {
	if b.len == 0 {
		panic("cannot remove from empty gcIteratorRingBuf")
	}
	b.buf[b.head] = engine.MVCCKeyValue{}
	b.head = (b.head + 1) % gcIteratorRingBufSize
	b.len--
}

func (b *gcIteratorRingBuf) pushBack(kv engine.MVCCKeyValue) {
	if b.len == gcIteratorRingBufSize {
		panic("cannot add to full gcIteratorRingBuf")
	}
	b.buf[(b.head+b.len)%gcIteratorRingBufSize] = kv
	b.len++
}
