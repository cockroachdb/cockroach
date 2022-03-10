// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gc

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// gcIterator wraps an rditer.ReplicaMVCCDataIterator which it reverse iterates for
// the purpose of discovering gc-able replicated data.
type gcIterator struct {
	it        *rditer.ReplicaMVCCDataIterator
	threshold hlc.Timestamp
	done      bool
	err       error
	buf       gcIteratorRingBuf

	// range tombstone timestamp caching
	cachedRangeTombstoneTS hlc.Timestamp
	keyAlloc               bufalloc.ByteAllocator
	tsCachedKey            roachpb.Key
}

func makeGCIterator(
	desc *roachpb.RangeDescriptor, snap storage.Reader, threshold hlc.Timestamp,
) gcIterator {
	return gcIterator{
		it:        rditer.NewReplicaMVCCDataIterator(desc, snap, true /* seekEnd */),
		threshold: threshold,
	}
}

// We can do x things:
//  - synthesize delete points in ringbuf
//     need to track which objects are real and which are "synthetic"
//     no logic above that changes except for we don't delete synthetic ones,
//     we need to combine object history with range history to produce ringbuf
//  - synthesize delete points in state from ranges for next and beyond
//     all logic goes into state creation only
//  - keep range tombstone in state and compare directly with it
//     logic goes into state creation and isGarbage, ranges bubble up to main loop as we need to
//     pass them on (alternatively we only need timestamp for current key)
//     what is waste? do we need to scan range keys on every iteration?
//     we should only do that if we change key and we are only interested in
//     tombstone at or below threshold.

type gcIteratorState struct {
	// sequential elements in iteration order (newest to oldest)
	cur, next, afterNext *storage.MVCCKeyValue
	// first available range tombstone greater or equal than threshold
	// for the first key
	lastTombstone hlc.Timestamp
}

// curIsNewest returns true if the current MVCCKeyValue in the gcIteratorState
// is the newest committed version of the key.
//
// It returns true if next is nil or if next is an intent.
func (s *gcIteratorState) curIsNewest() bool {
	return s.cur.Key.IsValue() &&
		(s.next == nil || (s.afterNext != nil && !s.afterNext.Key.IsValue()))
}

// curIsNotValue returns true if the current MVCCKeyValue in the gcIteratorState
// is not a value, i.e. does not have a timestamp.
func (s *gcIteratorState) curIsNotValue() bool {
	return !s.cur.Key.IsValue()
}

// curIsIntent returns true if the current MVCCKeyValue in the gcIteratorState
// is an intent.
func (s *gcIteratorState) curIsIntent() bool {
	return s.next != nil && !s.next.Key.IsValue()
}

func KVString(v *storage.MVCCKeyValue) string {
	b := strings.Builder{}
	if v != nil {
		b.WriteString(v.Key.String())
		if len(v.Value) == 0 {
			b.WriteString(" del")
		}
	} else {
		b.WriteString("<nil>")
	}
	return b.String()
}

// String implements Stringer for debugging purposes.
func (s *gcIteratorState) String() string {
	b := strings.Builder{}
	add := func(v *storage.MVCCKeyValue, last bool) {
		b.WriteString(KVString(v))
		if !last {
			b.WriteString(", ")
		}
	}
	add(s.cur, false)
	add(s.next, false)
	add(s.afterNext, true)
	if ts := s.lastTombstone; !ts.IsEmpty() {
		b.WriteString(" rts@")
		b.WriteString(ts.String())
	}
	return b.String()
}

// state returns the current state of the iterator. The state contains the
// current and the two following versions of the current key if they exist.
//
// If ok is false, further iteration is unsafe; either the end of iteration has
// been reached or an error has occurred. Callers should check it.err to
// determine whether an error has occurred in cases where ok is false.
//
// It is not safe to use values in the state after subsequent calls to
// it.step().
func (it *gcIterator) state() (s gcIteratorState, ok bool) {
	// The current key is the newest if the key which comes next is different or
	// the key which comes after the current key is an intent or this is the first
	// key in the range.
	s.cur, s.lastTombstone, ok = it.peekAt(0)
	if !ok {
		return gcIteratorState{}, false
	}
	next, _, ok := it.peekAt(1)
	if !ok && it.err != nil { // cur is the first key in the range
		return gcIteratorState{}, false
	}
	if !ok || !next.Key.Key.Equal(s.cur.Key.Key) {
		return s, true
	}
	s.next = next
	afterNext, _, ok := it.peekAt(2)
	if !ok && it.err != nil { // cur is the first key in the range
		return gcIteratorState{}, false
	}
	if !ok || !afterNext.Key.Key.Equal(s.cur.Key.Key) {
		return s, true
	}
	s.afterNext = afterNext
	return s, true
}

func (it *gcIterator) step() {
	it.buf.removeFront()
}

// peekAt returns key value and a ts of first range tombstone greater or equal
// to gc threshold.
func (it *gcIterator) peekAt(i int) (*storage.MVCCKeyValue, hlc.Timestamp, bool) {
	if it.buf.len <= i {
		if !it.fillTo(i + 1) {
			return nil, hlc.Timestamp{}, false
		}
	}
	kv, rangeTs := it.buf.at(i)
	return kv, rangeTs, true
}

func (it *gcIterator) fillTo(targetLen int) (ok bool) {
	for it.buf.len < targetLen {
		if ok, err := it.it.Valid(); !ok {
			it.err, it.done = err, err == nil
			return false
		}
		if hasPoint, hasRange := it.it.HasPointAndRange(); hasPoint {
			// We only want to handle ranges once per key. How could we check that
			// we can't reuse previous value?
			ts := hlc.Timestamp{}
			if hasRange {
				ts = it.currentRangeTS()
			}
			it.buf.pushBack(it.it, ts)
		}
		it.it.Prev()
	}
	return true
}

// currentRangeTS returns timestamp of the first range tombstone at or below
// gc threshold for current key. it also updates cached value to avoid
// recomputation for every version.
func (it *gcIterator) currentRangeTS() (ts hlc.Timestamp) {
	currentKey := it.it.UnsafeKey().Key
	if currentKey.Equal(it.tsCachedKey) {
		return it.cachedRangeTombstoneTS
	}

	ts = hlc.Timestamp{}
	rangeKeys := it.it.RangeKeys()
	for i := len(rangeKeys) - 1; i >= 0; i-- {
		if it.threshold.Less(rangeKeys[i].Timestamp) {
			break
		}
		ts = rangeKeys[i].Timestamp
	}
	it.cachedRangeTombstoneTS = ts
	it.keyAlloc = it.keyAlloc.Truncate()
	if !ts.IsEmpty() {
		it.keyAlloc, it.tsCachedKey = it.keyAlloc.Copy(currentKey, 0)
	}
	return ts
}

func (it *gcIterator) close() {
	it.it.Close()
	it.it = nil
}

// gcIteratorRingBufSize is 3 because the gcIterator.state method at most needs
// to look forward two keys ahead of the current key.
const gcIteratorRingBufSize = 3

type gcIteratorRingBuf struct {
	allocs [gcIteratorRingBufSize]bufalloc.ByteAllocator
	buf    [gcIteratorRingBufSize]storage.MVCCKeyValue
	// If there are any range tombstones available for the key, this buffer will
	// contain ts of first range at or below gc threshold.
	liveRTSTimestamp [gcIteratorRingBufSize]hlc.Timestamp
	len              int
	head             int
}

func (b *gcIteratorRingBuf) String() string {
	sb := strings.Builder{}
	ptr := b.head
	for i := 0; i < b.len; i++ {
		sb.WriteString(KVString(&b.buf[ptr]))
		if ts := b.liveRTSTimestamp[ptr]; !ts.IsEmpty() {
			sb.WriteString(" trs@")
			sb.WriteString(b.liveRTSTimestamp[ptr].String())
		}
		if i < b.len-1 {
			sb.WriteString(", ")
		}
		ptr = (ptr + 1) % gcIteratorRingBufSize
	}
	return sb.String()
}

func (b *gcIteratorRingBuf) at(i int) (*storage.MVCCKeyValue, hlc.Timestamp) {
	if i >= b.len {
		panic("index out of range")
	}
	idx := (b.head + i) % gcIteratorRingBufSize
	return &b.buf[idx], b.liveRTSTimestamp[idx]
}

func (b *gcIteratorRingBuf) removeFront() {
	if b.len == 0 {
		panic("cannot remove from empty gcIteratorRingBuf")
	}
	b.buf[b.head] = storage.MVCCKeyValue{}
	b.liveRTSTimestamp[b.head] = hlc.Timestamp{}
	b.head = (b.head + 1) % gcIteratorRingBufSize
	b.len--
}

type iterator interface {
	UnsafeKey() storage.MVCCKey
	UnsafeValue() []byte
	RangeKeys() []storage.MVCCRangeKey
}

func (b *gcIteratorRingBuf) pushBack(it iterator, rangeTS hlc.Timestamp) {
	if b.len == gcIteratorRingBufSize {
		panic("cannot add to full gcIteratorRingBuf")
	}
	i := (b.head + b.len) % gcIteratorRingBufSize
	b.allocs[i] = b.allocs[i].Truncate()
	k := it.UnsafeKey()
	v := it.UnsafeValue()
	b.allocs[i], k.Key = b.allocs[i].Copy(k.Key, len(v))
	b.allocs[i], v = b.allocs[i].Copy(v, 0)
	b.buf[i] = storage.MVCCKeyValue{
		Key:   k,
		Value: v,
	}
	b.liveRTSTimestamp[i] = rangeTS
	b.len++
}
