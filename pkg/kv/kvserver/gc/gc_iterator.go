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

	// Range tombstone timestamp caching to avoid recomputing timestamp for every
	// object covered by current range key.
	cachedRangeTombstoneTS  hlc.Timestamp
	cachedRangeTombstoneKey roachpb.Key
}

func makeGCIterator(
	desc *roachpb.RangeDescriptor,
	snap storage.Reader,
	threshold hlc.Timestamp,
	excludeUserKeySpan bool,
) gcIterator {
	return gcIterator{
		it: rditer.NewReplicaMVCCDataIterator(desc, snap, rditer.ReplicaDataIteratorOptions{
			Reverse:            true,
			IterKind:           storage.MVCCKeyAndIntentsIterKind,
			KeyTypes:           storage.IterKeyTypePointsAndRanges,
			ExcludeUserKeySpan: excludeUserKeySpan,
		}),
		threshold: threshold,
	}
}

type gcIteratorState struct {
	// Sequential elements in iteration order (oldest to newest).
	cur, next, afterNext *storage.MVCCKeyValue
	// Optional timestamp of the first available range tombstone at or below the
	// GC threshold for the cur key.
	firstRangeTombstoneTsAtOrBelowGC hlc.Timestamp
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

func kVString(v *storage.MVCCKeyValue) string {
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
		b.WriteString(kVString(v))
		if !last {
			b.WriteString(", ")
		}
	}
	add(s.cur, false)
	add(s.next, false)
	add(s.afterNext, true)
	if ts := s.firstRangeTombstoneTsAtOrBelowGC; !ts.IsEmpty() {
		b.WriteString(" rts@")
		b.WriteString(ts.String())
	}
	return b.String()
}

// state returns the current state of the iterator. The state contains the
// current and the two following versions of the current key if they exist
// as well as an optional timestamp of the first range tombstone covering
// current key at or below GC threshold.
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
	s.cur, s.firstRangeTombstoneTsAtOrBelowGC, ok = it.peekAt(0)
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

// peekAt returns key value and a ts of first range tombstone less or equal
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
			ts := hlc.Timestamp{}
			if hasRange {
				ts = it.currentRangeTS()
			}
			it.buf.pushBack(it.it.UnsafeKey(), it.it.UnsafeValue(), ts)
		}
		it.it.Prev()
	}
	return true
}

// currentRangeTS returns timestamp of the first range tombstone at or below
// gc threshold for current key. it also updates cached value to avoid
// recomputation for every key and version by checking current range bounds
// start key.
// Note: should only be called if HasPointAndRange() indicated that we have
// a range key.
func (it *gcIterator) currentRangeTS() hlc.Timestamp {
	rangeTombstoneStartKey := it.it.RangeBounds().Key
	if rangeTombstoneStartKey.Equal(it.cachedRangeTombstoneKey) {
		return it.cachedRangeTombstoneTS
	}
	it.cachedRangeTombstoneKey = append(it.cachedRangeTombstoneKey[:0], rangeTombstoneStartKey...)

	if v, ok := it.it.RangeKeys().FirstAtOrBelow(it.threshold); ok {
		it.cachedRangeTombstoneTS = v.Timestamp
	} else {
		it.cachedRangeTombstoneTS = hlc.Timestamp{}
	}
	return it.cachedRangeTombstoneTS
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
	// contain ts of first range at or below gc threshold. Otherwise, it'll be an
	// empty timestamp.
	firstRangeTombstoneAtOrBelowGCTss [gcIteratorRingBufSize]hlc.Timestamp
	len                               int
	head                              int
}

func (b *gcIteratorRingBuf) String() string {
	sb := strings.Builder{}
	ptr := b.head
	for i := 0; i < b.len; i++ {
		sb.WriteString(kVString(&b.buf[ptr]))
		if ts := b.firstRangeTombstoneAtOrBelowGCTss[ptr]; !ts.IsEmpty() {
			sb.WriteString(" trs@")
			sb.WriteString(b.firstRangeTombstoneAtOrBelowGCTss[ptr].String())
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
	return &b.buf[idx], b.firstRangeTombstoneAtOrBelowGCTss[idx]
}

func (b *gcIteratorRingBuf) removeFront() {
	if b.len == 0 {
		panic("cannot remove from empty gcIteratorRingBuf")
	}
	b.buf[b.head] = storage.MVCCKeyValue{}
	b.firstRangeTombstoneAtOrBelowGCTss[b.head] = hlc.Timestamp{}
	b.head = (b.head + 1) % gcIteratorRingBufSize
	b.len--
}

func (b *gcIteratorRingBuf) pushBack(k storage.MVCCKey, v []byte, rangeTS hlc.Timestamp) {
	if b.len == gcIteratorRingBufSize {
		panic("cannot add to full gcIteratorRingBuf")
	}
	i := (b.head + b.len) % gcIteratorRingBufSize
	b.allocs[i] = b.allocs[i].Truncate()
	b.allocs[i], k.Key = b.allocs[i].Copy(k.Key, len(v))
	b.allocs[i], v = b.allocs[i].Copy(v, 0)
	b.buf[i] = storage.MVCCKeyValue{
		Key:   k,
		Value: v,
	}
	b.firstRangeTombstoneAtOrBelowGCTss[i] = rangeTS
	b.len++
}
