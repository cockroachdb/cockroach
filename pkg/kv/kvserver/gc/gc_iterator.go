// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gc

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// gcIterator wraps an rditer.ReplicaMVCCDataIterator which it reverse iterates for
// the purpose of discovering gc-able replicated data.
type gcIterator struct {
	it        storage.MVCCIterator
	threshold hlc.Timestamp
	err       error
	buf       gcIteratorRingBuf

	// Range tombstone timestamp caching to avoid recomputing timestamp for every
	// object covered by current range key.
	cachedRangeTombstoneTS  hlc.Timestamp
	cachedRangeTombstoneKey roachpb.Key
}

// TODO(sumeer): change gcIterator to use MVCCValueLenAndIsTombstone(). It
// needs to get the value only for intents.

func makeGCIterator(iter storage.MVCCIterator, threshold hlc.Timestamp) gcIterator {
	return gcIterator{
		it:        iter,
		threshold: threshold,
	}
}

type gcIteratorState struct {
	// Sequential elements in iteration order (oldest to newest).
	cur, next, afterNext *mvccKeyValue
	// Optional timestamp of the first available range tombstone at or below the
	// GC threshold for the cur key.
	firstRangeTombstoneTsAtOrBelowGC hlc.Timestamp
}

// curIsNewest returns true if the current MVCCKeyValue in the gcIteratorState
// is the newest committed version of the key.
//
// It returns true if next is nil or if next is an intent.
func (s *gcIteratorState) curIsNewest() bool {
	return s.cur.key.IsValue() &&
		(s.next == nil || (s.afterNext != nil && !s.afterNext.key.IsValue()))
}

// True if we are positioned on newest version when there's no intent or on
// intent.
func (s *gcIteratorState) curLastKeyVersion() bool {
	return s.next == nil
}

// curIsNotValue returns true if the current MVCCKeyValue in the gcIteratorState
// is not a value, i.e. does not have a timestamp.
func (s *gcIteratorState) curIsNotValue() bool {
	return !s.cur.key.IsValue()
}

// curIsIntent returns true if the current MVCCKeyValue in the gcIteratorState
// is an intent.
func (s *gcIteratorState) curIsIntent() bool {
	return s.next != nil && !s.next.key.IsValue()
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
	if !ok || !next.key.Key.Equal(s.cur.key.Key) {
		return s, true
	}
	s.next = next
	afterNext, _, ok := it.peekAt(2)
	if !ok && it.err != nil { // cur is the first key in the range
		return gcIteratorState{}, false
	}
	if !ok || !afterNext.key.Key.Equal(s.cur.key.Key) {
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
func (it *gcIterator) peekAt(i int) (*mvccKeyValue, hlc.Timestamp, bool) {
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
			it.err = err
			return false
		}
		if hasPoint, hasRange := it.it.HasPointAndRange(); hasPoint {
			ts := hlc.Timestamp{}
			if hasRange {
				ts = it.currentRangeTS()
			}
			key := it.it.UnsafeKey()
			var mvccValueLen int
			var mvccValueIsTombstone bool
			var metaValue []byte
			if key.IsValue() {
				var err error
				mvccValueLen, mvccValueIsTombstone, err = it.it.MVCCValueLenAndIsTombstone()
				if err != nil {
					it.err = err
					return false
				}
			} else {
				var err error
				metaValue, err = it.it.UnsafeValue()
				if err != nil {
					it.err = err
					return false
				}
			}
			it.buf.pushBack(key, mvccValueLen, mvccValueIsTombstone, metaValue, ts)
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

// gcIteratorRingBufSize is 3 because the gcIterator.state method at most needs
// to look forward two keys ahead of the current key.
const gcIteratorRingBufSize = 3

type mvccKeyValue struct {
	// If key.IsValue(), mvccValueLen and mvccValueIsTombstone are populated,
	// else, metaValue is populated.
	key                  storage.MVCCKey
	mvccValueLen         int
	mvccValueIsTombstone bool
	metaValue            []byte
}

type gcIteratorRingBuf struct {
	allocs [gcIteratorRingBufSize]bufalloc.ByteAllocator
	buf    [gcIteratorRingBufSize]mvccKeyValue
	// If there are any range tombstones available for the key, this buffer will
	// contain ts of first range at or below gc threshold. Otherwise, it'll be an
	// empty timestamp.
	firstRangeTombstoneAtOrBelowGCTss [gcIteratorRingBufSize]hlc.Timestamp
	len                               int
	head                              int
}

func (b *gcIteratorRingBuf) at(i int) (*mvccKeyValue, hlc.Timestamp) {
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
	b.buf[b.head] = mvccKeyValue{}
	b.firstRangeTombstoneAtOrBelowGCTss[b.head] = hlc.Timestamp{}
	b.head = (b.head + 1) % gcIteratorRingBufSize
	b.len--
}

func (b *gcIteratorRingBuf) pushBack(
	k storage.MVCCKey,
	mvccValueLen int,
	mvccValueIsTombstone bool,
	metaValue []byte,
	rangeTS hlc.Timestamp,
) {
	if b.len == gcIteratorRingBufSize {
		panic("cannot add to full gcIteratorRingBuf")
	}
	i := (b.head + b.len) % gcIteratorRingBufSize
	b.allocs[i] = b.allocs[i].Truncate()
	b.allocs[i], k.Key = b.allocs[i].Copy(k.Key, len(metaValue))
	if len(metaValue) > 0 {
		b.allocs[i], metaValue = b.allocs[i].Copy(metaValue, 0)
	}
	b.buf[i] = mvccKeyValue{
		key:                  k,
		mvccValueLen:         mvccValueLen,
		mvccValueIsTombstone: mvccValueIsTombstone,
		metaValue:            metaValue,
	}
	b.firstRangeTombstoneAtOrBelowGCTss[i] = rangeTS
	b.len++
}
