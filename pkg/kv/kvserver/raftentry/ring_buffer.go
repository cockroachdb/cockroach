// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raftentry

import (
	"math/bits"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// ringBuf is a ring buffer of raft entries.
type ringBuf struct {
	buf  []raftpb.Entry
	head int
	len  int
}

const (
	shrinkThreshold = 8  // shrink buf if len(buf)/len is above this.
	minBufSize      = 16 /* entries */
)

// add adds ents to the ringBuf keeping track of how much was actually added
// given that ents may overlap with existing entries or may be rejected from
// the buffer. ents must not be empty.
func (b *ringBuf) add(ents []raftpb.Entry) (addedBytes, addedEntries int32) {
	if it := last(b); it.valid(b) && ents[0].Index > it.index(b)+1 {
		// If ents is non-contiguous and later than the currently cached range then
		// remove the current entries and add ents in their place.
		removedBytes, removedEntries := b.clearTo(it.index(b) + 1)
		addedBytes, addedEntries = -1*removedBytes, -1*removedEntries
	}
	before, after, ok := computeExtension(b, ents[0].Index, ents[len(ents)-1].Index)
	if !ok {
		return
	}
	extend(b, before, after)
	it := first(b)
	if before == 0 && after != b.len { // skip unchanged prefix
		it, _ = iterateFrom(b, ents[0].Index) // safe by construction
	}
	firstNewAfter := len(ents) - after
	for i, e := range ents {
		if i < before || i >= firstNewAfter {
			addedEntries++
			addedBytes += int32(e.Size())
		} else {
			addedBytes += int32(e.Size() - it.entry(b).Size())
		}
		it = it.push(b, e)
	}
	return
}

// truncateFrom clears all entries from the ringBuf with index equal to or
// greater than lo. The method returns the aggregate size and count of entries
// removed. Note that lo itself may or may not be in the cache.
func (b *ringBuf) truncateFrom(lo uint64) (removedBytes, removedEntries int32) {
	if b.len == 0 {
		return
	}
	if idx := first(b).index(b); idx > lo {
		// If `lo` precedes the indexes in the buffer
		// (say the buf is idx=[100, 101, 102] and `lo` is 99),
		// `iterateFrom` will return an invalid iter. But we
		// need to truncate everything and so advance to the
		// first index before constructing the iterator.
		lo = idx
	}
	it, ok := iterateFrom(b, lo)
	for ok {
		removedBytes += int32(it.entry(b).Size())
		removedEntries++
		it.clear(b)
		it, ok = it.next(b)
	}
	b.len -= int(removedEntries)
	if b.len < (len(b.buf) / shrinkThreshold) {
		realloc(b, 0, b.len)
	}
	if util.RaceEnabled {
		if b.len > 0 {
			if lastIdx := last(b).index(b); lastIdx >= lo {
				panic(errors.AssertionFailedf(
					"buffer truncated to [..., %d], but current last index is %d",
					lo, lastIdx,
				))
			}
		}
	}
	return removedBytes, removedEntries
}

// clearTo clears all entries from the ringBuf with index less than hi. The
// method returns the aggregate size and count of entries removed.
func (b *ringBuf) clearTo(hi uint64) (removedBytes, removedEntries int32) {
	if b.len == 0 || hi < first(b).index(b) {
		return
	}
	it := first(b)
	ok := it.valid(b) // true
	firstIndex := it.index(b)
	for ok && it.index(b) < hi {
		removedBytes += int32(it.entry(b).Size())
		removedEntries++
		it.clear(b)
		it, ok = it.next(b)
	}
	offset := int(hi - firstIndex)
	if offset > b.len {
		offset = b.len
	}
	b.len = b.len - offset
	b.head = (b.head + offset) % len(b.buf)
	if b.len < (len(b.buf) / shrinkThreshold) {
		realloc(b, 0, b.len)
	}
	return
}

func (b *ringBuf) get(index uint64) (e raftpb.Entry, ok bool) {
	it, ok := iterateFrom(b, index)
	if !ok {
		return e, ok
	}
	return *it.entry(b), ok
}

func (b *ringBuf) scan(
	ents []raftpb.Entry, lo, hi, maxBytes uint64,
) (_ []raftpb.Entry, bytes uint64, nextIdx uint64, exceededMaxBytes bool) {
	var it iterator
	nextIdx = lo
	it, ok := iterateFrom(b, lo)
	for ok && !exceededMaxBytes && it.index(b) < hi {
		e := it.entry(b)
		s := uint64(e.Size())
		exceededMaxBytes = bytes+s > maxBytes
		if exceededMaxBytes && len(ents) > 0 {
			break
		}
		bytes += s
		ents = append(ents, *e)
		nextIdx++
		it, ok = it.next(b)
	}
	return ents, bytes, nextIdx, exceededMaxBytes
}

// reallocs b.buf into a new buffer of newSize leaving before zero value entries
// at the front of b.
func realloc(b *ringBuf, before, newLen int) {
	newBuf := make([]raftpb.Entry, reallocLen(newLen))
	if b.head+b.len > len(b.buf) {
		n := copy(newBuf[before:], b.buf[b.head:])
		copy(newBuf[before+n:], b.buf[:(b.head+b.len)%len(b.buf)])
	} else {
		copy(newBuf[before:], b.buf[b.head:b.head+b.len])
	}
	b.buf = newBuf
	b.head = 0
	b.len = newLen
}

// reallocLen returns a new length which is a power-of-two greater than or equal
// to need and at least minBufSize.
func reallocLen(need int) (newLen int) {
	if need <= minBufSize {
		return minBufSize
	}
	return 1 << uint(bits.Len(uint(need)))
}

// extend takes a number of entries before and after the current cached values
// to increase the length of b. The before-length prefix of b will now be zero
// valued entries.
func extend(b *ringBuf, before, after int) {
	size := before + b.len + after
	if size > len(b.buf) {
		realloc(b, before, size)
	} else {
		b.head = (b.head - before) % len(b.buf)
		if b.head < 0 {
			b.head += len(b.buf)
		}
	}
	b.len = size
}

// computeExtension returns the number of entries in [lo, hi] which will be
// added before and after the current range of the cache. Note that lo and hi
// here are inclusive indices for the range being added and that before and
// after are counts, not indices, of number of entries which precede and follow
// the currently cached range. If [lo, hi] is not overlapping or directly
// adjacent to the current cache bounds, ok will be false.
func computeExtension(b *ringBuf, lo, hi uint64) (before, after int, ok bool) {
	if b.len == 0 {
		return 0, int(hi) - int(lo) + 1, true
	}
	first, last := first(b).index(b), last(b).index(b)
	if lo > (last+1) || hi < (first-1) { // gap case
		return 0, 0, false
	}
	if lo < first {
		before = int(first) - int(lo)
	}
	if hi > last {
		after = int(hi) - int(last)
	}
	return before, after, true
}

// iterator indexes into a ringBuf. A value of -1 is not valid.
type iterator int

func iterateFrom(b *ringBuf, index uint64) (_ iterator, ok bool) {
	if b.len == 0 {
		return -1, false
	}
	offset := int(index) - int(first(b).index(b))
	if offset < 0 || offset >= b.len {
		return -1, false
	}
	return iterator((b.head + offset) % len(b.buf)), true
}

// first returns an iterator pointing to the first entry of the ringBuf.
// If b is empty, the returned iterator is not valid.
func first(b *ringBuf) iterator {
	if b.len == 0 {
		return iterator(-1)
	}
	return iterator(b.head)
}

// last returns an iterator pointing to the last element in b.
// If b is empty, the returned iterator is not valid.
func last(b *ringBuf) iterator {
	if b.len == 0 {
		return iterator(-1)
	}
	return iterator((b.head + b.len - 1) % len(b.buf))
}

func (it iterator) valid(b *ringBuf) bool {
	return it >= 0 && int(it) < len(b.buf)
}

// index returns the index of the entry at iterator's curent position.
func (it iterator) index(b *ringBuf) uint64 {
	return b.buf[it].Index
}

// entry returns the entry at iterator's curent position.
func (it iterator) entry(b *ringBuf) *raftpb.Entry {
	return &b.buf[it]
}

// clear zeroes the current value in b.
func (it iterator) clear(b *ringBuf) {
	b.buf[it] = raftpb.Entry{}
}

// next returns an iterator which points to the next element in b.
// If it is invalid or points to the last element in b, (-1, false) is returned.
func (it iterator) next(b *ringBuf) (_ iterator, ok bool) {
	if !it.valid(b) || it == last(b) {
		return -1, false
	}
	return iterator(int(it+1) % len(b.buf)), true
}

// push sets the iterator's current value in b to e and calls next
// It is the caller's responsibility to ensure that b has space for the new
// entry.
func (it iterator) push(b *ringBuf, e raftpb.Entry) iterator {
	b.buf[it] = e
	it, _ = it.next(b)
	return it
}
