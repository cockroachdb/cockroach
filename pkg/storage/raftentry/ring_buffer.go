// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package raftentry

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"go.etcd.io/etcd/raft/raftpb"
)

const (
	shrinkThreshold = 8
	shrinkFactor    = 4
	growthFactor    = 2
)

// ringBuf is a ring buffer of raft entries.
type ringBuf struct {
	buf  []raftpb.Entry
	head int
	len  int
}

// add adds ents to the ringBuf keeping track of how much was actually added
// given that ents may overlap with existing entries or may be rejected from
// the buffer.
// ents must not be empty.
func (b *ringBuf) add(ents []raftpb.Entry) (addedBytes, addedEntries int) {
	before, after, ok := computeExtension(b, ents[0].Index, ents[len(ents)-1].Index)
	if !ok {
		return
	}
	extend(b, before, after)
	it := first(b)
	if before == 0 && after != b.len {
		it, _ = iterateFrom(b, ents[0].Index) // safe by construction
	}
	firstNewAfter := len(ents) - after
	for i, e := range ents {
		if i < before || i >= firstNewAfter {
			addedEntries++
			addedBytes += e.Size()
		} else {
			addedBytes += e.Size() - it.entry(b).Size()
		}
		it.push(b, e)
	}
	return
}

func (b *ringBuf) clear(hi uint64) (bytesRemoved, entriesRemoved int) {
	if b.len == 0 || hi < first(b).index(b) {
		return
	}
	it, ok := first(b), true
	firstIndex := int(it.index(b))
	for ok && it.index(b) < hi {
		bytesRemoved += it.entry(b).Size()
		entriesRemoved++
		it.clear(b)
		ok = it.next(b)
	}
	offset := int(hi) - firstIndex
	if offset > b.len {
		offset = b.len
	}
	b.len = b.len - offset
	b.head = (b.head + offset) % len(b.buf)
	if b.len > 0 {
		if b.len < (len(b.buf) / shrinkThreshold) {
			realloc(b, 0, make([]raftpb.Entry, len(b.buf)/shrinkFactor))
		}
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
	ents []raftpb.Entry, id roachpb.RangeID, lo, hi, maxBytes uint64,
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
		ok = it.next(b)
	}
	return ents, bytes, nextIdx, exceededMaxBytes
}

func (b *ringBuf) length() int {
	return b.len
}

// realloc copies the data from the current slice into new buf leaving
// before buf untouched
func realloc(b *ringBuf, before int, newBuf []raftpb.Entry) {
	if b.head+b.len > len(b.buf) {
		n := copy(newBuf[before:], b.buf[b.head:])
		copy(newBuf[before+n:], b.buf[:(b.head+b.len)%len(b.buf)])
	} else {
		copy(newBuf[before:], b.buf[b.head:b.head+b.len])
	}
	b.buf = newBuf
	b.head = 0
}

func extend(b *ringBuf, before, after int) {
	size := before + b.len + after
	if size > len(b.buf) {
		realloc(b, before, make([]raftpb.Entry, growthFactor*size))
	} else {
		b.head = (b.head - before) % len(b.buf)
		if b.head < 0 {
			b.head += len(b.buf)
		}
	}
	b.len = size
}

func computeExtension(b *ringBuf, lo, hi uint64) (before, after int, ok bool) {
	if b.len == 0 {
		return 0, int(hi) - int(lo) + 1, true
	}
	first, last := first(b).index(b), last(b).index(b)
	if lo > (last+1) || hi < (first-1) {
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

// iterator is an index into a ringBuf
type iterator int

func iterateFrom(b *ringBuf, index uint64) (_ iterator, ok bool) {
	offset := int(index) - int(first(b).index(b))
	if offset < 0 || offset >= b.len {
		return -1, false
	}
	return iterator((b.head + offset) % len(b.buf)), true
}

func first(b *ringBuf) iterator {
	return iterator(b.head)
}

// unsafe to call on empty buffer
func last(b *ringBuf) iterator {
	return iterator((b.head + b.len - 1) % len(b.buf))
}

// unsafe to call on empty buffer
func (it *iterator) next(b *ringBuf) (ok bool) {
	if *it == last(b) || len(b.buf) == 0 {
		return false
	}
	*it = iterator(int(*it+1) % len(b.buf))
	return true
}

// push sets the iterator's current value in b to e and calls next
func (it *iterator) push(b *ringBuf, e raftpb.Entry) {
	b.buf[*it] = e
	it.next(b)
}

// index returns the index of the entry at iterator's
// curent position
func (it iterator) index(b *ringBuf) uint64 {
	return b.buf[it].Index
}

// index returns the entry at iterator's curent position
func (it iterator) entry(b *ringBuf) *raftpb.Entry {
	return &b.buf[it]
}

// clear zeroes the current value in b
func (it iterator) clear(b *ringBuf) {
	b.buf[it] = raftpb.Entry{}
}
