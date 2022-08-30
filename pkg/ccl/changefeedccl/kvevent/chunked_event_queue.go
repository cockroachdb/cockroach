// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvevent

import "sync"

const bufferEventChunkArrSize = 128

// bufferEventChunkQueue is a queue implemented as a linked-list of bufferEntry.
type bufferEventChunkQueue struct {
	head, tail *bufferEventChunk
}

func (l *bufferEventChunkQueue) enqueue(e Event) {
	if l.tail == nil {
		chunk := newBufferEntryChunk()
		l.head, l.tail = chunk, chunk
		l.head.push(e) // guaranteed to insert into new chunk
		return
	}

	if !l.tail.push(e) {
		chunk := newBufferEntryChunk()
		l.tail.next = chunk
		l.tail = chunk
		l.tail.push(e) // guaranteed to insert into new chunk
	}
}

func (l *bufferEventChunkQueue) empty() bool {
	return l.head == nil || l.head.empty()
}

func (l *bufferEventChunkQueue) dequeue() (e Event, ok bool) {
	if l.head == nil {
		return Event{}, false
	}

	e, ok, consumedAllFromChunk := l.head.pop()

	if consumedAllFromChunk {
		toFree := l.head
		if l.tail == l.head {
			l.tail = l.head.next
		}
		l.head = l.head.next
		freeBufferEventChunk(toFree)
		return l.dequeue()
	}

	if !ok {
		return Event{}, false
	}

	return e, true

}

func (l *bufferEventChunkQueue) purge() {
	for l.head != nil {
		chunkToFree := l.head
		l.head = l.head.next
		freeBufferEventChunk(chunkToFree)
	}
	l.tail = l.head
}

type bufferEventChunk struct {
	events [bufferEventChunkArrSize]Event
	// Since bufferEventChunkArrSize may be increased beyond 128 in the future, we
	// can leave this as an int32 for now. Also, bufferEventChunk allocations are
	// pooled, so there should not be a significant increase in memory because of
	// this.
	head, tail int32
	next       *bufferEventChunk // linked-list element
}

var bufferEntryChunkPool = sync.Pool{
	New: func() interface{} {
		return new(bufferEventChunk)
	},
}

func newBufferEntryChunk() *bufferEventChunk {
	return bufferEntryChunkPool.Get().(*bufferEventChunk)
}

func freeBufferEventChunk(c *bufferEventChunk) {
	*c = bufferEventChunk{}
	bufferEntryChunkPool.Put(c)
}

func (bec *bufferEventChunk) push(e Event) (inserted bool) {
	if bec.tail == bufferEventChunkArrSize {
		return false
	}

	bec.events[bec.tail] = e
	bec.tail++
	return true
}

func (bec *bufferEventChunk) pop() (e Event, ok bool, consumedAll bool) {
	if bec.head == bufferEventChunkArrSize {
		return Event{}, false, true
	}

	if bec.head == bec.tail {
		return Event{}, false, false
	}

	e = bec.events[bec.head]
	bec.head++
	return e, true, false
}

func (bec *bufferEventChunk) empty() bool {
	return bec.tail == bec.head
}
