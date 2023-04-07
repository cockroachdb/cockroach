// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package queue

import "math/bits"

// Queue is a generic queue container with amortized allocation cost.
// Zero initialized Queue[T] is safe to use.
type Queue[T any] struct {
	head, tail *chunk[T]
	len        int
	rPos       int // read position in this queue relative to head.
	wPos       int // write position in this queue relative to tail.
}

// Len returns the number of elements in the queue.
func (q *Queue[T]) Len() int {
	return q.len
}

// Empty returns true if the queue is empty.
func (q *Queue[T]) Empty() bool {
	return q.len == 0
}

// Push adds element to this queue.
func (q *Queue[T]) Push(v T) {
	if q.head == nil {
		if q.tail != nil {
			panic("expected empty queue tail")
		}
		c := q.newChunk()
		q.head = &c
		q.tail = q.head
		q.rPos = 0
		q.wPos = 0
	}

	if q.wPos == cap(q.tail.data) {
		c := q.newChunk()
		c.prev = q.tail
		q.tail.next = &c
		q.tail = &c
		q.wPos = 0
	}

	q.tail.data[q.wPos] = v
	q.wPos++
	q.len++
}

// Pop returns the last element in the queue, or false if the queue is empty.
func (q *Queue[T]) Pop() (v T, ok bool) {
	if q.len == 0 {
		return v, false
	}

	q.wPos--
	v = q.tail.data[q.wPos]
	q.len--

	if q.head == q.tail && q.rPos == q.wPos {
		// Everything was consumed from the single chunk.
		// Reset read/write positions.
		q.rPos = 0
		q.wPos = 0
	} else if q.wPos == 0 {
		if prev := q.tail.prev; prev != nil {
			prev.next = nil
			q.tail = prev
			q.wPos = cap(prev.data)
		}
	}

	return v, true
}

// PopFront returns the first element in the queue, or false if queue is empty.
func (q *Queue[T]) PopFront() (v T, ok bool) {
	if q.len == 0 {
		return v, false
	}

	v = q.head.data[q.rPos]
	q.rPos++
	q.len--

	if q.head == q.tail && q.rPos == q.wPos {
		// Everything was consumed from the single chunk.
		// Reset read/write positions.
		q.rPos = 0
		q.wPos = 0
	} else if q.rPos == cap(q.head.data) {
		// Chunk is now empty, release it.
		q.rPos = 0
		q.head = q.head.next
	}

	return v, true
}

func (q *Queue[T]) newChunk() (c chunk[T]) {
	c.data = make([]T, chunkSize(q.len))
	return c
}

// returns next chunk size based on the current queue length.
func chunkSize(l int) int {
	if l < 16 {
		return 16
	}
	if l > 512 {
		return 1024
	}
	return 1 << (64 - bits.LeadingZeros(uint(l)))
}

type chunk[T any] struct {
	data []T
	next *chunk[T]
	prev *chunk[T]
}
