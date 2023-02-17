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

import "github.com/cockroachdb/cockroach/pkg/util/pool"

// Queue is a generic FIFO queue container.
// This particular implementation allocates chunks of memory to
// reduce allocations.
// Zero initialized Queue[T] is safe to use.
type Queue[T any] struct {
	head, tail *chunk[T]
	len        int
	chunkPool  *pool.Pool[chunk[T]]
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
	if q.chunkPool == nil {
		p := pool.MakePool[chunk[T]](func(c *chunk[T]) {
			c.r = 0
			c.w = 0
			c.next = nil
		})
		q.chunkPool = &p
	}

	if q.head == nil {
		if q.tail != nil {
			panic("expected empty queue tail")
		}
		q.head = q.chunkPool.Get()
		q.tail = q.head
	}

	if q.tail.w == chunkSize {
		t := q.chunkPool.Get()
		q.tail.next = t
		q.tail = t
	}

	q.tail.arr[q.tail.w] = v
	q.tail.w++
	q.len++
}

// PopFront returns the front element in the queue, or false if
// queue is empty.
func (q *Queue[T]) PopFront() (v T, ok bool) {
	if q.len == 0 {
		return v, false
	}

	v = q.head.arr[q.head.r]
	q.head.r++
	q.len--

	if q.head.r == q.head.w {
		// Chunk is now empty, put it back into the pool.
		nextHead := q.head.next
		q.chunkPool.Put(q.head)
		q.head = nextHead
		if q.head == nil {
			q.tail = nil
		}
	}

	return v, true
}

// Release releases whatever chunks maybe held by this queue back into the pool.
func (q *Queue[T]) Release() {
	for q.head != nil {
		c := q.head
		q.head = q.head.next
		q.chunkPool.Put(c)
	}
	q.len = 0
	q.head = nil
	q.tail = nil
}

const chunkSize = 256

type chunk[T any] struct {
	arr  [chunkSize]T
	r, w int // read/write positions in array.
	next *chunk[T]
}
