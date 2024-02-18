// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package queue

import "github.com/cockroachdb/errors"

// An Option is a configurable parameter of the queue.
type Option[T any] func(q *Queue[T]) error

func (o Option[T]) apply(q *Queue[T]) error {
	return o(q)
}

// WithChunkSize configures the size of slices used in the queue.
func WithChunkSize[T any](i int) Option[T] {
	return func(q *Queue[T]) error {
		if i < 1 {
			return errors.Newf("invalid chunk size %d", i)
		}
		q.chunkSize = i
		return nil
	}
}

const defaultChunkSize = 128

// Queue is a FIFO queue implemented as a chunked linked list. The default chunk
// size is 128.
type Queue[T any] struct {
	head, tail *queueChunk[T]

	chunkSize int
}

// NewQueue returns a Queue of T.
func NewQueue[T any](opts ...Option[T]) (*Queue[T], error) {
	q := &Queue[T]{
		chunkSize: defaultChunkSize,
	}
	for _, opt := range opts {
		if err := opt.apply(q); err != nil {
			return nil, err
		}
	}
	return q, nil
}

// Enqueue adds an element to the back of the queue.
func (q *Queue[T]) Enqueue(e T) {
	if q.tail == nil {
		chunk := newQueueChunk[T](q.chunkSize)
		q.head, q.tail = chunk, chunk
		q.head.push(e) // guaranteed to insert into new chunk
		return
	}

	if !q.tail.push(e) {
		chunk := newQueueChunk[T](q.chunkSize)
		q.tail.next = chunk
		q.tail = chunk
		q.tail.push(e) // guaranteed to insert into new chunk
	}
}

// Empty returns true IFF the Queue is empty.
func (q *Queue[T]) Empty() bool {
	return q.head == nil || q.head.empty()
}

// Dequeue removes an element from the front of the queue and returns it.
func (q *Queue[T]) Dequeue() (e T, ok bool) {
	if q.head == nil {
		return e, false
	}

	e, ok, consumedAllFromChunk := q.head.pop()

	if consumedAllFromChunk {
		if q.tail == q.head {
			q.tail = q.head.next
		}
		q.head = q.head.next
		// NB: The previous value of q.head will be garbage collected.
		if !ok {
			// We expect a maximum recursion depth of 1 because there is an
			// invariant that adjacent chunks will not (be non-nil and contain 0 events).
			return q.Dequeue()
		}
	}

	if !ok {
		return e, false
	}

	return e, true

}

func (q *Queue[T]) purge() {
	for q.head != nil {
		q.head = q.head.next
		// NB: The previous value of q.head will be garbage collected.
	}
	q.tail = q.head
}

type queueChunk[T any] struct {
	events     []T
	head, tail int
	next       *queueChunk[T] // linked-list element
}

func newQueueChunk[T any](sz int) *queueChunk[T] {
	return &queueChunk[T]{
		events: make([]T, sz),
	}
}

func (c *queueChunk[T]) push(e T) (inserted bool) {
	if c.tail == len(c.events) {
		return false
	}

	c.events[c.tail] = e
	c.tail++
	return true
}

func (c *queueChunk[T]) pop() (e T, ok bool, consumedAll bool) {
	if c.head == len(c.events) {
		return e, false, true
	}

	if c.head == c.tail {
		return e, false, false
	}

	e = c.events[c.head]
	c.head++
	return e, true, c.head == len(c.events)
}

func (c *queueChunk[T]) empty() bool {
	return c.tail == c.head
}
