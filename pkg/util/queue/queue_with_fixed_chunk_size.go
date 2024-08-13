// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package queue

import "sync"

const fixedChunkSize = 4096

func sharedEventQueueChunkSyncPool[T any]() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			return new(queueChunkWithFixedSize[T])
		},
	}
}

func newEventQueueChunk[T any](p *sync.Pool) *queueChunkWithFixedSize[T] {
	return p.Get().(*queueChunkWithFixedSize[T])
}

func releasePooledEventQueueChunk[T any](p *sync.Pool, e *queueChunkWithFixedSize[T]) {
	*e = queueChunkWithFixedSize[T]{}
	p.Put(e)
}

// QueueWithFixedChunkSize is like Queue but uses a fixed size for the chunked
// linked list. Each chunk has a fixed size of 4096 elements. This
// implementation uses sync.Pool to reduce the number of allocations. It should
// be used when the queue is used to store a large number of elements.
//
// Enqueue, Dequeue, Len, Empty run in constant time. RemoveAll runs in linear
// time with respect to the number of elements in the queue.
//
// See the Queue type for more details. This structure is not safe for
// concurrent use.
type QueueWithFixedChunkSize[T any] struct {
	pool       *sync.Pool
	head, tail *queueChunkWithFixedSize[T]
	eventCount int64
}

// NewQueueWithFixedChunkSize returns a QueueWithFixedChunkSize of T.
func NewQueueWithFixedChunkSize[T any]() *QueueWithFixedChunkSize[T] {
	return &QueueWithFixedChunkSize[T]{
		pool: sharedEventQueueChunkSyncPool[T](),
	}
}

// Len returns the number of elements in the queue.
func (q *QueueWithFixedChunkSize[T]) Len() int64 {
	return q.eventCount
}

// Enqueue adds an element to the back of the queue. It is guaranteed that this
// operation would succeed.
func (q *QueueWithFixedChunkSize[T]) Enqueue(e T) {
	// If the queue is empty, create a new chunk.
	if q.tail == nil {
		chunk := newEventQueueChunk[T](q.pool)
		q.head, q.tail = chunk, chunk
		q.head.push(e) // guaranteed to insert into new chunk
		q.eventCount++
		return
	}

	// If the current chunk is full (first push would do nothing and return
	// false), create a new chunk and push again.
	if !q.tail.push(e) {
		chunk := newEventQueueChunk[T](q.pool)
		q.tail.next = chunk
		q.tail = chunk
		q.tail.push(e) // guaranteed to insert into new chunk
	}
	q.eventCount++
}

// Empty returns true IFF the Queue is empty. Note that it is possible for the
// queue to be considered empty but have a non-nil head. RemoveAll should be
// used to clear the queue properly.
func (q *QueueWithFixedChunkSize[T]) Empty() bool {
	return q.Len() == 0
}

// Dequeue removes an element from the front of the queue and returns it. ok is
// true if succeed and false if the queue is empty.
func (q *QueueWithFixedChunkSize[T]) Dequeue() (e T, ok bool) {
	if q.head == nil {
		return e, false
	}

	e, ok = q.head.pop()
	if !ok {
		return e, false
	}

	// If everything has been consumed from the chunk, remove it.
	if q.head.finished() {
		if q.tail == q.head {
			q.tail = q.head.next
		}
		removed := q.head
		q.head = q.head.next
		releasePooledEventQueueChunk[T](q.pool, removed)
	}
	q.eventCount--
	return e, true
}

// RemoveAll removes all entries in the queue and calls the callback on each
// entry. q.head == q.tail == nil after this call.
func (q *QueueWithFixedChunkSize[T]) RemoveAll(callback func(e T)) {
	for q.head != nil {
		q.head.clearAll(func(e T) {
			callback(e)
			q.eventCount--
		})
		remove := q.head
		q.head = q.head.next
		releasePooledEventQueueChunk[T](q.pool, remove)
	}
	q.tail = q.head
}

type queueChunkWithFixedSize[T any] struct {
	events     [fixedChunkSize]T
	head, tail int
	next       *queueChunkWithFixedSize[T] // linked-list element
}

// clearAll clears all the elements in the chunk to its zero value and calls the
// callback on each element.
func (c *queueChunkWithFixedSize[T]) clearAll(callback func(e T)) {
	// Iterate over all the elements in the chunk and call the callback.
	for i := c.head; i < c.tail; i++ {
		callback(c.events[i])
		var zeroValue T
		// Clear the value to make the data garbage collectable.
		c.events[i] = zeroValue
	}
}

// push adds an element to the chunk. It returns true if succeed and false if
// the chunk is full.
func (c *queueChunkWithFixedSize[T]) push(e T) (inserted bool) {
	if c.tail == len(c.events) {
		return false
	}

	c.events[c.tail] = e
	c.tail++
	return true
}

// pop removes an element from the chunk and returns it. It returns true if
// succeed and false if the chunk is empty.
func (c *queueChunkWithFixedSize[T]) pop() (e T, ok bool) {
	if c.head == c.tail {
		return e, false
	}

	e = c.events[c.head]
	var zeroValue T
	// Clear the value to make the data garbage collectable.
	c.events[c.head] = zeroValue
	c.head++
	return e, true
}

// finished returns true if all events have been inserted and consumed from the
// chunk.
func (c *queueChunkWithFixedSize[T]) finished() bool {
	return c.head == len(c.events)
}

// empty returns true if tail == head. Note that the chunk itself is not nil.
func (c *queueChunkWithFixedSize[T]) empty() bool {
	return c.tail == c.head
}
