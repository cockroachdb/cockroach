// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

// Package notifyqueue provides an efficient fifo for chan struct{}.
//
// Internally efficiency is achieved using a linked-list of ring-buffers
// which utilize a sync.Pool. A Pool may be created in order to construct
// multiple queues which internally share a sync.Pool for their buffers.
package notifyqueue

import "sync"

// blockSize is the size of blocks served from the default pool.
//
// TODO(ajwerner): Justify this further.
// Each node is 32 + 8 * blockSize bytes so at 28 a node is 256 bytes which
// feels like a nice number.
const blockSize = 28

// Pool constructs NotifyQueue objects which internally pool their buffers.
type Pool struct {
	pool sync.Pool
}

// NewPool returns a new Pool which can be used to construct NotifyQueues which
// internally pool their blocks.
func NewPool() *Pool {
	p := &Pool{}
	p.pool.New = func() interface{} {
		return &node{}
	}
	return p
}

// NewNotifyQueue creates a NotifyQueue which will share a sync.Pool for
// buffers with the other NotifyQueue instances created from this Pool.
func (p *Pool) NewNotifyQueue() *NotifyQueue {
	return &NotifyQueue{pool: p}
}

// NotifyQueue provides an allocation efficient FIFO for chan struct{}.
type NotifyQueue struct {
	len  int
	pool *Pool
	head *node
}

var defaultPool = NewPool()

// New returns a NotifyQueue with the default block size.
func New() *NotifyQueue {
	return defaultPool.NewNotifyQueue()
}

// Enqueue adds c to the end of the queue.
func (q *NotifyQueue) Enqueue(c chan struct{}) {
	if q.head == nil {
		q.head = q.pool.pool.Get().(*node)
		q.head.prev = q.head
		q.head.next = q.head
	}
	if !q.head.prev.enqueue(c) {
		tail := q.head.prev
		newTail := q.pool.pool.Get().(*node)
		tail.next = newTail
		q.head.prev = newTail
		newTail.prev = tail
		newTail.next = q.head
		newTail.enqueue(c)
	}
	q.len++
}

// Len returns the current length of the queue.
func (q *NotifyQueue) Len() int {
	return q.len
}

// Dequeue removes the head of the queue and returns it and nil if the queue is
// empty.
func (q *NotifyQueue) Dequeue() (c chan struct{}) {
	if q.head == nil {
		return nil
	}
	ret := q.head.dequeue()
	if q.head.len == 0 {
		oldHead := q.head
		if oldHead.next == oldHead {
			q.head = nil
		} else {
			q.head = oldHead.next
			q.head.prev = oldHead.prev
			q.head.prev.next = q.head
		}
		*oldHead = node{}
		q.pool.pool.Put(oldHead)
	}
	q.len--
	return ret
}

// Peek returns the current head of the queue or nil if the queeu is empty.
// It does not modify the queue.
func (q *NotifyQueue) Peek() chan struct{} {
	if q.head == nil {
		return nil
	}
	return q.head.buf[q.head.head]
}

type node struct {
	ringBuf
	prev, next *node
}

type ringBuf struct {
	buf  [blockSize]chan struct{}
	head int
	len  int
}

func (rb *ringBuf) enqueue(c chan struct{}) (enqueued bool) {
	if rb.len == blockSize {
		return false
	}
	rb.buf[(rb.head+rb.len)%blockSize] = c
	rb.len++
	return true
}

func (rb *ringBuf) dequeue() chan struct{} {
	// NB: the NotifyQueue never contains an empty ringBuf.
	if rb.len == 0 {
		panic("cannot dequeue from an empty buffer")
	}
	ret := rb.buf[rb.head]
	rb.buf[rb.head] = nil
	rb.head++
	rb.head %= blockSize
	rb.len--
	return ret
}
