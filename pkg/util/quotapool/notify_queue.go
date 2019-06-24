// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package quotapool

import "sync"

// bufferSize is the size of the ringBuf buf served from a notifyQueueNodePool.
//
// Each node is 32+8*bufferSize bytes so at 28 a node is 256 bytes which
// feels like a nice number.
const bufferSize = 28

// notifyQueue provides an allocation efficient FIFO queue for chan struct{}.
//
// notifyQueue is not safe for concurrent use.
type notifyQueue struct {
	len  int64
	head *node

	pool *notifyQueueNodePool
}

// initializeNotifyQueue initializes a notifyQueue.
// notifyQueue values should not be used until they have been initialized.
// It is illegal to initialize a notifyQueue more than once and this function
// will panic if called with an already initialized notifyQueue.
func initializeNotifyQueue(q *notifyQueue) {
	if q.pool != nil {
		panic("cannot re-initialize a notifyQueue")
	}
	defaultNotifyQueueNodePool.initialize(q)
}

var defaultNotifyQueueNodePool = newNotifyQueueNodePool()

// enqueue adds c to the end of the queue.
func (q *notifyQueue) enqueue(c chan struct{}) {
	if q.head == nil {
		q.head = q.pool.pool.Get().(*node)
		q.head.prev = q.head
		q.head.next = q.head
	}
	tail := q.head.prev
	if !tail.enqueue(c) {
		newTail := q.pool.pool.Get().(*node)
		tail.next = newTail
		q.head.prev = newTail
		newTail.prev = tail
		newTail.next = q.head
		if !newTail.enqueue(c) {
			panic("failed to enqueue into a fresh buffer")
		}
	}
	q.len++
}

// dequeue removes and returns the head of the queue or nil if the queue is
// empty.
func (q *notifyQueue) dequeue() (c chan struct{}) {
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

// peek returns the current head of the queue or nil if the queue is empty.
// It does not modify the queue.
func (q *notifyQueue) peek() chan struct{} {
	if q.head == nil {
		return nil
	}
	return q.head.buf[q.head.head]
}

// notifyQueueNodePool constructs notifyQueue objects which internally pool
// their buffers.
type notifyQueueNodePool struct {
	pool sync.Pool
}

// newNotifyQueueNodePool returns a new notifyQueueNodePool which can be used
// to construct notifyQueues which internally pool their buffers.
func newNotifyQueueNodePool() *notifyQueueNodePool {
	return &notifyQueueNodePool{
		pool: sync.Pool{
			New: func() interface{} { return &node{} },
		},
	}
}

// initialize initializes a which will share a sync.Pool of nodes with the
// other notifyQueue instances initialized with this pool.
func (p *notifyQueueNodePool) initialize(q *notifyQueue) {
	*q = notifyQueue{pool: p}
}

type node struct {
	ringBuf
	prev, next *node
}

type ringBuf struct {
	buf  [bufferSize]chan struct{}
	head int64
	len  int64
}

func (rb *ringBuf) enqueue(c chan struct{}) (enqueued bool) {
	if rb.len == bufferSize {
		return false
	}
	rb.buf[(rb.head+rb.len)%bufferSize] = c
	rb.len++
	return true
}

func (rb *ringBuf) dequeue() chan struct{} {
	// NB: the notifyQueue never contains an empty ringBuf.
	if rb.len == 0 {
		panic("cannot dequeue from an empty buffer")
	}
	ret := rb.buf[rb.head]
	rb.buf[rb.head] = nil
	rb.head++
	rb.head %= bufferSize
	rb.len--
	return ret
}
