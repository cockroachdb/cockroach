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

// pushBack adds c to the end of the queue and returns the address of the added
// notifyee.
func (q *notifyQueue) pushBack(c chan struct{}) (n *notifyee) {
	q.maybeInitHead()
	tail := q.head.prev
	if n = tail.pushBack(c); n == nil {
		newTail := q.pool.pool.Get().(*node)
		tail.next = newTail
		q.head.prev = newTail
		newTail.prev = tail
		newTail.next = q.head
		if n = newTail.pushBack(c); n == nil {
			panic("failed to pushBack into a fresh buffer")
		}
	}
	q.len++
	return n
}

func (q *notifyQueue) pushFront(c chan struct{}) (n *notifyee) {
	q.maybeInitHead()
	head := q.head
	if n = head.pushFront(c); n == nil {
		newHead := q.pool.pool.Get().(*node)
		newHead.next = head
		newHead.prev = head.prev
		head.prev = newHead
		q.head = newHead
		if n = newHead.pushFront(c); n == nil {
			panic("failed to pushBack into a fresh buffer")
		}
	}
	q.len++
	return n
}

func (q *notifyQueue) maybeInitHead() {
	if q.head != nil {
		return
	}
	q.head = q.pool.pool.Get().(*node)
	q.head.prev = q.head
	q.head.next = q.head
}

// popFront removes the current head of the queue which can be accessed with
// peek().
func (q *notifyQueue) popFront() {
	if q.head == nil {
		return
	}
	q.head.popFront()
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
}

// peek returns the current head of the queue or nil if the queue is empty.
// It does not modify the queue. It is illegal to use the returned pointer after
// the next call to popFront.
func (q *notifyQueue) peek() *notifyee {
	if q.head == nil {
		return nil
	}
	return &q.head.buf[q.head.head]
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

type notifyee struct {
	c chan struct{} // set to nil after the notifyee is done
}

type ringBuf struct {
	buf  [bufferSize]notifyee
	head int64
	len  int64
}

func (rb *ringBuf) pushBack(c chan struct{}) *notifyee {
	if rb.len == bufferSize {
		return nil
	}
	i := (rb.head + rb.len) % bufferSize
	rb.buf[i] = notifyee{c: c}
	rb.len++
	return &rb.buf[i]
}

func (rb *ringBuf) pushFront(c chan struct{}) *notifyee {
	if rb.len == bufferSize {
		return nil
	}
	if rb.head--; rb.head == -1 {
		rb.head += bufferSize
	}
	rb.buf[rb.head] = notifyee{c: c}
	rb.len++
	return &rb.buf[rb.head]
}

func (rb *ringBuf) popFront() {
	// NB: the notifyQueue never contains an empty ringBuf.
	if rb.len == 0 {
		panic("cannot popFront from an empty buffer")
	}
	rb.buf[rb.head] = notifyee{}
	rb.head++
	rb.head %= bufferSize
	rb.len--
}
