// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"fmt"
	"sync"
)

// applicationStateBufSize is the size of the arrays in an
// applicationStateBufNode.
// TODO(ajwerner): justify this number.
const applicationStateBufSize = 8

// applicationStateBuf is an allocation-efficient buffer used during the
// application of raft entries. Initialization occurs lazily upon the first
// call to pushBack but used applicationStateBuf objects should be released
// explicitly with the destroy() method to release the allocated buffers back
// to the pool.
type applicationStateBuf struct {
	len        int32
	unpushed   bool
	head, tail *applicationStateBufNode
}

var applicationStateBufNodeSyncPool = sync.Pool{
	New: func() interface{} { return new(applicationStateBufNode) },
}

type applicationStateBufNode struct {
	applicationStateRingBuf
	next *applicationStateBufNode
}

// unpush allows the caller to keep the tail element of the buffer allocated but
// remove it from iteration. After calling unpush, the next call to pushBack
// will return the value from the previous call to pushBack.
// This function may not be called more than once between calls to pushBack and
// may not be called on an empty buf.
func (buf *applicationStateBuf) unpush() {
	if buf.unpushed {
		panic("already unpushed")
	} else if buf.len == 0 {
		panic("cannot unpush empty buffer")
	}
	buf.len--
	buf.unpushed = true
}

func (buf *applicationStateBuf) first() *applicationState {
	return buf.head.at(0)
}

// pushBack extends the length of buf by one and returns the newly
// added element. If this is the fist call to pushBack following a call to
// unpush, the element returned from this call will be the same value returned
// from the previous call to pushBack.
func (buf *applicationStateBuf) pushBack() *applicationState {
	if buf.tail == nil {
		n := applicationStateBufNodeSyncPool.Get().(*applicationStateBufNode)
		buf.head, buf.tail = n, n
	}
	buf.len++
	if buf.unpushed {
		buf.unpushed = false
		return buf.tail.at(buf.tail.len - 1)
	}
	if buf.tail.len == applicationStateBufSize {
		newTail := applicationStateBufNodeSyncPool.Get().(*applicationStateBufNode)
		buf.tail.next = newTail
		buf.tail = newTail
	}
	return buf.tail.pushBack()
}

// destroy releases allocated nodes back into the sync pool.
// It is illegal to use buf after a call to destroy.
func (buf *applicationStateBuf) destroy() {
	for cur := buf.head; cur != nil; {
		next := cur.next
		*cur = applicationStateBufNode{}
		applicationStateBufNodeSyncPool.Put(cur)
		cur, buf.head = next, next
	}
	*buf = applicationStateBuf{}
}

// truncate clears all of the entries currently in a buffer though will never
// release an entry which has been allocated and unpushed.
func (buf *applicationStateBuf) truncate() {
	for buf.head != buf.tail {
		buf.len -= buf.head.len
		buf.head.truncate(buf.head.len)
		oldHead := buf.head
		newHead := oldHead.next
		buf.head = newHead
		*oldHead = applicationStateBufNode{}
		applicationStateBufNodeSyncPool.Put(oldHead)
	}
	buf.head.truncate(buf.len)
	buf.len = 0
}

// applicationStateRingBuf is a ring-buffer of applicationState.
// It offers indexing and truncation from the front.
type applicationStateRingBuf struct {
	len  int32
	head int32
	buf  [applicationStateBufSize]applicationState
}

// at returns the application state
func (rb *applicationStateRingBuf) at(idx int32) *applicationState {
	if idx >= rb.len {
		panic(fmt.Sprintf("index out of range %v, %v", idx, rb.len))
	}
	return &rb.buf[(rb.head+idx)%applicationStateBufSize]
}

// pushBack extends the length of the ring buffer by one and returns the newly
// added element. It is illegal to call pushBack on a full ring buf.
func (rb *applicationStateRingBuf) pushBack() *applicationState {
	if rb.len == applicationStateBufSize {
		panic("cannot push onto a full applicationStateRingBuf")
	}
	ret := &rb.buf[(rb.head+rb.len)%applicationStateBufSize]
	rb.len++
	return ret
}

// truncate removes the first n elements from the buffer.
// It is illegal to pass a number greater than the current len.
func (rb *applicationStateRingBuf) truncate(n int32) {
	if n > rb.len {
		panic("cannot truncate more than have")
	}
	// TODO(ajwerner): consider removing this as an optimization.
	for i := int32(0); i < n; i++ {
		*rb.at(i) = applicationState{}
	}
	rb.len -= n
	rb.head += n
	rb.head %= applicationStateBufSize
}

type applicationStateBufIterator struct {
	idx    int32
	offset int32
	buf    *applicationStateBuf
	node   *applicationStateBufNode
}

func (it *applicationStateBufIterator) init(buf *applicationStateBuf) bool {
	*it = applicationStateBufIterator{
		buf:  buf,
		node: buf.head,
	}
	return it.buf.len > 0
}

func (it *applicationStateBufIterator) next() bool {
	if it.idx+1 == it.buf.len {
		return false
	}
	it.idx++
	it.offset++
	if it.offset == applicationStateBufSize {
		it.node = it.node.next
		it.offset = 0
	}
	return true
}

func (it *applicationStateBufIterator) state() *applicationState {
	return it.node.at(it.offset)
}
