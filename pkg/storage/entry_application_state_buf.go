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

// entryApplicationStateBufNodeSize is the size of the arrays in an
// entryApplicationStateBufNode.
// TODO(ajwerner): justify this number.
const entryApplicationStateBufNodeSize = 8

// entryApplicationStateBuf is an allocation-efficient buffer used during the
// application of raft entries. Initialization occurs lazily upon the first
// call to allocate but used entryApplicationStateBuf objects should be released
// explicitly with the destroy() method to release the allocated buffers back
// to the pool.
type entryApplicationStateBuf struct {
	len        int32
	head, tail *entryApplicationStateBufNode
}

var entryApplicationStateBufNodeSyncPool = sync.Pool{
	New: func() interface{} { return new(entryApplicationStateBufNode) },
}

type entryApplicationStateBufNode struct {
	entryApplicationStateRingBuf
	next *entryApplicationStateBufNode
}

func (buf *entryApplicationStateBuf) last() *entryApplicationState {
	return buf.tail.at(buf.tail.len - 1)
}

// allocate extends the length of buf by one and returns the newly
// added element. If this is the fist call to allocate it will initialize buf.
// After a buf is initialized it should be explicitly destroyed.
func (buf *entryApplicationStateBuf) allocate() *entryApplicationState {
	if buf.tail == nil {
		n := entryApplicationStateBufNodeSyncPool.Get().(*entryApplicationStateBufNode)
		buf.head, buf.tail = n, n
	}
	buf.len++
	if buf.tail.len == entryApplicationStateBufNodeSize {
		newTail := entryApplicationStateBufNodeSyncPool.Get().(*entryApplicationStateBufNode)
		buf.tail.next = newTail
		buf.tail = newTail
	}
	return buf.tail.allocate()
}

// destroy releases allocated nodes back into the sync pool.
// It is illegal to use buf after a call to destroy.
func (buf *entryApplicationStateBuf) destroy() {
	for cur := buf.head; cur != nil; {
		next := cur.next
		*cur = entryApplicationStateBufNode{}
		entryApplicationStateBufNodeSyncPool.Put(cur)
		cur, buf.head = next, next
	}
	*buf = entryApplicationStateBuf{}
}

// truncate clears all of the entries currently in a buffer.
func (buf *entryApplicationStateBuf) truncate() {
	for buf.head != buf.tail {
		buf.len -= buf.head.len
		buf.head.truncate(buf.head.len)
		oldHead := buf.head
		newHead := oldHead.next
		buf.head = newHead
		*oldHead = entryApplicationStateBufNode{}
		entryApplicationStateBufNodeSyncPool.Put(oldHead)
	}
	buf.head.truncate(buf.len)
	buf.len = 0
}

// entryApplicationStateRingBuf is a ring-buffer of entryApplicationState.
// It offers indexing and truncation from the front.
type entryApplicationStateRingBuf struct {
	len  int32
	head int32
	buf  [entryApplicationStateBufNodeSize]entryApplicationState
}

// at returns the application state at the requested idx.
func (rb *entryApplicationStateRingBuf) at(idx int32) *entryApplicationState {
	if idx >= rb.len {
		panic(fmt.Sprintf("index out of range %v, %v", idx, rb.len))
	}
	return &rb.buf[(rb.head+idx)%entryApplicationStateBufNodeSize]
}

// allocate extends the length of the ring buffer by one and returns the newly
// added element. It is illegal to call allocate on a full ring buf.
func (rb *entryApplicationStateRingBuf) allocate() *entryApplicationState {
	if rb.len == entryApplicationStateBufNodeSize {
		panic("cannot push onto a full entryApplicationStateRingBuf")
	}
	ret := &rb.buf[(rb.head+rb.len)%entryApplicationStateBufNodeSize]
	rb.len++
	return ret
}

// truncate removes the first n elements from the buffer.
// It is illegal to pass a number greater than the current len.
func (rb *entryApplicationStateRingBuf) truncate(n int32) {
	if n > rb.len {
		panic("cannot truncate more than have")
	}
	// TODO(ajwerner): consider removing this as an optimization.
	for i := int32(0); i < n; i++ {
		*rb.at(i) = entryApplicationState{}
	}
	rb.len -= n
	rb.head += n
	rb.head %= entryApplicationStateBufNodeSize
}

type entryApplicationStateBufIterator struct {
	idx    int32
	offset int32
	buf    *entryApplicationStateBuf
	node   *entryApplicationStateBufNode
}

func (it *entryApplicationStateBufIterator) init(buf *entryApplicationStateBuf) bool {
	*it = entryApplicationStateBufIterator{
		buf:  buf,
		node: buf.head,
	}
	return it.buf.len > 0
}

func (it *entryApplicationStateBufIterator) state() *entryApplicationState {
	return it.node.at(it.offset)
}

func (it *entryApplicationStateBufIterator) isLast() bool {
	return it.idx+1 == it.buf.len
}

func (it *entryApplicationStateBufIterator) next() bool {
	if it.idx+1 == it.buf.len {
		return false
	}
	it.idx++
	it.offset++
	if it.offset == entryApplicationStateBufNodeSize {
		it.node = it.node.next
		it.offset = 0
	}
	return true
}
