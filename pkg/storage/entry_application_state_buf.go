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

import "sync"

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

// entryApplicationStateRingBuf is a ring-buffer of entryApplicationState.
// It offers indexing and truncation.
type entryApplicationStateBufNode struct {
	len  int32
	buf  [entryApplicationStateBufNodeSize]entryApplicationState
	next *entryApplicationStateBufNode
}

func (buf *entryApplicationStateBuf) last() *entryApplicationState {
	return &buf.tail.buf[buf.tail.len-1]
}

// allocate extends the length of buf by one and returns the newly
// added element. If this is the fist call to allocate it will initialize buf.
// After a buf is initialized it should be explicitly destroyed.
func (buf *entryApplicationStateBuf) allocate() *entryApplicationState {
	if buf.tail == nil { // lazy initialization
		n := entryApplicationStateBufNodeSyncPool.Get().(*entryApplicationStateBufNode)
		buf.head, buf.tail = n, n
	}
	if buf.tail.len == entryApplicationStateBufNodeSize {
		newTail := entryApplicationStateBufNodeSyncPool.Get().(*entryApplicationStateBufNode)
		buf.tail.next = newTail
		buf.tail = newTail
	}
	buf.len++
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
func (buf *entryApplicationStateBuf) clear() {
	for buf.head != buf.tail {
		buf.len -= buf.head.len
		oldHead := buf.head
		newHead := oldHead.next
		buf.head = newHead
		oldHead.clear()
		entryApplicationStateBufNodeSyncPool.Put(oldHead)
	}
	buf.head.clear()
	buf.len = 0
}

// allocate extends the length of the node by one and returns the pointer to the
// newly allocated element. It is illegal to call allocate on a full node.
func (rb *entryApplicationStateBufNode) allocate() *entryApplicationState {
	if rb.len == entryApplicationStateBufNodeSize {
		panic("cannot push onto a full entryApplicationStateBufNode")
	}
	ret := &rb.buf[rb.len]
	rb.len++
	return ret
}

// clear zeroes rb.
func (rb *entryApplicationStateBufNode) clear() {
	*rb = entryApplicationStateBufNode{}
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
	return &it.node.buf[it.offset]
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
