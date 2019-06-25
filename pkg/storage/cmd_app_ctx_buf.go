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

// cmdAppCtxBufNodeSize is the size of the arrays in an
// cmdAppStateBufNode.
// TODO(ajwerner): justify this number.
const cmdAppCtxBufNodeSize = 8

// cmdAppCtxBuf is an allocation-efficient buffer used during the
// application of raft entries. Initialization occurs lazily upon the first
// call to allocate but used cmdAppCtxBuf objects should be released
// explicitly with the clear() method to release the allocated buffers back
// to the pool.
type cmdAppCtxBuf struct {
	len        int32
	head, tail *cmdAppCtxBufNode
}

// cmdAppCtxBufNode is a linked-list element in an
// cmdAppStateBuf.
type cmdAppCtxBufNode struct {
	len  int32
	buf  [cmdAppCtxBufNodeSize]cmdAppCtx
	next *cmdAppCtxBufNode
}

var cmdAppStateBufNodeSyncPool = sync.Pool{
	New: func() interface{} { return new(cmdAppCtxBufNode) },
}

// allocate extends the length of buf by one and returns the newly
// added element. If this is the first call to allocate it will initialize buf.
// After a buf is initialized it should be explicitly destroyed.
func (buf *cmdAppCtxBuf) allocate() *cmdAppCtx {
	if buf.tail == nil { // lazy initialization
		n := cmdAppStateBufNodeSyncPool.Get().(*cmdAppCtxBufNode)
		buf.head, buf.tail = n, n
	}
	if buf.tail.len == cmdAppCtxBufNodeSize {
		newTail := cmdAppStateBufNodeSyncPool.Get().(*cmdAppCtxBufNode)
		buf.tail.next = newTail
		buf.tail = newTail
	}
	ret := &buf.tail.buf[buf.tail.len]
	buf.tail.len++
	buf.len++
	return ret
}

// truncate clears all of the entries currently in a buffer and returns any
// allocated buffers to the pool.
func (buf *cmdAppCtxBuf) clear() {
	for buf.head != nil {
		buf.len -= buf.head.len
		oldHead := buf.head
		newHead := oldHead.next
		buf.head = newHead
		*oldHead = cmdAppCtxBufNode{}
		cmdAppStateBufNodeSyncPool.Put(oldHead)
	}
	*buf = cmdAppCtxBuf{}
}

// last returns a pointer to the last element in the buffer.
func (buf *cmdAppCtxBuf) last() *cmdAppCtx {
	return &buf.tail.buf[buf.tail.len-1]
}

// cmdAppCtxBufIterator iterates through the entries in an
// cmdAppStateBuf.
type cmdAppCtxBufIterator struct {
	idx  int32
	buf  *cmdAppCtxBuf
	node *cmdAppCtxBufNode
}

// init seeks the iterator to the front of buf. It returns true if buf is not
// empty and false if it is.
func (it *cmdAppCtxBufIterator) init(buf *cmdAppCtxBuf) (ok bool) {
	*it = cmdAppCtxBufIterator{buf: buf, node: buf.head}
	return it.buf.len > 0
}

// cur returns the cmdAppState currently pointed to by it.
func (it *cmdAppCtxBufIterator) cur() *cmdAppCtx {
	return &it.node.buf[it.idx%cmdAppCtxBufNodeSize]
}

// isLast returns true if it currently points to the last element in the buffer.
func (it *cmdAppCtxBufIterator) isLast() bool {
	return it.idx+1 == it.buf.len
}

// next moves it to point to the next element. It returns false if it.isLast()
// is true, indicating that there are no more elements in the buffer.
func (it *cmdAppCtxBufIterator) next() bool {
	if it.isLast() {
		return false
	}
	it.idx++
	if it.idx%cmdAppCtxBufNodeSize == 0 {
		it.node = it.node.next
	}
	return true
}
