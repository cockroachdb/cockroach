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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/storage/apply"
)

// cmdAppCtxBufNodeSize is the size of the arrays in an cmdAppStateBufNode.
// TODO(ajwerner): justify this number.
const cmdAppCtxBufNodeSize = 8

// cmdAppCtxBufSliceFreeListSize is the size of the cmdAppCtxBufSlice free
// list. The size has been tuned for the maximum number of iterators that
// the storage/apply package uses at once. If this size is insufficient
// then the freelist will panic to ensure that regressions are loud.
const cmdAppCtxBufSliceFreeListSize = 3

// cmdAppCtxBuf is an allocation-efficient buffer used during the
// application of raft entries. Initialization occurs lazily upon the first
// call to allocate but used cmdAppCtxBuf objects should be released
// explicitly with the clear() method to release the allocated buffers back
// to the pool.
type cmdAppCtxBuf struct {
	len        int32
	head, tail *cmdAppCtxBufNode
	free       cmdAppCtxBufSliceFreeList
}

// cmdAppCtxBufNode is a linked-list element in an cmdAppStateBuf.
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

// newIter returns a pointer to a new uninitialized iterator. The iterator
// should be closed when no longer in use.
func (buf *cmdAppCtxBuf) newIter() *cmdAppCtxBufSlice {
	return buf.free.get()
}

// cmdAppCtxBufPtr is a pointer into a cmdAppCtxBuf.
type cmdAppCtxBufPtr struct {
	idx  int32
	buf  *cmdAppCtxBuf
	node *cmdAppCtxBufNode
}

func (ptr *cmdAppCtxBufPtr) valid() bool {
	return ptr.idx < ptr.buf.len
}

func (ptr *cmdAppCtxBufPtr) cur() *cmdAppCtx {
	return &ptr.node.buf[ptr.idx%cmdAppCtxBufNodeSize]
}

func (ptr *cmdAppCtxBufPtr) next() {
	ptr.idx++
	if !ptr.valid() {
		return
	}
	if ptr.idx%cmdAppCtxBufNodeSize == 0 {
		ptr.node = ptr.node.next
	}
}

// cmdAppCtxBufSlice iterates through the entries in an cmdAppStateBuf.
type cmdAppCtxBufSlice struct {
	head, tail cmdAppCtxBufPtr
}

// init initializes the slice over the entries cmdAppCtxBuf.
func (it *cmdAppCtxBufSlice) init(buf *cmdAppCtxBuf) {
	*it = cmdAppCtxBufSlice{
		head: cmdAppCtxBufPtr{idx: 0, buf: buf, node: buf.head},
		tail: cmdAppCtxBufPtr{idx: buf.len, buf: buf, node: buf.tail},
	}
}

// initEmpty initializes the slice with a length of 0 and pointing at
// the head of the cmdAppCtxBuf.
func (it *cmdAppCtxBufSlice) initEmpty(buf *cmdAppCtxBuf) {
	*it = cmdAppCtxBufSlice{
		head: cmdAppCtxBufPtr{idx: 0, buf: buf, node: buf.head},
		tail: cmdAppCtxBufPtr{idx: 0, buf: buf, node: buf.head},
	}
}

// len returns the length of the slice.
func (it *cmdAppCtxBufSlice) len() int {
	return int(it.tail.idx - it.head.idx)
}

// Valid implements the apply.CommandIteratorBase interface.
func (it *cmdAppCtxBufSlice) Valid() bool {
	return it.len() > 0
}

// Next implements the apply.CommandIteratorBase interface.
func (it *cmdAppCtxBufSlice) Next() {
	it.head.next()
}

// cur and its variants implement the apply.{Checked,Applied}CommandIterator interface.
func (it *cmdAppCtxBufSlice) cur() *cmdAppCtx                  { return it.head.cur() }
func (it *cmdAppCtxBufSlice) Cur() apply.Command               { return it.head.cur() }
func (it *cmdAppCtxBufSlice) CurChecked() apply.CheckedCommand { return it.head.cur() }
func (it *cmdAppCtxBufSlice) CurApplied() apply.AppliedCommand { return it.head.cur() }

// append and its variants implement the apply.{Checked,Applied}CommandList interface.
func (it *cmdAppCtxBufSlice) append(cmd *cmdAppCtx) {
	cur := it.tail.cur()
	if cur == cmd {
		// Avoid the copy.
	} else {
		*cur = *cmd
	}
	it.tail.next()
}
func (it *cmdAppCtxBufSlice) Append(cmd apply.Command)               { it.append(cmd.(*cmdAppCtx)) }
func (it *cmdAppCtxBufSlice) AppendChecked(cmd apply.CheckedCommand) { it.append(cmd.(*cmdAppCtx)) }
func (it *cmdAppCtxBufSlice) AppendApplied(cmd apply.AppliedCommand) { it.append(cmd.(*cmdAppCtx)) }

// newList and its variants implement the apply.{Checked}CommandIterator interface.
func (it *cmdAppCtxBufSlice) newList() *cmdAppCtxBufSlice {
	it2 := it.head.buf.newIter()
	it2.initEmpty(it.head.buf)
	return it2
}
func (it *cmdAppCtxBufSlice) NewList() apply.CommandList               { return it.newList() }
func (it *cmdAppCtxBufSlice) NewCheckedList() apply.CheckedCommandList { return it.newList() }
func (it *cmdAppCtxBufSlice) NewAppliedList() apply.AppliedCommandList { return it.newList() }

// Close implements the apply.CommandIteratorBase interface.
func (it *cmdAppCtxBufSlice) Close() {
	it.head.buf.free.put(it)
}

// cmdAppCtxBufSliceFreeList is a free list of cmdAppCtxBufSlice objects that is
// used to avoid memory allocations for short-lived cmdAppCtxBufSlice objects
// that require heap allocation.
type cmdAppCtxBufSliceFreeList struct {
	iters [cmdAppCtxBufSliceFreeListSize]cmdAppCtxBufSlice
	inUse [cmdAppCtxBufSliceFreeListSize]bool
}

func (f *cmdAppCtxBufSliceFreeList) put(it *cmdAppCtxBufSlice) {
	*it = cmdAppCtxBufSlice{}
	for i := range f.iters {
		if &f.iters[i] == it {
			f.inUse[i] = false
			return
		}
	}
}

func (f *cmdAppCtxBufSliceFreeList) get() *cmdAppCtxBufSlice {
	for i, inUse := range f.inUse {
		if !inUse {
			f.inUse[i] = true
			return &f.iters[i]
		}
	}
	panic("cmdAppCtxBufSliceFreeList has no free elements. Is cmdAppCtxBufSliceFreeListSize " +
		"tuned properly? Are we leaking iterators by not calling Close?")
}
