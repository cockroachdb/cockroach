// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/apply"
)

// replica_application_*.go files provide concrete implementations of
// the interfaces defined in the storage/apply package:
//
// replica_application_state_machine.go  ->  apply.StateMachine
// replica_application_decoder.go        ->  apply.Decoder
// replica_application_cmd.go            ->  apply.Command         (and variants)
// replica_application_cmd_buf.go        ->  apply.CommandIterator (and variants)
// replica_application_cmd_buf.go        ->  apply.CommandList     (and variants)
//
// These allow Replica to interface with the storage/apply package.

// replicatedCmdBufNodeSize is the size of arrays in an replicatedCmdBufBufNode.
const replicatedCmdBufNodeSize = 8

// replicatedCmdBufSliceFreeListSize is the size of the replicatedCmdBufSlice
// free list. The size has been tuned for the maximum number of iterators that
// the storage/apply package uses at once. If this size is insufficient then the
// freelist will panic to ensure that regressions are loud.
const replicatedCmdBufSliceFreeListSize = 3

// replicatedCmdBuf is an allocation-efficient buffer used during the
// application of raft entries. Initialization occurs lazily upon the first
// call to allocate but used replicatedCmdBuf objects should be released
// explicitly with the clear() method to release the allocated buffers back
// to the pool.
type replicatedCmdBuf struct {
	len        int32
	head, tail *replicatedCmdBufNode
	free       replicatedCmdBufSliceFreeList
}

// replicatedCmdBufNode is a linked-list element in an replicatedCmdBufBuf.
type replicatedCmdBufNode struct {
	len  int32
	buf  [replicatedCmdBufNodeSize]replicatedCmd
	next *replicatedCmdBufNode
}

var replicatedCmdBufBufNodeSyncPool = sync.Pool{
	New: func() interface{} { return new(replicatedCmdBufNode) },
}

// allocate extends the length of buf by one and returns the newly added
// element. If this is the first call to allocate it will initialize buf.
// After a buf is initialized it should be explicitly destroyed.
func (buf *replicatedCmdBuf) allocate() *replicatedCmd {
	if buf.tail == nil { // lazy initialization
		n := replicatedCmdBufBufNodeSyncPool.Get().(*replicatedCmdBufNode)
		buf.head, buf.tail = n, n
	}
	if buf.tail.len == replicatedCmdBufNodeSize {
		newTail := replicatedCmdBufBufNodeSyncPool.Get().(*replicatedCmdBufNode)
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
func (buf *replicatedCmdBuf) clear() {
	for buf.head != nil {
		buf.len -= buf.head.len
		oldHead := buf.head
		newHead := oldHead.next
		buf.head = newHead
		*oldHead = replicatedCmdBufNode{}
		replicatedCmdBufBufNodeSyncPool.Put(oldHead)
	}
	*buf = replicatedCmdBuf{}
}

// newIter returns a pointer to a new uninitialized iterator. The iterator
// should be closed when no longer in use.
func (buf *replicatedCmdBuf) newIter() *replicatedCmdBufSlice {
	return buf.free.get()
}

// replicatedCmdBufPtr is a pointer into a replicatedCmdBuf.
type replicatedCmdBufPtr struct {
	idx  int32
	buf  *replicatedCmdBuf
	node *replicatedCmdBufNode
}

func (ptr *replicatedCmdBufPtr) valid() bool {
	return ptr.idx < ptr.buf.len
}

func (ptr *replicatedCmdBufPtr) cur() *replicatedCmd {
	return &ptr.node.buf[ptr.idx%replicatedCmdBufNodeSize]
}

func (ptr *replicatedCmdBufPtr) next() {
	ptr.idx++
	if !ptr.valid() {
		return
	}
	if ptr.idx%replicatedCmdBufNodeSize == 0 {
		ptr.node = ptr.node.next
	}
}

// replicatedCmdBufSlice iterates through the entries in a replicatedCmdBufBuf.
type replicatedCmdBufSlice struct {
	head, tail replicatedCmdBufPtr
}

// init initializes the slice over the entries in replicatedCmdBuf.
func (it *replicatedCmdBufSlice) init(buf *replicatedCmdBuf) {
	*it = replicatedCmdBufSlice{
		head: replicatedCmdBufPtr{idx: 0, buf: buf, node: buf.head},
		tail: replicatedCmdBufPtr{idx: buf.len, buf: buf, node: buf.tail},
	}
}

// initEmpty initializes the slice with a length of 0 and pointing at
// the head of the replicatedCmdBuf.
func (it *replicatedCmdBufSlice) initEmpty(buf *replicatedCmdBuf) {
	*it = replicatedCmdBufSlice{
		head: replicatedCmdBufPtr{idx: 0, buf: buf, node: buf.head},
		tail: replicatedCmdBufPtr{idx: 0, buf: buf, node: buf.head},
	}
}

// len returns the length of the slice.
func (it *replicatedCmdBufSlice) len() int {
	return int(it.tail.idx - it.head.idx)
}

// Valid implements the apply.CommandIteratorBase interface.
func (it *replicatedCmdBufSlice) Valid() bool {
	return it.len() > 0
}

// Next implements the apply.CommandIteratorBase interface.
func (it *replicatedCmdBufSlice) Next() {
	it.head.next()
}

// cur and its variants implement the apply.{Checked,Applied}CommandIterator interface.
func (it *replicatedCmdBufSlice) cur() *replicatedCmd              { return it.head.cur() }
func (it *replicatedCmdBufSlice) Cur() apply.Command               { return it.head.cur() }
func (it *replicatedCmdBufSlice) CurChecked() apply.CheckedCommand { return it.head.cur() }
func (it *replicatedCmdBufSlice) CurApplied() apply.AppliedCommand { return it.head.cur() }

// append and its variants implement the apply.{Checked,Applied}CommandList interface.
func (it *replicatedCmdBufSlice) append(cmd *replicatedCmd) {
	cur := it.tail.cur()
	if cur == cmd {
		// Avoid the copy.
	} else {
		*cur = *cmd
	}
	it.tail.next()
}
func (it *replicatedCmdBufSlice) Append(cmd apply.Command) { it.append(cmd.(*replicatedCmd)) }
func (it *replicatedCmdBufSlice) AppendChecked(cmd apply.CheckedCommand) {
	it.append(cmd.(*replicatedCmd))
}
func (it *replicatedCmdBufSlice) AppendApplied(cmd apply.AppliedCommand) {
	it.append(cmd.(*replicatedCmd))
}

// newList and its variants implement the apply.{Checked}CommandIterator interface.
func (it *replicatedCmdBufSlice) newList() *replicatedCmdBufSlice {
	it2 := it.head.buf.newIter()
	it2.initEmpty(it.head.buf)
	return it2
}
func (it *replicatedCmdBufSlice) NewList() apply.CommandList               { return it.newList() }
func (it *replicatedCmdBufSlice) NewCheckedList() apply.CheckedCommandList { return it.newList() }
func (it *replicatedCmdBufSlice) NewAppliedList() apply.AppliedCommandList { return it.newList() }

// Close implements the apply.CommandIteratorBase interface.
func (it *replicatedCmdBufSlice) Close() {
	it.head.buf.free.put(it)
}

// replicatedCmdBufSliceFreeList is a free list of replicatedCmdBufSlice
// objects that is used to avoid memory allocations for short-lived
// replicatedCmdBufSlice objects that require heap allocation.
type replicatedCmdBufSliceFreeList struct {
	iters [replicatedCmdBufSliceFreeListSize]replicatedCmdBufSlice
	inUse [replicatedCmdBufSliceFreeListSize]bool
}

func (f *replicatedCmdBufSliceFreeList) put(it *replicatedCmdBufSlice) {
	*it = replicatedCmdBufSlice{}
	for i := range f.iters {
		if &f.iters[i] == it {
			f.inUse[i] = false
			return
		}
	}
}

func (f *replicatedCmdBufSliceFreeList) get() *replicatedCmdBufSlice {
	for i, inUse := range f.inUse {
		if !inUse {
			f.inUse[i] = true
			return &f.iters[i]
		}
	}
	panic("replicatedCmdBufSliceFreeList has no free elements. Is " +
		"replicatedCmdBufSliceFreeListSize tuned properly? Are we " +
		"leaking iterators by not calling Close?")
}
