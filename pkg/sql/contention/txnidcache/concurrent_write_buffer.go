// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package txnidcache

import (
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const messageBlockSize = 1024

type messageBlock [messageBlockSize]ResolvedTxnID

// concurrentWriteBuffer is a data structure that optimizes for concurrent
// writes and also implements the Writer interface.
//
// Any write requests initially starts by holding a read lock (flushSyncLock)
// and then reserves an index to the messageBlock (a fixed-length array). If the
// reserved index is valid, concurrentWriteBuffer immediately writes to the
// array at the reserved index. However, if the reserved index is not valid,
// (that is, array index out of bound), there are two scenarios:
// 1. If the reserved index == size of the array, then the caller of Record()
//    method is responsible for flushing the entire array into the channel. The
//    caller does so by upgrading the read-lock to a write-lock, therefore
//    blocks all future writers from writing to the shared array. After the
//    flush is performed, the write-lock is then downgraded to a read-lock.
// 2. If the reserved index > size of the array, then the caller of Record()
//    is blocked until the array is flushed. This is achieved by waiting on the
//    conditional variable (flushDone) while holding onto the read-lock. After
//    the flush is completed, the writer is unblocked and allowed to retry.
type concurrentWriteBuffer struct {
	flushSyncLock syncutil.RWMutex
	flushDone     sync.Cond

	msgBlockPool *sync.Pool

	// msgBlock is the temporary buffer that concurrentWriteBuffer uses to batch
	// write requests before sending them into the channel.
	msgBlock *messageBlock

	// atomicIdx is the index pointing into the fixed-length array within the
	// msgBlock.This should only be accessed using atomic package.
	atomicIdx int64

	// sink is the flush target that concurrentWriteBuffer flushes to once
	// msgBlock is full.
	sink messageSink
}

var _ Writer = &concurrentWriteBuffer{}

// newConcurrentWriteBuffer returns a new instance of concurrentWriteBuffer.
func newConcurrentWriteBuffer(sink messageSink, msgBlockPool *sync.Pool) *concurrentWriteBuffer {
	writeBuffer := &concurrentWriteBuffer{
		sink:         sink,
		msgBlockPool: msgBlockPool,
		msgBlock:     msgBlockPool.Get().(*messageBlock),
	}
	writeBuffer.flushDone.L = writeBuffer.flushSyncLock.RLocker()
	return writeBuffer
}

// Record records a mapping from txnID to its corresponding transaction
// fingerprint ID. Record is safe to be used concurrently.
func (c *concurrentWriteBuffer) Record(resolvedTxnID ResolvedTxnID) {
	c.flushSyncLock.RLock()
	defer c.flushSyncLock.RUnlock()
	for {
		reservedIdx := c.reserveMsgBlockIndex()
		if reservedIdx < messageBlockSize {
			c.msgBlock[reservedIdx] = resolvedTxnID
			return
		} else if reservedIdx == messageBlockSize {
			c.flushMsgBlockToChannelRLocked()
		} else {
			c.flushDone.Wait()
		}
	}
}

// Flush flushes concurrentWriteBuffer into the channel. It implements the
// txnidcache.Writer interface.
func (c *concurrentWriteBuffer) Flush() {
	c.flushSyncLock.Lock()
	c.flushMsgBlockToChannelLocked()
	c.flushSyncLock.Unlock()
}

func (c *concurrentWriteBuffer) flushMsgBlockToChannelRLocked() {
	// We upgrade the read-lock to a write-lock, then when we are done flushing,
	// the lock is downgraded to a read-lock.
	c.flushSyncLock.RUnlock()
	defer c.flushSyncLock.RLock()
	c.flushSyncLock.Lock()
	defer c.flushSyncLock.Unlock()
	c.flushMsgBlockToChannelLocked()
}

func (c *concurrentWriteBuffer) flushMsgBlockToChannelLocked() {
	c.sink.push(c.msgBlock)
	c.msgBlock = c.msgBlockPool.Get().(*messageBlock)
	c.flushDone.Broadcast()
	atomic.StoreInt64(&c.atomicIdx, 0)
}

func (c *concurrentWriteBuffer) reserveMsgBlockIndex() int64 {
	return atomic.AddInt64(&c.atomicIdx, 1) - 1 // since array is 0-indexed.
}
