// Copyright 2021 The Cockroach Authors.
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

const messageBlockSize = 512

type messageBlock [messageBlockSize]ResolvedTxnID

// ConcurrentWriteBuffer is a data structure that optimizes for concurrent
// writes and also implements the Writer interface.
//
// Any write requests initially starts by holding a read lock (flushSyncLock)
// and then reserves an index to the messageBlock (a fixed-length array). If the
// reserved index is valid, ConcurrentWriteBuffer immediately writes to the
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
type ConcurrentWriteBuffer struct {
	flushSyncLock syncutil.RWMutex
	flushDone     sync.Cond

	// msgBlock is the temporary buffer that ConcurrentWriteBuffer uses to batch
	// write requests before sending them into the channel.
	msgBlock messageBlock

	// atomicIdx is the index pointing into the fixed-length array within the
	// msgBlock.This should only be accessed using atomic package.
	atomicIdx int64

	// sink is the flush target that ConcurrentWriteBuffer flushes to once
	// msgBlock is full.
	sink messageSink
}

var _ Writer = &ConcurrentWriteBuffer{}

// NewConcurrentWriteBuffer returns a new instance of ConcurrentWriteBuffer.
func NewConcurrentWriteBuffer(sink messageSink) *ConcurrentWriteBuffer {
	writeBuffer := &ConcurrentWriteBuffer{
		sink: sink,
	}
	writeBuffer.flushDone.L = writeBuffer.flushSyncLock.RLocker()
	return writeBuffer
}

// Record records a mapping from txnID to its corresponding transaction
// fingerprint ID. Record is safe to be used concurrently.
func (c *ConcurrentWriteBuffer) Record(resolvedTxnID ResolvedTxnID) {
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

// Close forces the ConcurrentWriteBuffer to be flushed into the channel and
// delete itself from txnidcache.Cache's list of active write buffer. This
// allows the ConcurrentWriteBuffer to be GC'd next time when garbage collector
// runs. It implements the txnidcache.Writer interface.
func (c *ConcurrentWriteBuffer) Close() {
	c.Flush()
	c.sink.disconnect(c)
}

// Flush flushes ConcurrentWriteBuffer into the channel. It implements the
// txnidcache.Writer interface.
func (c *ConcurrentWriteBuffer) Flush() {
	c.flushSyncLock.Lock()
	c.flushMsgBlockToChannelWLocked()
	c.flushSyncLock.Unlock()
}

func (c *ConcurrentWriteBuffer) flushMsgBlockToChannelRLocked() {
	// We upgrade the read-lock to a write-lock, then when we are done flushing,
	// the lock is downgraded to a read-lock.
	c.flushSyncLock.RUnlock()
	defer c.flushSyncLock.RLock()
	c.flushSyncLock.Lock()
	defer c.flushSyncLock.Unlock()
	c.flushMsgBlockToChannelWLocked()
}

func (c *ConcurrentWriteBuffer) flushMsgBlockToChannelWLocked() {
	c.sink.push(c.msgBlock)
	c.msgBlock = messageBlock{}
	c.flushDone.Broadcast()
	atomic.StoreInt64(&c.atomicIdx, 0)
}

func (c *ConcurrentWriteBuffer) flushForTest() {
	c.flushSyncLock.Lock()
	c.flushMsgBlockToChannelWLocked()
	c.flushSyncLock.Unlock()
}

func (c *ConcurrentWriteBuffer) reserveMsgBlockIndex() int64 {
	return atomic.AddInt64(&c.atomicIdx, 1) - 1 // since array is 0-indexed.
}
