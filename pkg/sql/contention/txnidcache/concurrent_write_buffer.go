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

	"github.com/cockroachdb/cockroach/pkg/sql/contention/contentionutils"
)

const messageBlockSize = 1024

type messageBlock [messageBlockSize]ResolvedTxnID

// concurrentWriteBuffer is a data structure that optimizes for concurrent
// writes and also implements the Writer interface.
type concurrentWriteBuffer struct {
	guard *contentionutils.ConcurrentBufferGuard

	msgBlockPool *sync.Pool

	// msgBlock is the temporary buffer that concurrentWriteBuffer uses to batch
	// write requests before sending them into the channel.
	msgBlock *messageBlock

	// sink is the flush target that ConcurrentWriteBuffer flushes to once
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
	writeBuffer.guard = contentionutils.NewConcurrentBufferGuard(
		func() int64 {
			return messageBlockSize
		}, /* limiter */
		func(_ int64) {
			writeBuffer.sink.push(writeBuffer.msgBlock)

			// Resets the msgBlock.
			writeBuffer.msgBlock = writeBuffer.msgBlockPool.Get().(*messageBlock)

		} /* onBufferFull */)

	return writeBuffer
}

// Record records a mapping from txnID to its corresponding transaction
// fingerprint ID. Record is safe to be used concurrently.
func (c *concurrentWriteBuffer) Record(resolvedTxnID ResolvedTxnID) {
	c.guard.AtomicWrite(func(writerIdx int64) {
		c.msgBlock[writerIdx] = resolvedTxnID
	})
}

// Flush flushes concurrentWriteBuffer into the channel. It implements the
// txnidcache.Writer interface.
func (c *concurrentWriteBuffer) Flush() {
	c.guard.ForceSync()
}
