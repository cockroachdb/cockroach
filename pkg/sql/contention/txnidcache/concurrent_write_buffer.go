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

import "github.com/cockroachdb/cockroach/pkg/sql/contention/contentionutils"

const messageBlockSize = 512

type messageBlock [messageBlockSize]ResolvedTxnID

// ConcurrentWriteBuffer is a data structure that optimizes for concurrent
// writes and also implements the Writer interface.
type ConcurrentWriteBuffer struct {
	guard *contentionutils.ConcurrentBufferGuard

	// msgBlock is the temporary buffer that ConcurrentWriteBuffer uses to batch
	// write requests before sending them into the channel.
	msgBlock messageBlock

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
	writeBuffer.guard = contentionutils.NewConcurrentBufferGuard(
		func() int64 {
			return messageBlockSize
		}, /* limiter */
		func(_ int64) {
			writeBuffer.sink.push(writeBuffer.msgBlock)

			// Resets the msgBlock.
			writeBuffer.msgBlock = messageBlock{}
		} /* onBufferFull */)

	return writeBuffer
}

// Record records a mapping from txnID to its corresponding transaction
// fingerprint ID. Record is safe to be used concurrently.
func (c *ConcurrentWriteBuffer) Record(resolvedTxnID ResolvedTxnID) {
	c.guard.AtomicWrite(func(writerIdx int64) {
		c.msgBlock[writerIdx] = resolvedTxnID
	})
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
	c.guard.ForceSync()
}
