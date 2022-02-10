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

// blockSize is chosen as 168 since each ResolvedTxnID is 24 byte.
// 168 * 24 = 4032 bytes < 4KiB page size.
const blockSize = 168

type block [blockSize]ResolvedTxnID

func (m *block) isFull() bool {
	return m[blockSize-1].valid()
}

// concurrentWriteBuffer is a data structure that optimizes for concurrent
// writes and also implements the Writer interface.
type concurrentWriteBuffer struct {
	guard struct {
		*contentionutils.ConcurrentBufferGuard

		// block is the temporary buffer that concurrentWriteBuffer uses to batch
		// write requests before sending them into the channel.
		block *block
	}

	blockPool *sync.Pool

	// sink is the flush target that ConcurrentWriteBuffer flushes to once
	// block is full.
	sink blockSink
}

var _ Writer = &concurrentWriteBuffer{}

// newConcurrentWriteBuffer returns a new instance of concurrentWriteBuffer.
func newConcurrentWriteBuffer(sink blockSink, blockPool *sync.Pool) *concurrentWriteBuffer {
	writeBuffer := &concurrentWriteBuffer{
		sink:      sink,
		blockPool: blockPool,
	}

	writeBuffer.guard.block = blockPool.Get().(*block)
	writeBuffer.guard.ConcurrentBufferGuard = contentionutils.NewConcurrentBufferGuard(
		func() int64 {
			return blockSize
		}, /* limiter */
		func(_ int64) {
			writeBuffer.sink.push(writeBuffer.guard.block)

			// Resets the block.
			writeBuffer.guard.block = writeBuffer.blockPool.Get().(*block)
		} /* onBufferFull */)

	return writeBuffer
}

// Record records a mapping from txnID to its corresponding transaction
// fingerprint ID. Record is safe to be used concurrently.
func (c *concurrentWriteBuffer) Record(resolvedTxnID ResolvedTxnID) {
	c.guard.AtomicWrite(func(writerIdx int64) {
		c.guard.block[writerIdx] = resolvedTxnID
	})
}

// Flush flushes concurrentWriteBuffer into the channel. It implements the
// txnidcache.Writer interface.
func (c *concurrentWriteBuffer) Flush() {
	c.guard.ForceSync()
}
