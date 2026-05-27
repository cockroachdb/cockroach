// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enrichment

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contention/contentionutils"
)

// blockSize is the number of entries packed into a single write block.
// Each entry holds a 16-byte key plus an Attributes struct (currently a
// little over 100 bytes including the slice/string headers), so 32
// entries keeps a block under ~4 KiB and roughly within a page.
const blockSize = 32

// block is a fixed-size batch of entries that flows through the cache.
// Writes from the executor accumulate into a block; when the block fills
// up it is pushed to the cache's ingestion channel and a fresh block is
// pulled from the pool.
type block [blockSize]entry

var blockPool = &sync.Pool{
	New: func() interface{} {
		return &block{}
	},
}

// concurrentWriteBuffer is a single shard of the writer. It uses a
// ConcurrentBufferGuard to absorb concurrent writes without blocking the
// hot path: each writer atomically reserves a slot in the current block
// and writes into it. When a writer reserves the last slot it flushes
// the block to the sink and rotates in a fresh one.
type concurrentWriteBuffer struct {
	guard struct {
		*contentionutils.ConcurrentBufferGuard

		// block is the current target for incoming writes. It is
		// rotated by the onBufferFull callback.
		block *block
	}

	// sink receives full blocks. The cache implements blockSink and
	// forwards them to its FIFO store via a buffered channel.
	sink blockSink
}

func newConcurrentWriteBuffer(sink blockSink) *concurrentWriteBuffer {
	b := &concurrentWriteBuffer{sink: sink}
	b.guard.block = blockPool.Get().(*block)
	b.guard.ConcurrentBufferGuard = contentionutils.NewConcurrentBufferGuard(
		func() int64 { return blockSize },
		func(_ int64) {
			b.sink.push(b.guard.block)
			b.guard.block = blockPool.Get().(*block)
		})
	return b
}

// record stores e in the current block. It is safe to call concurrently.
func (b *concurrentWriteBuffer) record(id clusterunique.ID, attrs Attributes) {
	b.guard.AtomicWrite(func(writerIdx int64) {
		b.guard.block[writerIdx] = entry{Key: id, Attrs: attrs}
	})
}

// drain flushes the in-flight block to the sink, even if it isn't full.
func (b *concurrentWriteBuffer) drain() {
	b.guard.ForceSync()
}
