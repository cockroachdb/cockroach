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
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	shardCount  = 256
	channelSize = 128
)

// Cache stores the mapping from the Transaction IDs (UUID) of recently
// executed transactions to their corresponding Transaction Fingerprint ID (uint64).
// The size of Cache is controlled via sql.contention.txn_id_cache.max_size
// cluster setting, and it follows FIFO eviction policy once the cache size
// reaches the limit defined by the cluster setting.
//
// Cache's overall architecture is as follows:
//   +------------------------------------+
//   | connExecutor ---------*            |
//   |      +-------------+  | writes to  |
//   |      | Writer      |<-*            |
//   |      +-----+-------+               |
//   +------------|-----------------------+
//                |
//               when full, Writer flushes into a channel.
//                |
//                v
//              channel
//                ^
//                |
//              Cache runs a goroutine that polls the channel --*
//                |  and sends the message to its               |
//                |  corresponding shard                        |
//                |               *-----------------------------*
//   +----------------------------|---+
//   |    Cache                   |   |
//   |       |      +-------------v--------------------------------------*
//   |       *----> | shard1  [writeBuffer] ----------- when full ---*   |
//   |       |      |                                                |   |
//   |       |      |         [cache.UnorderedCache] <---- flush ----*   |
//   |       |      +----------------------------------------------------*
//   |       *----> shard2            |
//   |       |                        |
//   |       *----> shard2            |
//   |       |                        |
//   |       .......                  |
//   |       |                        |
//   |       *----> shard256          |
//   +--------------------------------+
type Cache struct {
	st *cluster.Settings

	msgChan chan messageBlock

	store storage
	pool  writerPool

	metrics *Metrics
}

var (
	entrySize = int64(uuid.UUID{}.Size()) +
		roachpb.TransactionFingerprintID(0).Size() + 8 // the size of a hash.

	_ Provider = &Cache{}
)

// NewTxnIDCache creates a new instance of Cache.
func NewTxnIDCache(st *cluster.Settings, metrics *Metrics) *Cache {
	t := &Cache{
		st:      st,
		metrics: metrics,
		msgChan: make(chan messageBlock, channelSize),
	}

	t.pool = newWriterPoolImpl(func() Writer {
		return t.createNewWriteBuffer()
	})

	t.store = newShardedCache(func() storage {
		return newTxnIDCacheShard(t)
	})

	return t
}

// Start implements the Provider interface.
func (t *Cache) Start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "txn-id-cache-ingest", func(ctx context.Context) {
		for {
			select {
			case msgBlock := <-t.msgChan:
				for i := range msgBlock.data {
					t.store.Record(msgBlock.data[i])
				}
				if msgBlock.forceImmediateFlushIntoCache {
					t.store.Flush()
				}
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	})
}

// Lookup implements the reader interface.
func (t *Cache) Lookup(txnID uuid.UUID) (result roachpb.TransactionFingerprintID, found bool) {
	return t.store.Lookup(txnID)
}

// GetWriter implements the writerPool interface.
func (t *Cache) GetWriter() Writer {
	return t.pool.GetWriter()
}

// push implements the messageSink interface.
func (t *Cache) push(msg messageBlock) {
	select {
	case t.msgChan <- msg:
		// noop.
	default:
		t.metrics.DiscardedResolvedTxnIDCount.Inc(messageBlockSize)
	}
}

// disconnect implements the messageSink interface.
func (t *Cache) disconnect(w Writer) {
	t.pool.disconnect(w)
}

func (t *Cache) createNewWriteBuffer() *ConcurrentWriteBuffer {
	writeBuffer := &ConcurrentWriteBuffer{
		sink: t,
	}
	writeBuffer.flushDone.L = writeBuffer.flushSyncLock.RLocker()
	return writeBuffer
}

// FlushActiveWritersForTest is only used in test to flush all writers so the
// content of the Cache can be inspected.
func (t *Cache) FlushActiveWritersForTest() {
	poolImpl := t.pool.(*writerPoolImpl)
	poolImpl.mu.Lock()
	defer poolImpl.mu.Unlock()
	for txnIDCacheWriteBuffer := range poolImpl.mu.activeWriters {
		txnIDCacheWriteBuffer.(*ConcurrentWriteBuffer).flushForTest()
	}
}

func hashTxnID(txnID uuid.UUID) int {
	b := txnID.GetBytes()
	b, val1, err := encoding.DecodeUint64Descending(b)
	if err != nil {
		panic(err)
	}
	_, val2, err := encoding.DecodeUint64Descending(b)
	if err != nil {
		panic(err)
	}
	return int((val1 ^ val2) % shardCount)
}
