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

const channelSize = 128

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
//               when full, Writer writes a messageBlock to a buffered channel.
//                |
//                |
//                V
//               channel
//                ^
//                |
//               Cache polls the channel using a goroutine and push the
//                |   messageBlock into its sharded map.
//                |
//   +----------------------------------+
//   |    Cache:                        |
//   |     The cache contains a         |
//   |     fixed number of shards.      |
//   |     Each TxnID is hashed         |
//   |     before writes to its         |
//   |     corresponding shard.         |
//   |                                  |
//   |       *----> shard1              |
//   |       |                          |
//   |       *----> shard2              |
//   |       |                          |
//   |       |      +-------------------------------------------------------------*
//   |       *----> | shard3: Each shard contain a ring buffer of strips,         |
//   |       |      |         the size of the ring buffer is fixed. All writes    |
//   |       |      |         go into the strip pointed by the "head" pointer.    |
//   |       |      |         Once a strip is full, "head" pointer is             |
//   |       |      |         incremented to point to the next strip. If the      |
//   |       |      |         ring buffer is full, the contents in the strip      |
//   |       |      |         pointed by the "tail" is discarded. This implements |
//   |       |      |         a coarse-grain FIFO eviction policy.                |
//   |       |      |                                                             |
//   |       |      |         tail               head                             |
//   |       |      |         V                  V                                |
//   |       |      |        +--------+--------+--------+----------------------------------+
//   |       |      |        | strip1 | strip2 | strip3 | strip4:                          |
//   |       |      |        +--------+--------+--------+    A strip is a fixed-size map.  |
//   |       |      |                                   |    Once it's full, strip will    |
//   |       |      |                                   |    refuse to accept any writes.  |
//   |       |      |                                   |    It's shard's responsibility   |
//   |       |      |                                   |    handle it (by incrementing    |
//   |       |      |                                   |    the head pointer and possibly |
//   |       |      |                                   |    evict the contents in the     |
//   |       |      |                                   |    strip.                        |
//   |       |      |                                   +----------------------------------+
//   |       |      |                                                             |
//   |       |      |                                                             |
//   |       |      +-------------------------------------------------------------*
//   |       |                          |
//   |       *----> shard4              |
//   |       |                          |
//   |       *----> shard5              |
//   |       |                          |
//   |       .......                    |
//   |       |                          |
//   |       *----> shard256            |
//   +----------------------------------+
type Cache struct {
	st *cluster.Settings

	msgChan chan messageBlock

	store storage
	pool  writerPool

	metrics *Metrics
}

var (
	entrySize = int64(uuid.UUID{}.Size()) +
		roachpb.TransactionFingerprintID(0).Size()
)

// ResolvedTxnID represents a TxnID that is resolved to its corresponding
// TxnFingerprintID.
type ResolvedTxnID struct {
	TxnID            uuid.UUID
	TxnFingerprintID roachpb.TransactionFingerprintID
}

func (r *ResolvedTxnID) valid() bool {
	return r.TxnID != uuid.UUID{}
}

var (
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
		return NewConcurrentWriteBuffer(t)
	})

	t.store = newShardedCache(func() storage {
		return newShard(func() int64 {
			return MaxSize.Get(&st.SV) / entrySize / shardCount
		}, defaultRingSize, metrics.EvictedTxnIDCacheCount)
	})

	return t
}

// Start implements the Provider interface.
func (t *Cache) Start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "txn-id-cache-ingest", func(ctx context.Context) {
		for {
			select {
			case msgBlock := <-t.msgChan:
				t.store.push(msgBlock)
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
