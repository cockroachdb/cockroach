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
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
//                |   messageBlock into its storage.
//                |
//   +----------------------------------+
//   |    Cache:                        |
//   |     The cache contains a         |
//   |     FIFO buffer backed by        |
//   |     cache.UnorderedCache         |
//   +----------------------------------+
type Cache struct {
	st *cluster.Settings

	msgChan chan messageBlock

	mu struct {
		syncutil.RWMutex

		store *cache.UnorderedCache
	}
	pool writerPool

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
	return r.TxnFingerprintID != roachpb.InvalidTransactionFingerprintID
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

	t.mu.store = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, _, _ interface{}) bool {
			return int64(size)*entrySize > MaxSize.Get(&st.SV)
		},
		OnEvictedEntry: func(_ *cache.Entry) {
			metrics.EvictedTxnIDCacheCount.Inc(1)
		},
	})

	return t
}

// Start implements the Provider interface.
func (t *Cache) Start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "txn-id-cache-ingest", func(ctx context.Context) {
		for {
			select {
			case msgBlock := <-t.msgChan:
				func() {
					t.mu.Lock()
					defer t.mu.Unlock()
					for blockIdx := range msgBlock {
						if !msgBlock[blockIdx].valid() {
							break
						}
						t.mu.store.Add(msgBlock[blockIdx].TxnID, msgBlock[blockIdx].TxnFingerprintID)
					}
				}()
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	})
}

// Lookup implements the reader interface.
func (t *Cache) Lookup(txnID uuid.UUID) (result roachpb.TransactionFingerprintID, found bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	txnFingerprintID, ok := t.mu.store.Get(txnID)
	if !ok {
		return roachpb.InvalidTransactionFingerprintID, false /* found */
	}

	return txnFingerprintID.(roachpb.TransactionFingerprintID), true /* found */
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

func (t *Cache) Size() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return int64(t.mu.store.Len()) * entrySize
}
