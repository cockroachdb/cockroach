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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Reader is the interface that can be used to query the transaction fingerprint
// ID of a transaction ID.
type Reader interface {
	// Lookup returns the corresponding transaction fingerprint ID for a given txnID,
	// if the given txnID has no entry in the Cache, the returned "found" boolean
	// will be false.
	Lookup(txnID uuid.UUID) (result roachpb.TransactionFingerprintID, found bool)
}

// Writer is the interface that can be used to write to txnidcache.
type Writer interface {
	// Record writes a pair of transactionID and transaction fingerprint ID
	// into a temporary buffer. This buffer will eventually be flushed into
	// the transaction ID cache asynchronously.
	Record(resolvedTxnID ResolvedTxnID)

	// Flush starts the flushing process of writer's temporary buffer.
	Flush()
}

type messageSink interface {
	// push allows a messageBlock to be pushed into the pusher.
	push(*messageBlock)
}

const channelSize = 128

// Cache stores the mapping from the Transaction IDs (UUID) of recently
// executed transactions to their corresponding Transaction Fingerprint ID (uint64).
// The size of Cache is controlled via sql.contention.txn_id_cache.max_size
// cluster setting, and it follows FIFO eviction policy once the cache size
// reaches the limit defined by the cluster setting.
//
// Cache's overall architecture is as follows:
//   +------------------------------------------------------------+
//   | connExecutor  --------*                                    |
//   |                       |  writes resolvedTxnID to Writer    |
//   |                       v                                    |
//   |      +---------------------------------------------------+ |
//   |      | Writer                                            | |
//   |      |                                                   | |
//   |      |  Writer contains multiple shards of concurrent    | |
//   |      |  write buffer. Each incoming resolvedTxnID is     | |
//   |      |  first hashed to a corresponding shard, and then  | |
//   |      |  is written to the concurrent write buffer        | |
//   |      |  backing that shard. Once the concurrent write    | |
//   |      |  buffer is full, a flush is performed and the     | |
//   |      |  content of the buffer is send into the channel.  | |
//   |      |                                                   | |
//   |      | +------------+                                    | |
//   |      | | shard1     |                                    | |
//   |      | +------------+                                    | |
//   |      | | shard2     |                                    | |
//   |      | +------------+                                    | |
//   |      | | shard3     |                                    | |
//   |      | +------------+                                    | |
//   |      | | .....      |                                    | |
//   |      | | .....      |                                    | |
//   |      | +------------+                                    | |
//   |      | | shard128   |                                    | |
//   |      | +------------+                                    | |
//   |      |                                                   | |
//   |      +-----+---------------------------------------------+ |
//   +------------|-----------------------------------------------+
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

	msgChan chan *messageBlock
	closeCh chan struct{}

	store  *fifoCache
	writer Writer

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
	_ Reader      = &Cache{}
	_ Writer      = &Cache{}
	_ messageSink = &Cache{}
)

// NewTxnIDCache creates a new instance of Cache.
func NewTxnIDCache(st *cluster.Settings, metrics *Metrics) *Cache {
	t := &Cache{
		st:      st,
		metrics: metrics,
		msgChan: make(chan *messageBlock, channelSize),
		closeCh: make(chan struct{}),
	}

	t.store = newFIFOCache(func() int64 {
		return MaxSize.Get(&st.SV) / entrySize
	} /* capacity */)

	t.writer = newWriter(st, t)
	return t
}

// Start implements the Provider interface.
func (t *Cache) Start(ctx context.Context, stopper *stop.Stopper) {
	err := stopper.RunAsyncTask(ctx, "txn-id-cache-ingest", func(ctx context.Context) {
		for {
			select {
			case msgBlock := <-t.msgChan:
				t.store.Add(msgBlock)
			case <-stopper.ShouldQuiesce():
				close(t.closeCh)
				return
			}
		}
	})
	if err != nil {
		close(t.closeCh)
	}
}

// Lookup implements the Reader interface.
func (t *Cache) Lookup(txnID uuid.UUID) (result roachpb.TransactionFingerprintID, found bool) {
	t.metrics.CacheReadCounter.Inc(1)

	txnFingerprintID, found := t.store.Get(txnID)
	if !found {
		t.metrics.CacheMissCounter.Inc(1)
		return roachpb.InvalidTransactionFingerprintID, found
	}

	return txnFingerprintID, found
}

// Record implements the Writer interface.
func (t *Cache) Record(resolvedTxnID ResolvedTxnID) {
	t.writer.Record(resolvedTxnID)
}

// push implements the messageSink interface.
func (t *Cache) push(msg *messageBlock) {
	select {
	case t.msgChan <- msg:
	case <-t.closeCh:
	}
}

// Flush flushes the resolved txn IDs in the Writer into the Cache.
func (t *Cache) Flush() {
	t.writer.Flush()
}

// Size return the current size of the Cache.
func (t *Cache) Size() int64 {
	return int64(t.store.Size()) * entrySize
}
