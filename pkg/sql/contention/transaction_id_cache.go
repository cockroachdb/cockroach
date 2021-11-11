// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package contention

import (
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// TxnIDCache stores the mapping from the Transaction IDs (UUID) of recently
// executed transactions to their corresponding Transaction Fingerprint ID (uint64).
// The size of TxnIDCache is controlled via sql.contention.txn_id_cache.max_size
// cluster setting, and it follows FIFO eviction policy once the cache size
// reaches the limit defined by the cluster setting.
type TxnIDCache struct {
	st *cluster.Settings

	mu struct {
		syncutil.RWMutex
		store *cache.UnorderedCache
	}

	atomic struct {
		allocatedMem int64
	}

	metrics *Metrics
}

var (
	entrySize = int64(uuid.UUID{}.Size()) +
		roachpb.TransactionFingerprintID(0).Size() + 8 // the size of a hash.
)

// NewTxnIDCache creates a new instance of TxnIDCache.
func NewTxnIDCache(st *cluster.Settings, metrics *Metrics) *TxnIDCache {
	t := &TxnIDCache{
		st:      st,
		metrics: metrics,
	}

	t.atomic.allocatedMem = 0
	t.mu.store = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(_ int, _, _ interface{}) bool {
			return atomic.LoadInt64(&t.atomic.allocatedMem) > TxnIDCacheMaxSize.Get(&t.st.SV)
		},
		OnEvictedEntry: func(_ *cache.Entry) {
			atomic.AddInt64(&t.atomic.allocatedMem, -entrySize)
			t.metrics.TxnIDCacheSize.Dec(entrySize)
		},
	})

	return t
}

// Record records a mapping from txnID to its corresponding transaction
// fingerprint ID.
func (t *TxnIDCache) Record(txnID uuid.UUID, txnFingerprintID roachpb.TransactionFingerprintID) {
	t.mu.Lock()
	t.mu.store.Add(txnID, txnFingerprintID)
	t.mu.Unlock()
	atomic.AddInt64(&t.atomic.allocatedMem, entrySize)
	t.metrics.TxnIDCacheSize.Inc(entrySize)
}

// Lookup returns the corresponding transaction fingerprint ID for a given txnID,
// if the given txnID has no entry in the TxnIDCache, the returned "found" boolean
// will be false.
func (t *TxnIDCache) Lookup(txnID uuid.UUID) (result roachpb.TransactionFingerprintID, found bool) {
	t.mu.RLock()
	value, found := t.mu.store.Get(txnID)
	t.mu.RUnlock()
	if !found {
		return result, found
	}

	return value.(roachpb.TransactionFingerprintID), found
}
