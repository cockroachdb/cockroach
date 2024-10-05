// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TxnFingerprintIDCacheCapacity is the cluster setting that controls the
// capacity of the txn fingerprint ID cache. The cache will be resized
// on the next insert or get operation.
var TxnFingerprintIDCacheCapacity = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.txn_fingerprint_id_cache.capacity",
	"the maximum number of txn fingerprint IDs stored",
	100,
	settings.NonNegativeInt,
	settings.WithPublic)

// TxnFingerprintIDCache is a thread-safe cache tracking transaction
// fingerprint IDs at the session level.
type TxnFingerprintIDCache struct {
	st *cluster.Settings

	mu struct {
		syncutil.RWMutex
		cache *cache.UnorderedCache
	}
}

// NewTxnFingerprintIDCache returns a new TxnFingerprintIDCache.
func NewTxnFingerprintIDCache(st *cluster.Settings) *TxnFingerprintIDCache {
	b := &TxnFingerprintIDCache{st: st}
	b.mu.cache = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, _, _ interface{}) bool {
			// Note that because the cache evicts as many elements as possible
			// when adding an element, the cache will appropriately truncate
			// when the capacity cluster setting is changed on the addition
			// of an entry.
			capacity := TxnFingerprintIDCacheCapacity.Get(&st.SV)
			return int64(size) > capacity
		},
	})
	return b
}

// Add adds a TxnFingerprintID to the cache, truncating the cache to the cache's
// capacity if necessary.
func (b *TxnFingerprintIDCache) Add(id appstatspb.TransactionFingerprintID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// TODO(yuzefovich): we should perform memory accounting for this. Note that
	// it can be quite tricky to get right - see #121844.
	b.mu.cache.Add(id, nil /* value */)
}

// GetAllTxnFingerprintIDs returns a slice of all TxnFingerprintIDs in the cache.
// The cache may be truncated if the capacity was updated to a smaller size.
func (b *TxnFingerprintIDCache) GetAllTxnFingerprintIDs() []appstatspb.TransactionFingerprintID {
	b.mu.Lock()
	defer b.mu.Unlock()

	size := int64(b.mu.cache.Len())
	capacity := TxnFingerprintIDCacheCapacity.Get(&b.st.SV)
	if size > capacity {
		size = capacity
	}

	txnFingerprintIDs := make([]appstatspb.TransactionFingerprintID, 0, size)
	txnFingerprintIDsRemoved := make([]appstatspb.TransactionFingerprintID, 0)

	b.mu.cache.Do(func(entry *cache.Entry) {
		id := entry.Key.(appstatspb.TransactionFingerprintID)

		if int64(len(txnFingerprintIDs)) == size {
			txnFingerprintIDsRemoved = append(txnFingerprintIDsRemoved, id)
			return
		}

		txnFingerprintIDs = append(txnFingerprintIDs, id)
	})

	for _, id := range txnFingerprintIDsRemoved {
		b.mu.cache.Del(id)
	}

	return txnFingerprintIDs
}

func (b *TxnFingerprintIDCache) size() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.mu.cache.Len()
}
