// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TxnFingerprintIDCacheCapacity is the cluster setting that controls the
// capacity of the txn fingerprint ID cache. The cache will be resized
// on the next insert or get operation.
var TxnFingerprintIDCacheCapacity = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.txn_fingerprint_id_cache.capacity",
	"the maximum number of txn fingerprint IDs stored",
	100,
	settings.NonNegativeInt,
).WithPublic()

// TxnFingerprintIDCache is a thread-safe cache tracking transaction
// fingerprint IDs at the session level.
type TxnFingerprintIDCache struct {
	st *cluster.Settings

	mu struct {
		syncutil.RWMutex
		acc   *mon.BoundAccount
		cache *cache.UnorderedCache
	}

	mon *mon.BytesMonitor
}

// NewTxnFingerprintIDCache returns a new TxnFingerprintIDCache.
func NewTxnFingerprintIDCache(
	st *cluster.Settings, parentMon *mon.BytesMonitor,
) *TxnFingerprintIDCache {
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
		OnEvictedEntry: func(entry *cache.Entry) {
			b.mu.acc.Shrink(context.Background(), 1)
		},
	})

	monitor := mon.NewMonitorInheritWithLimit("txn-fingerprint-id-cache", 0 /* limit */, parentMon)
	b.mon = monitor
	b.mon.StartNoReserved(context.Background(), parentMon)

	return b
}

// Add adds a TxnFingerprintID to the cache, truncating the cache to the cache's capacity
// if necessary.
func (b *TxnFingerprintIDCache) Add(value appstatspb.TransactionFingerprintID) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.mu.acc.Grow(context.Background(), 1); err != nil {
		return err
	}

	b.mu.cache.Add(value, value)

	return nil
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
		id := entry.Value.(appstatspb.TransactionFingerprintID)

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
