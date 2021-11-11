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
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const cacheShardWriteBufferSize = 24

// cacheShard is a shard of the txnidcache.Cache. The idea behind sharding
// the cache is to reduce the mutex contention.
type cacheShard struct {
	mu struct {
		syncutil.RWMutex
		store *cache.UnorderedCache
	}

	atomic struct {
		allocatedMem int64
	}
	writeBuffer []ResolvedTxnID
	txnIDCache  *Cache
}

var _ storage = &cacheShard{}

type ResolvedTxnID struct {
	TxnID            uuid.UUID
	TxnFingerprintID roachpb.TransactionFingerprintID
}

func newTxnIDCacheShard(t *Cache) *cacheShard {
	shard := &cacheShard{
		writeBuffer: make([]ResolvedTxnID, 0, cacheShardWriteBufferSize),
		txnIDCache:  t,
	}
	shard.mu.store = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(_ int, _, _ interface{}) bool {
			limit := MaxSize.Get(&t.st.SV)
			limitPerShard := limit / shardCount
			return atomic.LoadInt64(&shard.atomic.allocatedMem) > limitPerShard
		},
		OnEvictedEntry: func(_ *cache.Entry) {
			atomic.AddInt64(&shard.atomic.allocatedMem, -entrySize)
			t.metrics.TxnIDCacheSize.Dec(entrySize)
			t.metrics.EvictedTxnIDCacheCount.Inc(1)
		},
	})
	return shard
}

func (s *cacheShard) flushLocked() {
	memUsed := entrySize * int64(len(s.writeBuffer))
	atomic.AddInt64(&s.atomic.allocatedMem, memUsed)
	s.txnIDCache.metrics.TxnIDCacheSize.Inc(memUsed)

	for _, v := range s.writeBuffer {
		s.mu.store.Add(v.TxnID, v.TxnFingerprintID)
	}
	s.writeBuffer = s.writeBuffer[:0]
}

// Record implements the writer interface.
func (s *cacheShard) Record(msg ResolvedTxnID) {
	s.writeBuffer = append(s.writeBuffer, msg)
	if len(s.writeBuffer) == cap(s.writeBuffer) {
		s.Flush()
	}
}

// Flush implements the writer interface.
func (s *cacheShard) Flush() {
	s.mu.Lock()
	s.flushLocked()
	s.mu.Unlock()
}

// Lookup implements the reader interface.
func (s *cacheShard) Lookup(txnID uuid.UUID) (result roachpb.TransactionFingerprintID, found bool) {
	s.mu.RLock()
	value, found := s.mu.store.Get(txnID)
	s.mu.RUnlock()
	if !found {
		return result, found
	}
	return value.(roachpb.TransactionFingerprintID), found
}
