// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt

package recent

import (
	"context"
	"sync"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TransactionsCacheCapacity is the cluster setting that controls the
// maximum number of recent Transactions in the cache.
var TransactionsCacheCapacity = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.recent.transactions_cache.capacity",
	"the maximum number of Transactions in the cache",
	20000,
	settings.NonNegativeIntWithMaximum(100000),
).WithPublic()

// TransactionsCacheTimeToLive is the cluster setting that controls the
// maximum time to live that a statement remains in the cache.
var TransactionsCacheTimeToLive = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.recent.transactions_cache.time_to_live",
	"the maximum time to live",
	time.Hour,
	settings.NonNegativeDurationWithMaximum(time.Hour*24),
).WithPublic()

// TransactionsCache is a FIFO cache for recent transactions.
type TransactionsCache struct {
	st      *cluster.Settings
	timeSrc timeSource

	mu struct {
		syncutil.RWMutex
		acc  mon.BoundAccount
		data *cache.UnorderedCache
	}

	mon *mon.BytesMonitor
}

// transactionsNode represents the value for each cache entry.
type transactionsNode struct {
	txn       serverpb.TxnInfo
	stmts     StatementsCache
	timestamp time.Time
}

// TransactionsPool allows for the "reuse" of TransactionsNodes.
// If there are available nodes in the pool, it returns those
// previously allocated nodes. If there aren't, it returns a newly
// created node. If a node is evicted, it is returned to the pool
// to be used again. This prevents the de-allocation and reallocation
// of nodes every time we add to the pool.
var transactionsPool = sync.Pool{
	New: func() interface{} {
		return new(transactionsNode)
	},
}

// NewTransactionsCache initializes and returns a new TransactionsCache.
func NewTransactionsCache(
	st *cluster.Settings, parentMon *mon.BytesMonitor, timeSrc timeSource,
) *TransactionsCache {
	monitor := mon.NewMonitorInheritWithLimit("recent-transactions-cache", 0, parentMon)

	c := &TransactionsCache{st: st, timeSrc: timeSrc}

	c.mu.data = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, _, value interface{}) bool {
			capacity := TransactionsCacheCapacity.Get(&st.SV)
			return int64(size) > capacity
		},
		OnEvicted: func(_, value interface{}) {
			node := value.(*transactionsNode)
			size := int64(node.size())
			transactionsPool.Put(node)
			c.mu.acc.Shrink(context.Background(), size)
		},
	})

	c.mu.acc = monitor.MakeBoundAccount()
	c.mon = monitor
	c.mon.StartNoReserved(context.Background(), parentMon)
	return c
}

// Add stores an ActiveQuery in the cache.
func (rc *TransactionsCache) Add(
	ctx context.Context, txn serverpb.TxnInfo, stmt serverpb.ActiveQuery,
) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	txnNode, ok := rc.mu.data.Get(txn.ID)
	if ok {
		_ = txnNode.(*transactionsNode).stmts.Add(ctx, stmt)
	} else {
		// Get a blank transactionsNode from the transactionsPool
		node := transactionsPool.Get().(*transactionsNode)
		*node = transactionsNode{
			txn:       txn,
			stmts:     *NewStatementsCache(rc.st, rc.mon, rc.timeSrc),
			timestamp: rc.timeSrc(),
		}
		_ = node.stmts.Add(ctx, stmt)
		rc.mu.data.Add(txn.ID, node)
	}
	// TODO: add bytes monitor
	//if err := rc.mu.acc.Grow(ctx, int64(node.size())); err != nil {
	//	return err
	//}
	return nil
}

// IterateRecentTransactions iterates through all the transactions in
// the cache, evicting those that have reached the TTL.
func (rc *TransactionsCache) IterateRecentTransactions(
	ctx context.Context, visitor func(context.Context, *serverpb.TxnInfo, *StatementsCache),
) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	toEvict := ""
	rc.mu.data.Do(func(entry *cache.Entry) {
		if toEvict != "" {
			rc.mu.data.Del(toEvict)
			toEvict = ""
		}
		node := entry.Value.(*transactionsNode)
		if rc.pastTTL(node.timestamp) {
			// TODO: fix eviction policy to be during ShouldEvict
			// We keep track of the entry to evict so
			// that we can evict on the next iteration.
			// Evicting during this iteration does not
			// work since the current entry is still needed
			// during the next iteration to find the next entry.
			toEvict = entry.Key.(string)
		} else {
			visitor(ctx, &node.txn, &node.stmts)
		}
	})
}

func (rc *TransactionsCache) pastTTL(timestamp time.Time) bool {
	ttl := TransactionsCacheTimeToLive.Get(&rc.st.SV)
	return rc.timeSrc().UnixNano()-timestamp.UnixNano() > ttl.Nanoseconds()
}

func (n *transactionsNode) size() int {
	size := 0
	size += int(unsafe.Sizeof(clusterunique.ID{}))
	size += n.txn.Size()
	// TODO: implement finding size of statements cache
	//size += n.stmts.Size
	size += int(unsafe.Sizeof(time.Time{}))
	return size
}
