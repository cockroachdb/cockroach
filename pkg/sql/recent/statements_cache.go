// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

type timeSource func() time.Time

// StatementsCacheCapacity is the cluster setting that controls the
// maximum number of recent statements in the cache.
var StatementsCacheCapacity = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.recent.statements_cache.capacity",
	"the maximum number of statements in the cache",
	20000,
	settings.NonNegativeIntWithMaximum(100000),
).WithPublic()

// StatementsCacheTimeToLive is the cluster setting that controls the
// maximum time to live that a statement remains in the cache.
var StatementsCacheTimeToLive = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.recent.statements_cache.time_to_live",
	"the maximum time to live",
	time.Hour,
	settings.NonNegativeDurationWithMaximum(time.Hour*24),
).WithPublic()

// StatementsCache is a FIFO cache for recent statements.
type StatementsCache struct {
	st *cluster.Settings
	// timeSrc is used to determine if a statement has reached
	// the maximum time to live in the cache.
	timeSrc timeSource

	mu struct {
		syncutil.RWMutex
		acc  mon.BoundAccount
		data *cache.UnorderedCache
	}

	mon *mon.BytesMonitor
}

// statementsNode represents the value for each cache entry.
type statementsNode struct {
	data      serverpb.ActiveQuery
	timestamp time.Time
}

// statementsPool allows for the "reuse" of statementsNodes.
// If there are available nodes in the pool, it returns those
// previously allocated nodes. If there aren't, it returns a newly
// created node. If a node is evicted, it is returned to the pool
// to be used again. This prevents the de-allocation and reallocation
// of nodes every time we add to the pool.
var statementsPool = sync.Pool{
	New: func() interface{} {
		return new(statementsNode)
	},
}

// NewStatementsCache initializes and returns a new StatementsCache.
func NewStatementsCache(
	st *cluster.Settings, parentMon *mon.BytesMonitor, timeSrc timeSource,
) *StatementsCache {
	monitor := mon.NewMonitorInheritWithLimit("recent-statements-cache", 0, parentMon)

	c := &StatementsCache{st: st, timeSrc: timeSrc}

	c.mu.data = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, _, value interface{}) bool {
			capacity := StatementsCacheCapacity.Get(&st.SV)
			ttl := StatementsCacheTimeToLive.Get(&st.SV)
			return int64(size) > capacity ||
				c.timeSrc().UnixNano()-value.(*statementsNode).timestamp.UnixNano() > ttl.Nanoseconds()
		},
		OnEvicted: func(_, value interface{}) {
			node := value.(*statementsNode)
			size := int64(node.size())
			statementsPool.Put(node)
			c.mu.acc.Shrink(context.Background(), size)
		},
	})

	c.mu.acc = monitor.MakeBoundAccount()
	c.mon = monitor
	c.mon.StartNoReserved(context.Background(), parentMon)
	return c
}

// Add stores an ActiveQuery in the cache.
func (rc *StatementsCache) Add(ctx context.Context, stmt serverpb.ActiveQuery) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Get a blank statementsNode from the statementsPool
	node := statementsPool.Get().(*statementsNode)
	*node = statementsNode{
		data:      stmt,
		timestamp: rc.timeSrc(),
	}

	if err := rc.mu.acc.Grow(ctx, int64(node.size())); err != nil {
		return err
	}
	rc.mu.data.Add(stmt.ID, node)
	return nil
}

// IterateRecentStatements iterates through all the statements in
// the cache.
func (rc *StatementsCache) IterateRecentStatements(
	ctx context.Context, visitor func(context.Context, *serverpb.ActiveQuery),
) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	rc.mu.data.Do(func(entry *cache.Entry) {
		node := entry.Value.(*statementsNode)
		visitor(ctx, &node.data)
	})
}

func (n *statementsNode) size() int {
	size := 0
	size += int(unsafe.Sizeof(clusterunique.ID{}))
	size += n.data.Size()
	size += int(unsafe.Sizeof(time.Time{}))
	return size
}
