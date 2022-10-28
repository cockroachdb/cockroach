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
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type timeSource func() time.Time

// StatementsCacheCapacity is the cluster setting that controls the
// maximum number of recent statements in the cache.
var StatementsCacheCapacity = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.recent_statements_cache.capacity",
	"the maximum number of statements in the cache",
	20000,
).WithPublic()

// StatementsCacheTimeToLive is the cluster setting that controls the
// maximum time to live that a statement remains in the cache, in seconds.
var StatementsCacheTimeToLive = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.recent_statements_cache.time_to_live",
	"the maximum time to live, in seconds",
	86400, // 1 day
).WithPublic()

// StatementsCache is a FIFO cache for recent statements.
type StatementsCache struct {
	st      *cluster.Settings
	timeSrc timeSource

	mu struct {
		syncutil.RWMutex
		data *cache.UnorderedCache
	}
}

// statementsNode represents the value for each cache entry.
type statementsNode struct {
	sessionID clusterunique.ID
	data      serverpb.ActiveQuery
	timestamp time.Time
}

var statementsPool = sync.Pool{
	New: func() interface{} {
		return new(statementsNode)
	},
}

// NewStatementsCache initializes and returns a new StatementsCache.
func NewStatementsCache(st *cluster.Settings, timeSrc timeSource) *StatementsCache {
	c := &StatementsCache{st: st, timeSrc: timeSrc}

	c.mu.data = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, _, _ interface{}) bool {
			capacity := StatementsCacheCapacity.Get(&st.SV)
			return int64(size) > capacity
		},
		OnEvicted: func(_, value interface{}) {
			node := value.(*statementsNode)
			statementsPool.Put(node)
		},
	})

	return c
}

// Add stores an ActiveQuery in the cache.
func (rc *StatementsCache) Add(sessionID clusterunique.ID, stmt serverpb.ActiveQuery) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	node := statementsPool.Get().(*statementsNode)
	*node = statementsNode{
		sessionID: sessionID,
		data:      stmt,
		timestamp: rc.timeSrc(),
	}
	rc.mu.data.Add(stmt.ID, node)
}

// GetRecentStatementsForSession returns cached statements for the
// given session.
func (rc *StatementsCache) GetRecentStatementsForSession(
	sessionID clusterunique.ID,
) []serverpb.ActiveQuery {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	var recentStatements []serverpb.ActiveQuery
	var toEvict []string

	rc.mu.data.Do(func(entry *cache.Entry) {
		node := entry.Value.(*statementsNode)
		age := int64(node.getAge(rc.timeSrc).Seconds())

		if age > StatementsCacheTimeToLive.Get(&rc.st.SV) {
			toEvict = append(toEvict, entry.Key.(string))
		} else if node.sessionID == sessionID {
			recentStatements = append(recentStatements, node.data)
		}
	})

	rc.evict(toEvict)
	return recentStatements
}

func (rc *StatementsCache) evict(toEvict []string) {
	for _, id := range toEvict {
		rc.mu.data.Del(id)
	}
}

func (n *statementsNode) getAge(now timeSource) time.Duration {
	return now().Sub(n.timestamp)
}
