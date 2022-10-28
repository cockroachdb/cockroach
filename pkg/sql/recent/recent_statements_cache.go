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

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// StatementsCacheCapacity is the cluster setting that controls the
// maximum number of recent statements in the cache.
var StatementsCacheCapacity = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.recent_statements_cache.capacity",
	"the maximum number of statements in the cache",
	1000,
).WithPublic()

// StatementsCache is a FIFO cache for recent statements.
// TODO(amy): add time source field.
type StatementsCache struct {
	st *cluster.Settings

	mu struct {
		syncutil.RWMutex
		data *cache.UnorderedCache
	}
}

// statementsNode represents the value for each cache entry.
// TODO(amy): add timestamp field.
type statementsNode struct {
	sessionID clusterunique.ID
	data      serverpb.ActiveQuery
}

var statementsPool = sync.Pool{
	New: func() interface{} {
		return new(statementsNode)
	},
}

// NewStatementsCache initializes and returns a new StatementsCache.
func NewStatementsCache(st *cluster.Settings) *StatementsCache {
	c := &StatementsCache{st: st}

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

// Add uses the previously described recentStatementsCacheKey and statementsNode
// to Add a new statement entry to the StatementsCache.
func (rc *StatementsCache) Add(
	sessionID clusterunique.ID, stmtID clusterunique.ID, activeQuery serverpb.ActiveQuery,
) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	node := statementsPool.Get().(*statementsNode)
	*node = statementsNode{
		sessionID: sessionID,
		data:      activeQuery,
	}
	rc.mu.data.Add(stmtID, node)
}

func (rc *StatementsCache) len() int {
	if rc == nil {
		return 0
	}
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return rc.mu.data.Len()
}

// GetRecentStatementsForSession takes a Session ID, and returns a list of the Statement IDs and
// a list of the corresponding queryMeta for the recent statements executed for that Session.
func (rc *StatementsCache) GetRecentStatementsForSession(
	sessionID clusterunique.ID,
) []serverpb.ActiveQuery {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	recentStatements := make([]serverpb.ActiveQuery, 0)

	rc.mu.data.Do(func(entry *cache.Entry) {
		node := entry.Value.(*statementsNode)

		if node.sessionID == sessionID {
			recentStatements = append(recentStatements, node.data)
		}
	})

	return recentStatements
}
