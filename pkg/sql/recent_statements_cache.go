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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// RecentStatementsCacheCapacity is the cluster setting that controls the
// maximum number of recent statements in the cache.
var RecentStatementsCacheCapacity = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.recent_statements_cache.capacity",
	"the maximum number of statements in the cache",
	1000,
).WithPublic()

// RecentStatementsCache is a FIFO cache for recent statements.
// TODO(amy): add time source field
type RecentStatementsCache struct {
	st *cluster.Settings

	mu struct {
		syncutil.RWMutex
		data *cache.UnorderedCache
	}
}

// recentStatementsCacheKey represents the key for each cache entry.
// The key consists of both Session ID and Statement ID since we want to
// filter the cache by Session, but we need the Statement ID to have unique keys.
type recentStatementsCacheKey struct {
	SessionID   clusterunique.ID
	StatementID clusterunique.ID
}

// recentStatementsNode represents the value for each cache entry.
// TODO(amy): add timestamp field
type recentStatementsNode struct {
	data *queryMeta
}

// NewRecentStatementsCache initializes and returns a new RecentStatementsCache.
func NewRecentStatementsCache(st *cluster.Settings) *RecentStatementsCache {
	c := &RecentStatementsCache{st: st}

	c.mu.data = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, _, _ interface{}) bool {
			capacity := RecentStatementsCacheCapacity.Get(&st.SV)
			return int64(size) > capacity
		},
	})

	return c
}

// add uses the previously described recentStatementsCacheKey and recentStatementsNode
// to add a new statement entry to the RecentStatementsCache
func (rc *RecentStatementsCache) add(
	sessionID clusterunique.ID, stmtID clusterunique.ID, qm *queryMeta,
) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	key := recentStatementsCacheKey{
		SessionID:   sessionID,
		StatementID: stmtID,
	}
	node := &recentStatementsNode{data: qm}
	rc.mu.data.Add(&key, node)
}

func (rc *RecentStatementsCache) len() int {
	if rc == nil {
		return 0
	}
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return rc.mu.data.Len()
}

// getRecentStatementsForSession takes a Session ID, and returns a list of the Statement IDs and
// a list of the corresponding queryMeta for the recent statements executed for that Session.
func (rc *RecentStatementsCache) getRecentStatementsForSession(
	sessionID clusterunique.ID,
) map[clusterunique.ID]*queryMeta {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	stmtIDToQM := make(map[clusterunique.ID]*queryMeta)

	rc.mu.data.Do(func(entry *cache.Entry) {
		key := entry.Key.(*recentStatementsCacheKey)
		node := entry.Value.(*recentStatementsNode)

		if key.SessionID == sessionID {
			stmtIDToQM[key.StatementID] = node.data
		}
	})

	return stmtIDToQM
	//var qms []*queryMeta
	//var stmtIDs []clusterunique.ID
	//
	//rc.mu.data.Do(func(entry *cache.Entry) {
	//	key := entry.Key.(*recentStatementsCacheKey)
	//	node := entry.Value.(*recentStatementsNode)
	//
	//	if key.SessionID == sessionID {
	//		qms = append(qms, node.data)
	//		stmtIDs = append(stmtIDs, key.StatementID)
	//	}
	//})
	//
	//return stmtIDs, qms
}

func (rc *RecentStatementsCache) clear() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.mu.data.Clear()
}
