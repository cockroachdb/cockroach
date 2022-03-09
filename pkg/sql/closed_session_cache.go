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
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type timeSource func() time.Time

// ClosedSessionCacheCapacity is the cluster setting that controls the maximum number
// of sessions in the cache.
var ClosedSessionCacheCapacity = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.closed_session_cache.capacity",
	"the maximum number of sessions in the cache",
	100, // TODO(gtr): Totally arbitrary for now, adjust later.
).WithPublic()

// ClosedSessionCacheTimeToLive is the cluster setting that controls the maximum time
// to live for a session's information in the cache, in seconds.
var ClosedSessionCacheTimeToLive = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.closed_session_cache.time_to_live",
	"the maximum time to live, in seconds",
	3600, // One hour
).WithPublic()

// ClosedSessionCache is an in-memory FIFO cache for closed sessions.
type ClosedSessionCache struct {
	st      *cluster.Settings
	timeSrc timeSource

	mu struct {
		syncutil.RWMutex
		data *cache.UnorderedCache
	}
}

// NewClosedSessionCache returns a new ClosedSessionCache.
func NewClosedSessionCache(st *cluster.Settings, timeSrc timeSource) *ClosedSessionCache {
	c := &ClosedSessionCache{st: st, timeSrc: timeSrc}

	c.mu.data = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, _, _ interface{}) bool {
			capacity := ClosedSessionCacheCapacity.Get(&st.SV)
			return int64(size) > capacity
		},
	})

	return c
}

// Add adds a closed session to the ClosedSessionCache.
func (c *ClosedSessionCache) Add(id ClusterWideID, session serverpb.Session) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node := &SessionNode{id: id, data: session, timestamp: c.timeSrc()}
	c.mu.data.Add(id, node)
}

func (c *ClosedSessionCache) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.mu.data.Len()
}

// GetSerializedSessions returns a list of []serverpb.Session, ordered from
// newest to oldest in the cache.
func (c *ClosedSessionCache) GetSerializedSessions() []serverpb.Session {
	var serializedSessions []serverpb.Session

	sessions := c.getSessions()
	for _, s := range sessions {
		serializedSessions = append(serializedSessions, s.data)
	}

	return serializedSessions
}

// getSessions iterates through each SessionNode, evicting those whose age
// exceeds the cache's maximum time to live, and returns a list of the
// remaining SessionNodes
func (c *ClosedSessionCache) getSessions() []*SessionNode {
	c.mu.Lock()
	defer c.mu.Unlock()

	var result []*SessionNode
	var sessionsToEvict []*SessionNode

	c.mu.data.Do(func(entry *cache.Entry) {
		node := entry.Value.(*SessionNode)
		if int64(node.getAge(c.timeSrc).Seconds()) > ClosedSessionCacheTimeToLive.Get(&c.st.SV) {
			sessionsToEvict = append(sessionsToEvict, node)
		} else {
			result = append(result, node)
		}
	})

	c.evictSessionsLocked(sessionsToEvict)
	return result
}

func (c *ClosedSessionCache) evictSessionsLocked(toEvict []*SessionNode) {
	for _, entry := range toEvict {
		c.mu.data.Del(entry.id)
	}
}

func (c *ClosedSessionCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mu.data.Clear()
}

// SessionNode encapsulates the session information that will be inserted into
// the cache.
type SessionNode struct {
	id        ClusterWideID
	data      serverpb.Session
	timestamp time.Time
}

func (n *SessionNode) getAge(now timeSource) time.Duration {
	return now().Sub(n.timestamp)
}

func (n *SessionNode) getAgeString(now timeSource) string {
	return n.getAge(now).Round(time.Second).String()
}
