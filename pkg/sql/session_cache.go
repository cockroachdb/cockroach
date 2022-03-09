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
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// SessionCacheCapacity is the cluster setting that controls the maximum number
// of sessions in the cache.
var SessionCacheCapacity = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.session_cache.capacity",
	"the maximum number of sessions in the cache",
	100, // Totally arbitrary for now
).WithPublic()

// SessionCacheTimeToLive is the cluster setting that controls the maximum time
// to live for a session's information in the cache, in seconds.
var SessionCacheTimeToLive = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.session_cache.time_to_live",
	"the maximum time to live, in seconds",
	3600, // One hour
).WithPublic()

// SessionCache is an in-memory FIFO cache for closed sessions.
type SessionCache struct {
	st *cluster.Settings

	mu struct {
		syncutil.RWMutex
		queue *cache.UnorderedCache
	}
}

// NewSessionCache returns a new session cache.
func NewSessionCache(st *cluster.Settings) *SessionCache {
	c := &SessionCache{st: st}

	c.mu.queue = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, _, _ interface{}) bool {
			capacity := SessionCacheCapacity.Get(&st.SV)
			return int64(size) > capacity
		},
	})

	return c
}

// Add adds a closed session to the SessionCache.
func (c *SessionCache) Add(id ClusterWideID, session registrySession) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node := &SessionNode{id: id, data: session, timestamp: timeutil.Now()}
	c.mu.queue.Add(id, node)
}

func (c *SessionCache) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.mu.queue.Len()
}

// GetSerializedSessions returns a list of []serverpb.Session, ordered from
// newest to oldest in the cache.
func (c *SessionCache) GetSerializedSessions() []serverpb.Session {
	var serializedSessions []serverpb.Session

	sessions := c.getSessions()
	for _, s := range sessions {
		serializedSessions = append(serializedSessions, s.data.serialize())
	}

	return serializedSessions
}

// viewCachedSessions returns a string representation of the sessions in the
// cache, ordered from newest to oldest. This function is used for testing
// purposes.
func (c *SessionCache) viewCachedSessions() string {
	var result []string

	sessions := c.getSessions()
	for _, s := range sessions {
		result = append(result, fmt.Sprintf("id: %s age: %s session: {}", s.id, s.getAgeString()))
	}

	if len(result) == 0 {
		return "empty"
	}
	return strings.Join(result, "\n")
}

// getSessions iterates through each SessionNode, evicting those whose age
// exceeds the cache's maximum time to live, and returns a list of the
// remaining SessionNodes
func (c *SessionCache) getSessions() []*SessionNode {
	c.mu.Lock()
	defer c.mu.Unlock()

	var result []*SessionNode
	var sessionsToEvict []*SessionNode

	c.mu.queue.Do(func(entry *cache.Entry) {
		node := entry.Value.(*SessionNode)
		if int64(node.getAge().Seconds()) > SessionCacheTimeToLive.Get(&c.st.SV) {
			sessionsToEvict = append(sessionsToEvict, node)
		} else {
			result = append(result, node)
		}
	})

	c.evictSessionsLocked(sessionsToEvict)
	return result
}

func (c *SessionCache) evictSessionsLocked(toEvict []*SessionNode) {
	for _, entry := range toEvict {
		c.mu.queue.Del(entry.id)
	}
}

// Clear clears the cache.
func (c *SessionCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mu.queue.Clear()
}

// SessionNode encapsulates the session information that will be inserted into
// the cache.
type SessionNode struct {
	id        ClusterWideID
	data      registrySession
	timestamp time.Time
}

func (n *SessionNode) getAge() time.Duration {
	return timeutil.Since(n.timestamp)
}

func (n *SessionNode) getAgeString() string {
	return n.getAge().Round(time.Second).String()
}
