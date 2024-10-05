// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type timeSource func() time.Time

// closedSessionCacheCapacity is the cluster setting that controls the maximum
// number of sessions in the cache.
var closedSessionCacheCapacity = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.closed_session_cache.capacity",
	"the maximum number of sessions in the cache",
	1000, // TODO(gtr): Totally arbitrary for now, adjust later.
	settings.WithPublic)

// closedSessionCacheTimeToLive is the cluster setting that controls the maximum
// time to live for a session's information in the cache, in seconds.
var closedSessionCacheTimeToLive = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.closed_session_cache.time_to_live",
	"the maximum time to live, in seconds",
	3600, // One hour
	settings.WithPublic)

// closedSessionCacheInternalSamplingProbability controls the sampling rate for
// internal queries before including them into the cache. Serializing the
// connExecutor could have non-trivial overhead for frequently executed internal
// queries, so we want to reduce that.
var closedSessionCacheInternalSamplingProbability = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"sql.closed_session_cache.internal_queries.sample_rate",
	"determines the sampling rate at which internal queries are included into the closed session cache",
	0.01,
	settings.Fraction,
)

// ClosedSessionCache is an in-memory FIFO cache for closed sessions.
type ClosedSessionCache struct {
	st      *cluster.Settings
	timeSrc timeSource

	mu struct {
		syncutil.RWMutex
		acc  mon.BoundAccount
		data *cache.UnorderedCache
	}

	mon *mon.BytesMonitor
}

// NewClosedSessionCache returns a new ClosedSessionCache.
func NewClosedSessionCache(
	st *cluster.Settings, parentMon *mon.BytesMonitor, timeSrc timeSource,
) *ClosedSessionCache {
	monitor := mon.NewMonitorInheritWithLimit(
		"closed-session-cache", 0 /* limit */, parentMon, true, /* longLiving */
	)

	c := &ClosedSessionCache{st: st, timeSrc: timeSrc}
	c.mu.data = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, _, _ interface{}) bool {
			capacity := closedSessionCacheCapacity.Get(&st.SV)
			return int64(size) > capacity
		},
		OnEvictedEntry: func(entry *cache.Entry) {
			node := entry.Value.(*sessionNode)
			size := int64(node.size())
			c.mu.acc.Shrink(context.Background(), size)
		},
	})

	c.mu.acc = monitor.MakeBoundAccount()
	c.mon = monitor
	c.mon.StartNoReserved(context.Background(), parentMon)
	return c
}

// add adds a closed session to the ClosedSessionCache.
func (c *ClosedSessionCache) add(
	ctx context.Context, id clusterunique.ID, session serverpb.Session,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	end := timeutil.Now()
	session.End = &end
	session.Status = serverpb.Session_CLOSED
	node := &sessionNode{id: id, data: session, timestamp: c.timeSrc()}

	if err := c.mu.acc.Grow(ctx, int64(node.size())); err != nil {
		return err
	}

	c.mu.data.Add(id, node)
	return nil
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

// getSessions iterates through each sessionNode, evicting those whose age
// exceeds the cache's maximum time to live, and returns a list of the
// remaining sessionNodes.
func (c *ClosedSessionCache) getSessions() []*sessionNode {
	c.mu.Lock()
	defer c.mu.Unlock()

	var result []*sessionNode
	var sessionsToEvict []*sessionNode

	c.mu.data.Do(func(entry *cache.Entry) {
		node := entry.Value.(*sessionNode)
		if int64(node.getAge(c.timeSrc).Seconds()) > closedSessionCacheTimeToLive.Get(&c.st.SV) {
			sessionsToEvict = append(sessionsToEvict, node)
		} else {
			result = append(result, node)
		}
	})

	c.evictSessionsLocked(sessionsToEvict)
	return result
}

func (c *ClosedSessionCache) evictSessionsLocked(toEvict []*sessionNode) {
	for _, entry := range toEvict {
		c.mu.data.Del(entry.id)
	}
}

func (c *ClosedSessionCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mu.data.Clear()
}

// sessionNode encapsulates the session information that will be inserted into
// the cache.
type sessionNode struct {
	id        clusterunique.ID
	data      serverpb.Session
	timestamp time.Time
}

func (n *sessionNode) getAge(now timeSource) time.Duration {
	return now().Sub(n.timestamp)
}

func (n *sessionNode) getAgeString(now timeSource) string {
	return n.getAge(now).Round(time.Second).String()
}

func (n *sessionNode) size() int {
	size := 0
	size += int(unsafe.Sizeof(clusterunique.ID{}))
	size += n.data.Size()
	size += int(unsafe.Sizeof(time.Time{}))
	return size
}
