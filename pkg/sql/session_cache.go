// Copyright 2015 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"strings"
	"time"
)

// SessionCache is an in-memory FIFO cache for closed sessions.
type SessionCache struct {
	capacity   int   // The maximum number of sessions in the cache.
	timeToLive int64 // The maximum time to live, in seconds.

	mu struct {
		// TODO(gtr): possibly switch to using a RW mutex.
		syncutil.Mutex
		size  int
		data  map[ClusterWideID]SessionNode
		queue SessionList
	}
}

func NewSessionCache(capacity int, timeToLive int64) *SessionCache {
	cache := &SessionCache{
		capacity:   capacity,
		timeToLive: timeToLive,
	}

	cache.mu.data = make(map[ClusterWideID]SessionNode)
	cache.mu.queue = SessionList{}

	// Start checkForTimeToLiveEviction as a separate goroutine.
	go cache.checkForTimeToLiveEviction()

	return cache
}

// checkForTimeToLiveEviction periodically checks if the age of the node at the
// head of the queue has exceeded the allowed timeToLive of the cache.
func (c *SessionCache) checkForTimeToLiveEviction() {
	// TODO(gtr): Play around with the time interval for optimal performance
	// 						since locking the mutex frequently is an expensive operation.
	for range time.Tick(time.Second) {
		c.mu.Lock()
		node := c.mu.queue.peek()
		if node != nil && node.getAge() > c.timeToLive {
			c.mu.queue.pop()
			c.evict(node)
		}
		c.mu.Unlock()
	}
}

func (c *SessionCache) add(id ClusterWideID, s registrySession) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node := &SessionNode{data: s, id: id, timestamp: time.Now().Unix()}
	c.mu.size++
	c.mu.data[id] = *node
	c.mu.queue.addNode(node)
	c.checkForCapacityEviction()
}

func (c *SessionCache) get(id ClusterWideID) (registrySession, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, found := c.mu.data[id]
	return node.data, found
}

// getSerializedSessions returns a list of []serverpb.Session ordered from
// oldest to newest in the cache.
func (c *SessionCache) getSerializedSessions() []serverpb.Session {
	c.mu.Lock()
	defer c.mu.Unlock()

	var sessions []serverpb.Session
	head := c.mu.queue.head
	for head != nil {
		sessions = append(sessions, head.data.serialize())
		head = head.next
	}

	return sessions
}

func (c *SessionCache) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.mu.size
}

// checkForCapacityEviction checks if the size of the cache has exceeded the
// maximum capacity. The caller must handle locking the mutex.
func (c *SessionCache) checkForCapacityEviction() {
	if c.mu.size > c.capacity {
		node := c.mu.queue.pop()
		if node == nil {
			return
		}
		c.evict(node)
	}
}

// evict evicts a specific node from the cache. The caller must handle locking
// the mutex.
func (c *SessionCache) evict(node *SessionNode) {
	c.mu.size--
	delete(c.mu.data, node.id)
}

func (c *SessionCache) viewCachedSessions() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mu.size == 0 {
		return "empty"
	}

	var result []string
	head := c.mu.queue.head
	for head != nil {
		result = append(result, fmt.Sprintf("id: %s age: %s session: {}", head.id, head.getAgeString()))
		head = head.next
	}

	return strings.Join(result, "\n")
}

func (c *SessionCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k := range c.mu.data {
		delete(c.mu.data, k)
	}
}

type SessionNode struct {
	id        ClusterWideID
	data      registrySession
	next      *SessionNode
	timestamp int64
}

func (n *SessionNode) getAge() int64 {
	return time.Now().Unix() - n.timestamp
}

func (n *SessionNode) getAgeString() string {
	seconds := n.getAge()
	hours := seconds / 3600
	seconds %= 3600
	minutes := seconds / 60
	seconds %= 60
	return fmt.Sprintf("%dh%dm%ds", hours, minutes, seconds)
}

// SessionList is a singly-linked list used for FIFO evictions.
type SessionList struct {
	head *SessionNode
	tail *SessionNode
}

func (l *SessionList) addNode(n *SessionNode) {
	if l.head == nil {
		l.head = n
	} else {
		l.tail.next = n
	}
	l.tail = n
}

func (l *SessionList) pop() *SessionNode {
	if l.head == nil {
		return nil
	}

	popped := l.head
	l.head = l.head.next
	if l.head == nil {
		l.tail = nil
	}

	return popped
}

func (l *SessionList) peek() *SessionNode {
	return l.head
}

func (l *SessionList) clear() {
	for l.head != nil && l.tail != nil {
		l.pop()
	}
}
