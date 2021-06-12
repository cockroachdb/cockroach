// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cache

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// ConnCache is interface for caching connections from IP and tenant ID pairs.
// TODO(spaskob): once we have more clients of this interface split it in a
// separate package.
type ConnCache interface {
	Insert(key *ConnKey)
	Exists(key *ConnKey) bool
}

// ConnKey is the key for a connection for TenantID and IPAddress.
type ConnKey struct {
	TenantID  roachpb.TenantID
	IPAddress string
}

// cappedConnCache implements the ConnCache interface.
type cappedConnCache struct {
	maxMapSize int

	mu struct {
		syncutil.Mutex
		conns map[ConnKey]struct{}
	}
}

// NewCappedConnCache returns a cache service that has a limit on the total
// entries.
func NewCappedConnCache(maxMapSize int) ConnCache {
	c := &cappedConnCache{
		maxMapSize: maxMapSize,
	}
	c.mu.conns = make(map[ConnKey]struct{})
	return c
}

// Exists checks is the connection with the given key is in the cache.
func (c *cappedConnCache) Exists(conn *ConnKey) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.mu.conns[*conn]
	return ok
}

// Insert the given connection.
func (c *cappedConnCache) Insert(conn *ConnKey) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.mu.conns) >= c.maxMapSize {
		toEvict, pos := rand.Intn(len(c.mu.conns)), 0
		for k := range c.mu.conns {
			// Choose a random key to evict by iterating through the keys in the map.
			// As we expect that eviction will be a very rare event, it is fine to pay
			// the penalty of iterating through potentially map's elements.
			if pos < toEvict {
				pos++
				continue
			}
			delete(c.mu.conns, k)
			break
		}
	}
	c.mu.conns[*conn] = struct{}{}
}
