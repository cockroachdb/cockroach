// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import (
	"context"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TODO(cameron): best to update this after understanding performance more
const CacheCapacityMax = 1500

// ClientCertExpirationCacheCapacity is the cluster setting that controls the
// maximum number of client cert expirations in the cache.
var ClientCertExpirationCacheCapacity = settings.RegisterIntSetting(
	settings.TenantWritable,
	"server.client_cert_expiration_cache.capacity",
	"the maximum number of client cert expirations stored",
	1000,
).WithPublic()

// ClientCertExpirationCache contains a cache of gauge objects keyed by
// SQL username strings. It is a FIFO cache that stores gauges valued by
// minimum expiration of the client certs seen (per user).
type ClientCertExpirationCache struct {
	mu struct {
		// NB: Cannot be a RWMutex for Get because UnorderedCache.Get manipulates
		// an internal hashmap.
		syncutil.Mutex
		cache *cache.UnorderedCache
		acc   mon.BoundAccount
	}
	settings *cluster.Settings
	mon      *mon.BytesMonitor
}

func NewClientCertExpirationCache(
	st *cluster.Settings, parentMon *mon.BytesMonitor,
) *ClientCertExpirationCache {
	c := &ClientCertExpirationCache{settings: st}
	c.mu.cache = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, _, _ interface{}) bool {
			var capacity int64
			settingCapacity := ClientCertExpirationCacheCapacity.Get(&st.SV)
			if settingCapacity < CacheCapacityMax {
				capacity = settingCapacity
			} else {
				capacity = CacheCapacityMax
			}
			return int64(size) > capacity
		},
		OnEvictedEntry: func(entry *cache.Entry) {
			gauge := entry.Value.(*aggmetric.Gauge)
			gauge.Unlink()
			c.mu.acc.Shrink(context.Background(), int64(unsafe.Sizeof(*gauge)))
		},
	})
	c.mon = mon.NewMonitorInheritWithLimit(
		"client-expiration-cache", 0 /* limit*/, parentMon,
	)
	c.mu.acc = c.mon.MakeBoundAccount()
	c.mon.StartNoReserved(context.Background(), parentMon)
	return c
}

// Get retrieves the cert expiration for the given username, if it exists.
// An expiration of 0 indicates an entry was not found.
func (c *ClientCertExpirationCache) Get(key string) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.mu.cache.Get(key)
	if !ok {
		return 0, ok
	}
	return value.(*aggmetric.Gauge).Value(), ok
}

// MaybeUpsert may update or insert a client cert expiration gauge for a
// particular user into the cache. An update is contingent on whether the
// old expiration is after the new expiration. This ensures that the cache
// maintains the minimum expiration for each user.
func (c *ClientCertExpirationCache) MaybeUpsert(
	ctx context.Context, key string, newExpiry int64, parentGauge *aggmetric.AggGauge,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	value, ok := c.mu.cache.Get(key)
	if !ok {
		gauge := parentGauge.AddChild(key)
		gauge.Update(newExpiry)
		c.mu.cache.Add(key, gauge)
		if err := c.mu.acc.Grow(ctx, int64(unsafe.Sizeof(*gauge))); err != nil {
			log.Ops.Warningf(ctx, "no memory available to cache cert expiry: %v", err)
		}
	} else {
		if gauge := value.(*aggmetric.Gauge); newExpiry < gauge.Value() || gauge.Value() == 0 {
			gauge.Update(newExpiry)
		}
	}
}

func (c *ClientCertExpirationCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.cache.Clear()
}

// Len returns the number of cert expirations in the cache.
func (c *ClientCertExpirationCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.cache.Len()
}
