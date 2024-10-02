// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security

import (
	"context"
	math_rand "math/rand"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// CacheCapacityMax is set arbitrarily high; configurable later if needed.
const CacheCapacityMax = 65000

// ClientCertExpirationCacheCapacity is the cluster setting that controls the
// maximum number of client cert expirations in the cache.
var ClientCertExpirationCacheCapacity = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"server.client_cert_expiration_cache.capacity",
	"the maximum number of client cert expirations stored",
	1000,
	settings.WithPublic)

type clientCertExpirationMetrics struct {
	expiration aggmetric.Gauge
	ttl        aggmetric.Gauge
}

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
	stopper  *stop.Stopper
	mon      *mon.BytesMonitor
	timeSrc  interface{}
}

// NewClientCertExpirationCache creates a new client cert expiration cache.
func NewClientCertExpirationCache(
	ctx context.Context,
	st *cluster.Settings,
	stopper *stop.Stopper,
	timeSrc timeutil.TimeSource,
	parentMon *mon.BytesMonitor,
) *ClientCertExpirationCache {
	c := &ClientCertExpirationCache{settings: st}
	c.stopper = stopper

	switch timeSrc := timeSrc.(type) {
	case *timeutil.DefaultTimeSource, *timeutil.ManualTime:
		c.timeSrc = timeSrc
	default:
		c.timeSrc = &timeutil.DefaultTimeSource{}
	}

	c.mu.cache = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, _, value interface{}) bool {
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
			metrics := entry.Value.(*clientCertExpirationMetrics)
			// The child metric will continue to report into the parent metric even
			// after unlinking, so we also reset it to 0.
			metrics.expiration.Update(0)
			metrics.expiration.Unlink()
			metrics.ttl.Update(0)
			metrics.ttl.Unlink()
			c.mu.acc.Shrink(ctx, int64(unsafe.Sizeof(*metrics)))
		},
	})
	c.mon = mon.NewMonitorInheritWithLimit(
		"client-expiration-cache", 0 /* limit */, parentMon, true, /* longLiving */
	)
	c.mu.acc = c.mon.MakeBoundAccount()
	c.mon.StartNoReserved(ctx, parentMon)

	// Begin an async task to periodically evict entries associated with
	// expiration values that are in the past.
	if err := c.startPurgePastExpirations(ctx); err != nil {
		log.Ops.Warningf(
			ctx, "failed to initiate periodic purge of expiration cache entries: %v", err,
		)
	}

	return c
}

// GetTTL retrieves seconds till cert expiration for the given username, if it exists.
// A TTL of 0 indicates an entry was not found.
func (c *ClientCertExpirationCache) GetTTL(key string) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.mu.cache.Get(key)
	if !ok {
		return 0, ok
	}
	// If the metrics has already been reached, remove the entry and indicate
	// that the entry was not found.
	metrics := value.(*clientCertExpirationMetrics)
	if metrics.expiration.Value() < c.timeNow() {
		c.mu.cache.Del(key)
		return 0, false
	}
	return metrics.ttl.Value(), ok
}

// GetExpiration retrieves the cert expiration for the given username, if it exists.
// An expiration of 0 indicates an entry was not found.
func (c *ClientCertExpirationCache) GetExpiration(key string) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.mu.cache.Get(key)
	if !ok {
		return 0, ok
	}
	// If the metrics has already been reached, remove the entry and indicate
	// that the entry was not found.
	metrics := value.(*clientCertExpirationMetrics)
	if metrics.expiration.Value() < c.timeNow() {
		c.mu.cache.Del(key)
		return 0, false
	}
	return metrics.expiration.Value(), ok
}

// ttlFunc returns a function function which takes a time,
// if the time is past returns 0, otherwise returns the number
// of seconds until that timestamp
func ttlFunc(now func() int64, exp int64) func() int64 {
	return func() int64 {
		ttl := exp - now()
		if ttl > 0 {
			return ttl
		} else {
			return 0
		}
	}
}

// MaybeUpsert may update or insert a client cert expiration gauge for a
// particular user into the cache. An update is contingent on whether the
// old expiration is after the new expiration. This ensures that the cache
// maintains the minimum expiration for each user.
func (c *ClientCertExpirationCache) MaybeUpsert(
	ctx context.Context,
	key string,
	newExpiry int64,
	parentExpirationGauge *aggmetric.AggGauge,
	parentTTLGauge *aggmetric.AggGauge,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	value, ok := c.mu.cache.Get(key)
	if !ok {
		err := c.mu.acc.Grow(ctx, int64(unsafe.Sizeof(clientCertExpirationMetrics{})))
		if err == nil {
			// Only create new gauges for expirations in the future.
			if newExpiry > c.timeNow() {
				expiration := parentExpirationGauge.AddChild(key)
				expiration.Update(newExpiry)
				ttl := parentTTLGauge.AddFunctionalChild(ttlFunc(c.timeNow, newExpiry), key)
				c.mu.cache.Add(key, &clientCertExpirationMetrics{*expiration, *ttl})
			}
		} else {
			log.Ops.Warningf(ctx, "no memory available to cache cert expiry: %v", err)
		}
	} else if metrics := value.(*clientCertExpirationMetrics); newExpiry < metrics.expiration.Value() || metrics.expiration.Value() == 0 {
		metrics.expiration.Update(newExpiry)
		metrics.ttl.UpdateFn(ttlFunc(c.timeNow, newExpiry))
	}
}

// Clear removes all entries from the cache.
func (c *ClientCertExpirationCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.cache.Clear()
}

// Len returns the number of cert expirations in the cache.
func (c *ClientCertExpirationCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.cache.Len()
}

// timeNow returns the current time depending on the time source of the cache.
func (c *ClientCertExpirationCache) timeNow() int64 {
	if timeSrc, ok := c.timeSrc.(timeutil.TimeSource); ok {
		return timeSrc.Now().Unix()
	}
	return timeutil.Now().Unix()
}

// startPurgePastExpirations runs an infinite loop in a goroutine which
// regularly evicts entries associated with expiration values that have already
// passed.
func (c *ClientCertExpirationCache) startPurgePastExpirations(ctx context.Context) error {
	return c.stopper.RunAsyncTask(ctx, "purge-cert-expiry-cache", func(context.Context) {
		const period = time.Hour

		var timer timeutil.Timer
		defer timer.Stop()

		timer.Reset(jitteredInterval(period))
		for ; ; timer.Reset(period) {
			select {
			case <-timer.C:
				timer.Read = true
				c.PurgePastExpirations()
			case <-c.stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			}
		}
	},
	)
}

// PurgePastExpirations removes entries associated with expiration values that
// have already passed. This helps ensure that the cache contains gauges
// with expiration values in the future only.
func (c *ClientCertExpirationCache) PurgePastExpirations() {
	c.mu.Lock()
	defer c.mu.Unlock()
	var deleteEntryKeys []interface{}
	now := c.timeNow()
	c.mu.cache.Do(func(entry *cache.Entry) {
		metrics := entry.Value.(*clientCertExpirationMetrics)
		if metrics.expiration.Value() <= now {
			deleteEntryKeys = append(deleteEntryKeys, entry.Key)
		}
	})
	for _, key := range deleteEntryKeys {
		c.mu.cache.Del(key)
	}
}

// jitteredInterval returns a randomly jittered (+/-25%) duration
// from the interval.
func jitteredInterval(interval time.Duration) time.Duration {
	return time.Duration(float64(interval) * (0.75 + 0.5*math_rand.Float64()))
}
