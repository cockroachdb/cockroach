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
			gauge := entry.Value.(*aggmetric.Gauge)
			// The child metric will continue to report into the parent metric even
			// after unlinking, so we also reset it to 0.
			gauge.Update(0)
			gauge.Unlink()
			c.mu.acc.Shrink(ctx, int64(unsafe.Sizeof(*gauge)))
		},
	})
	c.mon = mon.NewMonitorInheritWithLimit(
		"client-expiration-cache", 0 /* limit */, parentMon,
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

// Get retrieves the cert expiration for the given username, if it exists.
// An expiration of 0 indicates an entry was not found.
func (c *ClientCertExpirationCache) Get(key string) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.mu.cache.Get(key)
	if !ok {
		return 0, ok
	}
	// If the expiration has already been reached, remove the entry and indicate
	// that the entry was not found.
	gauge := value.(*aggmetric.Gauge)
	if gauge.Value() < c.timeNow() {
		c.mu.cache.Del(key)
		return 0, false
	}
	return gauge.Value(), ok
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
		err := c.mu.acc.Grow(ctx, int64(unsafe.Sizeof(aggmetric.Gauge{})))
		if err == nil {
			// Only create new gauges for expirations in the future.
			if newExpiry > c.timeNow() {
				gauge := parentGauge.AddChild(key)
				gauge.Update(newExpiry)
				c.mu.cache.Add(key, gauge)
			}
		} else {
			log.Ops.Warningf(ctx, "no memory available to cache cert expiry: %v", err)
		}
	} else if gauge := value.(*aggmetric.Gauge); newExpiry < gauge.Value() || gauge.Value() == 0 {
		gauge.Update(newExpiry)
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

		timer := timeutil.NewTimer()
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
		gauge := entry.Value.(*aggmetric.Gauge)
		if gauge.Value() <= now {
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
