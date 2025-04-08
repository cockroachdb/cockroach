// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clientcert

import (
	"context"
	math_rand "math/rand"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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

// certInfo holds information about a certificate, including its expiration
// time and the last time it was seen.
type certInfo struct {
	expiration int64
	last_seen  time.Time
}

// CacheTTL is an overridable duration for when certificates should be evicted.
var CacheTTL = 24 * time.Hour

// The size of the cache and the certInfo struct. To be used for memory tracking.
var GaugeSize = int64(unsafe.Sizeof(aggmetric.AggGauge{}))
var CertInfoSize = int64(unsafe.Sizeof(certInfo{}))

// ClientCertExpirationCache contains a cache of gauge objects keyed by
// SQL username strings. It is a FIFO cache that stores gauges valued by
// minimum expiration of the client certs seen (per user).
type ClientCertExpirationCache struct {
	mu struct {
		// NB: Cannot be a RWMutex for Get because UnorderedCache.Get manipulates
		// an internal hashmap.
		syncutil.Mutex
		cache             map[string]map[string]certInfo
		acc               mon.BoundAccount
		expirationMetrics *aggmetric.AggGauge
		ttlMetrics        *aggmetric.AggGauge
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
	expirationMetrics *aggmetric.AggGauge,
	ttlMetrics *aggmetric.AggGauge,
) *ClientCertExpirationCache {
	c := &ClientCertExpirationCache{settings: st}
	c.stopper = stopper
	c.mu.expirationMetrics = expirationMetrics
	c.mu.ttlMetrics = ttlMetrics

	switch timeSrc := timeSrc.(type) {
	case *timeutil.DefaultTimeSource, *timeutil.ManualTime:
		c.timeSrc = timeSrc
	default:
		c.timeSrc = &timeutil.DefaultTimeSource{}
	}

	c.mu.cache = make(map[string]map[string]certInfo)
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

// GetTTL is a deprecated function for retrieving the ttl of a given user.
func (c *ClientCertExpirationCache) GetTTL(user string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	expiration := c.getExpirationLocked(user)
	ttl := expiration - c.timeNow()

	if ttl > 0 {
		return ttl
	} else {
		return 0
	}
}

// GetExpiration is a deprecated function for retrieving the expiration of a given user.
func (c *ClientCertExpirationCache) GetExpiration(user string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.getExpirationLocked(user)
}

func (c *ClientCertExpirationCache) getExpirationLocked(user string) int64 {
	if certs, ok := c.mu.cache[user]; ok {
		// compute the earliest expiration time.
		var minExpiration int64
		for _, cert := range certs {
			if minExpiration == 0 || cert.expiration < minExpiration {
				minExpiration = cert.expiration
			}
		}

		return minExpiration
	}
	return 0
}

// MaybeUpsert may update or insert a client cert expiration gauge for a
// particular user into the cache. An update is contingent on whether the
// old expiration is after the new expiration. This ensures that the cache
// maintains the minimum expiration for each user.
func (c *ClientCertExpirationCache) MaybeUpsert(
	ctx context.Context, user string, serial string, newExpiry int64,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// if the user is not in the cache, add them, and add their expiration to a gauge.
	if _, ok := c.mu.cache[user]; !ok {
		err := c.mu.acc.Grow(ctx, 2*GaugeSize)
		if err != nil {
			log.Ops.Warningf(ctx, "no memory available to cache cert expiry: %v", err)
			return
		}
		c.mu.cache[user] = map[string]certInfo{}
	}

	err := c.mu.acc.Grow(ctx, CertInfoSize)
	if err != nil {
		log.Warningf(ctx, "no memory available to cache cert expiry: %v", err)
		c.evictLocked(ctx, user, serial)
	}

	// insert / update the certificate expiration time.
	certs := c.mu.cache[user]
	certs[serial] = certInfo{
		expiration: newExpiry,
		last_seen:  timeutil.Unix(c.timeNow(), 0),
	}

	// update the metrics with the new certificate in mind.
	c.upsertMetricsLocked(user)
}

// Clear removes all entries from the cache.
func (c *ClientCertExpirationCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for user, certs := range c.mu.cache {
		for serial := range certs {
			c.evictLocked(context.Background(), user, serial)
		}
	}
}

// Len returns the number of cert expirations in the cache.
func (c *ClientCertExpirationCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.mu.cache)
}

// timeNow returns the current time depending on the time source of the cache.
func (c *ClientCertExpirationCache) timeNow() int64 {
	if timeSrc, ok := c.timeSrc.(timeutil.TimeSource); ok {
		return timeSrc.Now().Unix()
	}
	return timeutil.Now().Unix()
}

// timeSince returns the current time depending on the time source of the cache.
func (c *ClientCertExpirationCache) timeSince(t time.Time) time.Duration {
	if timeSrc, ok := c.timeSrc.(timeutil.TimeSource); ok {
		return timeSrc.Since(t)
	}
	return timeutil.Since(t)
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
				c.Purge(ctx)
			case <-c.stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			}
		}
	},
	)
}

// Purge removes any certificates which have not been seen in the last 24 hours.
func (c *ClientCertExpirationCache) Purge(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for user, certs := range c.mu.cache {
		for serial, cert := range certs {
			if c.timeSince(cert.last_seen) >= CacheTTL {
				c.evictLocked(ctx, user, serial)
			}
		}
	}
}

func (c *ClientCertExpirationCache) Account() *mon.BoundAccount {
	return &c.mu.acc
}

// evictLocked is a utility function for removing a specific certificate from the
// cache and removing the corresponding user if they have no more certificates.
func (c *ClientCertExpirationCache) evictLocked(ctx context.Context, user, serial string) {
	c.mu.acc.Shrink(ctx, CertInfoSize)
	delete(c.mu.cache[user], serial)

	// if there are no more certificates for the user, remove the user from the cache.
	// and remove their corresponding gauge.
	if len(c.mu.cache[user]) == 0 {
		delete(c.mu.cache, user)
		c.mu.acc.Shrink(ctx, 2*GaugeSize)
		c.removeMetricsLocked(user)
	} else {
		c.upsertMetricsLocked(user)
	}
}

// ttlFunc returns a function which computes the ttl for a given expiration.
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

// upsertMetricsLocked updates the expiration and ttl for a given user in the cache.
func (c *ClientCertExpirationCache) upsertMetricsLocked(user string) {
	expiration := c.getExpirationLocked(user)
	// the update functions on the metrics objects act as upserts.
	expMetric := c.mu.expirationMetrics.GetChild(user)
	if expMetric == nil {
		expMetric = c.mu.expirationMetrics.AddChild(user)
	}

	ttlMetric := c.mu.ttlMetrics.GetChild(user)
	if ttlMetric == nil {
		ttlMetric = c.mu.ttlMetrics.AddChild(user)
	}
	expMetric.Update(expiration)
	ttlMetric.UpdateFn(ttlFunc(c.timeNow, expiration))
}

// removeMetricsLocked removes the expiration and ttl for a given user from the cache.
func (c *ClientCertExpirationCache) removeMetricsLocked(user string) {
	c.mu.expirationMetrics.RemoveChild(user)
	c.mu.ttlMetrics.RemoveChild(user)
}

// jitteredInterval returns a randomly jittered (+/-25%) duration
// from the interval.
func jitteredInterval(interval time.Duration) time.Duration {
	return time.Duration(float64(interval) * (0.75 + 0.5*math_rand.Float64()))
}
