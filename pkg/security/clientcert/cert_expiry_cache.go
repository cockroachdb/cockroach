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

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// MemAccount is an interface for tracking memory usage.
// It exists here because the types used for account tracking do not share a
// common interface.
type MemAccount interface {
	Grow(ctx context.Context, n int64) error
	Shrink(ctx context.Context, n int64)
}

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

// Cache contains a cache of gauge objects keyed by
// SQL username strings. It is a FIFO cache that stores gauges valued by
// minimum expiration of the client certs seen (per user).
type Cache struct {
	syncutil.Mutex
	cache             map[string]map[string]certInfo
	account           MemAccount
	expirationMetrics *aggmetric.AggGauge
	ttlMetrics        *aggmetric.AggGauge
	timeSrc           timeutil.TimeSource
}

// Cache keeps track of when users certificates are expiring. It does this by
// keeping track of every certificate which has been used within some recent
// time window (24h) and groups the certificates by user. When certificates are
// added or deleted from the cache for a specific user, the expiration metrics
// are updated to reflect the new earliest expiration seen for that user.
func NewCache(
	timeSrc timeutil.TimeSource,
	account MemAccount,
	expirationMetrics *aggmetric.AggGauge,
	ttlMetrics *aggmetric.AggGauge,
) *Cache {
	return &Cache{
		cache:             make(map[string]map[string]certInfo),
		account:           account,
		expirationMetrics: expirationMetrics,
		ttlMetrics:        ttlMetrics,
		timeSrc:           timeSrc,
	}
}

func (c *Cache) getExpirationLocked(user string) int64 {
	if certs, ok := c.cache[user]; ok {
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

// Upsert will insert or update a client cert for a particular
// user and certificate into the cache. If the user hasn't been
// seen before, it has the side effect of adding that user to
// the expiration and ttl gauges.
func (c *Cache) Upsert(ctx context.Context, user string, serial string, newExpiry int64) {
	c.Lock()
	defer c.Unlock()

	// if the user is not in the cache, add them, and add their expiration to a gauge.
	if _, ok := c.cache[user]; !ok {
		err := c.account.Grow(ctx, 2*GaugeSize)
		if err != nil {
			log.Ops.Warningf(ctx, "no memory available to cache cert expiry: %v", err)
			return
		}
		c.cache[user] = map[string]certInfo{}
	}

	err := c.account.Grow(ctx, CertInfoSize)
	if err != nil {
		log.Warningf(ctx, "no memory available to cache cert expiry: %v", err)
		c.evictLocked(ctx, user, serial)
	}

	// insert / update the certificate expiration time.
	certs := c.cache[user]
	certs[serial] = certInfo{
		expiration: newExpiry,
		last_seen:  c.timeSrc.Now(),
	}

	// update the metrics with the new certificate in mind.
	c.upsertMetricsLocked(user)
}

// Clear removes all entries from the cache.
func (c *Cache) Clear() {
	c.Lock()
	defer c.Unlock()

	for user, certs := range c.cache {
		for serial := range certs {
			c.evictLocked(context.Background(), user, serial)
		}
	}
}

// Len returns the number of users found in the cache.
func (c *Cache) Len() int {
	c.Lock()
	defer c.Unlock()

	return len(c.cache)
}

// StartPurgeJob runs an infinite loop in a goroutine which
// regularly evicts entries associated with expiration values that have already
// passed.
func (c *Cache) StartPurgeJob(ctx context.Context, stopper *stop.Stopper) error {
	return stopper.RunAsyncTask(ctx, "purge-cert-expiry-cache", func(context.Context) {
		const period = time.Hour

		for {
			select {
			case <-time.After(jitteredInterval(period)):
				c.Purge(ctx)
			case <-stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			}
		}
	},
	)
}

// Purge removes any certificates which have not been seen in the last 24 hours.
func (c *Cache) Purge(ctx context.Context) {
	c.Lock()
	defer c.Unlock()

	for user, certs := range c.cache {
		for serial, cert := range certs {
			if c.timeSrc.Since(cert.last_seen) >= CacheTTL {
				c.evictLocked(ctx, user, serial)
			}
		}
	}
}

// evictLocked is a utility function for removing a specific certificate from the
// cache and removing the corresponding user if they have no more certificates.
func (c *Cache) evictLocked(ctx context.Context, user, serial string) {
	c.account.Shrink(ctx, CertInfoSize)
	delete(c.cache[user], serial)

	// if there are no more certificates for the user, remove the user from the cache.
	// and remove their corresponding gauge.
	if len(c.cache[user]) == 0 {
		delete(c.cache, user)
		c.account.Shrink(ctx, 2*GaugeSize)
		c.removeMetricsLocked(user)
	} else {
		c.upsertMetricsLocked(user)
	}
}

// ttlFunc returns a function which computes the ttl for a given expiration.
func ttlFunc(now func() time.Time, exp int64) func() int64 {
	return func() int64 {
		ttl := exp - now().Unix()
		if ttl > 0 {
			return ttl
		} else {
			return 0
		}
	}
}

// upsertMetricsLocked updates the expiration and ttl for a given user in the cache.
func (c *Cache) upsertMetricsLocked(user string) {
	expiration := c.getExpirationLocked(user)
	// the update functions on the metrics objects act as upserts.
	c.expirationMetrics.Update(expiration, user)
	c.ttlMetrics.UpdateFn(ttlFunc(c.timeSrc.Now, expiration), user)
}

// removeMetricsLocked removes the expiration and ttl for a given user from the cache.
func (c *Cache) removeMetricsLocked(user string) {
	c.expirationMetrics.RemoveChild(user)
	c.ttlMetrics.RemoveChild(user)
}

// jitteredInterval returns a randomly jittered (+/-25%) duration
// from the interval.
func jitteredInterval(interval time.Duration) time.Duration {
	return time.Duration(float64(interval) * (0.75 + 0.5*math_rand.Float64()))
}
