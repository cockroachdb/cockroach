// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clientcert

import (
	"context"
	"time"
	"unsafe"

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

// PurgeLoopInterval is the interval at which the cache should be purged.
var PurgeLoopInterval = time.Hour

// The size of the cache and the certInfo struct. To be used for memory tracking.
var GaugeSize = int64(unsafe.Sizeof(aggmetric.AggGauge{}))
var CertInfoSize = int64(unsafe.Sizeof(certInfo{}))

// Cache keeps track of when users certificates are expiring. It does this by
// keeping track of every certificate which has been used within some recent
// time window (24h) and groups the certificates by user. When metrics are
// read, the cache reports for each user the earliest expiration for any of
// their recently used certificates.
type Cache struct {
	mu      syncutil.Mutex
	account MemAccount

	cache             map[string]map[string]certInfo
	expirationMetrics *aggmetric.AggGauge
	ttlMetrics        *aggmetric.AggGauge
	stopper           *stop.Stopper
	timesource        timeutil.TimeSource
}

func NewCache(
	account MemAccount,
	stopper *stop.Stopper,
	expirationMetrics *aggmetric.AggGauge,
	ttlMetrics *aggmetric.AggGauge,
) *Cache {
	return NewCacheWithTimeSource(account, stopper, expirationMetrics, ttlMetrics, timeutil.DefaultTimeSource{})
}

func NewCacheWithTimeSource(
	account MemAccount,
	stopper *stop.Stopper,
	expirationMetrics *aggmetric.AggGauge,
	ttlMetrics *aggmetric.AggGauge,
	timesource timeutil.TimeSource,
) *Cache {
	cache := &Cache{
		account: account,

		cache:             make(map[string]map[string]certInfo),
		expirationMetrics: expirationMetrics,
		ttlMetrics:        ttlMetrics,
		stopper:           stopper,
		timesource:        timesource,
	}
	return cache
}

// Upsert updates the cache with the expiration time of a certificate.
// If the user is not already in the cache, a gauge for its expiration is added.
func (c *Cache) Upsert(ctx context.Context, user, serial string, expiration int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// if the user is not in the cache, add them, and add their expiration to a gauge.
	if _, ok := c.cache[user]; !ok {
		c.cache[user] = map[string]certInfo{}
		c.expirationMetrics.AddFunctionalChild(func() int64 { return c.GetExpiration(user) }, user)
		c.ttlMetrics.AddFunctionalChild(func() int64 { return c.GetTTL(user) }, user)

		err := c.account.Grow(ctx, 2*GaugeSize)
		if err != nil {
			return err
		}
	}

	// insert / update the certificate expiration time.
	certs := c.cache[user]
	certs[serial] = certInfo{
		expiration: expiration,
		last_seen:  c.timesource.Now(),
	}

	return c.account.Grow(ctx, CertInfoSize)
}

// GetTTL returns the time-to-live of the earliest expiration in seconds.
// It returns 0 if the user is not in the cache or if the earliest expiration
// has already passed.
func (c *Cache) GetTTL(user string) int64 {
	expiration := c.GetExpiration(user)
	ttl := expiration - c.timesource.Now().Unix()
	if ttl > 0 {
		return ttl
	} else {
		return 0
	}
}

// GetExpiration returns the earliest expiration time for any of the certificates
// used by the given user. It returns 0 if the user is not in the cache.
func (c *Cache) GetExpiration(user string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

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

// Purge removes any certificates which have not been seen in the last 24 hours.
func (c *Cache) Purge(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for user, certs := range c.cache {
		for serial, cert := range certs {
			if c.timesource.Since(cert.last_seen) >= CacheTTL {
				c.evictLocked(ctx, user, serial)
			}
		}
	}
}

// StartPurgeLoop runs an infinite loop in a goroutine which
// regularly evicts entries which haven't been seen past the ttl.
func (c *Cache) StartPurgeLoop(ctx context.Context) error {
	return c.stopper.RunAsyncTask(ctx, "purge-cert-expiry-cache", func(context.Context) {
		var timer timeutil.Timer
		defer timer.Stop()

		for {
			timer.Reset(timeutil.Jitter(PurgeLoopInterval, .25))
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

// Clear evicts all certificates from the cache.
func (c *Cache) Clear(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for user := range c.cache {
		for serial := range c.cache[user] {
			c.evictLocked(ctx, user, serial)
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
		c.expirationMetrics.RemoveChild(user)
		c.ttlMetrics.RemoveChild(user)
	}
}
