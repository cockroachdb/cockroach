// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clientcert_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/clientcert"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) (context.Context, *timeutil.ManualTime, *stop.Stopper, func()) {
	ctx := context.Background()
	stopper := stop.NewStopper()

	clock := timeutil.NewManualTime(timeutil.Now())
	leaktestChecker := leaktest.AfterTest(t)

	// all tests in this package have a one minute ttl.
	clientcert.CacheTTL = time.Minute

	teardown := func() {
		stopper.Stop(ctx)
		leaktestChecker()
	}

	return ctx, clock, stopper, teardown
}

func newCacheMetricsAndAccount(
	clock timeutil.TimeSource, stopper *stop.Stopper,
) (*clientcert.Cache, *aggmetric.AggGauge, *aggmetric.AggGauge, *mon.BoundAccount) {
	expiration := aggmetric.NewGauge(metric.Metadata{Name: "cert_expiry_metric"}, "user")
	ttl := aggmetric.NewGauge(metric.Metadata{Name: "cert_ttl_metric"}, "user")
	account := mon.NewStandaloneUnlimitedAccount()
	cache := clientcert.NewCacheWithTimeSource(account, stopper, expiration, ttl, clock)
	return cache, expiration, ttl, account
}

func newCache(clock timeutil.TimeSource, stopper *stop.Stopper) *clientcert.Cache {
	cache, _, _, _ := newCacheMetricsAndAccount(clock, stopper)
	return cache
}

// cacheDotUpsert exists to get around a million error checks.
func cacheDotUpsert(
	t *testing.T, cache *clientcert.Cache, ctx context.Context, user, serial string, expiration int64,
) {
	err := cache.Upsert(ctx, user, serial, expiration)
	if err != nil {
		t.Fatal(err)
	}
}

func assertMetricsHasUser(
	t *testing.T, expiration *aggmetric.AggGauge, ttl *aggmetric.AggGauge, user string,
) {
	if !expiration.Has(user) {
		t.Fatal("expiration metrics does not contain user", user)
	}
	if !ttl.Has(user) {
		t.Fatal("ttl metrics does not contain user", user)
	}
}

func assertMetricsMissingUser(
	t *testing.T, expiration *aggmetric.AggGauge, ttl *aggmetric.AggGauge, user string,
) {
	if expiration.Has(user) {
		t.Fatal("expiration metrics contains user", user)
	}
	if ttl.Has(user) {
		t.Fatal("ttl metrics contains user", user)
	}
}

func TestUpsert(t *testing.T) {
	ctx, clock, stopper, teardown := setup(t)
	defer teardown()
	cache, expirationMetrics, ttlMetrics, _ := newCacheMetricsAndAccount(clock, stopper)

	// check no users in cache.
	require.False(t, expirationMetrics.Has("user1"))
	require.False(t, expirationMetrics.Has("user2"))
	require.False(t, ttlMetrics.Has("user1"))
	require.False(t, ttlMetrics.Has("user2"))
	require.Equal(t, int64(0), cache.GetExpiration("user1"))
	require.Equal(t, int64(0), cache.GetExpiration("user2"))

	cacheDotUpsert(t, cache, ctx, "user1", "serial1", 100)

	// check user1 in cache.
	require.True(t, expirationMetrics.Has("user1"))
	require.True(t, ttlMetrics.Has("user1"))
	require.Equal(t, int64(100), cache.GetExpiration("user1"))
	// check user 2 not in the cache.
	require.False(t, expirationMetrics.Has("user2"))
	require.False(t, ttlMetrics.Has("user2"))
	require.Equal(t, int64(0), cache.GetExpiration("user2"))

	cacheDotUpsert(t, cache, ctx, "user2", "serial2", 90)

	// check both in cache now.
	require.True(t, expirationMetrics.Has("user1"))
	require.True(t, ttlMetrics.Has("user1"))
	require.Equal(t, int64(100), cache.GetExpiration("user1"))
	require.True(t, expirationMetrics.Has("user2"))
	require.True(t, ttlMetrics.Has("user2"))
	require.Equal(t, int64(90), cache.GetExpiration("user2"))
}

func TestGetExpiration(t *testing.T) {
	ctx, clock, stopper, teardown := setup(t)
	defer teardown()

	t.Run("basic test", func(t *testing.T) {
		cache := newCache(clock, stopper)
		cacheDotUpsert(t, cache, ctx, "user1", "serial1", 100)
		require.Equal(t, int64(100), cache.GetExpiration("user1"))
	})

	// This should work, but it's unlikely this is a possible use case.
	t.Run("update existing certificate", func(t *testing.T) {
		cache := newCache(clock, stopper)
		cacheDotUpsert(t, cache, ctx, "user1", "serial1", 100)
		cacheDotUpsert(t, cache, ctx, "user1", "serial1", 90)
		require.Equal(t, int64(90), cache.GetExpiration("user1"))
	})

	t.Run("multiple certs for a single user", func(t *testing.T) {
		cache := newCache(clock, stopper)
		cacheDotUpsert(t, cache, ctx, "user1", "serial1", 120)
		cacheDotUpsert(t, cache, ctx, "user1", "serial2", 80)
		// verify that the lowest expiration is chosen.
		require.Equal(t, int64(80), cache.GetExpiration("user1"))

		// verify that when last inserted doesn't matter.
		cacheDotUpsert(t, cache, ctx, "user1", "serial3", 100)
		require.Equal(t, int64(80), cache.GetExpiration("user1"))
	})

	t.Run("multiple users", func(t *testing.T) {
		cache := newCache(clock, stopper)
		cacheDotUpsert(t, cache, ctx, "user1", "serial1", 80)
		cacheDotUpsert(t, cache, ctx, "user2", "serial2", 120)
		cacheDotUpsert(t, cache, ctx, "user3", "serial3", 60)

		require.Equal(t, int64(80), cache.GetExpiration("user1"))
		require.Equal(t, int64(120), cache.GetExpiration("user2"))
		require.Equal(t, int64(60), cache.GetExpiration("user3"))
	})

	t.Run("returns nothing when the certificates are purged", func(t *testing.T) {
		cache := newCache(clock, stopper)
		cacheDotUpsert(t, cache, ctx, "user1", "serial1", 100)
		clock.Advance(time.Minute)
		cache.Purge(ctx)
		require.Equal(t, int64(0), cache.GetExpiration("user1"))
	})
}

func TestGetTTL(t *testing.T) {
	ctx, _, stopper, teardown := setup(t)
	defer teardown()

	t.Run("reports ttl based on expiration", func(t *testing.T) {
		manClock := timeutil.NewManualTime(time.Unix(50, 0))
		cache := newCache(manClock, stopper)

		cacheDotUpsert(t, cache, ctx, "user1", "serial1", 100)

		require.Equal(t, int64(50), cache.GetTTL("user1"))
	})

	t.Run("negative ttls get clamped to 0", func(t *testing.T) {
		manClock := timeutil.NewManualTime(time.Unix(50, 0))
		cache := newCache(manClock, stopper)
		cacheDotUpsert(t, cache, ctx, "user1", "serial1", 49)

		require.Equal(t, int64(0), cache.GetTTL("user1"))
	})
}

func TestPurge(t *testing.T) {
	ctx, clock, stopper, teardown := setup(t)
	defer teardown()

	t.Run("Purge empty cache", func(t *testing.T) {
		cache := newCache(clock, stopper)
		// nothing scary happens.
		cache.Purge(ctx)
	})

	t.Run("Purge when no certificates are evictable", func(t *testing.T) {
		cache, expirationMetrics, ttlMetrics, _ := newCacheMetricsAndAccount(clock, stopper)
		cacheDotUpsert(t, cache, ctx, "user1", "serial1", 100)
		cache.Purge(ctx)
		require.Equal(t, int64(100), cache.GetExpiration("user1"))
		assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user1")
	})

	t.Run("Purge last certificate for a user", func(t *testing.T) {
		cache, expirationMetrics, ttlMetrics, _ := newCacheMetricsAndAccount(clock, stopper)
		cacheDotUpsert(t, cache, ctx, "user1", "serial1", 100)
		clock.Advance(time.Minute)
		cache.Purge(ctx)
		require.Equal(t, int64(0), cache.GetExpiration("user1"))
		assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user1")
	})

	t.Run("Purge not the last certificate for a user", func(t *testing.T) {
		cache, expirationMetrics, ttlMetrics, _ := newCacheMetricsAndAccount(clock, stopper)
		cacheDotUpsert(t, cache, ctx, "user1", "serial1", 100)
		clock.Advance(time.Minute)
		cacheDotUpsert(t, cache, ctx, "user1", "serial2", 120)
		cache.Purge(ctx)
		require.Equal(t, int64(120), cache.GetExpiration("user1"))
		assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user1")
	})

	t.Run("Purge certificates for multiple users", func(t *testing.T) {
		// In this test, user1 will have two certificates, and user2 will have one.
		// One certificate for each user will be purged, and therefore user1 will
		// be the only user "left" in the cache.
		cache, expirationMetrics, ttlMetrics, _ := newCacheMetricsAndAccount(clock, stopper)
		cacheDotUpsert(t, cache, ctx, "user1", "serial1", 100)
		cacheDotUpsert(t, cache, ctx, "user2", "serial2", 65)
		clock.Advance(time.Minute)
		cacheDotUpsert(t, cache, ctx, "user1", "serial3", 150)
		cache.Purge(ctx)

		// verify that user 1 still exists, but user2 is gone.
		require.Equal(t, int64(150), cache.GetExpiration("user1"))
		assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user1")
		require.Equal(t, int64(0), cache.GetExpiration("user2"))
		assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user2")
	})
}

func TestClear(t *testing.T) {
	ctx, clock, stopper, teardown := setup(t)
	defer teardown()
	cache, expirationMetrics, ttlMetrics, _ := newCacheMetricsAndAccount(clock, stopper)

	// Set up a user with multiple expired certificates.
	cacheDotUpsert(t, cache, ctx, "user1", "serial1", 5)
	cacheDotUpsert(t, cache, ctx, "user1", "serial2", 10)
	// Set up a user with a single expired, and one unexpired ceritificate.
	cacheDotUpsert(t, cache, ctx, "user2", "serial3", 15)
	// Set up a user with a single expired certificate.
	cacheDotUpsert(t, cache, ctx, "user3", "serial4", 20)

	clock.Advance(time.Minute)

	// user2's unexpired certificate.
	cacheDotUpsert(t, cache, ctx, "user2", "serial5", 25)

	// Set up a user with no expired certificates.
	cacheDotUpsert(t, cache, ctx, "user4", "serial6", 30)

	// Verify initial state
	assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user1")
	require.Equal(t, int64(5), cache.GetExpiration("user1"))
	assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user2")
	require.Equal(t, int64(15), cache.GetExpiration("user2"))
	assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user3")
	require.Equal(t, int64(20), cache.GetExpiration("user3"))
	assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user4")
	require.Equal(t, int64(30), cache.GetExpiration("user4"))
	// check for a user who will be added after the clear call.
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user5")
	require.Equal(t, int64(0), cache.GetExpiration("user5"))
	// check for a user who is not in the cache.
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user6")
	require.Equal(t, int64(0), cache.GetExpiration("user6"))

	// Clear all certificates
	cache.Clear(ctx)

	cacheDotUpsert(t, cache, ctx, "user5", "serial7", 35)

	// Verify final state - all users should be removed
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user1")
	require.Equal(t, int64(0), cache.GetExpiration("user1"))
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user2")
	require.Equal(t, int64(0), cache.GetExpiration("user2"))
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user3")
	require.Equal(t, int64(0), cache.GetExpiration("user3"))
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user4")
	require.Equal(t, int64(0), cache.GetExpiration("user4"))
	// nothing changes for a user who has never been seen.
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user6")
	require.Equal(t, int64(0), cache.GetExpiration("user6"))

	// user5 should be the only one in the cache.
	assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user5")
	require.Equal(t, int64(35), cache.GetExpiration("user5"))
}

func TestAllocationTracking(t *testing.T) {
	ctx, clock, stopper, teardown := setup(t)
	defer teardown()

	var certInfoSize, gaugeSize = clientcert.CertInfoSize, clientcert.GaugeSize

	t.Run("starts at zero", func(t *testing.T) {
		_, _, _, account := newCacheMetricsAndAccount(clock, stopper)
		// verify no usage to start
		require.Equal(t, int64(0), account.Used())
	})

	t.Run("increases two gauge and num certs per user", func(t *testing.T) {
		cache, _, _, account := newCacheMetricsAndAccount(clock, stopper)
		cacheDotUpsert(t, cache, ctx, "user1", "serial1", 100)
		cacheDotUpsert(t, cache, ctx, "user1", "serial2", 120)
		cacheDotUpsert(t, cache, ctx, "user2", "serial3", 65)
		cacheDotUpsert(t, cache, ctx, "user2", "serial4", 75)
		require.Equal(t, (4*certInfoSize)+(4*gaugeSize), account.Used())
	})

	t.Run("Purge removes the gauges and cert allocations correctly", func(t *testing.T) {
		cache, _, _, account := newCacheMetricsAndAccount(clock, stopper)
		cacheDotUpsert(t, cache, ctx, "user1", "serial1", 100)
		cacheDotUpsert(t, cache, ctx, "user1", "serial2", 120)
		cacheDotUpsert(t, cache, ctx, "user2", "serial3", 65)
		clock.Advance(time.Minute)
		cache.Purge(ctx)
		cacheDotUpsert(t, cache, ctx, "user2", "serial4", 75)
		require.Equal(t, certInfoSize+(2*gaugeSize), account.Used())
	})

	t.Run("Clear also removes the gauges and cert allocations correctly", func(t *testing.T) {
		cache, _, _, account := newCacheMetricsAndAccount(clock, stopper)
		cacheDotUpsert(t, cache, ctx, "user1", "serial1", 100)
		cacheDotUpsert(t, cache, ctx, "user1", "serial2", 120)
		cacheDotUpsert(t, cache, ctx, "user2", "serial3", 65)
		cacheDotUpsert(t, cache, ctx, "user2", "serial4", 75)
		cache.Clear(ctx)
		require.Equal(t, int64(0), account.Used())
	})
}
