// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clientcert_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/clientcert"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

var defaultSerial = "1"
var secondarySerial = "2"

func setup(t *testing.T) (context.Context, *timeutil.ManualTime, *stop.Stopper, func()) {
	ctx := context.Background()
	stopper := stop.NewStopper()

	clock := timeutil.NewManualTime(timeutil.Now())
	leaktestChecker := leaktest.AfterTest(t)

	teardown := func() {
		stopper.Stop(ctx)
		leaktestChecker()
	}

	return ctx, clock, stopper, teardown
}

func metricsHasUser(metrics *aggmetric.AggGauge, user string) bool {
	has := false
	metrics.Each([]*io_prometheus_client.LabelPair{}, func(metric *io_prometheus_client.Metric) {
		if metric.GetLabel()[0].GetValue() == user {
			has = true
		}
	})
	return has
}

func assertMetricsHasUser(
	t *testing.T, expiration *aggmetric.AggGauge, ttl *aggmetric.AggGauge, user string,
) {
	if !metricsHasUser(expiration, user) {
		t.Fatal("expiration metrics does not contain user", user)
	}
	if !metricsHasUser(ttl, user) {
		t.Fatal("ttl metrics does not contain user", user)
	}
}

func assertMetricsMissingUser(
	t *testing.T, expiration *aggmetric.AggGauge, ttl *aggmetric.AggGauge, user string,
) {
	if metricsHasUser(expiration, user) {
		t.Fatal("expiration metrics does not contain user", user)
	}
	if metricsHasUser(ttl, user) {
		t.Fatal("ttl metrics does not contain user", user)
	}
}

func TestEntryCache(t *testing.T) {
	ctx, clock, stopper, teardown := setup(t)
	defer teardown()

	fooUser := "foo"
	barUser := "bar"
	fakeUser := "fake"

	laterExpiration := timeutil.Now().Add(100 * time.Second).Unix()
	closerExpiration := timeutil.Now().Unix()

	cache := newCache(ctx, clock, stopper)
	require.Equal(t, 0, cache.Len())

	// Verify insert.
	cache.MaybeUpsert(ctx, fooUser, defaultSerial, laterExpiration)
	require.Equal(t, 1, cache.Len())

	// Verify update.
	cache.MaybeUpsert(ctx, fooUser, defaultSerial, closerExpiration)
	require.Equal(t, 1, cache.Len())

	// Verify retrieval.
	expiration := cache.GetExpiration(fooUser)
	require.Equal(t, closerExpiration, expiration)

	// Verify the cache retains the minimum expiration for a user, assuming no
	// eviction.
	cache.MaybeUpsert(ctx, barUser, defaultSerial, closerExpiration)
	require.Equal(t, 2, cache.Len())
	cache.MaybeUpsert(ctx, barUser, secondarySerial, laterExpiration)
	require.Equal(t, 2, cache.Len())
	expiration = cache.GetExpiration(barUser)
	require.Equal(t, closerExpiration, expiration)

	// Verify indication of absence for non-existent values.
	expiration = cache.GetExpiration(fakeUser)
	require.Equal(t, int64(0), expiration)

	// Verify value of TTL metrics
	cache.Clear()
	when := timeutil.Unix(closerExpiration+20, 0)
	clock.AdvanceTo(when)
	cache.MaybeUpsert(ctx, fooUser, defaultSerial, closerExpiration)
	cache.MaybeUpsert(ctx, barUser, defaultSerial, laterExpiration)
	ttl := cache.GetTTL(fooUser)
	require.Equal(t, int64(0), ttl)
	ttl = cache.GetTTL(barUser)
	require.Equal(t, laterExpiration-(closerExpiration+20), ttl)
}

// TestCacheMetricsSync verifies that the cache metrics are correctly synchronized
// when entries are inserted and updated. It checks that the cache length and
// expiration times are properly updated and reflected in the metrics.
func TestCacheMetricsSync(t *testing.T) {
	ctx, clock, stopper, teardown := setup(t)
	defer teardown()

	findChildMetric := func(metrics *aggmetric.AggGauge, childName string) *io_prometheus_client.Metric {
		var result *io_prometheus_client.Metric
		metrics.Each([]*io_prometheus_client.LabelPair{}, func(metric *io_prometheus_client.Metric) {
			if metric.GetLabel()[0].GetValue() == childName {
				result = metric
			}
		})
		return result
	}

	fooUser := "foo"

	laterExpiration := timeutil.Now().Add(100 * time.Second).Unix()
	closerExpiration := timeutil.Now().Unix()
	clock.Backwards(time.Second * time.Duration(clock.Now().Unix()))

	cache, expMetric, ttlMetric := newCacheAndMetrics(ctx, clock, stopper)
	require.Equal(t, 0, cache.Len())

	// insert.
	cache.MaybeUpsert(ctx, fooUser, defaultSerial, laterExpiration)
	// update.
	cache.MaybeUpsert(ctx, fooUser, secondarySerial, closerExpiration)

	expFloat := *(findChildMetric(expMetric, fooUser).Gauge.Value)
	expiration := cache.GetExpiration(fooUser)
	ttlFloat := *(findChildMetric(ttlMetric, fooUser).Gauge.Value)
	ttl := cache.GetTTL(fooUser)

	// verify that both the cache and metric are in sync.
	require.Equal(t, closerExpiration, expiration)
	require.Equal(t, closerExpiration, int64(expFloat))
	require.Equal(t, closerExpiration, ttl)
	require.Equal(t, closerExpiration, int64(ttlFloat))
}

func TestPurge(t *testing.T) {
	ctx, clock, stopper, teardown := setup(t)
	defer teardown()

	t.Run("Purge empty cache", func(t *testing.T) {
		cache := newCache(ctx, clock, stopper)
		// nothing scary happens.
		cache.Purge(ctx)
	})

	t.Run("Purge when no certificates are evictable", func(t *testing.T) {
		cache, expirationMetrics, ttlMetrics := newCacheAndMetrics(ctx, clock, stopper)
		cache.MaybeUpsert(ctx, "user1", "serial1", 100)
		cache.Purge(ctx)
		require.Equal(t, int64(100), cache.GetExpiration("user1"))
		assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user1")
	})

	t.Run("Purge last certificate for a user", func(t *testing.T) {
		cache, expirationMetrics, ttlMetrics := newCacheAndMetrics(ctx, clock, stopper)
		cache.MaybeUpsert(ctx, "user1", "serial1", 100)
		clock.Advance(time.Minute)
		cache.Purge(ctx)
		require.Equal(t, int64(0), cache.GetExpiration("user1"))
		assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user1")
	})

	t.Run("Purge not the last certificate for a user", func(t *testing.T) {
		cache, expirationMetrics, ttlMetrics := newCacheAndMetrics(ctx, clock, stopper)
		cache.MaybeUpsert(ctx, "user1", "serial1", 100)
		clock.Advance(time.Minute)
		cache.MaybeUpsert(ctx, "user1", "serial2", 120)
		cache.Purge(ctx)
		require.Equal(t, int64(120), cache.GetExpiration("user1"))
		assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user1")
	})

	t.Run("Purge certificates for multiple users", func(t *testing.T) {
		// In this test, user1 will have two certificates, and user2 will have one.
		// One certificate for each user will be purged, and therefore user1 will
		// be the only user "left" in the cache.
		cache, expirationMetrics, ttlMetrics := newCacheAndMetrics(ctx, clock, stopper)
		cache.MaybeUpsert(ctx, "user1", "serial1", 100)
		cache.MaybeUpsert(ctx, "user2", "serial2", 65)
		clock.Advance(time.Minute)
		cache.MaybeUpsert(ctx, "user1", "serial3", 150)
		cache.Purge(ctx)

		// verify that user 1 still exists, but user2 is gone.
		require.Equal(t, int64(150), cache.GetExpiration("user1"))
		assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user1")
		require.Equal(t, int64(0), cache.GetExpiration("user2"))
		assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user2")
	})
}

// TestConcurrentUpdates ensures that concurrent updates do not race with each
// other.
func TestConcurrentUpdates(t *testing.T) {
	ctx, clock, stopper, teardown := setup(t)
	defer teardown()
	cache := newCache(ctx, clock, stopper)

	var (
		user       = "testUser"
		expiration = int64(1684359292)
	)

	// NB: N is chosen based on the race detector's limit of 8128 goroutines.
	const N = 8000
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			if i%2 == 1 {
				cache.MaybeUpsert(ctx, user, defaultSerial, expiration)
			} else {
				cache.Clear()
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	cache.Clear()
}

func TestUpsert(t *testing.T) {
	ctx, clock, stopper, teardown := setup(t)
	defer teardown()
	cache, expirationMetrics, ttlMetrics := newCacheAndMetrics(ctx, clock, stopper)

	// check no users in cache.
	require.Equal(t, int64(0), cache.GetExpiration("user1"))
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user1")
	require.Equal(t, int64(0), cache.GetExpiration("user2"))
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user2")

	cache.MaybeUpsert(ctx, "user1", "serial1", 100)

	// check user1 in cache.
	require.Equal(t, int64(100), cache.GetExpiration("user1"))
	assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user1")
	// check user 2 not in the cache.
	require.Equal(t, int64(0), cache.GetExpiration("user2"))
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user2")

	cache.MaybeUpsert(ctx, "user2", "serial2", 90)

	// check both in cache now.
	require.Equal(t, int64(100), cache.GetExpiration("user1"))
	assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user1")
	require.Equal(t, int64(90), cache.GetExpiration("user2"))
	assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user2")
}

func TestClear(t *testing.T) {
	ctx, clock, stopper, teardown := setup(t)
	defer teardown()
	cache, expirationMetrics, ttlMetrics := newCacheAndMetrics(ctx, clock, stopper)

	// Set up a user with multiple expired certificates.
	cache.MaybeUpsert(ctx, "user1", "serial1", 5)
	cache.MaybeUpsert(ctx, "user1", "serial2", 10)
	// Set up a user with a single expired, and one unexpired ceritificate.
	cache.MaybeUpsert(ctx, "user2", "serial3", 15)
	// Set up a user with a single expired certificate.
	cache.MaybeUpsert(ctx, "user3", "serial4", 20)

	clock.Advance(time.Minute)

	// user2's unexpired certificate.
	cache.MaybeUpsert(ctx, "user2", "serial5", 25)

	// Set up a user with no expired certificates.
	cache.MaybeUpsert(ctx, "user4", "serial6", 30)

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
	cache.Clear()

	cache.MaybeUpsert(ctx, "user5", "serial7", 35)

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
		account := newCache(ctx, clock, stopper).Account()
		// verify no usage to start
		require.Equal(t, int64(0), account.Used())
	})

	t.Run("increases two gauge and num certs per user", func(t *testing.T) {
		cache := newCache(ctx, clock, stopper)
		account := cache.Account()
		cache.MaybeUpsert(ctx, "user1", "serial1", 100)
		cache.MaybeUpsert(ctx, "user1", "serial2", 120)
		cache.MaybeUpsert(ctx, "user2", "serial3", 65)
		cache.MaybeUpsert(ctx, "user2", "serial4", 75)
		require.Equal(t, (4*certInfoSize)+(4*gaugeSize), account.Used())
	})

	t.Run("Purge removes the gauges and cert allocations correctly", func(t *testing.T) {
		cache := newCache(ctx, clock, stopper)
		account := cache.Account()
		cache.MaybeUpsert(ctx, "user1", "serial1", 100)
		cache.MaybeUpsert(ctx, "user1", "serial2", 120)
		cache.MaybeUpsert(ctx, "user2", "serial3", 65)
		clock.Advance(time.Minute)
		cache.Purge(ctx)
		cache.MaybeUpsert(ctx, "user2", "serial4", 75)
		require.Equal(t, certInfoSize+(2*gaugeSize), account.Used())
	})

	t.Run("Clear also removes the gauges and cert allocations correctly", func(t *testing.T) {
		cache := newCache(ctx, clock, stopper)
		account := cache.Account()
		cache.MaybeUpsert(ctx, "user1", "serial1", 100)
		cache.MaybeUpsert(ctx, "user1", "serial2", 120)
		cache.MaybeUpsert(ctx, "user2", "serial3", 65)
		cache.MaybeUpsert(ctx, "user2", "serial4", 75)
		cache.Clear()
		require.Equal(t, int64(0), account.Used())
	})
}

func TestGetExpiration(t *testing.T) {
	ctx, clock, stopper, teardown := setup(t)
	defer teardown()

	t.Run("basic test", func(t *testing.T) {
		cache := newCache(ctx, clock, stopper)
		cache.MaybeUpsert(ctx, "user1", "serial1", 100)
		require.Equal(t, int64(100), cache.GetExpiration("user1"))
	})

	// This should work, but it's unlikely this is a possible use case.
	t.Run("update existing certificate", func(t *testing.T) {
		cache := newCache(ctx, clock, stopper)
		cache.MaybeUpsert(ctx, "user1", "serial1", 100)
		cache.MaybeUpsert(ctx, "user1", "serial1", 90)
		require.Equal(t, int64(90), cache.GetExpiration("user1"))
	})

	t.Run("multiple certs for a single user", func(t *testing.T) {
		cache := newCache(ctx, clock, stopper)
		cache.MaybeUpsert(ctx, "user1", "serial1", 120)
		cache.MaybeUpsert(ctx, "user1", "serial2", 80)
		// verify that the lowest expiration is chosen.
		require.Equal(t, int64(80), cache.GetExpiration("user1"))

		// verify that when last inserted doesn't matter.
		cache.MaybeUpsert(ctx, "user1", "serial3", 100)
		require.Equal(t, int64(80), cache.GetExpiration("user1"))
	})

	t.Run("multiple users", func(t *testing.T) {
		cache := newCache(ctx, clock, stopper)
		cache.MaybeUpsert(ctx, "user1", "serial1", 80)
		cache.MaybeUpsert(ctx, "user2", "serial2", 120)
		cache.MaybeUpsert(ctx, "user3", "serial3", 60)

		require.Equal(t, int64(80), cache.GetExpiration("user1"))
		require.Equal(t, int64(120), cache.GetExpiration("user2"))
		require.Equal(t, int64(60), cache.GetExpiration("user3"))
	})

	t.Run("returns nothing when the certificates are purged", func(t *testing.T) {
		cache := newCache(ctx, clock, stopper)
		cache.MaybeUpsert(ctx, "user1", "serial1", 100)
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
		cache := newCache(ctx, manClock, stopper)

		cache.MaybeUpsert(ctx, "user1", "serial1", 100)

		require.Equal(t, int64(50), cache.GetTTL("user1"))
	})

	t.Run("negative ttls get clamped to 0", func(t *testing.T) {
		manClock := timeutil.NewManualTime(time.Unix(50, 0))
		cache := newCache(ctx, manClock, stopper)
		cache.MaybeUpsert(ctx, "user1", "serial1", 49)

		require.Equal(t, int64(0), cache.GetTTL("user1"))
	})
}

func BenchmarkCertExpirationCacheInsert(b *testing.B) {
	ctx := context.Background()
	clock := timeutil.NewManualTime(timeutil.Unix(0, 123))
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cache := newCache(ctx, clock, stopper)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.MaybeUpsert(ctx, "foo", defaultSerial, clock.Now().Unix())
		cache.MaybeUpsert(ctx, "bar", defaultSerial, clock.Now().Unix())
		cache.MaybeUpsert(ctx, "blah", defaultSerial, clock.Now().Unix())
	}
}

func newCache(
	ctx context.Context, clock *timeutil.ManualTime, stopper *stop.Stopper,
) *clientcert.ClientCertExpirationCache {
	cache, _, _ := newCacheAndMetrics(ctx, clock, stopper)
	return cache
}

func newCacheAndMetrics(
	ctx context.Context, clock *timeutil.ManualTime, stopper *stop.Stopper,
) (*clientcert.ClientCertExpirationCache, *aggmetric.AggGauge, *aggmetric.AggGauge) {
	st := &cluster.Settings{}
	parentMon := mon.NewUnlimitedMonitor(ctx, mon.Options{
		Name:     "test",
		Settings: st,
	})

	clientcert.CacheTTL = time.Minute

	expirationMetrics := aggmetric.MakeBuilder("user").Gauge(metric.Metadata{})
	ttlMetrics := aggmetric.MakeBuilder("user").Gauge(metric.Metadata{})
	cache := clientcert.NewClientCertExpirationCache(ctx, st, stopper, clock, parentMon, expirationMetrics, ttlMetrics)
	return cache, expirationMetrics, ttlMetrics
}
