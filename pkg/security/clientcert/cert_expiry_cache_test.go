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

func assertMetricValue(t *testing.T, metrics *aggmetric.AggGauge, user string, value int64) {
	c := metrics.GetChild(user)
	require.NotNil(t, c)
	require.Equal(t, value, c.Value())
}

func TestEntryCache(t *testing.T) {
	ctx, clock, stopper, teardown := setup(t)
	defer teardown()

	fooUser := "foo"
	barUser := "bar"
	fakeUser := "fake"

	laterExpiration := timeutil.Now().Add(100 * time.Second).Unix()
	closerExpiration := timeutil.Now().Unix()

	cache, expirationMetrics, ttlMetrics := newCacheAndMetrics(ctx, clock, stopper)
	require.Equal(t, 0, cache.Len())

	// Verify insert.
	cache.Upsert(ctx, fooUser, defaultSerial, laterExpiration)
	require.Equal(t, 1, cache.Len())

	// Verify update.
	cache.Upsert(ctx, fooUser, defaultSerial, closerExpiration)
	require.Equal(t, 1, cache.Len())

	// Verify retrieval.
	assertMetricValue(t, expirationMetrics, fooUser, closerExpiration)

	// Verify the cache retains the minimum expiration for a user, assuming no
	// eviction.
	cache.Upsert(ctx, barUser, defaultSerial, closerExpiration)
	require.Equal(t, 2, cache.Len())
	cache.Upsert(ctx, barUser, secondarySerial, laterExpiration)
	require.Equal(t, 2, cache.Len())
	assertMetricValue(t, expirationMetrics, barUser, closerExpiration)

	// Verify indication of absence for non-existent values.
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, fakeUser)

	// Verify value of TTL metrics
	cache.Clear()
	when := timeutil.Unix(closerExpiration+20, 0)
	clock.AdvanceTo(when)
	cache.Upsert(ctx, fooUser, defaultSerial, closerExpiration)
	cache.Upsert(ctx, barUser, defaultSerial, laterExpiration)
	assertMetricValue(t, ttlMetrics, fooUser, int64(0))
	assertMetricValue(t, ttlMetrics, barUser, laterExpiration-(closerExpiration+20))
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
		cache.Upsert(ctx, "user1", "serial1", 100)
		cache.Purge(ctx)
		assertMetricValue(t, expirationMetrics, "user1", 100)
		assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user1")
	})

	t.Run("Purge last certificate for a user", func(t *testing.T) {
		cache, expirationMetrics, ttlMetrics := newCacheAndMetrics(ctx, clock, stopper)
		cache.Upsert(ctx, "user1", "serial1", 100)
		clock.Advance(time.Minute)
		cache.Purge(ctx)
		assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user1")
	})

	t.Run("Purge not the last certificate for a user", func(t *testing.T) {
		cache, expirationMetrics, ttlMetrics := newCacheAndMetrics(ctx, clock, stopper)
		cache.Upsert(ctx, "user1", "serial1", 100)
		clock.Advance(time.Minute)
		cache.Upsert(ctx, "user1", "serial2", 120)
		cache.Purge(ctx)
		assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user1")
	})

	t.Run("Purge certificates for multiple users", func(t *testing.T) {
		// In this test, user1 will have two certificates, and user2 will have one.
		// One certificate for each user will be purged, and therefore user1 will
		// be the only user "left" in the cache.
		cache, expirationMetrics, ttlMetrics := newCacheAndMetrics(ctx, clock, stopper)
		cache.Upsert(ctx, "user1", "serial1", 100)
		cache.Upsert(ctx, "user2", "serial2", 65)
		clock.Advance(time.Minute)
		cache.Upsert(ctx, "user1", "serial3", 150)
		cache.Purge(ctx)

		// verify that user 1 still exists, but user2 is gone.
		assertMetricValue(t, expirationMetrics, "user1", 150)
		assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user1")
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
	for i := range N {
		go func(i int) {
			if i%2 == 1 {
				cache.Upsert(ctx, user, defaultSerial, expiration)
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
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user1")
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user2")

	cache.Upsert(ctx, "user1", "serial1", 100)

	// check user1 in cache.
	assertMetricValue(t, expirationMetrics, "user1", 100)
	assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user1")
	// check user 2 not in the cache.
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user2")

	cache.Upsert(ctx, "user2", "serial2", 90)

	// check both in cache now.
	assertMetricValue(t, expirationMetrics, "user1", 100)
	assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user1")
	assertMetricValue(t, expirationMetrics, "user2", 90)
	assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user2")
}

func TestClear(t *testing.T) {
	ctx, clock, stopper, teardown := setup(t)
	defer teardown()
	cache, expirationMetrics, ttlMetrics := newCacheAndMetrics(ctx, clock, stopper)

	// Set up a user with multiple expired certificates.
	cache.Upsert(ctx, "user1", "serial1", 5)
	cache.Upsert(ctx, "user1", "serial2", 10)
	// Set up a user with a single expired, and one unexpired ceritificate.
	cache.Upsert(ctx, "user2", "serial3", 15)
	// Set up a user with a single expired certificate.
	cache.Upsert(ctx, "user3", "serial4", 20)

	clock.Advance(time.Minute)

	// user2's unexpired certificate.
	cache.Upsert(ctx, "user2", "serial5", 25)

	// Set up a user with no expired certificates.
	cache.Upsert(ctx, "user4", "serial6", 30)

	// Verify initial state
	assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user1")
	assertMetricValue(t, expirationMetrics, "user1", 5)
	assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user2")
	assertMetricValue(t, expirationMetrics, "user2", 15)
	assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user3")
	assertMetricValue(t, expirationMetrics, "user3", 20)
	assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user4")
	assertMetricValue(t, expirationMetrics, "user4", 30)
	// check for a user who will be added after the clear call.
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user5")
	// check for a user who is not in the cache.
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user6")

	// Clear all certificates
	cache.Clear()

	cache.Upsert(ctx, "user5", "serial7", 35)

	// Verify final state - all users should be removed
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user1")
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user2")
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user3")
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user4")
	// nothing changes for a user who has never been seen.
	assertMetricsMissingUser(t, expirationMetrics, ttlMetrics, "user6")

	// user5 should be the only one in the cache.
	assertMetricsHasUser(t, expirationMetrics, ttlMetrics, "user5")
	assertMetricValue(t, expirationMetrics, "user5", 35)
}

func TestAllocationTracking(t *testing.T) {
	ctx, clock, stopper, teardown := setup(t)
	defer teardown()

	var certInfoSize, gaugeSize = clientcert.CertInfoSize, clientcert.GaugeSize

	t.Run("starts at zero", func(t *testing.T) {
		_, account := newCacheAndAccount(ctx, clock, stopper)
		// verify no usage to start
		require.Equal(t, int64(0), account.Used())
	})

	t.Run("increases two gauge and num certs per user", func(t *testing.T) {
		cache, account := newCacheAndAccount(ctx, clock, stopper)
		cache.Upsert(ctx, "user1", "serial1", 100)
		cache.Upsert(ctx, "user1", "serial2", 120)
		cache.Upsert(ctx, "user2", "serial3", 65)
		cache.Upsert(ctx, "user2", "serial4", 75)
		require.Equal(t, (4*certInfoSize)+(4*gaugeSize), account.Used())
	})

	t.Run("Purge removes the gauges and cert allocations correctly", func(t *testing.T) {
		cache, account := newCacheAndAccount(ctx, clock, stopper)
		cache.Upsert(ctx, "user1", "serial1", 100)
		cache.Upsert(ctx, "user1", "serial2", 120)
		cache.Upsert(ctx, "user2", "serial3", 65)
		clock.Advance(time.Minute)
		require.Equal(t, (3*certInfoSize)+(4*gaugeSize), account.Used())
		cache.Purge(ctx)
		cache.Upsert(ctx, "user2", "serial4", 75)
		require.Equal(t, certInfoSize+(2*gaugeSize), account.Used())
	})

	t.Run("Clear also removes the gauges and cert allocations correctly", func(t *testing.T) {
		cache, account := newCacheAndAccount(ctx, clock, stopper)
		cache.Upsert(ctx, "user1", "serial1", 100)
		cache.Upsert(ctx, "user1", "serial2", 120)
		cache.Upsert(ctx, "user2", "serial3", 65)
		cache.Upsert(ctx, "user2", "serial4", 75)
		require.Equal(t, (4*certInfoSize)+(4*gaugeSize), account.Used())
		cache.Clear()
		require.Equal(t, int64(0), account.Used())
	})

	t.Run("overwriting an existing certificate does not change the reported memory allocation", func(t *testing.T) {
		cache, account := newCacheAndAccount(ctx, clock, stopper)
		require.Equal(t, int64(0), account.Used())
		cache.Upsert(ctx, "user1", "serial1", 100)
		require.Equal(t, certInfoSize+(2*gaugeSize), account.Used())
		cache.Upsert(ctx, "user1", "serial1", 100)
		require.Equal(t, certInfoSize+(2*gaugeSize), account.Used())
		cache.Upsert(ctx, "user1", "serial1", 100)
		require.Equal(t, certInfoSize+(2*gaugeSize), account.Used())
	})
}

func TestExpiration(t *testing.T) {
	ctx, clock, stopper, teardown := setup(t)
	defer teardown()

	t.Run("basic test", func(t *testing.T) {
		cache, expMetrics, _ := newCacheAndMetrics(ctx, clock, stopper)
		cache.Upsert(ctx, "user1", "serial1", 100)
		assertMetricValue(t, expMetrics, "user1", 100)
	})

	// This should work, but it's unlikely this is a possible use case.
	t.Run("update existing certificate", func(t *testing.T) {
		cache, expMetrics, _ := newCacheAndMetrics(ctx, clock, stopper)
		cache.Upsert(ctx, "user1", "serial1", 100)
		cache.Upsert(ctx, "user1", "serial1", 90)
		assertMetricValue(t, expMetrics, "user1", 90)
	})

	t.Run("multiple certs for a single user", func(t *testing.T) {
		cache, expMetrics, _ := newCacheAndMetrics(ctx, clock, stopper)
		cache.Upsert(ctx, "user1", "serial1", 120)
		cache.Upsert(ctx, "user1", "serial2", 80)
		// verify that the lowest expiration is chosen.
		assertMetricValue(t, expMetrics, "user1", 80)

		// verify that when last inserted doesn't matter.
		cache.Upsert(ctx, "user1", "serial3", 100)
		assertMetricValue(t, expMetrics, "user1", 80)
	})

	t.Run("multiple users", func(t *testing.T) {
		cache, expMetrics, _ := newCacheAndMetrics(ctx, clock, stopper)
		cache.Upsert(ctx, "user1", "serial1", 80)
		cache.Upsert(ctx, "user2", "serial2", 120)
		cache.Upsert(ctx, "user3", "serial3", 60)

		assertMetricValue(t, expMetrics, "user1", 80)
		assertMetricValue(t, expMetrics, "user2", 120)
		assertMetricValue(t, expMetrics, "user3", 60)
	})

	t.Run("returns nothing when the certificates are purged", func(t *testing.T) {
		cache, expMetrics, ttlMetrics := newCacheAndMetrics(ctx, clock, stopper)
		cache.Upsert(ctx, "user1", "serial1", 100)
		clock.Advance(time.Minute)
		cache.Purge(ctx)
		assertMetricsMissingUser(t, expMetrics, ttlMetrics, "user1")
	})
}

func TestTTL(t *testing.T) {
	ctx, _, stopper, teardown := setup(t)
	defer teardown()

	t.Run("reports ttl based on expiration", func(t *testing.T) {
		manClock := timeutil.NewManualTime(time.Unix(50, 0))
		cache, _, ttlMetrics := newCacheAndMetrics(ctx, manClock, stopper)

		cache.Upsert(ctx, "user1", "serial1", 100)
		assertMetricValue(t, ttlMetrics, "user1", 50)
	})

	t.Run("negative ttls get clamped to 0", func(t *testing.T) {
		manClock := timeutil.NewManualTime(time.Unix(50, 0))
		cache, _, ttlMetrics := newCacheAndMetrics(ctx, manClock, stopper)
		cache.Upsert(ctx, "user1", "serial1", 49)

		assertMetricValue(t, ttlMetrics, "user1", 0)
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
		cache.Upsert(ctx, "foo", defaultSerial, clock.Now().Unix())
		cache.Upsert(ctx, "bar", defaultSerial, clock.Now().Unix())
		cache.Upsert(ctx, "blah", defaultSerial, clock.Now().Unix())
	}
}

func newCache(
	ctx context.Context, clock *timeutil.ManualTime, stopper *stop.Stopper,
) *clientcert.Cache {
	cache, _, _ := newCacheAndMetrics(ctx, clock, stopper)
	return cache
}

func newCacheAndMetrics(
	ctx context.Context, clock *timeutil.ManualTime, stopper *stop.Stopper,
) (*clientcert.Cache, *aggmetric.AggGauge, *aggmetric.AggGauge) {
	cache, expirationMetrics, ttlMetrics, _ := newCacheAndMetricsAndAccount(ctx, clock, stopper)
	return cache, expirationMetrics, ttlMetrics
}

func newCacheAndAccount(
	ctx context.Context, clock *timeutil.ManualTime, stopper *stop.Stopper,
) (*clientcert.Cache, *mon.BoundAccount) {
	cache, _, _, account := newCacheAndMetricsAndAccount(ctx, clock, stopper)
	return cache, account
}

func newCacheAndMetricsAndAccount(
	ctx context.Context, clock *timeutil.ManualTime, stopper *stop.Stopper,
) (*clientcert.Cache, *aggmetric.AggGauge, *aggmetric.AggGauge, *mon.BoundAccount) {
	clientcert.CacheTTL = time.Minute
	expirationMetrics := aggmetric.MakeBuilder("user").Gauge(metric.Metadata{})
	ttlMetrics := aggmetric.MakeBuilder("user").Gauge(metric.Metadata{})
	account := mon.NewStandaloneUnlimitedAccount()
	cache := clientcert.NewCache(clock, account, expirationMetrics, ttlMetrics)
	return cache, expirationMetrics, ttlMetrics, account
}
