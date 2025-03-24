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

func TestEntryCache(t *testing.T) {
	ctx, clock, stopper, teardown := setup(t)
	defer teardown()

	const (
		fooUser  = "foo"
		barUser  = "bar"
		blahUser = "blah"
		fakeUser = "fake"

		laterExpiration  = int64(1684359292)
		closerExpiration = int64(1584359292)
	)

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
	cache.MaybeUpsert(ctx, barUser, defaultSerial, laterExpiration)
	require.Equal(t, 2, cache.Len())
	expiration = cache.GetExpiration(barUser)
	require.Equal(t, closerExpiration, expiration)

	// Verify indication of absence for non-existent values.
	expiration = cache.GetExpiration(fakeUser)
	require.Equal(t, int64(0), expiration)

	// Verify value of TTL metrics
	cache.Clear()
	clock.AdvanceTo(timeutil.Unix(closerExpiration+20, 0))
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

	const (
		fooUser = "foo"

		laterExpiration  = int64(1684359292)
		closerExpiration = int64(1584359292)
	)

	cache, expMetric, ttlMetric := newCacheAndMetrics(ctx, clock, stopper)
	require.Equal(t, 0, cache.Len())

	// insert.
	cache.MaybeUpsert(ctx, fooUser, defaultSerial, laterExpiration)
	// update.
	cache.MaybeUpsert(ctx, fooUser, defaultSerial, closerExpiration)

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
		Name:     mon.MakeMonitorName("test"),
		Settings: st,
	})

	clientcert.CacheTTL = time.Minute

	expirationMetrics := aggmetric.MakeBuilder("user").Gauge(metric.Metadata{})
	ttlMetrics := aggmetric.MakeBuilder("user").Gauge(metric.Metadata{})
	cache := clientcert.NewClientCertExpirationCache(ctx, st, stopper, clock, parentMon, expirationMetrics, ttlMetrics)
	return cache, expirationMetrics, ttlMetrics
}
