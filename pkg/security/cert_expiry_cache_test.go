// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security_test

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestEntryCache(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		fooUser  = "foo"
		barUser  = "bar"
		blahUser = "blah"
		fakeUser = "fake"

		laterExpiration  = int64(1684359292)
		closerExpiration = int64(1584359292)
	)

	ctx := context.Background()

	timesource := timeutil.NewManualTime(timeutil.Unix(0, 123))
	// Create a cache with a capacity of 3.
	cache, expMetric, ttlMetric := newCache(
		ctx,
		&cluster.Settings{},
		3, /* capacity */
		timesource,
	)
	require.Equal(t, 0, cache.Len())

	// Verify insert.
	cache.MaybeUpsert(ctx, fooUser, laterExpiration, expMetric, ttlMetric)
	require.Equal(t, 1, cache.Len())

	// Verify update.
	cache.MaybeUpsert(ctx, fooUser, closerExpiration, expMetric, ttlMetric)
	require.Equal(t, 1, cache.Len())

	// Verify retrieval.
	expiration, found := cache.GetExpiration(fooUser)
	require.Equal(t, true, found)
	require.Equal(t, closerExpiration, expiration)

	// Verify the cache retains the minimum expiration for a user, assuming no
	// eviction.
	cache.MaybeUpsert(ctx, barUser, closerExpiration, expMetric, ttlMetric)
	require.Equal(t, 2, cache.Len())
	cache.MaybeUpsert(ctx, barUser, laterExpiration, expMetric, ttlMetric)
	require.Equal(t, 2, cache.Len())
	expiration, found = cache.GetExpiration(barUser)
	require.Equal(t, true, found)
	require.Equal(t, closerExpiration, expiration)

	// Verify indication of absence for non-existent values.
	expiration, found = cache.GetExpiration(fakeUser)
	require.Equal(t, false, found)
	require.Equal(t, int64(0), expiration)

	// Verify eviction when the capacity is exceeded.
	cache.MaybeUpsert(ctx, blahUser, laterExpiration, expMetric, ttlMetric)
	require.Equal(t, 3, cache.Len())
	cache.MaybeUpsert(ctx, fakeUser, closerExpiration, expMetric, ttlMetric)
	require.Equal(t, 3, cache.Len())
	_, found = cache.GetExpiration(fooUser)
	require.Equal(t, false, found)
	_, found = cache.GetExpiration(barUser)
	require.Equal(t, true, found)

	// Verify previous entries can be inserted after the cache is cleared.
	cache.Clear()
	require.Equal(t, 0, cache.Len())
	_, found = cache.GetExpiration(fooUser)
	require.Equal(t, false, found)
	_, found = cache.GetExpiration(barUser)
	require.Equal(t, false, found)
	cache.MaybeUpsert(ctx, fooUser, laterExpiration, expMetric, ttlMetric)
	require.Equal(t, 1, cache.Len())
	cache.MaybeUpsert(ctx, barUser, laterExpiration, expMetric, ttlMetric)
	require.Equal(t, 2, cache.Len())
	expiration, found = cache.GetExpiration(fooUser)
	require.Equal(t, true, found)
	require.Equal(t, laterExpiration, expiration)
	expiration, found = cache.GetExpiration(barUser)
	require.Equal(t, true, found)
	require.Equal(t, laterExpiration, expiration)

	// Verify expirations in the past cannot be inserted into the cache.
	cache.Clear()
	cache.MaybeUpsert(ctx, fooUser, int64(0), expMetric, ttlMetric)
	require.Equal(t, 0, cache.Len())

	// Verify value of TTL metrics
	cache.Clear()
	timesource.AdvanceTo(timeutil.Unix(closerExpiration+20, 0))
	cache.MaybeUpsert(ctx, fooUser, closerExpiration, expMetric, ttlMetric)
	cache.MaybeUpsert(ctx, barUser, laterExpiration, expMetric, ttlMetric)
	ttl, found := cache.GetTTL(fooUser)
	require.Equal(t, false, found)
	require.Equal(t, int64(0), ttl)
	ttl, found = cache.GetTTL(barUser)
	require.Equal(t, true, found)
	require.Equal(t, laterExpiration-(closerExpiration+20), ttl)
}

func TestPurgePastEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		fooUser  = "foo"
		barUser  = "bar"
		blahUser = "blah"
		bazUser  = "baz"

		pastExpiration1  = int64(1000000000)
		pastExpiration2  = int64(2000000000)
		futureExpiration = int64(3000000000)
	)

	ctx := context.Background()

	// Create a cache with a capacity of 4.
	clock := timeutil.NewManualTime(timeutil.Unix(0, 123))
	cache, expMetric, ttlMetric := newCache(ctx, &cluster.Settings{}, 4 /* capacity */, clock)

	// Insert entries that we expect to be cleaned up after advancing in time.
	cache.MaybeUpsert(ctx, fooUser, pastExpiration1, expMetric, ttlMetric)
	cache.MaybeUpsert(ctx, barUser, pastExpiration2, expMetric, ttlMetric)
	cache.MaybeUpsert(ctx, blahUser, pastExpiration2, expMetric, ttlMetric)
	// Insert an entry that should NOT be removed after advancing in time
	// because it is still in the future.
	cache.MaybeUpsert(ctx, bazUser, futureExpiration, expMetric, ttlMetric)
	require.Equal(t, 4, cache.Len())

	// Advance time so that expirations have been reached already.
	clock.AdvanceTo(timeutil.Unix(2000000000, 123))

	// Verify an expiration from the past cannot be retrieved. Confirm it has
	// been removed after the attempt as well.
	_, found := cache.GetExpiration(fooUser)
	require.Equal(t, false, found)
	require.Equal(t, 3, cache.Len())

	// Verify that when the cache gets cleaned of the past expirations.
	// Confirm that expirations in the future do not get removed.
	cache.PurgePastExpirations()
	require.Equal(t, 1, cache.Len())
	_, found = cache.GetExpiration(bazUser)
	require.Equal(t, true, found)
}

// TestConcurrentUpdates ensures that concurrent updates do not race with each
// other.
func TestConcurrentUpdates(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := &cluster.Settings{}

	// Create a cache with a large capacity.
	cache, expMetric, ttlMetric := newCache(
		ctx,
		st,
		10000, /* capacity */
		timeutil.NewManualTime(timeutil.Unix(0, 123)),
	)

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
				cache.MaybeUpsert(ctx, user, expiration, expMetric, ttlMetric)
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
	st := &cluster.Settings{}
	clock := timeutil.NewManualTime(timeutil.Unix(0, 123))
	cache, expMetric, ttlMetric := newCache(ctx, st, 1000 /* capacity */, clock)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.MaybeUpsert(ctx, "foo", clock.Now().Unix(), expMetric, ttlMetric)
		cache.MaybeUpsert(ctx, "bar", clock.Now().Unix(), expMetric, ttlMetric)
		cache.MaybeUpsert(ctx, "blah", clock.Now().Unix(), expMetric, ttlMetric)
	}
}

func newCache(
	ctx context.Context, st *cluster.Settings, capacity int, clock *timeutil.ManualTime,
) (*security.ClientCertExpirationCache, *aggmetric.AggGauge, *aggmetric.AggGauge) {
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	security.ClientCertExpirationCacheCapacity.Override(ctx, &st.SV, int64(capacity))
	parentMon := mon.NewUnlimitedMonitor(ctx, mon.Options{
		Name:     mon.MakeMonitorName("test"),
		Settings: st,
	})
	cache := security.NewClientCertExpirationCache(ctx, st, stopper, clock, parentMon)
	return cache, aggmetric.MakeBuilder(security.SQLUserLabel).Gauge(metric.Metadata{}), aggmetric.MakeBuilder(security.SQLUserLabel).Gauge(metric.Metadata{})
}
