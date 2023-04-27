// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security_test

import (
	"context"
	"math"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestEntryCache(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := &cluster.Settings{}

	// Create a cache with a capacity of 3.
	cache, metric := newCacheTest(ctx, st, 3 /* capacity */)

	const (
		fooUser  = "foo"
		barUser  = "bar"
		blahUser = "blah"
		fakeUser = "fake"

		laterExpiration  = int64(1684359292)
		closerExpiration = int64(1584359292)
	)

	// Verify insert.
	cache.MaybeUpsert(ctx, fooUser, laterExpiration, metric)
	require.Equal(t, 1, cache.Len())

	// Verify update.
	cache.MaybeUpsert(ctx, fooUser, closerExpiration, metric)
	require.Equal(t, 1, cache.Len())

	// Verify retrieval.
	expiration, found := cache.Get(fooUser)
	require.Equal(t, true, found)
	require.Equal(t, closerExpiration, expiration)

	// Verify the cache retains the minimum expiration for a user, assuming no
	// eviction.
	cache.MaybeUpsert(ctx, barUser, closerExpiration, metric)
	require.Equal(t, 2, cache.Len())
	cache.MaybeUpsert(ctx, barUser, laterExpiration, metric)
	require.Equal(t, 2, cache.Len())
	expiration, found = cache.Get(barUser)
	require.Equal(t, true, found)
	require.Equal(t, closerExpiration, expiration)

	// Verify indication of absence for non-existent values.
	expiration, found = cache.Get(fakeUser)
	require.Equal(t, false, found)
	require.Equal(t, int64(0), expiration)

	// Verify eviction when the capacity is exceeded.
	cache.MaybeUpsert(ctx, blahUser, laterExpiration, metric)
	require.Equal(t, 3, cache.Len())
	cache.MaybeUpsert(ctx, fakeUser, closerExpiration, metric)
	require.Equal(t, 3, cache.Len())
	_, found = cache.Get(fooUser)
	require.Equal(t, false, found)
	_, found = cache.Get(barUser)
	require.Equal(t, true, found)

	// Verify previous entries can be inserted after the cache is cleared.
	cache.Clear()
	require.Equal(t, 0, cache.Len())
	_, found = cache.Get(fooUser)
	require.Equal(t, false, found)
	_, found = cache.Get(barUser)
	require.Equal(t, false, found)
	cache.MaybeUpsert(ctx, fooUser, laterExpiration, metric)
	require.Equal(t, 1, cache.Len())
	cache.MaybeUpsert(ctx, barUser, laterExpiration, metric)
	require.Equal(t, 2, cache.Len())
	expiration, found = cache.Get(fooUser)
	require.Equal(t, true, found)
	require.Equal(t, laterExpiration, expiration)
	expiration, found = cache.Get(barUser)
	require.Equal(t, true, found)
	require.Equal(t, laterExpiration, expiration)
}

// TestConcurrentUpdates ensures that concurrent updates do not race with each
// other.
func TestConcurrentUpdates(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := &cluster.Settings{}

	// Create a cache with a large capacity.
	cache, metric := newCacheTest(ctx, st, 10000 /* capacity */)

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
				cache.MaybeUpsert(ctx, user, expiration, metric)
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
	cache, metric := newCacheTest(ctx, st, 1000 /* capacity */)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.MaybeUpsert(ctx, "foo", clock.Now().Unix(), metric)
		cache.MaybeUpsert(ctx, "bar", clock.Now().Unix(), metric)
		cache.MaybeUpsert(ctx, "blah", clock.Now().Unix(), metric)
	}
}

func newCacheTest(
	ctx context.Context, st *cluster.Settings, capacity int,
) (*security.ClientCertExpirationCache, *aggmetric.AggGauge) {
	security.ClientCertExpirationCacheCapacity.Override(ctx, &st.SV, int64(capacity))
	cache := security.NewClientCertExpirationCache(st, mon.NewUnlimitedMonitor(
		ctx,
		"test", /* name */
		mon.MemoryResource,
		nil, /* currCount */
		nil, /* maxHist */
		math.MaxInt64,
		st,
	))
	return cache, aggmetric.MakeBuilder(security.SQLUserLabel).Gauge(metric.Metadata{})
}
