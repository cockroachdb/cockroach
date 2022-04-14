// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package balancer

import (
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestTenantCache(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cache := newTenantCache()
	require.Equal(t, 0, cache.ActiveCountByAddr("foo"))
	require.Equal(t, 0, cache.IdleCountByAddr("foo"))

	// Refresh the cache's data.
	data := newTenantCacheData()
	data.activeConnsCount["foo"] = 5
	data.activeConnsCount["bar"] = 10
	data.idleConnsCount["bar"] = 2
	cache.refreshData(data)

	require.Equal(t, 0, cache.ActiveCountByAddr("baz"))
	require.Equal(t, 5, cache.ActiveCountByAddr("foo"))
	require.Equal(t, 10, cache.ActiveCountByAddr("bar"))
	require.Equal(t, 0, cache.IdleCountByAddr("foo"))
	require.Equal(t, 2, cache.IdleCountByAddr("bar"))

	// Active counts can be updated, and won't drop below 0.
	cache.updateActiveCount("foo", 3)
	cache.updateActiveCount("foo", -2)
	cache.updateActiveCount("foo", 0)
	require.Equal(t, 6, cache.ActiveCountByAddr("foo"))
	cache.updateActiveCount("foo", -10)
	require.Equal(t, 0, cache.ActiveCountByAddr("foo"))

	// Adding a new pod should work.
	cache.updateActiveCount("carl", 3)
	require.Equal(t, 3, cache.ActiveCountByAddr("carl"))

	// Idle counts can be updated, and won't drop below 0.
	cache.updateIdleCount("bar", 3)
	cache.updateIdleCount("bar", -2)
	cache.updateIdleCount("bar", 0)
	require.Equal(t, 3, cache.IdleCountByAddr("bar"))
	cache.updateIdleCount("bar", -10)
	cache.updateIdleCount("bar", 10)
	require.Equal(t, 10, cache.IdleCountByAddr("bar"))

	// Adding a new pod should work.
	cache.updateIdleCount("foo", 3)
	require.Equal(t, 3, cache.IdleCountByAddr("foo"))

	// Reset the cache to empty.
	cache.refreshData(newTenantCacheData())
	require.Equal(t, 0, cache.ActiveCountByAddr("foo"))
	require.Equal(t, 0, cache.ActiveCountByAddr("bar"))
	require.Equal(t, 0, cache.IdleCountByAddr("foo"))
	require.Equal(t, 0, cache.IdleCountByAddr("bar"))

	// Test concurrency on active and idle counts.
	const runs = 100
	var wg sync.WaitGroup
	wg.Add(2 * runs)
	for i := 0; i < runs; i++ {
		go func() {
			defer wg.Done()
			cache.updateActiveCount("apple", 1)
			cache.updateIdleCount("apple", 2)
		}()

		// At the same time try to read data.
		go func() {
			defer wg.Done()
			_ = cache.ActiveCountByAddr("apple")
			_ = cache.IdleCountByAddr("apple")
		}()
	}

	wg.Wait()
	require.Equal(t, 100, cache.ActiveCountByAddr("apple"))
	require.Equal(t, 200, cache.IdleCountByAddr("apple"))
}
