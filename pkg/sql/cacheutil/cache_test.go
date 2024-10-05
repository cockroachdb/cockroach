// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cacheutil

import (
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestCache(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	memoryMonitor := mon.NewMonitor(mon.Options{
		Name:     "test-mem",
		Settings: st,
	})
	stopper := &stop.Stopper{}
	ctx := context.Background()

	m := mon.NewStandaloneBudget(math.MaxInt64)
	memoryMonitor.Start(ctx, nil, m)

	cache := NewCache(memoryMonitor.MakeBoundAccount(), stopper, 2 /* numSystemTables */)

	isEligible := cache.ClearCacheIfStaleLocked(ctx, []descpb.DescriptorVersion{1, 0})
	require.Equal(t, isEligible, false)

	isEligible = cache.ClearCacheIfStaleLocked(ctx, []descpb.DescriptorVersion{2, 2})
	require.Equal(t, isEligible, false)

	require.Equal(t, cache.tableVersions, []descpb.DescriptorVersion{2, 2})

	isEligible = cache.ClearCacheIfStaleLocked(ctx, []descpb.DescriptorVersion{2, 2})
	require.Equal(t, isEligible, true)

	// In theory, only one call should happen to the func passed into
	// LoadValueOutsideOfCacheSingleFlight due to singleflight.
	// Testing that only one call happens is hard to synchronize, we would
	// have to add a test hook into `DoChan` to make synchronize our calls.
	for i := 0; i < 5; i++ {
		go func() {
			val, err := cache.LoadValueOutsideOfCacheSingleFlight(ctx, "test", func(loadCtx context.Context) (interface{}, error) {
				return "val", nil
			})
			require.NoError(t, err)
			require.Equal(t, val, "val")
		}()
	}

	wrote := cache.MaybeWriteBackToCache(ctx, []descpb.DescriptorVersion{2, 2}, "test", "val", int64(len("test")+len("val")))
	require.Equal(t, wrote, true)

	wrote = cache.MaybeWriteBackToCache(ctx, []descpb.DescriptorVersion{0, 2}, "test", "val", int64(len("test")+len("val")))
	require.Equal(t, wrote, false)

	wrote = cache.MaybeWriteBackToCache(ctx, []descpb.DescriptorVersion{2, 0}, "test", "val", int64(len("test")+len("val")))
	require.Equal(t, wrote, false)

	val, ok := cache.GetValueLocked("test")
	require.Equal(t, ok, true)
	require.Equal(t, val, "val")

	cache.ClearCacheIfStaleLocked(ctx, []descpb.DescriptorVersion{3, 3})

	_, ok = cache.GetValueLocked("test")
	require.Equal(t, ok, false)
}
