// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

func TestSoftLimiter(t *testing.T) {
	lim := SoftLimit{Metric: metric.NewGauge(metric.Metadata{}), Limit: 1000}
	require.Zero(t, lim.Metric.Value())
	lim.acquire(100)
	require.Equal(t, int64(100), lim.Metric.Value())
	require.True(t, lim.withinLimit())
	lim.acquire(900)
	require.Equal(t, int64(1000), lim.Metric.Value())
	require.False(t, lim.withinLimit())
	lim.release(100)
	require.Equal(t, int64(900), lim.Metric.Value())
	require.True(t, lim.withinLimit())
	lim.acquire(10000)
	require.Equal(t, int64(10900), lim.Metric.Value())
	require.False(t, lim.withinLimit())
	lim.release(900)
	require.Equal(t, int64(10000), lim.Metric.Value())
	require.False(t, lim.withinLimit())
	lim.release(10000)
	require.Zero(t, lim.Metric.Value())
	require.True(t, lim.withinLimit())

	lim.Limit = 0 // no limit
	lim.acquire(100)
	require.True(t, lim.withinLimit())
	lim.acquire(1000000)
	require.True(t, lim.withinLimit())
}

func TestBytesAccount(t *testing.T) {
	lim := SoftLimit{Metric: metric.NewGauge(metric.Metadata{}), Limit: 1 << 20}
	a1, a2 := lim.NewAccount(nil), lim.NewAccount(nil)
	require.True(t, a1.Grow(256<<10))

	require.True(t, a2.Grow(128<<10))
	require.True(t, a2.Grow(256<<10))
	require.False(t, a2.Grow(512<<10))
	require.Equal(t, uint64(512+256+128)<<10, a2.used)
	require.Zero(t, a2.reserved)
	// a2 returns all the reserved bytes to the limiter.
	require.Equal(t, int64(a1.used+a1.reserved+a2.used), lim.Metric.Value())

	require.False(t, a1.Grow(10))
	require.Equal(t, uint64(10+256<<10), a1.used)
	require.Zero(t, a1.reserved)
	// a1 returns all the reserved bytes to the limiter.
	require.Equal(t, int64(a1.used+a2.used), lim.Metric.Value())

	require.False(t, lim.withinLimit())
	a2.Clear()
	require.True(t, lim.withinLimit())
	require.True(t, a1.Grow(1000))
	a1.Clear()

	require.Zero(t, lim.Metric.Value())
}

func TestSizeHelper(t *testing.T) {
	lim := SoftLimit{Metric: metric.NewGauge(metric.Metadata{}), Limit: 1 << 20}
	for _, tt := range []struct {
		max   uint64
		sizes []uint64
		take  int
	}{
		// Limits are not reached.
		{max: 1023, sizes: []uint64{10}, take: 1},
		{max: 100, sizes: []uint64{10, 10, 20, 10}, take: 4},
		// Max size limit is reached.
		{max: 100, sizes: []uint64{30, 30, 30, 30}, take: 3},
		{max: 100, sizes: []uint64{100, 1}, take: 1},
		{max: 100, sizes: []uint64{200, 1}, take: 1},
		{max: 1000, sizes: []uint64{100, 900, 100}, take: 2},
		{max: 1000, sizes: []uint64{100, 500, 10000}, take: 2},
		{max: 1000, sizes: []uint64{100, 1000}, take: 1},
		// Soft limiter kicks in.
		{max: 64 << 20, sizes: []uint64{8 << 20}, take: 1},
		{max: 64 << 20, sizes: []uint64{4 << 20, 4 << 20}, take: 1},
		{max: 64 << 20, sizes: []uint64{1 << 20, 1 << 20, 1 << 20}, take: 1},
		{max: 64 << 20, sizes: []uint64{100, 1 << 20, 1 << 20}, take: 2},
		{max: 64 << 20, sizes: []uint64{100, 1000, 1234, 1 << 20, 1 << 20}, take: 4},
	} {
		t.Run("", func(t *testing.T) {
			acc := lim.NewAccount(nil)
			defer acc.Clear()
			sh := sizeHelper{maxBytes: tt.max, account: &acc}

			took := 0
			for ln := len(tt.sizes); took < ln && !sh.done; took++ {
				if size := tt.sizes[took]; !sh.add(size) {
					require.True(t, sh.done)
					break
				}
			}
			require.Equal(t, tt.take, took)

			var want uint64
			for _, size := range tt.sizes[:tt.take] {
				want += size
			}
			require.Equal(t, want, sh.bytes)
		})
	}
}
