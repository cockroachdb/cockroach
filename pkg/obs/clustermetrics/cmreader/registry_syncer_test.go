// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmreader

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmmetrics"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmwatcher"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

// makeTestRegistrySyncer creates an registrySyncer with a fresh registry for unit testing.
// It does not create a watcher or connect to any rangefeed.
func makeTestRegistrySyncer() (*registrySyncer, *registry) {
	reg := newRegistry()
	u := &registrySyncer{registry: reg}
	u.mu.trackedMetrics = make(map[string]metric.Iterable)
	u.mu.trackedRows = make(map[int64]cmwatcher.ClusterMetricRow)
	return u, reg
}

func TestUpdateMetricLocked(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name   string
		setup  func() func()
		rows   []cmwatcher.ClusterMetricRow
		verify func(t *testing.T, u *registrySyncer, reg *registry)
	}{{
		name: "new gauge",
		setup: func() func() {
			return cmmetrics.TestingRegisterClusterMetric(
				"test.gauge", metric.Metadata{
					Name: "test.gauge",
					Help: "A test gauge",
				})
		},
		rows: []cmwatcher.ClusterMetricRow{{
			ID: 1, Name: "test.gauge", Type: "gauge", Value: 42,
		}},
		verify: func(t *testing.T, u *registrySyncer, reg *registry) {
			require.Contains(t, u.mu.trackedMetrics, "test.gauge")
			require.Contains(t, u.mu.trackedRows, int64(1))

			var found bool
			reg.Each(func(name string, _ interface{}) {
				if name == "test.gauge" {
					found = true
				}
			})
			require.True(t, found, "metric should be in registry")

			g := u.mu.trackedMetrics["test.gauge"].(*metric.Gauge)
			require.Equal(t, int64(42), g.Value())
		},
	}, {
		name: "existing gauge update",
		setup: func() func() {
			return cmmetrics.TestingRegisterClusterMetric(
				"test.gauge", metric.Metadata{
					Name: "test.gauge",
					Help: "A test gauge",
				})
		},
		rows: []cmwatcher.ClusterMetricRow{{
			ID: 1, Name: "test.gauge", Type: "gauge", Value: 10,
		}, {
			ID: 1, Name: "test.gauge", Type: "gauge", Value: 99,
		}},
		verify: func(t *testing.T, u *registrySyncer, _ *registry) {
			g := u.mu.trackedMetrics["test.gauge"].(*metric.Gauge)
			require.Equal(t, int64(99), g.Value())
		},
	}, {
		name: "counter",
		setup: func() func() {
			return cmmetrics.TestingRegisterClusterMetric(
				"test.counter", metric.Metadata{
					Name: "test.counter",
					Help: "A test counter",
				})
		},
		rows: []cmwatcher.ClusterMetricRow{{
			ID: 1, Name: "test.counter", Type: "counter", Value: 0,
		}, {
			ID: 1, Name: "test.counter", Type: "counter", Value: 42,
		}},
		verify: func(t *testing.T, u *registrySyncer, _ *registry) {
			c := u.mu.trackedMetrics["test.counter"].(*metric.Counter)
			require.Equal(t, int64(42), c.Count())
		},
	}, {
		name:  "not found",
		setup: func() func() { return func() {} },
		rows: []cmwatcher.ClusterMetricRow{{
			ID: 999, Name: "nonexistent.metric", Type: "gauge", Value: 42,
		}},
		verify: func(t *testing.T, u *registrySyncer, _ *registry) {
			require.Empty(t, u.mu.trackedMetrics)
			require.Empty(t, u.mu.trackedRows)
		},
	}, {
		name: "new gauge vec",
		setup: func() func() {
			return cmmetrics.TestingRegisterLabeledClusterMetric(
				"test.gaugevec", metric.Metadata{
					Name: "test.gaugevec",
					Help: "A test gauge vec",
				}, []string{"store"})
		},
		rows: []cmwatcher.ClusterMetricRow{{
			ID: 1, Name: "test.gaugevec",
			Labels: map[string]string{"store": "1"},
			Type:   "gauge", Value: 42,
		}},
		verify: func(t *testing.T, u *registrySyncer, reg *registry) {
			require.Contains(t, u.mu.trackedMetrics, "test.gaugevec")
			_, ok := u.mu.trackedMetrics["test.gaugevec"].(*metric.GaugeVec)
			require.True(t, ok, "expected *metric.GaugeVec")
			require.Contains(t, u.mu.trackedRows, int64(1))

			var found bool
			reg.Each(func(name string, _ interface{}) {
				if name == "test.gaugevec" {
					found = true
				}
			})
			require.True(t, found, "metric should be in registry")
		},
	}, {
		name: "vec second label set",
		setup: func() func() {
			return cmmetrics.TestingRegisterLabeledClusterMetric(
				"test.gaugevec", metric.Metadata{
					Name: "test.gaugevec",
					Help: "A test gauge vec",
				}, []string{"store"})
		},
		rows: []cmwatcher.ClusterMetricRow{{
			ID: 1, Name: "test.gaugevec",
			Labels: map[string]string{"store": "1"},
			Type:   "gauge", Value: 10,
		}, {
			ID: 2, Name: "test.gaugevec",
			Labels: map[string]string{"store": "2"},
			Type:   "gauge", Value: 20,
		}},
		verify: func(t *testing.T, u *registrySyncer, _ *registry) {
			require.Contains(t, u.mu.trackedRows, int64(1))
			require.Contains(t, u.mu.trackedRows, int64(2))
			require.Len(t, u.mu.trackedMetrics, 1)
			require.Contains(t, u.mu.trackedMetrics, "test.gaugevec")
		},
	}, {
		name: "existing gauge vec update",
		setup: func() func() {
			return cmmetrics.TestingRegisterLabeledClusterMetric(
				"test.gaugevec", metric.Metadata{
					Name: "test.gaugevec",
					Help: "A test gauge vec",
				}, []string{"store"})
		},
		rows: []cmwatcher.ClusterMetricRow{{
			ID: 1, Name: "test.gaugevec",
			Labels: map[string]string{"store": "1"},
			Type:   "gauge", Value: 10,
		}, {
			ID: 1, Name: "test.gaugevec",
			Labels: map[string]string{"store": "1"},
			Type:   "gauge", Value: 99,
		}},
		verify: func(t *testing.T, u *registrySyncer, _ *registry) {
			require.Contains(t, u.mu.trackedRows, int64(1))
			require.Equal(t, int64(99),
				u.mu.trackedRows[int64(1)].Value)
		},
	}, {
		name: "counter vec",
		setup: func() func() {
			return cmmetrics.TestingRegisterLabeledClusterMetric(
				"test.countervec", metric.Metadata{
					Name: "test.countervec",
					Help: "A test counter vec",
				}, []string{"store"})
		},
		rows: []cmwatcher.ClusterMetricRow{{
			ID: 1, Name: "test.countervec",
			Labels: map[string]string{"store": "1"},
			Type:   "counter", Value: 10,
		}, {
			ID: 2, Name: "test.countervec",
			Labels: map[string]string{"store": "2"},
			Type:   "counter", Value: 20,
		}, {
			ID: 1, Name: "test.countervec",
			Labels: map[string]string{"store": "1"},
			Type:   "counter", Value: 50,
		}},
		verify: func(t *testing.T, u *registrySyncer, _ *registry) {
			require.Contains(t, u.mu.trackedMetrics, "test.countervec")
			cv, ok := u.mu.trackedMetrics["test.countervec"].(*metric.CounterVec)
			require.True(t, ok, "expected *metric.CounterVec")
			require.Len(t, u.mu.trackedMetrics, 1)
			require.Equal(t, int64(50),
				cv.Count(map[string]string{"store": "1"}))
			require.Equal(t, int64(20),
				cv.Count(map[string]string{"store": "2"}))
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer tt.setup()()

			ctx := context.Background()
			u, reg := makeTestRegistrySyncer()

			u.mu.Lock()
			for _, row := range tt.rows {
				u.updateMetricLocked(ctx, row)
			}
			u.mu.Unlock()

			tt.verify(t, u, reg)
		})
	}
}

func TestDeleteMetricLocked(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name       string
		setup      func() func()
		insertRows []cmwatcher.ClusterMetricRow
		deleteRow  cmwatcher.ClusterMetricRow
		verify     func(t *testing.T, u *registrySyncer, reg *registry)
	}{{
		name: "scalar gauge",
		setup: func() func() {
			return cmmetrics.TestingRegisterClusterMetric(
				"test.gauge", metric.Metadata{
					Name: "test.gauge",
					Help: "A test gauge",
				})
		},
		insertRows: []cmwatcher.ClusterMetricRow{{
			ID: 1, Name: "test.gauge", Type: "gauge", Value: 42,
		}},
		deleteRow: cmwatcher.ClusterMetricRow{ID: 1},
		verify: func(t *testing.T, u *registrySyncer, reg *registry) {
			require.NotContains(t, u.mu.trackedMetrics, "test.gauge")
			require.NotContains(t, u.mu.trackedRows, int64(1))

			var count int
			reg.Each(func(_ string, _ interface{}) { count++ })
			require.Equal(t, 0, count)
		},
	}, {
		name: "vec metric label set",
		setup: func() func() {
			return cmmetrics.TestingRegisterLabeledClusterMetric(
				"test.vec", metric.Metadata{
					Name: "test.vec",
					Help: "A test vector gauge",
				}, []string{"store"})
		},
		insertRows: []cmwatcher.ClusterMetricRow{{
			ID: 1, Name: "test.vec",
			Labels: map[string]string{"store": "1"},
			Type:   "gauge", Value: 42,
		}},
		deleteRow: cmwatcher.ClusterMetricRow{ID: 1},
		verify: func(t *testing.T, u *registrySyncer, reg *registry) {
			// Vec metric stays in trackedMetrics and registry even
			// after all label sets are deleted.
			require.Contains(t, u.mu.trackedMetrics, "test.vec")
			require.NotContains(t, u.mu.trackedRows, int64(1))

			var found bool
			reg.Each(func(name string, _ interface{}) {
				if name == "test.vec" {
					found = true
				}
			})
			require.True(t, found,
				"vec metric should remain in registry")
		},
	}, {
		name:      "unknown ID",
		setup:     func() func() { return func() {} },
		deleteRow: cmwatcher.ClusterMetricRow{ID: 999},
		verify: func(t *testing.T, u *registrySyncer, _ *registry) {
			require.Empty(t, u.mu.trackedMetrics)
			require.Empty(t, u.mu.trackedRows)
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer tt.setup()()

			ctx := context.Background()
			u, reg := makeTestRegistrySyncer()

			u.mu.Lock()
			for _, row := range tt.insertRows {
				u.updateMetricLocked(ctx, row)
			}
			u.deregisterMetricLocked(ctx, tt.deleteRow)
			u.mu.Unlock()

			tt.verify(t, u, reg)
		})
	}
}

func TestStop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer cmmetrics.TestingRegisterClusterMetric("gauge_one", metric.Metadata{
		Name: "gauge_one",
		Help: "First gauge",
	})()
	defer cmmetrics.TestingRegisterClusterMetric("gauge_two", metric.Metadata{
		Name: "gauge_two",
		Help: "Second gauge",
	})()

	ctx := context.Background()
	u, reg := makeTestRegistrySyncer()

	u.mu.Lock()
	u.updateMetricLocked(ctx, cmwatcher.ClusterMetricRow{
		ID: 1, Name: "gauge_one", Type: "gauge", Value: 1,
	})
	u.updateMetricLocked(ctx, cmwatcher.ClusterMetricRow{
		ID: 2, Name: "gauge_two", Type: "gauge", Value: 2,
	})
	u.mu.Unlock()

	var count int
	reg.Each(func(_ string, _ interface{}) { count++ })
	require.Equal(t, 2, count)

	u.stop()

	count = 0
	reg.Each(func(_ string, _ interface{}) { count++ })
	require.Equal(t, 0, count)
}

func TestOnRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer cmmetrics.TestingRegisterClusterMetric("gauge.old", metric.Metadata{
		Name: "gauge.old",
		Help: "Old gauge",
	})()
	defer cmmetrics.TestingRegisterClusterMetric("gauge.new", metric.Metadata{
		Name: "gauge.new",
		Help: "New gauge",
	})()
	defer cmmetrics.TestingRegisterClusterMetric("counter.new", metric.Metadata{
		Name: "counter.new",
		Help: "New counter",
	})()

	ctx := context.Background()
	u, reg := makeTestRegistrySyncer()

	// Pre-populate with an old metric.
	u.mu.Lock()
	u.updateMetricLocked(ctx, cmwatcher.ClusterMetricRow{
		ID: 1, Name: "gauge.old", Type: "gauge", Value: 1,
	})
	u.mu.Unlock()

	require.Contains(t, u.mu.trackedMetrics, "gauge.old")

	// Simulate a full refresh with a different set of rows.
	refreshRows := map[int64]cmwatcher.ClusterMetricRow{
		10: {ID: 10, Name: "gauge.new", Type: "gauge", Value: 100},
		20: {ID: 20, Name: "counter.new", Type: "counter", Value: 200},
	}
	u.reloadAllMetrics(ctx, refreshRows)

	// Old metric should be gone.
	require.NotContains(t, u.mu.trackedMetrics, "gauge.old")
	require.NotContains(t, u.mu.trackedRows, int64(1))

	// New metrics should be present.
	require.Contains(t, u.mu.trackedMetrics, "gauge.new")
	require.Contains(t, u.mu.trackedMetrics, "counter.new")
	require.Contains(t, u.mu.trackedRows, int64(10))
	require.Contains(t, u.mu.trackedRows, int64(20))

	// Old metric should be removed from registry.
	var oldFound bool
	reg.Each(func(name string, _ interface{}) {
		if name == "gauge.old" {
			oldFound = true
		}
	})
	require.False(t, oldFound, "old metric should not be in registry")

	// New metrics should be in registry.
	var newGaugeFound, newCounterFound bool
	reg.Each(func(name string, _ interface{}) {
		switch name {
		case "gauge.new":
			newGaugeFound = true
		case "counter.new":
			newCounterFound = true
		}
	})
	require.True(t, newGaugeFound, "gauge.new should be in registry")
	require.True(t, newCounterFound, "counter.new should be in registry")

	// Verify values.
	g := u.mu.trackedMetrics["gauge.new"].(*metric.Gauge)
	require.Equal(t, int64(100), g.Value())
	c := u.mu.trackedMetrics["counter.new"].(*metric.Counter)
	require.Equal(t, int64(200), c.Count())
}
