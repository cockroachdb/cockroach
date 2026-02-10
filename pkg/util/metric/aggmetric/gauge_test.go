// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aggmetric

import (
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

func TestSQLGaugeEviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const cacheSize = 10
	r := metric.NewRegistry()
	writePrometheusMetrics := WritePrometheusMetricsFunc(r)

	g := NewSQLGauge(metric.Metadata{
		Name: "foo_gauge",
	})
	r.AddMetric(g)
	cacheStorage := cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > 10
		},
	})
	g.mu.children = &UnorderedCacheWrapper{
		cache: cacheStorage,
	}
	g.mu.labelConfig = metric.LabelConfigAppAndDB

	for i := 0; i < cacheSize; i++ {
		g.Update(1, "1", strconv.Itoa(i))
	}

	testFile := "SQLGauge_pre_eviction.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))

	for i := 0 + cacheSize; i < cacheSize+5; i++ {
		g.Update(10, "2", strconv.Itoa(i))
	}

	testFile = "SQLGauge_post_eviction.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))
}

func TestHighCardinalityGauge(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := metric.NewRegistry()
	writePrometheusMetrics := WritePrometheusMetricsFunc(r)

	// Create a high cardinality gauge with a low max label values for testing
	g := NewHighCardinalityGauge(
		metric.Metadata{Name: "test_gauge"},
		metric.HighCardinalityMetricOptions{MaxLabelValues: 3},
		[]string{"tenant_id"},
	)
	r.AddMetric(g)

	// Initialize the metric
	labelCache := metric.NewLabelSliceCache()
	g.InitializeMetrics(labelCache)

	// Verify evicted child is created
	require.NotNil(t, g.evictedChild)
	require.Equal(t, int64(0), g.evictedChild.Value())

	// Add some children
	g.Inc(10, "tenant1")
	g.Inc(20, "tenant2")
	g.Inc(30, "tenant3")

	// Helper to compute label cache keys.
	cacheKey := func(labelVals ...string) metric.LabelSliceCacheKey {
		return metric.LabelSliceCacheKey(metricKey(labelVals...))
	}

	// Verify label cache references exist for all children pre-eviction.
	for _, tenant := range []string{"tenant1", "tenant2", "tenant3", "_evicted"} {
		val, ok := labelCache.Get(cacheKey(tenant))
		require.True(t, ok, "expected label cache entry for %s", tenant)
		require.Equal(t, int64(1), val.Counter.Load(), "expected ref count 1 for %s", tenant)
	}

	// Verify evicted child is not exported when value is zero
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, "HighCardinalityGauge_pre_eviction.txt"))

	// Manually set old timestamps to trigger eviction
	now := time.Now().Unix()
	oldTime := now - int64(metric.LabelValueTTL.Seconds()) - 10

	g.mu.Lock()
	g.mu.children.ForEach(func(cm ChildMetric) {
		child := cm.(*HighCardinalityChildGauge)
		if child != g.evictedChild {
			child.updatedAt.Store(oldTime)
		}
	})
	g.mu.Unlock()

	// Trigger eviction
	g.EvictStaleMetrics()

	// Verify evicted child has accumulated values
	expectedEvictedValue := int64(60) // 10 + 20 + 30
	require.Equal(t, expectedEvictedValue, g.evictedChild.Value())

	// Verify evicted children are removed from both the children storage
	// and the label cache.
	for _, tenant := range []string{"tenant1", "tenant2", "tenant3"} {
		_, ok := g.mu.children.Get(tenant)
		require.False(t, ok, "expected child %s to be removed from children storage after eviction", tenant)
		_, ok = labelCache.Get(cacheKey(tenant))
		require.False(t, ok, "expected label cache entry for %s to be removed after eviction", tenant)
	}

	// Verify only the _evicted child remains in children storage.
	require.Equal(t, 1, g.mu.children.Len())
	val, ok := labelCache.Get(cacheKey("_evicted"))
	require.True(t, ok, "expected label cache entry for _evicted to remain")
	require.Equal(t, int64(1), val.Counter.Load())

	// Add a new child after eviction
	g.Inc(5, "tenant4")

	// Verify new child's label cache entry exists.
	val, ok = labelCache.Get(cacheKey("tenant4"))
	require.True(t, ok, "expected label cache entry for tenant4")
	require.Equal(t, int64(1), val.Counter.Load())

	// Verify new children coexist with the evicted bucket
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, "HighCardinalityGauge_post_eviction.txt"))
}

func TestSQLGaugeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r := metric.NewRegistry()
	writePrometheusMetrics := WritePrometheusMetricsFunc(r)

	g := NewSQLGauge(metric.Metadata{
		Name: "foo_gauge"})
	cacheStorage := cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > 10
		},
	})
	g.mu.children = &UnorderedCacheWrapper{
		cache: cacheStorage,
	}
	r.AddMetric(g)
	g.mu.labelConfig = metric.LabelConfigAppAndDB

	g.Update(10, "1", "1")
	g.Update(10, "2", "2")

	g.Inc(5, "1", "1")
	g.Dec(2, "2", "2")

	testFile := "SQLGauge.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))
}
