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

func TestHighCardinalityGauge(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const cacheSize = 10
	r := metric.NewRegistry()
	writePrometheusMetrics := WritePrometheusMetricsFunc(r)

	g := NewHighCardinalityGauge(metric.HighCardinalityMetricOptions{
		Metadata: metric.Metadata{Name: "foo_gauge"},
	}, "database", "application_name")
	g.mu.children = &UnorderedCacheWrapper{
		cache: initialiseCacheStorageForTesting(),
	}
	r.AddMetric(g)

	// Initialize with a label slice cache to test eviction
	labelSliceCache := metric.NewLabelSliceCache()
	g.InitializeMetrics(labelSliceCache)

	for i := 0; i < cacheSize+5; i++ {
		g.Update(int64(i+1), "1", strconv.Itoa(i))
	}

	// wait more than cache eviction time to make sure that keys are not evicted based on only cache size.
	time.Sleep(6 * time.Second)

	testFile := "HighCardinalityGauge_pre_eviction.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))

	for i := 0; i < cacheSize+5; i++ {
		metricKey := metric.LabelSliceCacheKey(metricKey([]string{"1", strconv.Itoa(i)}))
		labelSliceValue, ok := labelSliceCache.Get(metricKey)
		require.True(t, ok, "missing labelSliceValue in label slice cache")
		require.Equal(t, int64(1), labelSliceValue.Counter.Load(), "the reference count should be 1")
		require.Equal(t, []string{"1", strconv.Itoa(i)}, labelSliceValue.LabelValues, "label values are mismatching")
	}

	for i := 0 + cacheSize; i < cacheSize+5; i++ {
		g.Inc(5, "2", strconv.Itoa(i))
	}

	testFile = "HighCardinalityGauge_post_eviction.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))

	for i := 0; i < 5; i++ {
		metricKey := metric.LabelSliceCacheKey(metricKey([]string{"1", strconv.Itoa(i)}))
		_, ok := labelSliceCache.Get(metricKey)
		require.False(t, ok, "labelSliceValue should not be present.")
	}

	for i := 10; i < 15; i++ {
		metricKey := metric.LabelSliceCacheKey(metricKey([]string{"1", strconv.Itoa(i)}))
		labelSliceValue, ok := labelSliceCache.Get(metricKey)
		require.True(t, ok, "missing labelSliceValue in label slice cache")
		require.Equal(t, int64(1), labelSliceValue.Counter.Load(), "the reference count should be 1")
		require.Equal(t, []string{"1", strconv.Itoa(i)}, labelSliceValue.LabelValues, "label values are mismatching")
	}

	for i := 10; i < 15; i++ {
		metricKey := metric.LabelSliceCacheKey(metricKey([]string{"2", strconv.Itoa(i)}))
		labelSliceValue, ok := labelSliceCache.Get(metricKey)
		require.True(t, ok, "missing labelSliceValue in label slice cache")
		require.Equal(t, int64(1), labelSliceValue.Counter.Load(), "the reference count should be 1")
		require.Equal(t, []string{"2", strconv.Itoa(i)}, labelSliceValue.LabelValues, "label values are mismatching")
	}
}

func TestHighCardinalityGaugeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r := metric.NewRegistry()
	writePrometheusMetrics := WritePrometheusMetricsFunc(r)

	g := NewHighCardinalityGauge(metric.HighCardinalityMetricOptions{
		Metadata: metric.Metadata{Name: "foo_gauge"},
	}, "database", "application_name")
	g.mu.children = &UnorderedCacheWrapper{
		cache: initialiseCacheStorageForTesting(),
	}

	r.AddMetric(g)

	// Test Update operation
	g.Update(10, "1", "1")
	g.Update(20, "2", "2")

	// Test Inc operation
	g.Inc(5, "1", "1")
	g.Dec(3, "2", "2")

	testFile := "HighCardinalityGauge.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))
}
