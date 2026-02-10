// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aggmetric

import (
	"bufio"
	"bytes"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
	"sort"
	"strconv"
	"strings"
)

func TestAggCounter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const cacheSize = 10
	r := metric.NewRegistry()
	writePrometheusMetrics := func(t *testing.T) string {
		var in bytes.Buffer
		ex := metric.MakePrometheusExporter()
		scrape := func(ex *metric.PrometheusExporter) {
			ex.ScrapeRegistry(r, metric.WithIncludeChildMetrics(true), metric.WithIncludeAggregateMetrics(true))
		}
		require.NoError(t, ex.ScrapeAndPrintAsText(&in, expfmt.FmtText, scrape))
		var lines []string
		for sc := bufio.NewScanner(&in); sc.Scan(); {
			if !bytes.HasPrefix(sc.Bytes(), []byte{'#'}) {
				lines = append(lines, sc.Text())
			}
		}
		sort.Strings(lines)
		return strings.Join(lines, "\n")
	}

	c := NewSQLCounter(metric.Metadata{
		Name: "foo_counter",
	})
	c.mu.labelConfig = metric.LabelConfigAppAndDB
	r.AddMetric(c)
	cacheStorage := cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > 10
		},
	})
	c.mu.children = &UnorderedCacheWrapper{
		cache: cacheStorage,
	}

	for i := 0; i < cacheSize; i++ {
		c.Inc(1, "1", strconv.Itoa(i))
	}

	testFile := "SQLCounter_pre_eviction.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))

	for i := 0 + cacheSize; i < cacheSize+5; i++ {
		c.Inc(1, "2", strconv.Itoa(i))
	}

	testFile = "SQLCounter_post_eviction.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))
}

func TestHighCardinalityCounter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := metric.NewRegistry()
	writePrometheusMetrics := WritePrometheusMetricsFunc(r)

	// Create a high cardinality counter with a low max label values for testing
	c := NewHighCardinalityCounter(
		metric.Metadata{Name: "test_counter"},
		metric.HighCardinalityMetricOptions{MaxLabelValues: 3},
		[]string{"tenant_id"},
	)
	r.AddMetric(c)

	// Initialize the metric
	labelCache := metric.NewLabelSliceCache()
	c.InitializeMetrics(labelCache)

	// Verify evicted child is created
	require.NotNil(t, c.evictedChild)
	require.Equal(t, int64(0), c.evictedChild.Value())

	// Add some children
	c.Inc(10, "tenant1")
	c.Inc(20, "tenant2")
	c.Inc(30, "tenant3")

	// Helper to compute label cache keys.
	cacheKey := func(labelVals ...string) metric.LabelSliceCacheKey {
		return metric.LabelSliceCacheKey(metricKey(labelVals...))
	}

	// Verify label cache references to exist for all children pre-eviction.
	for _, tenant := range []string{"tenant1", "tenant2", "tenant3", "_evicted"} {
		val, ok := labelCache.Get(cacheKey(tenant))
		require.True(t, ok, "expected label cache entry for %s", tenant)
		require.Equal(t, int64(1), val.Counter.Load(), "expected ref count 1 for %s", tenant)
	}

	// Verify evicted child is not exported when value is zero
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, "HighCardinalityCounter_pre_eviction.txt"))

	// Manually set old timestamps to trigger eviction
	now := time.Now().Unix()
	oldTime := now - int64(metric.LabelValueTTL.Seconds()) - 10

	c.mu.Lock()
	c.mu.children.ForEach(func(cm ChildMetric) {
		child := cm.(*HighCardinalityChildCounter)
		if child != c.evictedChild {
			child.updatedAt.Store(oldTime)
		}
	})
	c.mu.Unlock()

	// Trigger eviction
	c.EvictStaleMetrics()

	// Verify evicted child has accumulated values
	expectedEvictedValue := int64(60) // 10 + 20 + 30
	require.Equal(t, expectedEvictedValue, c.evictedChild.Value())

	// Verify evicted children are removed from both the children storage
	// and the label cache.
	for _, tenant := range []string{"tenant1", "tenant2", "tenant3"} {
		_, ok := c.mu.children.Get(tenant)
		require.False(t, ok, "expected child %s to be removed from children storage after eviction", tenant)
		_, ok = labelCache.Get(cacheKey(tenant))
		require.False(t, ok, "expected label cache entry for %s to be removed after eviction", tenant)
	}

	// Verify only the _evicted child remains in children storage.
	require.Equal(t, 1, c.mu.children.Len())
	val, ok := labelCache.Get(cacheKey("_evicted"))
	require.True(t, ok, "expected label cache entry for _evicted to remain")
	require.Equal(t, int64(1), val.Counter.Load())

	// Add a new child after eviction
	c.Inc(5, "tenant4")

	// Verify new child's label cache entry exists.
	val, ok = labelCache.Get(cacheKey("tenant4"))
	require.True(t, ok, "expected label cache entry for tenant4")
	require.Equal(t, int64(1), val.Counter.Load())

	// Verify new children coexist with the evicted bucket
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, "HighCardinalityCounter_post_eviction.txt"))
}
