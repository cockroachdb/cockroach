// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aggmetric

import (
	"bufio"
	"bytes"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
)

func TestSQLHistogram(t *testing.T) {
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

	h := NewSQLHistogram(metric.HistogramOptions{
		Metadata: metric.Metadata{
			Name: "histo_gram",
		},
		Duration:     base.DefaultHistogramWindowInterval(),
		MaxVal:       100,
		SigFigs:      1,
		BucketConfig: metric.Percent100Buckets,
	})
	r.AddMetric(h)
	cacheStorage := cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > cacheSize
		},
	})
	h.mu.children = &UnorderedCacheWrapper{
		cache: cacheStorage,
	}
	h.mu.labelConfig = metric.LabelConfigAppAndDB

	for i := 0; i < cacheSize; i++ {
		h.RecordValue(1, "1", strconv.Itoa(i))
	}

	testFile := "SQLHistogram_pre_eviction.txt"
	if metric.HdrEnabled() {
		testFile = "SQLHistogram_pre_eviction_hdr.txt"
	}

	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))

	for i := 0 + cacheSize; i < cacheSize+5; i++ {
		h.RecordValue(10, "2", strconv.Itoa(i))
	}

	testFile = "SQLHistogram_post_eviction.txt"
	if metric.HdrEnabled() {
		testFile = "SQLHistogram_post_eviction_hdr.txt"
	}
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))
}

func TestHighCardinalityHistogramEvictedChild(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	// Create a high cardinality histogram with max label values lower than
	// the number of children we'll add, so eviction can be triggered.
	// Unlike counter/gauge, histogram has no evicted child occupying a slot.
	h := NewHighCardinalityHistogram(
		metric.HistogramOptions{
			Metadata:     metric.Metadata{Name: "test_histogram"},
			Duration:     base.DefaultHistogramWindowInterval(),
			MaxVal:       100,
			SigFigs:      1,
			BucketConfig: metric.Percent100Buckets,
			HighCardinalityOpts: metric.HighCardinalityMetricOptions{
				MaxLabelValues: 2,
			},
		},
		"tenant_id",
	)
	r.AddMetric(h)

	// Initialize the metric
	labelCache := metric.NewLabelSliceCache()
	h.InitializeMetrics(labelCache)

	// Add some children
	h.RecordValue(1, "tenant1")
	h.RecordValue(5, "tenant2")
	h.RecordValue(10, "tenant3")

	// Helper to compute label cache keys.
	cacheKey := func(labelVals ...string) metric.LabelSliceCacheKey {
		return metric.LabelSliceCacheKey(metricKey(labelVals...))
	}

	// Verify label cache references exist for all children pre-eviction.
	for _, tenant := range []string{"tenant1", "tenant2", "tenant3"} {
		val, ok := labelCache.Get(cacheKey(tenant))
		require.True(t, ok, "expected label cache entry for %s", tenant)
		require.Equal(t, int64(1), val.Counter.Load(), "expected ref count 1 for %s", tenant)
	}

	testFile := "HighCardinalityHistogram_pre_eviction.txt"
	if metric.HdrEnabled() {
		testFile = "HighCardinalityHistogram_pre_eviction_hdr.txt"
	}
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))

	// Manually set old timestamps to trigger eviction
	now := time.Now().Unix()
	oldTime := now - int64(metric.LabelValueTTL.Seconds()) - 10

	h.mu.Lock()
	h.mu.children.ForEach(func(cm ChildMetric) {
		child := cm.(*HighCardinalityChildHistogram)
		child.updatedAt.Store(oldTime)
	})
	h.mu.Unlock()

	// Trigger eviction
	h.EvictStaleMetrics()

	// Verify evicted children are removed from both the children storage
	// and the label cache.
	for _, tenant := range []string{"tenant1", "tenant2", "tenant3"} {
		_, ok := h.mu.children.Get(tenant)
		require.False(t, ok, "expected child %s to be removed from children storage after eviction", tenant)
		_, ok = labelCache.Get(cacheKey(tenant))
		require.False(t, ok, "expected label cache entry for %s to be removed after eviction", tenant)
	}

	// Verify no children remain in children storage.
	require.Equal(t, 0, h.mu.children.Len())

	// Add a new child after eviction
	h.RecordValue(20, "tenant4")

	// Verify new child's label cache entry exists.
	val, ok := labelCache.Get(cacheKey("tenant4"))
	require.True(t, ok, "expected label cache entry for tenant4")
	require.Equal(t, int64(1), val.Counter.Load())

	// Verify new child coexists with aggregate (no evicted bucket for histograms)
	testFile = "HighCardinalityHistogram_post_eviction.txt"
	if metric.HdrEnabled() {
		testFile = "HighCardinalityHistogram_post_eviction_hdr.txt"
	}
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))
}
