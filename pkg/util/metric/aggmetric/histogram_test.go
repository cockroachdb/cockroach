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

func TestHighCardinalityHistogram(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const cacheSize = 10
	r := metric.NewRegistry()
	writePrometheusMetrics := WritePrometheusMetricsFunc(r)

	h := NewHighCardinalityHistogram(metric.HistogramOptions{
		Metadata: metric.Metadata{
			Name: "histo_gram",
		},
		Duration:            base.DefaultHistogramWindowInterval(),
		MaxVal:              100,
		SigFigs:             1,
		BucketConfig:        metric.Percent100Buckets,
		HighCardinalityOpts: metric.HighCardinalityMetricOptions{},
	}, "database", "application_name")

	h.mu.children = &UnorderedCacheWrapper{
		cache: initialiseCacheStorageForTesting(),
	}
	r.AddMetric(h)

	// Initialize with a label slice cache to test eviction
	labelSliceCache := metric.NewLabelSliceCache()
	h.InitializeMetrics(labelSliceCache)

	for i := 0; i < cacheSize+5; i++ {
		h.RecordValue(int64(i+1), "1", strconv.Itoa(i))
	}

	// Wait more than cache eviction time to make sure that keys are not evicted based on only cache size.
	time.Sleep(6 * time.Second)

	testFile := "HighCardinalityHistogram_pre_eviction.txt"
	if metric.HdrEnabled() {
		testFile = "HighCardinalityHistogram_pre_eviction_hdr.txt"
	}

	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))

	for i := 0; i < cacheSize+5; i++ {
		metricKey := metric.LabelSliceCacheKey(metricKey([]string{"1", strconv.Itoa(i)}))
		labelSliceValue, ok := labelSliceCache.Get(metricKey)
		require.True(t, ok, "missing labelSliceValue in label slice cache")
		require.Equal(t, int64(1), labelSliceValue.Counter.Load(), "the value should be 1")
		require.Equal(t, []string{"1", strconv.Itoa(i)}, labelSliceValue.LabelValues, "label values are mismatching")

	}

	for i := 0 + cacheSize; i < cacheSize+5; i++ {
		h.RecordValue(int64(i+10), "2", strconv.Itoa(i))
	}

	testFile = "HighCardinalityHistogram_post_eviction.txt"
	if metric.HdrEnabled() {
		testFile = "HighCardinalityHistogram_post_eviction_hdr.txt"
	}
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
		require.Equal(t, int64(1), labelSliceValue.Counter.Load(), "the value should be 1")
		require.Equal(t, []string{"1", strconv.Itoa(i)}, labelSliceValue.LabelValues, "label values are mismatching")
	}

	for i := 10; i < 15; i++ {
		metricKey := metric.LabelSliceCacheKey(metricKey([]string{"2", strconv.Itoa(i)}))
		labelSliceValue, ok := labelSliceCache.Get(metricKey)
		require.True(t, ok, "missing labelSliceValue in label slice cache")
		require.Equal(t, int64(1), labelSliceValue.Counter.Load(), "the value should be 1")
		require.Equal(t, []string{"2", strconv.Itoa(i)}, labelSliceValue.LabelValues, "label values are mismatching")
	}
}
