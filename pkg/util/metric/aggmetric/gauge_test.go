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

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/crlib/testutils/require"
	"github.com/prometheus/common/expfmt"
)

func TestAggGaugeEviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const cacheSize = 10
	r := metric.NewRegistry()
	writePrometheusMetrics := writePrometheusMetricsFunc(r)

	g := NewGauge(metric.Metadata{
		Name: "foo_gauge",
	}, "tenant_id", "gauge_label")
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

	for i := 0; i < cacheSize; i++ {
		g.Update(1, "1", strconv.Itoa(i))
	}

	testFile := "aggGauge_pre_eviction.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))

	for i := 0 + cacheSize; i < cacheSize+5; i++ {
		g.Update(10, "2", strconv.Itoa(i))
	}

	testFile = "aggGauge_post_eviction.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))
}

func TestAggGaugeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r := metric.NewRegistry()
	writePrometheusMetrics := func(t *testing.T) string {
		var in bytes.Buffer
		ex := metric.MakePrometheusExporter()
		scrape := func(ex *metric.PrometheusExporter) {
			ex.ScrapeRegistry(r, true /* includeChildMetrics */, true)
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

	g := NewGauge(metric.Metadata{
		Name: "foo_gauge",
	}, "tenant_id")
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

	g.Update(10, "1")
	g.Update(10, "2")

	g.Inc(5, "1")
	g.Dec(2, "2")

	testFile := "aggGauge.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))
}

func TestAggGaugeFloat64Eviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const cacheSize = 10
	r := metric.NewRegistry()
	writePrometheusMetrics := writePrometheusMetricsFunc(r)

	g := NewGaugeFloat64(metric.Metadata{
		Name: "foo_gauge_float64",
	}, "tenant_id", "gauge_label")
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

	for i := 0; i < cacheSize; i++ {
		g.Update(1.5, "1", strconv.Itoa(i))
	}

	testFile := "aggGaugeFloat64_pre_eviction.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))

	for i := 0 + cacheSize; i < cacheSize+5; i++ {
		g.Update(10.5, "2", strconv.Itoa(i))
	}

	testFile = "aggGaugeFloat64_post_eviction.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))
}

func writePrometheusMetricsFunc(r *metric.Registry) func(t *testing.T) string {
	writePrometheusMetrics := func(t *testing.T) string {
		var in bytes.Buffer
		ex := metric.MakePrometheusExporter()
		scrape := func(ex *metric.PrometheusExporter) {
			ex.ScrapeRegistry(r, true /* includeChildMetrics */, true)
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
	return writePrometheusMetrics
}
