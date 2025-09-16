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
	
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
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

func TestBoundedCounter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const cacheSize = 10
	r := metric.NewRegistry()
	writePrometheusMetrics := WritePrometheusMetricsFunc(r)

	c := NewBoundedCounter(metric.Metadata{
		Name: "foo_counter",
	}, "database", "application_name")
	c.mu.children = &UnorderedCacheWrapper{
		cache: initialiseCacheStorageForTesting(),
	}

	r.AddMetric(c)

	for i := 0; i < cacheSize+5; i++ {
		c.Inc(1, "1", strconv.Itoa(i))
	}

	//wait more than cache eviction time to make sure that keys are not evicted based on only cache size.
	time.Sleep(6 * time.Second)

	testFile := "boundedCounter_pre_eviction.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))

	for i := 0 + cacheSize; i < cacheSize+5; i++ {
		c.Inc(1, "2", strconv.Itoa(i))
	}

	testFile = "boundedCounter_post_eviction.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))
}

func initialiseCacheStorageForTesting() *cache.UnorderedCache {
	return cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			childMetric, _ := value.(ChildMetric)

			// Check if the child metric has exceeded 20 seconds and cache size is greater than 5000
			if labelSliceCachedChildMetric, ok := childMetric.(LabelSliceCachedChildMetric); ok {
				currentTime := timeutil.Now()
				age := currentTime.Sub(labelSliceCachedChildMetric.CreatedAt())
				return size > 10 && age > 5*time.Second
			}
			return size > cacheSize
		},
		OnEvictedEntry: func(entry *cache.Entry) {
			if childMetric, ok := entry.Value.(LabelSliceCachedChildMetric); ok {
				childMetric.DecrementLabelSliceCacheReference()
			}
		},
	})
}
