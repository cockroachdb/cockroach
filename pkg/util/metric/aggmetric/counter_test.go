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
	"github.com/stretchr/testify/assert"
)

func TestAggCounter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const cacheSize = 10
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

	c := NewCounter(metric.Metadata{
		Name: "foo_counter",
	}, "tenant_id", "counter_label")
	r.AddMetric(c)
	c.initWithCacheStorageType([]string{"tenant_id", "counter_label"})
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

	testFile := "aggCounter_pre_eviction.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))

	for i := 0 + cacheSize; i < cacheSize+5; i++ {
		c.Inc(1, "2", strconv.Itoa(i))
	}

	testFile = "aggCounter_post_eviction.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))
}

func TestPanicForAggCounterWithBtreeStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r := metric.NewRegistry()
	c := NewCounter(metric.Metadata{
		Name: "foo_counter",
	}, "tenant_id", "counter_label")
	r.AddMetric(c)

	assert.Panics(t, func() {
		c.Inc(1, "1", "1")
	}, "expected panic when Inc is invoked on AggCounter with BTreeStorage")
}
