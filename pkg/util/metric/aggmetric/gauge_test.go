// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aggmetric

import (
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/assert"
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
	g.labelConfig.Store(LabelConfigAppAndDB)

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
	g.labelConfig.Store(LabelConfigAppAndDB)

	g.Update(10, "1", "1")
	g.Update(10, "2", "2")

	g.Inc(5, "1", "1")
	g.Dec(2, "2", "2")

	testFile := "SQLGauge.txt"
	echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))
}

func TestPanicForAggGaugeWithBtreeStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	g := NewGauge(metric.Metadata{
		Name: "foo_gauge",
	}, "tenant_id")

	assert.Panics(t, func() {
		g.Inc(1, "1", "1")
	}, "expected panic when Inc is invoked on AggGauge with BTreeStorage")

	assert.Panics(t, func() {
		g.Dec(1, "1", "1")
	}, "expected panic when Dec is invoked on AggGauge with BTreeStorage")

	assert.Panics(t, func() {
		g.Update(1, "1", "1")
	}, "expected panic when Update is invoked on AggGauge with BTreeStorage")
}

func TestPanicForAggGaugeFloat64WithBtreeStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	g := NewGaugeFloat64(metric.Metadata{
		Name: "foo_gauge",
	}, "tenant_id")

	assert.Panics(t, func() {
		g.Update(1, "1", "1")
	}, "expected panic when Update is invoked on AggGaugeFloat64 with BTreeStorage")
}
