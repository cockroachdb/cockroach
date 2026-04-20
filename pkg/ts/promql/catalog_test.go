// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package promql

import (
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestTsdbToPromName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "node metric", input: "cr.node.sql.conn.count", expected: "sql_conn_count"},
		{name: "store metric", input: "cr.store.livebytes", expected: "livebytes"},
		{name: "cluster metric", input: "cr.cluster.rebalancing", expected: "rebalancing"},
		{name: "multi-segment node", input: "cr.node.a.b.c", expected: "a_b_c"},
		{name: "unknown prefix not stripped", input: "unknown.prefix.metric", expected: "unknown_prefix_metric"},
		{name: "prefix only", input: "cr.node.", expected: ""},
		{name: "empty string", input: "", expected: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tsdbToPromName(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestInstanceType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "node metric", input: "cr.node.sql.conn.count", expected: "node"},
		{name: "store metric", input: "cr.store.livebytes", expected: "store"},
		{name: "cluster metric", input: "cr.cluster.rebalancing", expected: "cluster"},
		{name: "unknown prefix defaults to node", input: "cr.unknown.something", expected: "node"},
		{name: "empty string defaults to node", input: "", expected: "node"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, InstanceType(tc.input))
		})
	}
}

func TestSourceLabel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		instType string
		expected string
	}{
		{name: "store returns store_id", instType: "store", expected: storeIDLabel},
		{name: "node returns node_id", instType: "node", expected: nodeIDLabel},
		{name: "cluster returns node_id", instType: "cluster", expected: nodeIDLabel},
		{name: "empty returns node_id", instType: "", expected: nodeIDLabel},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, SourceLabel(tc.instType))
		})
	}
}

// testCatalog builds a MetricCatalog with a small known set of metrics.
func testCatalog() *MetricCatalog {
	tsdbNames := map[string]string{
		"sql.conn.count":       "cr.node.sql.conn.count",
		"sql.query.count":      "cr.node.sql.query.count",
		"livebytes":            "cr.store.livebytes",
		"rebalancing":          "cr.cluster.rebalancing",
		"sql.conn.latency":     "cr.node.sql.conn.latency",
		"sql.conn.latency-p99": "cr.node.sql.conn.latency-p99",
	}
	gaugeType := prometheusgo.MetricType_GAUGE
	counterType := prometheusgo.MetricType_COUNTER
	histType := prometheusgo.MetricType_HISTOGRAM
	allMetadata := map[string]metric.Metadata{
		"sql.conn.count":   {Help: "Number of SQL connections.", MetricType: gaugeType, Unit: metric.Unit_COUNT},
		"sql.query.count":  {Help: "Number of SQL queries.", MetricType: counterType, Unit: metric.Unit_COUNT},
		"livebytes":        {Help: "Number of live bytes.", MetricType: gaugeType, Unit: metric.Unit_BYTES},
		"rebalancing":      {Help: "Rebalancing status.", MetricType: gaugeType},
		"sql.conn.latency": {Help: "SQL connection latency.", MetricType: histType, Unit: metric.Unit_NANOSECONDS},
	}
	return NewMetricCatalog(tsdbNames, allMetadata)
}

func TestMetricCatalogLookup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := testCatalog()

	// Forward lookup: prom → tsdb.
	tsdbName, ok := c.TSDBName("sql_conn_count")
	require.True(t, ok)
	require.Equal(t, "cr.node.sql.conn.count", tsdbName)

	// Reverse lookup: tsdb → prom.
	promName, ok := c.PromName("cr.node.sql.conn.count")
	require.True(t, ok)
	require.Equal(t, "sql_conn_count", promName)

	// Nonexistent names.
	_, ok = c.TSDBName("nonexistent")
	require.False(t, ok)
	_, ok = c.PromName("cr.node.nonexistent")
	require.False(t, ok)
}

func TestMetricCatalogAllPromNames(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := testCatalog()

	names := c.AllPromNames()
	// Must be sorted.
	for i := 1; i < len(names); i++ {
		require.True(t, names[i-1] < names[i], "not sorted: %q >= %q", names[i-1], names[i])
	}

	// Must contain the known metrics.
	require.Contains(t, names, "sql_conn_count")
	require.Contains(t, names, "livebytes")
	require.Contains(t, names, "rebalancing")

	// Must be a defensive copy.
	names[0] = "MUTATED"
	fresh := c.AllPromNames()
	require.NotEqual(t, "MUTATED", fresh[0])
}

func TestMetricCatalogMatchPromNames(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := testCatalog()

	// Match prefix.
	matched := c.MatchPromNames(func(s string) bool {
		return strings.HasPrefix(s, "sql_")
	})
	for _, name := range matched {
		require.True(t, strings.HasPrefix(name, "sql_"), "unexpected: %s", name)
	}
	require.NotEmpty(t, matched)

	// Match nothing.
	matched = c.MatchPromNames(func(string) bool { return false })
	require.Empty(t, matched)
}

func TestMetricCatalogRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := testCatalog()

	// Verify initial state.
	_, ok := c.TSDBName("sql_conn_count")
	require.True(t, ok)

	// Refresh with a completely different set.
	c.Refresh(
		map[string]string{"new.metric": "cr.node.new.metric"},
		map[string]metric.Metadata{},
	)

	// Old name gone.
	_, ok = c.TSDBName("sql_conn_count")
	require.False(t, ok)

	// New name present.
	tsdbName, ok := c.TSDBName("new_metric")
	require.True(t, ok)
	require.Equal(t, "cr.node.new.metric", tsdbName)
}

func TestMetricCatalogAllMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := testCatalog()

	md := c.AllMetadata()

	// Check a gauge metric.
	infos, ok := md["sql_conn_count"]
	require.True(t, ok)
	require.Len(t, infos, 1)
	require.Equal(t, "gauge", infos[0].Type)
	require.Equal(t, "Number of SQL connections.", infos[0].Help)

	// Check a counter metric.
	infos = md["sql_query_count"]
	require.Len(t, infos, 1)
	require.Equal(t, "counter", infos[0].Type)

	// Check a metric with no metadata falls back to "gauge" type.
	for _, info := range md {
		require.NotEmpty(t, info[0].Type)
	}
}

func TestMetricCatalogHistogramSuffixStripping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := testCatalog()

	// "sql.conn.latency-p99" should resolve metadata from base "sql.conn.latency".
	// Note: tsdbToPromName only replaces dots with underscores, so the hyphen
	// in "-p99" is preserved: prom name is "sql_conn_latency-p99".
	md := c.AllMetadata()
	infos, ok := md["sql_conn_latency-p99"]
	require.True(t, ok)
	require.Len(t, infos, 1)
	require.Equal(t, "histogram", infos[0].Type)
	require.Equal(t, "SQL connection latency.", infos[0].Help)
}

func TestPromMetricType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		input    prometheusgo.MetricType
		expected string
	}{
		{name: "counter", input: prometheusgo.MetricType_COUNTER, expected: "counter"},
		{name: "gauge", input: prometheusgo.MetricType_GAUGE, expected: "gauge"},
		{name: "histogram", input: prometheusgo.MetricType_HISTOGRAM, expected: "histogram"},
		{name: "summary", input: prometheusgo.MetricType_SUMMARY, expected: "summary"},
		{name: "untyped defaults to gauge", input: prometheusgo.MetricType_UNTYPED, expected: "gauge"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, promMetricType(tc.input))
		})
	}
}

func TestMetricCatalogConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := testCatalog()

	const goroutines = 8
	const iterations = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				switch id % 4 {
				case 0:
					c.TSDBName("sql_conn_count")
				case 1:
					c.AllPromNames()
				case 2:
					c.MatchPromNames(func(s string) bool {
						return strings.HasPrefix(s, "sql_")
					})
				case 3:
					c.Refresh(
						map[string]string{
							"sql.conn.count": "cr.node.sql.conn.count",
							"livebytes":      "cr.store.livebytes",
						},
						map[string]metric.Metadata{},
					)
				}
			}
		}(i)
	}
	wg.Wait()
}
