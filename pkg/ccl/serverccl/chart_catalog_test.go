// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package serverccl_test

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/ts/catalog"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// TestChartCatalogMetric ensures that all metrics are included in at least one
// chart, and that every metric included in a chart is still part of the metrics
// registry.
//
// This test lives in CCL code so that it can pick up the full set of metrics,
// including those registered from CCL code.
func TestChartCatalogMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer s.Stopper().Stop(context.Background())

	metricsMetadata := s.Server(0).MetricsRecorder().GetMetricsMetadata()

	chartCatalog, err := catalog.GenerateCatalog(metricsMetadata, true /* strict */)

	if err != nil {
		t.Fatal(err)
	}

	// Each metric in metricsMetadata should have at least one entry in
	// chartCatalog, which we track by deleting the metric from metricsMetadata.
	for _, v := range chartCatalog {
		deleteSeenMetrics(&v, metricsMetadata, t)
	}

	if len(metricsMetadata) > 0 {
		var metricNames []string
		for metricName := range metricsMetadata {
			metricNames = append(metricNames, metricName)
		}
		sort.Strings(metricNames)
		t.Errorf(`The following metrics need to be added to the chart catalog
		    (pkg/ts/catalog/chart_catalog.go): %v`, metricNames)
	}

	internalTSDBMetricNamesWithoutPrefix := map[string]struct{}{}
	for _, name := range catalog.AllInternalTimeseriesMetricNames() {
		name = strings.TrimPrefix(name, "cr.node.")
		name = strings.TrimPrefix(name, "cr.store.")
		internalTSDBMetricNamesWithoutPrefix[name] = struct{}{}
	}
	walkAllSections(chartCatalog, func(cs *catalog.ChartSection) {
		for _, chart := range cs.Charts {
			for _, metric := range chart.Metrics {
				if *metric.MetricType.Enum() != io_prometheus_client.MetricType_HISTOGRAM {
					continue
				}
				// We have a histogram. Make sure that it is properly represented in
				// AllInternalTimeseriesMetricNames(). It's not a complete check but good enough in
				// practice. Ideally we wouldn't require `histogramMetricsNames` and
				// the associated manual step when adding a histogram. See:
				// https://github.com/cockroachdb/cockroach/issues/64373
				_, ok := internalTSDBMetricNamesWithoutPrefix[metric.Name+"-p50"]
				if !ok {
					t.Errorf("histogram %s needs to be added to `catalog.histogramMetricsNames` manually",
						metric.Name)
				}
			}
		}
	})
}

// TestChartCatalog ensures that the server successfully generates the chart catalog.
func TestChartCatalogGen(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer s.Stopper().Stop(context.Background())

	metricsMetadata := s.Server(0).MetricsRecorder().GetMetricsMetadata()
	// todo: extend test to pass false and true to strict mode
	// NB: strict mode verifies that all metrics mentioned in the catalog
	// exist.
	chartCatalog, err := catalog.GenerateCatalog(metricsMetadata, true /* strict */)

	if err != nil {
		t.Fatal(err)
	}

	// Ensure each of the 9 constant sections of the chart catalog exist.
	if len(chartCatalog) != 9 {
		t.Fatal("Chart catalog failed to generate.")
	}

	for _, section := range chartCatalog {
		// Ensure that one of the chartSections has defined Subsections.
		if len(section.Subsections) == 0 {
			t.Fatalf(`Chart catalog has missing subsections in %v`, section)
		}
	}
}

// deleteSeenMetrics removes all metrics in a section from the metricMetadata map.
func deleteSeenMetrics(c *catalog.ChartSection, metadata map[string]metric.Metadata, t *testing.T) {
	for _, x := range c.Charts {
		for _, metric := range x.Metrics {
			_, ok := metadata[metric.Name]
			if ok {
				delete(metadata, metric.Name)
			}
		}
	}

	for _, x := range c.Subsections {
		deleteSeenMetrics(x, metadata, t)
	}
}

// walkAllSections invokes the visitor on each of the ChartSections nestled under
// the input one.
func walkAllSections(chartCatalog []catalog.ChartSection, visit func(c *catalog.ChartSection)) {
	for _, c := range chartCatalog {
		visit(&c)
		for _, ic := range c.Subsections {
			visit(ic)
		}
	}
}
