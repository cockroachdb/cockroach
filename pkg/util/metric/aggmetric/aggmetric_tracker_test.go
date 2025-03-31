// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aggmetric

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestAggMetricTrackerForGaugeAndCounter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := metric.NewRegistry()
	st := cluster.MakeTestingClusterSettings()
	sv := &st.SV
	metricTracker := GetMetricTracker()

	c1 := NewCounterWithCacheStorageType(metric.Metadata{
		Name: "counter_1"})
	r.AddMetric(c1)
	metricTracker.RegisterWithMetric(c1.GetName(), &c1.childSet)

	c2 := NewCounterWithCacheStorageType(metric.Metadata{
		Name: "counter_2"}, "tenant_id")
	r.AddMetric(c2)
	metricTracker.RegisterWithMetric(c2.GetName(), &c2.childSet)

	c3 := NewCounterFloat64WithCacheStorageType(metric.Metadata{
		Name: "counter_3"})
	r.AddMetric(c3)
	metricTracker.RegisterWithMetric(c3.GetName(), &c3.childSet)

	g1 := NewGaugeWithCacheStorageType(metric.Metadata{
		Name: "gauge_1"})
	r.AddMetric(g1)
	metricTracker.RegisterWithMetric(g1.GetName(), &g1.childSet)

	g2 := NewGaugeFloat64WithCacheStorageType(metric.Metadata{
		Name: "gauge_2"})
	r.AddMetric(g2)
	metricTracker.RegisterWithMetric(g2.GetName(), &g2.childSet)

	AppNameLabelEnabled.SetOnChange(sv, func(_ context.Context) {
		metricTracker.ReinitialiseMetrics(sv)
	})

	DBNameLabelEnabled.SetOnChange(sv, func(_ context.Context) {
		metricTracker.ReinitialiseMetrics(sv)
	})

	runTest := func(t *testing.T, dbNameMetricsEnabled, appNameMetricsEnabled bool) string {
		DBNameLabelEnabled.Override(context.Background(), sv, dbNameMetricsEnabled)
		AppNameLabelEnabled.Override(context.Background(), sv, appNameMetricsEnabled)

		c1.Inc(1, metricTracker.GetMetricLabelValues("test_db", "test_app")...)
		c2.Inc(1, metricTracker.GetMetricLabelValues("test_db", "test_app", "test_tenant")...)
		c3.Inc(1.5, metricTracker.GetMetricLabelValues("test_db", "test_app")...)
		g1.Update(1, metricTracker.GetMetricLabelValues("test_db", "test_app")...)
		g2.Update(1.5, metricTracker.GetMetricLabelValues("test_db", "test_app")...)

		writePrometheusMetrics := WritePrometheusMetricsFunc(r)
		return writePrometheusMetrics(t)
	}

	datadriven.RunTest(t, "testdata/aggmetric_tracker_counter_gauge.txt",
		func(t *testing.T, d *datadriven.TestData) string {
			arg, ok := d.Arg("appNameMetricsEnabled")
			require.True(t, ok)
			appNameMetricsEnabled := arg.FirstVal(t) == "true"

			arg, ok = d.Arg("dbNameMetricsEnabled")
			require.True(t, ok)
			dbNameMetricsEnabled := arg.FirstVal(t) == "true"

			return runTest(t, dbNameMetricsEnabled, appNameMetricsEnabled)
		})
}

func TestAggMetricTrackerForHistogram(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := metric.NewRegistry()
	st := cluster.MakeTestingClusterSettings()
	sv := &st.SV
	metricTracker := GetMetricTracker()

	h := NewHistogramWithCacheStorage(metric.HistogramOptions{
		Metadata: metric.Metadata{
			Name: "histo_gram",
		},
		Duration:     base.DefaultHistogramWindowInterval(),
		MaxVal:       100,
		SigFigs:      1,
		BucketConfig: metric.Count1KBuckets,
	})
	r.AddMetric(h)
	metricTracker.RegisterWithMetric(h.GetName(), &h.childSet)

	AppNameLabelEnabled.SetOnChange(sv, func(_ context.Context) {
		metricTracker.ReinitialiseMetrics(sv)
	})

	DBNameLabelEnabled.SetOnChange(sv, func(_ context.Context) {
		metricTracker.ReinitialiseMetrics(sv)
	})

	runTest := func(t *testing.T, dbNameMetricsEnabled, appNameMetricsEnabled bool) string {
		DBNameLabelEnabled.Override(context.Background(), sv, dbNameMetricsEnabled)
		AppNameLabelEnabled.Override(context.Background(), sv, appNameMetricsEnabled)

		h.RecordValue(1, metricTracker.GetMetricLabelValues("test_db", "test_app")...)

		writePrometheusMetrics := WritePrometheusMetricsFunc(r)
		return writePrometheusMetrics(t)
	}

	path := "testdata/aggmetric_tracker_histogram.txt"
	if metric.HdrEnabled() {
		path = "testdata/aggmetric_tracker_histogram_hdr.txt"
	}

	datadriven.RunTest(t, path,
		func(t *testing.T, d *datadriven.TestData) string {
			arg, ok := d.Arg("appNameMetricsEnabled")
			require.True(t, ok)
			appNameMetricsEnabled := arg.FirstVal(t) == "true"

			arg, ok = d.Arg("dbNameMetricsEnabled")
			require.True(t, ok)
			dbNameMetricsEnabled := arg.FirstVal(t) == "true"

			return runTest(t, dbNameMetricsEnabled, appNameMetricsEnabled)
		})
}
