// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
)

func TestPrometheusExporter(t *testing.T) {
	r1, r2 := NewRegistry(), NewRegistry()
	// r2 has a registry-level label, r1 does not.
	r2.AddLabel("registry", "two")

	r1.AddMetric(NewGauge(Metadata{Name: "one.gauge"}))
	g1Dup := NewGauge(Metadata{Name: "one.gauge_dup"})
	r1.AddMetric(g1Dup)
	r2.AddMetric(NewGauge(Metadata{Name: "two.gauge"}))

	c1Meta := Metadata{Name: "shared.counter"}
	c1Meta.AddLabel("counter", "one")
	c2Meta := Metadata{Name: "shared.counter"}
	c2Meta.AddLabel("counter", "two")
	c2MetaDup := Metadata{Name: "shared.counter_dup"}
	c2MetaDup.AddLabel("counter", "two")

	r1.AddMetric(NewCounter(c1Meta))
	r2.AddMetric(NewCounter(c2Meta))
	c2Dup := NewCounter(c2MetaDup)
	r2.AddMetric(c2Dup)

	multilineHelp := `This is a multiline
      help message. With a second 
      sentence.`
	r1.AddMetric(NewGauge(Metadata{
		Name: "help.multiline",
		Help: multilineHelp,
	}))

	pe := MakePrometheusExporter()
	const includeChildMetrics = false
	pe.ScrapeRegistry(r1, includeChildMetrics)
	pe.ScrapeRegistry(r2, includeChildMetrics)

	type metricLabels map[string]string
	type family struct {
		// List of metric. The order depends on the order of "AddMetricsFromRegistry".
		// Each entry is a list of labels for the metric at that index,  "registry labels" followed by
		// "metric labels" in the order they were added.
		metrics []metricLabels
	}

	expected := map[string]family{
		"one_gauge": {[]metricLabels{
			{},
		}},
		"one_gauge_dup": {[]metricLabels{
			{},
		}},
		"two_gauge": {[]metricLabels{
			{"registry": "two"},
		}},
		"shared_counter": {[]metricLabels{
			{"counter": "one"},
			{"counter": "two", "registry": "two"},
		}},
		"shared_counter_dup": {[]metricLabels{
			{"counter": "two", "registry": "two"},
		}},
		"help_multiline": {[]metricLabels{
			{},
		}},
	}

	if lenExpected, lenExporter := len(expected), len(pe.families); lenExpected != lenExporter {
		t.Errorf("wrong number of families, expected %d, got %d", lenExpected, lenExporter)
	}

	for name, fam := range expected {
		fam2, ok := pe.families[name]
		if !ok {
			t.Errorf("exporter does not have metric family named %s", name)
		}
		if lenExpected, lenExporter := len(fam.metrics), len(fam2.GetMetric()); lenExpected != lenExporter {
			t.Errorf("wrong number of metrics for family %s, expected %d, got %d", name, lenExpected, lenExporter)
		}
		for i, met := range fam2.GetMetric() {
			expectedLabels := fam.metrics[i]
			if lenExpected, lenExporter := len(expectedLabels), len(met.Label); lenExpected != lenExporter {
				t.Errorf("wrong number of labels for metric %d in family %s, expected %d, got %d",
					i, name, lenExpected, lenExporter)
			}
			for _, l := range met.Label {
				if val, ok := expectedLabels[l.GetName()]; !ok {
					t.Errorf("unexpected label name %s for metric %d in family %s", l.GetName(), i, name)
				} else if val != l.GetValue() {
					t.Errorf("label %s for metric %d in family %s has value %s, expected %s",
						l.GetName(), i, name, l.GetValue(), val)
				}
			}
		}
	}

	// Test Gather
	families, err := pe.Gather()
	if err != nil {
		t.Errorf("unexpected error from Gather(): %v", err)
	}
	for _, fam := range families {
		if len(fam.Metric) == 0 {
			t.Errorf("gathered %s has no data points", fam.GetName())
		}
	}

	// Test clearMetrics
	pe.clearMetrics()
	for _, fam := range pe.families {
		if numPoints := len(fam.Metric); numPoints != 0 {
			t.Errorf("%s has %d data points, want 0", fam.GetName(), numPoints)
		}
	}
	// Check families returned by Gather are empty, right after calling
	// clearMetrics before another call to scrape.
	families, err = pe.Gather()
	if err != nil {
		t.Errorf("unexpected error from Gather(): %v", err)
	}
	for _, fam := range families {
		if num := len(fam.Metric); num != 0 {
			t.Errorf("gathered %s has %d data points but expect none", fam.GetName(), num)
		}
	}
	// Remove a metric, followed by a call to scrape and Gather. Results should
	// only include metrics with data points.
	r2.RemoveMetric(c2Dup)
	pe.ScrapeRegistry(r1, includeChildMetrics)
	pe.ScrapeRegistry(r2, includeChildMetrics)
	families, err = pe.Gather()
	if err != nil {
		t.Errorf("unexpected error from Gather(): %v", err)
	}
	for _, fam := range families {
		if len(fam.Metric) == 0 {
			t.Errorf("gathered %s has no data points", fam.GetName())
		}
	}

	// Test ScrapeAndPrintAsText
	var buf bytes.Buffer
	pe = MakePrometheusExporter()
	err = pe.ScrapeAndPrintAsText(&buf, expfmt.FmtText, func(exporter *PrometheusExporter) {
		exporter.ScrapeRegistry(r1, true)
	})
	require.NoError(t, err)
	output := buf.String()
	require.Regexp(t, "one_gauge 0", output)
	require.Regexp(t, "one_gauge_dup 0", output)
	require.Regexp(t, "shared_counter{counter=\"one\"}", output)

	require.Regexp(t, "This is a multiline help message", output)
	require.NotRegexp(t, multilineHelp, output)

	require.Len(t, strings.Split(output, "\n"), 13)

	buf.Reset()
	r1.RemoveMetric(g1Dup)
	err = pe.ScrapeAndPrintAsText(&buf, expfmt.FmtText, func(exporter *PrometheusExporter) {
		exporter.ScrapeRegistry(r1, true)
	})
	require.NoError(t, err)
	output = buf.String()
	require.Regexp(t, "one_gauge 0", output)
	require.NotRegexp(t, "one_gauge_dup 0", output)
	require.Len(t, strings.Split(output, "\n"), 10)
}

func TestPrometheusExporterNativeHistogram(t *testing.T) {
	defer func(enabled bool) {
		nativeHistogramsEnabled = enabled
	}(nativeHistogramsEnabled)
	nativeHistogramsEnabled = true
	r := NewRegistry()

	histogram := NewHistogram(HistogramOptions{
		Duration: time.Second,
		Mode:     HistogramModePrometheus,
		Metadata: Metadata{
			Name: "histogram",
		},
		BucketConfig: staticBucketConfig{
			distribution: Exponential,
			min:          10e3, // 10µs
			max:          10e9, // 10s
			count:        60,
		},
	})
	r.AddMetric(histogram)

	var buf bytes.Buffer
	pe := MakePrometheusExporter()
	// Print metrics as proto text, since native histograms aren't yet supported.
	// in the prometheus text exposition format.
	err := pe.ScrapeAndPrintAsText(&buf, expfmt.FmtProtoText, func(exporter *PrometheusExporter) {
		exporter.ScrapeRegistry(r, false)
	})
	require.NoError(t, err)
	output := buf.String()
	// Assert that output contains the native histogram schema.
	require.Regexp(t, "schema: 3", output)

	buf.Reset()
	r.RemoveMetric(histogram)
	err = pe.ScrapeAndPrintAsText(&buf, expfmt.FmtProtoText, func(exporter *PrometheusExporter) {
		exporter.ScrapeRegistry(r, false)
	})
	require.NoError(t, err)
	output = buf.String()
	require.Empty(t, output)
}
