// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metric

import "testing"

func TestPrometheusExporter(t *testing.T) {
	r1, r2 := NewRegistry(), NewRegistry()
	// r2 has a registry-level label, r1 does not.
	r2.AddLabel("registry", "two")

	r1.AddMetric(NewGauge(Metadata{Name: "one.gauge"}))
	r2.AddMetric(NewGauge(Metadata{Name: "two.gauge"}))

	c1Meta := Metadata{Name: "shared.counter"}
	c1Meta.AddLabel("counter", "one")
	c2Meta := Metadata{Name: "shared.counter"}
	c2Meta.AddLabel("counter", "two")

	r1.AddMetric(NewCounter(c1Meta))
	r2.AddMetric(NewCounter(c2Meta))

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
		"two_gauge": {[]metricLabels{
			{"registry": "two"},
		}},
		"shared_counter": {[]metricLabels{
			{"counter": "one"},
			{"counter": "two", "registry": "two"},
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
	// Check families returned by Gather are empty, right after calling clearMetrics
	// before another call to scrape.
	families, err = pe.Gather()
	if err != nil {
		t.Errorf("unexpected error from Gather(): %v", err)
	}
	for _, fam := range families {
		if num := len(fam.Metric); num != 0 {
			t.Errorf("gathered %s has %d data points but expect none", fam.GetName(), num)
		}
	}
}
