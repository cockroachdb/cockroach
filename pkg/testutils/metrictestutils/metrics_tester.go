// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metrictestutils

import (
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/errors"
	prometheus "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// MetricsTester scrapes metrics sources.
type MetricsTester struct {
	t            *testing.T
	sources      []func() string
	sourceLabels []func(int) *prometheus.LabelPair
}

// MetricFamilyTester focuses on a single metric name matching any given labels.
type MetricFamilyTester struct {
	t             *testing.T
	parent        *MetricsTester
	name          string
	kind          prometheus.MetricType
	value         func(*prometheus.Metric) float64
	labelMatchers map[string]func() (string, error)
}

// MetricValueTester aggregates the matched metric values.
type MetricValueTester struct {
	t         *testing.T
	parent    *MetricFamilyTester
	aggregate func([]*prometheus.Metric) (*prometheus.Metric, error)
}

// NewMetricsTester builds a MetricsTester.
func NewMetricsTester(t *testing.T, sources ...func() string) *MetricsTester {
	return &MetricsTester{
		t:       t,
		sources: sources,
	}
}

// LabelingSources specifies an additional prometheus.LabelPair to be added to
// all the metrics from a source. We can use this when scraping multiple
// sources to differentiate their metrics.
func (a *MetricsTester) LabelingSources(
	name string, valuef func(sourceIdx int) string,
) *MetricsTester {
	a.sourceLabels = append(a.sourceLabels, func(sourceIdx int) *prometheus.LabelPair {
		value := valuef(sourceIdx)
		return &prometheus.LabelPair{
			Name:  &name,
			Value: &value,
		}
	})
	return a
}

// Gauge selects the named gauge for testing.
func (a *MetricsTester) Gauge(name string) *MetricFamilyTester {
	return &MetricFamilyTester{
		t:      a.t,
		parent: a,
		name:   name,
		kind:   prometheus.MetricType_GAUGE,
		value: func(m *prometheus.Metric) float64 {
			return m.GetGauge().GetValue()
		},
		labelMatchers: make(map[string]func() (string, error)),
	}
}

func (a *MetricsTester) metricFamily(name string) (*prometheus.MetricFamily, error) {
	var parser expfmt.TextParser
	result := prometheus.MetricFamily{
		Name: &name,
	}
	for i, source := range a.sources {
		sourceFamilies, err := parser.TextToMetricFamilies(strings.NewReader(source()))
		if err != nil {
			return nil, err
		}
		sourceFamily, ok := sourceFamilies[name]
		if !ok {
			return nil, errors.Errorf("metric %s not found in source %d: %v", name, i, sourceFamilies)
		}
		result.Type = sourceFamily.Type
		for _, m := range sourceFamily.GetMetric() {
			for _, sourceLabel := range a.sourceLabels {
				m.Label = append(m.Label, sourceLabel(i))
			}
			result.Metric = append(result.Metric, m)
		}
	}
	return &result, nil
}

// WithLabel selects only metrics with the given label.
func (a *MetricFamilyTester) WithLabel(name, value string) *MetricFamilyTester {
	a.labelMatchers[name] = func() (string, error) {
		return value, nil
	}
	return a
}

// WithLabelf selects only metrics with the given label, lazily calculating the matching value.
func (a *MetricFamilyTester) WithLabelf(
	name string, f func(arg string) (string, error), arg string,
) *MetricFamilyTester {
	a.labelMatchers[name] = func() (string, error) {
		return f(arg)
	}
	return a
}

// Max selects the metric with the largest value in the set.
func (a *MetricFamilyTester) Max() *MetricValueTester {
	return &MetricValueTester{
		t:      a.t,
		parent: a,
		aggregate: func(metrics []*prometheus.Metric) (*prometheus.Metric, error) {
			if len(metrics) == 0 {
				return nil, errors.Errorf("no metrics selected")
			}
			max := metrics[0]
			for _, m := range metrics {
				if a.value(m) > a.value(max) {
					max = m
				}
			}
			return max, nil
		},
	}
}

// Single asserts there is only one metric in the set and selects it.
func (a *MetricFamilyTester) Single() *MetricValueTester {
	return &MetricValueTester{
		t:      a.t,
		parent: a,
		aggregate: func(m []*prometheus.Metric) (*prometheus.Metric, error) {
			if len(m) != 1 {
				return nil, errors.Errorf("not a single metric: %v", m)
			}
			return m[0], nil
		},
	}
}

func (a *MetricFamilyTester) metrics() ([]*prometheus.Metric, error) {
	family, err := a.parent.metricFamily(a.name)
	if err != nil {
		return nil, err
	}
	if family.GetType() != a.kind {
		return nil, errors.Errorf("metric %s is not a %s %v", a.name, a.kind, family)
	}
	all := family.GetMetric()
	var matched []*prometheus.Metric
nextMetric:
	for _, m := range all {
		labels := make(map[string]string, len(m.GetLabel()))
		for _, label := range m.GetLabel() {
			labels[label.GetName()] = label.GetValue()
		}
		for name, valuef := range a.labelMatchers {
			expected, err := valuef()
			if err != nil {
				a.t.Logf("error calculating expected value for label %s: %v", name, err)
			}
			if actual, ok := labels[name]; !ok || expected != actual {
				continue nextMetric
			}
		}
		matched = append(matched, m)
	}
	return matched, nil
}

// Label accesses the value of the named label for this metric.
func (a *MetricValueTester) Label(name string) (string, error) {
	metrics, err := a.parent.metrics()
	if err != nil {
		return "", err
	}
	m, err := a.aggregate(metrics)
	if err != nil {
		return "", err
	}
	for _, label := range m.GetLabel() {
		if label.GetName() == name {
			return label.GetValue(), nil
		}
	}
	return "", errors.Errorf("no such label %s", name)
}

// ShouldEventuallyEqual asserts on the aggregated value of this metric, retrying for up to 1 second.
func (a *MetricValueTester) ShouldEventuallyEqual(expected float64) {
	testutils.SucceedsWithin(a.t, func() error {
		metrics, err := a.parent.metrics()
		if err != nil {
			return err
		}
		m, err := a.aggregate(metrics)
		if err != nil {
			return err
		}
		value := a.parent.value(m)
		if value != expected {
			return errors.Errorf("expected %f but was %f aggregating %v", expected, value, metrics)
		}
		return nil
	}, 1*time.Second)
}
