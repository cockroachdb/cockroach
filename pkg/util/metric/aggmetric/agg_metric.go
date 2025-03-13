// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package aggmetric provides functionality to create metrics which expose
// aggregate metrics for internal collection and additionally per-child
// reporting to prometheus.
package aggmetric

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// Builder is used to ease constructing metrics with the same labels.
type Builder struct {
	labels []string
}

// MakeBuilder makes a new Builder.
func MakeBuilder(labels ...string) Builder {
	return Builder{labels: labels}
}

// Gauge constructs a new AggGauge with the Builder's labels.
func (b Builder) Gauge(metadata metric.Metadata) *AggGauge {
	return NewGauge(metadata, b.labels...)
}

// FunctionalGauge constructs a new AggGauge with the Builder's labels who's
// value is determined when asked for.
func (b Builder) FunctionalGauge(metadata metric.Metadata, f func(cvs []int64) int64) *AggGauge {
	return NewFunctionalGauge(metadata, f, b.labels...)
}

// GaugeFloat64 constructs a new AggGaugeFloat64 with the Builder's labels.
func (b Builder) GaugeFloat64(metadata metric.Metadata) *AggGaugeFloat64 {
	return NewGaugeFloat64(metadata, b.labels...)
}

// Counter constructs a new AggCounter with the Builder's labels.
func (b Builder) Counter(metadata metric.Metadata) *AggCounter {
	return NewCounter(metadata, b.labels...)
}

// CounterFloat64 constructs a new AggCounter with the Builder's labels.
func (b Builder) CounterFloat64(metadata metric.Metadata) *AggCounterFloat64 {
	return NewCounterFloat64(metadata, b.labels...)
}

// Histogram constructs a new AggHistogram with the Builder's labels.
func (b Builder) Histogram(opts metric.HistogramOptions) *AggHistogram {
	return NewHistogram(opts, b.labels...)
}

type childSet struct {
	labels []string
	mu     struct {
		syncutil.Mutex
		children *cache.UnorderedCache
	}
}

func (cs *childSet) init(labels []string) {
	cs.labels = labels
	cs.mu.children = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > 2000 // TODO: make this configurable
		},
	})
}

func (cs *childSet) Each(
	labels []*io_prometheus_client.LabelPair, f func(metric *io_prometheus_client.Metric),
) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.mu.children.Do(func(e *cache.Entry) {
		cm := e.Value.(childMetric)
		pm := cm.ToPrometheusMetric()
		childLabels := make([]*io_prometheus_client.LabelPair, 0, len(labels)+len(cs.labels))
		childLabels = append(childLabels, labels...)
		lvs := cm.labelValues()
		for i := range cs.labels {
			childLabels = append(childLabels, &io_prometheus_client.LabelPair{
				Name:  &cs.labels[i],
				Value: &lvs[i],
			})
		}
		pm.Label = childLabels
		f(pm)
	})
}

func (cs *childSet) add(metric childMetric) {
	lvs := metric.labelValues()
	if len(lvs) != len(cs.labels) {
		panic(errors.AssertionFailedf(
			"cannot add child with %d label values %v to a metric with %d labels %v",
			len(lvs), lvs, len(cs.labels), cs.labels))
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if _, ok := cs.mu.children.Get(metricKey(metric)); ok {
		panic(errors.AssertionFailedf("child %v already exists", metric.labelValues()))
	}
	cs.mu.children.Add(metricKey(metric), metric)
}

func (cs *childSet) remove(metric childMetric) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	key := metricKey(metric)
	if _, ok := cs.mu.children.Get(key); ok {
		cs.mu.children.Del(key)
		return
	}
	panic(errors.AssertionFailedf("child %v does not exists", metric.labelValues()))
}

type MetricItem interface {
	labelValuer
}

type childMetric interface {
	MetricItem
	ToPrometheusMetric() *io_prometheus_client.Metric
}

type labelValuer interface {
	labelValues() []string
}

type labelValuesSlice []string

func (lv *labelValuesSlice) labelValues() []string { return []string(*lv) }

func metricKey(metric childMetric) string {
	return strings.Join(metric.labelValues(), ",")
}

func (cs *childSet) apply(applyFn func(item MetricItem)) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.mu.children.Do(func(e *cache.Entry) {
		applyFn(e.Value.(MetricItem))
	})
}
