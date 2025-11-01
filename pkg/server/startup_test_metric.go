// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// metaStartupTestLastUpdated records when the startup test metric was last set.
// We set this once during node startup to the current Unix timestamp.
var metaStartupTestLastUpdated = metric.Metadata{
	Name:        "server.startup-test.last-updated",
	Help:        "Unix timestamp of when the startup test metric was set during server startup",
	Measurement: "Timestamp",
	Unit:        metric.Unit_TIMESTAMP_SEC,
}

var metaStartupTestHistogram = metric.Metadata{
	Name:        "server.test.last-updated-histogram",
	Help:        "test histogram",
	Measurement: "Histogram",
	Unit:        metric.Unit_NANOSECONDS,
}

var metaStartupTestCounter = metric.Metadata{
	Name:        "server.test.last-updated-counter",
	Help:        "test counter",
	Measurement: "Counter",
	Unit:        metric.Unit_COUNT,
}

// StartupTestMetrics contains a very simple metric that gets populated during
// server startup. It mirrors other startup-populated metrics by recording a
// timestamp in a gauge.
type StartupTestMetrics struct {
	StartupLastUpdated *metric.Gauge
}

type StartupTestMetricsHistogram struct {
	StartupLastUpdatedHistogram metric.IHistogram
}

type StartupTestMetricsCounter struct {
	StartupLastUpdatedCounter *metric.Counter
}

func (StartupTestMetrics) MetricStruct() {}
func (StartupTestMetricsHistogram) MetricStruct() {}
func (StartupTestMetricsCounter) MetricStruct() {}

func MakeStartupTestMetrics() StartupTestMetrics {
	g := metric.NewGauge(metaStartupTestLastUpdated)
	g.Update(timeutil.Now().Unix())
	return StartupTestMetrics{StartupLastUpdated: g}
}

func MakeStartupTestMetricsHistogram() StartupTestMetricsHistogram {
	c := metric.NewHistogram(metric.HistogramOptions{
		Metadata: metaStartupTestHistogram,
		BucketConfig: metric.IOLatencyBuckets,
	})
	c.RecordValue(1)
	return StartupTestMetricsHistogram{StartupLastUpdatedHistogram: c}
}

func MakeStartupTestMetricsCounter() StartupTestMetricsCounter {
	c := metric.NewCounter(metaStartupTestCounter)
	c.Inc(1)
	return StartupTestMetricsCounter{StartupLastUpdatedCounter: c}
}
