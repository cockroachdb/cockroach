// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tscache

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// Metrics holds all metrics relating to a Cache.
type Metrics struct {
	Skl sklMetrics
}

// sklMetrics holds all metrics relating to an intervalSkl.
type sklMetrics struct {
	Pages         *metric.Gauge
	PageRotations *metric.Counter
}

// MetricStruct implements the metrics.Struct interface.
func (sklMetrics) MetricStruct() {}

var _ metric.Struct = sklMetrics{}

var (
	metaSklPages = metric.Metadata{
		Name:        "tscache.skl.pages",
		Help:        "Number of pages in the timestamp cache",
		Measurement: "Pages",
		Unit:        metric.Unit_COUNT,
	}
	metaSklRotations = metric.Metadata{
		Name:        "tscache.skl.rotations",
		Help:        "Number of page rotations in the timestamp cache",
		Measurement: "Page Rotations",
		Unit:        metric.Unit_COUNT,
	}
)

func makeMetrics() Metrics {
	return Metrics{
		Skl: sklMetrics{
			Pages:         metric.NewGauge(metaSklPages),
			PageRotations: metric.NewCounter(metaSklRotations),
		},
	}
}
