// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tscache

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// Metrics holds all metrics relating to a Cache.
type Metrics struct {
	Skl sklImplMetrics
}

// sklImplMetrics holds all metrics relating to an sklImpl Cache implementation.
type sklImplMetrics struct {
	Read, Write sklMetrics
}

// sklMetrics holds all metrics relating to an intervalSkl.
type sklMetrics struct {
	Pages         *metric.Gauge
	PageRotations *metric.Counter
}

// MetricStruct implements the metrics.Struct interface.
func (sklImplMetrics) MetricStruct() {}
func (sklMetrics) MetricStruct()     {}

var _ metric.Struct = sklImplMetrics{}
var _ metric.Struct = sklMetrics{}

var (
	metaSklReadPages = metric.Metadata{
		Name:        "tscache.skl.read.pages",
		Help:        "Number of pages in the read timestamp cache",
		Measurement: "Pages",
		Unit:        metric.Unit_COUNT,
	}
	metaSklReadRotations = metric.Metadata{
		Name:        "tscache.skl.read.rotations",
		Help:        "Number of page rotations in the read timestamp cache",
		Measurement: "Page Rotations",
		Unit:        metric.Unit_COUNT,
	}
	metaSklWritePages = metric.Metadata{
		Name:        "tscache.skl.write.pages",
		Help:        "Number of pages in the write timestamp cache",
		Measurement: "Pages",
		Unit:        metric.Unit_COUNT,
	}
	metaSklWriteRotations = metric.Metadata{
		Name:        "tscache.skl.write.rotations",
		Help:        "Number of page rotations in the write timestamp cache",
		Measurement: "Page Rotations",
		Unit:        metric.Unit_COUNT,
	}
)

func makeMetrics() Metrics {
	return Metrics{
		Skl: sklImplMetrics{
			Read: sklMetrics{
				Pages:         metric.NewGauge(metaSklReadPages),
				PageRotations: metric.NewCounter(metaSklReadRotations),
			},
			Write: sklMetrics{
				Pages:         metric.NewGauge(metaSklWritePages),
				PageRotations: metric.NewCounter(metaSklWriteRotations),
			},
		},
	}
}
