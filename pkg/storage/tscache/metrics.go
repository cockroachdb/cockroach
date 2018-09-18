// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tscache

import (
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

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
	Latency       *metric.Histogram
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
	metaSklReadLatency = metric.Metadata{
		Name:        "tscache.skl.read.latency",
		Help:        "Latency reading from the read timestamp cache",
		Measurement: "Latency Histogram",
		Unit:        metric.Unit_NANOSECONDS,
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
	metaSklWriteLatency = metric.Metadata{
		Name:        "tscache.skl.write.latency",
		Help:        "Latency writing to timestamp cache",
		Measurement: "Latency Histogram",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

func makeMetrics() Metrics {
	return Metrics{
		Skl: sklImplMetrics{
			Read: sklMetrics{
				Pages:         metric.NewGauge(metaSklReadPages),
				PageRotations: metric.NewCounter(metaSklReadRotations),
				Latency:       metric.NewLatency(metaSklReadLatency, metric.TestSampleInterval),
			},
			Write: sklMetrics{
				Pages:         metric.NewGauge(metaSklWritePages),
				PageRotations: metric.NewCounter(metaSklWriteRotations),
				Latency:       metric.NewLatency(metaSklWriteLatency, metric.TestSampleInterval),
			},
		},
	}
}
