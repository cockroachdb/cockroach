// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstreamer

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	metaStreamerCount = metric.Metadata{
		Name:        "kv.streamer.operators.active",
		Help:        "Number of KV Streamer operators currently in use",
		Measurement: "Operators",
		Unit:        metric.Unit_COUNT,
	}
	metaBatchesSent = metric.Metadata{
		Name:        "kv.streamer.batches.sent",
		Help:        "Number of BatchRequests sent across all KV Streamer operators",
		Measurement: "Batches",
		Unit:        metric.Unit_COUNT,
	}
	metaBatchesInProgress = metric.Metadata{
		Name:        "kv.streamer.batches.in_progress",
		Help:        "Number of BatchRequests in progress across all KV Streamer operators",
		Measurement: "Batches",
		Unit:        metric.Unit_COUNT,
	}
	metaBatchesThrottled = metric.Metadata{
		Name:        "kv.streamer.batches.throttled",
		Help:        "Number of BatchRequests currently being throttled due to reaching the concurrency limit, across all KV Streamer operators",
		Measurement: "Batches",
		Unit:        metric.Unit_COUNT,
	}
)

type Metrics struct {
	OperatorsCount    *metric.Gauge
	BatchesSent       *metric.Counter
	BatchesInProgress *metric.Gauge
	BatchesThrottled  *metric.Gauge
}

var _ metric.Struct = Metrics{}

func (Metrics) MetricStruct() {}

func MakeMetrics() Metrics {
	return Metrics{
		OperatorsCount:    metric.NewGauge(metaStreamerCount),
		BatchesSent:       metric.NewCounter(metaBatchesSent),
		BatchesInProgress: metric.NewGauge(metaBatchesInProgress),
		BatchesThrottled:  metric.NewGauge(metaBatchesThrottled),
	}
}
