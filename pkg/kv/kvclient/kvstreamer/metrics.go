// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstreamer

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	metaStreamerCount = metric.Metadata{
		Name:        "kv.streamer.operators_count",
		Help:        "Number of KV Streamer operators currently in use",
		Measurement: "Operators",
		Unit:        metric.Unit_COUNT,
	}
	metaStreamerRequestsInProgress = metric.Metadata{
		Name:        "kv.streamer.requests_in_progress",
		Help:        "Number of BatchRequests in progress across all KV Streamer operators",
		Measurement: "Batches",
		Unit:        metric.Unit_COUNT,
	}
	metaStreamerRequestsThrottled = metric.Metadata{
		Name:        "kv.streamer.requests_throttled",
		Help:        "Number of BatchRequests currently throttled due to reaching the concurrency limit, across all KV Streamer operators",
		Measurement: "Batches",
		Unit:        metric.Unit_COUNT,
	}
)

type Metrics struct {
	OperatorsCount     *metric.Gauge
	RequestsInProgress *metric.Gauge
	RequestsThrottled  *metric.Gauge
}

var _ metric.Struct = Metrics{}

func (Metrics) MetricStruct() {}

func MakeMetrics() Metrics {
	return Metrics{
		OperatorsCount:     metric.NewGauge(metaStreamerCount),
		RequestsInProgress: metric.NewGauge(metaStreamerRequestsInProgress),
		RequestsThrottled:  metric.NewGauge(metaStreamerRequestsThrottled),
	}
}
