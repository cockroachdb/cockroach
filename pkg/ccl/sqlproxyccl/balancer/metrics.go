// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package balancer

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// Metrics contains pointers to the metrics for monitoring balancer-related
// operations.
type Metrics struct {
	rebalanceReqRunning *metric.Gauge
	rebalanceReqQueued  *metric.Gauge
	rebalanceReqTotal   *metric.Counter
}

// MetricStruct implements the metrics.Struct interface.
func (Metrics) MetricStruct() {}

var _ metric.Struct = Metrics{}

var (
	metaRebalanceReqRunning = metric.Metadata{
		Name:        "proxy.balancer.rebalance.running",
		Help:        "Number of rebalance requests currently running",
		Measurement: "Rebalance Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaRebalanceReqQueued = metric.Metadata{
		Name:        "proxy.balancer.rebalance.queued",
		Help:        "Number of rebalance requests currently queued",
		Measurement: "Rebalance Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaRebalanceReqTotal = metric.Metadata{
		Name:        "proxy.balancer.rebalance.total",
		Help:        "Number of rebalance requests that were processed",
		Measurement: "Rebalance Requests",
		Unit:        metric.Unit_COUNT,
	}
)

// NewMetrics instantiates the metrics holder for balancer monitoring.
func NewMetrics() *Metrics {
	return &Metrics{
		rebalanceReqRunning: metric.NewGauge(metaRebalanceReqRunning),
		rebalanceReqQueued:  metric.NewGauge(metaRebalanceReqQueued),
		rebalanceReqTotal:   metric.NewCounter(metaRebalanceReqTotal),
	}
}

// processRebalanceStart indicates the start of processing a rebalance request.
func (m *Metrics) processRebalanceStart() {
	m.rebalanceReqRunning.Inc(1)
	m.rebalanceReqTotal.Inc(1)
}

// processRebalanceFinish indicates the end of processing a rebalance request.
func (m *Metrics) processRebalanceFinish() {
	m.rebalanceReqRunning.Dec(1)
}
