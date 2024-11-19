// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package balancer

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// Metrics contains pointers to the metrics for monitoring balancer-related
// operations.
type Metrics struct {
	RebalanceReqRunning *metric.Gauge
	RebalanceReqQueued  *metric.Gauge
	RebalanceReqTotal   *metric.Counter
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
		RebalanceReqRunning: metric.NewGauge(metaRebalanceReqRunning),
		RebalanceReqQueued:  metric.NewGauge(metaRebalanceReqQueued),
		RebalanceReqTotal:   metric.NewCounter(metaRebalanceReqTotal),
	}
}

// processRebalanceStart indicates the start of processing a rebalance request.
func (m *Metrics) processRebalanceStart() {
	m.RebalanceReqRunning.Inc(1)
	m.RebalanceReqTotal.Inc(1)
}

// processRebalanceFinish indicates the end of processing a rebalance request.
func (m *Metrics) processRebalanceFinish() {
	m.RebalanceReqRunning.Dec(1)
}
