// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// DistSQLMetrics contains pointers to the metrics for monitoring DistSQL
// processing.
type DistSQLMetrics struct {
	QueriesActive *metric.Gauge
	QueriesTotal  *metric.Counter
	FlowsActive   *metric.Gauge
	FlowsTotal    *metric.Counter
	FlowsQueued   *metric.Gauge
	QueueWaitHist *metric.Histogram
	MaxBytesHist  *metric.Histogram
	CurBytesCount *metric.Gauge
}

// MetricStruct implements the metrics.Struct interface.
func (DistSQLMetrics) MetricStruct() {}

var _ metric.Struct = DistSQLMetrics{}

var (
	metaQueriesActive = metric.Metadata{
		Name:        "sql.distsql.queries.active",
		Help:        "Number of distributed SQL queries currently active",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaQueriesTotal = metric.Metadata{
		Name:        "sql.distsql.queries.total",
		Help:        "Number of distributed SQL queries executed",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaFlowsActive = metric.Metadata{
		Name:        "sql.distsql.flows.active",
		Help:        "Number of distributed SQL flows currently active",
		Measurement: "Flows",
		Unit:        metric.Unit_COUNT,
	}
	metaFlowsTotal = metric.Metadata{
		Name:        "sql.distsql.flows.total",
		Help:        "Number of distributed SQL flows executed",
		Measurement: "Flows",
		Unit:        metric.Unit_COUNT,
	}
	metaFlowsQueued = metric.Metadata{
		Name:        "sql.distsql.flows.queued",
		Help:        "Number of distributed SQL flows currently queued",
		Measurement: "Flows",
		Unit:        metric.Unit_COUNT,
	}
	metaQueueWaitHist = metric.Metadata{
		Name:        "sql.distsql.flows.queue_wait",
		Help:        "Duration of time flows spend waiting in the queue",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaMemMaxBytes = metric.Metadata{
		Name:        "sql.mem.distsql.max",
		Help:        "Memory usage per sql statement for distsql",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaMemCurBytes = metric.Metadata{
		Name:        "sql.mem.distsql.current",
		Help:        "Current sql statement memory usage for distsql",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
)

// See pkg/sql/mem_metrics.go
// log10int64times1000 = log10(math.MaxInt64) * 1000, rounded up somewhat
const log10int64times1000 = 19 * 1000

// MakeDistSQLMetrics instantiates the metrics holder for DistSQL monitoring.
func MakeDistSQLMetrics(histogramWindow time.Duration) DistSQLMetrics {
	return DistSQLMetrics{
		QueriesActive: metric.NewGauge(metaQueriesActive),
		QueriesTotal:  metric.NewCounter(metaQueriesTotal),
		FlowsActive:   metric.NewGauge(metaFlowsActive),
		FlowsTotal:    metric.NewCounter(metaFlowsTotal),
		FlowsQueued:   metric.NewGauge(metaFlowsQueued),
		QueueWaitHist: metric.NewLatency(metaQueueWaitHist, histogramWindow),
		MaxBytesHist:  metric.NewHistogram(metaMemMaxBytes, histogramWindow, log10int64times1000, 3),
		CurBytesCount: metric.NewGauge(metaMemCurBytes),
	}
}

// QueryStart registers the start of a new DistSQL query.
func (m *DistSQLMetrics) QueryStart() {
	m.QueriesActive.Inc(1)
	m.QueriesTotal.Inc(1)
}

// QueryStop registers the end of a DistSQL query.
func (m *DistSQLMetrics) QueryStop() {
	m.QueriesActive.Dec(1)
}

// FlowStart registers the start of a new DistSQL flow.
func (m *DistSQLMetrics) FlowStart() {
	m.FlowsActive.Inc(1)
	m.FlowsTotal.Inc(1)
}

// FlowStop registers the end of a DistSQL flow.
func (m *DistSQLMetrics) FlowStop() {
	m.FlowsActive.Dec(1)
}
