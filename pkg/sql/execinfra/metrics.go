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
	QueriesActive         *metric.Gauge
	QueriesTotal          *metric.Counter
	ContendedQueriesCount *metric.Counter
	FlowsActive           *metric.Gauge
	FlowsTotal            *metric.Counter
	MaxBytesHist          metric.IHistogram
	CurBytesCount         *metric.Gauge
	VecOpenFDs            *metric.Gauge
	CurDiskBytesCount     *metric.Gauge
	MaxDiskBytesHist      metric.IHistogram
	QueriesSpilled        *metric.Counter
	SpilledBytesWritten   *metric.Counter
	SpilledBytesRead      *metric.Counter
}

// MetricStruct implements the metrics.Struct interface.
func (DistSQLMetrics) MetricStruct() {}

var _ metric.Struct = DistSQLMetrics{}

var (
	metaQueriesActive = metric.Metadata{
		Name:        "sql.distsql.queries.active",
		Help:        "Number of SQL queries currently active",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaQueriesTotal = metric.Metadata{
		Name:        "sql.distsql.queries.total",
		Help:        "Number of SQL queries executed",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaContendedQueriesCount = metric.Metadata{
		Name:        "sql.distsql.contended_queries.count",
		Help:        "Number of SQL queries that experienced contention",
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
	metaVecOpenFDs = metric.Metadata{
		Name:        "sql.distsql.vec.openfds",
		Help:        "Current number of open file descriptors used by vectorized external storage",
		Measurement: "Files",
		Unit:        metric.Unit_COUNT,
	}
	metaDiskCurBytes = metric.Metadata{
		Name:        "sql.disk.distsql.current",
		Help:        "Current sql statement disk usage for distsql",
		Measurement: "Disk",
		Unit:        metric.Unit_BYTES,
	}
	metaDiskMaxBytes = metric.Metadata{
		Name:        "sql.disk.distsql.max",
		Help:        "Disk usage per sql statement for distsql",
		Measurement: "Disk",
		Unit:        metric.Unit_BYTES,
	}
	metaQueriesSpilled = metric.Metadata{
		Name:        "sql.distsql.queries.spilled",
		Help:        "Number of queries that have spilled to disk",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaSpilledBytesWritten = metric.Metadata{
		Name:        "sql.disk.distsql.spilled.bytes.written",
		Help:        "Number of bytes written to temporary disk storage as a result of spilling",
		Measurement: "Disk",
		Unit:        metric.Unit_BYTES,
	}
	metaSpilledBytesRead = metric.Metadata{
		Name:        "sql.disk.distsql.spilled.bytes.read",
		Help:        "Number of bytes read from temporary disk storage as a result of spilling",
		Measurement: "Disk",
		Unit:        metric.Unit_BYTES,
	}
)

// See pkg/sql/mem_metrics.go
// log10int64times1000 = log10(math.MaxInt64) * 1000, rounded up somewhat
const log10int64times1000 = 19 * 1000

// MakeDistSQLMetrics instantiates the metrics holder for DistSQL monitoring.
func MakeDistSQLMetrics(histogramWindow time.Duration) DistSQLMetrics {
	return DistSQLMetrics{
		QueriesActive:         metric.NewGauge(metaQueriesActive),
		QueriesTotal:          metric.NewCounter(metaQueriesTotal),
		ContendedQueriesCount: metric.NewCounter(metaContendedQueriesCount),
		FlowsActive:           metric.NewGauge(metaFlowsActive),
		FlowsTotal:            metric.NewCounter(metaFlowsTotal),
		MaxBytesHist: metric.NewHistogram(metric.HistogramOptions{
			Metadata: metaMemMaxBytes,
			Duration: histogramWindow,
			MaxVal:   log10int64times1000,
			SigFigs:  3,
			Buckets:  metric.MemoryUsage64MBBuckets,
		}),
		CurBytesCount:     metric.NewGauge(metaMemCurBytes),
		VecOpenFDs:        metric.NewGauge(metaVecOpenFDs),
		CurDiskBytesCount: metric.NewGauge(metaDiskCurBytes),
		MaxDiskBytesHist: metric.NewHistogram(metric.HistogramOptions{
			Metadata: metaDiskMaxBytes,
			Duration: histogramWindow,
			MaxVal:   log10int64times1000,
			SigFigs:  3,
			Buckets:  metric.MemoryUsage64MBBuckets}),
		QueriesSpilled:      metric.NewCounter(metaQueriesSpilled),
		SpilledBytesWritten: metric.NewCounter(metaSpilledBytesWritten),
		SpilledBytesRead:    metric.NewCounter(metaSpilledBytesRead),
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
