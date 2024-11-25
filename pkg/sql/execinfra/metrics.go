// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfra

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// DistSQLMetrics contains pointers to the metrics for monitoring DistSQL
// processing.
type DistSQLMetrics struct {
	QueriesActive               *metric.Gauge
	QueriesTotal                *metric.Counter
	DistributedCount            *metric.Counter
	ContendedQueriesCount       *metric.Counter
	CumulativeContentionNanos   *metric.Counter
	FlowsActive                 *metric.Gauge
	FlowsTotal                  *metric.Counter
	MaxBytesHist                metric.IHistogram
	CurBytesCount               *metric.Gauge
	VecOpenFDs                  *metric.Gauge
	CurDiskBytesCount           *metric.Gauge
	MaxDiskBytesHist            metric.IHistogram
	QueriesSpilled              *metric.Counter
	SpilledBytesWritten         *metric.Counter
	SpilledBytesRead            *metric.Counter
	DistErrorLocalRetryAttempts *metric.Counter
	DistErrorLocalRetryFailures *metric.Counter
}

// MetricStruct implements the metrics.Struct interface.
func (DistSQLMetrics) MetricStruct() {}

var _ metric.Struct = DistSQLMetrics{}

var (
	metaQueriesActive = metric.Metadata{
		Name:        "sql.distsql.queries.active",
		Help:        "Number of invocations of the execution engine currently active (multiple of which may occur for a single SQL statement)",
		Measurement: "DistSQL runs",
		Unit:        metric.Unit_COUNT,
	}
	metaQueriesTotal = metric.Metadata{
		Name:        "sql.distsql.queries.total",
		Help:        "Number of invocations of the execution engine executed (multiple of which may occur for a single SQL statement)",
		Measurement: "DistSQL runs",
		Unit:        metric.Unit_COUNT,
	}
	metaDistributedCount = metric.Metadata{
		Name:        "sql.distsql.distributed_exec.count",
		Help:        "Number of invocations of the execution engine executed with full or partial distribution (multiple of which may occur for a single SQL statement)",
		Measurement: "DistSQL runs",
		Unit:        metric.Unit_COUNT,
	}
	metaContendedQueriesCount = metric.Metadata{
		Name:        "sql.distsql.contended_queries.count",
		Help:        "Number of SQL queries that experienced contention",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaCumulativeContentionNanos = metric.Metadata{
		Name:        "sql.distsql.cumulative_contention_nanos",
		Help:        "Cumulative contention across all queries (in nanoseconds)",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
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
	metaDistErrorLocalRetryAttempts = metric.Metadata{
		Name:        "sql.distsql.dist_query_rerun_locally.count",
		Help:        "Total number of cases when distributed query error resulted in a local rerun",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaDistErrorLocalRetryFailures = metric.Metadata{
		Name:        "sql.distsql.dist_query_rerun_locally.failure_count",
		Help:        "Total number of cases when the local rerun of a distributed query resulted in an error",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
)

// See pkg/sql/mem_metrics.go
// log10int64times1000 = log10(math.MaxInt64) * 1000, rounded up somewhat
const log10int64times1000 = 19 * 1000

// MakeDistSQLMetrics instantiates the metrics holder for DistSQL monitoring.
func MakeDistSQLMetrics(histogramWindow time.Duration) DistSQLMetrics {
	return DistSQLMetrics{
		QueriesActive:             metric.NewGauge(metaQueriesActive),
		QueriesTotal:              metric.NewCounter(metaQueriesTotal),
		DistributedCount:          metric.NewCounter(metaDistributedCount),
		ContendedQueriesCount:     metric.NewCounter(metaContendedQueriesCount),
		CumulativeContentionNanos: metric.NewCounter(metaCumulativeContentionNanos),
		FlowsActive:               metric.NewGauge(metaFlowsActive),
		FlowsTotal:                metric.NewCounter(metaFlowsTotal),
		MaxBytesHist: metric.NewHistogram(metric.HistogramOptions{
			Metadata:     metaMemMaxBytes,
			Duration:     histogramWindow,
			MaxVal:       log10int64times1000,
			SigFigs:      3,
			BucketConfig: metric.MemoryUsage64MBBuckets,
		}),
		CurBytesCount:     metric.NewGauge(metaMemCurBytes),
		VecOpenFDs:        metric.NewGauge(metaVecOpenFDs),
		CurDiskBytesCount: metric.NewGauge(metaDiskCurBytes),
		MaxDiskBytesHist: metric.NewHistogram(metric.HistogramOptions{
			Metadata:     metaDiskMaxBytes,
			Duration:     histogramWindow,
			MaxVal:       log10int64times1000,
			SigFigs:      3,
			BucketConfig: metric.MemoryUsage64MBBuckets}),
		QueriesSpilled:              metric.NewCounter(metaQueriesSpilled),
		SpilledBytesWritten:         metric.NewCounter(metaSpilledBytesWritten),
		SpilledBytesRead:            metric.NewCounter(metaSpilledBytesRead),
		DistErrorLocalRetryAttempts: metric.NewCounter(metaDistErrorLocalRetryAttempts),
		DistErrorLocalRetryFailures: metric.NewCounter(metaDistErrorLocalRetryFailures),
	}
}

// RunStart registers the start of an invocation of the DistSQL engine.
func (m *DistSQLMetrics) RunStart(distributed bool) {
	m.QueriesActive.Inc(1)
	m.QueriesTotal.Inc(1)
	if distributed {
		m.DistributedCount.Inc(1)
	}
}

// RunStop registers the end of an invocation of the DistSQL engine.
func (m *DistSQLMetrics) RunStop() {
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
