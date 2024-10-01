// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// BaseMemoryMetrics contains a max histogram and a current count of the
// bytes allocated by a sql endpoint.
type BaseMemoryMetrics struct {
	MaxBytesHist  metric.IHistogram
	CurBytesCount *metric.Gauge
}

// MetricStruct implements the metrics.Struct interface.
func (BaseMemoryMetrics) MetricStruct() {}

// MemoryMetrics contains pointers to the metrics object
// for one of the SQL endpoints:
// - "client" for connections received via pgwire.
// - "admin" for connections received via the admin RPC.
// - "internal" for activities related to leases, schema changes, etc.
type MemoryMetrics struct {
	BaseMemoryMetrics
	TxnMaxBytesHist      metric.IHistogram
	TxnCurBytesCount     *metric.Gauge
	SessionMaxBytesHist  metric.IHistogram
	SessionCurBytesCount *metric.Gauge

	// For prepared statements.
	SessionPreparedMaxBytesHist  metric.IHistogram
	SessionPreparedCurBytesCount *metric.Gauge
}

// MetricStruct implements the metrics.Struct interface.
func (MemoryMetrics) MetricStruct() {}

var _ metric.Struct = MemoryMetrics{}

// TODO(knz): Until #10014 is addressed, the UI graphs don't have a
// log scale on the Y axis and the histograms are thus displayed using
// a manual log scale: we store the logarithm in the value in the DB
// and plot that logarithm in the UI.
//
// We could, but do not, store the full value in the DB and compute
// the log in the UI, because the current histogram implementation
// does not deal well with large maxima (#10015).
//
// Since the DB stores an integer, we scale the values by 1000 so that
// a modicum of precision is restored when exponentiating the value.
//

// log10int64times1000 = log10(math.MaxInt64) * 1000, rounded up somewhat
const log10int64times1000 = 19 * 1000

func makeMemMetricMetadata(name, help string) metric.Metadata {
	return metric.Metadata{
		Name:        name,
		Help:        help,
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
}

func makeMemMetricHistogram(
	metadata metric.Metadata, histogramWindow time.Duration,
) metric.IHistogram {
	return metric.NewHistogram(metric.HistogramOptions{
		Metadata:     metadata,
		Duration:     histogramWindow,
		MaxVal:       log10int64times1000,
		SigFigs:      3,
		BucketConfig: metric.MemoryUsage64MBBuckets,
	})
}

// MakeBaseMemMetrics instantiates the metric objects for an SQL endpoint, but
// only includes the root metrics: .max and .current, without txn and session.
func MakeBaseMemMetrics(endpoint string, histogramWindow time.Duration) BaseMemoryMetrics {
	prefix := "sql.mem." + endpoint
	MetaMemMaxBytes := makeMemMetricMetadata(prefix+".max", "Memory usage per sql statement for "+endpoint)
	MetaMemCurBytes := makeMemMetricMetadata(prefix+".current", "Current sql statement memory usage for "+endpoint)
	return BaseMemoryMetrics{
		MaxBytesHist:  makeMemMetricHistogram(MetaMemMaxBytes, histogramWindow),
		CurBytesCount: metric.NewGauge(MetaMemCurBytes),
	}
}

// MakeMemMetrics instantiates the metric objects for an SQL endpoint.
func MakeMemMetrics(endpoint string, histogramWindow time.Duration) MemoryMetrics {
	base := MakeBaseMemMetrics(endpoint, histogramWindow)
	prefix := "sql.mem." + endpoint
	MetaMemMaxTxnBytes := makeMemMetricMetadata(prefix+".txn.max", "Memory usage per sql transaction for "+endpoint)
	MetaMemTxnCurBytes := makeMemMetricMetadata(prefix+".txn.current", "Current sql transaction memory usage for "+endpoint)
	MetaMemMaxSessionBytes := makeMemMetricMetadata(prefix+".session.max", "Memory usage per sql session for "+endpoint)
	MetaMemSessionCurBytes := makeMemMetricMetadata(prefix+".session.current", "Current sql session memory usage for "+endpoint)
	MetaMemMaxSessionPreparedBytes := makeMemMetricMetadata(prefix+".session.prepared.max", "Memory usage by prepared statements per sql session for "+endpoint)
	MetaMemSessionPreparedCurBytes := makeMemMetricMetadata(prefix+".session.prepared.current", "Current sql session memory usage by prepared statements for "+endpoint)
	return MemoryMetrics{
		BaseMemoryMetrics:            base,
		TxnMaxBytesHist:              makeMemMetricHistogram(MetaMemMaxTxnBytes, histogramWindow),
		TxnCurBytesCount:             metric.NewGauge(MetaMemTxnCurBytes),
		SessionMaxBytesHist:          makeMemMetricHistogram(MetaMemMaxSessionBytes, histogramWindow),
		SessionCurBytesCount:         metric.NewGauge(MetaMemSessionCurBytes),
		SessionPreparedMaxBytesHist:  makeMemMetricHistogram(MetaMemMaxSessionPreparedBytes, histogramWindow),
		SessionPreparedCurBytesCount: metric.NewGauge(MetaMemSessionPreparedCurBytes),
	}
}
