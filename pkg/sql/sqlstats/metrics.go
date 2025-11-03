// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlstats

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

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
const metricsSampleInterval = 10 * time.Second

// Metrics groups metrics related to SQL Stats collection.
type Metrics struct {
	SQLStatsMemoryMaxBytesHist  metric.IHistogram
	SQLStatsMemoryCurBytesCount *metric.Gauge

	ReportedSQLStatsMemoryMaxBytesHist  metric.IHistogram
	ReportedSQLStatsMemoryCurBytesCount *metric.Gauge

	DiscardedStatsCount *metric.Counter

	SQLStatsFlushesSuccessful       *metric.Counter
	SQLStatsFlushDoneSignalsIgnored *metric.Counter
	SQLStatsFlushFingerprintCount   *metric.Counter
	SQLStatsFlushesFailed           *metric.Counter
	SQLStatsFlushLatency            metric.IHistogram
	SQLStatsRemovedRows             *metric.Counter

	SQLTxnStatsCollectionOverhead metric.IHistogram
	// IngesterNumProcessed tracks how many items have been processed.
	IngesterNumProcessed *metric.Counter

	// IngesterQueueSize tracks the current number of items queued to be processed.
	IngesterQueueSize *metric.Gauge
}

// StatsMetrics is part of the metric.Struct interface.
var _ metric.Struct = Metrics{}

var (
	MetaSQLStatsMemMaxBytes = metric.Metadata{
		Name:        "sql.stats.mem.max",
		Help:        "Memory usage for fingerprint storage",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	MetaSQLStatsMemCurBytes = metric.Metadata{
		Name:        "sql.stats.mem.current",
		Help:        "Current memory usage for fingerprint storage",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	MetaReportedSQLStatsMemMaxBytes = metric.Metadata{
		Name:        "sql.stats.reported.mem.max",
		Help:        "Memory usage for reported fingerprint storage",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	MetaReportedSQLStatsMemCurBytes = metric.Metadata{
		Name:        "sql.stats.reported.mem.current",
		Help:        "Current memory usage for reported fingerprint storage",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	MetaDiscardedSQLStats = metric.Metadata{
		Name:        "sql.stats.discarded.current",
		Help:        "Number of fingerprint statistics being discarded",
		Measurement: "Discarded SQL Stats",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLStatsFlushesSuccessful = metric.Metadata{
		Name:        "sql.stats.flushes.successful",
		Help:        "Number of times SQL Stats are flushed successfully to persistent storage",
		Measurement: "successful flushes",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLStatsFlushDoneSignalsIgnored = metric.Metadata{
		Name: "sql.stats.flush.done_signals.ignored",
		Help: "Number of times the SQL Stats activity update job ignored the signal sent to it indicating " +
			"a flush has completed",
		Measurement: "flush done signals ignored",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	MetaSQLStatsFlushFingerprintCount = metric.Metadata{
		Name:        "sql.stats.flush.fingerprint.count",
		Help:        "The number of unique statement and transaction fingerprints included in the SQL Stats flush",
		Measurement: "statement & transaction fingerprints",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLStatsFlushesFailed = metric.Metadata{
		Name:        "sql.stats.flushes.failed",
		Help:        "Number of attempted SQL Stats flushes that failed with errors",
		Measurement: "failed flushes",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLStatsFlushLatency = metric.Metadata{
		Name:        "sql.stats.flush.latency",
		Help:        "The latency of SQL Stats flushes to persistent storage. Includes failed flush attempts",
		Measurement: "nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaSQLStatsRemovedRows = metric.Metadata{
		Name:        "sql.stats.cleanup.rows_removed",
		Help:        "Number of stale statistics rows that are removed",
		Measurement: "SQL Stats Cleanup",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLTxnStatsCollectionOverhead = metric.Metadata{
		Name:        "sql.stats.txn_stats_collection.duration",
		Help:        "Time took in nanoseconds to collect transaction stats",
		Measurement: "SQL Transaction Stats Collection Overhead",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaIngesterNumProcessed = metric.Metadata{
		Name:        "sql.stats.ingester.num_processed",
		Help:        "Number of items processed by the SQL stats ingester",
		Measurement: "Items",
		Unit:        metric.Unit_COUNT,
	}
	MetaIngesterQueueSize = metric.Metadata{
		Name:        "sql.stats.ingester.queue_size",
		Help:        "Current number of items queued in the SQL stats ingester",
		Measurement: "Items",
		Unit:        metric.Unit_COUNT,
	}
)

// MetricStruct is part of the metric.Struct interface.
func (Metrics) MetricStruct() {}

func NewMetrics(HistogramWindowInterval time.Duration) Metrics {
	return Metrics{
		SQLStatsMemoryMaxBytesHist: metric.NewHistogram(metric.HistogramOptions{
			Metadata:     MetaSQLStatsMemMaxBytes,
			Duration:     HistogramWindowInterval,
			MaxVal:       log10int64times1000,
			SigFigs:      3,
			BucketConfig: metric.MemoryUsage64MBBuckets,
		}),
		SQLStatsMemoryCurBytesCount: metric.NewGauge(MetaSQLStatsMemCurBytes),
		ReportedSQLStatsMemoryMaxBytesHist: metric.NewHistogram(metric.HistogramOptions{
			Metadata:     MetaReportedSQLStatsMemMaxBytes,
			Duration:     HistogramWindowInterval,
			MaxVal:       log10int64times1000,
			SigFigs:      3,
			BucketConfig: metric.MemoryUsage64MBBuckets,
		}),
		ReportedSQLStatsMemoryCurBytesCount: metric.NewGauge(MetaReportedSQLStatsMemCurBytes),
		DiscardedStatsCount:                 metric.NewCounter(MetaDiscardedSQLStats),
		SQLStatsFlushesSuccessful:           metric.NewCounter(MetaSQLStatsFlushesSuccessful),
		SQLStatsFlushDoneSignalsIgnored:     metric.NewCounter(MetaSQLStatsFlushDoneSignalsIgnored),
		SQLStatsFlushFingerprintCount:       metric.NewCounter(MetaSQLStatsFlushFingerprintCount),

		SQLStatsFlushesFailed: metric.NewCounter(MetaSQLStatsFlushesFailed),
		SQLStatsFlushLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     MetaSQLStatsFlushLatency,
			Duration:     6 * metricsSampleInterval,
			BucketConfig: metric.BatchProcessLatencyBuckets,
		}),
		SQLStatsRemovedRows: metric.NewCounter(MetaSQLStatsRemovedRows),
		SQLTxnStatsCollectionOverhead: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     MetaSQLTxnStatsCollectionOverhead,
			Duration:     6 * metricsSampleInterval,
			BucketConfig: metric.IOLatencyBuckets,
		}),
		IngesterNumProcessed: metric.NewCounter(MetaIngesterNumProcessed),
		IngesterQueueSize:    metric.NewGauge(MetaIngesterQueueSize),
	}
}
