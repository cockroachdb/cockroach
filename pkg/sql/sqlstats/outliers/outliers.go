// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package outliers

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	prometheus "github.com/prometheus/client_model/go"
)

// LatencyThreshold configures the execution time beyond which a statement is
// considered an outlier. A LatencyThreshold of 0 (the default) disables the
// outliers subsystem. This setting lives in an "experimental" namespace
// because we expect to supplant threshold-based detection with something
// more statistically interesting, see #79451.
var LatencyThreshold = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.stats.outliers.experimental.latency_threshold",
	"amount of time after which an executing statement is considered an outlier. Use 0 to disable.",
	0,
)

// LatencyQuantileDetectorEnabled turns on a per-fingerprint heuristic-based
// algorithm for marking statements as outliers, attempting to capture elevated
// p99 latency while generally excluding uninteresting executions less than
// 100ms.
var LatencyQuantileDetectorEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.stats.outliers.experimental.latency_quantile_detection.enabled",
	"enable per-fingerprint latency recording and outlier detection",
	false,
)

// LatencyQuantileDetectorInterestingThreshold sets the bar above which
// statements are considered "interesting" from an outliers perspective. A
// statement's latency must first cross this threshold before we begin tracking
// further execution latencies for its fingerprint (this is a memory
// optimization), and any potential outlier execution must also cross this
// threshold to be reported (this is a UX optimization, removing noise).
var LatencyQuantileDetectorInterestingThreshold = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.stats.outliers.experimental.latency_quantile_detection.interesting_threshold",
	"statements must surpass this threshold to trigger outlier detection and identification",
	100*time.Millisecond,
	settings.NonNegativeDuration,
)

// LatencyQuantileDetectorMemoryCap restricts the overall memory available for
// tracking per-statement execution latencies. When changing this setting, keep
// an eye on the metrics for memory usage and evictions to avoid introducing
// churn.
var LatencyQuantileDetectorMemoryCap = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"sql.stats.outliers.experimental.latency_quantile_detection.memory_limit",
	"the maximum amount of memory allowed for tracking statement latencies",
	1024*1024,
	settings.NonNegativeInt,
)

// Metrics holds running measurements of various outliers-related runtime stats.
type Metrics struct {
	// Fingerprints measures the number of statement fingerprints being monitored for
	// outlier detection.
	Fingerprints *metric.Gauge

	// Memory measures the memory used in support of outlier detection.
	Memory *metric.Gauge

	// Evictions counts fingerprint latency summaries discarded due to memory
	// pressure.
	Evictions *metric.Counter
}

// MetricStruct marks Metrics for automatic member metric registration.
func (Metrics) MetricStruct() {}

var _ metric.Struct = Metrics{}

// NewMetrics builds a new instance of our Metrics struct.
func NewMetrics() Metrics {
	return Metrics{
		Fingerprints: metric.NewGauge(metric.Metadata{
			Name:        "sql.stats.outliers.latency_quantile_detector.fingerprints",
			Help:        "Current number of statement fingerprints being monitored for outlier detection",
			Measurement: "Fingerprints",
			Unit:        metric.Unit_COUNT,
			MetricType:  prometheus.MetricType_GAUGE,
		}),
		Memory: metric.NewGauge(metric.Metadata{
			Name:        "sql.stats.outliers.latency_quantile_detector.memory",
			Help:        "Current memory used to support outlier detection",
			Measurement: "Memory",
			Unit:        metric.Unit_BYTES,
			MetricType:  prometheus.MetricType_GAUGE,
		}),
		Evictions: metric.NewCounter(metric.Metadata{
			Name:        "sql.stats.outliers.latency_quantile_detector.evictions",
			Help:        "Evictions of fingerprint latency summaries due to memory pressure",
			Measurement: "Evictions",
			Unit:        metric.Unit_COUNT,
			MetricType:  prometheus.MetricType_COUNTER,
		}),
	}
}

// Reader offers read-only access to the currently retained set of outliers.
type Reader interface {
	// IterateOutliers calls visitor with each of the currently retained set of outliers.
	IterateOutliers(context.Context, func(context.Context, *Outlier))
}

// Registry is the central object in the outliers subsystem. It observes
// statement execution to determine which statements are outliers and
// exposes the set of currently retained outliers.
type Registry interface {
	Start(ctx context.Context, stopper *stop.Stopper)

	// ObserveStatement notifies the registry of a statement execution.
	ObserveStatement(sessionID clusterunique.ID, statement *Statement)

	// ObserveTransaction notifies the registry of the end of a transaction.
	ObserveTransaction(sessionID clusterunique.ID, transaction *Transaction)

	Reader

	enabled() bool
}

// New builds a new Registry.
func New(st *cluster.Settings, metrics Metrics) Registry {
	return newRegistry(st, metrics)
}
