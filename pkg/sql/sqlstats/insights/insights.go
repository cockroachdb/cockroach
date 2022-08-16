// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package insights

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
// considered slow. A LatencyThreshold of 0 (the default) disables this
// detection.
var LatencyThreshold = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.insights.latency_threshold",
	"amount of time after which an executing statement is considered slow. Use 0 to disable.",
	100*time.Millisecond,
).WithPublic()

// AnomalyDetectionEnabled turns on a per-fingerprint heuristic-based
// algorithm for marking statements as slow, attempting to capture elevated
// p99 latency while generally excluding uninteresting executions less than
// 100ms.
var AnomalyDetectionEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.insights.anomaly_detection.enabled",
	"enable per-fingerprint latency recording and anomaly detection",
	false,
)

// AnomalyDetectionLatencyThreshold sets the bar above which we consider
// statement executions worth inspecting for slow execution. A statement's
// latency must first cross this threshold before we begin tracking further
// execution latencies for its fingerprint (this is a memory optimization),
// and any potential slow execution must also cross this threshold to be
// reported (this is a UX optimization, removing noise).
var AnomalyDetectionLatencyThreshold = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.insights.anomaly_detection.latency_threshold",
	"statements must surpass this threshold to trigger anomaly detection and identification",
	100*time.Millisecond,
	settings.NonNegativeDuration,
)

// AnomalyDetectionMemoryLimit restricts the overall memory available for
// tracking per-statement execution latencies. When changing this setting, keep
// an eye on the metrics for memory usage and evictions to avoid introducing
// churn.
var AnomalyDetectionMemoryLimit = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"sql.insights.anomaly_detection.memory_limit",
	"the maximum amount of memory allowed for tracking statement latencies",
	1024*1024,
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
			Name:        "sql.insights.anomaly_detection.fingerprints",
			Help:        "Current number of statement fingerprints being monitored for anomaly detection",
			Measurement: "Fingerprints",
			Unit:        metric.Unit_COUNT,
			MetricType:  prometheus.MetricType_GAUGE,
		}),
		Memory: metric.NewGauge(metric.Metadata{
			Name:        "sql.insights.anomaly_detection.memory",
			Help:        "Current memory used to support anomaly detection",
			Measurement: "Memory",
			Unit:        metric.Unit_BYTES,
			MetricType:  prometheus.MetricType_GAUGE,
		}),
		Evictions: metric.NewCounter(metric.Metadata{
			Name:        "sql.insights.anomaly_detection.evictions",
			Help:        "Evictions of fingerprint latency summaries due to memory pressure",
			Measurement: "Evictions",
			Unit:        metric.Unit_COUNT,
			MetricType:  prometheus.MetricType_COUNTER,
		}),
	}
}

// Reader offers read-only access to the currently retained set of insights.
type Reader interface {
	// IterateInsights calls visitor with each of the currently retained set of insights.
	IterateInsights(context.Context, func(context.Context, *Insight))
}

// Registry is the central object in the insights subsystem. It observes
// statement execution, looking for suggestions we may expose to the user.
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
	return newConcurrentBufferIngester(newRegistry(st, metrics))
}
