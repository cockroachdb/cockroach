// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	prometheus "github.com/prometheus/client_model/go"
)

// ExecutionInsightsCapacity limits the number of execution insights retained in memory.
// As further insights are had, the oldest ones are evicted.
var ExecutionInsightsCapacity = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.insights.execution_insights_capacity",
	"the size of the per-node store of execution insights",
	1000,
	settings.NonNegativeInt,
	settings.WithPublic)

// LatencyThreshold configures the execution time beyond which a statement is
// considered slow. A LatencyThreshold of 0 (the default) disables this
// detection.
var LatencyThreshold = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.insights.latency_threshold",
	"amount of time after which an executing statement is considered slow. Use 0 to disable.",
	100*time.Millisecond,
	settings.WithPublic)

// AnomalyDetectionEnabled turns on a per-fingerprint heuristic-based
// algorithm for marking statements as slow, attempting to capture elevated
// p99 latency while generally excluding uninteresting executions less than
// 100ms.
var AnomalyDetectionEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.insights.anomaly_detection.enabled",
	"enable per-fingerprint latency recording and anomaly detection",
	true,
	settings.WithPublic)

// AnomalyDetectionLatencyThreshold sets the bar above which we consider
// statement executions worth inspecting for slow execution. A statement's
// latency must first cross this threshold before we begin tracking further
// execution latencies for its fingerprint (this is a memory optimization),
// and any potential slow execution must also cross this threshold to be
// reported (this is a UX optimization, removing noise).
var AnomalyDetectionLatencyThreshold = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.insights.anomaly_detection.latency_threshold",
	"statements must surpass this threshold to trigger anomaly detection and identification",
	50*time.Millisecond,
	settings.NonNegativeDuration,
	settings.WithPublic)

// AnomalyDetectionMemoryLimit restricts the overall memory available for
// tracking per-statement execution latencies. When changing this setting, keep
// an eye on the metrics for memory usage and evictions to avoid introducing
// churn.
var AnomalyDetectionMemoryLimit = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"sql.insights.anomaly_detection.memory_limit",
	"the maximum amount of memory allowed for tracking statement latencies",
	1024*1024,
	settings.WithPublic)

// HighRetryCountThreshold sets the number of times a slow statement must have
// been retried to be marked as having a high retry count.
var HighRetryCountThreshold = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.insights.high_retry_count.threshold",
	"the number of retries a slow statement must have undergone for its high retry count to be highlighted as a potential problem",
	10,
	settings.NonNegativeInt,
	settings.WithPublic)

// Metrics holds running measurements of various insights-related runtime stats.
type Metrics struct {
	// Fingerprints measures the number of statement fingerprints being monitored for
	// anomaly detection.
	Fingerprints *metric.Gauge

	// Memory measures the memory used in support of anomaly detection.
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

type PercentileValues struct {
	P50 float64
	P90 float64
	P99 float64
}

// New builds a new Provider.
func New(st *cluster.Settings, metrics Metrics, knobs *TestingKnobs) *Provider {
	store := newStore(st)
	anomalyDetector := newAnomalyDetector(st, metrics)

	return &Provider{
		store: store,
		ingester: newConcurrentBufferIngester(
			newRegistry(st, &compositeDetector{detectors: []detector{
				&latencyThresholdDetector{st: st},
				anomalyDetector,
			}}, store, knobs),
		),
		anomalyDetector: anomalyDetector,
	}
}
