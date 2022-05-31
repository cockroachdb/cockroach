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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

// LatencyQuantileDetectionEnabled turns on a per-fingerprint heuristic-based
// algorithm for marking statements as outliers, attempting to capture elevated
// p99 latency while generally excluding uninteresting executions less than
// 100ms.
var LatencyQuantileDetectionEnabled = settings.RegisterBoolSetting(
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
func NewMetrics() *Metrics {
	return &Metrics{
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

// maxCacheSize is the number of detected outliers we will retain in memory.
// We choose a small value for the time being to allow us to iterate without
// worrying about memory usage. See #79450.
const (
	maxCacheSize = 10
)

// Registry is the central object in the outliers subsystem. It observes
// statement execution to determine which statements are outliers and
// exposes the set of currently retained outliers.
type Registry struct {
	detector detector

	// Note that this single mutex places unnecessary constraints on outlier
	// detection and reporting. We will develop a higher-throughput system
	// before enabling the outliers subsystem by default.
	mu struct {
		syncutil.RWMutex
		statements map[clusterunique.ID][]*Outlier_Statement
		outliers   *cache.UnorderedCache
	}
}

// New builds a new Registry.
func New(st *cluster.Settings, metrics *Metrics) *Registry {
	config := cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > maxCacheSize
		},
	}
	r := &Registry{
		detector: anyDetector{detectors: []detector{
			latencyThresholdDetector{st: st},
			newLatencyQuantileDetector(st, metrics),
		}}}
	r.mu.statements = make(map[clusterunique.ID][]*Outlier_Statement)
	r.mu.outliers = cache.NewUnorderedCache(config)
	return r
}

// ObserveStatement notifies the registry of a statement execution.
func (r *Registry) ObserveStatement(
	sessionID clusterunique.ID,
	statementID clusterunique.ID,
	statementFingerprintID roachpb.StmtFingerprintID,
	latencyInSeconds float64,
) {
	if !r.enabled() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.statements[sessionID] = append(r.mu.statements[sessionID], &Outlier_Statement{
		ID:               statementID.GetBytes(),
		FingerprintID:    statementFingerprintID,
		LatencyInSeconds: latencyInSeconds,
	})
}

// ObserveTransaction notifies the registry of the end of a transaction.
func (r *Registry) ObserveTransaction(sessionID clusterunique.ID, txnID uuid.UUID) {
	if !r.enabled() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	statements := r.mu.statements[sessionID]
	delete(r.mu.statements, sessionID)

	hasOutlier := false
	for _, s := range statements {
		if r.detector.isOutlier(s) {
			hasOutlier = true
		}
	}

	if hasOutlier {
		for _, s := range statements {
			r.mu.outliers.Add(uint128.FromBytes(s.ID), &Outlier{
				Session:     &Outlier_Session{ID: sessionID.GetBytes()},
				Transaction: &Outlier_Transaction{ID: &txnID},
				Statement:   s,
			})
		}
	}
}

// TODO(todd):
//   Once we can handle sufficient throughput to live on the hot
//   execution path in #81021, we can probably get rid of this external
//   concept of "enabled" and let the detectors just decide for themselves
//   internally.
func (r *Registry) enabled() bool {
	return r.detector.enabled()
}

// Reader offers read-only access to the currently retained set of outliers.
type Reader interface {
	IterateOutliers(context.Context, func(context.Context, *Outlier))
}

// IterateOutliers calls visitor with each of the currently retained set of
// outliers.
func (r *Registry) IterateOutliers(ctx context.Context, visitor func(context.Context, *Outlier)) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.mu.outliers.Do(func(e *cache.Entry) {
		visitor(ctx, e.Value.(*Outlier))
	})
}
