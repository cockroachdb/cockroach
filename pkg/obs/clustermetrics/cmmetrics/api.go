// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmmetrics

import (
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	prometheusgo "github.com/prometheus/client_model/go"
)

// TestingAllowNonInitConstruction disables the init-time assertion for
// cluster metric constructors. It returns a cleanup function that
// re-enables the check.
func TestingAllowNonInitConstruction() func() {
	skipInitCheck.Store(true)
	return func() { skipInitCheck.Store(false) }
}

var skipInitCheck atomic.Bool

// ClusterMetric is a marker interface for metrics intended to be registered
// with the cluster metrics writer. The Writer rejects metrics that do not
// implement this interface, mirroring how metric.NonExportableMetric
// prevents cluster metrics from being added to prometheus registries.
type ClusterMetric interface {
	ClusterMetric()
}

// IsClusterMetric is a mixin that implements the ClusterMetric interface.
// Embed it in metric types that should be accepted by the Writer.
type IsClusterMetric struct{}

// ClusterMetric implements the ClusterMetric interface.
func (IsClusterMetric) ClusterMetric() {}

// WritableMetric provides the data needed to write a metric to storage.
// Both Metric types and metricSnapshot (from vec children) satisfy
// this interface.
type WritableMetric interface {
	// GetName returns the metric name.
	GetName(useStaticLabels bool) string
	// Value returns the current value of the metric.
	Value() int64
	// GetType returns the prometheus metric type enum (e.g. GAUGE,
	// COUNTER). Use MetricTypeString to get the string stored in
	// system.cluster_metrics, which may differ (e.g. WriteStopwatch
	// reports GAUGE here but is stored as "STOPWATCH").
	GetType() *prometheusgo.MetricType
	// GetLabels returns the label key-value pairs for this metric.
	// Returns nil for unlabeled metrics.
	GetLabels() map[string]string
}

// Metric is the interface for cluster metrics. It extends
// WritableMetric with dirty tracking and reset for the flush lifecycle.
// Embedding NonExportableMetric prevents these metrics from being
// accidentally registered with node/store/tenant prometheus registries.
type Metric interface {
	metric.Iterable
	metric.NonExportableMetric
	WritableMetric
	// IsDirty returns true if the metric has changed since the last flush.
	IsDirty() bool
	// Reset resets the metric's state after a successful flush.
	// For counters, this resets the count to zero and clears the dirty flag.
	// For gauges, this clears the dirty flag (value is retained).
	Reset()
}

// MetricVec is the interface for vector metrics that contain labeled
// children. The writer iterates children via Each() and resets the
// parent after a successful flush. Reset clears all children so
// only values set since the last flush are emitted next time.
// Embedding NonExportableMetric prevents these metrics from being
// accidentally registered with node/store/tenant prometheus registries.
type MetricVec interface {
	metric.Iterable
	metric.NonExportableMetric
	// Each calls f for every child metric.
	Each(f func(WritableMetric))
	// Reset clears all children. Called after a successful flush.
	Reset()
}

// MetricTypeString returns the type string stored in the
// cluster_metrics table. For most metrics this is the prometheus type
// name (e.g. "COUNTER", "GAUGE"). Stopwatch metrics report GAUGE as
// their prometheus type but are stored as "STOPWATCH" so the read
// path can distinguish them.
func MetricTypeString(m WritableMetric) string {
	if _, ok := m.(*WriteStopwatch); ok {
		return "STOPWATCH"
	}
	if s, ok := m.(*metricSnapshot); ok && s.typeString != "" {
		return s.typeString
	}
	return m.GetType().String()
}
