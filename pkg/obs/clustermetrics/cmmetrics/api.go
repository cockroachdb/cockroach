// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmmetrics

import (
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
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

// assertCalledDuringInit panics in test builds if the caller is not
// inside a package-level variable declaration or an init() function.
// Cluster metrics must be created at init time so that their metadata
// is registered before the cmreader starts consuming rangefeed events.
func assertCalledDuringInit() {
	if !buildutil.CrdbTestBuild || skipInitCheck.Load() {
		return
	}
	var pcs [10]uintptr
	n := runtime.Callers(3, pcs[:]) // skip Callers, assertCalledDuringInit, NewXxx
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, ".init") ||
			strings.Contains(frame.Function, ".init.") {
			return
		}
		if !more {
			break
		}
	}
	panic("cluster metric constructors must be called from a package-level var declaration or init() function")
}

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
// Both scalar Metric types and metricSnapshot (from vec children) satisfy
// this interface.
type WritableMetric interface {
	// GetName returns the metric name.
	GetName(useStaticLabels bool) string
	// Value returns the current value of the metric.
	Value() int64
	// GetType returns the metric type. For standard metrics this is a
	// prometheus enum value; for stopwatch it is the custom
	// metric.MetricType_STOPWATCH sentinel. Use MetricTypeString to
	// convert to the storage string.
	GetType() *prometheusgo.MetricType
	// GetLabels returns the label key-value pairs for this metric.
	// Returns nil for unlabeled metrics.
	GetLabels() map[string]string
}

// Metric is the interface for scalar cluster metrics. It extends
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

// MetricTypeString converts a metric type enum to the string stored in
// the cluster_metrics table.
func MetricTypeString(t *prometheusgo.MetricType) string {
	if *t == metric.MetricType_STOPWATCH {
		return "STOPWATCH"
	}
	return t.String()
}
