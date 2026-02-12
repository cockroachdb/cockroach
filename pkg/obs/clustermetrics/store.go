// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clustermetrics

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// MetricStore abstracts the storage for cluster metrics.
type MetricStore interface {
	// Write writes the given metrics to storage. The store can use GetName()
	// and Get() on each metric to retrieve the name and value.
	Write(ctx context.Context, metrics []Metric) error
	// Get retrieves all stored metrics.
	Get(ctx context.Context) ([]Metric, error)
}

// LabeledMetricMetadata pairs metric metadata with the label names that
// the metric uses. It is stored in the labeled metadata registry.
type LabeledMetricMetadata struct {
	Metadata metric.Metadata
	Labels   []string
}

// metadataMu protects the registeredClusterMetrics and
// registeredLabeledClusterMetrics maps from concurrent access.
var metadataMu syncutil.RWMutex

var registeredClusterMetrics = map[string]metric.Metadata{}

var registeredLabeledClusterMetrics = map[string]LabeledMetricMetadata{}

// RegisterClusterMetric registers metadata for a scalar cluster metric.
// This should be called in an init() function so that metadata is available
// before the cmreader.updater starts reading from the rangefeed.
func RegisterClusterMetric(name string, md metric.Metadata) {
	metadataMu.Lock()
	defer metadataMu.Unlock()
	registeredClusterMetrics[name] = md
}

// RegisterLabeledClusterMetric registers metadata for a labeled (vector)
// cluster metric. This should be called in an init() function so that
// metadata is available before the cmreader.updater starts reading from
// the rangefeed.
func RegisterLabeledClusterMetric(name string, md LabeledMetricMetadata) {
	metadataMu.Lock()
	defer metadataMu.Unlock()
	registeredLabeledClusterMetrics[name] = md
}

// GetLabeledClusterMetricMetadata retrieves the metadata for a labeled
// cluster metric by name.
func GetLabeledClusterMetricMetadata(name string) (LabeledMetricMetadata, bool) {
	metadataMu.RLock()
	defer metadataMu.RUnlock()
	meta, ok := registeredLabeledClusterMetrics[name]
	return meta, ok
}

// GetClusterMetricMetadata retrieves the metadata for a scalar cluster
// metric by name.
func GetClusterMetricMetadata(name string) (metric.Metadata, bool) {
	metadataMu.RLock()
	defer metadataMu.RUnlock()
	meta, ok := registeredClusterMetrics[name]
	return meta, ok
}

// TestingRegisterClusterMetric adds a metric to the registry for testing.
// It returns a cleanup function that removes the entry.
func TestingRegisterClusterMetric(name string, md metric.Metadata) func() {
	RegisterClusterMetric(name, md)
	return func() {
		metadataMu.Lock()
		defer metadataMu.Unlock()
		delete(registeredClusterMetrics, name)
	}
}

// TestingRegisterLabeledClusterMetric adds a labeled metric for testing.
// It returns a cleanup function that removes the entry.
func TestingRegisterLabeledClusterMetric(name string, md LabeledMetricMetadata) func() {
	RegisterLabeledClusterMetric(name, md)
	return func() {
		metadataMu.Lock()
		defer metadataMu.Unlock()
		delete(registeredLabeledClusterMetrics, name)
	}
}
